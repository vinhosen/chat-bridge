const DEFAULT_GEMINI_MODEL = 'gemini-2.5-flash';

const AGENT_PROFILES = {
  gemini: {
    sender: 'gemini',
    defaultTarget: 'antigravity',
    systemPrompt:
      'You are Gemini, a sharp technical architect and code/system reviewer inside a collaborative Chat Bridge with Human and Antigravity. ' +
      'Respond concisely and constructively in the same language as the conversation (Thai or English). ' +
      'Offer concrete architectural feedback, edge cases, or next steps. Avoid repeating what was already agreed.'
  },
  antigravity: {
    sender: 'antigravity',
    defaultTarget: 'gemini',
    systemPrompt:
      'You are Antigravity, a pragmatic software engineer and builder inside a collaborative Chat Bridge with Human and Gemini. ' +
      'Respond concisely and concretely in the same language as the conversation (Thai or English). ' +
      'Propose actionable implementation steps, verify trade-offs with Gemini, and move the task toward completion.'
  }
};

export class AiWorker {
  /**
   * @param {import('./bridge.js').ChatBridge} bridge
   * @param {object} [options]
   * @param {string} [options.geminiApiKey]
   * @param {string} [options.antigravityApiKey]
   * @param {string} [options.geminiModel]
   * @param {string} [options.antigravityModel]
   * @param {number} [options.debounceMs]
   * @param {typeof fetch} [options.fetchImpl]
   */
  constructor(bridge, options = {}) {
    this.bridge = bridge;
    this.geminiApiKey = (
      options.geminiApiKey ??
      process.env.GEMINI_API_KEY ??
      ''
    ).trim();
    this.antigravityApiKey = (
      options.antigravityApiKey ??
      process.env.ANTIGRAVITY_API_KEY ??
      this.geminiApiKey
    ).trim();
    this.geminiModel = (
      options.geminiModel ??
      process.env.GEMINI_MODEL ??
      DEFAULT_GEMINI_MODEL
    ).trim();
    this.antigravityModel = (
      options.antigravityModel ??
      process.env.ANTIGRAVITY_MODEL ??
      this.geminiModel
    ).trim();
    this.debounceMs = Number.isFinite(Number(options.debounceMs))
      ? Math.max(0, Number(options.debounceMs))
      : 350;
    this.fetchImpl = options.fetchImpl ?? fetch;

    /** @type {Set<string>} */
    this.inFlight = new Set();
    /** @type {Map<string, NodeJS.Timeout>} */
    this.timers = new Map();
    this.closed = false;

    this.unsubscribe = this.bridge.subscribe((event) => {
      this.#onBridgeEvent(event);
    });
  }

  getStatus() {
    return {
      gemini_configured: Boolean(this.geminiApiKey),
      antigravity_configured: Boolean(this.antigravityApiKey),
      gemini_model: this.geminiModel,
      antigravity_model: this.antigravityModel
    };
  }

  #onBridgeEvent(event) {
    if (this.closed || !event || event.type !== 'message') return;

    const { channel_id, message, channel } = event;
    if (!message || !channel) return;
    if (channel.state === 'PAUSED' || channel.state === 'HALTED') return;
    if (channel.ai_turn_count >= channel.max_ai_turns) return;

    const senderLower = String(message.sender || '').toLowerCase();
    const targetLower = String(message.target || 'all').toLowerCase();
    const statusUpper = String(message.status || 'INFO').toUpperCase();

    if (senderLower === 'system') return;

    // Determine which auto-agent(s) should respond to this message
    if (
      this.#shouldRespond({
        agent: 'gemini',
        enabled: Boolean(channel.auto_gemini),
        apiKey: this.geminiApiKey,
        senderLower,
        targetLower,
        statusUpper
      })
    ) {
      this.#schedule(channel_id, 'gemini');
    }

    if (
      this.#shouldRespond({
        agent: 'antigravity',
        enabled: Boolean(channel.auto_antigravity),
        apiKey: this.antigravityApiKey,
        senderLower,
        targetLower,
        statusUpper
      })
    ) {
      this.#schedule(channel_id, 'antigravity');
    }
  }

  #shouldRespond({ agent, enabled, apiKey, senderLower, targetLower, statusUpper }) {
    if (!enabled || !apiKey) return false;
    if (senderLower === agent) return false;
    if (targetLower !== 'all' && targetLower !== agent) return false;
    // Avoid endless ping-pong after an AI explicitly marks its message DONE
    if (senderLower !== 'human' && statusUpper === 'DONE' && targetLower !== agent) {
      return false;
    }
    return true;
  }

  #schedule(channelId, agent) {
    const key = `${channelId}:${agent}`;
    if (this.timers.has(key)) {
      clearTimeout(this.timers.get(key));
    }

    const timer = setTimeout(() => {
      this.timers.delete(key);
      this.#runAgent(channelId, agent).catch((err) => {
        if (!this.closed) {
          this.bridge.sendMessage({
            channel_id: channelId,
            sender: 'system',
            target: 'all',
            content: `[Auto-${agent}] Error: ${err instanceof Error ? err.message : String(err)}`,
            status: 'ERROR'
          });
        }
      });
    }, this.debounceMs);
    timer.unref?.();
    this.timers.set(key, timer);
  }

  async #runAgent(channelId, agent) {
    if (this.closed) return;
    const key = `${channelId}:${agent}`;
    if (this.inFlight.has(key)) return;

    this.inFlight.add(key);
    try {
      const channel = this.bridge.getChannelState(channelId);
      if (channel.state === 'PAUSED' || channel.state === 'HALTED') return;
      if (channel.ai_turn_count >= channel.max_ai_turns) return;

      const enabled =
        agent === 'gemini' ? Boolean(channel.auto_gemini) : Boolean(channel.auto_antigravity);
      if (!enabled) return;

      const apiKey = agent === 'gemini' ? this.geminiApiKey : this.antigravityApiKey;
      const model = agent === 'gemini' ? this.geminiModel : this.antigravityModel;
      if (!apiKey) return;

      const { messages } = this.bridge.getMessages({
        channel_id: channelId,
        since_id: 0,
        limit: 20
      });
      if (messages.length === 0) return;

      const lastMsg = messages[messages.length - 1];
      if (String(lastMsg.sender || '').toLowerCase() === agent) return;

      const profile = AGENT_PROFILES[agent];
      const transcript = messages
        .map((m) => `[#${m.id} ${m.sender} -> ${m.target} (${m.status})]: ${m.content}`)
        .join('\n');

      const prompt =
        `Channel: ${channelId}\n` +
        `Recent conversation:\n${transcript}\n\n` +
        `Reply directly as ${profile.sender} (do not include prefixes like "[gemini]:" in your reply).`;

      const replyText = await this.#callGemini({
        apiKey,
        model,
        systemPrompt: profile.systemPrompt,
        prompt
      });

      if (this.closed || !replyText) return;

      const latestChannel = this.bridge.getChannelState(channelId);
      if (latestChannel.state === 'PAUSED' || latestChannel.state === 'HALTED') return;

      const lastSender = String(lastMsg.sender || '').toLowerCase();
      const otherEnabled =
        agent === 'gemini'
          ? Boolean(latestChannel.auto_antigravity)
          : Boolean(latestChannel.auto_gemini);
      const replyTarget =
        lastSender === 'human' && !otherEnabled ? 'human' : profile.defaultTarget;

      this.bridge.sendMessage({
        channel_id: channelId,
        sender: profile.sender,
        target: replyTarget,
        content: replyText.trim(),
        status: 'INFO'
      });
    } finally {
      this.inFlight.delete(key);
    }
  }

  async #callGemini({ apiKey, model, systemPrompt, prompt }) {
    const endpoint = `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(
      model
    )}:generateContent?key=${encodeURIComponent(apiKey)}`;

    const res = await this.fetchImpl(endpoint, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        systemInstruction: {
          parts: [{ text: systemPrompt }]
        },
        contents: [
          {
            role: 'user',
            parts: [{ text: prompt }]
          }
        ],
        generationConfig: {
          temperature: 0.7,
          maxOutputTokens: 1024
        }
      })
    });

    const data = await res.json().catch(() => ({}));
    if (!res.ok) {
      throw new Error(data?.error?.message || `Gemini API HTTP ${res.status}`);
    }

    const parts = data?.candidates?.[0]?.content?.parts;
    if (!Array.isArray(parts)) return '';
    return parts
      .map((p) => (typeof p?.text === 'string' ? p.text : ''))
      .join('')
      .trim();
  }

  close() {
    this.closed = true;
    this.unsubscribe?.();
    for (const timer of this.timers.values()) {
      clearTimeout(timer);
    }
    this.timers.clear();
  }
}
