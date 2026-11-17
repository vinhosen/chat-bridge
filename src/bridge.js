import fs from 'node:fs';
import path from 'node:path';
import { DatabaseSync } from 'node:sqlite';

const DEFAULT_CHANNEL = 'default';
const DEFAULT_MAX_AI_TURNS = 10;

export function resolveDbPath(customPath) {
  const raw =
    customPath ||
    process.env.DB_PATH ||
    process.env.BRIDGE_DB_PATH ||
    './data/bridge.db';
  if (raw === ':memory:') {
    return ':memory:';
  }
  return path.resolve(raw);
}

export function normalizeChannelId(channelId) {
  const clean = String(channelId ?? DEFAULT_CHANNEL).trim();
  return clean || DEFAULT_CHANNEL;
}

export function normalizeSender(sender, fallback = 'human') {
  const clean = String(sender ?? fallback).trim();
  return clean || fallback;
}

export function isHumanSender(sender) {
  return normalizeSender(sender).toLowerCase() === 'human';
}

export function isSystemSender(sender) {
  return normalizeSender(sender).toLowerCase() === 'system';
}

export class ChatBridge {
  /**
   * @param {object} [options]
   * @param {string} [options.dbPath]
   * @param {number} [options.maxAiTurns]
   */
  constructor(options = {}) {
    this.dbPath = resolveDbPath(options.dbPath);
    const configuredMax = Number(
      options.maxAiTurns ?? process.env.MAX_AI_TURNS ?? DEFAULT_MAX_AI_TURNS
    );
    this.maxAiTurns =
      Number.isFinite(configuredMax) && configuredMax > 0
        ? Math.floor(configuredMax)
        : DEFAULT_MAX_AI_TURNS;

    if (this.dbPath !== ':memory:') {
      fs.mkdirSync(path.dirname(this.dbPath), { recursive: true });
    }

    this.db = new DatabaseSync(this.dbPath);
    this.db.exec('PRAGMA busy_timeout = 5000;');
    if (this.dbPath !== ':memory:') {
      this.db.exec('PRAGMA journal_mode = WAL;');
    }

    this.#initSchema();

    /** @type {Set<(event: { type: string, channel_id: string, message?: object, channel: object, reason?: string }) => void>} */
    this.listeners = new Set();

    /** @type {Set<{ channelId: string, senderLower: string, cursor: number, resolve: Function, timer: NodeJS.Timeout, cleanup: Function }>} */
    this.waiters = new Set();

    this.closed = false;
  }

  #initSchema() {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS channels (
        channel_id TEXT PRIMARY KEY,
        state TEXT NOT NULL DEFAULT 'ACTIVE',
        ai_turn_count INTEGER NOT NULL DEFAULT 0,
        max_ai_turns INTEGER NOT NULL DEFAULT 10,
        updated_at TEXT NOT NULL
      );

      CREATE TABLE IF NOT EXISTS messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        channel_id TEXT NOT NULL,
        sender TEXT NOT NULL,
        target TEXT NOT NULL DEFAULT 'all',
        content TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'INFO',
        created_at TEXT NOT NULL
      );

      CREATE INDEX IF NOT EXISTS idx_messages_channel_id
        ON messages (channel_id, id);
    `);
  }

  /**
   * Subscribe to bridge events (messages & state transitions).
   * @param {(event: any) => void} fn
   * @returns {() => void} unsubscribe
   */
  subscribe(fn) {
    this.listeners.add(fn);
    return () => {
      this.listeners.delete(fn);
    };
  }

  #emit(event) {
    for (const fn of this.listeners) {
      try {
        fn(event);
      } catch {
        // Ignore subscriber errors so one broken client never breaks the bridge
      }
    }
  }

  ensureChannel(channelId = DEFAULT_CHANNEL) {
    const cid = normalizeChannelId(channelId);
    const now = new Date().toISOString();
    this.db
      .prepare(
        `INSERT OR IGNORE INTO channels (channel_id, state, ai_turn_count, max_ai_turns, updated_at)
         VALUES (?, 'ACTIVE', 0, ?, ?)`
      )
      .run(cid, this.maxAiTurns, now);

    return this.db
      .prepare(
        `SELECT channel_id, state, ai_turn_count, max_ai_turns, updated_at
         FROM channels
         WHERE channel_id = ?`
      )
      .get(cid);
  }

  getChannelState(channelId = DEFAULT_CHANNEL) {
    return this.ensureChannel(channelId);
  }

  listChannels() {
    this.ensureChannel(DEFAULT_CHANNEL);
    return this.db
      .prepare(
        `SELECT channel_id, state, ai_turn_count, max_ai_turns, updated_at
         FROM channels
         ORDER BY channel_id ASC`
      )
      .all();
  }

  getMaxMessageId(channelId = DEFAULT_CHANNEL) {
    const cid = normalizeChannelId(channelId);
    const row = this.db
      .prepare(`SELECT COALESCE(MAX(id), 0) AS max_id FROM messages WHERE channel_id = ?`)
      .get(cid);
    return Number(row?.max_id ?? 0);
  }

  getMessages({ channel_id = DEFAULT_CHANNEL, since_id = 0, limit = 200 } = {}) {
    const cid = normalizeChannelId(channel_id);
    const channel = this.ensureChannel(cid);
    const parsedSince = Number(since_id);
    const since = Number.isFinite(parsedSince) && parsedSince >= 0 ? Math.floor(parsedSince) : 0;
    const parsedLimit = Number(limit);
    const cappedLimit =
      Number.isFinite(parsedLimit) && parsedLimit > 0
        ? Math.min(1000, Math.floor(parsedLimit))
        : 200;

    const messages = this.db
      .prepare(
        `SELECT id, channel_id, sender, target, content, status, created_at
         FROM messages
         WHERE channel_id = ? AND id > ?
         ORDER BY id ASC
         LIMIT ?`
      )
      .all(cid, since, cappedLimit);

    return {
      channel_id: cid,
      state: channel.state,
      ai_turn_count: channel.ai_turn_count,
      max_ai_turns: channel.max_ai_turns,
      channel,
      messages,
      count: messages.length
    };
  }

  sendMessage({
    channel_id = DEFAULT_CHANNEL,
    sender = 'human',
    target = 'all',
    content,
    status = 'INFO'
  }) {
    if (typeof content !== 'string' || content.trim().length === 0) {
      return {
        ok: false,
        blocked: false,
        statusCode: 400,
        code: 'INVALID_CONTENT',
        error: 'Message content must be a non-empty string.'
      };
    }

    const cid = normalizeChannelId(channel_id);
    const cleanSender = normalizeSender(sender, 'human');
    const cleanTarget = String(target ?? 'all').trim() || 'all';
    const cleanStatus = String(status ?? 'INFO').trim() || 'INFO';
    const now = new Date().toISOString();
    const human = isHumanSender(cleanSender);
    const system = isSystemSender(cleanSender);

    let result;
    let stateChangedReason = null;

    this.db.exec('BEGIN IMMEDIATE');
    try {
      const channel = this.ensureChannel(cid);
      const maxTurns = channel.max_ai_turns || this.maxAiTurns;

      if (!human && !system) {
        if (channel.state === 'PAUSED') {
          this.db.exec('ROLLBACK');
          return {
            ok: false,
            blocked: true,
            statusCode: 423,
            code: 'CHANNEL_PAUSED',
            error:
              'Channel is PAUSED. Human must resume or press reset before AI can send messages.',
            channel
          };
        }

        if (channel.state === 'HALTED') {
          this.db.exec('ROLLBACK');
          return {
            ok: false,
            blocked: true,
            statusCode: 423,
            code: 'LOOP_GUARD_HALTED',
            error:
              'Channel is HALTED by Loop Guard. Human must reply or press reset before AI can send messages.',
            channel
          };
        }

        if (channel.ai_turn_count >= maxTurns) {
          this.db
            .prepare(
              `UPDATE channels SET state = 'HALTED', updated_at = ? WHERE channel_id = ?`
            )
            .run(now, cid);
          const haltedChannel = this.ensureChannel(cid);
          this.db.exec('COMMIT');

          const reason = `Loop Guard triggered: consecutive AI turns exceeded ${maxTurns}.`;
          this.#emit({
            type: 'state',
            channel_id: cid,
            channel: haltedChannel,
            reason
          });

          return {
            ok: false,
            blocked: true,
            statusCode: 429,
            code: 'LOOP_GUARD_HALTED',
            error: `${reason} State changed to HALTED until Human replies or presses reset.`,
            channel: haltedChannel
          };
        }

        this.db
          .prepare(
            `UPDATE channels SET ai_turn_count = ai_turn_count + 1, updated_at = ? WHERE channel_id = ?`
          )
          .run(now, cid);
      } else if (human) {
        const nextState = channel.state === 'HALTED' ? 'ACTIVE' : channel.state;
        if (channel.state === 'HALTED') {
          stateChangedReason = 'Human replied; Loop Guard cleared.';
        }
        this.db
          .prepare(
            `UPDATE channels SET ai_turn_count = 0, state = ?, updated_at = ? WHERE channel_id = ?`
          )
          .run(nextState, now, cid);
      }

      const insertInfo = this.db
        .prepare(
          `INSERT INTO messages (channel_id, sender, target, content, status, created_at)
           VALUES (?, ?, ?, ?, ?, ?)`
        )
        .run(cid, cleanSender, cleanTarget, content, cleanStatus, now);

      const message = this.db
        .prepare(
          `SELECT id, channel_id, sender, target, content, status, created_at
           FROM messages
           WHERE id = ?`
        )
        .get(Number(insertInfo.lastInsertRowid));

      const updatedChannel = this.ensureChannel(cid);
      this.db.exec('COMMIT');

      result = {
        ok: true,
        message,
        channel: updatedChannel
      };
    } catch (error) {
      try {
        this.db.exec('ROLLBACK');
      } catch {
        // Ignore rollback failure if no active transaction
      }
      throw error;
    }

    this.#notifyWaiters(result.message, result.channel);
    this.#emit({
      type: 'message',
      channel_id: cid,
      message: result.message,
      channel: result.channel
    });
    if (stateChangedReason) {
      this.#emit({
        type: 'state',
        channel_id: cid,
        channel: result.channel,
        reason: stateChangedReason
      });
    }

    return result;
  }

  setTaskState({ channel_id = DEFAULT_CHANNEL, state, reason } = {}) {
    if (typeof state !== 'string' || state.trim().length === 0) {
      return {
        ok: false,
        statusCode: 400,
        code: 'INVALID_STATE',
        error: 'State must be a non-empty string.'
      };
    }

    const cid = normalizeChannelId(channel_id);
    const cleanState = state.trim().toUpperCase();
    const now = new Date().toISOString();

    const current = this.ensureChannel(cid);
    const shouldResetTurns =
      current.state === 'HALTED' && (cleanState === 'ACTIVE' || cleanState === 'IDLE');

    if (shouldResetTurns) {
      this.db
        .prepare(
          `UPDATE channels SET state = ?, ai_turn_count = 0, updated_at = ? WHERE channel_id = ?`
        )
        .run(cleanState, now, cid);
    } else {
      this.db
        .prepare(`UPDATE channels SET state = ?, updated_at = ? WHERE channel_id = ?`)
        .run(cleanState, now, cid);
    }

    const updatedChannel = this.ensureChannel(cid);
    this.#emit({
      type: 'state',
      channel_id: cid,
      channel: updatedChannel,
      reason: reason || `State updated to ${cleanState}`
    });

    return {
      ok: true,
      channel: updatedChannel
    };
  }

  pauseChannel(channel_id = DEFAULT_CHANNEL) {
    return this.setTaskState({
      channel_id,
      state: 'PAUSED',
      reason: 'Paused by operator'
    });
  }

  resetChannel(channel_id = DEFAULT_CHANNEL) {
    const cid = normalizeChannelId(channel_id);
    const now = new Date().toISOString();
    this.ensureChannel(cid);
    this.db
      .prepare(
        `UPDATE channels SET state = 'ACTIVE', ai_turn_count = 0, updated_at = ? WHERE channel_id = ?`
      )
      .run(now, cid);

    const updatedChannel = this.ensureChannel(cid);
    this.#emit({
      type: 'state',
      channel_id: cid,
      channel: updatedChannel,
      reason: 'Reset by operator'
    });

    return {
      ok: true,
      channel: updatedChannel
    };
  }

  #notifyWaiters(message, channel) {
    const msgSenderLower = normalizeSender(message.sender).toLowerCase();
    for (const waiter of Array.from(this.waiters)) {
      if (
        waiter.channelId === message.channel_id &&
        message.id > waiter.cursor &&
        msgSenderLower !== waiter.senderLower
      ) {
        waiter.cleanup();
        waiter.resolve({
          ok: true,
          timed_out: false,
          halted: channel.state === 'HALTED',
          paused: channel.state === 'PAUSED',
          state: channel.state,
          channel_id: message.channel_id,
          channel,
          messages: [message]
        });
      }
    }
  }

  /**
   * Long-poll for a new message in `channel_id` from any sender other than `sender`.
   * @param {object} params
   * @param {string} [params.channel_id]
   * @param {string} [params.sender]
   * @param {number|string} [params.since_id]
   * @param {number|string} [params.timeout] timeout in seconds (default 60)
   * @param {AbortSignal} [params.signal]
   */
  waitForReply({
    channel_id = DEFAULT_CHANNEL,
    sender = 'antigravity',
    since_id,
    timeout = 60,
    signal
  } = {}) {
    const cid = normalizeChannelId(channel_id);
    const waiterSender = normalizeSender(sender, 'antigravity');
    const senderLower = waiterSender.toLowerCase();
    const channel = this.ensureChannel(cid);

    const parsedTimeout = Number(timeout);
    const timeoutSec =
      Number.isFinite(parsedTimeout) && parsedTimeout > 0
        ? Math.min(300, Math.max(0.05, parsedTimeout))
        : 60;

    const hasExplicitSinceId =
      since_id !== undefined && since_id !== null && since_id !== '';
    let cursor;

    if (hasExplicitSinceId) {
      const parsedSince = Number(since_id);
      cursor = Number.isFinite(parsedSince) && parsedSince >= 0 ? Math.floor(parsedSince) : 0;
      const existing = this.db
        .prepare(
          `SELECT id, channel_id, sender, target, content, status, created_at
           FROM messages
           WHERE channel_id = ? AND id > ? AND LOWER(sender) != ?
           ORDER BY id ASC`
        )
        .all(cid, cursor, senderLower);

      if (existing.length > 0) {
        return Promise.resolve({
          ok: true,
          timed_out: false,
          halted: channel.state === 'HALTED',
          paused: channel.state === 'PAUSED',
          state: channel.state,
          channel_id: cid,
          channel,
          messages: existing
        });
      }
    } else {
      cursor = this.getMaxMessageId(cid);
    }

    if (this.closed || signal?.aborted) {
      return Promise.resolve({
        ok: true,
        timed_out: false,
        aborted: true,
        halted: channel.state === 'HALTED',
        paused: channel.state === 'PAUSED',
        state: channel.state,
        channel_id: cid,
        channel,
        messages: []
      });
    }

    return new Promise((resolve) => {
      let settled = false;
      const waiter = {
        channelId: cid,
        senderLower,
        cursor,
        resolve: (payload) => {
          if (settled) return;
          settled = true;
          resolve(payload);
        },
        timer: null,
        cleanup: null
      };

      const onAbort = () => {
        waiter.cleanup();
        const latestChannel = this.closed ? channel : this.ensureChannel(cid);
        waiter.resolve({
          ok: true,
          timed_out: false,
          aborted: true,
          halted: latestChannel.state === 'HALTED',
          paused: latestChannel.state === 'PAUSED',
          state: latestChannel.state,
          channel_id: cid,
          channel: latestChannel,
          messages: []
        });
      };

      waiter.cleanup = () => {
        if (waiter.timer) {
          clearTimeout(waiter.timer);
          waiter.timer = null;
        }
        this.waiters.delete(waiter);
        if (signal) {
          signal.removeEventListener('abort', onAbort);
        }
      };

      waiter.timer = setTimeout(() => {
        waiter.cleanup();
        const latestChannel = this.closed ? channel : this.ensureChannel(cid);
        waiter.resolve({
          ok: true,
          timed_out: true,
          halted: latestChannel.state === 'HALTED',
          paused: latestChannel.state === 'PAUSED',
          state: latestChannel.state,
          channel_id: cid,
          channel: latestChannel,
          messages: []
        });
      }, Math.round(timeoutSec * 1000));

      if (signal) {
        signal.addEventListener('abort', onAbort, { once: true });
      }

      this.waiters.add(waiter);
    });
  }

  healthCheck() {
    this.db.prepare('SELECT 1 AS ok').get();
    return {
      ok: true,
      db: 'connected',
      channels: this.listChannels()
    };
  }

  close() {
    this.closed = true;
    for (const waiter of Array.from(this.waiters)) {
      waiter.cleanup();
      waiter.resolve({
        ok: true,
        timed_out: false,
        aborted: true,
        halted: false,
        paused: false,
        state: 'CLOSED',
        channel_id: waiter.channelId,
        messages: []
      });
    }
    this.listeners.clear();
    try {
      this.db.close();
    } catch {
      // Ignore if already closed
    }
  }
}
