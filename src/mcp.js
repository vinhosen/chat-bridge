import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { z } from 'zod';

function jsonResult(payload, isError = false) {
  return {
    ...(isError ? { isError: true } : {}),
    content: [
      {
        type: 'text',
        text: JSON.stringify(payload, null, 2)
      }
    ]
  };
}

/**
 * Creates an McpServer bound to the given ChatBridge instance.
 * A dedicated McpServer instance is created per SSE session because
 * McpServer binds to one Transport instance at a time.
 *
 * @param {import('./bridge.js').ChatBridge} bridge
 */
export function createMcpServer(bridge) {
  const server = new McpServer({
    name: 'chat-bridge',
    version: '1.0.0'
  });

  server.registerTool(
    'get_messages',
    {
      title: 'Get Messages',
      description:
        'Retrieve chat messages and current channel state (including AI turn count and Loop Guard status) from the bridge.',
      inputSchema: {
        channel_id: z
          .string()
          .optional()
          .default('default')
          .describe('Channel ID to read from (defaults to "default").'),
        since_id: z
          .number()
          .int()
          .min(0)
          .optional()
          .default(0)
          .describe('Return only messages with id greater than since_id.'),
        limit: z
          .number()
          .int()
          .min(1)
          .max(1000)
          .optional()
          .default(200)
          .describe('Maximum number of messages to return.')
      }
    },
    async ({ channel_id, since_id, limit }) => {
      const data = bridge.getMessages({ channel_id, since_id, limit });
      return jsonResult({
        ok: true,
        channel_id: data.channel_id,
        state: data.state,
        ai_turn_count: data.ai_turn_count,
        max_ai_turns: data.max_ai_turns,
        count: data.count,
        messages: data.messages
      });
    }
  );

  server.registerTool(
    'send_message',
    {
      title: 'Send Message',
      description:
        'Send a message into a bridge channel. Enforces Loop Guard (halts AI after 10 consecutive AI turns until Human replies or resets) and PAUSED checks.',
      inputSchema: {
        channel_id: z
          .string()
          .optional()
          .default('default')
          .describe('Channel ID to post into (defaults to "default").'),
        sender: z
          .string()
          .optional()
          .default('gemini')
          .describe('Sender identity (e.g. "gemini", "antigravity", "human").'),
        target: z
          .string()
          .optional()
          .default('all')
          .describe('Target recipient (e.g. "all", "antigravity", "human", "gemini").'),
        content: z.string().min(1).describe('Message body text.'),
        status: z
          .string()
          .optional()
          .default('INFO')
          .describe('Message status tag (e.g. "INFO", "WORKING", "QUESTION", "DONE", "BLOCKED").')
      }
    },
    async ({ channel_id, sender, target, content, status }) => {
      const res = bridge.sendMessage({
        channel_id,
        sender,
        target,
        content,
        status
      });

      if (!res.ok) {
        return jsonResult(
          {
            ok: false,
            error: res.error,
            code: res.code,
            channel: res.channel
          },
          true
        );
      }

      return jsonResult({
        ok: true,
        message: res.message,
        channel: res.channel
      });
    }
  );

  server.registerTool(
    'wait_for_reply',
    {
      title: 'Wait For Reply',
      description:
        'Long-poll until a new message arrives in the channel from another sender (different from caller sender) or until timeout.',
      inputSchema: {
        channel_id: z
          .string()
          .optional()
          .default('default')
          .describe('Channel ID to watch (defaults to "default").'),
        sender: z
          .string()
          .optional()
          .default('gemini')
          .describe('Current caller sender identity; replies from other senders will resolve this call.'),
        since_id: z
          .number()
          .int()
          .min(0)
          .optional()
          .describe(
            'Optional message id cursor. When provided, returns immediately if messages with id > since_id from other senders already exist.'
          ),
        timeout: z
          .number()
          .min(0.05)
          .max(300)
          .optional()
          .default(60)
          .describe('Timeout in seconds before returning empty result (default 60).')
      }
    },
    async ({ channel_id, sender, since_id, timeout }, extra) => {
      const result = await bridge.waitForReply({
        channel_id,
        sender,
        since_id,
        timeout,
        signal: extra?.signal
      });
      return jsonResult(result);
    }
  );

  server.registerTool(
    'set_task_state',
    {
      title: 'Set Task State',
      description:
        'Update the task state of a bridge channel (e.g. ACTIVE, IDLE, WORKING, WAITING_HUMAN, DONE, PAUSED, HALTED).',
      inputSchema: {
        channel_id: z
          .string()
          .optional()
          .default('default')
          .describe('Channel ID to update (defaults to "default").'),
        state: z
          .string()
          .min(1)
          .describe('Target state name (e.g. "ACTIVE", "WORKING", "DONE", "PAUSED", "HALTED").')
      }
    },
    async ({ channel_id, state }) => {
      const res = bridge.setTaskState({ channel_id, state });
      if (!res.ok) {
        return jsonResult(
          {
            ok: false,
            error: res.error,
            code: res.code
          },
          true
        );
      }
      return jsonResult(res);
    }
  );

  return server;
}
