import fs from 'node:fs';
import http from 'node:http';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import cors from 'cors';
import express from 'express';
import { WebSocket, WebSocketServer } from 'ws';
import { SSEServerTransport } from '@modelcontextprotocol/sdk/server/sse.js';
import { ChatBridge, normalizeChannelId } from './bridge.js';
import { AiWorker } from './ai-worker.js';
import { createMcpServer } from './mcp.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const PUBLIC_DIR = path.resolve(__dirname, '../public');

/**
 * Create and configure the Chat Bridge HTTP + Express + MCP SSE + WebSocket server.
 *
 * @param {object} [options]
 * @param {string} [options.dbPath]
 * @param {number} [options.maxAiTurns]
 * @param {number} [options.port]
 * @param {string} [options.host]
 * @param {boolean} [options.enableAiWorker]
 * @param {object} [options.aiWorkerOptions]
 */
export function createBridgeServer(options = {}) {
  const bridge = new ChatBridge({
    dbPath: options.dbPath,
    maxAiTurns: options.maxAiTurns
  });

  const aiWorker =
    options.enableAiWorker === false
      ? null
      : new AiWorker(bridge, options.aiWorkerOptions || {});

  const app = express();
  app.disable('x-powered-by');
  app.set('trust proxy', 1);
  app.use(cors());

  /** @type {Map<string, { transport: SSEServerTransport, mcpServer: import('@modelcontextprotocol/sdk/server/mcp.js').McpServer, keepAlive: NodeJS.Timeout }>} */
  const sseSessions = new Map();

  // Mount MCP SSE endpoints BEFORE express.json() so SSEServerTransport.handlePostMessage
  // can read the raw request body stream without body-parser consuming it first.
  app.get('/sse', async (req, res) => {
    res.setHeader('X-Accel-Buffering', 'no');
    const transport = new SSEServerTransport('/messages', res);
    const mcpServer = createMcpServer(bridge);

    const keepAlive = setInterval(() => {
      if (!res.writableEnded && !res.destroyed) {
        res.write(': ping\n\n');
      }
    }, 25000);
    keepAlive.unref();

    sseSessions.set(transport.sessionId, { transport, mcpServer, keepAlive });

    res.on('close', () => {
      clearInterval(keepAlive);
      sseSessions.delete(transport.sessionId);
      mcpServer.close().catch(() => {});
    });

    await mcpServer.connect(transport);
  });

  app.post('/messages', async (req, res) => {
    const sessionId = String(req.query.sessionId || '');
    const session = sseSessions.get(sessionId);
    if (!session) {
      res.status(404).json({
        ok: false,
        error: `Unknown or expired MCP SSE sessionId: ${sessionId}`
      });
      return;
    }

    try {
      await session.transport.handlePostMessage(req, res);
    } catch (error) {
      if (!res.headersSent) {
        res.status(500).json({
          ok: false,
          error: error instanceof Error ? error.message : String(error)
        });
      }
    }
  });

  // JSON body parser for REST API endpoints
  app.use(express.json({ limit: '2mb' }));

  // Serve Web Chat UI
  app.use(express.static(PUBLIC_DIR));

  // REST API Endpoints
  app.post('/api/send', (req, res) => {
    const {
      channel_id = 'default',
      sender = 'human',
      target = 'all',
      content,
      status = 'INFO'
    } = req.body || {};

    const result = bridge.sendMessage({
      channel_id,
      sender,
      target,
      content,
      status
    });

    if (!result.ok) {
      res.status(result.statusCode || 400).json(result);
      return;
    }

    res.status(201).json(result);
  });

  app.get('/api/messages', (req, res) => {
    const data = bridge.getMessages({
      channel_id: req.query.channel_id,
      since_id: req.query.since_id,
      limit: req.query.limit
    });
    res.json({ ok: true, ...data });
  });

  app.get('/api/wait', async (req, res) => {
    const ac = new AbortController();
    res.on('close', () => {
      if (!res.writableFinished) {
        ac.abort();
      }
    });

    const result = await bridge.waitForReply({
      channel_id: req.query.channel_id,
      sender: req.query.sender,
      since_id: req.query.since_id,
      timeout: req.query.timeout,
      signal: ac.signal
    });

    if (!res.writableEnded && !res.destroyed) {
      res.json(result);
    }
  });

  app.post('/api/state', (req, res) => {
    const { channel_id = 'default', state } = req.body || {};
    const result = bridge.setTaskState({ channel_id, state });
    if (!result.ok) {
      res.status(result.statusCode || 400).json(result);
      return;
    }
    res.json(result);
  });

  app.post('/api/pause', (req, res) => {
    const channel_id = req.body?.channel_id ?? req.query?.channel_id ?? 'default';
    const result = bridge.pauseChannel(channel_id);
    res.json(result);
  });

  app.post('/api/reset', (req, res) => {
    const channel_id = req.body?.channel_id ?? req.query?.channel_id ?? 'default';
    const result = bridge.resetChannel(channel_id);
    res.json(result);
  });

  app.post('/api/auto', (req, res) => {
    const {
      channel_id = 'default',
      auto_gemini,
      auto_antigravity
    } = req.body || {};
    const result = bridge.setAutoMode({
      channel_id,
      auto_gemini,
      auto_antigravity
    });
    res.json({
      ...result,
      ai_worker: aiWorker ? aiWorker.getStatus() : null
    });
  });

  app.get('/health', (req, res) => {
    try {
      const health = bridge.healthCheck();
      res.json({
        ...health,
        status: 'ok',
        service: 'chat-bridge',
        uptime: Math.round(process.uptime() * 100) / 100,
        sse_sessions: sseSessions.size,
        ws_clients: wss.clients.size,
        ai_worker: aiWorker ? aiWorker.getStatus() : null
      });
    } catch (error) {
      res.status(500).json({
        ok: false,
        status: 'error',
        error: error instanceof Error ? error.message : String(error)
      });
    }
  });

  // Error fallback middleware (handles invalid JSON payloads cleanly)
  app.use((err, req, res, next) => {
    if (res.headersSent) {
      next(err);
      return;
    }
    const status = err?.status || err?.statusCode || 500;
    res.status(status).json({
      ok: false,
      error: err?.message || 'Internal Server Error'
    });
  });

  const server = http.createServer(app);
  const wss = new WebSocketServer({ server });

  function sendWsInit(ws, channelId) {
    const cid = normalizeChannelId(channelId);
    ws.channelId = cid;
    const data = bridge.getMessages({ channel_id: cid, since_id: 0, limit: 200 });
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(
        JSON.stringify({
          type: 'init',
          channel_id: data.channel_id,
          channel: data.channel,
          messages: data.messages,
          ai_worker: aiWorker ? aiWorker.getStatus() : null
        })
      );
    }
  }

  wss.on('connection', (ws, req) => {
    let initialChannel = 'default';
    try {
      const reqUrl = new URL(req.url || '/', 'http://localhost');
      initialChannel = reqUrl.searchParams.get('channel_id') || 'default';
    } catch {
      initialChannel = 'default';
    }

    ws.isAlive = true;
    ws.on('pong', () => {
      ws.isAlive = true;
    });

    sendWsInit(ws, initialChannel);

    ws.on('message', (raw) => {
      let msg;
      try {
        msg = JSON.parse(String(raw));
      } catch {
        if (ws.readyState === WebSocket.OPEN) {
          ws.send(JSON.stringify({ type: 'error', error: 'Invalid JSON payload' }));
        }
        return;
      }

      if (!msg || typeof msg !== 'object') return;

      switch (msg.type) {
        case 'ping':
          if (ws.readyState === WebSocket.OPEN) {
            ws.send(JSON.stringify({ type: 'pong' }));
          }
          break;
        case 'subscribe':
          sendWsInit(ws, msg.channel_id || 'default');
          break;
        case 'send': {
          const result = bridge.sendMessage({
            channel_id: msg.channel_id || ws.channelId || 'default',
            sender: msg.sender || 'human',
            target: msg.target || 'all',
            content: msg.content,
            status: msg.status || 'INFO'
          });
          if (!result.ok && ws.readyState === WebSocket.OPEN) {
            ws.send(JSON.stringify({ type: 'error', ...result }));
          }
          break;
        }
        case 'pause':
          bridge.pauseChannel(msg.channel_id || ws.channelId || 'default');
          break;
        case 'reset':
          bridge.resetChannel(msg.channel_id || ws.channelId || 'default');
          break;
        case 'state': {
          const resState = bridge.setTaskState({
            channel_id: msg.channel_id || ws.channelId || 'default',
            state: msg.state
          });
          if (!resState.ok && ws.readyState === WebSocket.OPEN) {
            ws.send(JSON.stringify({ type: 'error', ...resState }));
          }
          break;
        }
        default:
          break;
      }
    });
  });

  const unsubscribeBridge = bridge.subscribe((event) => {
    const payload = JSON.stringify(event);
    for (const client of wss.clients) {
      if (client.readyState === WebSocket.OPEN && client.channelId === event.channel_id) {
        client.send(payload);
      }
    }
  });

  const wsHeartbeat = setInterval(() => {
    for (const client of wss.clients) {
      if (client.isAlive === false) {
        client.terminate();
        continue;
      }
      client.isAlive = false;
      client.ping();
    }
  }, 30000);
  wsHeartbeat.unref();

  let closed = false;

  async function close() {
    if (closed) return;
    closed = true;

    clearInterval(wsHeartbeat);
    unsubscribeBridge();
    aiWorker?.close();

    for (const session of sseSessions.values()) {
      clearInterval(session.keepAlive);
      await session.transport.close().catch(() => {});
      await session.mcpServer.close().catch(() => {});
    }
    sseSessions.clear();

    for (const client of wss.clients) {
      client.terminate();
    }
    await new Promise((resolve) => wss.close(() => resolve()));

    bridge.close();

    if (server.listening) {
      await new Promise((resolve) => server.close(() => resolve()));
    }
  }

  function start(
    port = Number(options.port ?? process.env.PORT ?? 3100),
    host = options.host ?? '0.0.0.0'
  ) {
    return new Promise((resolve, reject) => {
      server.once('error', reject);
      server.listen(port, host, () => {
        server.removeListener('error', reject);
        resolve(server.address());
      });
    });
  }

  return { app, server, wss, bridge, start, close };
}

const isMain = (() => {
  if (!process.argv[1]) return false;
  try {
    return fs.realpathSync(process.argv[1]) === fs.realpathSync(__filename);
  } catch {
    return path.resolve(process.argv[1]) === __filename;
  }
})();

if (isMain) {
  const port = Number(process.env.PORT) || 3100;
  const host = '0.0.0.0';
  const instance = createBridgeServer();

  instance
    .start(port, host)
    .then((addr) => {
      const boundPort = typeof addr === 'object' && addr ? addr.port : port;
      console.log(`chat-bridge listening on http://${host}:${boundPort}`);
      console.log(`  MCP SSE:   GET /sse | POST /messages`);
      console.log(`  REST API:  /api/send, /api/messages, /api/wait, /api/state, /api/pause, /api/reset`);
      console.log(`  WebSocket: ws://${host}:${boundPort}`);
      console.log(`  Health:    GET /health`);
    })
    .catch((err) => {
      console.error('Failed to start chat-bridge:', err);
      process.exit(1);
    });

  const shutdown = async () => {
    await instance.close();
    process.exit(0);
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}
