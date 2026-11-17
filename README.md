# chat-bridge

Dual-Mode Chat Bridge (**MCP SSE + REST API + WebSocket + SQLite**) built with Node.js ESM (`>=22`, `node:sqlite`), ready to deploy on Render or Railway.

## Features

- **MCP over SSE (`/sse`, `/messages`)**: Exposes 4 MCP tools (`get_messages`, `send_message`, `wait_for_reply`, `set_task_state`) mounted before `express.json()`.
- **REST API (`/api/*`)**: Send messages, fetch message history, long-poll for replies from other senders, update task state, pause/reset, and health check.
- **Live WebSocket + Web Chat UI (`public/index.html`)**: Real-time stream with `Gemini`, `Antigravity`, and `Human` badges, task state pill, AI turn counter, Human composer, and `[PAUSE]` / `[RESET]` controls.
- **Built-in SQLite (`node:sqlite`)**: Persists messages and channel state in `DB_PATH` / `BRIDGE_DB_PATH` (defaults to `./data/bridge.db`) with automatic directory creation.
- **Loop Guard**: Counts consecutive AI turns and automatically transitions the channel state to `HALTED` when exceeding 10 AI turns, blocking further AI sends until a Human replies or `/api/reset` is triggered.
- **CLI Helper (`cli.js`)**: Lightweight command-line client reading `BRIDGE_URL` (defaults to `http://localhost:3100`).

## Quick Start

```bash
npm install
npm start
```

Server binds to `0.0.0.0` on `process.env.PORT || 3100`.

Open `http://localhost:3100` for the Web Chat UI.

## REST API

- `POST /api/send` — `{ channel_id, sender, target, content, status }`
- `GET /api/messages?channel_id=default&since_id=0`
- `GET /api/wait?channel_id=default&sender=antigravity&timeout=60`
- `POST /api/state` — `{ channel_id, state }`
- `POST /api/pause` — `{ channel_id }`
- `POST /api/reset` — `{ channel_id }`
- `GET /health`

## CLI Helper

```bash
export BRIDGE_URL=http://localhost:3100
node cli.js send "Hello from Antigravity" "WORKING"
node cli.js wait
```

## MCP SSE Tools

Connect MCP SSE clients to `GET /sse` (which routes JSON-RPC messages to `POST /messages`):

- `get_messages`
- `send_message`
- `wait_for_reply`
- `set_task_state`

## Testing

```bash
npm test
```

## Cloud Deployment (Render / Railway)

Includes [`render.yaml`](./render.yaml) configured with:
- `NODE_VERSION: 24.0.0`
- `DB_PATH: ./data/bridge.db`
- `healthCheckPath: /health`
- `buildCommand: npm install`
- `startCommand: npm start`
