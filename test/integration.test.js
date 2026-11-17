import assert from 'node:assert/strict';
import { execFile, spawn } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { after, before, describe, test } from 'node:test';
import { promisify } from 'node:util';
import { WebSocket } from 'ws';
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { SSEClientTransport } from '@modelcontextprotocol/sdk/client/sse.js';
import { createBridgeServer } from '../src/server.js';

const execFileAsync = promisify(execFile);
const CLI_PATH = path.resolve(import.meta.dirname, '../cli.js');

async function jsonFetch(url, options = {}) {
  const res = await fetch(url, {
    ...options,
    headers: {
      'content-type': 'application/json',
      ...(options.headers || {})
    }
  });
  const data = await res.json();
  return { status: res.status, ok: res.ok, data };
}

function nextWsMessage(ws, predicate, timeoutMs = 3000) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      ws.off('message', onMessage);
      reject(new Error('Timed out waiting for WebSocket event'));
    }, timeoutMs);

    function onMessage(raw) {
      let parsed;
      try {
        parsed = JSON.parse(String(raw));
      } catch {
        return;
      }
      if (!predicate || predicate(parsed)) {
        clearTimeout(timer);
        ws.off('message', onMessage);
        resolve(parsed);
      }
    }

    ws.on('message', onMessage);
  });
}

describe('chat-bridge integration suite', () => {
  let tmpDir;
  let dbPath;
  let bridgeServer;
  let baseUrl;
  let port;

  before(async () => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'chat-bridge-test-'));
    dbPath = path.join(tmpDir, 'nested', 'data', 'bridge.db');
    bridgeServer = createBridgeServer({ dbPath });
    const addr = await bridgeServer.start(0, '127.0.0.1');
    port = addr.port;
    baseUrl = `http://127.0.0.1:${port}`;
  });

  after(async () => {
    await bridgeServer.close();
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  test('auto-creates DB directory, serves /health and Web Chat UI', async () => {
    assert.equal(fs.existsSync(dbPath), true);

    const health = await jsonFetch(`${baseUrl}/health`);
    assert.equal(health.status, 200);
    assert.equal(health.data.ok, true);
    assert.equal(health.data.status, 'ok');
    assert.equal(health.data.db, 'connected');

    const uiRes = await fetch(`${baseUrl}/`);
    assert.equal(uiRes.status, 200);
    const html = await uiRes.text();
    assert.match(html, /Chat Bridge/);
    assert.match(html, /Gemini/);
    assert.match(html, /Antigravity/);
    assert.match(html, /Human/);
    assert.match(html, /\[PAUSE\]/);
    assert.match(html, /\[RESET\]/);
  });

  test('REST API: send, messages, since_id, wait long-polling, state, pause, reset', async () => {
    const channel = 'rest-room';

    const invalid = await jsonFetch(`${baseUrl}/api/send`, {
      method: 'POST',
      body: JSON.stringify({ channel_id: channel, sender: 'human', content: '   ' })
    });
    assert.equal(invalid.status, 400);
    assert.equal(invalid.data.ok, false);

    const m1 = await jsonFetch(`${baseUrl}/api/send`, {
      method: 'POST',
      body: JSON.stringify({
        channel_id: channel,
        sender: 'human',
        target: 'all',
        content: 'Initial task prompt',
        status: 'INSTRUCTION'
      })
    });
    assert.equal(m1.status, 201);
    assert.equal(m1.data.ok, true);
    assert.equal(m1.data.channel.ai_turn_count, 0);

    const m2 = await jsonFetch(`${baseUrl}/api/send`, {
      method: 'POST',
      body: JSON.stringify({
        channel_id: channel,
        sender: 'antigravity',
        target: 'gemini',
        content: 'Starting analysis',
        status: 'WORKING'
      })
    });
    assert.equal(m2.status, 201);
    assert.equal(m2.data.channel.ai_turn_count, 1);

    const allMsgs = await jsonFetch(`${baseUrl}/api/messages?channel_id=${channel}&since_id=0`);
    assert.equal(allMsgs.status, 200);
    assert.equal(allMsgs.data.messages.length, 2);

    const deltaMsgs = await jsonFetch(
      `${baseUrl}/api/messages?channel_id=${channel}&since_id=${m1.data.message.id}`
    );
    assert.equal(deltaMsgs.data.messages.length, 1);
    assert.equal(deltaMsgs.data.messages[0].id, m2.data.message.id);

    // Long-poll wait timeout case
    const timedOutWait = await jsonFetch(
      `${baseUrl}/api/wait?channel_id=${channel}&sender=antigravity&timeout=0.15`
    );
    assert.equal(timedOutWait.status, 200);
    assert.equal(timedOutWait.data.timed_out, true);
    assert.equal(timedOutWait.data.messages.length, 0);

    // Long-poll wait ignores own messages and resolves on another sender
    const waitPromise = jsonFetch(
      `${baseUrl}/api/wait?channel_id=${channel}&sender=antigravity&timeout=5`
    );
    await new Promise((r) => setTimeout(r, 40));

    // Same sender should NOT wake antigravity's wait
    await jsonFetch(`${baseUrl}/api/send`, {
      method: 'POST',
      body: JSON.stringify({
        channel_id: channel,
        sender: 'antigravity',
        content: 'Self update',
        status: 'INFO'
      })
    });

    // Different sender SHOULD wake antigravity's wait
    await jsonFetch(`${baseUrl}/api/send`, {
      method: 'POST',
      body: JSON.stringify({
        channel_id: channel,
        sender: 'gemini',
        target: 'antigravity',
        content: 'Here is my reply',
        status: 'DONE'
      })
    });

    const waitRes = await waitPromise;
    assert.equal(waitRes.status, 200);
    assert.equal(waitRes.data.timed_out, false);
    assert.equal(waitRes.data.messages.length, 1);
    assert.equal(waitRes.data.messages[0].sender, 'gemini');
    assert.equal(waitRes.data.messages[0].content, 'Here is my reply');

    // State + Pause + Reset
    const stateRes = await jsonFetch(`${baseUrl}/api/state`, {
      method: 'POST',
      body: JSON.stringify({ channel_id: channel, state: 'WORKING' })
    });
    assert.equal(stateRes.status, 200);
    assert.equal(stateRes.data.channel.state, 'WORKING');

    const pauseRes = await jsonFetch(`${baseUrl}/api/pause`, {
      method: 'POST',
      body: JSON.stringify({ channel_id: channel })
    });
    assert.equal(pauseRes.status, 200);
    assert.equal(pauseRes.data.channel.state, 'PAUSED');

    const blockedAi = await jsonFetch(`${baseUrl}/api/send`, {
      method: 'POST',
      body: JSON.stringify({
        channel_id: channel,
        sender: 'gemini',
        content: 'Should be blocked while paused'
      })
    });
    assert.equal(blockedAi.status, 423);
    assert.equal(blockedAi.data.code, 'CHANNEL_PAUSED');

    const resetRes = await jsonFetch(`${baseUrl}/api/reset`, {
      method: 'POST',
      body: JSON.stringify({ channel_id: channel })
    });
    assert.equal(resetRes.status, 200);
    assert.equal(resetRes.data.channel.state, 'ACTIVE');
    assert.equal(resetRes.data.channel.ai_turn_count, 0);
  });

  test('Loop Guard: halts after >10 consecutive AI turns and recovers on Human reply or reset', async () => {
    const channel = 'loop-guard-room';

    for (let i = 1; i <= 10; i++) {
      const sender = i % 2 === 1 ? 'gemini' : 'antigravity';
      const res = await jsonFetch(`${baseUrl}/api/send`, {
        method: 'POST',
        body: JSON.stringify({
          channel_id: channel,
          sender,
          content: `AI turn ${i}`,
          status: 'WORKING'
        })
      });
      assert.equal(res.status, 201, `Turn ${i} should succeed`);
      assert.equal(res.data.channel.ai_turn_count, i);
      assert.equal(res.data.channel.state, 'ACTIVE');
    }

    // 11th AI turn exceeds 10 -> triggers HALTED
    const turn11 = await jsonFetch(`${baseUrl}/api/send`, {
      method: 'POST',
      body: JSON.stringify({
        channel_id: channel,
        sender: 'gemini',
        content: 'AI turn 11 should trip Loop Guard'
      })
    });
    assert.equal(turn11.status, 429);
    assert.equal(turn11.data.ok, false);
    assert.equal(turn11.data.code, 'LOOP_GUARD_HALTED');
    assert.equal(turn11.data.channel.state, 'HALTED');

    // Subsequent AI send stays blocked while HALTED
    const turn12 = await jsonFetch(`${baseUrl}/api/send`, {
      method: 'POST',
      body: JSON.stringify({
        channel_id: channel,
        sender: 'antigravity',
        content: 'Still halted'
      })
    });
    assert.equal(turn12.status, 423);
    assert.equal(turn12.data.channel.state, 'HALTED');

    // Human reply clears HALTED and resets AI turn counter to 0
    const humanReply = await jsonFetch(`${baseUrl}/api/send`, {
      method: 'POST',
      body: JSON.stringify({
        channel_id: channel,
        sender: 'Human',
        content: 'Continue please'
      })
    });
    assert.equal(humanReply.status, 201);
    assert.equal(humanReply.data.channel.state, 'ACTIVE');
    assert.equal(humanReply.data.channel.ai_turn_count, 0);

    // AI can send again after Human reply
    const postHumanAi = await jsonFetch(`${baseUrl}/api/send`, {
      method: 'POST',
      body: JSON.stringify({
        channel_id: channel,
        sender: 'gemini',
        content: 'Resumed after Human reply'
      })
    });
    assert.equal(postHumanAi.status, 201);
    assert.equal(postHumanAi.data.channel.ai_turn_count, 1);

    // Trip Loop Guard a second time and verify /api/reset recovery
    for (let i = 2; i <= 10; i++) {
      await jsonFetch(`${baseUrl}/api/send`, {
        method: 'POST',
        body: JSON.stringify({ channel_id: channel, sender: 'antigravity', content: `Turn ${i}` })
      });
    }
    const trippedAgain = await jsonFetch(`${baseUrl}/api/send`, {
      method: 'POST',
      body: JSON.stringify({ channel_id: channel, sender: 'gemini', content: 'Trip again' })
    });
    assert.equal(trippedAgain.status, 429);
    assert.equal(trippedAgain.data.channel.state, 'HALTED');

    const resetRes = await jsonFetch(`${baseUrl}/api/reset`, {
      method: 'POST',
      body: JSON.stringify({ channel_id: channel })
    });
    assert.equal(resetRes.status, 200);
    assert.equal(resetRes.data.channel.state, 'ACTIVE');
    assert.equal(resetRes.data.channel.ai_turn_count, 0);

    const postResetAi = await jsonFetch(`${baseUrl}/api/send`, {
      method: 'POST',
      body: JSON.stringify({ channel_id: channel, sender: 'gemini', content: 'After reset' })
    });
    assert.equal(postResetAi.status, 201);
    assert.equal(postResetAi.data.channel.ai_turn_count, 1);
  });

  test('MCP SSE: /sse + /messages expose and execute all 4 tools', async () => {
    const channel = 'mcp-room';
    const client = new Client({ name: 'test-mcp-client', version: '1.0.0' });
    const transport = new SSEClientTransport(new URL(`${baseUrl}/sse`));

    try {
      await client.connect(transport);

      const listed = await client.listTools();
      const toolNames = listed.tools.map((t) => t.name).sort();
      assert.deepEqual(toolNames, [
        'get_messages',
        'send_message',
        'set_task_state',
        'wait_for_reply'
      ]);

      // set_task_state
      const stateToolRes = await client.callTool({
        name: 'set_task_state',
        arguments: { channel_id: channel, state: 'WORKING' }
      });
      const statePayload = JSON.parse(stateToolRes.content[0].text);
      assert.equal(statePayload.ok, true);
      assert.equal(statePayload.channel.state, 'WORKING');

      // send_message
      const sendToolRes = await client.callTool({
        name: 'send_message',
        arguments: {
          channel_id: channel,
          sender: 'gemini',
          target: 'antigravity',
          content: 'Hello from MCP SSE',
          status: 'QUESTION'
        }
      });
      const sendPayload = JSON.parse(sendToolRes.content[0].text);
      assert.equal(sendPayload.ok, true);
      assert.equal(sendPayload.message.sender, 'gemini');
      assert.equal(sendPayload.message.content, 'Hello from MCP SSE');

      // get_messages
      const getToolRes = await client.callTool({
        name: 'get_messages',
        arguments: { channel_id: channel, since_id: 0 }
      });
      const getPayload = JSON.parse(getToolRes.content[0].text);
      assert.equal(getPayload.count, 1);
      assert.equal(getPayload.state, 'WORKING');

      // wait_for_reply woken by REST POST /api/send
      const waitToolPromise = client.callTool({
        name: 'wait_for_reply',
        arguments: { channel_id: channel, sender: 'gemini', timeout: 5 }
      });
      await new Promise((r) => setTimeout(r, 50));

      await jsonFetch(`${baseUrl}/api/send`, {
        method: 'POST',
        body: JSON.stringify({
          channel_id: channel,
          sender: 'antigravity',
          target: 'gemini',
          content: 'Reply via REST to MCP waiter',
          status: 'INFO'
        })
      });

      const waitToolRes = await waitToolPromise;
      const waitPayload = JSON.parse(waitToolRes.content[0].text);
      assert.equal(waitPayload.timed_out, false);
      assert.equal(waitPayload.messages[0].sender, 'antigravity');
      assert.equal(waitPayload.messages[0].content, 'Reply via REST to MCP waiter');

      // Verify MCP send_message respects PAUSED state with isError: true
      await jsonFetch(`${baseUrl}/api/pause`, {
        method: 'POST',
        body: JSON.stringify({ channel_id: channel })
      });

      const blockedMcp = await client.callTool({
        name: 'send_message',
        arguments: {
          channel_id: channel,
          sender: 'gemini',
          content: 'Should error while paused'
        }
      });
      assert.equal(blockedMcp.isError, true);
      const blockedPayload = JSON.parse(blockedMcp.content[0].text);
      assert.equal(blockedPayload.code, 'CHANNEL_PAUSED');
    } finally {
      await transport.close().catch(() => {});
      await client.close().catch(() => {});
    }
  });

  test('WebSocket: receives init, live message broadcasts, state transitions, and handles incoming send', async () => {
    const channel = 'ws-room';
    const ws = new WebSocket(`ws://127.0.0.1:${port}/?channel_id=${channel}`);

    try {
      const initMsg = await nextWsMessage(ws, (m) => m.type === 'init');
      assert.equal(initMsg.channel_id, channel);
      assert.equal(initMsg.channel.state, 'ACTIVE');
      assert.deepEqual(initMsg.messages, []);

      // Trigger REST send and confirm live WS broadcast
      const msgWaiter = nextWsMessage(ws, (m) => m.type === 'message');
      await jsonFetch(`${baseUrl}/api/send`, {
        method: 'POST',
        body: JSON.stringify({
          channel_id: channel,
          sender: 'gemini',
          content: 'Broadcast over WS',
          status: 'INFO'
        })
      });
      const msgEvent = await msgWaiter;
      assert.equal(msgEvent.message.content, 'Broadcast over WS');
      assert.equal(msgEvent.channel.ai_turn_count, 1);

      // Pause & Reset broadcast state events
      const pauseWaiter = nextWsMessage(ws, (m) => m.type === 'state' && m.channel.state === 'PAUSED');
      await jsonFetch(`${baseUrl}/api/pause`, {
        method: 'POST',
        body: JSON.stringify({ channel_id: channel })
      });
      await pauseWaiter;

      const resetWaiter = nextWsMessage(ws, (m) => m.type === 'state' && m.channel.state === 'ACTIVE');
      await jsonFetch(`${baseUrl}/api/reset`, {
        method: 'POST',
        body: JSON.stringify({ channel_id: channel })
      });
      const resetEvent = await resetWaiter;
      assert.equal(resetEvent.channel.ai_turn_count, 0);

      // Send over WebSocket directly as Human
      const wsSendWaiter = nextWsMessage(
        ws,
        (m) => m.type === 'message' && m.message?.content === 'Sent from WebSocket client'
      );
      ws.send(
        JSON.stringify({
          type: 'send',
          sender: 'human',
          target: 'all',
          content: 'Sent from WebSocket client',
          status: 'INSTRUCTION'
        })
      );
      const wsSent = await wsSendWaiter;
      assert.equal(wsSent.message.sender, 'human');
    } finally {
      ws.close();
    }
  });

  test('cli.js: supports send, wait, and exits non-zero when blocked', async () => {
    const channel = 'cli-room';
    const env = {
      ...process.env,
      BRIDGE_URL: baseUrl,
      BRIDGE_CHANNEL: channel
    };

    const sendRes = await execFileAsync(
      process.execPath,
      [CLI_PATH, 'send', 'Hello from CLI helper', 'WORKING'],
      { env }
    );
    const parsedSend = JSON.parse(sendRes.stdout);
    assert.equal(parsedSend.ok, true);
    assert.equal(parsedSend.message.content, 'Hello from CLI helper');
    assert.equal(parsedSend.message.status, 'WORKING');

    // Run `node cli.js wait` in child process and wake it via REST send from gemini
    const waitChild = spawn(process.execPath, [CLI_PATH, 'wait', '--timeout', '5'], {
      env,
      stdio: ['ignore', 'pipe', 'pipe']
    });

    let stdout = '';
    let stderr = '';
    waitChild.stdout.on('data', (chunk) => {
      stdout += String(chunk);
    });
    waitChild.stderr.on('data', (chunk) => {
      stderr += String(chunk);
    });

    await new Promise((r) => setTimeout(r, 150));

    await jsonFetch(`${baseUrl}/api/send`, {
      method: 'POST',
      body: JSON.stringify({
        channel_id: channel,
        sender: 'gemini',
        target: 'antigravity',
        content: 'Wake up CLI waiter',
        status: 'DONE'
      })
    });

    const exitCode = await new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        waitChild.kill();
        reject(new Error('cli.js wait timed out'));
      }, 5000);
      waitChild.on('close', (code) => {
        clearTimeout(timer);
        resolve(code);
      });
    });

    assert.equal(exitCode, 0, `cli.js wait failed with stderr: ${stderr}`);
    const parsedWait = JSON.parse(stdout);
    assert.equal(parsedWait.timed_out, false);
    assert.equal(parsedWait.messages[0].content, 'Wake up CLI waiter');

    // Verify cli.js send fails cleanly when channel is PAUSED
    await jsonFetch(`${baseUrl}/api/pause`, {
      method: 'POST',
      body: JSON.stringify({ channel_id: channel })
    });

    await assert.rejects(
      () =>
        execFileAsync(process.execPath, [CLI_PATH, 'send', 'Should fail', 'INFO'], {
          env
        }),
      (err) => err.code === 1 && err.stderr.includes('CHANNEL_PAUSED')
    );
  });
});
