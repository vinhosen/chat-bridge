#!/usr/bin/env node

const BRIDGE_URL = (process.env.BRIDGE_URL || 'http://localhost:3100').replace(/\/+$/, '');
const DEFAULT_CHANNEL = process.env.BRIDGE_CHANNEL || 'default';
const DEFAULT_SENDER = process.env.BRIDGE_SENDER || 'antigravity';
const DEFAULT_TARGET = process.env.BRIDGE_TARGET || 'all';

function parseArgs(argv) {
  const positionals = [];
  const flags = {};

  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg.startsWith('--')) {
      const eqIdx = arg.indexOf('=');
      if (eqIdx !== -1) {
        const key = arg.slice(2, eqIdx);
        flags[key] = arg.slice(eqIdx + 1);
      } else {
        const key = arg.slice(2);
        const next = argv[i + 1];
        if (next !== undefined && !next.startsWith('--')) {
          flags[key] = next;
          i++;
        } else {
          flags[key] = true;
        }
      }
    } else {
      positionals.push(arg);
    }
  }

  return { positionals, flags };
}

function printUsage() {
  console.log(`Usage:
  node cli.js send "<content>" [STATUS] [--channel default] [--sender antigravity] [--target all]
  node cli.js wait [--channel default] [--sender antigravity] [--timeout 60] [--since-id 0]
  node cli.js messages [--channel default] [--since-id 0]
  node cli.js state <STATE> [--channel default]
  node cli.js pause [--channel default]
  node cli.js reset [--channel default]
  node cli.js health

Environment:
  BRIDGE_URL      Base URL of the chat-bridge server (default: http://localhost:3100)
  BRIDGE_CHANNEL  Default channel_id (default: default)
  BRIDGE_SENDER   Default sender identity (default: antigravity)
  BRIDGE_TARGET   Default target recipient (default: all)`);
}

async function requestJson(urlPath, options = {}) {
  const url = `${BRIDGE_URL}${urlPath}`;
  const res = await fetch(url, {
    ...options,
    headers: {
      'content-type': 'application/json',
      ...(options.headers || {})
    }
  });

  const text = await res.text();
  let data;
  try {
    data = text ? JSON.parse(text) : {};
  } catch {
    data = { raw: text };
  }

  if (!res.ok) {
    const err = new Error(data?.error || `HTTP ${res.status} ${res.statusText}`);
    err.status = res.status;
    err.data = data;
    throw err;
  }

  return data;
}

async function main() {
  const { positionals, flags } = parseArgs(process.argv.slice(2));
  const command = positionals[0];

  if (!command || flags.help || flags.h || command === 'help') {
    printUsage();
    process.exit(command ? 0 : 1);
  }

  const channel_id = String(flags.channel || flags.channel_id || DEFAULT_CHANNEL);
  const sender = String(flags.sender || DEFAULT_SENDER);
  const target = String(flags.target || DEFAULT_TARGET);

  switch (command) {
    case 'send': {
      const content = positionals[1];
      const status = String(positionals[2] || flags.status || 'INFO');
      if (!content || !content.trim()) {
        console.error('Error: send requires message content.\nUsage: node cli.js send "<content>" "<STATUS>"');
        process.exit(1);
      }

      const data = await requestJson('/api/send', {
        method: 'POST',
        body: JSON.stringify({ channel_id, sender, target, content, status })
      });
      console.log(JSON.stringify(data, null, 2));
      break;
    }

    case 'wait': {
      const params = new URLSearchParams({
        channel_id,
        sender,
        timeout: String(flags.timeout ?? 60)
      });
      if (flags['since-id'] !== undefined || flags.since_id !== undefined) {
        params.set('since_id', String(flags['since-id'] ?? flags.since_id));
      }

      const data = await requestJson(`/api/wait?${params.toString()}`, {
        method: 'GET'
      });
      console.log(JSON.stringify(data, null, 2));
      break;
    }

    case 'messages': {
      const params = new URLSearchParams({
        channel_id,
        since_id: String(flags['since-id'] ?? flags.since_id ?? 0)
      });
      const data = await requestJson(`/api/messages?${params.toString()}`, {
        method: 'GET'
      });
      console.log(JSON.stringify(data, null, 2));
      break;
    }

    case 'state': {
      const state = positionals[1] || flags.state;
      if (!state) {
        console.error('Error: state requires a target state value.');
        process.exit(1);
      }
      const data = await requestJson('/api/state', {
        method: 'POST',
        body: JSON.stringify({ channel_id, state })
      });
      console.log(JSON.stringify(data, null, 2));
      break;
    }

    case 'pause': {
      const data = await requestJson('/api/pause', {
        method: 'POST',
        body: JSON.stringify({ channel_id })
      });
      console.log(JSON.stringify(data, null, 2));
      break;
    }

    case 'reset': {
      const data = await requestJson('/api/reset', {
        method: 'POST',
        body: JSON.stringify({ channel_id })
      });
      console.log(JSON.stringify(data, null, 2));
      break;
    }

    case 'health': {
      const data = await requestJson('/health', { method: 'GET' });
      console.log(JSON.stringify(data, null, 2));
      break;
    }

    default:
      console.error(`Unknown command: ${command}`);
      printUsage();
      process.exit(1);
  }
}

main().catch((err) => {
  console.error(
    JSON.stringify(
      {
        ok: false,
        error: err.message,
        status: err.status || undefined,
        details: err.data || undefined
      },
      null,
      2
    )
  );
  process.exit(1);
});
