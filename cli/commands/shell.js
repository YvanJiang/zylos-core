/**
 * zylos shell — CLI interactive mode
 *
 * Minimal-dependency REPL that communicates with Claude via C4.
 * Uses a Unix domain socket for real-time response delivery.
 */

import readline from 'node:readline';
import net from 'node:net';
import fs from 'node:fs';
import crypto from 'node:crypto';
import os from 'node:os';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import { bold, dim, cyan } from '../lib/colors.js';
import { createOutboxService } from '../../runtime/delivery/outbox-service.js';
import { createChannelNeutralTextRenderer } from '../../runtime/compatibility/c4-channel-fallback.js';

const ZYLOS_DIR = process.env.ZYLOS_DIR || path.join(os.homedir(), 'zylos');
const C4_RECEIVE = path.join(ZYLOS_DIR, '.claude', 'skills', 'comm-bridge', 'scripts', 'c4-receive.js');
const CORE_DATABASE_PATH = path.join(ZYLOS_DIR, 'comm-bridge', 'c4.db');

function deliverToSocket(socketPath, message) {
  return new Promise((resolve, reject) => {
    const client = net.createConnection({ path: socketPath }, () => client.end(message));
    client.once('error', reject);
    client.once('close', () => resolve());
  });
}

export async function shellCommand() {
  const { default: Database } = await import(new URL(
    '../../skills/comm-bridge/node_modules/better-sqlite3/lib/index.js',
    import.meta.url,
  ));
  const socketPath = path.join(os.tmpdir(), `zylos-shell-${process.pid}.sock`);

  // Clean up stale socket files from previous sessions (e.g. kill -9)
  cleanStaleSockets();

  // Clean up own socket file if it exists
  try { fs.unlinkSync(socketPath); } catch {}

  // Verify c4-receive exists
  if (!fs.existsSync(C4_RECEIVE)) {
    console.error('Error: comm-bridge not found. Run "zylos doctor" to check your installation.');
    process.exit(1);
  }

  // Start Unix socket server to receive responses
  let pendingResolve = null;

  const server = net.createServer((conn) => {
    let data = '';
    conn.setEncoding('utf8');
    conn.on('error', () => {}); // ignore client disconnect errors
    conn.on('data', (chunk) => { data += chunk; });
    conn.on('end', () => {
      if (data && pendingResolve) {
        pendingResolve(data);
        pendingResolve = null;
      } else if (data) {
        // Response arrived without a pending prompt (e.g. proactive agent message,
        // or a late reply after the 120s timeout cleared pendingResolve).
        // Print immediately and restore the prompt so the user can keep typing.
        process.stdout.write(`\n${formatResponse(data)}\n\n`);
        rl.prompt();
      }
    });
  });

  // Set umask before listen to create socket with correct permissions (owner-only)
  const oldMask = process.umask(0o177);
  server.listen(socketPath, () => {
    process.umask(oldMask);
  });

  server.on('error', (err) => {
    process.umask(oldMask);
    console.error(`Error: could not start shell server — ${err.message}`);
    process.exit(1);
  });

  fs.mkdirSync(path.dirname(CORE_DATABASE_PATH), { recursive: true });
  const database = new Database(CORE_DATABASE_PATH);
  database.pragma('journal_mode = WAL');
  database.pragma('busy_timeout = 5000');
  database.pragma('foreign_keys = ON');
  const deliveryOwner = createOutboxService({
    database,
    channel: 'shell',
    targetChatId: socketPath,
    serviceInstanceId: `shell-${process.pid}`,
    renderer: createChannelNeutralTextRenderer({
      async sendText(delivery) {
        if (delivery.target.chat_id !== socketPath) {
          throw new Error('Shell delivery target does not match this shell owner.');
        }
        await deliverToSocket(socketPath, delivery.text);
        return { platform_message_id: `shell:${delivery.delivery_id}` };
      },
    }),
  });
  let deliveryInFlight = false;
  async function drainDeliveries() {
    if (deliveryInFlight) return;
    deliveryInFlight = true;
    try {
      for (let count = 0; count < 20; count += 1) {
        const result = await deliveryOwner.dispatchNext();
        if (result.status === 'idle') break;
      }
    } catch (error) {
      console.error(`Shell delivery owner: ${error.message}`);
    } finally {
      deliveryInFlight = false;
    }
  }
  const deliveryTimer = setInterval(() => { drainDeliveries(); }, 250);

  // Cleanup on exit (guard against double invocation)
  let cleaned = false;
  function cleanup() {
    if (cleaned) return;
    cleaned = true;
    clearInterval(deliveryTimer);
    server.close();
    database.close();
    try { fs.unlinkSync(socketPath); } catch {}
  }
  process.on('SIGINT', () => { cleanup(); process.exit(0); });
  process.on('SIGTERM', () => { cleanup(); process.exit(0); });
  process.on('exit', cleanup);

  // Print banner
  console.log(bold('Zylos Shell'));
  console.log(dim('Interactive mode — type your message and press Enter.'));
  console.log(dim('Commands: /quit to exit, /help for help'));
  console.log();

  // Start REPL
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
    prompt: cyan('you> '),
    terminal: process.stdin.isTTY !== false,
  });

  rl.prompt();

  rl.on('line', async (line) => {
    const input = line.trim();

    if (!input) {
      rl.prompt();
      return;
    }

    // Handle local commands
    if (input === '/quit' || input === '/exit' || input === '/q') {
      console.log(dim('Goodbye.'));
      cleanup();
      process.exit(0);
    }

    if (input === '/help') {
      printHelp();
      rl.prompt();
      return;
    }

    // Send message via C4
    try {
      execFileSync(process.execPath, [
        C4_RECEIVE,
        '--channel', 'shell',
        '--endpoint', socketPath,
        '--message-id', `shell-${crypto.randomUUID()}`,
        '--actor-id', 'local-shell-user',
        '--content', input,
        '--json',
      ], { encoding: 'utf8', stdio: ['pipe', 'pipe', 'pipe'] });
    } catch (err) {
      // Parse JSON error from c4-receive if possible
      const stderr = err.stderr || '';
      const stdout = err.stdout || '';
      let errorMsg = 'Failed to send message';
      try {
        const result = JSON.parse(stdout);
        if (result.error?.message) errorMsg = result.error.message;
      } catch {
        if (stderr) errorMsg = stderr.trim();
      }
      console.log(`\n${dim('Error:')} ${errorMsg}\n`);
      rl.prompt();
      return;
    }

    // Wait for response with timeout
    process.stdout.write(dim('  thinking...'));

    try {
      const response = await waitForResponse(120000);
      // Clear "thinking..." and print response
      if (process.stdout.isTTY) {
        readline.clearLine(process.stdout, 0);
        readline.cursorTo(process.stdout, 0);
      } else {
        process.stdout.write('\n');
      }
      console.log(formatResponse(response));
    } catch {
      if (process.stdout.isTTY) {
        readline.clearLine(process.stdout, 0);
        readline.cursorTo(process.stdout, 0);
      } else {
        process.stdout.write('\n');
      }
      console.log(dim('  (no response within timeout — message is queued, check back later)'));
    }

    console.log();
    rl.prompt();
  });

  rl.on('close', () => {
    cleanup();
    process.exit(0);
  });

  function waitForResponse(timeoutMs) {
    return new Promise((resolve, reject) => {
      // Reject any previously pending promise to avoid memory leaks
      if (pendingResolve) {
        pendingResolve = null;
      }

      const timer = setTimeout(() => {
        pendingResolve = null;
        reject(new Error('timeout'));
      }, timeoutMs);

      pendingResolve = (data) => {
        clearTimeout(timer);
        resolve(data);
      };
    });
  }
}

function formatResponse(text) {
  return `${bold('zylos>')} ${text}`;
}

function cleanStaleSockets() {
  const tmpDir = os.tmpdir();
  try {
    const files = fs.readdirSync(tmpDir);
    for (const file of files) {
      const match = file.match(/^zylos-shell-(\d+)\.sock$/);
      if (!match) continue;
      const pid = Number(match[1]);
      // Check if the process is still running
      try {
        process.kill(pid, 0);
      } catch {
        // Process doesn't exist — clean up stale socket
        try { fs.unlinkSync(path.join(tmpDir, file)); } catch {}
      }
    }
  } catch {}
}

function printHelp() {
  console.log(`
${bold('Zylos Shell')} — Interactive CLI

Type any message to chat with the agent.

${bold('Commands:')}
  /help       Show this help
  /quit       Exit the shell (also: /exit, /q, Ctrl+D)
`);
}
