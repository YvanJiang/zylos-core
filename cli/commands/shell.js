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

export function createShellRuntimeIdentity({
  randomUUID = crypto.randomUUID,
  temporaryDirectory = os.tmpdir,
} = {}) {
  const birthId = randomUUID();
  return Object.freeze({
    birthId,
    socketPath: path.join(temporaryDirectory(), `zylos-shell-${birthId}.sock`),
    serviceInstanceId: `shell-${birthId}`,
  });
}

export function createDeliveryDrain({ dispatchNext, onError = () => {} }) {
  if (typeof dispatchNext !== 'function') throw new TypeError('dispatchNext must be a function');
  if (typeof onError !== 'function') throw new TypeError('onError must be a function');
  let stopped = false;
  let inFlight = null;

  function drain() {
    if (stopped) return inFlight ?? Promise.resolve();
    if (inFlight !== null) return inFlight;
    inFlight = (async () => {
      try {
        for (let count = 0; count < 20; count += 1) {
          const result = await dispatchNext();
          if (stopped || result.status === 'idle') break;
        }
      } catch (error) {
        onError(error);
      } finally {
        inFlight = null;
      }
    })();
    return inFlight;
  }

  async function stop() {
    stopped = true;
    if (inFlight !== null) await inFlight;
  }

  return Object.freeze({ drain, stop });
}

export async function shellCommand() {
  const { default: Database } = await import(new URL(
    '../../skills/comm-bridge/node_modules/better-sqlite3/lib/index.js',
    import.meta.url,
  ));
  const { socketPath, serviceInstanceId } = createShellRuntimeIdentity();

  // Install stop fencing before any socket, database owner, or polling timer
  // can exist. Node delivers signals between turns, so all resources registered
  // during this synchronous startup are visible to the same cleanup barrier.
  let server = null;
  let serverState = 'absent';
  let database = null;
  let deliveryDrain = null;
  let deliveryTimer = null;
  let rl = null;
  let cleanupPromise = null;
  function cleanup() {
    if (cleanupPromise !== null) return cleanupPromise;
    if (deliveryTimer !== null) clearInterval(deliveryTimer);
    cleanupPromise = (async () => {
      if (deliveryDrain !== null) await deliveryDrain.stop();
      if (server !== null) {
        await new Promise((resolve) => {
          const close = () => {
            server.off('error', close);
            if (server.listening) {
              server.close(() => {
                serverState = 'closed';
                resolve();
              });
            } else {
              resolve();
            }
          };
          if (server.listening) close();
          else if (serverState === 'failed' || serverState === 'closed') resolve();
          else {
            server.once('listening', close);
            server.once('error', close);
          }
        });
      }
      if (database !== null) database.close();
      try { fs.unlinkSync(socketPath); } catch {}
    })();
    return cleanupPromise;
  }

  let shutdownPromise = null;
  function shutdown() {
    if (shutdownPromise === null) {
      shutdownPromise = cleanup();
      if (rl !== null && !rl.closed) rl.close();
    }
    return shutdownPromise;
  }
  process.once('SIGINT', () => { void shutdown(); });
  process.once('SIGTERM', () => { void shutdown(); });

  // Clean up own socket file if it exists
  try { fs.unlinkSync(socketPath); } catch {}

  // Verify c4-receive exists
  if (!fs.existsSync(C4_RECEIVE)) {
    console.error('Error: comm-bridge not found. Run "zylos doctor" to check your installation.');
    process.exit(1);
  }

  // Start Unix socket server to receive responses
  let pendingResolve = null;

  server = net.createServer((conn) => {
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
        rl?.prompt();
      }
    });
  });
  serverState = 'starting';
  server.once('close', () => { serverState = 'closed'; });

  // Set umask before listen to create socket with correct permissions (owner-only)
  const oldMask = process.umask(0o177);
  server.listen(socketPath, () => {
    serverState = 'listening';
    process.umask(oldMask);
  });

  server.on('error', (err) => {
    serverState = 'failed';
    process.umask(oldMask);
    console.error(`Error: could not start shell server — ${err.message}`);
    process.exitCode = 1;
    void shutdown();
  });

  try {
    fs.mkdirSync(path.dirname(CORE_DATABASE_PATH), { recursive: true });
    database = new Database(CORE_DATABASE_PATH);
    database.pragma('journal_mode = WAL');
    database.pragma('busy_timeout = 5000');
    database.pragma('foreign_keys = ON');
    const deliveryOwner = createOutboxService({
      database,
      channel: 'shell',
      targetChatId: socketPath,
      serviceInstanceId,
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
    deliveryDrain = createDeliveryDrain({
      dispatchNext: () => deliveryOwner.dispatchNext(),
      onError(error) {
        console.error(`Shell delivery owner: ${error.message}`);
      },
    });
    deliveryTimer = setInterval(() => { void deliveryDrain.drain(); }, 250);

    console.log(bold('Zylos Shell'));
    console.log(dim('Interactive mode — type your message and press Enter.'));
    console.log(dim('Commands: /quit to exit, /help for help'));
    console.log();

    rl = readline.createInterface({
      input: process.stdin,
      output: process.stdout,
      prompt: cyan('you> '),
      terminal: process.stdin.isTTY !== false,
    });
  } catch (error) {
    process.exitCode = 1;
    await shutdown();
    throw error;
  }

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
      await shutdown();
      return;
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
    void shutdown();
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

function printHelp() {
  console.log(`
${bold('Zylos Shell')} — Interactive CLI

Type any message to chat with the agent.

${bold('Commands:')}
  /help       Show this help
  /quit       Exit the shell (also: /exit, /q, Ctrl+D)
`);
}
