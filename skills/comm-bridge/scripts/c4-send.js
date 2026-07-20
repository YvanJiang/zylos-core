#!/usr/bin/env node

/**
 * Record-only C4 utility.
 *
 * External replies are delivery side effects and therefore must be claimed
 * from Core's durable outbox by the owning channel renderer. This CLI keeps
 * only the internal `void` record used for non-dispatched handoff notes.
 */
import { close, insertConversation } from './c4-db.js';
import { validateEndpoint } from './c4-validate.js';

function readStdin() {
  return new Promise((resolve, reject) => {
    let data = '';
    process.stdin.setEncoding('utf8');
    process.stdin.on('data', (chunk) => { data += chunk; });
    process.stdin.on('end', () => resolve(data));
    process.stdin.on('error', reject);
  });
}

async function main() {
  const [channel, endpoint, argumentMessage] = process.argv.slice(2)
    .filter((argument) => argument !== '--stdin');
  if (channel !== 'void') {
    console.error('[C4] External normal replies must be rendered from the durable Core outbox by the channel delivery owner.');
    process.exit(2);
  }
  try {
    validateEndpoint(endpoint);
  } catch (error) {
    console.error(`[C4] Invalid void endpoint: ${error.message}`);
    process.exit(1);
  }
  const message = argumentMessage ?? (!process.stdin.isTTY ? (await readStdin()).trimEnd() : '');
  if (!message) {
    console.error('[C4] A void record requires message content on stdin.');
    process.exit(1);
  }
  try {
    insertConversation('out', 'void', endpoint, message);
    console.log('[C4] Message recorded on void channel (not dispatched)');
  } catch (error) {
    console.error(`[C4] Failed to record void message: ${error.message}`);
    process.exitCode = 1;
  } finally {
    close();
  }
}

main();
