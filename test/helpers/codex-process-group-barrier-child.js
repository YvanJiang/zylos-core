import { EventEmitter } from 'node:events';
import { PassThrough } from 'node:stream';

import { createCodexAppServerAdapter } from '../../runtime/providers/codex-app-server-adapter.js';

const child = new EventEmitter();
child.pid = 424_242;
child.stdin = new PassThrough();
child.stdout = new PassThrough();
child.stderr = new PassThrough();
let input = '';
let processGroupAlive = true;

function send(message) {
  child.stdout.write(`${JSON.stringify(message)}\n`);
}

child.stdin.on('data', (chunk) => {
  input += chunk.toString('utf8');
  while (input.includes('\n')) {
    const newline = input.indexOf('\n');
    const line = input.slice(0, newline);
    input = input.slice(newline + 1);
    if (line.length === 0) continue;
    const message = JSON.parse(line);
    if (message.method === 'initialize') {
      send({ id: message.id, result: { userAgent: 'process-group-barrier-test' } });
    } else if (message.method === 'thread/start') {
      send({ id: message.id, result: { thread: { id: 'barrier-thread' } } });
    } else if (message.method === 'turn/start') {
      send({ id: message.id, result: { turn: { id: 'barrier-turn' } } });
      queueMicrotask(() => {
        send({
          method: 'turn/started',
          params: {
            threadId: 'barrier-thread',
            turn: { id: 'barrier-turn', status: 'inProgress', items: [] },
          },
        });
        send({
          method: 'turn/completed',
          params: {
            threadId: 'barrier-thread',
            turn: { id: 'barrier-turn', status: 'completed', items: [] },
          },
        });
      });
    }
  }
});

const adapter = createCodexAppServerAdapter({
  spawnProcess: () => child,
  processTerminationGraceMs: 30,
  signalProcessGroup(_processGroupId, _child, signal) {
    if (signal === 'SIGTERM') queueMicrotask(() => child.emit('close', 0, signal));
    if (signal === 'SIGKILL') processGroupAlive = false;
    return true;
  },
  isProcessGroupAlive: () => processGroupAlive,
});

const context = {
  conversation_id: 'barrier-conversation',
  turn_id: 'barrier-core-turn',
  lineage_id: 'barrier-lineage',
  trace_id: 'barrier-trace',
  input: { kind: 'text', text: 'barrier', attachments: [] },
  lineage: { provider_native_id: null },
  async bindProviderNativeId() {},
  reportProviderState() {},
  reportRuntimeEvidence() {},
  interaction: {
    authorized_subjects: [{ type: 'actor', actor_id: 'barrier-user' }],
    allowed_sources: ['operations_control'],
  },
  attempt: { attempt_id: 'barrier-attempt', attempt_no: 1, lease_epoch: 1 },
};

for await (const _event of adapter.execute(context)) {
  // The fake turn has no normalized payload events.
}
await adapter.close();
process.stdout.write('PROCESS_GROUP_BARRIER_COMPLETED\n');
