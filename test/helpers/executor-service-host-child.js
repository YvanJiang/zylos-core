import net from 'node:net';

import { createExecutorServiceHost } from '../../runtime/executor/service-host.js';

const socketPath = process.argv[2];
const injectedErrorCodes = (process.argv[3] ?? 'EPIPE').split(',');
const originalSocketEnd = net.Socket.prototype.end;
let injectPeerDisconnectError = false;
let releaseUpgrade;
const upgradeGate = new Promise((resolve) => {
  releaseUpgrade = resolve;
});

// Kernel timing makes a late Unix-socket write error nondeterministic, so inject it at the
// transport boundary after the real client timeout and disconnect have completed.
net.Socket.prototype.end = function endWithPeerDisconnectFault(...args) {
  const result = originalSocketEnd.apply(this, args);
  if (injectPeerDisconnectError) {
    injectPeerDisconnectError = false;
    for (const code of injectedErrorCodes) {
      setImmediate(() => {
        const error = new Error(`write ${code}`);
        error.code = code;
        this.emit('error', error);
      });
    }
  }
  return result;
};

function send(message) {
  if (process.connected) process.send(message);
}

function reportFatal(kind, error) {
  send({
    type: 'fatal',
    kind,
    code: error?.code ?? null,
    message: error?.message ?? String(error),
  });
  setImmediate(() => process.exit(86));
}

process.once('uncaughtException', (error) => reportFatal('uncaughtException', error));
process.once('unhandledRejection', (error) => reportFatal('unhandledRejection', error));

const service = {
  start() {},
  async runNext() {
    return { status: 'idle' };
  },
  publishObservabilitySnapshot() {
    return { contract: 'socket-stability-fixture' };
  },
  async close() {},
};

const host = createExecutorServiceHost({
  database: null,
  adapter: {},
  provider: 'codex',
  serviceInstanceId: 'socket-stability-fixture',
  socketPath,
  workspaceRoot: process.cwd(),
  pollIntervalMs: 10_000,
  createService: () => service,
  onUpgrade: async () => {
    send({ type: 'upgrade_started' });
    await upgradeGate;
    return { state: 'rolled_back' };
  },
});

process.on('message', (message) => {
  if (message?.type === 'release_upgrade') {
    injectPeerDisconnectError = true;
    releaseUpgrade();
    return;
  }
  if (message?.type === 'close_twice') {
    Promise.all([host.close(), host.close()]).then(
      () => send({ type: 'closed_twice' }),
      (error) => reportFatal('closeFailure', error),
    );
    return;
  }
  if (message?.type === 'stop') {
    host.close().then(
      () => process.exit(0),
      (error) => reportFatal('closeFailure', error),
    );
  }
});

host.start().then(
  () => send({ type: 'ready' }),
  (error) => reportFatal('startFailure', error),
);
