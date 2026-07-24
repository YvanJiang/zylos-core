import fs from 'node:fs';
import { fork } from 'node:child_process';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { afterEach, expect, test } from '@jest/globals';

import { requestExecutorService } from '../runtime/executor/service-host.js';

const CHILD_PATH = fileURLToPath(
  new URL('./helpers/executor-service-host-child.js', import.meta.url),
);
const children = new Set();
const directories = [];

function fixture() {
  const tempRoot = fs.existsSync('/tmp') ? fs.realpathSync('/tmp') : os.tmpdir();
  const directory = fs.mkdtempSync(path.join(tempRoot, 'zylos-service-host-socket-'));
  directories.push(directory);
  return path.join(directory, 'executor-service.sock');
}

function waitForMessage(child, type, timeoutMs = 2_000) {
  return new Promise((resolve, reject) => {
    let timer;
    const cleanup = () => {
      clearTimeout(timer);
      child.off('message', onMessage);
      child.off('exit', onExit);
      child.off('error', onError);
    };
    const onMessage = (message) => {
      if (message?.type !== type) return;
      cleanup();
      resolve(message);
    };
    const onExit = (code, signal) => {
      cleanup();
      reject(new Error(`Executor host exited before ${type}: code=${code} signal=${signal}`));
    };
    const onError = (error) => {
      cleanup();
      reject(error);
    };
    child.on('message', onMessage);
    child.once('exit', onExit);
    child.once('error', onError);
    timer = setTimeout(() => {
      cleanup();
      reject(new Error(`Timed out waiting for executor host message: ${type}`));
    }, timeoutMs);
  });
}

async function stopChild(child) {
  children.delete(child);
  if (child.exitCode !== null || child.signalCode !== null) return;
  const exited = new Promise((resolve) => child.once('exit', resolve));
  if (child.connected) child.send({ type: 'stop' });
  const timer = setTimeout(() => child.kill('SIGKILL'), 1_000);
  timer.unref?.();
  await exited;
  clearTimeout(timer);
}

async function startChild(errorCode) {
  const socketPath = fixture();
  const child = fork(CHILD_PATH, [socketPath, errorCode], {
    cwd: path.dirname(fileURLToPath(import.meta.url)),
    silent: true,
  });
  children.add(child);
  const termination = new Promise((resolve) => {
    child.on('message', (message) => {
      if (message?.type === 'fatal') resolve(message);
    });
    child.once('exit', (code, signal) => resolve({ type: 'exit', code, signal }));
  });
  await waitForMessage(child, 'ready');
  return { child, socketPath, termination };
}

afterEach(async () => {
  await Promise.allSettled([...children].map(stopChild));
  for (const directory of directories.splice(0)) {
    fs.rmSync(directory, { recursive: true, force: true });
  }
});

test.each([
  'EPIPE',
  'ECONNRESET',
  'ECONNABORTED',
  'ENOTCONN',
  'ERR_STREAM_WRITE_AFTER_END',
])(
  'keeps the executor host alive when a timed-out control caller causes late-response %s',
  async (errorCode) => {
    const scenario = await startChild(errorCode);
    const upgradeStarted = waitForMessage(scenario.child, 'upgrade_started');
    const request = requestExecutorService(
      scenario.socketPath,
      { action: 'upgrade', target: {} },
      { timeoutMs: 50 },
    );
    await upgradeStarted;
    await expect(request).rejects.toMatchObject({ code: 'ETIMEDOUT', outcome: 'unknown' });
    scenario.child.send({ type: 'release_upgrade' });

    const crash = await Promise.race([
      scenario.termination,
      new Promise((resolve) => setTimeout(() => resolve(null), 250)),
    ]);
    expect(crash).toBeNull();
    await expect(requestExecutorService(scenario.socketPath, { action: 'health' })).resolves
      .toMatchObject({
        ok: true,
        result: {
          executor: { service_instance_id: 'socket-stability-fixture' },
        },
      });
  },
);
