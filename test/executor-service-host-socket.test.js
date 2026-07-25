import fs from 'node:fs';
import { fork } from 'node:child_process';
import net from 'node:net';
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
  return {
    directory,
    socketPath: path.join(directory, 'executor-service.sock'),
  };
}

function settleWithin(promise, timeoutMs, timeoutValue) {
  let timer;
  return Promise.race([
    promise,
    new Promise((resolve) => {
      timer = setTimeout(() => resolve(timeoutValue), timeoutMs);
    }),
  ]).finally(() => clearTimeout(timer));
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
      reject(new Error(`Executor host child exited before ${type}: code=${code} signal=${signal}`));
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
      reject(new Error(`Timed out waiting for executor host child message: ${type}`));
    }, timeoutMs);
  });
}

async function stopChild(child) {
  children.delete(child);
  if (child.exitCode !== null || child.signalCode !== null) return;
  const exited = new Promise((resolve) => child.once('exit', resolve));
  if (child.connected) child.send({ type: 'stop' });
  if (await settleWithin(exited.then(() => true), 1_000, false)) return;
  child.kill('SIGKILL');
  await exited;
}

async function startChild(injectedErrorCodes) {
  const state = fixture();
  const child = fork(CHILD_PATH, [state.socketPath, injectedErrorCodes.join(',')], {
    cwd: path.dirname(fileURLToPath(import.meta.url)),
    silent: true,
  });
  children.add(child);
  const stderr = [];
  child.stderr.on('data', (chunk) => stderr.push(chunk.toString('utf8')));
  const fatal = new Promise((resolve) => {
    child.on('message', (message) => {
      if (message?.type === 'fatal') resolve(message);
    });
  });
  const exited = new Promise((resolve) => {
    child.once('exit', (code, signal) => resolve({ type: 'exit', code, signal }));
  });
  await waitForMessage(child, 'ready');
  return {
    child,
    socketPath: state.socketPath,
    stderr,
    termination: Promise.race([fatal, exited]),
  };
}

async function triggerLateResponse(injectedErrorCodes) {
  const scenario = await startChild(injectedErrorCodes);
  const upgradeStarted = waitForMessage(scenario.child, 'upgrade_started');
  const request = requestExecutorService(
    scenario.socketPath,
    { action: 'upgrade', target: {} },
    { timeoutMs: 50 },
  );
  await upgradeStarted;
  await expect(request).rejects.toMatchObject({ code: 'ETIMEDOUT', outcome: 'unknown' });
  await new Promise((resolve) => setTimeout(resolve, 50));
  scenario.child.send({ type: 'release_upgrade' });
  return scenario;
}

function rawRequest(socketPath, requests) {
  return new Promise((resolve, reject) => {
    const socket = net.createConnection(socketPath);
    let data = '';
    socket.setEncoding('utf8');
    socket.once('connect', () => {
      socket.write(requests.map((request) => `${JSON.stringify(request)}\n`).join(''));
    });
    socket.on('data', (chunk) => {
      data += chunk;
    });
    socket.once('error', reject);
    socket.once('end', () => resolve(data));
  });
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
  'keeps the executor host alive when a timed-out client causes late-response %s',
  async (peerDisconnectErrorCode) => {
    const scenario = await triggerLateResponse([peerDisconnectErrorCode]);
    const crash = await settleWithin(scenario.termination, 250, null);
    expect(crash).toBeNull();
    await expect(requestExecutorService(scenario.socketPath, { action: 'health' })).resolves
      .toEqual({
        ok: true,
        result: {
          executor: {
            provider: 'codex',
            service_instance_id: 'socket-stability-fixture',
          },
          snapshot: { contract: 'socket-stability-fixture' },
        },
      });
    expect(scenario.stderr.join('')).toBe('');
  },
);

test('keeps the executor host alive across repeated peer-disconnect write errors', async () => {
  const scenario = await triggerLateResponse(['EPIPE', 'ECONNRESET']);

  const crash = await settleWithin(scenario.termination, 250, null);
  expect(crash).toBeNull();
  await expect(requestExecutorService(scenario.socketPath, { action: 'health' })).resolves
    .toMatchObject({
      ok: true,
      result: {
        executor: {
          service_instance_id: 'socket-stability-fixture',
        },
      },
    });
});

test('does not swallow an unexpected internal socket error', async () => {
  const scenario = await triggerLateResponse(['EINTERNAL']);

  await expect(settleWithin(scenario.termination, 1_000, null)).resolves.toMatchObject({
    type: 'fatal',
    kind: 'uncaughtException',
    code: 'EINTERNAL',
    message: 'write EINTERNAL',
  });
});

test('returns one normal response and closes idempotently for repeated client work', async () => {
  const scenario = await startChild(['EPIPE']);
  const responses = (await rawRequest(scenario.socketPath, [
    { action: 'health' },
    { action: 'health' },
  ])).trim().split('\n');

  expect(responses).toHaveLength(1);
  expect(JSON.parse(responses[0])).toMatchObject({
    ok: true,
    result: {
      executor: {
        provider: 'codex',
        service_instance_id: 'socket-stability-fixture',
      },
    },
  });
  await expect(requestExecutorService(scenario.socketPath, { action: 'health' })).resolves
    .toMatchObject({ ok: true });

  const closed = waitForMessage(scenario.child, 'closed_twice');
  scenario.child.send({ type: 'close_twice' });
  await expect(closed).resolves.toEqual({ type: 'closed_twice' });
  expect(fs.existsSync(scenario.socketPath)).toBe(false);
});
