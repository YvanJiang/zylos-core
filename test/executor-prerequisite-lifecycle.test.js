import fs from 'node:fs';
import net from 'node:net';
import os from 'node:os';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { pathToFileURL } from 'node:url';

import { afterEach, describe, expect, test } from '@jest/globals';

import { inspectListeningPort } from '../runtime/executor/prerequisite-process.js';

const OWNER_MODULE_URL = pathToFileURL(
  path.resolve(import.meta.dirname, '../runtime/executor/prerequisite-owner.js'),
).href;
const CONTRACT = 'zylos.prerequisite-child@1';
const tempDirectories = [];
const spawnedParents = [];

function isAlive(pid) {
  if (!Number.isSafeInteger(pid) || pid <= 0) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    return error?.code === 'EPERM';
  }
}

async function waitFor(predicate, {
  timeoutMs = 5_000,
  intervalMs = 25,
  message = 'condition was not satisfied',
} = {}) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }
  throw new Error(message);
}

async function unusedPort() {
  const server = net.createServer();
  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', resolve);
  });
  const { port } = server.address();
  await new Promise((resolve) => server.close(resolve));
  return port;
}

function appendOnlyPids(pidLog) {
  try {
    return fs.readFileSync(pidLog, 'utf8').trim().split('\n')
      .filter(Boolean)
      .map((line) => {
        const [service, rawPid] = line.split(':');
        return { service, pid: Number(rawPid) };
      });
  } catch {
    return [];
  }
}

function createFixture({ disconnectMode = 'exit' } = {}) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'zylos-prerequisite-lifecycle-'));
  tempDirectories.push(directory);
  const releasePath = path.join(directory, 'runtime', 'releases', 'release-A');
  const schedulerPath = path.join(releasePath, 'skills', 'scheduler', 'scripts', 'daemon.js');
  const webConsolePath = path.join(releasePath, 'skills', 'web-console', 'scripts', 'server.js');
  const parentHarnessPath = path.join(directory, 'owner-parent.mjs');
  const pidLog = path.join(directory, 'prerequisite-pids.log');
  fs.mkdirSync(path.dirname(schedulerPath), { recursive: true });
  fs.mkdirSync(path.dirname(webConsolePath), { recursive: true });

  fs.writeFileSync(schedulerPath, `
import fs from 'node:fs';
fs.appendFileSync(process.env.PREREQUISITE_PID_LOG, \`scheduler:\${process.pid}\\n\`);
process.send?.({
  contract: ${JSON.stringify(CONTRACT)},
  type: 'ready',
  service: 'scheduler',
  pid: process.pid,
});
if (process.env.PREREQUISITE_DISCONNECT_MODE === 'exit') {
  process.once('disconnect', () => process.exit(0));
}
process.once('SIGTERM', () => process.exit(0));
setInterval(() => {}, 1_000);
`);
  fs.writeFileSync(webConsolePath, `
import fs from 'node:fs';
import net from 'node:net';
const port = Number(process.env.WEB_CONSOLE_PORT);
const host = process.env.WEB_CONSOLE_BIND || '127.0.0.1';
const server = net.createServer((socket) => socket.end(String(process.pid)));
server.once('error', (error) => {
  process.send?.({
    contract: ${JSON.stringify(CONTRACT)},
    type: 'error',
    service: 'web-console',
    pid: process.pid,
    code: error.code || 'UNKNOWN',
    message: error.message,
    port,
    host,
  });
  process.exitCode = 1;
});
server.listen(port, host, () => {
  fs.appendFileSync(process.env.PREREQUISITE_PID_LOG, \`web-console:\${process.pid}\\n\`);
  process.send?.({
    contract: ${JSON.stringify(CONTRACT)},
    type: 'ready',
    service: 'web-console',
    pid: process.pid,
    port,
    host,
  });
});
function close() {
  if (!server.listening) {
    process.exit(process.exitCode || 0);
    return;
  }
  server.close(() => process.exit(process.exitCode || 0));
}
if (process.env.PREREQUISITE_DISCONNECT_MODE === 'exit') {
  process.once('disconnect', close);
}
process.once('SIGTERM', close);
`);
  fs.writeFileSync(parentHarnessPath, `
import { createExecutorPrerequisiteOwner } from ${JSON.stringify(OWNER_MODULE_URL)};
const owner = createExecutorPrerequisiteOwner({
  zylosDir: process.env.ZYLOS_DIR,
  releasePath: process.env.PREREQUISITE_RELEASE_PATH,
  closeGraceMs: 500,
});
let closing = false;
async function close() {
  if (closing) return;
  closing = true;
  try {
    await owner.close();
    process.exit(0);
  } catch (error) {
    process.send?.({ type: 'close-error', error: { code: error.code, message: error.message } });
    process.exit(1);
  }
}
process.once('SIGTERM', () => { void close(); });
try {
  const health = await owner.start();
  process.send?.({ type: 'started', parentPid: process.pid, health });
  setInterval(() => {}, 1_000);
} catch (error) {
  process.send?.({
    type: 'start-error',
    parentPid: process.pid,
    error: {
      code: error.code || null,
      message: error.message,
      evidence: error.evidence || null,
    },
  });
  process.exitCode = 1;
}
`);

  return {
    directory,
    disconnectMode,
    lockPath: path.join(directory, 'runtime', 'prerequisite-owner.lock'),
    parentHarnessPath,
    pidLog,
    releasePath,
  };
}

function startOwnerParent(state, port) {
  const child = spawn(process.execPath, [state.parentHarnessPath], {
    env: {
      ...process.env,
      NODE_ENV: 'test',
      PREREQUISITE_DISCONNECT_MODE: state.disconnectMode,
      PREREQUISITE_PID_LOG: state.pidLog,
      PREREQUISITE_RELEASE_PATH: state.releasePath,
      WEB_CONSOLE_BIND: '127.0.0.1',
      WEB_CONSOLE_PORT: String(port),
      ZYLOS_DIR: state.directory,
    },
    stdio: ['ignore', 'pipe', 'pipe', 'ipc'],
  });
  spawnedParents.push(child);
  let stderr = '';
  child.stderr.on('data', (chunk) => { stderr += chunk; });
  const result = new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      reject(new Error(`owner parent timed out; stderr=${stderr}`));
    }, 7_500);
    child.once('error', (error) => {
      clearTimeout(timer);
      reject(error);
    });
    child.once('message', (message) => {
      if (!['started', 'start-error'].includes(message?.type)) return;
      clearTimeout(timer);
      resolve(message);
    });
    child.once('exit', (code, signal) => {
      if (code === 0 || code === null) return;
      clearTimeout(timer);
      reject(new Error(`owner parent exited ${code ?? signal}; stderr=${stderr}`));
    });
  });
  return { child, result };
}

async function stopParent(child, signal = 'SIGTERM') {
  if (!isAlive(child.pid)) return;
  const exited = new Promise((resolve) => child.once('exit', resolve));
  child.kill(signal);
  let timer;
  try {
    await Promise.race([
      exited,
      new Promise((resolve) => { timer = setTimeout(resolve, 3_000); }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

async function readListeningPid(port) {
  return await new Promise((resolve, reject) => {
    const socket = net.createConnection({ host: '127.0.0.1', port });
    let data = '';
    socket.setEncoding('utf8');
    socket.once('error', reject);
    socket.on('data', (chunk) => { data += chunk; });
    socket.once('end', () => resolve(Number(data)));
  });
}

afterEach(async () => {
  await Promise.all(spawnedParents.splice(0).map((child) => stopParent(child, 'SIGKILL')));
  await new Promise((resolve) => setTimeout(resolve, 150));
  for (const directory of tempDirectories) {
    for (const { pid } of appendOnlyPids(path.join(directory, 'prerequisite-pids.log'))) {
      if (isAlive(pid)) {
        try { process.kill(pid, 'SIGKILL'); } catch {}
      }
    }
  }
  await new Promise((resolve) => setTimeout(resolve, 50));
  for (const directory of tempDirectories.splice(0)) {
    fs.rmSync(directory, { recursive: true, force: true });
  }
});

describe('executor prerequisite crash and port lifecycle', () => {
  test('normal close reaps scheduler and web-console and releases ownership', async () => {
    const state = createFixture();
    const port = await unusedPort();
    const parent = startOwnerParent(state, port);
    await expect(parent.result).resolves.toMatchObject({
      type: 'started',
      health: { ok: true, services: ['scheduler', 'web-console'] },
    });
    await waitFor(() => appendOnlyPids(state.pidLog).length === 2);
    const pids = appendOnlyPids(state.pidLog);
    expect(pids).toHaveLength(2);
    expect(fs.existsSync(state.lockPath)).toBe(true);

    await stopParent(parent.child);
    await waitFor(() => pids.every(({ pid }) => !isAlive(pid)), {
      message: 'normal owner shutdown left a prerequisite child alive',
    });
    expect(fs.existsSync(state.lockPath)).toBe(false);
  });

  test('parent crash closes the supervision channel and reaps its children', async () => {
    const state = createFixture();
    const port = await unusedPort();
    const parent = startOwnerParent(state, port);
    await parent.result;
    await waitFor(() => appendOnlyPids(state.pidLog).length === 2);
    const pids = appendOnlyPids(state.pidLog);
    expect(pids).toHaveLength(2);

    await stopParent(parent.child, 'SIGKILL');
    await waitFor(() => pids.every(({ pid }) => !isAlive(pid)), {
      message: 'crashed owner left scheduler or web-console alive',
    });
  });

  test('restart reclaims only an exactly proven stale owner and its process groups', async () => {
    const state = createFixture({ disconnectMode: 'ignore' });
    const port = await unusedPort();
    const original = startOwnerParent(state, port);
    await original.result;
    await waitFor(() => appendOnlyPids(state.pidLog).length === 2);
    const originalPids = appendOnlyPids(state.pidLog);
    const originalWeb = originalPids.find(({ service }) => service === 'web-console');
    expect(await readListeningPid(port)).toBe(originalWeb.pid);

    await stopParent(original.child, 'SIGKILL');
    expect(isAlive(originalWeb.pid)).toBe(true);

    const replacement = startOwnerParent(state, port);
    await expect(replacement.result).resolves.toMatchObject({
      type: 'started',
      health: { ok: true },
    });
    await waitFor(() => !isAlive(originalWeb.pid), {
      message: 'replacement did not reap the proven stale web-console process group',
    });
    await waitFor(() => appendOnlyPids(state.pidLog).length === 4);
    const replacementWeb = appendOnlyPids(state.pidLog).at(-1);
    expect(replacementWeb.service).toBe('web-console');
    expect(await readListeningPid(port)).toBe(replacementWeb.pid);
  });

  test('a duplicate live owner fails closed without disturbing the active instance', async () => {
    const state = createFixture();
    const port = await unusedPort();
    const active = startOwnerParent(state, port);
    await active.result;
    await waitFor(() => appendOnlyPids(state.pidLog).length === 2);
    const activeWebPid = await readListeningPid(port);

    const duplicate = startOwnerParent(state, port);
    await expect(duplicate.result).resolves.toMatchObject({
      type: 'start-error',
      error: {
        code: 'PREREQUISITE_OWNER_ACTIVE',
        evidence: { owner_pid: active.child.pid },
      },
    });
    expect(await readListeningPid(port)).toBe(activeWebPid);
    expect(isAlive(activeWebPid)).toBe(true);
  });

  test('a different release cannot claim or kill a stale owner process', async () => {
    const state = createFixture({ disconnectMode: 'ignore' });
    const port = await unusedPort();
    const original = startOwnerParent(state, port);
    await original.result;
    await waitFor(() => appendOnlyPids(state.pidLog).length === 2);
    const originalWebPid = await readListeningPid(port);
    await stopParent(original.child, 'SIGKILL');

    const otherRelease = path.join(state.directory, 'runtime', 'releases', 'release-B');
    fs.cpSync(state.releasePath, otherRelease, { recursive: true });
    const replacement = startOwnerParent({ ...state, releasePath: otherRelease }, port);
    await expect(replacement.result).resolves.toMatchObject({
      type: 'start-error',
      error: {
        code: 'PREREQUISITE_OWNER_UNPROVEN',
        evidence: { disposition: 'release_or_runtime_mismatch' },
      },
    });
    expect(await readListeningPid(port)).toBe(originalWebPid);
    expect(isAlive(originalWebPid)).toBe(true);
  });

  test('an unrelated listener produces EADDRINUSE owner evidence and is never killed', async () => {
    const state = createFixture();
    const foreign = net.createServer((socket) => socket.end('foreign'));
    await new Promise((resolve, reject) => {
      foreign.once('error', reject);
      foreign.listen(0, '127.0.0.1', resolve);
    });
    const { port } = foreign.address();

    try {
      const parent = startOwnerParent(state, port);
      await expect(parent.result).resolves.toMatchObject({
        type: 'start-error',
        error: {
          code: 'EADDRINUSE',
          message: expect.stringContaining('Owner evidence:'),
          evidence: {
            host: '127.0.0.1',
            port,
            disposition: 'foreign_or_unproven_owner',
          },
        },
      });
      expect(foreign.listening).toBe(true);
    } finally {
      await new Promise((resolve) => foreign.close(resolve));
    }
  });
});

describe('executor prerequisite port-owner evidence', () => {
  test('resolves a Linux listening socket inode to process birth evidence without lsof', () => {
    const tcp = [
      '  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode',
      '   0: 0100007F:0D80 00000000:0000 0A 00000000:00000000 00:00000000 00000000  501        0 12345 1 0000000000000000 100 0 0 10 0',
    ].join('\n');
    const evidence = inspectListeningPort({ host: '127.0.0.1', port: 3456 }, {
      platform: 'linux',
      readFileSync: (file) => {
        if (file === '/proc/net/tcp') return tcp;
        if (file === '/proc/net/tcp6') throw Object.assign(new Error('missing'), { code: 'ENOENT' });
        if (file === '/proc/777/comm') return 'node\n';
        throw new Error(`unexpected read: ${file}`);
      },
      readdirSync: (directory) => {
        if (directory === '/proc') {
          return [{ name: '777', isDirectory: () => true }];
        }
        if (directory === '/proc/777/fd') return ['4'];
        throw new Error(`unexpected readdir: ${directory}`);
      },
      readlinkSync: (file) => {
        expect(file).toBe('/proc/777/fd/4');
        return 'socket:[12345]';
      },
      inspectProcessFn: () => ({
        pid: 777,
        ppid: 1,
        pgid: 777,
        uid: 501,
        start_token: 'linux:9988',
        command: ['/usr/bin/node', '/release/skills/web-console/scripts/server.js'],
      }),
      execFileSyncFn: () => { throw new Error('lsof must not run'); },
    });

    expect(evidence).toEqual({
      inspection: 'identified',
      listeners: [{
        pid: 777,
        command_name: 'node',
        pgid: 777,
        start_token: 'linux:9988',
      }],
    });
  });
});
