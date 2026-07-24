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
const CHILD_CONTRACT = 'zylos.prerequisite-child@1';
const temporaryDirectories = [];
const parents = [];

function isAlive(pid) {
  if (!Number.isSafeInteger(pid) || pid <= 0) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    return error?.code === 'EPERM';
  }
}

async function waitFor(predicate, message, timeoutMs = 2_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  throw new Error(message);
}

function recordedPids(pidLog) {
  try {
    return fs.readFileSync(pidLog, 'utf8').trim().split('\n')
      .filter(Boolean)
      .map((line) => {
        const [service, pid] = line.split(':');
        return { service, pid: Number(pid) };
      });
  } catch {
    return [];
  }
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

function fixture({ disconnectMode = 'exit' } = {}) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'zylos-prerequisite-lifecycle-'));
  temporaryDirectories.push(directory);
  const releasePath = path.join(directory, 'runtime', 'releases', 'release-A');
  const schedulerPath = path.join(releasePath, 'skills', 'scheduler', 'scripts', 'daemon.js');
  const webConsolePath = path.join(releasePath, 'skills', 'web-console', 'scripts', 'server.js');
  const parentPath = path.join(directory, 'owner-parent.mjs');
  const pidLog = path.join(directory, 'prerequisite-pids.log');
  fs.mkdirSync(path.dirname(schedulerPath), { recursive: true });
  fs.mkdirSync(path.dirname(webConsolePath), { recursive: true });

  fs.writeFileSync(schedulerPath, `
import fs from 'node:fs';
fs.appendFileSync(process.env.PREREQUISITE_PID_LOG, \`scheduler:\${process.pid}\\n\`);
process.send?.({
  contract: ${JSON.stringify(CHILD_CONTRACT)},
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
const server = net.createServer((socket) => socket.end(String(process.pid)));
server.once('error', (error) => {
  process.send?.({
    contract: ${JSON.stringify(CHILD_CONTRACT)},
    type: 'error',
    service: 'web-console',
    pid: process.pid,
    code: error.code,
    message: error.message,
  });
  process.exitCode = 1;
});
server.listen(port, '127.0.0.1', () => {
  fs.appendFileSync(process.env.PREREQUISITE_PID_LOG, \`web-console:\${process.pid}\\n\`);
  process.send?.({
    contract: ${JSON.stringify(CHILD_CONTRACT)},
    type: 'ready',
    service: 'web-console',
    pid: process.pid,
    port,
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
  fs.writeFileSync(parentPath, `
import { createExecutorPrerequisiteOwner } from ${JSON.stringify(OWNER_MODULE_URL)};
const owner = createExecutorPrerequisiteOwner({
  zylosDir: process.env.ZYLOS_DIR,
  releasePath: process.env.PREREQUISITE_RELEASE_PATH,
  closeGraceMs: 500,
});
try {
  const health = await owner.start();
  process.send?.({ type: 'started', health });
  setInterval(() => {}, 1_000);
} catch (error) {
  process.send?.({ type: 'start-error', code: error.code ?? null, message: error.message });
  process.exitCode = 1;
}
`);
  return {
    directory,
    disconnectMode,
    lockPath: path.join(directory, 'runtime', 'prerequisite-owner.lock'),
    parentPath,
    pidLog,
    releasePath,
  };
}

function startParent(state, port) {
  const child = spawn(process.execPath, [state.parentPath], {
    env: {
      ...process.env,
      PREREQUISITE_DISCONNECT_MODE: state.disconnectMode,
      PREREQUISITE_PID_LOG: state.pidLog,
      PREREQUISITE_RELEASE_PATH: state.releasePath,
      WEB_CONSOLE_BIND: '127.0.0.1',
      WEB_CONSOLE_PORT: String(port),
      ZYLOS_DIR: state.directory,
    },
    stdio: ['ignore', 'pipe', 'pipe', 'ipc'],
  });
  parents.push(child);
  let stderr = '';
  child.stderr.on('data', (chunk) => { stderr += chunk; });
  const started = new Promise((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new Error(`owner parent timed out; stderr=${stderr}`)),
      5_000,
    );
    child.once('error', (error) => {
      clearTimeout(timer);
      reject(error);
    });
    child.on('message', (message) => {
      if (!['started', 'start-error'].includes(message?.type)) return;
      clearTimeout(timer);
      resolve(message);
    });
  });
  return { child, started };
}

async function killParent(child) {
  if (!isAlive(child.pid)) return;
  const exited = new Promise((resolve) => child.once('exit', resolve));
  child.kill('SIGKILL');
  await exited;
}

afterEach(async () => {
  await Promise.all(parents.splice(0).map(async (child) => {
    if (child.exitCode !== null || child.signalCode !== null || !isAlive(child.pid)) return;
    const exited = new Promise((resolve) => child.once('exit', resolve));
    child.kill('SIGKILL');
    await Promise.race([
      exited,
      new Promise((resolve) => setTimeout(resolve, 1_000)),
    ]);
  }));
  for (const directory of temporaryDirectories) {
    for (const { pid } of recordedPids(path.join(directory, 'prerequisite-pids.log'))) {
      if (isAlive(pid)) {
        try { process.kill(pid, 'SIGKILL'); } catch {}
      }
    }
  }
  await new Promise((resolve) => setTimeout(resolve, 50));
  for (const directory of temporaryDirectories.splice(0)) {
    fs.rmSync(directory, { recursive: true, force: true });
  }
});

describe('executor prerequisite process ownership', () => {
  test('parent crash closes the supervision channel and reaps prerequisite children', async () => {
    const state = fixture();
    const port = await unusedPort();
    const parent = startParent(state, port);
    await expect(parent.started).resolves.toMatchObject({
      type: 'started',
      health: { ok: true, services: ['scheduler', 'web-console'] },
    });
    await waitFor(
      () => recordedPids(state.pidLog).length === 2,
      'prerequisite children did not start',
    );
    const childPids = recordedPids(state.pidLog).map(({ pid }) => pid);

    await killParent(parent.child);
    await waitFor(
      () => childPids.every((pid) => !isAlive(pid)),
      'crashed owner left scheduler or web-console alive',
    );
  });

  test('restart reclaims a proven stale owner before binding the web-console port again', async () => {
    const state = fixture({ disconnectMode: 'ignore' });
    const port = await unusedPort();
    const original = startParent(state, port);
    await expect(original.started).resolves.toMatchObject({
      type: 'started',
      health: { ok: true },
    });
    await waitFor(
      () => recordedPids(state.pidLog).length === 2,
      'original prerequisite children did not start',
    );
    const originalWebPid = await readListeningPid(port);

    await killParent(original.child);
    expect(isAlive(originalWebPid)).toBe(true);

    const replacement = startParent(state, port);
    await expect(replacement.started).resolves.toMatchObject({
      type: 'started',
      health: { ok: true },
    });
    await waitFor(
      () => !isAlive(originalWebPid),
      'replacement did not reap the proven stale web-console process',
    );
    await waitFor(
      () => recordedPids(state.pidLog).length === 4,
      'replacement prerequisite children did not start',
    );
    expect(await readListeningPid(port)).not.toBe(originalWebPid);
    expect(fs.existsSync(state.lockPath)).toBe(true);
  });

  test('a duplicate live owner fails closed without disturbing the active instance', async () => {
    const state = fixture();
    const port = await unusedPort();
    const active = startParent(state, port);
    await expect(active.started).resolves.toMatchObject({
      type: 'started',
      health: { ok: true },
    });
    await waitFor(
      () => recordedPids(state.pidLog).length === 2,
      'active prerequisite children did not start',
    );
    const activeWebPid = await readListeningPid(port);

    const duplicate = startParent(state, port);
    await expect(duplicate.started).resolves.toMatchObject({
      type: 'start-error',
      code: 'PREREQUISITE_OWNER_ACTIVE',
    });
    expect(await readListeningPid(port)).toBe(activeWebPid);
    expect(isAlive(activeWebPid)).toBe(true);
  });

  test('an unrelated listener returns owner evidence and is never killed', async () => {
    const state = fixture();
    const foreign = net.createServer((socket) => socket.end('foreign'));
    await new Promise((resolve, reject) => {
      foreign.once('error', reject);
      foreign.listen(0, '127.0.0.1', resolve);
    });
    const { port } = foreign.address();

    try {
      const parent = startParent(state, port);
      await expect(parent.started).resolves.toMatchObject({
        type: 'start-error',
        code: 'EADDRINUSE',
        message: expect.stringContaining('Owner evidence:'),
      });
      expect(foreign.listening).toBe(true);
    } finally {
      await new Promise((resolve) => foreign.close(resolve));
    }
  });
});

describe('executor prerequisite Linux port-owner evidence', () => {
  test('resolves a listening socket inode without depending on lsof', () => {
    const tcp = [
      '  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode',
      '   0: 0100007F:0D80 00000000:0000 0A 00000000:00000000 00:00000000 00000000  501        0 12345',
    ].join('\n');
    const evidence = inspectListeningPort({ host: '127.0.0.1', port: 3456 }, {
      platform: 'linux',
      readFileSync: (file) => {
        if (file === '/proc/net/tcp') return tcp;
        if (file === '/proc/net/tcp6') {
          throw Object.assign(new Error('missing'), { code: 'ENOENT' });
        }
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
      readlinkSync: () => 'socket:[12345]',
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
