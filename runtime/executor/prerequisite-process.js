import fs from 'node:fs';
import { execFileSync } from 'node:child_process';
import net from 'node:net';

function requirePid(pid) {
  if (!Number.isSafeInteger(pid) || pid <= 0) {
    throw new TypeError('process identity requires a positive integer pid');
  }
  return pid;
}

function linuxIdentity(pid, { readFileSync, statSync }) {
  const procRoot = `/proc/${pid}`;
  let stat;
  let commandBuffer;
  let owner;
  try {
    stat = readFileSync(`${procRoot}/stat`, 'utf8');
    commandBuffer = readFileSync(`${procRoot}/cmdline`);
    owner = statSync(procRoot);
  } catch (error) {
    if (['ENOENT', 'ESRCH'].includes(error?.code)) return null;
    throw error;
  }
  const commandEnd = stat.lastIndexOf(')');
  if (commandEnd < 0) {
    throw new Error(`Cannot parse process identity for PID ${pid}.`);
  }
  const fields = stat.slice(commandEnd + 2).trim().split(/\s+/);
  const ppid = Number(fields[1]);
  const pgid = Number(fields[2]);
  const startTicks = fields[19];
  if (!Number.isSafeInteger(ppid) || !Number.isSafeInteger(pgid)
    || typeof startTicks !== 'string' || !/^\d+$/.test(startTicks)) {
    throw new Error(`Cannot parse process birth identity for PID ${pid}.`);
  }
  const command = commandBuffer.toString('utf8').split('\0').filter(Boolean);
  if (command.length === 0) {
    throw new Error(`Cannot read process command identity for PID ${pid}.`);
  }
  return Object.freeze({
    pid,
    ppid,
    pgid,
    uid: owner.uid,
    start_token: `linux:${startTicks}`,
    command: Object.freeze(command),
  });
}

function posixPsIdentity(pid, execFileSyncFn) {
  let output;
  try {
    output = execFileSyncFn('ps', [
      '-ww', '-p', String(pid),
      '-o', 'uid=', '-o', 'ppid=', '-o', 'pgid=',
      '-o', 'lstart=', '-o', 'command=',
    ], {
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'ignore'],
      timeout: 5_000,
    }).trim();
  } catch (error) {
    if ([1, 'ESRCH'].includes(error?.status) || error?.code === 'ESRCH') return null;
    throw error;
  }
  if (output.length === 0) return null;
  const fields = output.split(/\s+/);
  if (fields.length < 9) {
    throw new Error(`Cannot parse process identity for PID ${pid}.`);
  }
  const uid = Number(fields[0]);
  const ppid = Number(fields[1]);
  const pgid = Number(fields[2]);
  const startToken = fields.slice(3, 8).join(' ');
  const command = fields.slice(8).join(' ');
  if (![uid, ppid, pgid].every(Number.isSafeInteger) || command.length === 0) {
    throw new Error(`Cannot parse process birth identity for PID ${pid}.`);
  }
  return Object.freeze({
    pid,
    ppid,
    pgid,
    uid,
    start_token: `ps:${startToken}`,
    command,
  });
}

export function inspectProcessIdentity(pid, {
  platform = process.platform,
  readFileSync = fs.readFileSync,
  statSync = fs.statSync,
  execFileSyncFn = execFileSync,
} = {}) {
  requirePid(pid);
  if (platform === 'linux') return linuxIdentity(pid, { readFileSync, statSync });
  if (platform === 'darwin') return posixPsIdentity(pid, execFileSyncFn);
  throw new Error(`Prerequisite process identity is unsupported on ${platform}.`);
}

export function sameProcessIdentity(expected, actual) {
  if (expected === null || actual === null
    || typeof expected !== 'object' || typeof actual !== 'object') return false;
  return expected.pid === actual.pid
    && expected.pgid === actual.pgid
    && expected.uid === actual.uid
    && expected.start_token === actual.start_token
    && JSON.stringify(expected.command) === JSON.stringify(actual.command);
}

export function signalProcessGroup(pgid, signal, { killFn = process.kill } = {}) {
  requirePid(pgid);
  if (process.platform === 'win32') {
    throw new Error('Prerequisite process-group supervision requires POSIX.');
  }
  return killFn(-pgid, signal);
}

export function processGroupIsAlive(pgid, { killFn = process.kill } = {}) {
  requirePid(pgid);
  try {
    killFn(-pgid, 0);
    return true;
  } catch (error) {
    if (error?.code === 'ESRCH') return false;
    if (error?.code === 'EPERM') return true;
    throw error;
  }
}

export async function waitForProcessGroupExit(pgid, {
  isAliveFn = processGroupIsAlive,
  timeoutMs = 5_000,
  intervalMs = 25,
} = {}) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (!isAliveFn(pgid)) return true;
    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }
  return !isAliveFn(pgid);
}

export function probeListeningPort({ host, port }) {
  return new Promise((resolve, reject) => {
    const probe = net.createServer();
    let settled = false;
    const finish = (result) => {
      if (settled) return;
      settled = true;
      probe.removeAllListeners();
      resolve(Object.freeze(result));
    };
    probe.once('error', (error) => {
      if (error?.code === 'EADDRINUSE') {
        finish({ available: false, code: 'EADDRINUSE' });
      } else {
        reject(error);
      }
    });
    probe.listen(port, host, () => {
      probe.close(() => finish({ available: true, code: null }));
    });
  });
}

function parseLsofListeners(output, inspectProcessFn) {
  const listeners = [];
  let current = null;
  for (const line of output.split('\n')) {
    if (line.startsWith('p')) {
      if (current !== null) listeners.push(current);
      current = { pid: Number(line.slice(1)), command_name: null };
    } else if (current !== null && line.startsWith('c')) {
      current.command_name = line.slice(1) || null;
    }
  }
  if (current !== null) listeners.push(current);
  return listeners.filter(({ pid }) => Number.isSafeInteger(pid) && pid > 0)
    .map((listener) => {
      let identity = null;
      try { identity = inspectProcessFn(listener.pid); } catch {}
      return Object.freeze({
        pid: listener.pid,
        command_name: listener.command_name,
        pgid: identity?.pgid ?? null,
        start_token: identity?.start_token ?? null,
      });
    });
}

function linuxListeningSocketInodes(port, readFileSync) {
  const inodes = new Set();
  for (const table of ['/proc/net/tcp', '/proc/net/tcp6']) {
    let content;
    try { content = readFileSync(table, 'utf8'); } catch { continue; }
    for (const line of content.trim().split('\n').slice(1)) {
      const fields = line.trim().split(/\s+/);
      const localAddress = fields[1] ?? '';
      const state = fields[3];
      const inode = fields[9];
      const encodedPort = localAddress.slice(localAddress.lastIndexOf(':') + 1);
      if (state === '0A' && Number.parseInt(encodedPort, 16) === port
        && typeof inode === 'string' && /^\d+$/.test(inode)) {
        inodes.add(inode);
      }
    }
  }
  return inodes;
}

function inspectLinuxListeningPort(port, {
  readdirSync,
  readFileSync,
  readlinkSync,
  inspectProcessFn,
}) {
  const inodes = linuxListeningSocketInodes(port, readFileSync);
  if (inodes.size === 0) return null;
  const listeners = [];
  let entries;
  try { entries = readdirSync('/proc', { withFileTypes: true }); } catch { return null; }
  for (const entry of entries) {
    if (!entry.isDirectory() || !/^\d+$/.test(entry.name)) continue;
    const pid = Number(entry.name);
    let descriptors;
    try { descriptors = readdirSync(`/proc/${pid}/fd`); } catch { continue; }
    let ownsListener = false;
    for (const descriptor of descriptors) {
      let target;
      try { target = readlinkSync(`/proc/${pid}/fd/${descriptor}`); } catch { continue; }
      const match = target.match(/^socket:\[(\d+)]$/);
      if (match && inodes.has(match[1])) {
        ownsListener = true;
        break;
      }
    }
    if (!ownsListener) continue;
    let identity = null;
    let commandName = null;
    try { identity = inspectProcessFn(pid); } catch {}
    try { commandName = readFileSync(`/proc/${pid}/comm`, 'utf8').trim() || null; } catch {}
    listeners.push(Object.freeze({
      pid,
      command_name: commandName,
      pgid: identity?.pgid ?? null,
      start_token: identity?.start_token ?? null,
    }));
  }
  return Object.freeze({
    inspection: listeners.length > 0 ? 'identified' : 'owner_unavailable',
    listeners: Object.freeze(listeners),
  });
}

export function inspectListeningPort({ host, port }, {
  platform = process.platform,
  execFileSyncFn = execFileSync,
  inspectProcessFn = inspectProcessIdentity,
  readdirSync = fs.readdirSync,
  readFileSync = fs.readFileSync,
  readlinkSync = fs.readlinkSync,
} = {}) {
  if (platform === 'linux') {
    const procEvidence = inspectLinuxListeningPort(port, {
      readdirSync,
      readFileSync,
      readlinkSync,
      inspectProcessFn,
    });
    if (procEvidence !== null) return procEvidence;
  }
  try {
    const output = execFileSyncFn('lsof', [
      '-nP', `-iTCP@${host}:${port}`, '-sTCP:LISTEN', '-Fpc',
    ], {
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'ignore'],
      timeout: 5_000,
    });
    const listeners = parseLsofListeners(output, inspectProcessFn);
    return Object.freeze({
      inspection: listeners.length > 0 ? 'identified' : 'no_listener_identity',
      listeners: Object.freeze(listeners),
    });
  } catch (error) {
    return Object.freeze({
      inspection: error?.code === 'ENOENT' ? 'lsof_unavailable' : 'owner_unavailable',
      listeners: Object.freeze([]),
    });
  }
}
