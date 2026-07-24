import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { spawn } from 'node:child_process';

import {
  inspectListeningPort,
  inspectProcessIdentity,
  probeListeningPort,
  processGroupIsAlive,
  sameProcessIdentity,
  signalProcessGroup,
  waitForProcessGroupExit,
} from './prerequisite-process.js';

const OWNER_CONTRACT = 'zylos.prerequisite-owner@1';
const CHILD_CONTRACT = 'zylos.prerequisite-child@1';

function lifecycleError(code, message, evidence) {
  const error = new Error(message);
  error.code = code;
  error.evidence = Object.freeze(evidence);
  return error;
}

function atomicWriteJson(file, document) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  const temporary = `${file}.${crypto.randomUUID()}.partial`;
  const descriptor = fs.openSync(temporary, 'wx', 0o600);
  try {
    fs.writeFileSync(descriptor, `${JSON.stringify(document)}\n`);
    fs.fsyncSync(descriptor);
  } finally {
    fs.closeSync(descriptor);
  }
  fs.renameSync(temporary, file);
  const directory = fs.openSync(path.dirname(file), 'r');
  try { fs.fsyncSync(directory); } finally { fs.closeSync(directory); }
}

function validProcessIdentity(identity) {
  return identity !== null && typeof identity === 'object'
    && Number.isSafeInteger(identity.pid) && identity.pid > 0
    && Number.isSafeInteger(identity.pgid) && identity.pgid > 0
    && Number.isSafeInteger(identity.uid) && identity.uid >= 0
    && typeof identity.start_token === 'string' && identity.start_token.length > 0
    && (typeof identity.command === 'string'
      || (Array.isArray(identity.command)
        && identity.command.length > 0
        && identity.command.every((value) => typeof value === 'string')));
}

function validServiceRecord(service) {
  return service !== null && typeof service === 'object'
    && typeof service.name === 'string' && service.name.length > 0
    && typeof service.command === 'string' && service.command.length > 0
    && Array.isArray(service.args)
    && service.args.every((value) => typeof value === 'string')
    && (service.script === null || typeof service.script === 'string')
    && typeof service.supervised_ipc === 'boolean'
    && validProcessIdentity(service.process);
}

function readOwnerRecord(ownerFile) {
  let record;
  try {
    record = JSON.parse(fs.readFileSync(ownerFile, 'utf8'));
  } catch (error) {
    throw lifecycleError(
      'PREREQUISITE_OWNER_UNPROVEN',
      `Executor prerequisite owner record is unreadable: ${error.message}`,
      { owner_file: ownerFile, disposition: 'fail_closed' },
    );
  }
  if (record?.contract !== OWNER_CONTRACT
    || typeof record.owner_id !== 'string' || record.owner_id.length === 0
    || typeof record.zylos_dir !== 'string'
    || typeof record.release_path !== 'string'
    || typeof record.created_at !== 'string' || record.created_at.length === 0
    || !validProcessIdentity(record.owner_process)
    || !Array.isArray(record.services)
    || !record.services.every(validServiceRecord)) {
    throw lifecycleError(
      'PREREQUISITE_OWNER_UNPROVEN',
      'Executor prerequisite owner record is malformed.',
      { owner_file: ownerFile, disposition: 'fail_closed' },
    );
  }
  return record;
}

function requireSafeRoot(value, name) {
  if (typeof value !== 'string' || !path.isAbsolute(value)
    || path.parse(value).root === value) {
    throw new TypeError(`${name} must be an explicit absolute non-root path`);
  }
  return path.resolve(value);
}

function webConsoleEndpoint(environment) {
  const port = Number(environment.WEB_CONSOLE_PORT || 3456);
  const host = environment.WEB_CONSOLE_BIND || '127.0.0.1';
  if (!Number.isSafeInteger(port) || port < 1 || port > 65_535) {
    throw new TypeError('WEB_CONSOLE_PORT must be an integer from 1 through 65535');
  }
  if (typeof host !== 'string' || host.length === 0) {
    throw new TypeError('WEB_CONSOLE_BIND must be a non-empty host');
  }
  return Object.freeze({ host, port });
}

function childEvidence(descriptor, child, identity) {
  return Object.freeze({
    name: descriptor.name,
    command: descriptor.command,
    args: Object.freeze([...descriptor.args]),
    process: identity,
    script: descriptor.args[0] ?? null,
    supervised_ipc: descriptor.supervisedIpc,
  });
}

function sanitizeOwnerEvidence(record, disposition) {
  return Object.freeze({
    disposition,
    owner_id: record.owner_id,
    owner_pid: record.owner_process.pid,
    release_path: record.release_path,
    services: Object.freeze(record.services.map(({ name, process: identity }) => Object.freeze({
      name,
      pid: identity?.pid ?? null,
      pgid: identity?.pgid ?? null,
      start_token: identity?.start_token ?? null,
    }))),
  });
}

function listenerEvidenceText(listener) {
  return JSON.stringify({
    inspection: listener.inspection,
    listeners: listener.listeners,
  });
}

async function waitForChildReady(child, descriptor, timeoutMs) {
  if (!descriptor.supervisedIpc) return;
  await new Promise((resolve, reject) => {
    let settled = false;
    const finish = (callback, value) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      child.off('error', onError);
      child.off('exit', onExit);
      child.off('message', onMessage);
      callback(value);
    };
    const onError = (error) => finish(reject, error);
    const onExit = (code, signal) => finish(
      reject,
      lifecycleError(
        'PREREQUISITE_START_FAILED',
        `Executor prerequisite ${descriptor.name} exited before readiness (${code ?? signal}).`,
        {
          service: descriptor.name,
          pid: child.pid ?? null,
          exit_code: code,
          signal,
          disposition: 'start_failed',
        },
      ),
    );
    const onMessage = (message) => {
      if (message?.contract !== CHILD_CONTRACT
        || message.service !== descriptor.name
        || message.pid !== child.pid) return;
      if (message.type === 'ready') {
        finish(resolve);
      } else if (message.type === 'error') {
        const errorCode = typeof message.code === 'string'
          ? message.code : 'PREREQUISITE_START_FAILED';
        finish(
          reject,
          lifecycleError(
            errorCode,
            `Executor prerequisite ${descriptor.name} failed: ${message.message ?? errorCode}`,
            {
              service: descriptor.name,
              pid: child.pid ?? null,
              host: message.host ?? null,
              port: message.port ?? null,
              disposition: 'start_failed',
            },
          ),
        );
      }
    };
    const timer = setTimeout(() => finish(
      reject,
      lifecycleError(
        'PREREQUISITE_START_TIMEOUT',
        `Executor prerequisite ${descriptor.name} did not report readiness.`,
        {
          service: descriptor.name,
          pid: child.pid ?? null,
          timeout_ms: timeoutMs,
          disposition: 'start_failed',
        },
      ),
    ), timeoutMs);
    timer.unref?.();
    child.once('error', onError);
    child.once('exit', onExit);
    child.on('message', onMessage);
  });
}

async function terminateProcessGroup(service, {
  graceMs,
  inspectProcessFn,
  signalProcessGroupFn,
  processGroupIsAliveFn,
  waitForProcessGroupExitFn,
}) {
  const pgid = service.process.pgid;
  const actual = inspectProcessFn(service.process.pid);
  if (!sameProcessIdentity(service.process, actual)) {
    if (actual === null && !processGroupIsAliveFn(pgid)) return;
    throw lifecycleError(
      'PREREQUISITE_PROCESS_UNPROVEN',
      `Executor prerequisite ${service.name} process identity changed before cleanup.`,
      {
        service: service.name,
        pid: service.process.pid,
        pgid,
        disposition: 'fail_closed',
      },
    );
  }
  try { signalProcessGroupFn(pgid, 'SIGTERM'); } catch (error) {
    if (error?.code !== 'ESRCH') throw error;
  }
  if (await waitForProcessGroupExitFn(pgid, {
    isAliveFn: processGroupIsAliveFn,
    timeoutMs: graceMs,
  })) return;
  try { signalProcessGroupFn(pgid, 'SIGKILL'); } catch (error) {
    if (error?.code !== 'ESRCH') throw error;
  }
  if (!await waitForProcessGroupExitFn(pgid, {
    isAliveFn: processGroupIsAliveFn,
    timeoutMs: graceMs,
  })) {
    throw lifecycleError(
      'PREREQUISITE_REAP_FAILED',
      `Executor prerequisite ${service.name} process group ${pgid} did not exit.`,
      {
        service: service.name,
        pid: service.process.pid,
        pgid,
        disposition: 'fail_closed',
      },
    );
  }
}

export function createExecutorPrerequisiteOwner({
  zylosDir,
  releasePath,
  spawnFn = spawn,
  existsSync = fs.existsSync,
  environment = process.env,
  closeGraceMs = 5_000,
  startTimeoutMs = 10_000,
  ownerId = crypto.randomUUID(),
  now = () => new Date().toISOString(),
  inspectProcessFn = inspectProcessIdentity,
  probePortFn = probeListeningPort,
  inspectPortOwnerFn = inspectListeningPort,
  signalProcessGroupFn = signalProcessGroup,
  processGroupIsAliveFn = processGroupIsAlive,
  waitForProcessGroupExitFn = waitForProcessGroupExit,
} = {}) {
  const runtimeRoot = requireSafeRoot(zylosDir, 'zylosDir');
  const releaseRoot = requireSafeRoot(releasePath, 'releasePath');
  if (process.platform === 'win32') {
    throw new Error('Executor prerequisite process-group supervision requires POSIX.');
  }
  const runtimeDirectory = path.join(runtimeRoot, 'runtime');
  const lockPath = path.join(runtimeDirectory, 'prerequisite-owner.lock');
  const ownerFile = path.join(lockPath, 'owner.json');
  const endpoint = webConsoleEndpoint(environment);
  const children = new Map();
  let ownerRecord = null;
  let started = false;
  let closing = false;
  let failure = null;
  let recoveredOwner = null;

  function descriptors() {
    const skills = path.join(releaseRoot, 'skills');
    const values = [
      {
        name: 'scheduler',
        command: process.execPath,
        args: [path.join(skills, 'scheduler', 'scripts', 'daemon.js')],
        cwd: runtimeRoot,
        supervisedIpc: true,
      },
      {
        name: 'web-console',
        command: process.execPath,
        args: [path.join(skills, 'web-console', 'scripts', 'server.js')],
        cwd: runtimeRoot,
        supervisedIpc: true,
      },
    ];
    const caddy = path.join(runtimeRoot, 'bin', 'caddy');
    const caddyfile = path.join(runtimeRoot, 'http', 'Caddyfile');
    if (existsSync(caddy) && existsSync(caddyfile)) {
      values.push({
        name: 'caddy',
        command: caddy,
        args: ['run', '--config', caddyfile, '--adapter', 'caddyfile'],
        cwd: runtimeRoot,
        supervisedIpc: false,
      });
    }
    return values.filter(({ command, args }) => (
      command === process.execPath ? existsSync(args[0]) : existsSync(command)
    ));
  }

  function writeCurrentOwner() {
    if (ownerRecord === null) return;
    const current = readOwnerRecord(ownerFile);
    if (current.owner_id !== ownerId) {
      throw lifecycleError(
        'PREREQUISITE_OWNER_CHANGED',
        'Executor prerequisite ownership changed before its record update.',
        {
          expected_owner_id: ownerId,
          actual_owner_id: current.owner_id,
          disposition: 'fail_closed',
        },
      );
    }
    atomicWriteJson(ownerFile, ownerRecord);
  }

  async function reclaimStaleOwner(record) {
    if (record.zylos_dir !== runtimeRoot || record.release_path !== releaseRoot) {
      throw lifecycleError(
        'PREREQUISITE_OWNER_UNPROVEN',
        'Existing prerequisite owner belongs to a different runtime or release.',
        sanitizeOwnerEvidence(record, 'release_or_runtime_mismatch'),
      );
    }
    let actualOwner;
    try {
      actualOwner = inspectProcessFn(record.owner_process.pid);
    } catch (error) {
      throw lifecycleError(
        'PREREQUISITE_OWNER_UNPROVEN',
        `Existing prerequisite owner process cannot be inspected: ${error.message}`,
        sanitizeOwnerEvidence(record, 'owner_probe_failed'),
      );
    }
    if (sameProcessIdentity(record.owner_process, actualOwner)) {
      throw lifecycleError(
        'PREREQUISITE_OWNER_ACTIVE',
        `Executor prerequisites are already owned by live PID ${record.owner_process.pid}.`,
        sanitizeOwnerEvidence(record, 'live_owner'),
      );
    }

    const verified = [];
    const unproven = [];
    const expectedDescriptors = new Map(
      descriptors().map((descriptor) => [descriptor.name, descriptor]),
    );
    for (const service of record.services) {
      const expected = expectedDescriptors.get(service.name);
      const descriptorMatches = expected !== undefined
        && service.command === expected.command
        && service.script === (expected.args[0] ?? null)
        && JSON.stringify(service.args) === JSON.stringify(expected.args)
        && service.supervised_ipc === expected.supervisedIpc;
      if (!descriptorMatches) {
        unproven.push({
          name: service.name,
          pid: service.process?.pid ?? null,
          reason: 'service_descriptor_mismatch',
        });
        continue;
      }
      let actual = null;
      try { actual = inspectProcessFn(service.process?.pid); } catch (error) {
        unproven.push({
          name: service.name,
          pid: service.process?.pid ?? null,
          reason: `identity_probe_failed:${error.message}`,
        });
        continue;
      }
      if (actual === null) {
        if (processGroupIsAliveFn(service.process.pgid)) {
          unproven.push({
            name: service.name,
            pid: service.process.pid,
            reason: 'process_group_live_without_recorded_leader',
          });
        }
        continue;
      }
      if (sameProcessIdentity(service.process, actual)
        && service.process.pgid === service.process.pid) {
        verified.push(service);
      } else {
        unproven.push({
          name: service.name,
          pid: service.process?.pid ?? null,
          reason: 'process_identity_mismatch',
        });
      }
    }
    if (unproven.length > 0) {
      throw lifecycleError(
        'PREREQUISITE_OWNER_UNPROVEN',
        'Stale prerequisite owner has live processes whose identity cannot be proven.',
        {
          ...sanitizeOwnerEvidence(record, 'unproven_live_process'),
          unproven: Object.freeze(unproven.map(Object.freeze)),
        },
      );
    }
    for (const service of verified) {
      await terminateProcessGroup(service, {
        graceMs: closeGraceMs,
        inspectProcessFn,
        signalProcessGroupFn,
        processGroupIsAliveFn,
        waitForProcessGroupExitFn,
      });
    }

    const stalePath = `${lockPath}.stale-${crypto.randomUUID()}`;
    try {
      fs.renameSync(lockPath, stalePath);
    } catch (error) {
      if (error?.code === 'ENOENT') return false;
      throw error;
    }
    fs.rmSync(stalePath, { recursive: true, force: true });
    recoveredOwner = sanitizeOwnerEvidence(record, 'reaped_stale_owner');
    return true;
  }

  async function acquireOwnership() {
    fs.mkdirSync(runtimeDirectory, { recursive: true });
    for (let attempt = 0; attempt < 3; attempt += 1) {
      const candidate = `${lockPath}.acquiring-${ownerId}`;
      fs.rmSync(candidate, { recursive: true, force: true });
      fs.mkdirSync(candidate, { mode: 0o700 });
      const ownerProcess = inspectProcessFn(process.pid);
      if (ownerProcess === null) {
        fs.rmSync(candidate, { recursive: true, force: true });
        throw new Error('Cannot inspect current prerequisite owner process identity.');
      }
      const candidateRecord = Object.freeze({
        contract: OWNER_CONTRACT,
        owner_id: ownerId,
        zylos_dir: runtimeRoot,
        release_path: releaseRoot,
        created_at: now(),
        owner_process: ownerProcess,
        services: Object.freeze([]),
      });
      atomicWriteJson(path.join(candidate, 'owner.json'), candidateRecord);
      try {
        fs.renameSync(candidate, lockPath);
        ownerRecord = candidateRecord;
        return;
      } catch (error) {
        fs.rmSync(candidate, { recursive: true, force: true });
        if (!['EEXIST', 'ENOTEMPTY'].includes(error?.code)) throw error;
      }
      const existing = readOwnerRecord(ownerFile);
      if (!await reclaimStaleOwner(existing)) continue;
    }
    throw lifecycleError(
      'PREREQUISITE_OWNER_RACE',
      'Executor prerequisite ownership could not be acquired after reconciliation.',
      { lock_path: lockPath, disposition: 'fail_closed' },
    );
  }

  async function releaseOwnership() {
    if (ownerRecord === null || !fs.existsSync(lockPath)) {
      ownerRecord = null;
      return;
    }
    const current = readOwnerRecord(ownerFile);
    if (current.owner_id !== ownerId) {
      throw lifecycleError(
        'PREREQUISITE_OWNER_CHANGED',
        'Executor prerequisite ownership changed before release.',
        {
          expected_owner_id: ownerId,
          actual_owner_id: current.owner_id,
          disposition: 'fail_closed',
        },
      );
    }
    const releasedPath = `${lockPath}.released-${ownerId}`;
    fs.renameSync(lockPath, releasedPath);
    fs.rmSync(releasedPath, { recursive: true, force: true });
    ownerRecord = null;
  }

  async function assertWebConsolePortAvailable() {
    const probe = await probePortFn(endpoint);
    if (probe.available) return;
    const listener = inspectPortOwnerFn(endpoint);
    throw lifecycleError(
      'EADDRINUSE',
      `Web Console cannot bind ${endpoint.host}:${endpoint.port}; listener ownership is not proven. `
        + `Owner evidence: ${listenerEvidenceText(listener)}`,
      {
        ...endpoint,
        disposition: 'foreign_or_unproven_owner',
        listener,
      },
    );
  }

  async function spawnDescriptor(descriptor) {
    const child = spawnFn(descriptor.command, descriptor.args, {
      cwd: descriptor.cwd,
      detached: true,
      env: {
        ...environment,
        NODE_ENV: 'production',
        ZYLOS_DIR: runtimeRoot,
        ZYLOS_PREREQUISITE_OWNER_ID: ownerId,
        ZYLOS_PREREQUISITE_RELEASE_PATH: releaseRoot,
        ZYLOS_PREREQUISITE_SERVICE: descriptor.name,
      },
      stdio: descriptor.supervisedIpc
        ? ['ignore', 'inherit', 'inherit', 'ipc']
        : 'inherit',
    });
    await new Promise((resolve, reject) => {
      const onSpawn = () => {
        child.off('error', onError);
        resolve();
      };
      const onError = (error) => {
        child.off('spawn', onSpawn);
        reject(error);
      };
      child.once('spawn', onSpawn);
      child.once('error', onError);
    });
    if (!Number.isSafeInteger(child.pid) || child.pid <= 0) {
      throw new Error(`Executor prerequisite ${descriptor.name} has no process identity.`);
    }
    const identity = inspectProcessFn(child.pid);
    if (identity === null || identity.pgid !== child.pid) {
      throw lifecycleError(
        'PREREQUISITE_PROCESS_UNPROVEN',
        `Executor prerequisite ${descriptor.name} did not start as an isolated process group.`,
        {
          service: descriptor.name,
          pid: child.pid,
          pgid: identity?.pgid ?? null,
          disposition: 'fail_closed',
        },
      );
    }
    const service = childEvidence(descriptor, child, identity);
    children.set(descriptor.name, { child, service });
    ownerRecord = Object.freeze({
      ...ownerRecord,
      services: Object.freeze([
        ...ownerRecord.services.filter(({ name }) => name !== descriptor.name),
        service,
      ]),
    });
    writeCurrentOwner();
    child.once('exit', (code, signal) => {
      if (!closing) {
        failure = lifecycleError(
          'PREREQUISITE_EXITED',
          `Executor prerequisite ${descriptor.name} exited ${code ?? signal}.`,
          {
            service: descriptor.name,
            pid: child.pid,
            pgid: identity.pgid,
            exit_code: code,
            signal,
            disposition: 'degraded',
          },
        );
      }
    });
    await waitForChildReady(child, descriptor, startTimeoutMs);
  }

  async function acquire() {
    if (closing) {
      throw new Error('Executor prerequisite owner is closing.');
    }
    if (ownerRecord !== null) return health();
    await acquireOwnership();
    return health();
  }

  async function start() {
    if (started) return health();
    await acquire();
    try {
      await assertWebConsolePortAvailable();
      for (const descriptor of descriptors()) await spawnDescriptor(descriptor);
      started = true;
      return health();
    } catch (error) {
      let reportedError = error;
      if (error?.code === 'EADDRINUSE' && error?.evidence?.listener === undefined) {
        const listener = inspectPortOwnerFn(endpoint);
        const evidence = Object.freeze({
          ...error.evidence,
          ...endpoint,
          disposition: 'foreign_or_unproven_owner',
          listener,
        });
        reportedError = lifecycleError(
          'EADDRINUSE',
          `Web Console cannot bind ${endpoint.host}:${endpoint.port}; listener ownership is not proven. `
            + `Owner evidence: ${listenerEvidenceText(listener)}`,
          evidence,
        );
      }
      failure = reportedError;
      await close();
      throw reportedError;
    }
  }

  function health() {
    return Object.freeze({
      ok: failure === null,
      error: failure?.message ?? null,
      error_code: failure?.code ?? null,
      services: Object.freeze([...children.keys()]),
      recovered_owner: recoveredOwner,
    });
  }

  async function close() {
    if (closing) return;
    closing = true;
    const failures = [];
    const services = [...children.values()].map(({ service }) => service);
    for (const service of services.reverse()) {
      try {
        await terminateProcessGroup(service, {
          graceMs: closeGraceMs,
          inspectProcessFn,
          signalProcessGroupFn,
          processGroupIsAliveFn,
          waitForProcessGroupExitFn,
        });
      } catch (error) {
        failures.push(error);
      }
    }
    children.clear();
    if (failures.length === 0) {
      try { await releaseOwnership(); } catch (error) { failures.push(error); }
    }
    if (failures.length === 1) throw failures[0];
    if (failures.length > 1) {
      throw new AggregateError(failures, 'Executor prerequisite shutdown failed.');
    }
  }

  return Object.freeze({ acquire, close, health, start });
}
