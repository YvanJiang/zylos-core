import crypto from 'node:crypto';
import fs from 'node:fs';
import net from 'node:net';
import path from 'node:path';

import { createExecutorService } from './service.js';

const MAX_REQUEST_BYTES = 64 * 1024;
const READ_ONLY_ACTIONS = new Set([
  'get_background_task',
  'health',
  'resolve_interaction',
]);
const PEER_DISCONNECT_ERROR_CODES = new Set([
  'ECONNABORTED',
  'ECONNRESET',
  'ENOTCONN',
  'EPIPE',
]);

function reportPollError(error) {
  console.error('[zylos-executor] Background poll failed.', error);
}

function matchesSocketIdentity(stat, identity) {
  return stat.dev === identity.dev
    && stat.ino === identity.ino
    && stat.ctimeNs === identity.ctimeNs
    && stat.birthtimeNs === identity.birthtimeNs;
}

function requireSocketPath(socketPath) {
  if (typeof socketPath !== 'string' || !path.isAbsolute(socketPath)) {
    throw new TypeError('socketPath must be an absolute path');
  }
  if (path.parse(socketPath).root === socketPath) {
    throw new TypeError('socketPath must not be a filesystem root');
  }
  return socketPath;
}

function removeOwnedSocket(socketPath, identity = null) {
  let stat;
  try {
    stat = fs.lstatSync(socketPath, { bigint: true });
  } catch (error) {
    if (error?.code === 'ENOENT') return;
    throw error;
  }
  if (!stat.isSocket()) {
    throw new Error(`Refusing to replace non-socket control path: ${socketPath}`);
  }
  if (identity !== null && !matchesSocketIdentity(stat, identity)) {
    return false;
  }
  fs.unlinkSync(socketPath);
  return true;
}

async function prepareSocketPath(socketPath, probeControl) {
  let stat;
  try {
    stat = fs.lstatSync(socketPath, { bigint: true });
  } catch (error) {
    if (error?.code === 'ENOENT') return;
    throw error;
  }
  if (!stat.isSocket()) {
    throw new Error(`Refusing to replace non-socket control path: ${socketPath}`);
  }
  const observedIdentity = Object.freeze({
    dev: stat.dev,
    ino: stat.ino,
    ctimeNs: stat.ctimeNs,
    birthtimeNs: stat.birthtimeNs,
  });
  try {
    await probeControl(socketPath, { action: 'health' }, { timeoutMs: 500 });
    throw new Error(`Executor service control socket is already active: ${socketPath}`);
  } catch (error) {
    if (error?.message?.includes('already active')) throw error;
    if (!['ECONNREFUSED', 'ENOENT', 'ECONNRESET'].includes(error?.code)) throw error;
  }
  if (!removeOwnedSocket(socketPath, observedIdentity)) {
    throw new Error(`Executor service control socket changed during stale-path probing: ${socketPath}`);
  }
}

function listen(server, socketPath) {
  return new Promise((resolve, reject) => {
    const onError = (error) => {
      server.off('listening', onListening);
      reject(error);
    };
    const onListening = () => {
      server.off('error', onError);
      resolve();
    };
    server.once('error', onError);
    server.once('listening', onListening);
    server.listen(socketPath);
  });
}

function closeServer(server) {
  if (!server.listening) return Promise.resolve();
  return new Promise((resolve, reject) => {
    server.close((error) => (error ? reject(error) : resolve()));
  });
}

function writeResponse(socket, payload) {
  socket.end(`${JSON.stringify(payload)}\n`);
}

export function requestExecutorService(socketPath, request, { timeoutMs = 5_000 } = {}) {
  requireSocketPath(socketPath);
  if (!request || typeof request !== 'object' || Array.isArray(request)) {
    throw new TypeError('request must be an object');
  }
  if (!Number.isSafeInteger(timeoutMs) || timeoutMs <= 0) {
    throw new TypeError('timeoutMs must be a positive safe integer');
  }
  return new Promise((resolve, reject) => {
    const socket = net.createConnection(socketPath);
    let data = '';
    let dispatched = false;
    const classifyFailure = (error) => {
      if (dispatched && !READ_ONLY_ACTIONS.has(request.action)) {
        error.outcome = 'unknown';
      }
      return error;
    };
    const timer = setTimeout(() => {
      const error = new Error('Executor service control request timed out; the durable operation outcome is unknown.');
      error.code = 'ETIMEDOUT';
      error.outcome = 'unknown';
      socket.destroy(error);
    }, timeoutMs);
    timer.unref?.();
    socket.setEncoding('utf8');
    socket.on('connect', () => {
      dispatched = true;
      socket.write(`${JSON.stringify(request)}\n`);
    });
    socket.on('data', (chunk) => {
      data += chunk;
      if (data.length > MAX_REQUEST_BYTES) {
        socket.destroy(new Error('Executor service control response exceeded its size limit.'));
      }
    });
    socket.on('error', (error) => {
      clearTimeout(timer);
      reject(classifyFailure(error));
    });
    socket.on('end', () => {
      clearTimeout(timer);
      try {
        const line = data.trim();
        if (line.length === 0) throw new Error('Executor service returned an empty response.');
        resolve(JSON.parse(line));
      } catch (error) {
        reject(classifyFailure(error));
      }
    });
  });
}

export function createExecutorServiceHost({
  database,
  adapter,
  provider,
  serviceInstanceId,
  hostId = serviceInstanceId,
  socketPath,
  workspaceRoot,
  pollIntervalMs = 250,
  maxConcurrentRuns = 1,
  onPollError = reportPollError,
  onUpgrade = null,
  onClose = null,
  healthCheck = null,
  probeControl = requestExecutorService,
  createService = createExecutorService,
  ...serviceOptions
}) {
  requireSocketPath(socketPath);
  if (!Number.isSafeInteger(pollIntervalMs) || pollIntervalMs <= 0) {
    throw new TypeError('pollIntervalMs must be a positive safe integer');
  }
  if (!Number.isSafeInteger(maxConcurrentRuns) || maxConcurrentRuns <= 0) {
    throw new TypeError('maxConcurrentRuns must be a positive safe integer');
  }
  if (typeof createService !== 'function') throw new TypeError('createService must be a function');
  if (typeof onPollError !== 'function') throw new TypeError('onPollError must be a function');
  if (onUpgrade !== null && typeof onUpgrade !== 'function') {
    throw new TypeError('onUpgrade must be a function or null');
  }
  if (healthCheck !== null && typeof healthCheck !== 'function') {
    throw new TypeError('healthCheck must be a function or null');
  }
  if (typeof probeControl !== 'function') throw new TypeError('probeControl must be a function');
  if (onClose !== null && typeof onClose !== 'function') {
    throw new TypeError('onClose must be a function or null');
  }

  const service = createService({
    database,
    adapter,
    provider,
    serviceInstanceId,
    hostId,
    workspaceRoot,
    ...serviceOptions,
  });
  let lifecycle = 'created';
  let pollTimer = null;
  let pollDispatchActive = false;
  let resourceClosePromise = null;
  let closePromise = null;
  let ownedSocketIdentity = null;
  let lifecycleMutation = null;
  let resolveClosed;
  const closed = new Promise((resolve) => { resolveClosed = resolve; });
  const controlSockets = new Set();
  const inFlightRuns = new Set();

  async function poll() {
    if (lifecycle !== 'open' || pollDispatchActive) return;
    pollDispatchActive = true;
    try {
      while (lifecycle === 'open' && inFlightRuns.size < maxConcurrentRuns) {
        const run = Promise.resolve().then(() => service.runNext());
        inFlightRuns.add(run);
        run.then(
          () => {
            inFlightRuns.delete(run);
          },
          (error) => {
            inFlightRuns.delete(run);
            onPollError(error);
          },
        );
      }
    } finally {
      pollDispatchActive = false;
    }
  }

  const server = net.createServer((socket) => {
    controlSockets.add(socket);
    socket.once('close', () => controlSockets.delete(socket));
    // A control action may finish after its timed-out caller has closed the connection.
    socket.on('error', (error) => {
      if (!PEER_DISCONNECT_ERROR_CODES.has(error?.code)) throw error;
    });
    socket.setEncoding('utf8');
    let data = '';
    let handled = false;
    socket.on('data', (chunk) => {
      if (handled) return;
      data += chunk;
      if (data.length > MAX_REQUEST_BYTES) {
        handled = true;
        writeResponse(socket, { ok: false, error: 'request_too_large' });
        return;
      }
      const newline = data.indexOf('\n');
      if (newline === -1) return;
      handled = true;
      let request;
      Promise.resolve().then(async () => {
        request = JSON.parse(data.slice(0, newline));
        if (lifecycle !== 'open' && request.action !== 'shutdown') {
          throw new Error('executor_service_closing');
        }
        if (request.action === 'health') {
          const prerequisiteHealth = healthCheck?.();
          if (prerequisiteHealth?.ok === false) {
            throw new Error(prerequisiteHealth.error ?? 'executor_prerequisite_failed');
          }
          const snapshot = service.publishObservabilitySnapshot();
          return {
            executor: {
              provider,
              service_instance_id: serviceInstanceId,
            },
            snapshot,
          };
        }
        if (request.action === 'resolve_interaction') {
          return service.resolveInteractionTarget(request.target);
        }
        if (request.action === 'get_background_task') {
          return service.getBackgroundTask(request.background_task_id);
        }
        if (request.action === 'stop_background_task') {
          return service.stopBackgroundTask(request);
        }
        if (request.action === 'submit_interaction_answer') {
          const result = service.submitInteractionAnswer(
            request.answer,
            request.source_evidence ?? {},
          );
          if (
            ['accepted', 'duplicate'].includes(result?.status)
            && result.handoff_state === 'pending'
            && typeof result.handoff_id === 'string'
            && result.handoff_id.length > 0
          ) {
            Promise.resolve()
              .then(() => service.deliverInteractionAnswer(result.handoff_id))
              .catch(onPollError);
          }
          return result;
        }
        if (request.action === 'shutdown') {
          if (lifecycleMutation !== null) {
            throw new Error('executor_lifecycle_operation_in_progress');
          }
          await closeResources();
          return { status: 'completed', service_instance_id: serviceInstanceId };
        }
        if (request.action === 'upgrade') {
          if (onUpgrade === null) throw new Error('installed_runtime_upgrade_unavailable');
          if (lifecycleMutation !== null) {
            throw new Error('executor_lifecycle_operation_in_progress');
          }
          const operation = Promise.resolve().then(() => onUpgrade(request));
          lifecycleMutation = operation;
          try {
            return await operation;
          } finally {
            if (lifecycleMutation === operation) lifecycleMutation = null;
          }
        }
        throw new Error('unsupported_action');
      }).then(
        (result) => {
          const closeAfterResponse = request.action === 'shutdown'
            || (request.action === 'upgrade' && result?.state === 'committed');
          if (closeAfterResponse) {
            socket.once('finish', () => setImmediate(() => close().catch(() => {})));
          }
          writeResponse(socket, { ok: true, result });
        },
        (error) => {
          if (request?.action === 'shutdown') {
            socket.once('finish', () => setImmediate(() => close().catch(() => {})));
          }
          writeResponse(socket, { ok: false, error: error.message });
        },
      );
    });
  });

  async function start() {
    if (lifecycle !== 'created') throw new Error(`Executor service host is ${lifecycle}.`);
    fs.mkdirSync(path.dirname(socketPath), { recursive: true });
    await prepareSocketPath(socketPath, probeControl);
    service.start();
    try {
      await listen(server, socketPath);
      fs.chmodSync(socketPath, 0o600);
      const socketStat = fs.lstatSync(socketPath, { bigint: true });
      ownedSocketIdentity = Object.freeze({
        dev: socketStat.dev,
        ino: socketStat.ino,
        ctimeNs: socketStat.ctimeNs,
        birthtimeNs: socketStat.birthtimeNs,
      });
    } catch (error) {
      await service.close();
      throw error;
    }
    lifecycle = 'open';
    pollTimer = setInterval(() => poll().catch(onPollError), pollIntervalMs);
    pollTimer.unref?.();
    await poll();
    return service.publishObservabilitySnapshot();
  }

  function closeResources() {
    if (resourceClosePromise !== null) return resourceClosePromise;
    resourceClosePromise = (async () => {
      if (pollTimer !== null) {
        clearInterval(pollTimer);
        pollTimer = null;
      }
      lifecycle = 'closing';
      const failures = [];
      if (lifecycleMutation !== null) {
        try { await lifecycleMutation; } catch {
          // The control response owns reporting the upgrade failure; shutdown only joins it.
        }
      }
      try { await service.close(); } catch (error) { failures.push(error); }
      await Promise.allSettled([...inFlightRuns]);
      if (onClose !== null) {
        try { await onClose(); } catch (error) { failures.push(error); }
      }
      if (failures.length === 1) throw failures[0];
      if (failures.length > 1) {
        throw new AggregateError(failures, 'Executor host resource shutdown failed.');
      }
    })();
    return resourceClosePromise;
  }

  function close() {
    if (closePromise) return closePromise;
    closePromise = (async () => {
      const failures = [];
      try { await closeResources(); } catch (error) { failures.push(error); }
      let reboundPath = null;
      try {
        if (ownedSocketIdentity !== null) {
          try {
            const current = fs.lstatSync(socketPath, { bigint: true });
            if (!matchesSocketIdentity(current, ownedSocketIdentity)) {
              reboundPath = `${socketPath}.rebound-${process.pid}-${crypto.randomUUID()}`;
              fs.renameSync(socketPath, reboundPath);
            }
          } catch (error) {
            if (error?.code !== 'ENOENT') throw error;
          }
        }
        const serverClosed = closeServer(server);
        for (const socket of controlSockets) socket.destroy();
        await serverClosed;
      } catch (error) { failures.push(error); }
      if (reboundPath !== null) {
        try {
          if (fs.existsSync(socketPath)) {
            throw new Error(`Executor control path was rebound again during shutdown: ${socketPath}`);
          }
          fs.renameSync(reboundPath, socketPath);
          reboundPath = null;
        } catch (error) { failures.push(error); }
      }
      if (ownedSocketIdentity !== null) {
        try {
          removeOwnedSocket(socketPath, ownedSocketIdentity);
          ownedSocketIdentity = null;
        } catch (error) { failures.push(error); }
      }
      lifecycle = failures.length === 0 ? 'closed' : 'close_failed';
      resolveClosed(Object.freeze({
        ok: failures.length === 0,
        error: failures.length === 0 ? null : failures[0].message,
      }));
      if (failures.length === 1) throw failures[0];
      if (failures.length > 1) throw new AggregateError(failures, 'Executor host shutdown failed.');
    })();
    return closePromise;
  }

  return Object.freeze({ close, closed, service, start });
}
