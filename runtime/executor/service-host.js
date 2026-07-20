import fs from 'node:fs';
import net from 'node:net';
import path from 'node:path';

import { createExecutorService } from './service.js';

const MAX_REQUEST_BYTES = 64 * 1024;

function requireSocketPath(socketPath) {
  if (typeof socketPath !== 'string' || !path.isAbsolute(socketPath)) {
    throw new TypeError('socketPath must be an absolute path');
  }
  if (path.parse(socketPath).root === socketPath) {
    throw new TypeError('socketPath must not be a filesystem root');
  }
  return socketPath;
}

function removeOwnedSocket(socketPath) {
  let stat;
  try {
    stat = fs.lstatSync(socketPath);
  } catch (error) {
    if (error?.code === 'ENOENT') return;
    throw error;
  }
  if (!stat.isSocket()) {
    throw new Error(`Refusing to replace non-socket control path: ${socketPath}`);
  }
  fs.unlinkSync(socketPath);
}

async function prepareSocketPath(socketPath) {
  let stat;
  try {
    stat = fs.lstatSync(socketPath);
  } catch (error) {
    if (error?.code === 'ENOENT') return;
    throw error;
  }
  if (!stat.isSocket()) {
    throw new Error(`Refusing to replace non-socket control path: ${socketPath}`);
  }
  try {
    await requestExecutorService(socketPath, { action: 'health' }, { timeoutMs: 500 });
    throw new Error(`Executor service control socket is already active: ${socketPath}`);
  } catch (error) {
    if (error?.message?.includes('already active')) throw error;
    if (!['ECONNREFUSED', 'ENOENT', 'ECONNRESET'].includes(error?.code)) throw error;
  }
  removeOwnedSocket(socketPath);
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
    const timer = setTimeout(() => {
      socket.destroy(new Error('Executor service control request timed out.'));
    }, timeoutMs);
    timer.unref?.();
    socket.setEncoding('utf8');
    socket.on('connect', () => socket.write(`${JSON.stringify(request)}\n`));
    socket.on('data', (chunk) => {
      data += chunk;
      if (data.length > MAX_REQUEST_BYTES) {
        socket.destroy(new Error('Executor service control response exceeded its size limit.'));
      }
    });
    socket.on('error', (error) => {
      clearTimeout(timer);
      reject(error);
    });
    socket.on('end', () => {
      clearTimeout(timer);
      try {
        const line = data.trim();
        if (line.length === 0) throw new Error('Executor service returned an empty response.');
        resolve(JSON.parse(line));
      } catch (error) {
        reject(error);
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
  onUpgrade = null,
  onClose = null,
  createService = createExecutorService,
  ...serviceOptions
}) {
  requireSocketPath(socketPath);
  if (!Number.isSafeInteger(pollIntervalMs) || pollIntervalMs <= 0) {
    throw new TypeError('pollIntervalMs must be a positive safe integer');
  }
  if (typeof createService !== 'function') throw new TypeError('createService must be a function');
  if (onUpgrade !== null && typeof onUpgrade !== 'function') {
    throw new TypeError('onUpgrade must be a function or null');
  }
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
  let pollActive = false;
  let closePromise = null;
  let resolveClosed;
  const closed = new Promise((resolve) => { resolveClosed = resolve; });
  const controlSockets = new Set();

  async function poll() {
    if (lifecycle !== 'open' || pollActive) return;
    pollActive = true;
    try {
      await service.runNext();
    } finally {
      pollActive = false;
    }
  }

  const server = net.createServer((socket) => {
    controlSockets.add(socket);
    socket.once('close', () => controlSockets.delete(socket));
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
        if (request.action === 'health') {
          const snapshot = service.publishObservabilitySnapshot();
          return {
            executor: {
              provider,
              service_instance_id: serviceInstanceId,
            },
            snapshot,
          };
        }
        if (request.action === 'shutdown') {
          return { status: 'completed', service_instance_id: serviceInstanceId };
        }
        if (request.action === 'upgrade') {
          if (onUpgrade === null) throw new Error('installed_runtime_upgrade_unavailable');
          return onUpgrade(request);
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
        (error) => writeResponse(socket, { ok: false, error: error.message }),
      );
    });
  });

  async function start() {
    if (lifecycle !== 'created') throw new Error(`Executor service host is ${lifecycle}.`);
    fs.mkdirSync(path.dirname(socketPath), { recursive: true });
    await prepareSocketPath(socketPath);
    service.start();
    try {
      await listen(server, socketPath);
    } catch (error) {
      await service.close();
      throw error;
    }
    fs.chmodSync(socketPath, 0o600);
    lifecycle = 'open';
    pollTimer = setInterval(() => poll().catch(() => {}), pollIntervalMs);
    pollTimer.unref?.();
    await poll();
    return service.publishObservabilitySnapshot();
  }

  function close() {
    if (closePromise) return closePromise;
    closePromise = (async () => {
      if (pollTimer !== null) {
        clearInterval(pollTimer);
        pollTimer = null;
      }
      lifecycle = 'closing';
      const failures = [];
      try { await service.close(); } catch (error) { failures.push(error); }
      try {
        const serverClosed = closeServer(server);
        for (const socket of controlSockets) socket.destroy();
        await serverClosed;
      } catch (error) { failures.push(error); }
      try { removeOwnedSocket(socketPath); } catch (error) { failures.push(error); }
      if (onClose !== null) {
        try { await onClose(); } catch (error) { failures.push(error); }
      }
      lifecycle = failures.length === 0 ? 'closed' : 'close_failed';
      resolveClosed();
      if (failures.length === 1) throw failures[0];
      if (failures.length > 1) throw new AggregateError(failures, 'Executor host shutdown failed.');
    })();
    return closePromise;
  }

  return Object.freeze({ close, closed, service, start });
}
