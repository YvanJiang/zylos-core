import { execFileSync } from 'node:child_process';
import path from 'node:path';

import { requestExecutorService } from '../../runtime/executor/service-host.js';

export const EXECUTOR_SERVICE_NAME = 'zylos-executor';

function requireZylosDir(zylosDir) {
  if (typeof zylosDir !== 'string' || !path.isAbsolute(zylosDir)) {
    throw new TypeError('zylosDir must be an absolute path');
  }
  if (path.parse(zylosDir).root === zylosDir) {
    throw new TypeError('zylosDir must not be a filesystem root');
  }
  return path.resolve(zylosDir);
}

export function executorServiceSocketPath(zylosDir) {
  return path.join(requireZylosDir(zylosDir), 'runtime', 'executor-service.sock');
}

function ecosystemPath(zylosDir) {
  return path.join(requireZylosDir(zylosDir), 'pm2', 'ecosystem.config.cjs');
}

function normalizeHealth(response) {
  if (response?.ok !== true) {
    return { ok: false, error: response?.error ?? 'executor_health_unavailable' };
  }
  const snapshot = response.result?.snapshot ?? response.result;
  const executor = response.result?.executor ?? null;
  if (snapshot?.contract !== 'zylos.observability-snapshot'
    || snapshot.service?.service_instance_id !== snapshot.core_service_instance_id
    || (executor !== null
      && executor.service_instance_id !== snapshot.core_service_instance_id)) {
    return { ok: false, error: 'executor_health_invalid' };
  }
  return {
    ok: snapshot.service.health === 'healthy',
    error: snapshot.service.health === 'healthy' ? undefined : `executor_${snapshot.service.health}`,
    health: snapshot.service.health,
    provider: executor?.provider,
    serviceInstanceId: snapshot.core_service_instance_id,
    snapshot,
  };
}

async function readHealth({ zylosDir, requestFn }) {
  try {
    return normalizeHealth(await requestFn(
      executorServiceSocketPath(zylosDir),
      { action: 'health' },
    ));
  } catch (error) {
    return { ok: false, error: error?.code ?? error?.message ?? 'executor_health_unavailable' };
  }
}

function delay(milliseconds) {
  if (milliseconds === 0) return Promise.resolve();
  return new Promise((resolve) => {
    setTimeout(resolve, milliseconds);
  });
}

async function waitForHealthy({ zylosDir, requestFn, retryDelaysMs, previousServiceInstanceId = null }) {
  let last = { ok: false, error: 'executor_health_unavailable' };
  for (const waitMs of retryDelaysMs) {
    await delay(waitMs);
    last = await readHealth({ zylosDir, requestFn });
    if (last.ok && (
      previousServiceInstanceId === null
      || last.serviceInstanceId !== previousServiceInstanceId
    )) return last;
  }
  if (last.ok && previousServiceInstanceId !== null) {
    return { ok: false, error: 'executor_identity_did_not_change' };
  }
  return last;
}

function runPm2(execFileSyncFn, args) {
  execFileSyncFn('pm2', args, { stdio: 'pipe', timeout: 30_000 });
}

function pm2Failure(error) {
  return { ok: false, error: error?.message ?? 'pm2_control_failed' };
}

export async function startExecutorService({
  zylosDir,
  execFileSyncFn = execFileSync,
  requestFn = requestExecutorService,
  retryDelaysMs = [0, 100, 250, 500, 1_000, 2_000],
} = {}) {
  try {
    runPm2(execFileSyncFn, [
      'start', ecosystemPath(zylosDir), '--only', EXECUTOR_SERVICE_NAME,
    ]);
    runPm2(execFileSyncFn, ['save']);
  } catch (error) {
    return pm2Failure(error);
  }
  return waitForHealthy({ zylosDir, requestFn, retryDelaysMs });
}

export async function stopExecutorService({
  zylosDir,
  execFileSyncFn = execFileSync,
  requestFn = requestExecutorService,
} = {}) {
  let response;
  try {
    response = await requestFn(executorServiceSocketPath(zylosDir), { action: 'shutdown' });
  } catch (error) {
    return { ok: false, error: error?.code ?? error?.message ?? 'shutdown_unavailable' };
  }
  if (response?.ok !== true || response.result?.status !== 'completed') {
    return { ok: false, error: response?.error ?? 'shutdown_not_acknowledged' };
  }
  try {
    runPm2(execFileSyncFn, ['stop', EXECUTOR_SERVICE_NAME]);
    runPm2(execFileSyncFn, ['save']);
  } catch (error) {
    return pm2Failure(error);
  }
  return {
    ok: true,
    serviceInstanceId: response.result.service_instance_id,
    status: response.result.status,
  };
}

export async function restartExecutorService({
  zylosDir,
  execFileSyncFn = execFileSync,
  requestFn = requestExecutorService,
  retryDelaysMs = [0, 100, 250, 500, 1_000, 2_000],
} = {}) {
  const previous = await readHealth({ zylosDir, requestFn });
  if (!previous.ok) return previous;
  try {
    runPm2(execFileSyncFn, [
      'restart', ecosystemPath(zylosDir), '--only', EXECUTOR_SERVICE_NAME,
    ]);
    runPm2(execFileSyncFn, ['save']);
  } catch (error) {
    return pm2Failure(error);
  }
  const current = await waitForHealthy({
    zylosDir,
    requestFn,
    retryDelaysMs,
    previousServiceInstanceId: previous.serviceInstanceId,
  });
  return current.ok ? { ...current, previousServiceInstanceId: previous.serviceInstanceId } : current;
}

export async function selfHealExecutorService(options = {}) {
  const current = await readHealth({
    zylosDir: options.zylosDir,
    requestFn: options.requestFn ?? requestExecutorService,
  });
  if (current.ok) return { ...current, repaired: false };
  const repaired = await startExecutorService(options);
  return repaired.ok ? { ...repaired, repaired: true } : repaired;
}

export async function reconcileExecutorService(options = {}) {
  const current = await readHealth({
    zylosDir: options.zylosDir,
    requestFn: options.requestFn ?? requestExecutorService,
  });
  return current.ok ? restartExecutorService(options) : startExecutorService(options);
}

export async function getExecutorServiceHealth({
  zylosDir,
  requestFn = requestExecutorService,
} = {}) {
  return readHealth({ zylosDir, requestFn });
}

export function removeExecutorServiceRegistration({ execFileSyncFn = execFileSync } = {}) {
  try {
    runPm2(execFileSyncFn, ['delete', EXECUTOR_SERVICE_NAME]);
    runPm2(execFileSyncFn, ['save']);
    return { ok: true };
  } catch (error) {
    return pm2Failure(error);
  }
}
