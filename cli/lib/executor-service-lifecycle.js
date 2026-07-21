import { execFileSync } from 'node:child_process';
import path from 'node:path';

import { requestExecutorService } from '../../runtime/executor/service-host.js';
import { assertExecutorStartFence } from '../../runtime/executor/start-fence.js';

export const EXECUTOR_SERVICE_NAME = 'zylos-executor';

function expectedExecutorEntry() {
  const packageRoot = process.env.ZYLOS_PACKAGE_ROOT
    ? path.resolve(process.env.ZYLOS_PACKAGE_ROOT)
    : path.resolve(import.meta.dirname, '..', '..');
  return path.join(packageRoot, 'runtime', 'executor', 'launcher.js');
}

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

async function waitForHealthy({
  zylosDir,
  requestFn,
  retryDelaysMs,
  previousServiceInstanceId = null,
  expectedProvider = null,
}) {
  let last = { ok: false, error: 'executor_health_unavailable' };
  for (const waitMs of retryDelaysMs) {
    await delay(waitMs);
    last = await readHealth({ zylosDir, requestFn });
    if (last.ok && (expectedProvider === null || last.provider === expectedProvider) && (
      previousServiceInstanceId === null
      || last.serviceInstanceId !== previousServiceInstanceId
    )) return last;
  }
  if (last.ok && expectedProvider !== null && last.provider !== expectedProvider) {
    return {
      ...last,
      ok: false,
      error: 'executor_provider_mismatch',
      expectedProvider,
    };
  }
  if (last.ok && previousServiceInstanceId !== null) {
    return { ok: false, error: 'executor_identity_did_not_change' };
  }
  return last;
}

function runPm2(execFileSyncFn, args) {
  execFileSyncFn('pm2', args, { stdio: 'pipe', timeout: 30_000 });
}

function inspectExecutorRegistration({ zylosDir, execFileSyncFn, required }) {
  const root = requireZylosDir(zylosDir);
  const output = execFileSyncFn('pm2', ['jlist'], {
    encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'], timeout: 30_000,
  });
  const processes = JSON.parse(String(output));
  if (!Array.isArray(processes)) throw new Error('PM2 process inventory must be an array.');
  const matches = processes.filter(({ name }) => name === EXECUTOR_SERVICE_NAME);
  if (matches.length > 1) throw new Error('Ambiguous executor supervisor registrations.');
  if (matches.length === 0) {
    if (required) throw new Error('Executor supervisor registration is missing.');
    return null;
  }
  const registration = matches[0];
  const environment = registration.pm2_env ?? {};
  const actualEntry = environment.pm_exec_path ?? registration.pm_exec_path;
  const actualCwd = environment.pm_cwd ?? registration.pm_cwd;
  const actualRoot = environment.ZYLOS_DIR ?? registration.ZYLOS_DIR;
  if (typeof actualEntry !== 'string'
    || path.resolve(actualEntry) !== expectedExecutorEntry()
    || typeof actualCwd !== 'string' || path.resolve(actualCwd) !== root
    || typeof actualRoot !== 'string' || path.resolve(actualRoot) !== root) {
    throw new Error('Refusing to control a foreign zylos-executor PM2 registration.');
  }
  return registration;
}

function pm2Failure(error) {
  return { ok: false, error: error?.message ?? 'pm2_control_failed' };
}

export async function startExecutorService({
  zylosDir,
  expectedProvider = null,
  execFileSyncFn = execFileSync,
  requestFn = requestExecutorService,
  retryDelaysMs = [0, 100, 250, 500, 1_000, 2_000],
  assertStartFence = assertExecutorStartFence,
} = {}) {
  try {
    assertStartFence({ zylosDir });
    inspectExecutorRegistration({ zylosDir, execFileSyncFn, required: false });
    runPm2(execFileSyncFn, [
      'start', ecosystemPath(zylosDir), '--only', EXECUTOR_SERVICE_NAME,
    ]);
    runPm2(execFileSyncFn, ['save']);
  } catch (error) {
    return pm2Failure(error);
  }
  return waitForHealthy({ zylosDir, requestFn, retryDelaysMs, expectedProvider });
}

export async function stopExecutorService({
  zylosDir,
  execFileSyncFn = execFileSync,
  requestFn = requestExecutorService,
} = {}) {
  try {
    inspectExecutorRegistration({ zylosDir, execFileSyncFn, required: true });
  } catch (error) {
    return pm2Failure(error);
  }
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
  expectedProvider = null,
  beforeSupervisorRestart = null,
  restoreConfiguration = null,
  execFileSyncFn = execFileSync,
  requestFn = requestExecutorService,
  retryDelaysMs = [0, 100, 250, 500, 1_000, 2_000],
} = {}) {
  try {
    inspectExecutorRegistration({ zylosDir, execFileSyncFn, required: true });
  } catch (error) {
    return pm2Failure(error);
  }
  const previous = await readHealth({ zylosDir, requestFn });
  if (typeof previous.serviceInstanceId !== 'string') return previous;
  if (previous.snapshot?.service?.maintenance === true
    || previous.snapshot?.service?.draining === true) {
    return { ok: false, error: 'executor_lifecycle_operation_in_progress' };
  }
  let shutdown;
  try {
    shutdown = await requestFn(executorServiceSocketPath(zylosDir), { action: 'shutdown' });
  } catch (error) {
    return { ok: false, error: error?.code ?? error?.message ?? 'shutdown_unavailable' };
  }
  if (shutdown?.ok !== true || shutdown.result?.status !== 'completed') {
    return { ok: false, error: shutdown?.error ?? 'shutdown_not_acknowledged' };
  }
  try {
    await beforeSupervisorRestart?.();
  } catch (error) {
    let rollback = null;
    if (restoreConfiguration !== null) {
      try {
        await restoreConfiguration();
        runPm2(execFileSyncFn, [
          'restart', ecosystemPath(zylosDir), '--only', EXECUTOR_SERVICE_NAME,
        ]);
        runPm2(execFileSyncFn, ['save']);
        rollback = await waitForHealthy({
          zylosDir, requestFn, retryDelaysMs,
          previousServiceInstanceId: previous.serviceInstanceId,
          expectedProvider: previous.provider ?? null,
        });
      } catch (rollbackError) {
        rollback = {
          ok: false,
          error: rollbackError?.message ?? 'executor_provider_rollback_failed',
        };
      }
    }
    return {
      ok: false,
      error: error?.message ?? 'executor_restart_prepare_failed',
      configurationRollback: rollback,
    };
  }
  let restartFailure = null;
  try {
    runPm2(execFileSyncFn, [
      'restart', ecosystemPath(zylosDir), '--only', EXECUTOR_SERVICE_NAME,
    ]);
    runPm2(execFileSyncFn, ['save']);
  } catch (error) {
    restartFailure = pm2Failure(error);
  }
  const current = restartFailure ?? await waitForHealthy({
    zylosDir, requestFn, retryDelaysMs,
    previousServiceInstanceId: previous.serviceInstanceId, expectedProvider,
  });
  if (current.ok) return { ...current, previousServiceInstanceId: previous.serviceInstanceId };
  if (restoreConfiguration === null) return current;

  let rollback;
  try {
    await restoreConfiguration();
    runPm2(execFileSyncFn, [
      'restart', ecosystemPath(zylosDir), '--only', EXECUTOR_SERVICE_NAME,
    ]);
    runPm2(execFileSyncFn, ['save']);
    rollback = await waitForHealthy({
      zylosDir, requestFn, retryDelaysMs,
      previousServiceInstanceId: previous.serviceInstanceId,
      expectedProvider: previous.provider ?? null,
    });
  } catch (error) {
    rollback = { ok: false, error: error?.message ?? 'executor_provider_rollback_failed' };
  }
  return {
    ...current,
    configurationRollback: rollback,
  };
}

export async function selfHealExecutorService(options = {}) {
  const current = await readHealth({
    zylosDir: options.zylosDir,
    requestFn: options.requestFn ?? requestExecutorService,
  });
  const providerMatches = options.expectedProvider == null
    || current.provider === options.expectedProvider;
  if (current.ok && providerMatches) return { ...current, repaired: false };
  const repaired = typeof current.serviceInstanceId === 'string'
    ? await restartExecutorService(options)
    : await startExecutorService(options);
  return repaired.ok ? { ...repaired, repaired: true } : repaired;
}

export async function reconcileExecutorService(options = {}) {
  const current = await readHealth({
    zylosDir: options.zylosDir,
    requestFn: options.requestFn ?? requestExecutorService,
  });
  return typeof current.serviceInstanceId === 'string'
    ? restartExecutorService(options)
    : startExecutorService(options);
}

export async function getExecutorServiceHealth({
  zylosDir,
  requestFn = requestExecutorService,
} = {}) {
  return readHealth({ zylosDir, requestFn });
}

export function removeExecutorServiceRegistration({
  zylosDir,
  execFileSyncFn = execFileSync,
} = {}) {
  try {
    inspectExecutorRegistration({ zylosDir, execFileSyncFn, required: true });
    runPm2(execFileSyncFn, ['delete', EXECUTOR_SERVICE_NAME]);
    runPm2(execFileSyncFn, ['save']);
    return { ok: true };
  } catch (error) {
    return pm2Failure(error);
  }
}
