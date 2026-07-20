import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { execFileSync, spawn } from 'node:child_process';

import { canonicalizeJson } from '../../contracts/public/index.js';
import { initializeRuntimePersistence } from '../persistence/schema.js';
import {
  createAtomicReleaseAdapter,
  createLegacySourceQueueAdapter,
  createSqliteSnapshotAdapter,
} from './runtime-upgrade-coordinator.js';
import { createInstalledRuntimeUpgradeHost } from './installed-runtime-upgrade-host.js';
import { driveInstalledRuntimeUpgrade } from './executor-upgrade-driver.js';
import { legacyLifecycleArtifactPaths } from './legacy-lifecycle-artifacts.js';
import { findResumableRuntimeUpgrade } from './upgrade-state.js';

const LEGACY_SERVICE_NAMES = Object.freeze([
  'activity-monitor', 'c4-dispatcher', 'scheduler', 'web-console', 'caddy',
]);

function requireDirectory(name, value) {
  if (typeof value !== 'string' || !path.isAbsolute(value)
    || path.parse(value).root === value) {
    throw new TypeError(`${name} must be an explicit absolute non-root path`);
  }
  const resolved = fs.realpathSync(value);
  if (!fs.statSync(resolved).isDirectory()) throw new TypeError(`${name} must be a directory`);
  return resolved;
}

function atomicJson(file, document) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  const temporary = `${file}.${crypto.randomUUID()}.partial`;
  fs.writeFileSync(temporary, `${canonicalizeJson(document)}\n`, { mode: 0o600 });
  fs.renameSync(temporary, file);
}

function readUpgradePlan(planFile, expectedUpgradeId, {
  installationRoot,
  releaseRoot,
  provider,
}) {
  const plan = JSON.parse(fs.readFileSync(planFile, 'utf8'));
  if (plan?.schema_version !== 1 || plan.upgrade_id !== expectedUpgradeId
    || plan.preflight?.upgrade_id !== expectedUpgradeId) {
    throw new Error(`Runtime upgrade plan ${expectedUpgradeId} is invalid.`);
  }
  for (const [name, value] of Object.entries({
    from_release_path: plan.from_release_path,
    to_release_path: plan.to_release_path,
    legacy_queue_file: plan.legacy_queue_file,
  })) {
    if (typeof value !== 'string' || !path.isAbsolute(value)
      || path.parse(value).root === value) {
      throw new Error(`Runtime upgrade plan ${name} must be an absolute non-root path.`);
    }
  }
  if (plan.provider !== provider
    || typeof plan.from_package_version !== 'string'
    || typeof plan.to_package_version !== 'string'
    || !plan.legacy_batch || typeof plan.legacy_batch.batch_id !== 'string'
    || !Array.isArray(plan.legacy_batch.records)) {
    throw new Error(`Runtime upgrade plan ${expectedUpgradeId} is incomplete.`);
  }
  const expectedQueueFile = path.join(
    installationRoot,
    'runtime',
    'upgrade-input',
    `${expectedUpgradeId}.json`,
  );
  if (path.resolve(plan.legacy_queue_file) !== expectedQueueFile) {
    throw new Error(`Runtime upgrade plan ${expectedUpgradeId} has a conflicting queue path.`);
  }
  const fromReleasePath = requireDirectory('from_release_path', plan.from_release_path);
  const toReleasePath = requireDirectory('to_release_path', plan.to_release_path);
  const resolvedReleaseRoot = requireDirectory('releaseRoot', releaseRoot);
  if (path.dirname(toReleasePath) !== resolvedReleaseRoot) {
    throw new Error(`Runtime upgrade plan ${expectedUpgradeId} target escaped the release store.`);
  }
  const fromPackageVersion = readPackageRelease(fromReleasePath);
  const toPackageVersion = readPackageRelease(toReleasePath);
  if (fromPackageVersion !== plan.from_package_version
    || toPackageVersion !== plan.to_package_version) {
    throw new Error(`Runtime upgrade plan ${expectedUpgradeId} package identity conflicts.`);
  }
  for (const [releaseRef, packageVersion] of [
    [plan.preflight.from_release, fromPackageVersion],
    [plan.preflight.to_release, toPackageVersion],
  ]) {
    if (releaseRef !== packageVersion && !releaseRef.startsWith('branch:')) {
      throw new Error(`Runtime upgrade release ${releaseRef} does not match package ${packageVersion}.`);
    }
  }
  return Object.freeze({
    ...plan,
    from_release_path: fromReleasePath,
    to_release_path: toReleasePath,
  });
}

function upgradePlanPath(planDirectory, upgradeId) {
  if (typeof upgradeId !== 'string' || !/^[A-Za-z0-9._-]+$/.test(upgradeId)) {
    throw new Error('Runtime upgrade ID is unsafe for installed plan lookup.');
  }
  return path.join(planDirectory, `${upgradeId}.json`);
}

function readPackageRelease(releasePath) {
  const packageDocument = JSON.parse(fs.readFileSync(path.join(releasePath, 'package.json'), 'utf8'));
  if (packageDocument.name !== 'zylos' || typeof packageDocument.version !== 'string') {
    throw new Error('Downloaded release is not a zylos-core package.');
  }
  for (const entry of [
    path.join(releasePath, 'cli', 'launcher.js'),
    path.join(releasePath, 'cli', 'zylos.js'),
    path.join(releasePath, 'runtime', 'executor', 'daemon.js'),
    path.join(releasePath, 'runtime', 'executor', 'health-probe.js'),
    path.join(releasePath, 'runtime', 'executor', 'launcher.js'),
  ]) {
    if (!fs.statSync(entry).isFile()) throw new Error(`Downloaded release is missing ${entry}.`);
  }
  return packageDocument.version;
}

export function assertLegacyServicesInactive(execFileSyncFn = execFileSync) {
  const processes = JSON.parse(execFileSyncFn('pm2', ['jlist'], {
    encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'], timeout: 30_000,
  }));
  const active = processes.filter((processInfo) => LEGACY_SERVICE_NAMES.includes(processInfo.name)
    && !['stopped', 'errored'].includes(processInfo.pm2_env?.status));
  if (active.length > 0) {
    throw new Error(`Obsolete runtime services are still active: ${active.map(({ name }) => name).join(', ')}`);
  }
  return { stopped: true, stopped_at: new Date().toISOString() };
}

export function removeLegacyServiceRegistrations(execFileSyncFn = execFileSync) {
  const processes = JSON.parse(execFileSyncFn('pm2', ['jlist'], {
    encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'], timeout: 30_000,
  }));
  const registered = [...new Set(processes
    .map(({ name }) => name)
    .filter((name) => LEGACY_SERVICE_NAMES.includes(name)))];
  for (const serviceName of registered) {
    execFileSyncFn('pm2', ['delete', serviceName], { stdio: 'pipe', timeout: 30_000 });
  }
  if (registered.length > 0) {
    execFileSyncFn('pm2', ['save'], { stdio: 'pipe', timeout: 30_000 });
  }
  return Object.freeze({ removed_services: Object.freeze(registered) });
}

function prepareRelease({ source, releaseRef, branch, releaseRoot, execFileSyncFn }) {
  const sourcePath = requireDirectory('downloaded_source', source);
  const packageVersion = readPackageRelease(sourcePath);
  if (typeof releaseRef !== 'string' || releaseRef.length === 0) {
    throw new Error('Upgrade target release is required.');
  }
  if (branch === null && releaseRef !== packageVersion) {
    throw new Error(`Downloaded package version ${packageVersion} does not match target ${releaseRef}.`);
  }
  const safeRef = releaseRef.replace(/[^A-Za-z0-9._-]/g, '-').slice(0, 80);
  const releasePath = path.join(releaseRoot, `${safeRef}-${crypto.randomUUID()}`);
  fs.mkdirSync(releaseRoot, { recursive: true });
  fs.cpSync(sourcePath, releasePath, { recursive: true, errorOnExist: true, force: false });
  execFileSyncFn('npm', ['install', '--omit=dev', '--no-audit', '--no-fund'], {
    cwd: releasePath,
    stdio: 'pipe',
    timeout: 15 * 60_000,
  });
  return Object.freeze({ packageVersion, releasePath, releaseRef });
}

function decorateReleaseAdapter(adapter, activeReleaseFile, execFileSyncFn) {
  return Object.freeze({
    async activate(request) {
      const result = await adapter.activate(request);
      atomicJson(activeReleaseFile, {
        release_ref: result.release_ref,
        release_path: result.release_path,
        upgrade_id: request.upgrade_id,
      });
      return { ...result, upgrade_id: request.upgrade_id };
    },
    async restore(request) {
      const result = await adapter.restore(request);
      atomicJson(activeReleaseFile, {
        release_ref: result.release_ref,
        release_path: result.release_path,
        upgrade_id: null,
      });
      return { ...result, upgrade_id: null };
    },
    async cleanup(request) {
      const result = await adapter.cleanup(request);
      const registrations = removeLegacyServiceRegistrations(execFileSyncFn);
      return Object.freeze({ ...result, ...registrations });
    },
  });
}

function startTargetHealthProcess({ releasePath, zylosDir, provider, request }) {
  const entry = path.join(releasePath, 'runtime', 'executor', 'health-probe.js');
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, [entry], {
      cwd: zylosDir,
      env: {
        ...process.env,
        ZYLOS_DIR: zylosDir,
        ZYLOS_RUNTIME_PROVIDER: provider,
        ZYLOS_RELEASE_REF: request.release_ref,
        ZYLOS_UPGRADE_ID: request.upgrade_id,
      },
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    let stdout = '';
    let stderr = '';
    let settled = false;
    let exitState = null;
    const exitWaiters = [];
    let closePromise = null;
    const settleClose = ({ code, signal }, closeResolve, closeReject) => {
      if (code === 0 || (code === null && signal === 'SIGTERM')) closeResolve();
      else closeReject(new Error(
        `Target health probe exited ${code ?? signal}: ${stderr.trim()}`,
      ));
    };
    const closeTarget = () => {
      if (closePromise !== null) return closePromise;
      closePromise = new Promise((closeResolve, closeReject) => {
        if (exitState !== null) {
          settleClose(exitState, closeResolve, closeReject);
          return;
        }
        const timer = setTimeout(() => child.kill('SIGKILL'), 5_000);
        exitWaiters.push((state) => {
          clearTimeout(timer);
          settleClose(state, closeResolve, closeReject);
        });
        child.kill('SIGTERM');
      });
      return closePromise;
    };
    const fail = (error) => {
      if (settled) return;
      settled = true;
      closeTarget().then(
        () => reject(error),
        (cleanupError) => reject(new AggregateError(
          [error, cleanupError],
          `Target health proof failed and cleanup failed: ${error.message}`,
        )),
      );
    };
    child.once('error', (error) => {
      if (settled) return;
      settled = true;
      reject(error);
    });
    child.stderr.setEncoding('utf8');
    child.stderr.on('data', (chunk) => { stderr += chunk; });
    child.stdout.setEncoding('utf8');
    child.stdout.on('data', (chunk) => {
      if (settled) return;
      stdout += chunk;
      const newline = stdout.indexOf('\n');
      if (newline === -1) {
        if (stdout.length > 64 * 1024) fail(new Error('Target health proof exceeded its size limit.'));
        return;
      }
      try {
        const proof = JSON.parse(stdout.slice(0, newline));
        settled = true;
        resolve(Object.freeze({
          proof,
          close: closeTarget,
        }));
      } catch (error) {
        fail(error);
      }
    });
    child.once('close', (code, signal) => {
      exitState = { code, signal };
      for (const waiter of exitWaiters.splice(0)) waiter(exitState);
      if (!settled) fail(new Error(`Target health probe exited ${code ?? signal}: ${stderr.trim()}`));
    });
  });
}

export function createInstalledExecutorUpgradeHandler({
  database,
  Database,
  zylosDir,
  currentReleasePath,
  currentReleaseRef,
  provider,
  execFileSyncFn = execFileSync,
  startTargetHealth = startTargetHealthProcess,
  now = () => new Date().toISOString(),
}) {
  const installationRoot = requireDirectory('zylosDir', zylosDir);
  const packageRoot = requireDirectory('currentReleasePath', currentReleasePath);
  if (typeof Database !== 'function') throw new TypeError('Database must be a constructor');
  if (typeof startTargetHealth !== 'function') throw new TypeError('startTargetHealth must be a function');
  initializeRuntimePersistence(database);
  const activeReleaseFile = path.join(installationRoot, 'runtime', 'active-release.json');
  const snapshotDirectory = path.join(installationRoot, 'runtime', 'upgrade-snapshots');
  const releaseRoot = path.join(installationRoot, 'runtime', 'releases');
  const planDirectory = path.join(installationRoot, 'runtime', 'upgrade-plans');

  async function executePlan(plan) {
    assertLegacyServicesInactive(execFileSyncFn);
    const releases = {
      [plan.preflight.from_release]: requireDirectory('from_release_path', plan.from_release_path),
      [plan.preflight.to_release]: requireDirectory('to_release_path', plan.to_release_path),
    };
    const physicalReleaseAdapter = createAtomicReleaseAdapter({
      activeReleaseFile,
      releases,
      cleanupPaths: legacyLifecycleArtifactPaths(installationRoot),
    });
    const releaseAdapter = decorateReleaseAdapter(
      physicalReleaseAdapter,
      activeReleaseFile,
      execFileSyncFn,
    );
    const legacySourceAdapter = createLegacySourceQueueAdapter({
      sourceQueueFile: plan.legacy_queue_file,
      auditDirectory: path.join(installationRoot, 'runtime', 'legacy-upgrade-audit'),
      stopLegacyDispatcher: async () => assertLegacyServicesInactive(execFileSyncFn),
      restartLegacyDispatcher: async ({ step_id: stepId }) => ({
        step_id: stepId,
        restarted: true,
        restarted_at: now(),
      }),
    });
    let targetHealth = null;
    const executorAdapter = {
      async health(healthRequest) {
        if (targetHealth === null) {
          targetHealth = await startTargetHealth({
            releasePath: plan.to_release_path,
            zylosDir: installationRoot,
            provider: plan.provider,
            request: healthRequest,
          });
        }
        return targetHealth.proof;
      },
    };
    const host = createInstalledRuntimeUpgradeHost({
      database,
      snapshotAdapter: createSqliteSnapshotAdapter({
        database,
        snapshotDirectory,
        openDatabase: (file, options) => new Database(file, options),
      }),
      releaseAdapter,
      legacySourceAdapter,
      executorAdapter,
      zylosDir: installationRoot,
      now,
    });
    let result;
    let closeError = null;
    try {
      result = await driveInstalledRuntimeUpgrade({
        host,
        preflight: plan.preflight,
        legacyBatch: plan.legacy_batch,
      });
    } finally {
      if (targetHealth !== null) {
        try {
          await targetHealth.close();
        } catch (error) {
          closeError = error;
        }
      }
    }
    if (closeError !== null) {
      result = Object.freeze({
        ...result,
        success: false,
        committed: result?.state === 'committed',
        error: `Target health probe cleanup failed: ${closeError.message}`,
      });
    }
    const orchestrationComplete = result?.state === 'rolled_back'
      || (result?.state === 'committed' && result?.completedStep === 'postcommit-cleanup');
    if (orchestrationComplete) {
      fs.rmSync(plan.legacy_queue_file, { force: true });
      fs.rmSync(upgradePlanPath(planDirectory, plan.upgrade_id), { force: true });
    }
    return result;
  }

  async function resumeBlocking() {
    const blocking = findResumableRuntimeUpgrade(database);
    if (blocking === null) return null;
    const planFile = upgradePlanPath(planDirectory, blocking.upgrade_id);
    let plan;
    try {
      plan = readUpgradePlan(planFile, blocking.upgrade_id, {
        installationRoot,
        releaseRoot,
        provider,
      });
    } catch (error) {
      throw new Error(
        `Runtime upgrade ${blocking.upgrade_id} is ${blocking.state} but cannot resume: ${error.message}`,
      );
    }
    return executePlan(plan);
  }

  async function installedExecutorUpgrade(request) {
    const target = request?.target;
    if (!target || typeof target !== 'object' || Array.isArray(target)) {
      throw new TypeError('Upgrade request target is required.');
    }
    const blocking = findResumableRuntimeUpgrade(database);
    if (blocking !== null) {
      throw new Error(
        `Runtime upgrade ${blocking.upgrade_id} is already ${blocking.state}; restart the executor to resume it.`,
      );
    }
    assertLegacyServicesInactive(execFileSyncFn);
    const fromRelease = process.env.ZYLOS_RELEASE_REF || currentReleaseRef;
    if (typeof fromRelease !== 'string' || fromRelease.length === 0) {
      throw new Error('Current executor release identity is unavailable.');
    }
    if (fromRelease === target.release) {
      throw new Error('Target executor release must differ from the active release.');
    }
    const prepared = prepareRelease({
      source: target.downloaded_source,
      releaseRef: target.release,
      branch: target.branch ?? null,
      releaseRoot,
      execFileSyncFn,
    });
    const upgradeId = `upgrade-${crypto.randomUUID()}`;
    const legacyBatch = Object.freeze({ batch_id: `${upgradeId}-empty`, records: Object.freeze([]) });
    const legacyQueueFile = path.join(installationRoot, 'runtime', 'upgrade-input', `${upgradeId}.json`);
    atomicJson(legacyQueueFile, legacyBatch);
    if (fromRelease === prepared.releaseRef) {
      fs.rmSync(prepared.releasePath, { recursive: true, force: true });
      throw new Error('Target executor release must differ from the active release.');
    }
    if (!fs.existsSync(activeReleaseFile)) {
      atomicJson(activeReleaseFile, {
        release_ref: fromRelease,
        release_path: packageRoot,
        upgrade_id: null,
      });
    }
    const preflight = {
      upgrade_id: upgradeId,
      from_release: fromRelease,
      to_release: prepared.releaseRef,
      scope: { kind: 'installation', bot_id: null },
      checks: {
        sqlite_integrity: 'ok',
        codex_transport: 'official_app_server_only',
        delivery_contract: 'zylos.delivery-command@1.1',
        workspace_lease_fencing: 'intact',
        retention_cleanup: 'intact',
        normal_runtime_paths: 'new_only',
      },
    };
    const plan = Object.freeze({
      schema_version: 1,
      upgrade_id: upgradeId,
      preflight,
      legacy_batch: legacyBatch,
      legacy_queue_file: legacyQueueFile,
      from_release_path: packageRoot,
      to_release_path: prepared.releasePath,
      from_package_version: readPackageRelease(packageRoot),
      to_package_version: prepared.packageVersion,
      provider,
    });
    atomicJson(upgradePlanPath(planDirectory, upgradeId), plan);
    return executePlan(plan);
  }

  Object.defineProperty(installedExecutorUpgrade, 'resumeBlocking', {
    enumerable: true,
    value: resumeBlocking,
  });
  return Object.freeze(installedExecutorUpgrade);
}
