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
import {
  cleanupObsoleteLifecycleArtifacts,
  legacyLifecycleArtifactPaths,
} from './legacy-lifecycle-artifacts.js';
import {
  fenceLegacyBaseBatch,
  readLegacyBaseBatch,
  reconcileLegacyBaseRollback,
} from './legacy-base-source.js';
import {
  isCertifiedChannelAuthority,
  validateChannelAuthorityManifest,
} from './channel-authority-manifest.js';
import { findResumableRuntimeUpgrade } from './upgrade-state.js';
import { issueExecutorStartFence } from '../executor/start-fence.js';
import {
  RETIRED_PM2_SERVICE_NAMES,
  retiredPm2ServicePaths,
} from './retired-pm2-identities.js';

function readPm2Processes(execFileSyncFn) {
  const parsed = JSON.parse(execFileSyncFn('pm2', ['jlist'], {
    encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'], timeout: 30_000,
  }));
  if (!Array.isArray(parsed)) throw new Error('PM2 process inventory must be an array.');
  return parsed;
}

export function inspectLegacyServiceRegistrations({ zylosDir, execFileSyncFn = execFileSync }) {
  const expected = retiredPm2ServicePaths(requireDirectory('zylosDir', zylosDir));
  const owned = [];
  const collisions = [];
  for (const processInfo of readPm2Processes(execFileSyncFn)) {
    if (!RETIRED_PM2_SERVICE_NAMES.includes(processInfo.name)) continue;
    const actualPath = processInfo.pm2_env?.pm_exec_path ?? processInfo.pm_exec_path;
    const expectedPath = expected.get(processInfo.name);
    if (typeof actualPath !== 'string' || path.resolve(actualPath) !== path.resolve(expectedPath)) {
      collisions.push(Object.freeze({ name: processInfo.name, actual_path: actualPath ?? null }));
      continue;
    }
    const status = processInfo.pm2_env?.status ?? 'unknown';
    owned.push(Object.freeze({
      name: processInfo.name,
      status,
      was_running: !['stopped', 'errored'].includes(status),
      script_path: path.resolve(actualPath),
    }));
  }
  if (collisions.length > 0) {
    throw new Error(`Refusing to control ambiguous PM2 service name collisions: ${collisions.map(({ name }) => name).join(', ')}`);
  }
  const duplicateNames = owned.filter(({ name }, index) => owned.findIndex((entry) => entry.name === name) !== index);
  if (duplicateNames.length > 0) {
    throw new Error(`Refusing to control duplicate PM2 service identities: ${duplicateNames.map(({ name }) => name).join(', ')}`);
  }
  return Object.freeze(owned);
}

function recordLegacyServiceState({ stateFile, upgradeId, stepId, services, now }) {
  const state = Object.freeze({
    schema_version: 2,
    upgrade_id: upgradeId,
    step_id: stepId,
    phase: 'ownership_recorded',
    recorded_at: now(),
    services: Object.freeze(services.map((service) => Object.freeze({
      name: service.name,
      observed_status: service.status,
      observed_was_running: service.was_running,
      observed_script_path: service.script_path,
      observed_stop_command: Object.freeze(['pm2', 'stop', service.name]),
      observed_delete_command: Object.freeze(['pm2', 'delete', service.name]),
      stop_state: service.was_running ? 'pending' : 'verified_inactive',
      delete_state: 'pending',
    }))),
    save_state: services.length === 0 ? 'not_required' : 'pending',
  });
  atomicJson(stateFile, state);
  return state;
}

function readRecordedLegacyServiceState({ stateFile, upgradeId, stepId }) {
  const state = JSON.parse(fs.readFileSync(stateFile, 'utf8'));
  if (state?.schema_version !== 2 || state.upgrade_id !== upgradeId || state.step_id !== stepId
    || !Array.isArray(state.services) || typeof state.phase !== 'string') {
    throw new Error('Legacy service ownership record conflicts with the upgrade step.');
  }
  if (!['pending', 'intent_recorded', 'verified_saved', 'not_required'].includes(state.save_state)) {
    throw new Error('Legacy service ownership record has an invalid PM2 save state.');
  }
  for (const service of state.services) {
    if (!service || typeof service.name !== 'string' || typeof service.observed_script_path !== 'string'
      || !['pending', 'intent_recorded', 'verified_inactive'].includes(service.stop_state)
      || !['pending', 'intent_recorded', 'verified_deleted'].includes(service.delete_state)) {
      throw new Error('Legacy service ownership record is malformed.');
    }
  }
  return state;
}

function updateRecordedLegacyServiceState(stateFile, state, patch) {
  const next = Object.freeze({ ...state, ...patch });
  atomicJson(stateFile, next);
  return next;
}

function verifyRecordedLegacyServices({ zylosDir, execFileSyncFn, state, allowMissing = false }) {
  const actual = inspectLegacyServiceRegistrations({ zylosDir, execFileSyncFn });
  const expectedByName = new Map(state.services.map((service) => [service.name, service]));
  for (const service of actual) {
    const expected = expectedByName.get(service.name);
    if (!expected) {
      throw new Error(`Unexpected legacy PM2 registration appeared after ownership audit: ${service.name}.`);
    }
    if (service.script_path !== expected.observed_script_path) {
      throw new Error(`Legacy PM2 service identity changed after ownership audit: ${service.name}.`);
    }
  }
  if (!allowMissing) {
    for (const service of state.services) {
      if (!actual.some(({ name }) => name === service.name)) {
        throw new Error(`Legacy PM2 service disappeared before verified retirement: ${service.name}.`);
      }
    }
  }
  return actual;
}

function stateWithService(state, name, patch) {
  return Object.freeze({
    ...state,
    services: Object.freeze(state.services.map((service) => (
      service.name === name ? Object.freeze({ ...service, ...patch }) : service
    ))),
  });
}

/**
 * Retire only services recorded as exact Zylos-owned PM2 identities.  The
 * audit document is intentionally a per-action recovery ledger: an intent
 * survives a crash/timeout and may only advance from a fresh PM2 proof, never
 * by replaying the uncertain destructive command.
 */
export function retireOwnedLegacyServices({
  zylosDir,
  upgradeId,
  stepId,
  stateFile,
  execFileSyncFn = execFileSync,
  now = () => new Date().toISOString(),
}) {
  if (typeof upgradeId !== 'string' || upgradeId.length === 0
    || typeof stepId !== 'string' || stepId.length === 0
    || typeof stateFile !== 'string' || !path.isAbsolute(stateFile)) {
    throw new TypeError('Legacy service retirement requires durable upgrade, step, and audit identities.');
  }
  let state;
  try {
    state = readRecordedLegacyServiceState({ stateFile, upgradeId, stepId });
  } catch (error) {
    if (error?.code !== 'ENOENT') throw error;
    // This inspection is read-only. Persist its exact ownership/path facts
    // before the first stop/delete/save command is allowed.
    state = recordLegacyServiceState({
      stateFile,
      upgradeId,
      stepId,
      services: inspectLegacyServiceRegistrations({ zylosDir, execFileSyncFn }),
      now,
    });
  }

  for (const service of state.services) {
    // A persisted delete intent is resolved in the delete phase from a fresh
    // absence proof. Do not mistake that intentionally unknown deletion for a
    // stop-phase disappearance on resume.
    if (service.delete_state !== 'pending') continue;
    let actual = verifyRecordedLegacyServices({ zylosDir, execFileSyncFn, state });
    const current = actual.find(({ name }) => name === service.name);
    if (service.stop_state === 'pending') {
      if (!current.was_running) {
        state = updateRecordedLegacyServiceState(
          stateFile, stateWithService(state, service.name, { stop_state: 'verified_inactive' }),
          { phase: 'services_quiescing', updated_at: now() },
        );
        continue;
      }
      state = updateRecordedLegacyServiceState(
        stateFile, stateWithService(state, service.name, {
          stop_state: 'intent_recorded', stop_intent_at: now(),
        }),
        { phase: 'services_quiescing', updated_at: now() },
      );
      execFileSyncFn('pm2', ['stop', service.name], { stdio: 'pipe', timeout: 30_000 });
      actual = verifyRecordedLegacyServices({ zylosDir, execFileSyncFn, state });
      if (actual.find(({ name }) => name === service.name).was_running) {
        throw new Error(`PM2 stop outcome is not inactive for ${service.name}.`);
      }
      state = updateRecordedLegacyServiceState(
        stateFile, stateWithService(state, service.name, { stop_state: 'verified_inactive' }),
        { phase: 'services_quiescing', updated_at: now() },
      );
      continue;
    }
    if (service.stop_state === 'intent_recorded') {
      if (current.was_running) {
        throw new Error(`Unknown PM2 stop outcome requires authorized disposition: ${service.name}.`);
      }
      state = updateRecordedLegacyServiceState(
        stateFile, stateWithService(state, service.name, { stop_state: 'verified_inactive' }),
        { phase: 'services_quiescing', updated_at: now() },
      );
      continue;
    }
    if (current.was_running) {
      throw new Error(`Legacy PM2 service became active after verified stop: ${service.name}.`);
    }
  }

  for (const service of state.services) {
    const actual = verifyRecordedLegacyServices({
      zylosDir,
      execFileSyncFn,
      state,
      allowMissing: state.services.some(({ delete_state: deleteState }) => deleteState !== 'pending'),
    });
    const current = actual.find(({ name }) => name === service.name);
    if (service.delete_state === 'pending') {
      if (!current || current.was_running) {
        throw new Error(`Legacy PM2 service is not safely inactive before delete: ${service.name}.`);
      }
      state = updateRecordedLegacyServiceState(
        stateFile, stateWithService(state, service.name, {
          delete_state: 'intent_recorded', delete_intent_at: now(),
        }),
        { phase: 'registrations_removing', updated_at: now() },
      );
      execFileSyncFn('pm2', ['delete', service.name], { stdio: 'pipe', timeout: 30_000 });
      const afterDelete = verifyRecordedLegacyServices({
        zylosDir, execFileSyncFn, state, allowMissing: true,
      });
      if (afterDelete.some(({ name }) => name === service.name)) {
        throw new Error(`PM2 delete outcome is not absent for ${service.name}.`);
      }
      state = updateRecordedLegacyServiceState(
        stateFile, stateWithService(state, service.name, { delete_state: 'verified_deleted' }),
        { phase: 'registrations_removing', updated_at: now() },
      );
      continue;
    }
    if (service.delete_state === 'intent_recorded') {
      if (current) {
        throw new Error(`Unknown PM2 delete outcome requires authorized disposition: ${service.name}.`);
      }
      state = updateRecordedLegacyServiceState(
        stateFile, stateWithService(state, service.name, { delete_state: 'verified_deleted' }),
        { phase: 'registrations_removing', updated_at: now() },
      );
    }
  }
  if (verifyRecordedLegacyServices({ zylosDir, execFileSyncFn, state, allowMissing: true }).length > 0) {
    throw new Error('Obsolete runtime registrations remained after verified retirement.');
  }
  if (state.save_state === 'pending') {
    state = updateRecordedLegacyServiceState(
      stateFile, state, { phase: 'saving_pm2_state', save_state: 'intent_recorded', updated_at: now() },
    );
    execFileSyncFn('pm2', ['save'], { stdio: 'pipe', timeout: 30_000 });
    state = updateRecordedLegacyServiceState(
      stateFile, state, { phase: 'retired', save_state: 'verified_saved', updated_at: now() },
    );
  } else if (state.save_state === 'intent_recorded') {
    throw new Error('Unknown PM2 save outcome requires authorized disposition.');
  }
  return Object.freeze({
    step_id: stepId,
    stopped: true,
    deleted: true,
    pm2_saved: state.save_state === 'verified_saved' || state.save_state === 'not_required',
    stopped_at: now(),
    services_were_running: Object.freeze(
      state.services.filter(({ observed_was_running: wasRunning }) => wasRunning).map(({ name }) => name),
    ),
  });
}

function requireDirectory(name, value) {
  if (typeof value !== 'string' || !path.isAbsolute(value)
    || path.parse(value).root === value) {
    throw new TypeError(`${name} must be an explicit absolute non-root path`);
  }
  const resolved = fs.realpathSync(value);
  if (!fs.statSync(resolved).isDirectory()) throw new TypeError(`${name} must be a directory`);
  return resolved;
}

export function atomicJson(file, document) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  const temporary = `${file}.${crypto.randomUUID()}.partial`;
  const descriptor = fs.openSync(temporary, 'wx', 0o600);
  try {
    fs.writeFileSync(descriptor, `${canonicalizeJson(document)}\n`);
    fs.fsyncSync(descriptor);
  } finally {
    fs.closeSync(descriptor);
  }
  try {
    fs.renameSync(temporary, file);
    const directory = fs.openSync(path.dirname(file), 'r');
    try { fs.fsyncSync(directory); } finally { fs.closeSync(directory); }
  } finally {
    fs.rmSync(temporary, { force: true });
  }
}

function deployManagedFile(source, destination) {
  if (!fs.statSync(source).isFile()) {
    throw new Error(`Managed runtime file is missing: ${source}`);
  }
  fs.mkdirSync(path.dirname(destination), { recursive: true });
  const temporary = `${destination}.${crypto.randomUUID()}.partial`;
  try {
    fs.copyFileSync(source, temporary, fs.constants.COPYFILE_EXCL);
    fs.chmodSync(temporary, 0o644);
    fs.renameSync(temporary, destination);
  } finally {
    fs.rmSync(temporary, { force: true });
  }
  return destination;
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
  const fromReleaseKind = plan.from_release_kind ?? 'executor';
  if (!['executor', 'legacy_base'].includes(fromReleaseKind)) {
    throw new Error(`Runtime upgrade plan ${expectedUpgradeId} has an invalid source kind.`);
  }
  if (fromReleaseKind === 'legacy_base') {
    if (typeof plan.legacy_source_fenced !== 'boolean'
      || typeof plan.legacy_observed_at !== 'string') {
      throw new Error(`Runtime upgrade plan ${expectedUpgradeId} lacks legacy source facts.`);
    }
    const authority = validateChannelAuthorityManifest(plan.channel_authority);
    if (authority.sha256 !== plan.channel_authority_sha256) {
      throw new Error(`Runtime upgrade plan ${expectedUpgradeId} channel authority hash conflicts.`);
    }
  }
  const fromPackageVersion = readPackageRelease(fromReleasePath, {
    legacySource: fromReleaseKind === 'legacy_base',
  });
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

function readPackageRelease(releasePath, { legacySource = false } = {}) {
  const packageDocument = JSON.parse(fs.readFileSync(path.join(releasePath, 'package.json'), 'utf8'));
  if (packageDocument.name !== 'zylos' || typeof packageDocument.version !== 'string') {
    throw new Error('Downloaded release is not a zylos-core package.');
  }
  const requiredEntries = legacySource ? [
    path.join(releasePath, 'cli', 'zylos.js'),
  ] : [
    path.join(releasePath, 'cli', 'launcher.js'),
    path.join(releasePath, 'cli', 'zylos.js'),
    path.join(releasePath, 'runtime', 'executor', 'daemon.js'),
    path.join(releasePath, 'runtime', 'executor', 'health-probe.js'),
    path.join(releasePath, 'runtime', 'executor', 'launcher.js'),
    path.join(releasePath, 'scripts', 'postinstall.js'),
    path.join(releasePath, 'templates', 'pm2', 'ecosystem.config.cjs'),
  ];
  for (const entry of requiredEntries) {
    if (!fs.statSync(entry).isFile()) throw new Error(`Downloaded release is missing ${entry}.`);
  }
  return packageDocument.version;
}

export function assertLegacyServicesInactive({ zylosDir, execFileSyncFn = execFileSync }) {
  const active = inspectLegacyServiceRegistrations({ zylosDir, execFileSyncFn })
    .filter(({ was_running: wasRunning }) => wasRunning);
  if (active.length > 0) {
    throw new Error(`Obsolete runtime services are still active: ${active.map(({ name }) => name).join(', ')}`);
  }
  return { stopped: true, stopped_at: new Date().toISOString() };
}

export function reconcileLegacyServicesForExecutorStart({
  zylosDir,
  upgradeId,
  execFileSyncFn = execFileSync,
}) {
  if (typeof upgradeId !== 'string' || upgradeId.length === 0) {
    throw new TypeError('Committed executor reconciliation requires an upgrade id.');
  }
  assertLegacyServicesInactive({ zylosDir, execFileSyncFn });
  // All destructive retirement belongs to the earlier, durable
  // legacy-source-invalidate effect. A registration here is either a race,
  // tamper, or an unproven prior action; never delete it from postcommit
  // cleanup because that would turn a timeout/crash into an unsafe replay.
  if (inspectLegacyServiceRegistrations({ zylosDir, execFileSyncFn }).length > 0) {
    throw new Error('Obsolete runtime registrations reappeared after verified retirement.');
  }
  const residual = legacyLifecycleArtifactPaths(zylosDir).filter((artifact) => fs.existsSync(artifact));
  if (residual.length > 0) {
    throw new Error(`Obsolete lifecycle artifacts remained after reconciliation: ${residual.join(', ')}`);
  }
  const issued = issueExecutorStartFence({
    zylosDir,
    proof: {
      kind: 'committed_reconciliation',
      upgrade_id: upgradeId,
      legacy_services_quiesced: true,
      legacy_registrations_absent: true,
      legacy_artifacts_reconciled: true,
    },
  });
  return Object.freeze({
    removed_services: Object.freeze([]),
    executor_start_fence_path: issued.path,
  });
}

function installReleaseDependencies({ releasePath, execFileSyncFn, prepareOnly, installRoot = true }) {
  const dependencyArgs = ['ci', '--omit=dev', '--no-audit', '--no-fund'];
  const isolatedHome = path.join(releasePath, '.zylos-package-prepare-home');
  if (installRoot) {
    execFileSyncFn('npm', dependencyArgs, {
      cwd: releasePath,
      env: {
        ...process.env,
        HOME: isolatedHome,
        ZYLOS_PACKAGE_PREPARE: prepareOnly ? '1' : '',
      },
      stdio: 'pipe',
      timeout: 15 * 60_000,
    });
  }
  for (const skill of ['comm-bridge', 'scheduler', 'web-console']) {
    const skillDirectory = path.join(releasePath, 'skills', skill);
    execFileSyncFn('npm', dependencyArgs, {
      cwd: skillDirectory,
      env: { ...process.env, HOME: isolatedHome },
      stdio: 'pipe',
      timeout: 15 * 60_000,
    });
    execFileSyncFn(process.execPath, ['-e', [
      "const Database=require('better-sqlite3');",
      "const db=new Database(':memory:');",
      "db.exec('CREATE TABLE dependency_probe(value INTEGER); INSERT INTO dependency_probe VALUES (1)');",
      "if(db.prepare('SELECT value FROM dependency_probe').pluck().get()!==1)process.exit(2);",
      'db.close();',
    ].join('')], {
      cwd: skillDirectory,
      env: { ...process.env, HOME: isolatedHome },
      stdio: 'pipe',
      timeout: 30_000,
    });
  }
  fs.rmSync(isolatedHome, { recursive: true, force: true });
}

function copyPackagePayload({ sourcePath, releasePath, packlistFn }) {
  const output = packlistFn('npm', ['pack', '--dry-run', '--json', '--ignore-scripts'], {
    cwd: sourcePath,
    encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'], timeout: 60_000,
  });
  const payload = JSON.parse(String(output));
  const files = payload?.[0]?.files;
  if (!Array.isArray(files) || files.length === 0) throw new Error('Candidate package payload is empty.');
  fs.mkdirSync(releasePath, { recursive: true });
  for (const descriptor of files) {
    const relative = descriptor?.path;
    if (typeof relative !== 'string' || relative.length === 0 || path.isAbsolute(relative)) {
      throw new Error('Candidate package payload contains an invalid path.');
    }
    const sourceFile = path.resolve(sourcePath, relative);
    if (sourceFile !== sourcePath && !sourceFile.startsWith(`${sourcePath}${path.sep}`)) {
      throw new Error('Candidate package payload escaped its source directory.');
    }
    const resolvedSource = fs.realpathSync(sourceFile);
    if (resolvedSource !== sourcePath && !resolvedSource.startsWith(`${sourcePath}${path.sep}`)) {
      throw new Error('Candidate package payload contains an escaping symbolic link.');
    }
    const stat = fs.statSync(resolvedSource);
    if (!stat.isFile()) throw new Error(`Candidate package payload is not a file: ${relative}`);
    const destination = path.join(releasePath, relative);
    fs.mkdirSync(path.dirname(destination), { recursive: true });
    fs.copyFileSync(resolvedSource, destination, fs.constants.COPYFILE_EXCL);
    fs.chmodSync(destination, stat.mode & 0o777);
  }
}

function prepareRelease({
  source, releaseRef, branch, releaseRoot, execFileSyncFn, packlistFn,
}) {
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
  copyPackagePayload({ sourcePath, releasePath, packlistFn });
  installReleaseDependencies({ releasePath, execFileSyncFn, prepareOnly: true });
  return Object.freeze({ packageVersion, releasePath, releaseRef });
}

function decorateReleaseAdapter(adapter, activeReleaseFile, execFileSyncFn, {
  ecosystemSource,
  ecosystemDestination,
  zylosDir,
  targetReleasePath,
  packageLifecycle,
}) {
  return Object.freeze({
    async activate(request) {
      const packageResult = packageLifecycle === null
        ? null : await packageLifecycle.activate(request);
      const result = await adapter.activate(request);
      atomicJson(activeReleaseFile, {
        release_ref: result.release_ref,
        release_path: result.release_path,
        upgrade_id: request.upgrade_id,
      });
      return { ...result, upgrade_id: request.upgrade_id, package_activation: packageResult };
    },
    async restore(request) {
      const packageResult = packageLifecycle === null
        ? null : await packageLifecycle.restore(request);
      const result = await adapter.restore(request);
      atomicJson(activeReleaseFile, {
        release_ref: result.release_ref,
        release_path: result.release_path,
        upgrade_id: null,
      });
      return { ...result, upgrade_id: null, package_restoration: packageResult };
    },
    async cleanup(request) {
      assertLegacyServicesInactive({ zylosDir, execFileSyncFn });
      const deployedConfig = deployManagedFile(ecosystemSource, ecosystemDestination);
      execFileSyncFn(process.execPath, [path.join(targetReleasePath, 'scripts', 'postinstall.js')], {
        cwd: targetReleasePath,
        env: {
          ...process.env,
          HOME: path.dirname(zylosDir),
          ZYLOS_DIR: zylosDir,
          ZYLOS_SKIP_POSTINSTALL: '',
          ZYLOS_POSTINSTALL_STRICT: '1',
          CI: '',
        },
        stdio: 'pipe',
        timeout: 15 * 60_000,
      });
      installReleaseDependencies({
        releasePath: path.join(zylosDir, '.claude'),
        execFileSyncFn,
        prepareOnly: false,
        installRoot: false,
      });
      const result = await adapter.cleanup(request);
      const lifecycleCleanup = cleanupObsoleteLifecycleArtifacts({
        zylosDir,
        upgradeState: 'committed',
      });
      const registrations = reconcileLegacyServicesForExecutorStart({
        zylosDir,
        upgradeId: request.upgrade_id,
        execFileSyncFn,
      });
      return Object.freeze({
        ...result,
        ...registrations,
        lifecycle_cleanup: lifecycleCleanup,
        deployed_ecosystem_config: deployedConfig,
      });
    },
  });
}

function startTargetHealthProcess({
  releasePath,
  zylosDir,
  provider,
  request,
  proofTimeoutMs,
  terminationGraceMs,
}) {
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
    let proofTimer = null;
    const clearProofTimer = () => {
      if (proofTimer !== null) {
        clearTimeout(proofTimer);
        proofTimer = null;
      }
    };
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
        const timer = setTimeout(() => child.kill('SIGKILL'), terminationGraceMs);
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
      clearProofTimer();
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
      clearProofTimer();
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
        clearProofTimer();
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
    proofTimer = setTimeout(() => {
      fail(new Error(`Target health proof timed out after ${proofTimeoutMs}ms.`));
    }, proofTimeoutMs);
    proofTimer.unref?.();
  });
}

function createDurableNoticeWaiter(database, { timeoutMs = 30_000, pollMs = 100 } = {}) {
  return Object.freeze({
    async deliver({ upgrade_id: upgradeId, notice_ids: noticeIds }) {
      const deadline = Date.now() + timeoutMs;
      for (;;) {
        const pending = noticeIds.filter((notice) => {
          const row = database.prepare(`
            SELECT outbox.status, outbox.result_json
            FROM runtime_legacy_migration_notices AS notice
            JOIN runtime_outbox AS outbox ON outbox.outbox_id = notice.outbox_id
            WHERE notice.upgrade_id = ? AND notice.legacy_kind = ?
              AND notice.legacy_record_id = ?
          `).get(upgradeId, notice.legacy_kind, notice.legacy_record_id);
          if (!row || row.status !== 'delivered' || row.result_json === null) return true;
          const proof = JSON.parse(row.result_json);
          return proof.status !== 'delivered';
        });
        if (pending.length === 0) return;
        if (Date.now() >= deadline) {
          throw new Error('Legacy uncertainty notices were not durably delivered before migration.');
        }
        await new Promise((resolve) => setTimeout(resolve, pollMs));
      }
    },
  });
}

export function createInstalledExecutorUpgradeHandler({
  database,
  Database,
  zylosDir,
  currentReleasePath,
  currentReleaseRef,
  provider,
  allowLegacyFromRelease = false,
  execFileSyncFn = execFileSync,
  packlistFn = execFileSync,
  packageLifecycle = null,
  legacyChannelAuthority = null,
  noticeDeliveryTimeoutMs = 30_000,
  startTargetHealth = startTargetHealthProcess,
  targetHealthProofTimeoutMs = 30_000,
  targetHealthTerminationGraceMs = 5_000,
  now = () => new Date().toISOString(),
}) {
  const installationRoot = requireDirectory('zylosDir', zylosDir);
  const packageRoot = requireDirectory('currentReleasePath', currentReleasePath);
  if (typeof Database !== 'function') throw new TypeError('Database must be a constructor');
  if (typeof packlistFn !== 'function') throw new TypeError('packlistFn must be a function');
  if (packageLifecycle !== null && (typeof packageLifecycle.activate !== 'function'
    || typeof packageLifecycle.restore !== 'function')) {
    throw new TypeError('packageLifecycle must expose activate and restore');
  }
  if (allowLegacyFromRelease && !isCertifiedChannelAuthority(legacyChannelAuthority, {
    installationRoot,
  })) {
    throw new Error('Exact-base migration requires verified channel prerequisite authority.');
  }
  const channelAuthority = allowLegacyFromRelease
    ? validateChannelAuthorityManifest(legacyChannelAuthority.document) : null;
  if (typeof startTargetHealth !== 'function') throw new TypeError('startTargetHealth must be a function');
  if (!Number.isSafeInteger(targetHealthProofTimeoutMs) || targetHealthProofTimeoutMs <= 0) {
    throw new TypeError('targetHealthProofTimeoutMs must be a positive safe integer');
  }
  if (!Number.isSafeInteger(targetHealthTerminationGraceMs) || targetHealthTerminationGraceMs <= 0) {
    throw new TypeError('targetHealthTerminationGraceMs must be a positive safe integer');
  }
  if (!Number.isSafeInteger(noticeDeliveryTimeoutMs) || noticeDeliveryTimeoutMs <= 0) {
    throw new TypeError('noticeDeliveryTimeoutMs must be a positive safe integer');
  }
  initializeRuntimePersistence(database);
  const activeReleaseFile = path.join(installationRoot, 'runtime', 'active-release.json');
  const snapshotDirectory = path.join(installationRoot, 'runtime', 'upgrade-snapshots');
  const releaseRoot = path.join(installationRoot, 'runtime', 'releases');
  const planDirectory = path.join(installationRoot, 'runtime', 'upgrade-plans');

  function legacyServiceStateFile(upgradeId) {
    return path.join(installationRoot, 'runtime', 'legacy-upgrade-audit', `${upgradeId}-services.json`);
  }

  function stopOwnedLegacyServices({ upgradeId, stepId }) {
    return retireOwnedLegacyServices({
      zylosDir: installationRoot,
      upgradeId,
      stepId,
      stateFile: legacyServiceStateFile(upgradeId),
      execFileSyncFn,
      now,
    });
  }

  function restoreLegacySourceData({ upgradeId, stepId, rollbackQueueSha256 }) {
    const sourceQueueFile = path.join(installationRoot, 'runtime', 'upgrade-input', `${upgradeId}.json`);
    const sourceQueuePayload = fs.readFileSync(sourceQueueFile);
    if (typeof rollbackQueueSha256 !== 'string'
      || crypto.createHash('sha256').update(sourceQueuePayload).digest('hex') !== rollbackQueueSha256) {
      throw new Error('Legacy restored source queue changed before reconciliation.');
    }
    const sourceQueue = JSON.parse(sourceQueuePayload.toString('utf8'));
    if (allowLegacyFromRelease) {
      reconcileLegacyBaseRollback({ database, rollbackBatch: sourceQueue });
    }
    const registered = inspectLegacyServiceRegistrations({ zylosDir: installationRoot, execFileSyncFn });
    if (registered.length > 0) {
      throw new Error('Legacy runtime registrations must remain removed after rollback.');
    }
    return Object.freeze({
      step_id: stepId,
      source_data_restored: true,
      legacy_runtime_remained_inactive: true,
      reconciled_at: now(),
    });
  }

  async function executePlan(plan) {
    if (!fs.existsSync(activeReleaseFile)) {
      atomicJson(activeReleaseFile, {
        release_ref: plan.preflight.from_release,
        release_path: plan.from_release_path,
        upgrade_id: null,
      });
    }
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
      {
        ecosystemSource: path.join(
          plan.to_release_path,
          'templates',
          'pm2',
          'ecosystem.config.cjs',
        ),
        ecosystemDestination: path.join(
          installationRoot,
          'pm2',
          'ecosystem.config.cjs',
        ),
        zylosDir: installationRoot,
        targetReleasePath: plan.to_release_path,
        packageLifecycle,
      },
    );
    const legacySourceAdapter = createLegacySourceQueueAdapter({
      sourceQueueFile: plan.legacy_queue_file,
      auditDirectory: path.join(installationRoot, 'runtime', 'legacy-upgrade-audit'),
      stopLegacyDispatcher: async ({ step_id: stepId }) => stopOwnedLegacyServices({
        upgradeId: plan.upgrade_id,
        stepId,
      }),
      verifyLegacySourceRestored: async ({
        step_id: stepId,
        rollback_queue_sha256: rollbackQueueSha256,
      }) => restoreLegacySourceData({
        upgradeId: plan.upgrade_id,
        stepId,
        rollbackQueueSha256,
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
            proofTimeoutMs: targetHealthProofTimeoutMs,
            terminationGraceMs: targetHealthTerminationGraceMs,
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
      noticeAdapter: createDurableNoticeWaiter(database, {
        timeoutMs: noticeDeliveryTimeoutMs,
      }),
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
      fs.rmSync(legacyServiceStateFile(plan.upgrade_id), { force: true });
    }
    return result;
  }

  async function resumeBlocking() {
    const blocking = findResumableRuntimeUpgrade(database);
    let upgradeId = blocking?.upgrade_id ?? null;
    if (upgradeId === null) {
      let orphanPlans = [];
      try {
        orphanPlans = fs.readdirSync(planDirectory)
          .filter((entry) => entry.endsWith('.json'))
          .map((entry) => entry.slice(0, -'.json'.length));
      } catch (error) {
        if (error?.code !== 'ENOENT') throw error;
      }
      if (orphanPlans.length === 0) return null;
      if (orphanPlans.length !== 1) {
        throw new Error('Multiple durable runtime upgrade plans require operator reconciliation.');
      }
      [upgradeId] = orphanPlans;
    }
    const planFile = upgradePlanPath(planDirectory, upgradeId);
    let plan;
    try {
      plan = readUpgradePlan(planFile, upgradeId, {
        installationRoot,
        releaseRoot,
        provider,
      });
    } catch (error) {
      throw new Error(
        `Runtime upgrade ${upgradeId} is ${blocking?.state ?? 'prepared'} but cannot resume: ${error.message}`,
      );
    }
    const rollbackReconciliationStarted = database.prepare(`
      SELECT 1 FROM runtime_upgrade_effects
      WHERE upgrade_id = ? AND step_key = 'legacy-source-reconciliation'
      LIMIT 1
    `).get(upgradeId) !== undefined;
    if (plan.from_release_kind === 'legacy_base' && !rollbackReconciliationStarted) {
      fenceLegacyBaseBatch({
        database,
        expectedBatch: plan.legacy_batch,
        batchId: plan.legacy_batch.batch_id,
        provider,
        channelAuthority: plan.channel_authority,
        observedAt: plan.legacy_observed_at,
      });
      if (plan.legacy_source_fenced !== true) {
        plan = Object.freeze({ ...plan, legacy_source_fenced: true });
        atomicJson(planFile, plan);
      }
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
      packlistFn,
    });
    const upgradeId = `upgrade-${crypto.randomUUID()}`;
    const observedAt = now();
    let legacyBatch;
    try {
      legacyBatch = allowLegacyFromRelease
        ? readLegacyBaseBatch({
          database, batchId: `${upgradeId}-legacy-base`, provider,
          channelAuthority, observedAt,
        })
        : Object.freeze({ batch_id: `${upgradeId}-empty`, records: Object.freeze([]) });
    } catch (error) {
      fs.rmSync(prepared.releasePath, { recursive: true, force: true });
      throw error;
    }
    const legacyQueueFile = path.join(installationRoot, 'runtime', 'upgrade-input', `${upgradeId}.json`);
    atomicJson(legacyQueueFile, legacyBatch);
    if (fromRelease === prepared.releaseRef) {
      fs.rmSync(prepared.releasePath, { recursive: true, force: true });
      throw new Error('Target executor release must differ from the active release.');
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
    let plan = Object.freeze({
      schema_version: 1,
      upgrade_id: upgradeId,
      preflight,
      legacy_batch: legacyBatch,
      legacy_queue_file: legacyQueueFile,
      from_release_path: packageRoot,
      to_release_path: prepared.releasePath,
      from_package_version: readPackageRelease(packageRoot, {
        legacySource: allowLegacyFromRelease,
      }),
      to_package_version: prepared.packageVersion,
      provider,
      from_release_kind: allowLegacyFromRelease ? 'legacy_base' : 'executor',
      ...(allowLegacyFromRelease ? {
        legacy_source_fenced: false,
        legacy_observed_at: observedAt,
        channel_authority: channelAuthority.document,
        channel_authority_sha256: channelAuthority.sha256,
      } : {}),
    });
    const planFile = upgradePlanPath(planDirectory, upgradeId);
    atomicJson(planFile, plan);
    if (allowLegacyFromRelease) {
      try {
        fenceLegacyBaseBatch({
          database,
          expectedBatch: legacyBatch,
          batchId: legacyBatch.batch_id,
          provider,
          channelAuthority,
          observedAt,
        });
      } catch (error) {
        fs.rmSync(planFile, { force: true });
        fs.rmSync(legacyQueueFile, { force: true });
        fs.rmSync(prepared.releasePath, { recursive: true, force: true });
        throw error;
      }
      plan = Object.freeze({ ...plan, legacy_source_fenced: true });
      atomicJson(planFile, plan);
    }
    return executePlan(plan);
  }

  Object.defineProperty(installedExecutorUpgrade, 'resumeBlocking', {
    enumerable: true,
    value: resumeBlocking,
  });
  return Object.freeze(installedExecutorUpgrade);
}
