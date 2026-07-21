#!/usr/bin/env node

/** Durable one-time exact-base to executor lifecycle bootstrap, owned by Global26. */
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { createRequire } from 'node:module';
import { execFileSync } from 'node:child_process';
import { pathToFileURL } from 'node:url';

import { startExecutorService } from '../cli/lib/executor-service-lifecycle.js';
import { createInstalledExecutorUpgradeHandler } from '../runtime/migration/installed-executor-upgrade.js';
import {
  readChannelAuthorityManifest,
  readDurableBootstrapAuthority,
} from '../runtime/migration/channel-authority-manifest.js';

const MANIFEST_NAME = 'base-executor-bootstrap.json';

function requireDirectory(name, value) {
  if (typeof value !== 'string' || !path.isAbsolute(value)
    || path.parse(value).root === value || !fs.statSync(value).isDirectory()) {
    throw new TypeError(`${name} must be an explicit absolute non-root directory`);
  }
  return fs.realpathSync(value);
}

function requireFile(name, value) {
  if (typeof value !== 'string' || !path.isAbsolute(value)
    || path.parse(value).root === value || !fs.statSync(value).isFile()) {
    throw new TypeError(`${name} must be an explicit absolute non-root file`);
  }
  return fs.realpathSync(value);
}

function sha256(file) {
  return crypto.createHash('sha256').update(fs.readFileSync(file)).digest('hex');
}

function atomicManifest(file, document) {
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

function configuredProvider(zylosDir) {
  try {
    const config = JSON.parse(fs.readFileSync(path.join(zylosDir, '.zylos', 'config.json'), 'utf8'));
    return config.runtime === 'codex' ? 'codex' : 'claude';
  } catch {
    return 'claude';
  }
}

function installPackage({ tarball, installMode, restore, execFileSyncFn }) {
  const npmArgs = ['install', '-g', '--install-links'];
  if (restore) npmArgs.push('--ignore-scripts');
  npmArgs.push(tarball);
  if (installMode === 'sudo') {
    execFileSyncFn('sudo', ['env', 'ZYLOS_PACKAGE_PREPARE=1', 'npm', ...npmArgs], {
      stdio: 'pipe', timeout: 15 * 60_000,
    });
  } else if (installMode === 'direct') {
    execFileSyncFn('npm', npmArgs, {
      env: { ...process.env, ZYLOS_PACKAGE_PREPARE: '1' },
      stdio: 'pipe', timeout: 15 * 60_000,
    });
  } else {
    throw new TypeError('installMode must be direct or sudo');
  }
}

function createPackageLifecycle({ manifestFile, manifest, execFileSyncFn }) {
  function update(state) {
    manifest = { ...manifest, state, updated_at: new Date().toISOString() };
    atomicManifest(manifestFile, manifest);
  }
  return Object.freeze({
    async activate() {
      if (sha256(manifest.target_package_tarball) !== manifest.target_package_sha256) {
        throw new Error('Staged executor package hash changed before activation.');
      }
      installPackage({
        tarball: manifest.target_package_tarball,
        installMode: manifest.install_mode,
        restore: false,
        execFileSyncFn,
      });
      update('candidate_installed');
      return Object.freeze({ package_sha256: manifest.target_package_sha256, installed: true });
    },
    async restore() {
      if (sha256(manifest.from_package_tarball) !== manifest.from_package_sha256) {
        throw new Error('Exact-base rollback package hash changed before restoration.');
      }
      installPackage({
        tarball: manifest.from_package_tarball,
        installMode: manifest.install_mode,
        restore: true,
        execFileSyncFn,
      });
      update('base_restored');
      return Object.freeze({ package_sha256: manifest.from_package_sha256, restored: true });
    },
    mark(state) { update(state); },
    current() { return manifest; },
  });
}

function prepareManifest({
  manifestFile,
  fromReleasePath,
  targetReleasePath,
  fromPackageTarball,
  targetPackageTarball,
  channelAuthority,
  installMode,
}) {
  let existing = null;
  try { existing = JSON.parse(fs.readFileSync(manifestFile, 'utf8')); } catch (error) {
    if (error?.code !== 'ENOENT') throw error;
  }
  if (existing !== null && existing.state !== 'base_restored') {
    const requested = {
      from_release_path: requireDirectory('fromReleasePath', fromReleasePath),
      target_release_path: requireDirectory('targetReleasePath', targetReleasePath),
      from_package_tarball: requireFile('fromPackageTarball', fromPackageTarball),
      target_package_tarball: requireFile('targetPackageTarball', targetPackageTarball),
      channel_authority_source_path: channelAuthority.path,
      channel_authority_sha256: channelAuthority.sha256,
      install_mode: installMode,
    };
    for (const [key, value] of Object.entries(requested)) {
      if (existing[key] !== value) throw new Error(`Existing bootstrap manifest conflicts on ${key}.`);
    }
    return existing;
  }
  if (existing?.state === 'base_restored') {
    fs.renameSync(manifestFile, `${manifestFile}.rolled-back-${Date.now()}`);
  }
  const manifest = {
    schema_version: 1,
    state: 'prepared',
    from_release_path: requireDirectory('fromReleasePath', fromReleasePath),
    target_release_path: requireDirectory('targetReleasePath', targetReleasePath),
    from_package_tarball: requireFile('fromPackageTarball', fromPackageTarball),
    target_package_tarball: requireFile('targetPackageTarball', targetPackageTarball),
    channel_authority_source_path: channelAuthority.path,
    channel_authority: channelAuthority.document,
    from_package_sha256: sha256(fromPackageTarball),
    target_package_sha256: sha256(targetPackageTarball),
    channel_authority_sha256: channelAuthority.sha256,
    channel_authority_raw_sha256: channelAuthority.raw_sha256,
    channel_authority_provider_binding: channelAuthority.provider_binding,
    install_mode: installMode,
    created_at: new Date().toISOString(),
    updated_at: new Date().toISOString(),
  };
  atomicManifest(manifestFile, manifest);
  return manifest;
}

async function startCommittedExecutor({ zylosDir, startService, execFileSyncFn }) {
  const executable = String(execFileSyncFn('sh', ['-lc', 'command -v zylos'], {
    encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'], timeout: 30_000,
  })).trim();
  if (!executable) throw new Error('Committed executor package has no zylos executable.');
  const packageRoot = path.dirname(path.dirname(fs.realpathSync(executable)));
  const previous = process.env.ZYLOS_PACKAGE_ROOT;
  process.env.ZYLOS_PACKAGE_ROOT = packageRoot;
  try {
    const result = await startService({ zylosDir, execFileSyncFn });
    if (result?.ok !== true || typeof result.serviceInstanceId !== 'string') {
      throw new Error(`Committed executor supervisor did not become healthy: ${result?.error ?? 'unknown'}`);
    }
    return result;
  } finally {
    if (previous === undefined) delete process.env.ZYLOS_PACKAGE_ROOT;
    else process.env.ZYLOS_PACKAGE_ROOT = previous;
  }
}

export async function runBaseToExecutorBootstrap({
  zylosDir,
  fromReleasePath = null,
  targetReleasePath = null,
  fromPackageTarball = null,
  targetPackageTarball = null,
  channelAuthorityManifest = null,
  installMode = null,
  resume = false,
  Database = null,
  createHandler = createInstalledExecutorUpgradeHandler,
  execFileSyncFn = execFileSync,
  startService = startExecutorService,
  handlerOverrides = {},
} = {}) {
  const installationRoot = requireDirectory('zylosDir', zylosDir);
  const manifestFile = path.join(installationRoot, 'runtime', MANIFEST_NAME);
  const managedLifecycle = fromPackageTarball !== null || resume;
  let manifest = null;
  let packageLifecycle = null;
  let authority = null;
  let fromPath;
  let targetPath;
  if (managedLifecycle) {
    if (resume) {
      const durableAuthority = readDurableBootstrapAuthority(manifestFile, {
        expectedPath: manifestFile,
      });
      manifest = durableAuthority.manifest;
      authority = durableAuthority.authority;
    } else {
      authority = readChannelAuthorityManifest(channelAuthorityManifest, {
        expectedPath: path.join(installationRoot, 'runtime', 'channel-authority.json'),
      });
      manifest = prepareManifest({
        manifestFile, fromReleasePath, targetReleasePath,
        fromPackageTarball, targetPackageTarball, channelAuthority: authority, installMode,
      });
    }
    fromPath = requireDirectory('fromReleasePath', manifest.from_release_path);
    targetPath = requireDirectory('targetReleasePath', manifest.target_release_path);
    packageLifecycle = createPackageLifecycle({ manifestFile, manifest, execFileSyncFn });
  } else {
    fromPath = requireDirectory('fromReleasePath', fromReleasePath);
    targetPath = requireDirectory('targetReleasePath', targetReleasePath);
    if (channelAuthorityManifest !== null) {
      authority = readChannelAuthorityManifest(channelAuthorityManifest, {
        expectedPath: path.join(installationRoot, 'runtime', 'channel-authority.json'),
      });
    }
  }
  const DatabaseConstructor = Database ?? createRequire(path.join(
    installationRoot, '.claude', 'skills', 'comm-bridge', 'package.json',
  ))('better-sqlite3');
  const database = new DatabaseConstructor(path.join(installationRoot, 'comm-bridge', 'c4.db'));
  let result;
  try {
    const handler = createHandler({
      database,
      Database: DatabaseConstructor,
      zylosDir: installationRoot,
      currentReleasePath: fromPath,
      currentReleaseRef: 'branch:exact-base-bootstrap',
      provider: configuredProvider(installationRoot),
      allowLegacyFromRelease: true,
      legacyChannelAuthority: authority,
      packageLifecycle,
      execFileSyncFn,
      ...handlerOverrides,
    });
    result = typeof handler.resumeBlocking === 'function' ? await handler.resumeBlocking() : null;
    if (result === null) {
      const current = packageLifecycle?.current();
      let activeRelease = null;
      try {
        activeRelease = JSON.parse(fs.readFileSync(
          path.join(installationRoot, 'runtime', 'active-release.json'), 'utf8',
        ));
      } catch (error) {
        if (error?.code !== 'ENOENT') throw error;
      }
      if (current?.state === 'committed' || current?.state === 'supervisor_started'
        || (current?.state === 'candidate_installed'
          && activeRelease?.release_ref === 'branch:executor-lifecycle')) {
        result = { success: true, state: 'committed' };
      } else if (current?.state === 'base_restored') {
        throw new Error('Executor lifecycle bootstrap previously rolled back to exact base.');
      } else {
        result = await handler({
          action: 'upgrade',
          target: {
            release: 'branch:executor-lifecycle',
            branch: 'executor-lifecycle',
            downloaded_source: targetPath,
          },
        });
      }
    }
    if (result?.state !== 'committed') {
      const error = new Error(
        `Executor lifecycle bootstrap did not commit; rollback state is ${result?.state ?? 'unknown'}: ${result?.error ?? 'unknown error'}`,
      );
      error.rollbackPending = result?.state === 'rollback_failed';
      error.rollbackState = result?.state ?? 'unknown';
      throw error;
    }
    if (result.success !== true) {
      const error = new Error(
        `Executor lifecycle bootstrap committed but post-commit completion failed: ${result.error ?? 'unknown error'}`,
      );
      error.committed = true;
      throw error;
    }
    packageLifecycle?.mark('committed');
  } finally {
    database.close();
  }
  if (managedLifecycle) {
    try {
      const health = await startCommittedExecutor({
        zylosDir: installationRoot, startService, execFileSyncFn,
      });
      packageLifecycle.mark('supervisor_started');
      result = { ...result, executor_service_instance_id: health.serviceInstanceId };
    } catch (error) {
      error.committed = true;
      throw error;
    }
  }
  return result;
}

export function bootstrapFailureExitCode(error) {
  if (error?.committed === true) return 2;
  if (error?.rollbackPending === true) return 3;
  return 1;
}

function argument(name) {
  const index = process.argv.indexOf(name);
  return index === -1 ? null : process.argv[index + 1];
}

if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) {
  try {
    const result = await runBaseToExecutorBootstrap({
      zylosDir: argument('--zylos-dir'),
      fromReleasePath: argument('--from-release'),
      targetReleasePath: argument('--target-release'),
      fromPackageTarball: argument('--from-package'),
      targetPackageTarball: argument('--target-package'),
      channelAuthorityManifest: argument('--channel-authority-manifest'),
      installMode: argument('--install-mode'),
      resume: process.argv.includes('--resume'),
    });
    process.stdout.write(`${JSON.stringify(result)}\n`);
  } catch (error) {
    console.error(error);
    process.exitCode = bootstrapFailureExitCode(error);
  }
}
