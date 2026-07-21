#!/usr/bin/env node

/** Read-only installer inventory. It never starts, stops, or mutates a service. */
import fs from 'node:fs';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import { pathToFileURL } from 'node:url';
import {
  RETIRED_PM2_SERVICE_NAMES,
  retiredPm2ServicePaths,
} from '../runtime/migration/retired-pm2-identities.js';

function requireDirectory(name, value) {
  if (typeof value !== 'string' || !path.isAbsolute(value) || path.parse(value).root === value
    || !fs.statSync(value).isDirectory()) {
    throw new TypeError(`${name} must be an explicit absolute non-root directory`);
  }
  return fs.realpathSync(value);
}

function readPm2(execFileSyncFn) {
  try {
    const parsed = JSON.parse(String(execFileSyncFn('pm2', ['jlist'], {
      encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'], timeout: 30_000,
    })));
    return Array.isArray(parsed) ? parsed : null;
  } catch {
    return null;
  }
}

export function inspectInstalledRuntime({
  zylosDir,
  installedRoot,
  execFileSyncFn = execFileSync,
}) {
  const root = requireDirectory('zylosDir', zylosDir);
  const packageRoot = requireDirectory('installedRoot', installedRoot);
  const ecosystemFile = path.join(root, 'pm2', 'ecosystem.config.cjs');
  const ecosystemExists = fs.existsSync(ecosystemFile);
  const ecosystem = ecosystemExists ? fs.readFileSync(ecosystemFile, 'utf8') : '';
  const executorPackage = fs.existsSync(path.join(packageRoot, 'runtime', 'executor', 'launcher.js'));
  const executorConfig = ecosystemExists && /\bzylos-executor\b/.test(ecosystem);
  const legacyConfig = ecosystemExists && RETIRED_PM2_SERVICE_NAMES
    .some((name) => new RegExp(`['\"]${name}['\"]`).test(ecosystem));
  const legacyDatabase = fs.existsSync(path.join(root, 'comm-bridge', 'c4.db'));
  const pm2 = readPm2(execFileSyncFn);
  const reasons = [];
  if (pm2 === null) reasons.push('pm2_inventory_unavailable');
  const legacyPm2 = [];
  const collisions = [];
  const expectedLegacyPaths = retiredPm2ServicePaths(root);
  for (const processInfo of pm2 ?? []) {
    const expected = expectedLegacyPaths.get(processInfo?.name);
    if (!expected) continue;
    const actual = processInfo.pm2_env?.pm_exec_path ?? processInfo.pm_exec_path;
    if (typeof actual !== 'string' || path.resolve(actual) !== expected) {
      collisions.push(processInfo.name);
    } else {
      legacyPm2.push(processInfo.name);
    }
  }
  if (collisions.length > 0) reasons.push('legacy_pm2_name_collision');
  if (legacyPm2.length > 0 && !legacyConfig) reasons.push('legacy_pm2_without_service_config');
  if (legacyDatabase && !legacyConfig && !executorConfig) {
    reasons.push('legacy_database_without_service_config');
  }
  if (executorConfig !== executorPackage) reasons.push('executor_package_config_mismatch');
  if (ecosystemExists && !executorConfig && !legacyConfig) reasons.push('unrecognized_service_config');
  if (executorConfig && (legacyConfig || legacyPm2.length > 0)) reasons.push('mixed_runtime_identity');
  let state;
  if (reasons.length > 0) state = 'ambiguous';
  else if (executorConfig && executorPackage) state = 'executor';
  else if (legacyConfig) state = 'legacy';
  else state = 'uninitialized';
  return Object.freeze({
    state,
    reasons: Object.freeze(reasons),
    executor_package: executorPackage,
    executor_config: executorConfig,
    legacy_config: legacyConfig,
    legacy_database: legacyDatabase,
    legacy_pm2: Object.freeze(legacyPm2),
  });
}

function argument(name) {
  const index = process.argv.indexOf(name);
  return index === -1 ? null : process.argv[index + 1];
}

if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) {
  const result = inspectInstalledRuntime({
    zylosDir: argument('--zylos-dir'),
    installedRoot: argument('--installed-root'),
  });
  process.stdout.write(`${JSON.stringify(result)}\n`);
}
