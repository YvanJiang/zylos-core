#!/usr/bin/env node

/** One-time exact-base to executor lifecycle bootstrap, owned by Global26. */
import fs from 'node:fs';
import path from 'node:path';
import { createRequire } from 'node:module';
import { pathToFileURL } from 'node:url';

import { createInstalledExecutorUpgradeHandler } from '../runtime/migration/installed-executor-upgrade.js';

function requireDirectory(name, value) {
  if (typeof value !== 'string' || !path.isAbsolute(value)
    || path.parse(value).root === value || !fs.statSync(value).isDirectory()) {
    throw new TypeError(`${name} must be an explicit absolute non-root directory`);
  }
  return fs.realpathSync(value);
}

function configuredProvider(zylosDir) {
  try {
    const config = JSON.parse(fs.readFileSync(path.join(zylosDir, '.zylos', 'config.json'), 'utf8'));
    return config.runtime === 'codex' ? 'codex' : 'claude';
  } catch {
    return 'claude';
  }
}

export async function runBaseToExecutorBootstrap({
  zylosDir,
  fromReleasePath,
  targetReleasePath,
  Database = null,
  createHandler = createInstalledExecutorUpgradeHandler,
} = {}) {
  const installationRoot = requireDirectory('zylosDir', zylosDir);
  const fromPath = requireDirectory('fromReleasePath', fromReleasePath);
  const targetPath = requireDirectory('targetReleasePath', targetReleasePath);
  const DatabaseConstructor = Database ?? createRequire(path.join(
    installationRoot, '.claude', 'skills', 'comm-bridge', 'package.json',
  ))('better-sqlite3');
  const database = new DatabaseConstructor(path.join(installationRoot, 'comm-bridge', 'c4.db'));
  try {
    const handler = createHandler({
      database,
      Database: DatabaseConstructor,
      zylosDir: installationRoot,
      currentReleasePath: fromPath,
      currentReleaseRef: 'branch:exact-base-bootstrap',
      provider: configuredProvider(installationRoot),
      allowLegacyFromRelease: true,
    });
    const result = await handler({
      action: 'upgrade',
      target: {
        release: 'branch:executor-lifecycle',
        branch: 'executor-lifecycle',
        downloaded_source: targetPath,
      },
    });
    if (result?.state !== 'committed') {
      throw new Error(
        `Executor lifecycle bootstrap did not commit; rollback state is ${result?.state ?? 'unknown'}: ${result?.error ?? 'unknown error'}`,
      );
    }
    if (result.success !== true) {
      const error = new Error(
        `Executor lifecycle bootstrap committed but post-commit completion failed: ${result.error ?? 'unknown error'}`,
      );
      error.committed = true;
      throw error;
    }
    return result;
  } finally {
    database.close();
  }
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
    });
    process.stdout.write(`${JSON.stringify(result)}\n`);
  } catch (error) {
    console.error(error);
    process.exitCode = error?.committed === true ? 2 : 1;
  }
}
