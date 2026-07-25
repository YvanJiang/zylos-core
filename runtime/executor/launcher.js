#!/usr/bin/env node

import fs from 'node:fs';
import path from 'node:path';
import { pathToFileURL } from 'node:url';

import { runForwardedNode } from './forwarded-node-process.js';

export function requireSafeReleasePath(releasePath) {
  if (typeof releasePath !== 'string' || !path.isAbsolute(releasePath)
    || path.parse(releasePath).root === releasePath) {
    throw new Error('Active executor release path must be an explicit absolute non-root path.');
  }
  const resolved = path.resolve(releasePath);
  const entry = path.join(resolved, 'runtime', 'executor', 'daemon.js');
  if (!fs.statSync(entry).isFile()) {
    throw new Error(`Active executor release has no daemon entrypoint: ${entry}`);
  }
  return { entry, releasePath: resolved };
}

export function resolveActiveRelease({ zylosDir, packageRoot, useActiveRelease = true }) {
  if (useActiveRelease === false) {
    const release = requireSafeReleasePath(packageRoot);
    return { ...release, releaseRef: null, upgradeId: null };
  }
  const activeFile = path.join(zylosDir, 'runtime', 'active-release.json');
  try {
    const active = JSON.parse(fs.readFileSync(activeFile, 'utf8'));
    const release = requireSafeReleasePath(active.release_path);
    if (typeof active.release_ref !== 'string' || active.release_ref.length === 0) {
      throw new Error('Active executor release has no release_ref.');
    }
    return { ...release, releaseRef: active.release_ref, upgradeId: active.upgrade_id ?? null };
  } catch (error) {
    if (error?.code !== 'ENOENT') throw error;
    const release = requireSafeReleasePath(packageRoot);
    return { ...release, releaseRef: null, upgradeId: null };
  }
}

function main() {
  const zylosDir = process.env.ZYLOS_DIR;
  const packageRoot = process.env.ZYLOS_PACKAGE_ROOT || path.resolve(import.meta.dirname, '..', '..');
  if (typeof zylosDir !== 'string' || !path.isAbsolute(zylosDir)
    || path.parse(zylosDir).root === zylosDir) {
    throw new Error('ZYLOS_DIR must be an explicit absolute non-root path.');
  }
  const active = resolveActiveRelease({
    zylosDir,
    packageRoot,
    useActiveRelease: process.env.ZYLOS_EXECUTOR_IGNORE_ACTIVE_RELEASE !== '1',
  });
  runForwardedNode(active.entry, [], {
    cwd: zylosDir,
    env: {
      ...process.env,
      ZYLOS_DIR: zylosDir,
      ZYLOS_RELEASE_REF: active.releaseRef ?? '',
      ZYLOS_UPGRADE_ID: active.upgradeId ?? '',
    },
  });
}

if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) {
  try {
    main();
  } catch (error) {
    console.error(error);
    process.exitCode = 1;
  }
}
