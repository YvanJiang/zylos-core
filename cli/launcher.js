#!/usr/bin/env node

import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { pathToFileURL } from 'node:url';

import { runForwardedNode } from '../runtime/executor/forwarded-node-process.js';

export function requireCliEntry(releasePath) {
  if (typeof releasePath !== 'string' || !path.isAbsolute(releasePath)
    || path.parse(releasePath).root === releasePath) {
    throw new Error('CLI release path must be an explicit absolute non-root path.');
  }
  const entry = path.join(path.resolve(releasePath), 'cli', 'zylos.js');
  if (!fs.statSync(entry).isFile()) throw new Error(`CLI release entrypoint is missing: ${entry}`);
  return entry;
}

export function resolveCliEntry({ zylosDir, packageRoot }) {
  const activeReleaseFile = path.join(zylosDir, 'runtime', 'active-release.json');
  try {
    const active = JSON.parse(fs.readFileSync(activeReleaseFile, 'utf8'));
    return requireCliEntry(active.release_path);
  } catch (error) {
    if (error?.code !== 'ENOENT') throw error;
    return requireCliEntry(packageRoot);
  }
}

function main() {
  const zylosDir = process.env.ZYLOS_DIR || path.join(os.homedir(), 'zylos');
  const packageRoot = path.resolve(import.meta.dirname, '..');
  const entry = resolveCliEntry({ zylosDir, packageRoot });
  runForwardedNode(entry, process.argv.slice(2), {
    env: { ...process.env, ZYLOS_PACKAGE_ROOT: packageRoot },
  });
}

let invokedEntry = null;
try {
  invokedEntry = process.argv[1] ? fs.realpathSync(process.argv[1]) : null;
} catch {
  invokedEntry = null;
}

if (invokedEntry && import.meta.url === pathToFileURL(invokedEntry).href) {
  try {
    main();
  } catch (error) {
    console.error(error);
    process.exitCode = 1;
  }
}
