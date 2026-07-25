#!/usr/bin/env node

/** Remove only a completed, disposable exact-base bootstrap staging directory. */
import fs from 'node:fs';
import path from 'node:path';
import { pathToFileURL } from 'node:url';

function explicitDirectory(name, value) {
  if (typeof value !== 'string' || !path.isAbsolute(value) || path.parse(value).root === value) {
    throw new TypeError(`${name} must be an explicit absolute non-root directory`);
  }
  const resolved = fs.realpathSync(value);
  if (!fs.statSync(resolved).isDirectory()) throw new TypeError(`${name} must be a directory`);
  return resolved;
}

function contains(root, candidate) {
  const relative = path.relative(root, candidate);
  return relative === '' || (!relative.startsWith(`..${path.sep}`) && relative !== '..');
}

export function cleanupCompletedBootstrapStaging({ zylosDir }) {
  const installationRoot = explicitDirectory('zylosDir', zylosDir);
  const runtimeRoot = explicitDirectory('runtimeRoot', path.join(installationRoot, 'runtime'));
  const manifestFile = path.join(runtimeRoot, 'base-executor-bootstrap.json');
  const manifest = JSON.parse(fs.readFileSync(manifestFile, 'utf8'));
  if (manifest?.state !== 'supervisor_started') {
    throw new Error('Bootstrap staging cleanup requires a supervisor_started manifest.');
  }
  const targetRelease = explicitDirectory('target_release_path', manifest.target_release_path);
  const stagingRoot = fs.realpathSync(path.dirname(targetRelease));
  if (path.dirname(stagingRoot) !== runtimeRoot
    || !/^install-inventory\.[A-Za-z0-9]+$/.test(path.basename(stagingRoot))
    || path.basename(targetRelease) !== 'target-release') {
    throw new Error('Bootstrap staging path is outside the managed disposable inventory root.');
  }
  try {
    const active = JSON.parse(fs.readFileSync(path.join(runtimeRoot, 'active-release.json'), 'utf8'));
    const activeRelease = fs.realpathSync(active.release_path);
    if (contains(stagingRoot, activeRelease)) {
      throw new Error('Refusing to remove staging that contains the active executor release.');
    }
  } catch (error) {
    if (error?.code !== 'ENOENT') throw error;
  }
  fs.rmSync(stagingRoot, { recursive: true });
  return Object.freeze({ removed_staging_path: stagingRoot });
}

function argument(name) {
  const index = process.argv.indexOf(name);
  return index === -1 ? null : process.argv[index + 1];
}

if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) {
  try {
    process.stdout.write(`${JSON.stringify(cleanupCompletedBootstrapStaging({
      zylosDir: argument('--zylos-dir'),
    }))}\n`);
  } catch (error) {
    console.error(error);
    process.exitCode = 1;
  }
}
