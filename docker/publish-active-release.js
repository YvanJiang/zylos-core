#!/usr/bin/env node
import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { pathToFileURL } from 'node:url';

function requireDirectory(name, value) {
  if (typeof value !== 'string' || !path.isAbsolute(value)
    || path.parse(value).root === value) {
    throw new TypeError(`${name} must be an explicit absolute non-root path`);
  }
  const resolved = path.resolve(value);
  if (!fs.statSync(resolved).isDirectory()) {
    throw new Error(`${name} must be an existing directory: ${resolved}`);
  }
  return resolved;
}

function writeAtomicJson(file, document) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  const temporary = `${file}.${crypto.randomUUID()}.partial`;
  const descriptor = fs.openSync(temporary, 'wx', 0o600);
  try {
    fs.writeFileSync(descriptor, `${JSON.stringify(document)}\n`);
    fs.fsyncSync(descriptor);
  } finally {
    fs.closeSync(descriptor);
  }
  try {
    fs.renameSync(temporary, file);
  } finally {
    fs.rmSync(temporary, { force: true });
  }
}

function safeReleaseName(value) {
  return value.replace(/[^A-Za-z0-9._-]+/g, '-').replace(/^-+|-+$/g, '') || 'docker-current';
}

export function publishDockerActiveRelease({
  zylosDir = process.argv[2] || process.env.ZYLOS_DIR || path.join(os.homedir(), 'zylos'),
  packageRoot = process.argv[3] || process.env.ZYLOS_PACKAGE_ROOT,
  hostname = process.env.HOSTNAME || os.hostname(),
  upgradeId = process.env.ZYLOS_DOCKER_START_FENCE_UPGRADE_ID || 'docker-entrypoint',
} = {}) {
  const resolvedZylosDir = requireDirectory('zylosDir', zylosDir);
  const resolvedPackageRoot = requireDirectory('packageRoot', packageRoot);
  const packageDocument = JSON.parse(fs.readFileSync(path.join(resolvedPackageRoot, 'package.json'), 'utf8'));
  if (packageDocument.name !== 'zylos' || typeof packageDocument.version !== 'string') {
    throw new Error('Core package root is not a zylos release.');
  }
  for (const entry of [
    path.join(resolvedPackageRoot, 'runtime', 'executor', 'daemon.js'),
    path.join(resolvedPackageRoot, 'skills', 'comm-bridge', 'scripts', 'c4-receive.js'),
  ]) {
    if (!fs.statSync(entry).isFile()) {
      throw new Error(`Core package root is missing required runtime entry: ${entry}`);
    }
  }

  const releaseRef = `docker-${packageDocument.version}-${safeReleaseName(hostname)}`;
  const releasesDir = path.join(resolvedZylosDir, 'runtime', 'releases');
  const releasePath = path.join(releasesDir, safeReleaseName(releaseRef));
  const temporary = path.join(releasesDir, `.${path.basename(releasePath)}.${crypto.randomUUID()}.partial`);
  fs.mkdirSync(releasesDir, { recursive: true });
  fs.rmSync(temporary, { recursive: true, force: true });
  fs.cpSync(resolvedPackageRoot, temporary, {
    recursive: true,
    dereference: false,
    force: true,
    errorOnExist: false,
    verbatimSymlinks: true,
  });
  fs.rmSync(releasePath, { recursive: true, force: true });
  fs.renameSync(temporary, releasePath);

  const activeReleasePath = path.join(resolvedZylosDir, 'runtime', 'active-release.json');
  writeAtomicJson(activeReleasePath, {
    release_ref: releaseRef,
    release_path: releasePath,
    upgrade_id: upgradeId,
  });
  return Object.freeze({ releaseRef, releasePath, activeReleasePath });
}

function main() {
  const result = publishDockerActiveRelease();
  console.log(`[zylos] Active Docker release published: ${result.releasePath}`);
}

if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) {
  try {
    main();
  } catch (error) {
    console.error(`[zylos] Failed to publish active Docker release: ${error?.message ?? error}`);
    process.exit(1);
  }
}
