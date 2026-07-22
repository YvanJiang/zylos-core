#!/usr/bin/env node
/**
 * Install dependencies for skills that have their own package.json.
 * Runs as a pretest hook so `npm test` works out of the box.
 */

import { readdirSync, existsSync } from 'node:fs';
import { join } from 'node:path';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const root = join(fileURLToPath(import.meta.url), '..', '..');
const skillsDir = join(root, 'skills');

for (const name of readdirSync(skillsDir, { withFileTypes: true })) {
  if (!name.isDirectory()) continue;
  const dir = join(skillsDir, name.name);
  const pkg = join(dir, 'package.json');
  const lock = join(dir, 'package-lock.json');
  const modules = join(dir, 'node_modules');
  if (!existsSync(pkg) || existsSync(modules)) continue;
  console.log(`[pretest] Installing deps for skills/${name.name}`);
  const installArguments = ['install', '--omit=dev'];
  if (!existsSync(lock)) installArguments.push('--package-lock=false');
  execFileSync('npm', installArguments, { cwd: dir, stdio: 'inherit' });
}
