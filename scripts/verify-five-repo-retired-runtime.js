#!/usr/bin/env node

import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import {
  buildFiveRepoRetiredRuntimePlan,
  executeFiveRepoRetiredRuntimePlan,
} from './lib/five-repo-retired-runtime-gate.js';

const scriptsDirectory = path.dirname(fileURLToPath(import.meta.url));
const coreDirectory = path.resolve(scriptsDirectory, '..');
const coreParentDirectory = path.dirname(coreDirectory);
const defaultWorkspaceDirectory = path.basename(coreParentDirectory) === '.codex-worktrees'
  ? path.dirname(coreParentDirectory)
  : path.dirname(coreDirectory);
const workspaceDirectory = path.resolve(
  process.env.ZYLOS_RUNTIME_MIGRATION_WORKSPACE || defaultWorkspaceDirectory,
);
const allowlist = JSON.parse(fs.readFileSync(
  path.join(scriptsDirectory, 'config', 'five-repo-retired-runtime-allowlist.json'),
  'utf8',
));

const plan = buildFiveRepoRetiredRuntimePlan({
  coreDirectory,
  workspaceDirectory,
  environment: process.env,
  allowlist,
});
const outcome = executeFiveRepoRetiredRuntimePlan(plan);

for (const result of outcome.results) {
  console.log(`${result.passed ? 'PASS' : 'FAIL'} ${result.repository}`
    + ` revision=${result.revision ?? 'unavailable'}`
    + ` tracked=${result.trackedFiles} package=${result.packagedFiles}`);
  for (const entry of result.allowlisted) {
    console.log(`ALLOW ${entry.repository}:${entry.file}:${entry.rule} (${entry.purpose})`);
  }
  for (const violation of result.violations) {
    console.error(`VIOLATION ${result.repository}:${violation.file}`
      + ` scope=${violation.scope} rule=${violation.rule}: ${violation.description}`);
  }
}

console.log(`FIVE_REPO_RETIRED_RUNTIME_GATE=${outcome.passed ? 'PASS' : 'FAIL'}`);
process.exitCode = outcome.passed ? 0 : 1;
