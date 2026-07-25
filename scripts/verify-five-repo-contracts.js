#!/usr/bin/env node

import path from 'node:path';
import { fileURLToPath } from 'node:url';

import {
  buildFiveRepoCompatibilityPlan,
  executeFiveRepoCompatibilityPlan,
  runRepositoryCompatibilityCommand,
} from './lib/five-repo-contract-gate.js';

const scriptsDirectory = path.dirname(fileURLToPath(import.meta.url));
const coreDirectory = path.resolve(scriptsDirectory, '..');
const coreParentDirectory = path.dirname(coreDirectory);
const defaultWorkspaceDirectory = path.basename(coreParentDirectory) === '.codex-worktrees'
  ? path.dirname(coreParentDirectory)
  : path.dirname(coreDirectory);
const workspaceDirectory = path.resolve(
  process.env.ZYLOS_RUNTIME_MIGRATION_WORKSPACE || defaultWorkspaceDirectory,
);

let plan;
try {
  plan = buildFiveRepoCompatibilityPlan({
    coreDirectory,
    workspaceDirectory,
    environment: process.env,
  });
} catch (error) {
  console.error(`Five-repository contract gate preflight failed: ${error.message}`);
  process.exitCode = 1;
}

if (plan) {
  const outcome = executeFiveRepoCompatibilityPlan(plan, {
    runCommand: runRepositoryCompatibilityCommand,
  });
  for (const result of outcome.results) {
    const label = `${result.repository} test_exit=${result.commandExitCode} gate_exit=${result.exitCode}`;
    console.log(`\n===== ${label} =====`);
    if (result.stdout) process.stdout.write(result.stdout);
    if (result.stderr) process.stderr.write(result.stderr);
  }
  console.log('\n===== five-repository producer/consumer matrix =====');
  for (const result of outcome.results) {
    console.log(`${result.passed ? 'PASS' : 'FAIL'} ${result.repository}: ${result.flows.join(', ')}`);
  }
  console.log(`GATE ${outcome.passed ? 'PASS' : 'FAIL'}`);
  process.exitCode = outcome.passed ? 0 : 1;
}
