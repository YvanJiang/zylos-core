#!/usr/bin/env node
/** Upgrade the Claude prerequisite behind the authoritative executor lifecycle. */
import { execFileSync } from 'node:child_process';

function run(file, args, options = {}) {
  return execFileSync(file, args, { stdio: 'inherit', timeout: 15 * 60_000, ...options });
}

function main() {
  run('zylos', ['stop']);
  let upgradeError = null;
  try {
    run('bash', ['-lc', 'curl -fsSL https://claude.ai/install.sh | bash']);
  } catch (error) {
    upgradeError = error;
  }
  let startError = null;
  try {
    run('zylos', ['start']);
    run('zylos', ['status']);
  } catch (error) {
    startError = error;
  }
  if (upgradeError !== null && startError !== null) {
    throw new AggregateError([upgradeError, startError], 'Claude upgrade failed and executor restart failed.');
  }
  if (upgradeError !== null) throw upgradeError;
  if (startError !== null) throw startError;
}

try {
  main();
} catch (error) {
  console.error(error);
  process.exitCode = 1;
}
