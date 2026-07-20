import { createHash } from 'node:crypto';
import {
  existsSync,
  readFileSync,
  readdirSync,
} from 'node:fs';
import path from 'node:path';
import { spawnSync } from 'node:child_process';

export const COMPATIBILITY_ASSERTIONS = Object.freeze([
  'required_null_optional',
  'major_rejection',
  'minor_additive_compatibility',
  'unknown_safety_terminal_enum_rejection',
  'public_error_shape',
  'version_conflict',
  'jcs_bytes_from_raw_payload',
  'idempotency_key_from_raw_payload',
  'payload_hash_from_raw_payload',
]);

export const CONTRACT_FLOWS = Object.freeze([
  'ingress',
  'events',
  'interactions',
  'delivery',
  'observability_control',
  'dashboard_luna_projection',
]);

export const COMPATIBILITY_EVIDENCE_PREFIX = 'ZYLOS_CONTRACT_COMPATIBILITY_EVIDENCE=';

function matrixEntry(repository, directoryKey, flows, testFiles) {
  return Object.freeze({
    repository,
    directoryKey,
    flows: Object.freeze(flows),
    assertions: COMPATIBILITY_ASSERTIONS,
    testFiles: Object.freeze(testFiles),
    requiresEvidence: directoryKey !== 'core',
  });
}

export const FIVE_REPO_CONTRACT_MATRIX = Object.freeze([
  matrixEntry('zylos-core', 'core', CONTRACT_FLOWS, [
    'test/public-contract-kernel.test.js',
    'test/public-inbound-contracts.test.js',
    'test/public-normalized-event-contract.test.js',
    'test/public-interaction-contracts.test.js',
    'test/delivery-mapping-contracts.test.js',
    'test/runtime-observability-control-contracts.test.js',
    'test/public-runtime-contract-schemas.test.js',
  ]),
  matrixEntry('zylos-feishu', 'feishu', [
    'ingress', 'events', 'interactions', 'delivery',
  ], [
    'test/inbound-envelope.test.js',
    'test/interaction-adapter.test.js',
    'test/delivery-renderer.test.js',
  ]),
  matrixEntry('zylos-lark', 'lark', [
    'ingress', 'events', 'interactions', 'delivery',
  ], [
    'test/contract-security.test.js',
    'test/inbound-envelope.test.js',
    'test/interaction-adapter.test.js',
    'test/delivery-renderer.test.js',
  ]),
  matrixEntry('zylos-dashboard', 'dashboard', [
    'events', 'observability_control', 'dashboard_luna_projection',
  ], [
    'test/runtime-snapshot-consumer.test.js',
    'test/operations-auth-adapter.test.js',
    'test/operations-control-client.test.js',
    'test/operations-control-core-contract.test.js',
    'test/runtime-projection.test.js',
    'test/runtime-projection-sse.test.js',
  ]),
  matrixEntry('luna-pet', 'luna', ['dashboard_luna_projection'], [
    'test/runtime-projection-consumer.test.js',
    'test/sse-event-decoder.test.js',
  ]),
]);

const REPOSITORY_DIRECTORY_ENV = Object.freeze({
  feishu: 'ZYLOS_FEISHU_CONTRACT_REPO',
  lark: 'ZYLOS_LARK_CONTRACT_REPO',
  dashboard: 'ZYLOS_DASHBOARD_CONTRACT_REPO',
  luna: 'ZYLOS_LUNA_CONTRACT_REPO',
});

const DEFAULT_REPOSITORY_DIRECTORIES = Object.freeze({
  feishu: ['.codex-worktrees', 'zylos-feishu-integration'],
  lark: ['.codex-worktrees', 'zylos-lark-integration'],
  dashboard: ['.codex-worktrees', 'zylos-dashboard-integration'],
  luna: ['.codex-worktrees', 'luna-pet-integration'],
});

function requireFile(filename, description) {
  if (!existsSync(filename)) {
    throw new Error(`${description} was not found at ${filename}.`);
  }
}

function hashCoreFixtures(publicContractsDirectory) {
  const fixturesDirectory = path.join(publicContractsDirectory, 'fixtures');
  const fixtureFiles = readdirSync(fixturesDirectory)
    .filter((filename) => filename.endsWith('.json'))
    .sort();
  if (fixtureFiles.length === 0) {
    throw new Error(`No Core public JSON fixtures were found at ${fixturesDirectory}.`);
  }
  const hash = createHash('sha256');
  for (const filename of fixtureFiles) {
    hash.update(filename, 'utf8');
    hash.update(Buffer.from([0]));
    hash.update(readFileSync(path.join(fixturesDirectory, filename)));
    hash.update(Buffer.from([0]));
  }
  return hash.digest('hex');
}

function repositoryDirectory(entry, { coreDirectory, workspaceDirectory, environment }) {
  if (entry.directoryKey === 'core') return coreDirectory;
  const configured = environment[REPOSITORY_DIRECTORY_ENV[entry.directoryKey]];
  return configured
    ? path.resolve(configured)
    : path.join(workspaceDirectory, ...DEFAULT_REPOSITORY_DIRECTORIES[entry.directoryKey]);
}

export function buildFiveRepoCompatibilityPlan({
  coreDirectory,
  workspaceDirectory,
  environment = process.env,
}) {
  const resolvedCoreDirectory = path.resolve(coreDirectory);
  const resolvedWorkspaceDirectory = path.resolve(workspaceDirectory);
  const publicContractsDirectory = path.join(resolvedCoreDirectory, 'contracts', 'public');
  requireFile(
    path.join(publicContractsDirectory, 'index.js'),
    'The authoritative Core public contract entrypoint',
  );
  requireFile(
    path.join(publicContractsDirectory, 'fixtures', 'idempotency-v1.json'),
    'The raw canonicalization/idempotency fixture',
  );
  const coreFixtureSha256 = hashCoreFixtures(publicContractsDirectory);

  return FIVE_REPO_CONTRACT_MATRIX.map((entry) => {
    const directory = repositoryDirectory(entry, {
      coreDirectory: resolvedCoreDirectory,
      workspaceDirectory: resolvedWorkspaceDirectory,
      environment,
    });
    for (const testFile of entry.testFiles) {
      requireFile(path.join(directory, testFile), `${entry.repository} compatibility test ${testFile}`);
    }
    const isCore = entry.directoryKey === 'core';
    return Object.freeze({
      ...entry,
      directory,
      coreFixtureSha256,
      command: process.execPath,
      arguments: Object.freeze(isCore
        ? [
          '--experimental-vm-modules',
          path.join(resolvedCoreDirectory, 'node_modules', '.bin', 'jest'),
          '--runInBand',
          ...entry.testFiles,
        ]
        : ['--test', ...entry.testFiles]),
      environment: Object.freeze({
        ...environment,
        ZYLOS_CORE_PUBLIC_CONTRACTS_DIR: publicContractsDirectory,
      }),
    });
  });
}

function arraysEqual(left, right) {
  return Array.isArray(left)
    && left.length === right.length
    && left.every((value, index) => value === right[index]);
}

function extractConsumerEvidence(item, stdout) {
  const line = stdout
    .split(/\r?\n/u)
    .find((candidate) => candidate.includes(COMPATIBILITY_EVIDENCE_PREFIX));
  if (!line) throw new Error('consumer did not emit compatibility evidence');
  const evidenceStart = line.indexOf(COMPATIBILITY_EVIDENCE_PREFIX)
    + COMPATIBILITY_EVIDENCE_PREFIX.length;
  let evidence;
  try {
    evidence = JSON.parse(line.slice(evidenceStart));
  } catch {
    throw new Error('consumer emitted invalid JSON compatibility evidence');
  }
  if (
    evidence?.schema_version !== 1
    || evidence.repository !== item.repository
    || evidence.core_fixture_sha256 !== item.coreFixtureSha256
    || !arraysEqual(evidence.assertions, item.assertions)
    || !arraysEqual(evidence.flows, item.flows)
  ) {
    throw new Error('consumer compatibility evidence does not match the current Core matrix');
  }
  for (const assertion of [
    'jcs_bytes_from_raw_payload',
    'idempotency_key_from_raw_payload',
    'payload_hash_from_raw_payload',
  ]) {
    if (!Number.isSafeInteger(evidence.computations?.[assertion])
      || evidence.computations[assertion] < 1) {
      throw new Error(`consumer compatibility evidence is missing ${assertion}`);
    }
  }
  return Object.freeze(evidence);
}

export function executeFiveRepoCompatibilityPlan(plan, { runCommand }) {
  if (typeof runCommand !== 'function') {
    throw new TypeError('runCommand must execute one repository compatibility item.');
  }
  const results = plan.map((item) => {
    const result = runCommand(item);
    const commandExitCode = Number.isInteger(result?.exitCode) ? result.exitCode : 1;
    const stdout = typeof result?.stdout === 'string' ? result.stdout : '';
    let stderr = typeof result?.stderr === 'string' ? result.stderr : '';
    let evidence = null;
    let evidenceError = null;
    if (commandExitCode === 0 && item.requiresEvidence) {
      try {
        evidence = extractConsumerEvidence(item, stdout);
      } catch (error) {
        evidenceError = error;
        stderr += `${stderr && !stderr.endsWith('\n') ? '\n' : ''}${error.message}\n`;
      }
    }
    const exitCode = commandExitCode === 0 && !evidenceError ? 0 : 1;
    return Object.freeze({
      repository: item.repository,
      directory: item.directory,
      flows: item.flows,
      assertions: item.assertions,
      passed: exitCode === 0,
      exitCode,
      commandExitCode,
      stdout,
      stderr,
      evidence,
    });
  });
  return Object.freeze({
    passed: results.every(({ passed }) => passed),
    results: Object.freeze(results),
  });
}

export function runRepositoryCompatibilityCommand(item, { spawn = spawnSync } = {}) {
  const result = spawn(item.command, item.arguments, {
    cwd: item.directory,
    env: item.environment,
    encoding: 'utf8',
    shell: false,
  });
  const stdout = typeof result?.stdout === 'string' ? result.stdout : '';
  let stderr = typeof result?.stderr === 'string' ? result.stderr : '';
  if (result?.error) stderr += `${stderr && !stderr.endsWith('\n') ? '\n' : ''}${result.error.message}\n`;
  if (result?.signal) stderr += `${stderr && !stderr.endsWith('\n') ? '\n' : ''}terminated by ${result.signal}\n`;
  return {
    exitCode: Number.isInteger(result?.status) ? result.status : 1,
    stdout,
    stderr,
  };
}
