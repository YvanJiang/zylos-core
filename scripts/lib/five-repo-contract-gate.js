import { existsSync } from 'node:fs';
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

function matrixEntry(repository, directoryKey, flows, testFiles) {
  return Object.freeze({
    repository,
    directoryKey,
    flows: Object.freeze(flows),
    assertions: COMPATIBILITY_ASSERTIONS,
    testFiles: Object.freeze(testFiles),
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

export function executeFiveRepoCompatibilityPlan(plan, { runCommand }) {
  if (typeof runCommand !== 'function') {
    throw new TypeError('runCommand must execute one repository compatibility item.');
  }
  const results = plan.map((item) => {
    const result = runCommand(item);
    const exitCode = Number.isInteger(result?.exitCode) ? result.exitCode : 1;
    return Object.freeze({
      repository: item.repository,
      directory: item.directory,
      flows: item.flows,
      assertions: item.assertions,
      passed: exitCode === 0,
      exitCode,
      stdout: typeof result?.stdout === 'string' ? result.stdout : '',
      stderr: typeof result?.stderr === 'string' ? result.stderr : '',
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
  if (result?.error) stderr += `${stderr ? '\n' : ''}${result.error.message}`;
  if (result?.signal) stderr += `${stderr ? '\n' : ''}terminated by ${result.signal}`;
  return {
    exitCode: Number.isInteger(result?.status) ? result.status : 1,
    stdout,
    stderr,
  };
}
