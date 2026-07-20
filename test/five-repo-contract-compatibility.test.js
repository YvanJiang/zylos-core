import { describe, expect, test } from '@jest/globals';
import { mkdtempSync, mkdirSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';

import {
  buildFiveRepoCompatibilityPlan,
  COMPATIBILITY_ASSERTIONS,
  COMPATIBILITY_EVIDENCE_PREFIX,
  CONTRACT_FLOWS,
  executeFiveRepoCompatibilityPlan,
  FIVE_REPO_CONTRACT_MATRIX,
  runRepositoryCompatibilityCommand,
} from '../scripts/lib/five-repo-contract-gate.js';

describe('five-repository public contract compatibility gate', () => {
  test('owns one complete release matrix for every repository and contract flow', () => {
    expect(COMPATIBILITY_ASSERTIONS).toEqual([
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
    expect(CONTRACT_FLOWS).toEqual([
      'ingress',
      'events',
      'interactions',
      'delivery',
      'observability_control',
      'dashboard_luna_projection',
    ]);

    expect(FIVE_REPO_CONTRACT_MATRIX.map(({ repository }) => repository)).toEqual([
      'zylos-core',
      'zylos-feishu',
      'zylos-lark',
      'zylos-dashboard',
      'luna-pet',
    ]);
    expect(new Set(FIVE_REPO_CONTRACT_MATRIX.flatMap(({ flows }) => flows))).toEqual(
      new Set(CONTRACT_FLOWS),
    );
    for (const entry of FIVE_REPO_CONTRACT_MATRIX) {
      expect(entry.assertions).toEqual(COMPATIBILITY_ASSERTIONS);
      expect(entry.testFiles.length).toBeGreaterThan(0);
    }
  });

  test('runs every repository against the current Core fixtures and fails closed as one gate', () => {
    const workspaceDirectory = mkdtempSync(path.join(tmpdir(), 'five-repo-contract-gate-'));
    const coreDirectory = path.join(workspaceDirectory, '.codex-worktrees', 'zylos-core-issue-40');
    const directoryByKey = {
      core: coreDirectory,
      feishu: path.join(workspaceDirectory, '.codex-worktrees', 'zylos-feishu-integration'),
      lark: path.join(workspaceDirectory, '.codex-worktrees', 'zylos-lark-integration'),
      dashboard: path.join(workspaceDirectory, '.codex-worktrees', 'zylos-dashboard-integration'),
      luna: path.join(workspaceDirectory, '.codex-worktrees', 'luna-pet-integration'),
    };
    for (const entry of FIVE_REPO_CONTRACT_MATRIX) {
      for (const testFile of entry.testFiles) {
        const filename = path.join(directoryByKey[entry.directoryKey], testFile);
        mkdirSync(path.dirname(filename), { recursive: true });
        writeFileSync(filename, '// compatibility seam\n');
      }
    }
    mkdirSync(path.join(coreDirectory, 'contracts', 'public', 'fixtures'), { recursive: true });
    writeFileSync(path.join(coreDirectory, 'contracts', 'public', 'index.js'), 'export {};\n');
    writeFileSync(
      path.join(coreDirectory, 'contracts', 'public', 'fixtures', 'idempotency-v1.json'),
      '{}\n',
    );

    const plan = buildFiveRepoCompatibilityPlan({
      coreDirectory,
      workspaceDirectory,
      environment: {},
    });
    expect(plan).toHaveLength(5);
    expect(plan.map(({ repository }) => repository)).toEqual(
      FIVE_REPO_CONTRACT_MATRIX.map(({ repository }) => repository),
    );
    for (const item of plan.slice(1)) {
      expect(item.environment.ZYLOS_CORE_PUBLIC_CONTRACTS_DIR).toBe(
        path.join(coreDirectory, 'contracts', 'public'),
      );
    }

    const calls = [];
    const outcome = executeFiveRepoCompatibilityPlan(plan, {
      runCommand(item) {
        calls.push(item.repository);
        const evidence = {
          schema_version: 1,
          repository: item.repository,
          core_fixture_sha256: item.coreFixtureSha256,
          assertions: item.assertions,
          flows: item.flows,
          computations: {
            jcs_bytes_from_raw_payload: 1,
            idempotency_key_from_raw_payload: 1,
            payload_hash_from_raw_payload: 1,
          },
        };
        return {
          exitCode: item.repository === 'zylos-lark' ? 1 : 0,
          stdout: item.requiresEvidence
            ? `${item.repository} stdout\n# ${COMPATIBILITY_EVIDENCE_PREFIX}${JSON.stringify(evidence)}\n`
            : `${item.repository} stdout`,
          stderr: `${item.repository} stderr`,
        };
      },
    });
    expect(calls).toEqual(FIVE_REPO_CONTRACT_MATRIX.map(({ repository }) => repository));
    expect(outcome.passed).toBe(false);
    expect(outcome.results).toHaveLength(5);
    expect(outcome.results.find(({ repository }) => repository === 'zylos-lark')).toMatchObject({
      passed: false,
      exitCode: 1,
      commandExitCode: 1,
    });
  });

  test('a zero-exit consumer without current-fixture independent evidence still fails closed', () => {
    const item = {
      repository: 'luna-pet',
      directory: '/workspace/luna',
      flows: ['dashboard_luna_projection'],
      assertions: COMPATIBILITY_ASSERTIONS,
      requiresEvidence: true,
      coreFixtureSha256: 'a'.repeat(64),
    };
    const outcome = executeFiveRepoCompatibilityPlan([item], {
      runCommand() {
        return { exitCode: 0, stdout: '7 tests passed\n', stderr: '' };
      },
    });
    expect(outcome.passed).toBe(false);
    expect(outcome.results[0]).toMatchObject({
      passed: false,
      exitCode: 1,
      commandExitCode: 0,
      stderr: expect.stringContaining('compatibility evidence'),
    });
  });

  test.each([
    ['malformed JSON', `${COMPATIBILITY_EVIDENCE_PREFIX}{broken`, 'invalid JSON'],
    ['stale fixture hash', `${COMPATIBILITY_EVIDENCE_PREFIX}${JSON.stringify({
      schema_version: 1,
      repository: 'luna-pet',
      core_fixture_sha256: 'b'.repeat(64),
      assertions: COMPATIBILITY_ASSERTIONS,
      flows: ['dashboard_luna_projection'],
      computations: {
        jcs_bytes_from_raw_payload: 1,
        idempotency_key_from_raw_payload: 1,
        payload_hash_from_raw_payload: 1,
      },
    })}`, 'current Core matrix'],
    ['missing independent computation', `${COMPATIBILITY_EVIDENCE_PREFIX}${JSON.stringify({
      schema_version: 1,
      repository: 'luna-pet',
      core_fixture_sha256: 'a'.repeat(64),
      assertions: COMPATIBILITY_ASSERTIONS,
      flows: ['dashboard_luna_projection'],
      computations: {
        jcs_bytes_from_raw_payload: 1,
        idempotency_key_from_raw_payload: 1,
      },
    })}`, 'payload_hash_from_raw_payload'],
  ])('rejects %s evidence after a successful consumer test process', (_name, evidenceLine, error) => {
    const item = {
      repository: 'luna-pet',
      directory: '/workspace/luna',
      flows: ['dashboard_luna_projection'],
      assertions: COMPATIBILITY_ASSERTIONS,
      requiresEvidence: true,
      coreFixtureSha256: 'a'.repeat(64),
    };
    const outcome = executeFiveRepoCompatibilityPlan([item], {
      runCommand() {
        return { exitCode: 0, stdout: `# ${evidenceLine}\n`, stderr: '' };
      },
    });
    expect(outcome.results[0]).toMatchObject({
      commandExitCode: 0,
      exitCode: 1,
      stderr: expect.stringContaining(error),
    });
  });

  test.each([
    ['a valid line followed by malformed evidence', (validEvidence) => [
      validEvidence,
      `${COMPATIBILITY_EVIDENCE_PREFIX}{broken`,
    ]],
    ['duplicate valid evidence', (validEvidence) => [validEvidence, validEvidence]],
  ])('rejects %s instead of accepting the first evidence line', (_name, evidenceLines) => {
    const item = {
      repository: 'luna-pet',
      directory: '/workspace/luna',
      flows: ['dashboard_luna_projection'],
      assertions: COMPATIBILITY_ASSERTIONS,
      requiresEvidence: true,
      coreFixtureSha256: 'a'.repeat(64),
    };
    const validEvidence = `${COMPATIBILITY_EVIDENCE_PREFIX}${JSON.stringify({
      schema_version: 1,
      repository: item.repository,
      core_fixture_sha256: item.coreFixtureSha256,
      assertions: item.assertions,
      flows: item.flows,
      computations: {
        jcs_bytes_from_raw_payload: 1,
        idempotency_key_from_raw_payload: 1,
        payload_hash_from_raw_payload: 1,
      },
    })}`;
    const outcome = executeFiveRepoCompatibilityPlan([item], {
      runCommand() {
        return {
          exitCode: 0,
          stdout: `${evidenceLines(validEvidence).map((line) => `# ${line}`).join('\n')}\n`,
          stderr: '',
        };
      },
    });
    expect(outcome.results[0]).toMatchObject({
      commandExitCode: 0,
      exitCode: 1,
      stderr: expect.stringContaining('exactly one compatibility evidence line'),
    });
  });

  test('executes exact argv without a shell and treats process startup failures as gate failures', () => {
    const item = {
      command: '/usr/local/bin/node',
      arguments: ['--test', 'test/contract.test.js'],
      directory: '/workspace/consumer',
      environment: { CONTRACT_DIR: '/workspace/core/contracts/public' },
    };
    const calls = [];
    const success = runRepositoryCompatibilityCommand(item, {
      spawn(command, args, options) {
        calls.push({ command, args, options });
        return { status: 0, stdout: 'ok\n', stderr: '', signal: null };
      },
    });
    expect(calls).toEqual([{
      command: item.command,
      args: item.arguments,
      options: {
        cwd: item.directory,
        env: item.environment,
        encoding: 'utf8',
        shell: false,
      },
    }]);
    expect(success).toEqual({ exitCode: 0, stdout: 'ok\n', stderr: '' });

    expect(runRepositoryCompatibilityCommand(item, {
      spawn() {
        return { status: null, stdout: '', stderr: '', signal: 'SIGTERM' };
      },
    })).toMatchObject({ exitCode: 1, stderr: expect.stringContaining('SIGTERM') });
    expect(runRepositoryCompatibilityCommand(item, {
      spawn() {
        return { status: null, stdout: '', stderr: '', error: new Error('ENOENT') };
      },
    })).toMatchObject({ exitCode: 1, stderr: expect.stringContaining('ENOENT') });
  });
});
