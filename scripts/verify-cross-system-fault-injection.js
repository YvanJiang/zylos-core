#!/usr/bin/env node

import path from 'node:path';
import { fileURLToPath } from 'node:url';

import {
  buildFiveRepoCompatibilityPlan,
  executeFiveRepoCompatibilityPlan,
  runRepositoryCompatibilityCommand,
} from './lib/five-repo-contract-gate.js';
import {
  CROSS_SYSTEM_EVIDENCE_PREFIX,
  buildCrossSystemFaultInjectionEvidence,
  buildCrossSystemFaultInjectionPlan,
  detectCrossSystemRealEvidence,
  executeCrossSystemFaultInjectionPlan,
  inspectCrossSystemRepository,
  resolveCrossSystemRepositoryDirectories,
  runCrossSystemFaultProbe,
  validateCrossSystemRepositoryEvidence,
} from './lib/cross-system-fault-injection.js';

const scriptsDirectory = path.dirname(fileURLToPath(import.meta.url));
const coreDirectory = path.resolve(scriptsDirectory, '..');
const worktreesDirectory = path.dirname(coreDirectory);
const workspaceDirectory = path.basename(worktreesDirectory) === '.codex-worktrees'
  ? path.dirname(worktreesDirectory)
  : path.dirname(coreDirectory);

function contractEnvironment(directories) {
  return {
    ...process.env,
    ZYLOS_FEISHU_CONTRACT_REPO: directories['zylos-feishu'],
    ZYLOS_LARK_CONTRACT_REPO: directories['zylos-lark'],
    ZYLOS_DASHBOARD_CONTRACT_REPO: directories['zylos-dashboard'],
    ZYLOS_LUNA_CONTRACT_REPO: directories['luna-pet'],
  };
}

function normalizeContractEvidence(outcome) {
  return outcome.results
    .filter(({ evidence }) => evidence !== null)
    .map(({ repository, evidence }) => ({
      repository,
      fixture_sha256: evidence.core_fixture_sha256,
      raw_jcs_count: evidence.computations.jcs_bytes_from_raw_payload,
      idempotency_key_count: evidence.computations.idempotency_key_from_raw_payload,
      payload_hash_count: evidence.computations.payload_hash_from_raw_payload,
    }));
}

try {
  const directories = resolveCrossSystemRepositoryDirectories({
    coreDirectory,
    workspaceDirectory,
  });
  const repositories = Object.fromEntries(Object.entries(directories).map(
    ([repository, directory]) => [
      repository,
      inspectCrossSystemRepository(repository, directory),
    ],
  ));
  validateCrossSystemRepositoryEvidence(repositories);
  const plan = buildCrossSystemFaultInjectionPlan({ repositoryDirectories: directories });
  const probeOutcome = executeCrossSystemFaultInjectionPlan(plan, {
    runProbe: (item) => runCrossSystemFaultProbe(item, { coreDirectory }),
  });
  if (!probeOutcome.passed) {
    for (const result of probeOutcome.results.filter(({ exit_code: exitCode }) => exitCode !== 0)) {
      process.stderr.write(`FAIL ${result.probe_id}`
        + ` stdout_sha256=${result.stdout_sha256} stderr_sha256=${result.stderr_sha256}\n`);
    }
    throw new Error('one or more public fault probes failed');
  }

  const compatibilityPlan = buildFiveRepoCompatibilityPlan({
    coreDirectory,
    workspaceDirectory,
    environment: contractEnvironment(directories),
  });
  const compatibilityOutcome = executeFiveRepoCompatibilityPlan(compatibilityPlan, {
    runCommand: runRepositoryCompatibilityCommand,
  });
  if (!compatibilityOutcome.passed) {
    for (const result of compatibilityOutcome.results.filter(({ passed }) => !passed)) {
      process.stderr.write(`FAIL contract-${result.repository}`
        + ` command_exit=${result.commandExitCode} gate_exit=${result.exitCode}\n`);
    }
    throw new Error('five-repository contract evidence failed');
  }

  const evidence = buildCrossSystemFaultInjectionEvidence({
    repositoryEvidence: repositories,
    probeResults: probeOutcome.results,
    contractEvidence: normalizeContractEvidence(compatibilityOutcome),
    realEvidence: detectCrossSystemRealEvidence(),
    versions: {
      node: process.version,
      claude_agent_sdk: '0.3.215',
      codex_cli_target: '0.144.5',
    },
  });
  process.stdout.write(`${CROSS_SYSTEM_EVIDENCE_PREFIX}${JSON.stringify(evidence)}\n`);
} catch (error) {
  process.stderr.write(`Global47 fault-injection gate failed: ${error.message}\n`);
  process.exitCode = 1;
}
