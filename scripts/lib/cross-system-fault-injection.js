import { createHash } from 'node:crypto';
import { spawnSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';

import { CHANNEL_FAULT_PROBE_PREFIX } from './channel-delivery-fault-probe.js';

export const CONSENSUS_SHA256 =
  '57a67b0a359172924bb923aa5422398efb28e473988546e859b5e08521eac800';

export const CROSS_SYSTEM_EVIDENCE_PREFIX = 'ZYLOS_GLOBAL47_FAULT_INJECTION_EVIDENCE=';

export const CROSS_SYSTEM_BASELINES = Object.freeze({
  'zylos-core': 'b19b7e9cbb30ca9da25bcb049c8b7f0b6fa5a907',
  'zylos-dashboard': '6142782de860990d30376942d32d5fcfa385bf7a',
  'zylos-feishu': 'e46b75d32057f79514c1a5c0fdea927b0e258263',
  'zylos-lark': '92b86f3c5ba1e2ea6f6edb36e6ed0f6cc9a8c99a',
  'luna-pet': '59ce7e9b43539c63423c1f09c43eeaac06e5c6f5',
});

const CONTRACT_CONSUMERS = Object.freeze(
  Object.keys(CROSS_SYSTEM_BASELINES).filter((repository) => repository !== 'zylos-core'),
);

export const REQUIRED_ACCEPTANCE_DIMENSIONS = Object.freeze([
  'classification',
  'user_notification',
  'recovery_or_fallback',
  'audit_or_metrics',
  'backlog_drain',
]);

const ALL_DIMENSIONS = Object.freeze([...REQUIRED_ACCEPTANCE_DIMENSIONS]);

function probe(probeId, repository, kind, target) {
  return Object.freeze({
    probe_id: probeId,
    repository,
    kind,
    target: Object.freeze({ ...target }),
    assertions: ALL_DIMENSIONS,
  });
}

function expected({
  classification,
  userNotification,
  recoveryOrFallback,
  auditOrMetrics,
  backlogDrain,
}) {
  return Object.freeze({
    classification,
    user_notification: userNotification,
    recovery_or_fallback: recoveryOrFallback,
    audit_or_metrics: auditOrMetrics,
    backlog_drain: backlogDrain,
  });
}

function acceptanceCase(caseId, seam, expectation, probes) {
  return Object.freeze({
    case_id: caseId,
    seam,
    expected: expected(expectation),
    probes: Object.freeze(probes),
  });
}

const coreJest = (id, file, name) => probe(id, 'zylos-core', 'jest', {
  file,
  test_name: name,
});

const channelProbe = (id, repository, scenario) => probe(
  id,
  repository,
  'channel_renderer',
  { scenario },
);

const dashboardSmokeProbe = probe(
  'dashboard-luna-restart-smoke',
  'zylos-dashboard',
  'dashboard_smoke',
  { script: 'scripts/runtime-migration-smoke.js' },
);

const OBSERVABLE = Object.freeze({
  safeRestart: {
    classification: 'safe_not_started_restart',
    userNotification: 'durable_initial_ack_preserved',
    recoveryOrFallback: 'durable_queue_resumed',
    auditOrMetrics: 'service_instance_and_queue_snapshot',
    backlogDrain: 'queued_turns_completed_once',
  },
  unknownRecovery: {
    classification: 'side_effect_unknown',
    userNotification: 'recovery_notice_persisted_before_action',
    recoveryOrFallback: 'waiting_authorized_disposition',
    auditOrMetrics: 'degraded_snapshot_and_recovery_audit',
    backlogDrain: 'blocked_without_blind_replay',
  },
  staleFence: {
    classification: 'stale_fence',
    userNotification: 'authoritative_projection_unchanged',
    recoveryOrFallback: 'stale_result_rejected',
    auditOrMetrics: 'diagnostic_or_conflict_audit_recorded',
    backlogDrain: 'current_owner_completes_once',
  },
  busyRetry: {
    classification: 'storage_busy',
    userNotification: 'durable_prior_ack_remains_visible',
    recoveryOrFallback: 'retry_after_lock_release',
    auditOrMetrics: 'busy_code_and_queue_state_observed',
    backlogDrain: 'accepted_once_after_release',
  },
  transient: {
    classification: 'retryable_transient',
    userNotification: 'retry_or_ack_projection_visible',
    recoveryOrFallback: 'bounded_retry_then_success_or_fallback',
    auditOrMetrics: 'attempt_and_backoff_evidence',
    backlogDrain: 'lane_drained_without_duplicate',
  },
  permanent: {
    classification: 'permanent_failure',
    userNotification: 'plain_text_or_terminal_fallback_visible',
    recoveryOrFallback: 'no_retry_and_safe_fallback',
    auditOrMetrics: 'terminal_delivery_result_recorded',
    backlogDrain: 'failed_attempt_terminalized',
  },
  channelUnknown: {
    classification: 'delivery_unknown',
    userNotification: 'uncertainty_projection_visible',
    recoveryOrFallback: 'reconciliation_barrier_without_resend',
    auditOrMetrics: 'degraded_outbox_snapshot',
    backlogDrain: 'lane_blocked_pending_proof',
  },
});

export const CROSS_SYSTEM_FAULT_CASES = Object.freeze([
  acceptanceCase('executor_restart', 'runtime', OBSERVABLE.safeRestart, [
    coreJest(
      'core-executor-restart',
      'test/runtime-executor-service.test.js',
      'rebuilds its cache after process restart and continues the not-started durable queue',
    ),
  ]),
  acceptanceCase('channel_restart', 'runtime', OBSERVABLE.safeRestart, [
    channelProbe('feishu-channel-restart', 'zylos-feishu', 'restart_exact_target'),
    channelProbe('lark-channel-restart', 'zylos-lark', 'restart_exact_target'),
    dashboardSmokeProbe,
  ]),
  acceptanceCase('service_restart', 'runtime', OBSERVABLE.unknownRecovery, [
    coreJest(
      'core-service-restart',
      'test/runtime-executor-service.test.js',
      'startup reconciles an unproven nonterminal attempt without trusting PID evidence',
    ),
  ]),
  acceptanceCase('stale_provider_result', 'runtime', OBSERVABLE.staleFence, [
    coreJest(
      'core-stale-provider-result',
      'test/runtime-executor-service.test.js',
      'records callbacks from a superseded provider attempt as diagnostics only',
    ),
  ]),
  acceptanceCase('stale_delivery_result', 'runtime', OBSERVABLE.staleFence, [
    coreJest(
      'core-stale-delivery-result',
      'test/runtime-outbox-service.test.js',
      'fences delivery claims and applies only the current result exactly once',
    ),
  ]),
  acceptanceCase('stale_control_result', 'runtime', OBSERVABLE.staleFence, [
    coreJest(
      'core-stale-control-result',
      'test/runtime-operations-control.test.js',
      'durably rejects a stale trusted policy version and replays the same forbidden result',
    ),
  ]),
  acceptanceCase('stale_lease_result', 'runtime', OBSERVABLE.staleFence, [
    coreJest(
      'core-stale-lease-result',
      'test/runtime-workspace-leases.test.js',
      'normalizes roots and fences an expired holder from write, renew, or release',
    ),
  ]),
  acceptanceCase('orphan_runtime', 'runtime', OBSERVABLE.unknownRecovery, [
    coreJest(
      'core-orphan-runtime',
      'test/runtime-executor-service.test.js',
      'runs a thirty-second sweep and fences newly orphaned nonterminal work',
    ),
  ]),
  acceptanceCase('answer_send_before_ack', 'runtime', OBSERVABLE.unknownRecovery, [
    coreJest(
      'core-answer-send-before-ack',
      'test/runtime-interaction-happy-path.test.js',
      'fails a sent provider answer closed as delivery_unknown when app-server acknowledgement is uncertain',
    ),
  ]),
  acceptanceCase('sqlite_busy', 'storage', OBSERVABLE.busyRetry, [
    coreJest(
      'core-sqlite-busy',
      'test/runtime-cross-system-fault-injection.test.js',
      'retries scheduler admission after an injected SQLite writer lock without losing or duplicating work',
    ),
  ]),
  acceptanceCase('sqlite_checkpoint_busy', 'storage', OBSERVABLE.busyRetry, [
    coreJest(
      'core-sqlite-checkpoint-busy',
      'test/runtime-cross-system-fault-injection.test.js',
      'reports a busy WAL checkpoint while a reader is pinned and drains after release',
    ),
  ]),
  acceptanceCase('scheduler_busy', 'storage', OBSERVABLE.busyRetry, [
    coreJest(
      'core-scheduler-busy',
      'test/runtime-cross-system-fault-injection.test.js',
      'retries scheduler admission after an injected SQLite writer lock without losing or duplicating work',
    ),
  ]),
  acceptanceCase('same_root_writer_contention', 'workspace', {
    classification: 'workspace_conflict',
    userNotification: 'workspace_wait_projection_visible',
    recoveryOrFallback: 'strict_serialization',
    auditOrMetrics: 'workspace_waiter_observability',
    backlogDrain: 'second_writer_runs_after_release',
  }, [
    coreJest(
      'core-same-root-writer-contention',
      'test/runtime-workspace-leases.test.js',
      'keeps an overlapping turn durably queued with a user-visible workspace wait',
    ),
  ]),
  acceptanceCase('background_lease', 'workspace', {
    classification: 'background_writer_active',
    userNotification: 'workspace_wait_projection_visible',
    recoveryOrFallback: 'lease_retained_until_background_end',
    auditOrMetrics: 'background_holder_observability',
    backlogDrain: 'waiter_admitted_after_background_end',
  }, [
    coreJest(
      'core-background-lease',
      'test/runtime-workspace-leases.test.js',
      'retains a lease and LRU protection until durable background work really ends',
    ),
  ]),
  acceptanceCase('provider_network_fault', 'provider', {
    classification: 'pre_execution_transient_or_side_effect_unknown',
    userNotification: 'retry_or_recovery_notice_visible',
    recoveryOrFallback: 'bounded_retry_before_execution_or_authorized_disposition_after_send',
    auditOrMetrics: 'attempt_backoff_and_transport_loss_evidence',
    backlogDrain: 'retry_or_fenced_without_blind_replay',
  }, [
    coreJest(
      'core-provider-network-fault',
      'test/runtime-executor-service.test.js',
      'safely retries one turn at most three times with new attempts and lease epochs',
    ),
    coreJest(
      'core-provider-network-transport-loss',
      'test/codex-app-server-adapter.test.js',
      'reports recovery when transport is lost after turn/start write but before its response',
    ),
  ]),
  acceptanceCase('provider_rate_limit', 'provider', OBSERVABLE.transient, [
    coreJest(
      'core-provider-rate-limit',
      'test/runtime-executor-service.test.js',
      'schedules a safe retry for a terminal app-server 503 instead of uncertain recovery',
    ),
    channelProbe('feishu-rate-limit', 'zylos-feishu', 'transient_delivery'),
    channelProbe('lark-rate-limit', 'zylos-lark', 'transient_delivery'),
  ]),
  acceptanceCase('provider_auth_fault', 'provider', {
    ...OBSERVABLE.permanent,
    classification: 'provider_auth_failed',
  }, [
    coreJest(
      'core-provider-auth-fault',
      'test/runtime-executor-service.test.js',
      'persists a terminal app-server 401 as failed instead of uncertain recovery',
    ),
  ]),
  acceptanceCase('provider_context_fault', 'provider', {
    ...OBSERVABLE.permanent,
    classification: 'provider_context_invalid',
  }, [
    coreJest(
      'core-provider-context-fault',
      'test/codex-app-server-adapter.test.js',
      'classifies the target app-server missing-rollout response as context invalid',
    ),
  ]),
  acceptanceCase('feishu_transient_delivery', 'channel', OBSERVABLE.transient, [
    channelProbe('feishu-transient-delivery', 'zylos-feishu', 'transient_delivery'),
  ]),
  acceptanceCase('feishu_permanent_delivery', 'channel', OBSERVABLE.permanent, [
    channelProbe('feishu-permanent-delivery', 'zylos-feishu', 'permanent_delivery'),
  ]),
  acceptanceCase('feishu_unknown_delivery', 'channel', OBSERVABLE.channelUnknown, [
    channelProbe('feishu-unknown-delivery', 'zylos-feishu', 'unknown_delivery'),
    coreJest(
      'core-feishu-unknown-barrier',
      'test/runtime-outbox-service.test.js',
      'keeps an expired post-action result unconfirmed and blocks automatic replay',
    ),
  ]),
  acceptanceCase('lark_transient_delivery', 'channel', OBSERVABLE.transient, [
    channelProbe('lark-transient-delivery', 'zylos-lark', 'transient_delivery'),
  ]),
  acceptanceCase('lark_permanent_delivery', 'channel', OBSERVABLE.permanent, [
    channelProbe('lark-permanent-delivery', 'zylos-lark', 'permanent_delivery'),
  ]),
  acceptanceCase('lark_unknown_delivery', 'channel', OBSERVABLE.channelUnknown, [
    channelProbe('lark-unknown-delivery', 'zylos-lark', 'unknown_delivery'),
    coreJest(
      'core-lark-unknown-barrier',
      'test/runtime-outbox-service.test.js',
      'keeps an expired post-action result unconfirmed and blocks automatic replay',
    ),
  ]),
  acceptanceCase('outbox_replay', 'channel', OBSERVABLE.channelUnknown, [
    coreJest(
      'core-outbox-replay',
      'test/runtime-outbox-service.test.js',
      'quarantines an expired exact-base delivering claim without replay or new authority',
    ),
  ]),
]);

export function buildCrossSystemFaultInjectionPlan({ repositoryDirectories }) {
  const directories = requireRecord('repositoryDirectories', repositoryDirectories);
  for (const repository of Object.keys(CROSS_SYSTEM_BASELINES)) {
    if (typeof directories[repository] !== 'string' || directories[repository].length === 0) {
      throw new TypeError(`repositoryDirectories.${repository} must be a non-empty path`);
    }
  }
  const probes = CROSS_SYSTEM_FAULT_CASES.flatMap(({ probes: caseProbes }) => caseProbes);
  const unique = new Map();
  for (const item of probes) {
    const existing = unique.get(item.probe_id);
    if (existing && JSON.stringify(existing) !== JSON.stringify(item)) {
      throw new TypeError(`probe ${item.probe_id} has conflicting definitions`);
    }
    unique.set(item.probe_id, item);
  }
  return Object.freeze([...unique.values()].map((item) => Object.freeze({
    ...item,
    directory: directories[item.repository],
    ...(item.kind === 'dashboard_smoke'
      ? { luna_directory: directories['luna-pet'] }
      : {}),
  })));
}

function sha256(value) {
  return createHash('sha256').update(value).digest('hex');
}

function requireRecord(name, value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new TypeError(`${name} must be an object`);
  }
  return value;
}

function requireSha(name, value, length = 64) {
  if (typeof value !== 'string' || !new RegExp(`^[a-f0-9]{${length}}$`).test(value)) {
    throw new TypeError(`${name} must be a lowercase ${length}-character hex digest`);
  }
  return value;
}

function assertSafeEvidence(value, path = '$') {
  if (typeof value === 'string' && [
    /\bBearer\s+[A-Za-z0-9._~+/=-]{4,}/,
    /-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----/,
    /\bsk-(?:ant-|proj-|live-)?[A-Za-z0-9_-]{8,}/i,
    /\bxox[baprs]-[A-Za-z0-9-]{8,}/i,
    /\bAKIA[0-9A-Z]{16}\b/,
  ].some((pattern) => pattern.test(value))) {
    throw new TypeError(`unsafe evidence value at ${path}`);
  }
  if (Array.isArray(value)) {
    value.forEach((entry, index) => assertSafeEvidence(entry, `${path}[${index}]`));
    return;
  }
  if (!value || typeof value !== 'object') return;
  for (const [key, entry] of Object.entries(value)) {
    if (/(?:secret|password|credential|api[_-]?key|access[_-]?token|api[_-]?token|tenant[_-]?id|bot[_-]?id|chat[_-]?id|message[_-]?id|thread[_-]?id|operator[_-]?id)/i.test(key)) {
      throw new TypeError(`unsafe evidence field at ${path}.${key}`);
    }
    assertSafeEvidence(entry, `${path}.${key}`);
  }
}

function validateProbeResult(result) {
  const value = requireRecord('probe result', result);
  if (typeof value.probe_id !== 'string' || value.probe_id.length === 0) {
    throw new TypeError('probe result probe_id must be non-empty');
  }
  if (!Number.isInteger(value.exit_code)) throw new TypeError('probe result exit_code is required');
  requireSha('probe stdout_sha256', value.stdout_sha256);
  requireSha('probe stderr_sha256', value.stderr_sha256);
  if (!Array.isArray(value.assertions)) throw new TypeError('probe result assertions are required');
  for (const dimension of REQUIRED_ACCEPTANCE_DIMENSIONS) {
    if (!value.assertions.includes(dimension)) {
      throw new TypeError(`probe ${value.probe_id} omitted ${dimension}`);
    }
  }
  if (value.observations !== undefined) assertSafeEvidence(value.observations, '$.observations');
  return value;
}

export function executeCrossSystemFaultInjectionPlan(plan, { runProbe }) {
  if (!Array.isArray(plan) || plan.length === 0) throw new TypeError('plan must be non-empty');
  if (typeof runProbe !== 'function') throw new TypeError('runProbe must be a function');
  const results = plan.map((item) => validateProbeResult(runProbe(item)));
  const expectedIds = new Set(plan.map(({ probe_id: probeId }) => probeId));
  if (expectedIds.size !== plan.length) throw new TypeError('plan probe IDs must be unique');
  for (const result of results) {
    if (!expectedIds.has(result.probe_id)) throw new TypeError('probe result does not match the plan');
  }
  return Object.freeze({
    passed: results.every(({ exit_code: exitCode }) => exitCode === 0),
    results: Object.freeze(results.map((result) => Object.freeze({ ...result }))),
  });
}

export function validateCrossSystemRepositoryEvidence(repositories) {
  const value = requireRecord('repositories', repositories);
  for (const [repository, baseline] of Object.entries(CROSS_SYSTEM_BASELINES)) {
    const entry = requireRecord(`repositories.${repository}`, value[repository]);
    if (entry.baseline !== baseline) throw new TypeError(`${repository} baseline mismatch`);
    requireSha(`${repository} head`, entry.head, 40);
    if (entry.merge_base !== baseline) throw new TypeError(`${repository} merge base mismatch`);
    if (entry.clean !== true) throw new TypeError(`${repository} worktree must be clean`);
    if (repository !== 'zylos-core' && entry.head !== baseline) {
      throw new TypeError(`${repository} must be pinned to its exact integration baseline`);
    }
  }
}

function validateContractEvidence(entries) {
  if (!Array.isArray(entries) || entries.length !== CONTRACT_CONSUMERS.length) {
    throw new TypeError('one raw contract evidence record per consumer is required');
  }
  const repositories = new Set();
  let fixtureSha = null;
  for (const entry of entries) {
    const value = requireRecord('contract evidence entry', entry);
    if (!CONTRACT_CONSUMERS.includes(value.repository)) {
      throw new TypeError('unknown contract evidence consumer');
    }
    if (repositories.has(value.repository)) throw new TypeError('duplicate contract evidence');
    repositories.add(value.repository);
    requireSha('contract fixture_sha256', value.fixture_sha256);
    fixtureSha ??= value.fixture_sha256;
    if (value.fixture_sha256 !== fixtureSha) throw new TypeError('contract fixture SHA mismatch');
    for (const field of ['raw_jcs_count', 'idempotency_key_count', 'payload_hash_count']) {
      if (!Number.isSafeInteger(value[field]) || value[field] <= 0) {
        throw new TypeError(`contract evidence ${field} must be positive`);
      }
    }
  }
}

function deriveRealStatus(realEvidence) {
  const entries = Object.values(requireRecord('real_evidence', realEvidence));
  if (entries.length === 0) return 'unavailable';
  const statuses = entries.map((entry) => requireRecord('real evidence lane', entry).status);
  if (statuses.every((status) => status === 'passed')) return 'passed';
  if (statuses.some((status) => status === 'failed')) return 'failed';
  return 'unavailable';
}

export function validateCrossSystemFaultInjectionEvidence(evidence) {
  const value = requireRecord('evidence', evidence);
  assertSafeEvidence(value);
  if (value.evidence_schema_version !== 1 || value.global !== '47/49') {
    throw new TypeError('unexpected Global47 evidence identity');
  }
  if (value.consensus_sha256 !== CONSENSUS_SHA256) throw new TypeError('consensus SHA mismatch');
  validateCrossSystemRepositoryEvidence(value.repositories);
  if (!Array.isArray(value.cases) || value.cases.length !== CROSS_SYSTEM_FAULT_CASES.length) {
    throw new TypeError('every Global47 fault case is required');
  }
  const expectedCases = new Map(CROSS_SYSTEM_FAULT_CASES.map((entry) => [entry.case_id, entry]));
  const observedCases = new Set();
  for (const entry of value.cases) {
    const observed = requireRecord('case evidence', entry);
    const declared = expectedCases.get(observed.case_id);
    if (!declared || observedCases.has(observed.case_id)) throw new TypeError('unknown or duplicate case');
    observedCases.add(observed.case_id);
    for (const dimension of REQUIRED_ACCEPTANCE_DIMENSIONS) {
      if (observed[dimension] !== declared.expected[dimension]) {
        throw new TypeError(`${observed.case_id} ${dimension} mismatch`);
      }
    }
    if (!Array.isArray(observed.probes) || observed.probes.length !== declared.probes.length) {
      throw new TypeError(`${observed.case_id} probe evidence is incomplete`);
    }
    for (const result of observed.probes) {
      validateProbeResult(result);
      if (result.exit_code !== 0) throw new TypeError(`${observed.case_id} probe failed`);
    }
  }
  validateContractEvidence(value.contract_evidence);
  const realStatus = deriveRealStatus(value.real_evidence);
  if (value.real_status !== realStatus) throw new TypeError('real evidence status mismatch');
  if (value.deterministic_status !== 'passed') throw new TypeError('deterministic evidence must pass');
  if (value.release_ready !== (realStatus === 'passed')) {
    throw new TypeError('release_ready must require real evidence');
  }
  return value;
}

export function buildCrossSystemFaultInjectionEvidence({
  repositoryEvidence,
  probeResults,
  contractEvidence,
  realEvidence,
  versions,
}) {
  const resultsById = new Map(probeResults.map((result) => [result.probe_id, result]));
  const cases = CROSS_SYSTEM_FAULT_CASES.map((entry) => ({
    case_id: entry.case_id,
    ...entry.expected,
    probes: entry.probes.map(({ probe_id: probeId }) => resultsById.get(probeId)),
  }));
  const realStatus = deriveRealStatus(realEvidence);
  const evidence = {
    evidence_schema_version: 1,
    global: '47/49',
    consensus_sha256: CONSENSUS_SHA256,
    deterministic_status: cases.every(({ probes }) => probes.every(
      (result) => result?.exit_code === 0,
    )) ? 'passed' : 'failed',
    real_status: realStatus,
    release_ready: realStatus === 'passed',
    repositories: repositoryEvidence,
    cases,
    contract_evidence: contractEvidence,
    real_evidence: realEvidence,
    versions,
  };
  validateCrossSystemFaultInjectionEvidence(evidence);
  return Object.freeze(evidence);
}

export function hashProbeOutput(value) {
  return sha256(typeof value === 'string' ? value : '');
}

export function runCrossSystemFaultProbe(item, {
  coreDirectory,
  environment = process.env,
  spawn = spawnSync,
} = {}) {
  const probeItem = requireRecord('probe item', item);
  if (typeof coreDirectory !== 'string' || coreDirectory.length === 0) {
    throw new TypeError('coreDirectory must be a non-empty path');
  }
  let args;
  if (probeItem.kind === 'jest') {
    args = [
      '--experimental-vm-modules',
      path.join(coreDirectory, 'node_modules', '.bin', 'jest'),
      '--runInBand',
      probeItem.target.file,
      '--testNamePattern',
      probeItem.target.test_name,
    ];
  } else if (probeItem.kind === 'channel_renderer') {
    args = [
      path.join(coreDirectory, 'scripts', 'e2e', 'channel-delivery-fault-probe.js'),
      probeItem.repository,
      probeItem.directory,
      probeItem.target.scenario,
    ];
  } else if (probeItem.kind === 'dashboard_smoke') {
    args = [path.join(probeItem.directory, probeItem.target.script)];
  } else {
    throw new TypeError(`unsupported fault probe kind ${String(probeItem.kind)}`);
  }
  const child = spawn(process.execPath, args, {
    cwd: probeItem.directory,
    env: {
      ...environment,
      ZYLOS_CORE_FAULT_REPO: coreDirectory,
      ZYLOS_CORE_PUBLIC_CONTRACTS_DIR: path.join(coreDirectory, 'contracts', 'public'),
      ...(probeItem.kind === 'dashboard_smoke' ? {
        ZYLOS_CORE_REPO: coreDirectory,
        ZYLOS_LUNA_REPO: probeItem.luna_directory,
      } : {}),
    },
    encoding: 'utf8',
    shell: false,
  });
  const stdout = typeof child?.stdout === 'string' ? child.stdout : '';
  let stderr = typeof child?.stderr === 'string' ? child.stderr : '';
  if (child?.error) stderr += `${stderr && !stderr.endsWith('\n') ? '\n' : ''}${child.error.message}\n`;
  if (child?.signal) stderr += `${stderr && !stderr.endsWith('\n') ? '\n' : ''}signal=${child.signal}\n`;
  let observations;
  if (probeItem.kind === 'channel_renderer' && child?.status === 0) {
    const lines = stdout.split(/\r?\n/u).filter((line) => (
      line.startsWith(CHANNEL_FAULT_PROBE_PREFIX)
    ));
    if (lines.length !== 1) {
      stderr += `${stderr && !stderr.endsWith('\n') ? '\n' : ''}`
        + 'channel probe did not emit exactly one evidence record\n';
    } else {
      try {
        observations = JSON.parse(lines[0].slice(CHANNEL_FAULT_PROBE_PREFIX.length));
        assertSafeEvidence(observations, '$.observations');
      } catch {
        stderr += `${stderr && !stderr.endsWith('\n') ? '\n' : ''}`
          + 'channel probe emitted unsafe or malformed evidence\n';
      }
    }
  }
  return Object.freeze({
    probe_id: probeItem.probe_id,
    repository: probeItem.repository,
    exit_code: Number.isInteger(child?.status)
      && (probeItem.kind !== 'channel_renderer' || observations !== undefined)
      ? child.status
      : 1,
    stdout_sha256: hashProbeOutput(stdout),
    stderr_sha256: hashProbeOutput(stderr),
    assertions: probeItem.assertions,
    ...(observations === undefined ? {} : { observations: Object.freeze(observations) }),
  });
}

export function resolveCrossSystemRepositoryDirectories({
  coreDirectory,
  workspaceDirectory,
  environment = process.env,
}) {
  const directories = {
    'zylos-core': path.resolve(coreDirectory),
    'zylos-feishu': path.resolve(
      environment.ZYLOS_FEISHU_FAULT_REPO
        || path.join(workspaceDirectory, '.codex-worktrees', 'zylos-feishu-integration'),
    ),
    'zylos-lark': path.resolve(
      environment.ZYLOS_LARK_FAULT_REPO
        || path.join(workspaceDirectory, '.codex-worktrees', 'zylos-lark-integration'),
    ),
    'zylos-dashboard': path.resolve(
      environment.ZYLOS_DASHBOARD_FAULT_REPO
        || path.join(workspaceDirectory, '.codex-worktrees', 'zylos-dashboard-integration'),
    ),
    'luna-pet': path.resolve(
      environment.ZYLOS_LUNA_FAULT_REPO
        || path.join(workspaceDirectory, '.codex-worktrees', 'luna-pet-integration'),
    ),
  };
  for (const [repository, directory] of Object.entries(directories)) {
    if (!fs.existsSync(path.join(directory, 'package.json'))) {
      throw new TypeError(`${repository} fault-injection repository is unavailable`);
    }
  }
  return Object.freeze(directories);
}

function gitOutput(directory, args, spawn) {
  const result = spawn('git', args, {
    cwd: directory,
    encoding: 'utf8',
    shell: false,
  });
  if (result?.status !== 0) throw new TypeError(`git ${args[0]} failed for fault repository`);
  return typeof result.stdout === 'string' ? result.stdout.trim() : '';
}

export function inspectCrossSystemRepository(repository, directory, { spawn = spawnSync } = {}) {
  const baseline = CROSS_SYSTEM_BASELINES[repository];
  if (!baseline) throw new TypeError(`unknown fault repository ${repository}`);
  const head = gitOutput(directory, ['rev-parse', 'HEAD'], spawn);
  const mergeBase = gitOutput(directory, ['merge-base', 'HEAD', baseline], spawn);
  const status = gitOutput(directory, ['status', '--porcelain'], spawn);
  return Object.freeze({
    baseline,
    head,
    merge_base: mergeBase,
    clean: status.length === 0,
  });
}

export function detectCrossSystemRealEvidence(environment = process.env) {
  const claudeCredentialAvailable = [
    'ANTHROPIC_API_KEY',
    'ANTHROPIC_AUTH_TOKEN',
    'CLAUDE_CODE_OAUTH_TOKEN',
  ].some((name) => typeof environment[name] === 'string' && environment[name].length > 0);
  const codexFixtureAuthorized = environment.ZYLOS_GLOBAL47_CODEX_FAULT_FIXTURE === '1';
  const feishuFixtureAuthorized = environment.ZYLOS_GLOBAL47_FEISHU_DISPOSABLE_FIXTURE === '1';
  const larkFixtureAuthorized = environment.ZYLOS_GLOBAL47_LARK_DISPOSABLE_FIXTURE === '1';
  return Object.freeze({
    claude: Object.freeze({
      status: 'unavailable',
      reason: claudeCredentialAvailable
        ? 'controlled_fault_fixture_not_authorized'
        : 'credential_unavailable',
    }),
    codex: Object.freeze({
      status: 'unavailable',
      reason: codexFixtureAuthorized
        ? 'live_fault_runner_not_selected'
        : 'controlled_fault_fixture_unavailable',
    }),
    feishu: Object.freeze({
      status: 'unavailable',
      reason: feishuFixtureAuthorized
        ? 'live_fault_runner_not_selected'
        : 'disposable_target_unavailable',
    }),
    lark: Object.freeze({
      status: 'unavailable',
      reason: larkFixtureAuthorized
        ? 'live_fault_runner_not_selected'
        : 'disposable_target_unavailable',
    }),
  });
}
