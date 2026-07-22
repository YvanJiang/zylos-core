import { describe, expect, test } from '@jest/globals';

import {
  CONSENSUS_SHA256,
  CROSS_SYSTEM_BASELINES,
  CROSS_SYSTEM_EVIDENCE_PREFIX,
  CROSS_SYSTEM_FAULT_CASES,
  REQUIRED_ACCEPTANCE_DIMENSIONS,
  buildCrossSystemFaultInjectionPlan,
  buildCrossSystemFaultInjectionEvidence,
  executeCrossSystemFaultInjectionPlan,
  runCrossSystemFaultProbe,
  validateCrossSystemFaultInjectionEvidence,
} from '../scripts/lib/cross-system-fault-injection.js';

const EXPECTED_BASELINES = Object.freeze({
  'zylos-core': 'b19b7e9cbb30ca9da25bcb049c8b7f0b6fa5a907',
  'zylos-dashboard': '6142782de860990d30376942d32d5fcfa385bf7a',
  'zylos-feishu': 'e46b75d32057f79514c1a5c0fdea927b0e258263',
  'zylos-lark': '92b86f3c5ba1e2ea6f6edb36e6ed0f6cc9a8c99a',
  'luna-pet': '59ce7e9b43539c63423c1f09c43eeaac06e5c6f5',
});

function passingProbeResult(probe) {
  return {
    probe_id: probe.probe_id,
    repository: probe.repository,
    exit_code: 0,
    stdout_sha256: 'a'.repeat(64),
    stderr_sha256: 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855',
    assertions: probe.assertions,
  };
}

describe('Global47 cross-system fault-injection gate', () => {
  test('pins every repository and declares every required observable dimension per fault', () => {
    expect(CONSENSUS_SHA256).toBe(
      '57a67b0a359172924bb923aa5422398efb28e473988546e859b5e08521eac800',
    );
    expect(CROSS_SYSTEM_BASELINES).toEqual(EXPECTED_BASELINES);
    expect(CROSS_SYSTEM_EVIDENCE_PREFIX).toBe('ZYLOS_GLOBAL47_FAULT_INJECTION_EVIDENCE=');
    expect(REQUIRED_ACCEPTANCE_DIMENSIONS).toEqual([
      'classification',
      'user_notification',
      'recovery_or_fallback',
      'audit_or_metrics',
      'backlog_drain',
    ]);

    const caseIds = CROSS_SYSTEM_FAULT_CASES.map(({ case_id: caseId }) => caseId);
    expect(new Set(caseIds).size).toBe(caseIds.length);
    expect(caseIds).toEqual(expect.arrayContaining([
      'executor_restart',
      'channel_restart',
      'service_restart',
      'stale_provider_result',
      'stale_delivery_result',
      'stale_control_result',
      'stale_lease_result',
      'orphan_runtime',
      'answer_send_before_ack',
      'sqlite_busy',
      'sqlite_checkpoint_busy',
      'scheduler_busy',
      'same_root_writer_contention',
      'background_lease',
      'provider_network_fault',
      'provider_rate_limit',
      'provider_auth_fault',
      'provider_context_fault',
      'feishu_transient_delivery',
      'feishu_permanent_delivery',
      'feishu_unknown_delivery',
      'lark_transient_delivery',
      'lark_permanent_delivery',
      'lark_unknown_delivery',
      'outbox_replay',
    ]));
    for (const acceptanceCase of CROSS_SYSTEM_FAULT_CASES) {
      expect(Object.keys(acceptanceCase.expected).sort()).toEqual(
        [...REQUIRED_ACCEPTANCE_DIMENSIONS].sort(),
      );
      expect(acceptanceCase.probes.length).toBeGreaterThan(0);
      expect(acceptanceCase.probes.every(({ assertions }) => (
        REQUIRED_ACCEPTANCE_DIMENSIONS.every((dimension) => assertions.includes(dimension))
      ))).toBe(true);
    }
  });

  test('builds machine evidence only from successful public-seam probes and raw contract proof', () => {
    const plan = CROSS_SYSTEM_FAULT_CASES.flatMap(({ probes }) => probes);
    const uniquePlan = [...new Map(plan.map((probe) => [probe.probe_id, probe])).values()];
    const outcome = executeCrossSystemFaultInjectionPlan(uniquePlan, {
      runProbe: passingProbeResult,
    });
    const repositoryEvidence = Object.fromEntries(Object.entries(EXPECTED_BASELINES).map(
      ([repository, baseline]) => [repository, {
        baseline,
        head: repository === 'zylos-core' ? 'b'.repeat(40) : baseline,
        merge_base: baseline,
        clean: true,
      }],
    ));
    const evidence = buildCrossSystemFaultInjectionEvidence({
      repositoryEvidence,
      probeResults: outcome.results,
      contractEvidence: Object.keys(EXPECTED_BASELINES)
        .filter((repository) => repository !== 'zylos-core')
        .map((repository) => ({
          repository,
          fixture_sha256: 'c'.repeat(64),
          raw_jcs_count: 11,
          idempotency_key_count: 6,
          payload_hash_count: 6,
        })),
      realEvidence: {
        claude: { status: 'unavailable', reason: 'credential_unavailable' },
        codex: { status: 'unavailable', reason: 'controlled_fixture_unavailable' },
        feishu: { status: 'unavailable', reason: 'disposable_target_unavailable' },
        lark: { status: 'unavailable', reason: 'disposable_target_unavailable' },
      },
      versions: { node: 'v25.8.0' },
    });

    expect(outcome.passed).toBe(true);
    expect(evidence.deterministic_status).toBe('passed');
    expect(evidence.real_status).toBe('unavailable');
    expect(evidence.release_ready).toBe(false);
    expect(evidence.cases).toHaveLength(CROSS_SYSTEM_FAULT_CASES.length);
    expect(evidence.contract_evidence).toHaveLength(4);
    expect(() => validateCrossSystemFaultInjectionEvidence(evidence)).not.toThrow();
  });

  test('builds one deduplicated executable plan and retains only hashes from child output', () => {
    const repositoryDirectories = Object.freeze(Object.fromEntries(
      Object.keys(EXPECTED_BASELINES).map((repository) => [repository, `/fixtures/${repository}`]),
    ));
    const plan = buildCrossSystemFaultInjectionPlan({ repositoryDirectories });
    const declaredProbeIds = new Set(CROSS_SYSTEM_FAULT_CASES.flatMap(
      ({ probes }) => probes.map(({ probe_id: probeId }) => probeId),
    ));
    expect(plan).toHaveLength(declaredProbeIds.size);
    expect(plan.every((item) => (
      item.directory === repositoryDirectories[item.repository]
    ))).toBe(true);

    const coreProbe = plan.find(({ kind }) => kind === 'jest');
    const result = runCrossSystemFaultProbe(coreProbe, {
      coreDirectory: repositoryDirectories['zylos-core'],
      spawn: (command, args, options) => ({
        status: 0,
        stdout: `public assertions passed in ${options.cwd}`,
        stderr: '',
        command,
        args,
      }),
    });
    expect(result).toEqual({
      probe_id: coreProbe.probe_id,
      repository: 'zylos-core',
      exit_code: 0,
      stdout_sha256: '5d32b017648de3ef9f1628479f31eb727f92cba3131ea0896612ca2de4a9d3a4',
      stderr_sha256: 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855',
      assertions: REQUIRED_ACCEPTANCE_DIMENSIONS,
    });
    expect(JSON.stringify(result)).not.toContain('/fixtures/');
    expect(JSON.stringify(result)).not.toContain('public assertions passed');
  });

  test('fails closed on a missing dimension, failed probe, stale baseline, or unsafe evidence', () => {
    const probe = CROSS_SYSTEM_FAULT_CASES[0].probes[0];
    const failed = executeCrossSystemFaultInjectionPlan([probe], {
      runProbe: () => ({ ...passingProbeResult(probe), exit_code: 1 }),
    });
    expect(failed.passed).toBe(false);

    const invalid = {
      evidence_schema_version: 1,
      global: '47/49',
      consensus_sha256: CONSENSUS_SHA256,
      deterministic_status: 'passed',
      real_status: 'unavailable',
      release_ready: false,
      repositories: {
        'zylos-core': {
          baseline: EXPECTED_BASELINES['zylos-core'],
          head: 'b'.repeat(40),
          merge_base: '0'.repeat(40),
          clean: true,
        },
      },
      cases: [{
        case_id: 'executor_restart',
        classification: 'safe_restart',
        user_notification: 'durable_ack',
        recovery_or_fallback: 'durable_queue_resume',
        audit_or_metrics: 'snapshot',
        backlog_drain: 'drained',
        probes: [],
        api_token: 'must-not-appear',
      }],
      contract_evidence: [],
      real_evidence: {},
      versions: {},
    };
    expect(() => validateCrossSystemFaultInjectionEvidence(invalid)).toThrow();

    const secretShapedValue = structuredClone(invalid);
    delete secretShapedValue.cases[0].api_token;
    secretShapedValue.real_evidence = {
      codex: { status: 'unavailable', reason: ['Bearer', 'must-not-appear'].join(' ') },
    };
    expect(() => validateCrossSystemFaultInjectionEvidence(secretShapedValue))
      .toThrow('unsafe evidence value');
  });
});
