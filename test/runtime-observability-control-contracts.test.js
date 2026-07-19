import { readFileSync } from 'node:fs';

import { describe, expect, test } from '@jest/globals';

import {
  CANONICAL_TURN_STATES,
  CONTROL_REQUEST_V1_SCHEMA,
  CONTROL_RESULT_V1_SCHEMA,
  ContractKernelError,
  DASHBOARD_RUNTIME_PROJECTION_V1_SCHEMA,
  NORMALIZED_EVENT_PHASES,
  OBSERVABILITY_SNAPSHOT_V1_SCHEMA,
  resolveControlResultUpdate,
  resolveDashboardRuntimeProjectionUpdate,
  resolveObservabilitySnapshotUpdate,
  TURN_PHASES,
  TURN_STATES,
  validateControlRequest,
  validateControlResult,
  validateDashboardRuntimeProjection,
  validateObservabilitySnapshot,
  validatePublicFixtureSafety,
} from '../contracts/public/index.js';

const observabilityFixture = JSON.parse(readFileSync(
  new URL('../contracts/public/fixtures/observability-v1.json', import.meta.url),
  'utf8',
));

const controlFixture = JSON.parse(readFileSync(
  new URL('../contracts/public/fixtures/control-v1.json', import.meta.url),
  'utf8',
));

const projectionFixture = JSON.parse(readFileSync(
  new URL('../contracts/public/fixtures/dashboard-runtime-projection-v1.json', import.meta.url),
  'utf8',
));

describe('shared runtime vocabulary', () => {
  test('reuses the normalized event turn states and phases by identity', () => {
    expect(CANONICAL_TURN_STATES).toBe(TURN_STATES);
    expect(TURN_PHASES).toBe(NORMALIZED_EVENT_PHASES);
  });
});

describe('observability snapshot v1 contract', () => {
  test('publishes complete and explicitly degraded collection snapshots', () => {
    expect(OBSERVABILITY_SNAPSHOT_V1_SCHEMA.contract).toBe('zylos.observability-snapshot');
    expect(validatePublicFixtureSafety(observabilityFixture)).toBe(true);

    const complete = validateObservabilitySnapshot(observabilityFixture.cases.complete);
    expect(complete.known.snapshot_id).toBe('snapshot-complete-A');
    expect(complete.known.outbox.complete).toBe(true);

    const degraded = validateObservabilitySnapshot(observabilityFixture.cases.degraded);
    expect(degraded.known.outbox.complete).toBe(false);
    expect(degraded.known.outbox.error.code).toBe('observability_degraded');
    expect(degraded.known.error.code).toBe('observability_degraded');
  });

  test('treats runtime process identity as diagnostic-only and exposes unknown answer delivery', () => {
    const snapshot = validateObservabilitySnapshot(observabilityFixture.cases.complete).known;
    expect(snapshot.executors.items[0].runtime_identity).toEqual({
      diagnostic_only: true,
      pid: 4242,
      pgid: 4242,
      process_start_time: '2026-07-19T05:55:00Z',
    });
    expect(snapshot.interactions.items[0]).toMatchObject({
      state: 'delivery_unknown',
      handoff_state: 'delivery_unknown',
    });

    const authoritativePid = structuredClone(observabilityFixture.cases.complete);
    authoritativePid.executors.items[0].runtime_identity.diagnostic_only = false;
    expect(() => validateObservabilitySnapshot(authoritativePid)).toThrow(ContractKernelError);
  });

  test('replaces snapshots by service instance/version and rejects a conflicting equal version', () => {
    expect(resolveObservabilitySnapshotUpdate(
      observabilityFixture.cases.complete,
      observabilityFixture.cases.degraded,
    )).toEqual({ status: 'replace', apply: true });

    expect(resolveObservabilitySnapshotUpdate(
      observabilityFixture.cases.degraded,
      observabilityFixture.cases.replacement_instance,
    )).toEqual({ status: 'replace_instance', apply: true });

    const conflicting = structuredClone(observabilityFixture.cases.complete);
    conflicting.snapshot_id = 'snapshot-conflicting-A';
    expect(() => resolveObservabilitySnapshotUpdate(
      observabilityFixture.cases.complete,
      conflicting,
    )).toThrow(ContractKernelError);

    const degradedReplacement = structuredClone(observabilityFixture.cases.degraded);
    degradedReplacement.core_service_instance_id = 'core-service-restarting';
    degradedReplacement.service.service_instance_id = 'core-service-restarting';
    degradedReplacement.snapshot_version = 1;
    expect(resolveObservabilitySnapshotUpdate(
      observabilityFixture.cases.complete,
      degradedReplacement,
    )).toEqual({
      status: 'replace_instance_degraded',
      apply: false,
      requires_full: true,
    });
  });
});

describe('operations control v1 contracts', () => {
  test('validates caller namespace, trusted auth metadata, capability scopes, and every target shape', () => {
    expect(CONTROL_REQUEST_V1_SCHEMA.contract).toBe('zylos.control-request');
    expect(CONTROL_RESULT_V1_SCHEMA.contract).toBe('zylos.control-result');
    expect(validatePublicFixtureSafety(controlFixture)).toBe(true);

    const expectedTargets = [
      ['inspect', 'conversation'],
      ['stop_active_turn', 'turn'],
      ['clear_unstarted_queue', 'queue'],
      ['reconcile', 'service'],
      ['evict_idle_executor', 'executor'],
      ['confirm_recovery', 'recovery'],
      ['reject_recovery', 'recovery'],
    ];
    expect(Object.entries(controlFixture.requests).map(([name, request]) => {
      const validated = validateControlRequest(request).known;
      return [name, validated.target.aggregate_type];
    })).toEqual(expectedTargets);

    expect(controlFixture.requests.stop_active_turn.actor.capabilities[0]).toMatchObject({
      capability: 'turn.stop',
      policy_id: 'runtime-operations',
      policy_version: 3,
      scope: { scope_type: 'conversation', conversation_id: 'conversation-A' },
    });
    expect(controlFixture.requests.stop_active_turn.auth_context).toMatchObject({
      source: 'dashboard_session',
      authorization_policy_id: 'runtime-operations',
      authorization_policy_version: 3,
    });
  });

  test('rejects caller aliases, untrusted actor data, invalid scope nullability, and missing mutation CAS', () => {
    const invalidNamespace = structuredClone(controlFixture.requests.inspect);
    invalidNamespace.caller_namespace = 'Dashboard Display Name';
    expect(() => validateControlRequest(invalidNamespace)).toThrow(ContractKernelError);

    const unauthenticated = structuredClone(controlFixture.requests.inspect);
    unauthenticated.actor.authenticated = false;
    expect(() => validateControlRequest(unauthenticated)).toThrow(ContractKernelError);

    const invalidScope = structuredClone(controlFixture.requests.reconcile);
    invalidScope.actor.capabilities[0].scope.bot_id = 'bot-A';
    expect(() => validateControlRequest(invalidScope)).toThrow(ContractKernelError);

    const missingCas = structuredClone(controlFixture.requests.stop_active_turn);
    missingCas.expected_version = null;
    expect(() => validateControlRequest(missingCas)).toThrow(ContractKernelError);
  });

  test('requires an action capability grant whose scope covers auth context and target', () => {
    const recoveryScopedInspect = structuredClone(controlFixture.requests.inspect);
    recoveryScopedInspect.target = {
      aggregate_type: 'recovery',
      recovery_id: 'recovery-A',
    };
    recoveryScopedInspect.actor.capabilities[0].scope = structuredClone(
      controlFixture.requests.confirm_recovery.actor.capabilities[0].scope,
    );
    expect(validateControlRequest(recoveryScopedInspect).known.target)
      .toEqual(recoveryScopedInspect.target);

    const wrongCapability = structuredClone(controlFixture.requests.stop_active_turn);
    wrongCapability.actor.capabilities[0].capability = 'runtime.inspect';
    expect(() => validateControlRequest(wrongCapability)).toThrow(ContractKernelError);

    const wrongConversation = structuredClone(controlFixture.requests.stop_active_turn);
    wrongConversation.actor.capabilities[0].scope.conversation_id = 'conversation-B';
    expect(() => validateControlRequest(wrongConversation)).toThrow(ContractKernelError);

    const wrongTenant = structuredClone(controlFixture.requests.stop_active_turn);
    wrongTenant.actor.capabilities[0].scope.tenant_id = 'tenant-B';
    expect(() => validateControlRequest(wrongTenant)).toThrow(ContractKernelError);

    const wrongBot = structuredClone(controlFixture.requests.clear_unstarted_queue);
    wrongBot.actor.capabilities[0].scope.bot_id = 'bot-B';
    expect(() => validateControlRequest(wrongBot)).toThrow(ContractKernelError);

    const wrongService = structuredClone(controlFixture.requests.reconcile);
    wrongService.actor.capabilities[0].scope.service_instance_id = 'core-service-B';
    expect(() => validateControlRequest(wrongService)).toThrow(ContractKernelError);

    const wrongRecovery = structuredClone(controlFixture.requests.confirm_recovery);
    wrongRecovery.actor.capabilities[0].scope.recovery_id = 'recovery-B';
    expect(() => validateControlRequest(wrongRecovery)).toThrow(ContractKernelError);
  });

  test('publishes fenced result versions for synchronous and asynchronous controls', () => {
    for (const result of Object.values(controlFixture.results)) {
      expect(validateControlResult(result).known.control_result_version).toBeGreaterThan(0);
    }
    expect(validateControlResult(controlFixture.results.reconcile_accepted).metadata.action)
      .toBe('reconcile');

    expect(resolveControlResultUpdate(
      controlFixture.results.reconcile_accepted,
      controlFixture.results.reconcile_completed,
    )).toEqual({ status: 'replace', apply: true });

    const conflict = structuredClone(controlFixture.results.reconcile_accepted);
    conflict.target_version = 99;
    expect(() => resolveControlResultUpdate(
      controlFixture.results.reconcile_accepted,
      conflict,
    )).toThrow(ContractKernelError);

    const changedTrace = structuredClone(controlFixture.results.reconcile_completed);
    changedTrace.trace_id = 'trace-other';
    expect(() => resolveControlResultUpdate(
      controlFixture.results.reconcile_accepted,
      changedTrace,
    )).toThrow(ContractKernelError);

    const changedTarget = structuredClone(controlFixture.results.reconcile_completed);
    changedTarget.target.service_instance_id = 'core-service-other';
    expect(() => resolveControlResultUpdate(
      controlFixture.results.reconcile_accepted,
      changedTarget,
    )).toThrow(ContractKernelError);

    const changedAction = structuredClone(controlFixture.results.reconcile_completed);
    changedAction.result = { snapshot: { health: 'healthy' } };
    expect(() => resolveControlResultUpdate(
      controlFixture.results.reconcile_accepted,
      changedAction,
    )).toThrow(ContractKernelError);
  });

  test('constrains successful result objects and audits every mutation result', () => {
    const misspelledResult = structuredClone(controlFixture.results.stop_completed);
    misspelledResult.result.active_turn_vesrion = misspelledResult.result.active_turn_version;
    delete misspelledResult.result.active_turn_version;
    expect(() => validateControlResult(misspelledResult)).toThrow(ContractKernelError);

    const unauditedMutation = structuredClone(controlFixture.results.stop_completed);
    unauditedMutation.audit_id = null;
    expect(() => validateControlResult(unauditedMutation)).toThrow(ContractKernelError);

    const sensitiveInspect = structuredClone(controlFixture.results.stop_completed);
    sensitiveInspect.target = {
      aggregate_type: 'conversation',
      conversation_id: 'conversation-A',
    };
    sensitiveInspect.audit_id = null;
    sensitiveInspect.result = { snapshot: { password: 'not-public' } };
    expect(() => validateControlResult(sensitiveInspect)).toThrow(ContractKernelError);

    const ambiguousFailedMutation = structuredClone(controlFixture.results.forbidden_policy);
    ambiguousFailedMutation.status = 'failed';
    ambiguousFailedMutation.audit_id = null;
    expect(() => validateControlResult(ambiguousFailedMutation)).toThrow(ContractKernelError);
    expect(() => validateControlResult(
      ambiguousFailedMutation,
      { action: 'reconcile' },
    )).toThrow(ContractKernelError);
    ambiguousFailedMutation.audit_id = 'audit-failed-reconcile';
    expect(validateControlResult(
      ambiguousFailedMutation,
      { action: 'reconcile' },
    ).metadata.action).toBe('reconcile');
  });
});

describe('Dashboard to Luna runtime projection v1 contract', () => {
  test('requires a full first package and keeps Luna read-only behind Dashboard', () => {
    expect(DASHBOARD_RUNTIME_PROJECTION_V1_SCHEMA.contract)
      .toBe('zylos.dashboard-runtime-projection');
    expect(validatePublicFixtureSafety(projectionFixture)).toBe(true);

    const initial = validateDashboardRuntimeProjection(projectionFixture.cases.initial_full).known;
    expect(initial.capabilities).toMatchObject({
      control: false,
      core_direct_access: false,
    });
    expect(resolveDashboardRuntimeProjectionUpdate(
      null,
      projectionFixture.cases.initial_full,
    )).toEqual({ status: 'initial', apply: true, requires_full: false });
    expect(resolveDashboardRuntimeProjectionUpdate(
      null,
      projectionFixture.cases.degraded,
    )).toEqual({ status: 'initial_degraded', apply: false, requires_full: true });

    const controlLeak = structuredClone(projectionFixture.cases.initial_full);
    controlLeak.capabilities.control = true;
    expect(() => validateDashboardRuntimeProjection(controlLeak)).toThrow(ContractKernelError);
  });

  test('orders by Dashboard instance/sequence and makes gaps wait for a full replacement', () => {
    expect(resolveDashboardRuntimeProjectionUpdate(
      projectionFixture.cases.initial_full,
      projectionFixture.cases.next,
    )).toEqual({ status: 'replace', apply: true, requires_full: false });
    expect(resolveDashboardRuntimeProjectionUpdate(
      projectionFixture.cases.initial_full,
      projectionFixture.cases.gap,
    )).toEqual({ status: 'gap', apply: false, requires_full: true });
    expect(resolveDashboardRuntimeProjectionUpdate(
      projectionFixture.cases.initial_full,
      projectionFixture.cases.initial_full,
      { requiresFull: true },
    )).toEqual({ status: 'duplicate', apply: false, requires_full: true });
    expect(resolveDashboardRuntimeProjectionUpdate(
      projectionFixture.cases.initial_full,
      projectionFixture.cases.degraded,
      { requiresFull: true },
    )).toEqual({ status: 'degraded', apply: false, requires_full: true });
    expect(resolveDashboardRuntimeProjectionUpdate(
      projectionFixture.cases.initial_full,
      projectionFixture.cases.gap,
      { requiresFull: true },
    )).toEqual({ status: 'resync', apply: true, requires_full: false });
    expect(resolveDashboardRuntimeProjectionUpdate(
      projectionFixture.cases.degraded,
      projectionFixture.cases.replacement_instance,
    )).toEqual({ status: 'replace_instance', apply: true, requires_full: false });
  });

  test('keeps degraded state explicit and rejects unknown major/canonical state as unsupported', () => {
    const degraded = validateDashboardRuntimeProjection(projectionFixture.cases.degraded).known;
    expect(degraded.complete).toBe(false);
    expect(degraded.error.code).toBe('observability_degraded');

    for (const [name, errorCode] of [
      ['unsupported_major', 'unsupported_contract_version'],
      ['unknown_state', 'unsupported_capability'],
    ]) {
      try {
        validateDashboardRuntimeProjection(projectionFixture.cases[name]);
        throw new Error(`expected ${name} to be unsupported`);
      } catch (error) {
        expect(error).toBeInstanceOf(ContractKernelError);
        expect(error.contractError.code).toBe(errorCode);
      }
    }
  });

  test('preserves additive same-major projection fields without treating them as capabilities', () => {
    const extended = structuredClone(projectionFixture.cases.initial_full);
    extended.contract_version = '1.3';
    extended.future_optional = { display_hint: 'compact' };
    extended.runtimes[0].future_optional = { color_hint: 'teal' };
    extended.capabilities.future_optional = { rendering_mode: 'compact' };

    const validated = validateDashboardRuntimeProjection(extended);
    expect(validated.extensions).toEqual({ future_optional: { display_hint: 'compact' } });
    expect(validated.known.runtimes[0].future_optional).toEqual({ color_hint: 'teal' });
    expect(validated.known.capabilities).toMatchObject({
      control: false,
      core_direct_access: false,
    });
  });

  test('does not erase non-JSON extension identity during duplicate comparison', () => {
    const unsafe = structuredClone(projectionFixture.cases.initial_full);
    unsafe.future_optional = new Date(0);
    expect(() => resolveDashboardRuntimeProjectionUpdate(unsafe, unsafe))
      .toThrow(ContractKernelError);
  });
});
