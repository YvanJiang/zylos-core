import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { afterEach, describe, expect, test } from '@jest/globals';
import Database from '../skills/comm-bridge/node_modules/better-sqlite3/lib/index.js';

import { createIdempotencyKey } from '../contracts/public/index.js';
import { createOperationsControlService } from '../runtime/control/operations-control-service.js';
import { createOutboxService } from '../runtime/delivery/outbox-service.js';
import { createExecutorService } from '../runtime/executor/service.js';
import { acceptNormalInbound } from '../runtime/persistence/inbound-acceptance.js';
import { createExecutorStore } from '../runtime/persistence/executor-store.js';
import { initializeRuntimePersistence } from '../runtime/persistence/schema.js';
import { deliveredResult } from './helpers/delivered-result.js';

const inboundFixture = JSON.parse(fs.readFileSync(
  new URL('../contracts/public/fixtures/inbound-envelope-v1.json', import.meta.url),
  'utf8',
));

const temporaryDirectories = [];

function openTestDatabase() {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'zylos-operations-control-'));
  temporaryDirectories.push(directory);
  return new Database(path.join(directory, 'c4.db'));
}

function deterministicIds(namespace) {
  const counts = new Map();
  return (kind) => {
    const next = (counts.get(kind) ?? 0) + 1;
    counts.set(kind, next);
    return `${kind}-${namespace}-${next}`;
  };
}

function deferred() {
  let resolve;
  const promise = new Promise((resolvePromise) => { resolve = resolvePromise; });
  return { promise, resolve };
}

function normalEnvelope(suffix) {
  const fixture = inboundFixture.valid.find(
    ({ name }) => name === 'authenticated_dm_with_attachment',
  ).document;
  const envelope = structuredClone(fixture);
  envelope.inbound_event_id = `evt-${suffix}`;
  envelope.trace_id = `trace-${suffix}`;
  envelope.message_id = `message-${suffix}`;
  envelope.idempotency_key = createIdempotencyKey('inbound', {
    region: envelope.region,
    tenant_id: envelope.tenant_id,
    channel: envelope.channel,
    bot_id: envelope.bot_id,
    inbound_event_id: envelope.inbound_event_id,
  });
  return envelope;
}

function replyEnvelope(source, suffix, platformMessageId) {
  const envelope = normalEnvelope(suffix);
  envelope.chat_id = source.chat_id;
  envelope.reply = {
    root_message_id: platformMessageId,
    parent_message_id: platformMessageId,
    reply_to_message_id: platformMessageId,
  };
  return envelope;
}

function deliverNext(database, namespace) {
  const outbox = createOutboxService({
    database,
    serviceInstanceId: `delivery-${namespace}`,
    now: () => '2026-07-20T08:00:01Z',
    generateId: deterministicIds(`delivery-${namespace}`),
    throttleMs: 0,
  });
  const command = outbox.claimNext();
  if (!command) throw new Error(`No pending delivery exists for ${namespace}.`);
  const result = deliveredResult(command, '2026-07-20T08:00:01Z');
  outbox.recordResult(result);
  return { command, result };
}

function deliverUntilTurn(database, turnId, namespace) {
  for (let attempt = 1; attempt <= 10; attempt += 1) {
    const delivered = deliverNext(database, `${namespace}-${attempt}`);
    if (delivered.command.mapping.turn_id === turnId) return delivered;
  }
  throw new Error(`No pending delivery exists for turn ${turnId}.`);
}

function markAssociatedSideEffectUnknown(database, turnId) {
  const row = database.prepare(`
    SELECT event_id, event_json FROM runtime_normalized_events
    WHERE turn_id = ? ORDER BY event_sequence DESC LIMIT 1
  `).get(turnId);
  const event = JSON.parse(row.event_json);
  event.error = {
    code: 'side_effect_unknown',
    category: 'provider',
    retryable: false,
    side_effect_status: 'unknown',
    user_message: 'Associated work may have produced side effects.',
    occurred_at: '2026-07-20T08:00:00Z',
  };
  database.prepare('UPDATE runtime_normalized_events SET event_json = ? WHERE event_id = ?')
    .run(JSON.stringify(event), row.event_id);
}

function inspectRequest(conversationId, controlId = 'inspect-control-A') {
  return {
    contract: 'zylos.control-request',
    contract_version: '1.0',
    trace_id: `trace-${controlId}`,
    caller_namespace: 'dashboard.prod',
    control_id: controlId,
    action: 'inspect',
    target: { aggregate_type: 'conversation', conversation_id: conversationId },
    expected_version: null,
    actor: {
      type: 'user',
      actor_id: 'forged-admin',
      authenticated: true,
      roles: ['tenant-admin'],
      capabilities: [],
    },
    auth_context: {
      source: 'dashboard_session',
      auth_subject_id: 'forged-admin',
      tenant_id: 'tenant-forged',
      bot_id: 'bot-forged',
      authorization_policy_id: 'forged-policy',
      authorization_policy_version: 999,
      authenticated_at: '2026-07-20T08:00:00Z',
    },
    reason: 'Inspect the conversation runtime state.',
    idempotency_key: createIdempotencyKey('control', {
      caller_namespace: 'dashboard.prod',
      control_id: controlId,
    }),
    created_at: '2026-07-20T08:00:01Z',
  };
}

function mutationRequest({ action, target, aggregateId, version, controlId }) {
  const request = inspectRequest(target.conversation_id ?? 'not-a-conversation', controlId);
  request.action = action;
  request.target = target;
  request.expected_version = {
    aggregate_type: target.aggregate_type,
    aggregate_id: aggregateId,
    version,
  };
  request.reason = `Execute ${action} through the operations control seam.`;
  return request;
}

function operationsPolicy(envelope, conversationId) {
  return {
    policy_id: 'runtime-operations',
    policy_version: 7,
    grants: [{
      grant_id: 'grant-inspect-A',
      subject: { type: 'user', subject_id: 'operator-A' },
      capability: 'runtime.inspect',
      scope: {
        scope_type: 'conversation',
        region: envelope.region,
        tenant_id: envelope.tenant_id,
        bot_id: envelope.bot_id,
        conversation_id: conversationId,
        service_instance_id: null,
        recovery_id: null,
      },
      state: 'active',
      expires_at: null,
    }],
  };
}

function trustedTransport() {
  return {
    source: 'dashboard_session',
    verified_subject: { type: 'user', subject_id: 'operator-A', roles: ['observer'] },
    authorization_policy_id: 'runtime-operations',
    authorization_policy_version: 7,
    authenticated_at: '2026-07-20T08:00:00Z',
  };
}

afterEach(() => {
  for (const directory of temporaryDirectories.splice(0)) {
    fs.rmSync(directory, { recursive: true, force: true });
  }
});

describe('capability-first operations control', () => {
  test('migrates existing runtime aggregates before installing operations version triggers', () => {
    const database = openTestDatabase();
    initializeRuntimePersistence(database);
    database.exec(`
      DROP TRIGGER runtime_turn_queue_insert_version;
      DROP TRIGGER runtime_turn_queue_update_version;
      DROP TRIGGER runtime_turn_queue_delete_version;
      DROP TRIGGER runtime_execution_recovery_state_versions;
      DROP TRIGGER runtime_reply_mapping_recovery_state_versions;
      ALTER TABLE runtime_conversations DROP COLUMN queue_version;
      ALTER TABLE runtime_execution_recoveries DROP COLUMN recovery_version;
      ALTER TABLE runtime_reply_mapping_recoveries DROP COLUMN recovery_version;
    `);
    const databasePath = database.name;
    database.close();

    const reopened = new Database(databasePath);
    expect(() => initializeRuntimePersistence(reopened)).not.toThrow();
    expect(reopened.prepare("PRAGMA table_info('runtime_conversations')").all()
      .some(({ name }) => name === 'queue_version')).toBe(true);
    expect(reopened.prepare("PRAGMA table_info('runtime_execution_recoveries')").all()
      .some(({ name }) => name === 'recovery_version')).toBe(true);
    expect(reopened.prepare("PRAGMA table_info('runtime_reply_mapping_recoveries')").all()
      .some(({ name }) => name === 'recovery_version')).toBe(true);
    expect(reopened.prepare(`
      SELECT COUNT(*) AS count FROM sqlite_master
      WHERE type = 'trigger' AND name IN (
        'runtime_turn_queue_insert_version',
        'runtime_turn_queue_update_version',
        'runtime_turn_queue_delete_version',
        'runtime_execution_recovery_state_versions',
        'runtime_reply_mapping_recovery_state_versions'
      )
    `).get().count).toBe(5);
    reopened.close();
  });

  test('overwrites caller-reported auth with the verified subject and current deployment grant', async () => {
    const database = openTestDatabase();
    const envelope = normalEnvelope('verified-subject');
    const accepted = acceptNormalInbound(database, envelope, {
      now: () => '2026-07-20T08:00:00Z',
      generateId: deterministicIds('verified-subject'),
    });
    const service = createOperationsControlService({
      database,
      serviceInstanceId: 'core-service-operations-A',
      deploymentPolicy: operationsPolicy(envelope, accepted.conversation_id),
      now: () => '2026-07-20T08:00:02Z',
      generateId: deterministicIds('operations'),
    });

    const result = await service.execute(
      inspectRequest(accepted.conversation_id),
      trustedTransport(),
    );

    expect(result).toMatchObject({
      contract: 'zylos.control-result',
      control_id: 'inspect-control-A',
      control_result_version: 1,
      status: 'completed',
      previous_target_version: 1,
      target_version: 1,
      error: null,
      result: {
        snapshot: {
          aggregate_type: 'conversation',
          conversation_id: accepted.conversation_id,
          region: envelope.region,
          tenant_id: envelope.tenant_id,
          bot_id: envelope.bot_id,
        },
      },
    });
  });

  test('durably rejects a stale trusted policy version and replays the same forbidden result', async () => {
    const database = openTestDatabase();
    const envelope = normalEnvelope('stale-policy');
    const accepted = acceptNormalInbound(database, envelope, {
      now: () => '2026-07-20T08:00:00Z',
      generateId: deterministicIds('stale-policy'),
    });
    const service = createOperationsControlService({
      database,
      serviceInstanceId: 'core-service-operations-A',
      deploymentPolicy: operationsPolicy(envelope, accepted.conversation_id),
      now: () => '2026-07-20T08:00:02Z',
      generateId: deterministicIds('stale-policy-control'),
    });
    const stale = trustedTransport();
    stale.authorization_policy_version = 6;

    const first = await service.execute(inspectRequest(accepted.conversation_id), stale);
    const replay = await service.execute(inspectRequest(accepted.conversation_id), stale);

    expect(first).toMatchObject({
      status: 'forbidden',
      control_result_version: 1,
      previous_target_version: null,
      target_version: null,
      result: null,
      accepted_at: null,
      completed_at: '2026-07-20T08:00:02Z',
      error: { code: 'forbidden', category: 'authorization' },
    });
    expect(first.audit_id).toMatch(/^operations-audit-/);
    expect(replay).toEqual(first);
  });

  test('returns a durable idempotency conflict for the same control ID with a different payload', async () => {
    const database = openTestDatabase();
    const envelope = normalEnvelope('idempotency-conflict');
    const accepted = acceptNormalInbound(database, envelope, {
      now: () => '2026-07-20T08:00:00Z',
      generateId: deterministicIds('idempotency-conflict'),
    });
    const service = createOperationsControlService({
      database,
      serviceInstanceId: 'core-service-operations-A',
      deploymentPolicy: operationsPolicy(envelope, accepted.conversation_id),
      now: () => '2026-07-20T08:00:02Z',
      generateId: deterministicIds('idempotency-conflict-control'),
    });
    const originalRequest = inspectRequest(accepted.conversation_id, 'inspect-conflict-A');
    const changedRequest = structuredClone(originalRequest);
    changedRequest.reason = 'A different business reason for the same control ID.';

    const original = await service.execute(originalRequest, trustedTransport());
    const conflict = await service.execute(changedRequest, trustedTransport());
    const replay = await service.execute(originalRequest, trustedTransport());

    expect(original.status).toBe('completed');
    expect(conflict).toMatchObject({
      status: 'conflict',
      result: null,
      error: { code: 'idempotency_conflict', category: 'conflict' },
    });
    expect(conflict.audit_id).not.toBe(original.audit_id);
    expect(replay).toEqual(original);
  });

  test.each([
    ['revoked', (policy) => { policy.grants[0].state = 'revoked'; }],
    ['expired', (policy) => { policy.grants[0].expires_at = '2026-07-20T08:00:01Z'; }],
    ['out-of-scope', (policy) => { policy.grants[0].scope.conversation_id = 'conversation-other'; }],
  ])('default-denies a %s grant even when the request self-reports an admin role', async (
    suffix,
    mutatePolicy,
  ) => {
    const database = openTestDatabase();
    const envelope = normalEnvelope(suffix);
    const accepted = acceptNormalInbound(database, envelope, {
      now: () => '2026-07-20T08:00:00Z',
      generateId: deterministicIds(suffix),
    });
    const policy = operationsPolicy(envelope, accepted.conversation_id);
    mutatePolicy(policy);
    const service = createOperationsControlService({
      database,
      serviceInstanceId: 'core-service-operations-A',
      deploymentPolicy: policy,
      now: () => '2026-07-20T08:00:02Z',
      generateId: deterministicIds(`${suffix}-control`),
    });

    const result = await service.execute(
      inspectRequest(accepted.conversation_id, `inspect-${suffix}`),
      trustedTransport(),
    );

    expect(result).toMatchObject({
      status: 'forbidden',
      error: { code: 'forbidden', category: 'authorization' },
      result: null,
    });
  });

  test('clears only the queued cutoff under queue-version CAS and replays after reopen', async () => {
    const database = openTestDatabase();
    const envelope = normalEnvelope('queue-first');
    const first = acceptNormalInbound(database, envelope, {
      now: () => '2026-07-20T08:00:00Z',
      generateId: deterministicIds('queue-first'),
    });
    const second = acceptNormalInbound(database, normalEnvelope('queue-second'), {
      now: () => '2026-07-20T08:00:00Z',
      generateId: deterministicIds('queue-second'),
    });
    const third = acceptNormalInbound(database, normalEnvelope('queue-third'), {
      now: () => '2026-07-20T08:00:00Z',
      generateId: deterministicIds('queue-third'),
    });
    const policy = operationsPolicy(envelope, first.conversation_id);
    policy.grants[0].grant_id = 'grant-queue-clear-A';
    policy.grants[0].capability = 'queue.clear';
    const service = createOperationsControlService({
      database,
      serviceInstanceId: 'core-service-operations-A',
      deploymentPolicy: policy,
      runtimeStore: createExecutorStore({
        database,
        provider: 'codex',
        serviceInstanceId: 'core-service-operations-A',
        now: () => '2026-07-20T08:00:02Z',
        generateId: deterministicIds('queue-clear-store'),
      }),
      now: () => '2026-07-20T08:00:02Z',
      generateId: deterministicIds('queue-clear'),
    });
    const request = mutationRequest({
      action: 'clear_unstarted_queue',
      target: {
        aggregate_type: 'queue',
        conversation_id: first.conversation_id,
        through_queue_sequence: 2,
      },
      aggregateId: first.conversation_id,
      version: 4,
      controlId: 'queue-clear-A',
    });

    const result = await service.execute(request, trustedTransport());
    const reopened = createOperationsControlService({
      database,
      serviceInstanceId: 'core-service-operations-A',
      deploymentPolicy: policy,
      runtimeStore: createExecutorStore({
        database,
        provider: 'codex',
        serviceInstanceId: 'core-service-operations-A',
        now: () => '2026-07-20T08:00:03Z',
        generateId: deterministicIds('queue-clear-reopen-store'),
      }),
      now: () => '2026-07-20T08:00:03Z',
      generateId: deterministicIds('queue-clear-reopen'),
    });
    const replay = await reopened.execute(request, trustedTransport());

    expect(result).toMatchObject({
      status: 'completed',
      previous_target_version: 4,
      target_version: 7,
      result: {
        cleared_turn_ids: [first.turn_id, second.turn_id],
        through_queue_sequence: 2,
      },
    });
    expect(result.result.cleared_turn_ids).not.toContain(third.turn_id);
    expect(replay).toEqual(result);
  });

  test('returns a durable CAS conflict without clearing any queued turn', async () => {
    const database = openTestDatabase();
    const envelope = normalEnvelope('queue-conflict-first');
    const first = acceptNormalInbound(database, envelope, {
      now: () => '2026-07-20T08:00:00Z',
      generateId: deterministicIds('queue-conflict-first'),
    });
    const second = acceptNormalInbound(database, normalEnvelope('queue-conflict-second'), {
      now: () => '2026-07-20T08:00:00Z',
      generateId: deterministicIds('queue-conflict-second'),
    });
    const policy = operationsPolicy(envelope, first.conversation_id);
    policy.grants[0].grant_id = 'grant-queue-conflict-A';
    policy.grants[0].capability = 'queue.clear';
    const runtimeStore = createExecutorStore({
      database,
      provider: 'codex',
      serviceInstanceId: 'core-service-operations-A',
      now: () => '2026-07-20T08:00:02Z',
      generateId: deterministicIds('queue-conflict-store'),
    });
    const service = createOperationsControlService({
      database,
      serviceInstanceId: 'core-service-operations-A',
      deploymentPolicy: policy,
      runtimeStore,
      now: () => '2026-07-20T08:00:02Z',
      generateId: deterministicIds('queue-conflict'),
    });

    const conflict = await service.execute(mutationRequest({
      action: 'clear_unstarted_queue',
      target: {
        aggregate_type: 'queue',
        conversation_id: first.conversation_id,
        through_queue_sequence: 2,
      },
      aggregateId: first.conversation_id,
      version: 2,
      controlId: 'queue-conflict-A',
    }), trustedTransport());
    const completed = await service.execute(mutationRequest({
      action: 'clear_unstarted_queue',
      target: {
        aggregate_type: 'queue',
        conversation_id: first.conversation_id,
        through_queue_sequence: 2,
      },
      aggregateId: first.conversation_id,
      version: 3,
      controlId: 'queue-after-conflict-A',
    }), trustedTransport());

    expect(conflict).toMatchObject({
      status: 'conflict',
      previous_target_version: 3,
      target_version: 3,
      result: null,
      error: { code: 'version_conflict', category: 'conflict' },
    });
    expect(completed).toMatchObject({
      status: 'completed',
      result: { cleared_turn_ids: [first.turn_id, second.turn_id] },
    });
  });

  test('rolls back queue mutation when the durable operations audit cannot commit', async () => {
    const database = openTestDatabase();
    const envelope = normalEnvelope('operations-audit-rollback');
    const accepted = acceptNormalInbound(database, envelope, {
      now: () => '2026-07-20T08:00:00Z',
      generateId: deterministicIds('operations-audit-rollback'),
    });
    const policy = operationsPolicy(envelope, accepted.conversation_id);
    policy.grants[0].grant_id = 'grant-queue-audit-rollback';
    policy.grants[0].capability = 'queue.clear';
    const store = createExecutorStore({
      database,
      provider: 'codex',
      serviceInstanceId: 'core-service-operations-A',
      now: () => '2026-07-20T08:00:02Z',
      generateId: deterministicIds('operations-audit-rollback-store'),
    });
    const service = createOperationsControlService({
      database,
      serviceInstanceId: 'core-service-operations-A',
      deploymentPolicy: policy,
      runtimeStore: store,
      now: () => '2026-07-20T08:00:02Z',
      generateId: deterministicIds('operations-audit-rollback-control'),
    });
    const before = database.prepare(`
      SELECT queue_version FROM runtime_conversations WHERE conversation_id = ?
    `).get(accepted.conversation_id);
    database.exec(`
      CREATE TRIGGER fail_operations_audit
      BEFORE INSERT ON runtime_operations_audit
      BEGIN
        SELECT RAISE(ABORT, 'simulated operations audit failure');
      END;
    `);
    const request = mutationRequest({
      action: 'clear_unstarted_queue',
      target: {
        aggregate_type: 'queue',
        conversation_id: accepted.conversation_id,
        through_queue_sequence: 1,
      },
      aggregateId: accepted.conversation_id,
      version: before.queue_version,
      controlId: 'operations-audit-rollback',
    });

    let failure;
    try {
      const returned = await service.execute(request, trustedTransport());
      failure = new Error(`unexpected result: ${JSON.stringify(returned)}`);
    } catch (error) {
      failure = error;
    }
    expect(failure).toHaveProperty('message', 'simulated operations audit failure');

    expect(database.prepare(`
      SELECT queue.status, conversation.queue_version
      FROM runtime_turn_queue AS queue
      JOIN runtime_conversations AS conversation
        ON conversation.conversation_id = queue.conversation_id
      WHERE queue.turn_id = ?
    `).get(accepted.turn_id)).toEqual({
      status: 'queued',
      queue_version: before.queue_version,
    });
    expect(database.prepare(`
      SELECT COUNT(*) AS count FROM runtime_operations_controls
      WHERE caller_namespace = ? AND control_id = ?
    `).get(request.caller_namespace, request.control_id).count).toBe(0);
  });

  test('stops only the exact active turn and leaves the queued turn untouched', async () => {
    const database = openTestDatabase();
    const envelope = normalEnvelope('operations-stop-active');
    const active = acceptNormalInbound(database, envelope, {
      now: () => '2026-07-20T08:00:00Z',
      generateId: deterministicIds('operations-stop-active'),
    });
    const queued = acceptNormalInbound(database, normalEnvelope('operations-stop-queued'), {
      now: () => '2026-07-20T08:00:00Z',
      generateId: deterministicIds('operations-stop-queued'),
    });
    const policy = operationsPolicy(envelope, active.conversation_id);
    policy.grants[0].grant_id = 'grant-turn-stop-A';
    policy.grants[0].capability = 'turn.stop';
    const providerStarted = deferred();
    const providerCancelled = deferred();
    const service = createExecutorService({
      database,
      provider: 'codex',
      adapter: {
        async *execute(context) {
          context.reportProviderState({ state: 'started', provider_native_id: null });
          providerStarted.resolve();
          await providerCancelled.promise;
          yield { type: 'turn_result', outcome: 'cancelled' };
        },
        async cancel() { providerCancelled.resolve(); },
      },
      serviceInstanceId: 'core-service-operations-A',
      operationsPolicy: policy,
      now: () => '2026-07-20T08:00:02Z',
      generateId: deterministicIds('operations-stop-service'),
    });
    service.start();
    const activeRun = service.runNext();
    await providerStarted.promise;
    const before = service.publishObservabilitySnapshot();
    const activeProjection = before.turns.items.find(({ turn_id: turnId }) => turnId === active.turn_id);
    const request = mutationRequest({
      action: 'stop_active_turn',
      target: {
        aggregate_type: 'turn',
        conversation_id: active.conversation_id,
        turn_id: active.turn_id,
      },
      aggregateId: active.turn_id,
      version: activeProjection.turn_version,
      controlId: 'operations-stop-A',
    });

    const result = await service.executeOperationsControl(request, trustedTransport());
    await activeRun;
    const after = service.publishObservabilitySnapshot();

    expect(result).toMatchObject({
      status: 'completed',
      previous_target_version: activeProjection.turn_version,
      result: {
        winner: 'stop',
        active_turn_id: active.turn_id,
        priority_turn_created: false,
        priority_turn_cancelled: false,
      },
    });
    expect(after.turns.items.find(({ turn_id: turnId }) => turnId === active.turn_id).state)
      .toBe('stopped');
    expect(after.turns.items.find(({ turn_id: turnId }) => turnId === queued.turn_id).state)
      .toBe('queued');
    await service.close();
  });

  test('registers a fenced reconciliation intent without replaying uncertain work', async () => {
    const database = openTestDatabase();
    const envelope = normalEnvelope('operations-reconcile');
    const policy = operationsPolicy(envelope, 'unused-conversation');
    policy.grants[0] = {
      ...policy.grants[0],
      grant_id: 'grant-service-reconcile-A',
      capability: 'service.reconcile',
      scope: {
        scope_type: 'service',
        region: envelope.region,
        tenant_id: envelope.tenant_id,
        bot_id: null,
        conversation_id: null,
        service_instance_id: 'core-service-operations-A',
        recovery_id: null,
      },
    };
    const service = createExecutorService({
      database,
      provider: 'codex',
      adapter: { async *execute() {} },
      serviceInstanceId: 'core-service-operations-A',
      operationsPolicy: policy,
      operationsServiceNamespace: { region: envelope.region, tenant_id: envelope.tenant_id },
      now: () => '2026-07-20T08:00:02Z',
      generateId: deterministicIds('operations-reconcile'),
    });
    const request = mutationRequest({
      action: 'reconcile',
      target: {
        aggregate_type: 'service',
        service_instance_id: 'core-service-operations-A',
      },
      aggregateId: 'core-service-operations-A',
      version: 1,
      controlId: 'operations-reconcile-A',
    });

    const result = await service.executeOperationsControl(request, trustedTransport());
    const replay = await service.executeOperationsControl(request, trustedTransport());

    expect(result).toMatchObject({
      status: 'accepted',
      control_result_version: 1,
      previous_target_version: 1,
      target_version: 2,
      completed_at: null,
      result: { state: 'pending' },
    });
    expect(result.result.intent_id).toMatch(/^reconciliation-intent-/);
    expect(replay).toEqual(result);
    await service.close();
  });

  test('evicts only the canonical idle executor target and never uses runtime process identity', async () => {
    const database = openTestDatabase();
    const envelope = normalEnvelope('operations-evict');
    const accepted = acceptNormalInbound(database, envelope, {
      now: () => '2026-07-20T08:00:00Z',
      generateId: deterministicIds('operations-evict'),
    });
    const policy = operationsPolicy(envelope, accepted.conversation_id);
    policy.grants[0].grant_id = 'grant-executor-evict-A';
    policy.grants[0].capability = 'executor.evict';
    const evictionTargets = [];
    const service = createExecutorService({
      database,
      provider: 'claude',
      adapter: {
        async *execute(context) {
          context.reportProviderState({ state: 'started', provider_native_id: 'session-evict-A' });
          yield { type: 'turn_result', outcome: 'completed' };
        },
        async evictIdle({ canEvict, targetConversationId }) {
          evictionTargets.push(targetConversationId);
          return await canEvict(targetConversationId) ? [targetConversationId] : [];
        },
        async close() {},
      },
      serviceInstanceId: 'core-service-operations-A',
      operationsPolicy: policy,
      now: () => '2026-07-20T08:00:02Z',
      generateId: deterministicIds('operations-evict-service'),
    });
    service.start();
    await service.runNext();
    const before = service.publishObservabilitySnapshot();
    const executor = before.executors.items.find(
      ({ conversation_id: conversationId }) => conversationId === accepted.conversation_id,
    );
    const request = mutationRequest({
      action: 'evict_idle_executor',
      target: {
        aggregate_type: 'executor',
        conversation_id: accepted.conversation_id,
        executor_instance_id: executor.executor_instance_id,
      },
      aggregateId: executor.executor_instance_id,
      version: executor.executor_version,
      controlId: 'operations-evict-A',
    });

    const result = await service.executeOperationsControl(request, trustedTransport());
    const after = service.publishObservabilitySnapshot();
    const afterExecutor = after.executors.items.find(
      ({ conversation_id: conversationId }) => conversationId === accepted.conversation_id,
    );

    expect(result).toMatchObject({
      status: 'completed',
      previous_target_version: executor.executor_version,
      target_version: executor.executor_version + 1,
      result: {
        evicted: true,
        executor_instance_id: executor.executor_instance_id,
      },
    });
    expect(evictionTargets).toEqual([accepted.conversation_id]);
    expect(afterExecutor.resident).toBe(false);
    await service.close();
  });

  test('records a terminal unknown-side-effect result when provider eviction cannot be proven', async () => {
    const database = openTestDatabase();
    const envelope = normalEnvelope('operations-evict-unknown');
    const accepted = acceptNormalInbound(database, envelope, {
      now: () => '2026-07-20T08:00:00Z',
      generateId: deterministicIds('operations-evict-unknown'),
    });
    const policy = operationsPolicy(envelope, accepted.conversation_id);
    policy.grants[0].grant_id = 'grant-executor-evict-unknown';
    policy.grants[0].capability = 'executor.evict';
    const service = createExecutorService({
      database,
      provider: 'claude',
      adapter: {
        async *execute(context) {
          context.reportProviderState({ state: 'started', provider_native_id: 'session-evict-unknown' });
          yield { type: 'turn_result', outcome: 'completed' };
        },
        async evictIdle() { throw new Error('provider close acknowledgement was lost'); },
        async close() {},
      },
      serviceInstanceId: 'core-service-operations-A',
      operationsPolicy: policy,
      now: () => '2026-07-20T08:00:02Z',
      generateId: deterministicIds('operations-evict-unknown-service'),
    });
    service.start();
    await service.runNext();
    const before = service.publishObservabilitySnapshot();
    const executor = before.executors.items.find(
      ({ conversation_id: conversationId }) => conversationId === accepted.conversation_id,
    );
    const request = mutationRequest({
      action: 'evict_idle_executor',
      target: {
        aggregate_type: 'executor',
        conversation_id: accepted.conversation_id,
        executor_instance_id: executor.executor_instance_id,
      },
      aggregateId: executor.executor_instance_id,
      version: executor.executor_version,
      controlId: 'operations-evict-unknown',
    });

    const result = await service.executeOperationsControl(request, trustedTransport());

    expect(result).toMatchObject({
      status: 'failed',
      control_result_version: 2,
      result: null,
      error: {
        code: 'side_effect_unknown',
        category: 'provider',
        side_effect_status: 'unknown',
      },
    });
    await service.close();
  });

  test('requires both recovery.decide and the recovery authorized subject before confirmation', async () => {
    const database = openTestDatabase();
    const envelope = normalEnvelope('operations-recovery-confirm');
    const accepted = acceptNormalInbound(database, envelope, {
      now: () => '2026-07-20T08:00:00Z',
      generateId: deterministicIds('operations-recovery-confirm'),
    });
    const policy = operationsPolicy(envelope, accepted.conversation_id);
    policy.grants[0].grant_id = 'grant-recovery-other';
    policy.grants[0].capability = 'recovery.decide';
    policy.grants.push({
      ...structuredClone(policy.grants[0]),
      grant_id: 'grant-recovery-authorized',
      subject: { type: 'user', subject_id: envelope.actor.actor_id },
    });
    const service = createExecutorService({
      database,
      provider: 'codex',
      adapter: {
        async *execute(context) {
          await context.bindProviderNativeId('native-thread-operations-recovery');
          context.reportProviderFailure({
            providerError: {
              code: 'side_effect_unknown',
              category: 'provider',
              retryable: false,
              side_effect_status: 'unknown',
              user_message: 'The provider connection was lost.',
            },
          });
        },
        async close() {},
      },
      serviceInstanceId: 'core-service-operations-A',
      operationsPolicy: policy,
      now: () => '2026-07-20T08:00:02Z',
      generateId: deterministicIds('operations-recovery-service'),
    });
    service.start();
    await expect(service.runNext()).resolves.toMatchObject({ status: 'recovering' });
    const recovery = database.prepare(`
      SELECT recovery_id, recovery_version, interaction_id
      FROM runtime_execution_recoveries WHERE turn_id = ?
    `).get(accepted.turn_id);
    const interactionRow = database.prepare(`
      SELECT request_json FROM runtime_interactions WHERE interaction_id = ?
    `).get(recovery.interaction_id);
    const scopedRequest = JSON.parse(interactionRow.request_json);
    scopedRequest.authorized_subjects = [{
      type: 'capability',
      capability: 'recovery.decide',
      scope: {
        scope_type: 'recovery',
        region: envelope.region,
        tenant_id: envelope.tenant_id,
        bot_id: envelope.bot_id,
        conversation_id: 'conversation-outside-authorized-recovery',
        service_instance_id: null,
        recovery_id: recovery.recovery_id,
      },
    }];
    database.prepare(`
      UPDATE runtime_interactions SET request_json = ? WHERE interaction_id = ?
    `).run(JSON.stringify(scopedRequest), recovery.interaction_id);
    const unauthorizedRequest = mutationRequest({
      action: 'confirm_recovery',
      target: {
        aggregate_type: 'recovery',
        recovery_id: recovery.recovery_id,
        conversation_id: accepted.conversation_id,
        turn_id: accepted.turn_id,
      },
      aggregateId: recovery.recovery_id,
      version: recovery.recovery_version,
      controlId: 'operations-recovery-unauthorized',
    });
    const unauthorized = await service.executeOperationsControl(
      unauthorizedRequest,
      trustedTransport(),
    );
    database.prepare(`
      UPDATE runtime_interactions SET request_json = ? WHERE interaction_id = ?
    `).run(interactionRow.request_json, recovery.interaction_id);
    const authorizedTransport = trustedTransport();
    authorizedTransport.verified_subject.subject_id = envelope.actor.actor_id;
    const authorizedRequest = structuredClone(unauthorizedRequest);
    authorizedRequest.control_id = 'operations-recovery-confirm';
    authorizedRequest.trace_id = 'trace-operations-recovery-confirm';
    authorizedRequest.idempotency_key = createIdempotencyKey('control', {
      caller_namespace: authorizedRequest.caller_namespace,
      control_id: authorizedRequest.control_id,
    });
    const confirmed = await service.executeOperationsControl(
      authorizedRequest,
      authorizedTransport,
    );

    expect(unauthorized).toMatchObject({
      status: 'forbidden',
      error: { code: 'forbidden', category: 'authorization' },
    });
    expect(confirmed).toMatchObject({
      status: 'completed',
      previous_target_version: recovery.recovery_version,
      target_version: recovery.recovery_version + 1,
      result: { decision: 'confirmed', recovery_turn_id: null },
    });
    await service.close();
  });

  test('rejects a recovery decision under CAS and safely terminalizes the recovering turn', async () => {
    const database = openTestDatabase();
    const envelope = normalEnvelope('operations-recovery-reject');
    const accepted = acceptNormalInbound(database, envelope, {
      now: () => '2026-07-20T08:00:00Z',
      generateId: deterministicIds('operations-recovery-reject'),
    });
    const policy = operationsPolicy(envelope, accepted.conversation_id);
    policy.grants[0] = {
      ...policy.grants[0],
      grant_id: 'grant-recovery-reject',
      subject: { type: 'user', subject_id: envelope.actor.actor_id },
      capability: 'recovery.decide',
    };
    const service = createExecutorService({
      database,
      provider: 'codex',
      adapter: {
        async *execute(context) {
          await context.bindProviderNativeId('native-thread-operations-reject');
          context.reportProviderFailure({
            providerError: {
              code: 'side_effect_unknown',
              category: 'provider',
              retryable: false,
              side_effect_status: 'unknown',
              user_message: 'The provider connection was lost.',
            },
          });
        },
        async close() {},
      },
      serviceInstanceId: 'core-service-operations-A',
      operationsPolicy: policy,
      now: () => '2026-07-20T08:00:02Z',
      generateId: deterministicIds('operations-recovery-reject-service'),
    });
    service.start();
    await service.runNext();
    const recovery = database.prepare(`
      SELECT recovery_id, recovery_version
      FROM runtime_execution_recoveries WHERE turn_id = ?
    `).get(accepted.turn_id);
    const transport = trustedTransport();
    transport.verified_subject.subject_id = envelope.actor.actor_id;
    const request = mutationRequest({
      action: 'reject_recovery',
      target: {
        aggregate_type: 'recovery',
        recovery_id: recovery.recovery_id,
        conversation_id: accepted.conversation_id,
        turn_id: accepted.turn_id,
      },
      aggregateId: recovery.recovery_id,
      version: recovery.recovery_version,
      controlId: 'operations-recovery-reject',
    });

    const result = await service.executeOperationsControl(request, transport);
    const after = service.publishObservabilitySnapshot();

    expect(result).toMatchObject({
      status: 'completed',
      result: { decision: 'rejected', recovery_turn_id: null },
      previous_target_version: recovery.recovery_version,
      target_version: recovery.recovery_version + 1,
    });
    expect(after.turns.items.find(({ turn_id: turnId }) => turnId === accepted.turn_id).state)
      .toBe('stopped');
    await service.close();
  });

  test('rejects reply-mapping recovery through the same control chain and exposes its audit', async () => {
    const database = openTestDatabase();
    const sourceEnvelope = normalEnvelope('operations-reply-recovery-source');
    const source = acceptNormalInbound(database, sourceEnvelope, {
      now: () => '2026-07-20T08:00:00Z',
      generateId: deterministicIds('operations-reply-recovery-source'),
    });
    const sourceDelivery = deliverNext(database, 'operations-reply-recovery-source');
    markAssociatedSideEffectUnknown(database, source.turn_id);
    database.exec('DROP TRIGGER IF EXISTS runtime_bound_message_mapping_delete_immutable');
    database.prepare('DELETE FROM runtime_message_mappings WHERE platform_message_id = ?')
      .run(sourceDelivery.result.platform_message_id);
    database.prepare('DELETE FROM runtime_turn_queue WHERE turn_id = ?').run(source.turn_id);
    database.prepare("UPDATE runtime_turns SET state = 'completed' WHERE turn_id = ?")
      .run(source.turn_id);

    const pending = acceptNormalInbound(
      database,
      replyEnvelope(
        sourceEnvelope,
        'operations-reply-recovery-pending',
        sourceDelivery.result.platform_message_id,
      ),
      {
        now: () => '2026-07-20T08:00:00Z',
        generateId: deterministicIds('operations-reply-recovery-pending'),
      },
    );
    const policy = operationsPolicy(sourceEnvelope, pending.conversation_id);
    policy.grants[0] = {
      ...policy.grants[0],
      grant_id: 'grant-reply-recovery-reject',
      subject: { type: 'user', subject_id: sourceEnvelope.actor.actor_id },
      capability: 'recovery.decide',
    };
    const service = createExecutorService({
      database,
      provider: 'claude',
      adapter: {
        async *execute() {
          throw new Error('provider execution must not start before the recovery decision');
        },
        async close() {},
      },
      serviceInstanceId: 'core-service-operations-A',
      operationsPolicy: policy,
      now: () => '2026-07-20T08:00:02Z',
      generateId: deterministicIds('operations-reply-recovery-service'),
      scheduleResidentHeartbeat: () => ({ unref() {} }),
      cancelResidentHeartbeat: () => {},
    });

    await service.runNext();
    deliverUntilTurn(database, pending.turn_id, 'operations-reply-recovery-notice');
    await expect(service.runNext()).resolves.toMatchObject({
      status: 'waiting_decision',
      turn_id: pending.turn_id,
    });
    const recovery = database.prepare(`
      SELECT recovery_id, recovery_version
      FROM runtime_reply_mapping_recoveries WHERE turn_id = ?
    `).get(pending.turn_id);
    const transport = trustedTransport();
    transport.verified_subject.subject_id = sourceEnvelope.actor.actor_id;
    const result = await service.executeOperationsControl(mutationRequest({
      action: 'reject_recovery',
      target: {
        aggregate_type: 'recovery',
        recovery_id: recovery.recovery_id,
        conversation_id: pending.conversation_id,
        turn_id: pending.turn_id,
      },
      aggregateId: recovery.recovery_id,
      version: recovery.recovery_version,
      controlId: 'operations-reply-recovery-reject',
    }), transport);
    const snapshot = service.publishObservabilitySnapshot();

    expect(result).toMatchObject({
      status: 'completed',
      result: { decision: 'rejected', recovery_turn_id: null },
      previous_target_version: recovery.recovery_version,
      target_version: recovery.recovery_version + 1,
    });
    expect(database.prepare(`
      SELECT turn.state, recovery.state AS recovery_state
      FROM runtime_turns AS turn
      JOIN runtime_reply_mapping_recoveries AS recovery ON recovery.turn_id = turn.turn_id
      WHERE turn.turn_id = ?
    `).get(pending.turn_id)).toEqual({ state: 'stopped', recovery_state: 'rejected' });
    expect(snapshot.audit_summary.items).toContainEqual(expect.objectContaining({
      category: 'operations_control',
      count: 1,
    }));
    await service.close();
  });
});
