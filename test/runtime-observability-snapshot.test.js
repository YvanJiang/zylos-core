import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { afterEach, describe, expect, test } from '@jest/globals';
import Database from '../skills/comm-bridge/node_modules/better-sqlite3/lib/index.js';

import {
  createIdempotencyKey,
  resolveObservabilitySnapshotUpdate,
  validateObservabilitySnapshot,
} from '../contracts/public/index.js';
import { createExecutorService } from '../runtime/executor/service.js';
import { createOutboxService } from '../runtime/delivery/outbox-service.js';
import { createRuntimeSnapshotPublisher } from '../runtime/observability/snapshot-publisher.js';
import { acceptNormalInbound } from '../runtime/persistence/inbound-acceptance.js';
import { acceptScheduledOccurrence } from '../runtime/scheduler/scheduler-queue.js';

const inboundFixture = JSON.parse(fs.readFileSync(
  new URL('../contracts/public/fixtures/inbound-envelope-v1.json', import.meta.url),
  'utf8',
));

const temporaryDirectories = [];

function openDatabase(name = 'c4.db') {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'zylos-observability-'));
  temporaryDirectories.push(directory);
  return {
    database: new Database(path.join(directory, name)),
    databasePath: path.join(directory, name),
  };
}

function deterministicIds(namespace) {
  let sequence = 0;
  return (kind) => `${kind}-${namespace}-${++sequence}`;
}

function instrumentPrepares(database) {
  let prepareCount = 0;
  return {
    exec: database.exec.bind(database),
    pragma: database.pragma.bind(database),
    transaction: database.transaction.bind(database),
    prepare(...args) {
      prepareCount += 1;
      return database.prepare(...args);
    },
    resetPrepareCount() {
      prepareCount = 0;
    },
    get prepareCount() {
      return prepareCount;
    },
  };
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

function createPublisher(database, overrides = {}) {
  return createRuntimeSnapshotPublisher({
    database,
    serviceInstanceId: 'core-service-observability-A',
    hostId: 'core-host-observability-A',
    startedAt: '2026-07-20T07:59:00Z',
    now: () => '2026-07-20T08:00:05Z',
    generateId: deterministicIds('observability'),
    ...overrides,
  });
}

function seedUnknownRuntimeState(database) {
  const unknownError = JSON.stringify({
    code: 'interaction_answer_delivery_unknown',
    category: 'provider',
    retryable: false,
    side_effect_status: 'unknown',
    user_message: 'Provider acknowledgement is unknown.',
    detail_ref: 'diagnostic:handoff-observability-A',
    occurred_at: '2026-07-20T08:00:03Z',
  });
  const runtimeEvidence = JSON.stringify({
    runtime_instance_id: 'provider-runtime-observability-A',
    handle_kind: 'codex_app_server_connection',
    controllable: true,
    process: {
      pid: 4242,
      pgid: 4242,
      started_at: '2026-07-20T07:59:58Z',
      diagnostic_only: true,
    },
  });
  const event = JSON.stringify({
    event_id: 'event-observability-A',
    phase: 'recovering',
    payload: {
      recovery_id: 'recovery-observability-A',
      recovery_of_turn_id: null,
      side_effect_status: 'unknown',
    },
    error: JSON.parse(unknownError),
  });
  const request = JSON.stringify({
    interaction_id: 'interaction-observability-A',
    kind: 'tool_approval',
    state: 'delivery_unknown',
    version: 4,
    handoff_state: 'delivery_unknown',
    authorized_subjects: [
      { type: 'actor', actor_id: 'actor-observability-A' },
      { type: 'capability', capability: 'tool.approve', scope: {} },
    ],
    prompt: 'Do not publish this private prompt or token sk-private-answer-value.',
    created_at: '2026-07-20T08:00:00Z',
    expires_at: '2026-07-20T08:10:00Z',
  });
  const handoff = JSON.stringify({
    handoff_id: 'handoff-observability-A',
    state: 'delivery_unknown',
    handoff_deadline_at: '2026-07-20T08:05:00Z',
    error: JSON.parse(unknownError),
    side_effect_status: 'unknown',
  });

  database.transaction(() => {
    database.prepare(`
      INSERT INTO runtime_conversations (
        conversation_id, conversation_key, region, tenant_id, bot_id,
        chat_type, chat_id, native_thread_or_topic_id, last_queue_sequence, created_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, 1, ?)
    `).run(
      'conversation-observability-A',
      '["cn","tenant-A","bot-A","group","chat-A",null]',
      'cn',
      'tenant-A',
      'bot-A',
      'group',
      'chat-A',
      '2026-07-20T07:59:30Z',
    );
    database.prepare(`
      INSERT INTO runtime_lineages (
        lineage_id, conversation_id, lineage_kind, is_default, created_at,
        provider, provider_native_id, provider_native_id_bound_at, provider_native_state
      ) VALUES (?, ?, 'provider', 1, ?, 'codex', ?, ?, 'valid')
    `).run(
      'lineage-observability-A',
      'conversation-observability-A',
      '2026-07-20T07:59:30Z',
      'native-thread-observability-A',
      '2026-07-20T07:59:59Z',
    );
    database.prepare(`
      INSERT INTO runtime_inbound_events (
        inbound_event_id, idempotency_key, conversation_id, message_id,
        payload_hash, envelope_json, received_at, committed_at
      ) VALUES (?, ?, ?, ?, ?, '{}', ?, ?)
    `).run(
      'inbound-observability-A',
      'zid:v1:inbound:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
      'conversation-observability-A',
      'message-observability-A',
      'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
      '2026-07-20T07:59:30Z',
      '2026-07-20T07:59:30Z',
    );
    database.prepare(`
      INSERT INTO runtime_turns (
        turn_id, conversation_id, lineage_id, inbound_event_id, state, turn_version,
        attempt_id, attempt_no, lease_epoch, queue_sequence, provider_input_json,
        created_at, committed_at
      ) VALUES (?, ?, ?, ?, 'recovering', 19, ?, 1, 14, 1, '{}', ?, ?)
    `).run(
      'turn-observability-A',
      'conversation-observability-A',
      'lineage-observability-A',
      'inbound-observability-A',
      'attempt-observability-A',
      '2026-07-20T07:59:30Z',
      '2026-07-20T08:00:03Z',
    );
    database.prepare(`
      INSERT INTO runtime_turn_queue (
        conversation_id, queue_sequence, turn_id, status, priority,
        wait_reason, wait_detail_json, enqueued_at
      ) VALUES (?, 1, ?, 'claimed', 0, 'interaction_delivery_unknown', NULL, ?)
    `).run(
      'conversation-observability-A',
      'turn-observability-A',
      '2026-07-20T07:59:30Z',
    );
    database.prepare(`
      INSERT INTO runtime_executor_leases (
        conversation_id, lease_owner, lease_epoch, turn_id, attempt_id,
        attempt_no, lease_expires_at, updated_at
      ) VALUES (?, ?, 14, ?, ?, 1, ?, ?)
    `).run(
      'conversation-observability-A',
      'core-service-observability-A',
      'turn-observability-A',
      'attempt-observability-A',
      '2026-07-20T08:00:15Z',
      '2026-07-20T08:00:03Z',
    );
    database.prepare(`
      INSERT INTO runtime_provider_attempts (
        attempt_id, turn_id, conversation_id, attempt_no, lease_epoch, provider,
        service_instance_id, executor_instance_id, state, runtime_instance_id,
        runtime_evidence_json, last_provider_event_at, last_lease_renewed_at,
        side_effect_status, error_json, started_at, updated_at
      ) VALUES (?, ?, ?, 1, 14, 'codex', ?, ?, 'recovering', ?, ?, ?, ?, 'unknown', ?, ?, ?)
    `).run(
      'attempt-observability-A',
      'turn-observability-A',
      'conversation-observability-A',
      'core-service-observability-A',
      'executor-observability-A',
      'provider-runtime-observability-A',
      runtimeEvidence,
      '2026-07-20T08:00:03Z',
      '2026-07-20T08:00:03Z',
      unknownError,
      '2026-07-20T07:59:59Z',
      '2026-07-20T08:00:03Z',
    );
    database.prepare(`
      INSERT INTO runtime_normalized_events (
        event_id, turn_id, event_sequence, turn_version, event_json, persisted_at
      ) VALUES (?, ?, 19, 19, ?, ?)
    `).run(
      'event-observability-A',
      'turn-observability-A',
      event,
      '2026-07-20T08:00:03Z',
    );
    database.prepare(`
      INSERT INTO runtime_interactions (
        interaction_id, conversation_id, turn_id, lineage_id, parent_type,
        parent_id, ordinal, state, version, handoff_state, handoff_version,
        request_json, created_at, updated_at
      ) VALUES (?, ?, ?, ?, 'provider_turn', ?, 1, 'delivery_unknown', 4,
        'delivery_unknown', 3, ?, ?, ?)
    `).run(
      'interaction-observability-A',
      'conversation-observability-A',
      'turn-observability-A',
      'lineage-observability-A',
      'turn-observability-A',
      request,
      '2026-07-20T08:00:00Z',
      '2026-07-20T08:00:03Z',
    );
    database.prepare(`
      INSERT INTO runtime_interaction_handoffs (
        handoff_id, interaction_id, answer_id, state, parent_type,
        provider_attempt_id, handoff_attempt_id, handoff_attempt_no,
        lease_epoch, record_json, created_at, updated_at
      ) VALUES (?, ?, ?, 'delivery_unknown', 'provider_turn', ?, ?, 1, 14, ?, ?, ?)
    `).run(
      'handoff-observability-A',
      'interaction-observability-A',
      'answer-observability-A',
      'attempt-observability-A',
      'handoff-attempt-observability-A',
      handoff,
      '2026-07-20T08:00:01Z',
      '2026-07-20T08:00:03Z',
    );
    database.prepare(`
      INSERT INTO runtime_interaction_answers (
        answer_id, interaction_id, idempotency_key, payload_hash,
        answer_json, result_json, committed_at
      ) VALUES (?, ?, ?, ?, ?, '{}', ?)
    `).run(
      'answer-observability-A',
      'interaction-observability-A',
      'zid:v1:interaction:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc',
      'dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd',
      JSON.stringify({ value: 'sk-private-answer-value' }),
      '2026-07-20T08:00:01Z',
    );
    database.prepare(`
      INSERT INTO runtime_workspace_leases (
        workspace_lease_id, workspace_root, mode, holder_service_instance_id,
        holder_conversation_id, holder_turn_id, lease_epoch, lease_expires_at,
        state, acquired_at, updated_at, released_at
      ) VALUES (?, ?, 'writable', ?, ?, ?, 22, ?, 'active', ?, ?, NULL)
    `).run(
      'workspace-lease-observability-A',
      '/srv/zylos/workspace',
      'core-service-observability-A',
      'conversation-observability-A',
      'turn-observability-A',
      '2026-07-20T08:00:15Z',
      '2026-07-20T07:59:59Z',
      '2026-07-20T08:00:03Z',
    );
    database.prepare(`
      INSERT INTO runtime_workspace_background_work (
        background_work_id, workspace_lease_id, holder_turn_id,
        provider_task_id, state, started_at, ended_at, error_json
      ) VALUES (?, ?, ?, ?, 'active', ?, NULL, NULL)
    `).run(
      'background-work-observability-A',
      'workspace-lease-observability-A',
      'turn-observability-A',
      'provider-task-observability-A',
      '2026-07-20T08:00:00Z',
    );
    database.prepare(`
      INSERT INTO runtime_outbox (
        outbox_id, delivery_id, aggregate_type, aggregate_id, turn_id,
        aggregate_version, status, command_json, attempt_count,
        last_error_json, created_at, updated_at
      ) VALUES (?, ?, 'turn_main', ?, ?, 19, 'retry_wait', ?, 1, ?, ?, ?)
    `).run(
      'outbox-observability-A',
      'delivery-observability-A',
      'turn-observability-A',
      'turn-observability-A',
      JSON.stringify({ target: { channel: 'feishu' } }),
      unknownError,
      '2026-07-20T07:59:57Z',
      '2026-07-20T08:00:03Z',
    );
    database.prepare(`
      INSERT INTO runtime_provider_stop_incidents (
        incident_id, turn_id, attempt_id, attempt_no, lease_epoch,
        provider_stop_status, side_effect_status, disposition,
        error_json, outbox_id, created_at
      ) VALUES (?, ?, ?, 1, 14, 'uncertain', 'unknown',
        'manual_recovery_required', ?, ?, ?)
    `).run(
      'provider-stop-incident-observability-A',
      'turn-observability-A',
      'attempt-observability-A',
      unknownError,
      'outbox-observability-A',
      '2026-07-20T08:00:03Z',
    );
    database.prepare(`
      INSERT INTO runtime_permission_audit (
        audit_id, action, actor_id, source, scope_json, policy_revision,
        reason, redacted_context_json, committed_at
      ) VALUES (?, 'grant', ?, 'platform_original', '{}', 1, 'approved', '{}', ?)
    `).run(
      'audit-observability-A',
      'actor-observability-A',
      '2026-07-20T08:00:02Z',
    );
  }).immediate();
}

afterEach(() => {
  for (const directory of temporaryDirectories.splice(0)) {
    fs.rmSync(directory, { recursive: true, force: true });
  }
});

describe('Core runtime observability snapshot publisher', () => {
  test('publishes through the executor service production surface', async () => {
    const { database } = openDatabase();
    const service = createExecutorService({
      database,
      adapter: { async *execute() {} },
      provider: 'codex',
      serviceInstanceId: 'core-service-production-surface',
      now: () => '2026-07-20T08:00:05Z',
      generateId: deterministicIds('production-surface'),
    });

    const beforeStart = service.publishObservabilitySnapshot();
    expect(validateObservabilitySnapshot(beforeStart).known).toMatchObject({
      core_service_instance_id: 'core-service-production-surface',
      snapshot_version: 1,
      service: {
        host_id: 'core-service-production-surface',
        health: 'offline',
      },
    });

    service.start();
    expect(service.publishObservabilitySnapshot()).toMatchObject({
      snapshot_version: 2,
      service: { health: 'healthy' },
    });
    await service.close();
    expect(service.publishObservabilitySnapshot()).toMatchObject({
      snapshot_version: 3,
      service: { health: 'offline' },
    });
    database.close();
  });

  test('projects durable scheduler queue facts through the provider-neutral snapshot', () => {
    const { database } = openDatabase();
    const accepted = acceptScheduledOccurrence(database, {
      schedule_id: 'schedule-observability',
      task_id: 'task-observability',
      occurrence_id: 'occurrence-observability',
      prompt: 'Run the scheduled observability check.',
      notification_text: 'Scheduled observability check queued.',
      occurred_at: '2026-07-20T07:59:55Z',
      received_at: '2026-07-20T07:59:56Z',
      region: 'global',
      tenant_id: 'tenant-observability',
      bot_id: 'bot-observability',
      bound_conversation: null,
    }, {
      now: () => '2026-07-20T07:59:57Z',
      generateId: deterministicIds('scheduler-observability'),
    });

    const snapshot = createPublisher(database).publish();
    expect(validateObservabilitySnapshot(snapshot).known).toEqual(snapshot);
    expect(snapshot.executors).toMatchObject({ complete: true, items: [] });
    expect(snapshot.turns.items).toContainEqual(expect.objectContaining({
      turn_id: accepted.turn_id,
      conversation_id: accepted.conversation_id,
      state: 'queued',
      phase: 'queued',
    }));
    expect(snapshot.outbox.items).toContainEqual({
      channel: 'scheduler',
      status: 'pending',
      count: 1,
      oldest_age_seconds: 8,
    });
    expect(snapshot.audit_summary.items).toContainEqual({
      category: 'scheduler',
      count: 1,
      last_committed_at: '2026-07-20T07:59:57Z',
    });
    database.close();
  });

  test('publishes an active non-resident Codex executor from the real service path', async () => {
    const { database } = openDatabase();
    const accepted = acceptNormalInbound(database, normalEnvelope('codex-observability'), {
      now: () => '2026-07-20T08:00:00Z',
      generateId: deterministicIds('codex-inbound'),
    });
    let finishProvider;
    const providerFinished = new Promise((resolve) => { finishProvider = resolve; });
    const service = createExecutorService({
      database,
      provider: 'codex',
      serviceInstanceId: 'core-service-codex-observability',
      now: () => '2026-07-20T08:00:05Z',
      generateId: deterministicIds('codex-observability'),
      adapter: {
        async *execute(context) {
          context.reportRuntimeEvidence({
            runtime_instance_id: 'codex-runtime-observability',
            handle_kind: 'codex_app_server_connection',
            controllable: true,
          });
          context.reportProviderState({
            state: 'started',
            provider_native_id: null,
          });
          await providerFinished;
        },
      },
    });

    const running = service.runNext();
    await new Promise((resolve) => setImmediate(resolve));
    try {
      const snapshot = service.publishObservabilitySnapshot();
      expect(snapshot.executors).toMatchObject({
        complete: true,
        items: [expect.objectContaining({
          conversation_id: accepted.conversation_id,
          provider: 'codex',
          provider_native_id: null,
          resident: false,
          evictable: false,
          active_turn_id: accepted.turn_id,
          health: 'healthy',
        })],
      });
    } finally {
      finishProvider();
      await running;
    }
    await service.close();
    database.close();
  });

  test('degrades health for an expired outbox claim fenced by an unknown side effect', () => {
    const { database } = openDatabase();
    acceptNormalInbound(database, normalEnvelope('outbox-side-effect-unknown'), {
      now: () => '2026-07-20T07:59:50Z',
      generateId: deterministicIds('outbox-side-effect-unknown'),
    });
    let ownerTime = '2026-07-20T07:59:55Z';
    const owner = createOutboxService({
      database,
      serviceInstanceId: 'outbox-side-effect-owner',
      now: () => ownerTime,
      generateId: deterministicIds('outbox-side-effect-owner'),
      leaseDurationMs: 10_000,
    });
    const command = owner.claimNext();
    ownerTime = '2026-07-20T07:59:56Z';
    owner.assertCurrentClaim(command);

    const snapshot = createPublisher(database, {
      now: () => '2026-07-20T03:00:07-05:00',
    }).publish();
    expect(snapshot.service.health).toBe('degraded');
    expect(snapshot.outbox.items).toContainEqual({
      channel: command.target.channel,
      status: 'delivery_unknown',
      count: 1,
      oldest_age_seconds: 17,
    });
    database.close();
  });

  test('publishes complete full replacements with durable monotonic versions across reopen', () => {
    const { database, databasePath } = openDatabase();
    const firstPublisher = createPublisher(database);

    const first = firstPublisher.publish();
    const second = firstPublisher.publish();
    expect(validateObservabilitySnapshot(first).known).toMatchObject({
      core_service_instance_id: 'core-service-observability-A',
      snapshot_version: 1,
      service: { complete: true, health: 'healthy' },
      executors: { complete: true, items: [] },
      turns: { complete: true, items: [] },
      interactions: { complete: true, items: [] },
      workspace_leases: { complete: true, items: [] },
      outbox: { complete: true, items: [], retry_count: 0, dead_letter_count: 0 },
      audit_summary: { complete: true, items: [] },
      error: null,
    });
    expect(second.snapshot_version).toBe(2);
    expect(resolveObservabilitySnapshotUpdate(first, second)).toEqual({
      status: 'replace',
      apply: true,
    });
    expect(resolveObservabilitySnapshotUpdate(second, first)).toEqual({
      status: 'obsolete',
      apply: false,
    });
    expect(resolveObservabilitySnapshotUpdate(first, first)).toEqual({
      status: 'duplicate',
      apply: false,
    });
    database.close();

    const reopened = new Database(databasePath);
    const reopenedPublisher = createPublisher(reopened, {
      generateId: deterministicIds('observability-reopened'),
    });
    const third = reopenedPublisher.publish();
    expect(third.snapshot_version).toBe(3);

    const replacementPublisher = createPublisher(reopened, {
      serviceInstanceId: 'core-service-observability-B',
      startedAt: '2026-07-20T08:00:04Z',
      generateId: deterministicIds('observability-replacement'),
    });
    const replacement = replacementPublisher.publish();
    expect(replacement.snapshot_version).toBe(1);
    expect(resolveObservabilitySnapshotUpdate(third, replacement)).toEqual({
      status: 'replace_instance',
      apply: true,
    });
    reopened.close();
  });

  test('rejects non-RFC timestamps before persistence and rejects poisoned registrations', () => {
    const { database, databasePath } = openDatabase();
    createPublisher(database);
    expect(() => createPublisher(database, {
      serviceInstanceId: 'core-service-invalid-time',
      startedAt: 'July 20, 2026',
    })).toThrow(/RFC 3339/);
    expect(database.prepare(`
      SELECT 1 FROM runtime_observability_instances
      WHERE service_instance_id = 'core-service-invalid-time'
    `).get()).toBeUndefined();

    database.prepare(`
      UPDATE runtime_observability_instances SET started_at = 'July 20, 2026'
      WHERE service_instance_id = 'core-service-observability-A'
    `).run();
    database.close();

    const reopened = new Database(databasePath);
    expect(() => createPublisher(reopened)).toThrow(/RFC 3339/);
    reopened.prepare(`
      UPDATE runtime_observability_instances SET started_at = '2026-07-20T07:59:00Z'
      WHERE service_instance_id = 'core-service-observability-A'
    `).run();
    expect(createPublisher(reopened).publish().snapshot_version).toBe(1);
    reopened.close();
  });

  test('rejects a stale read instead of assigning it a newer replace version', () => {
    const { database, databasePath } = openDatabase();
    const writer = new Database(databasePath);
    let writeCommitted = false;
    const concurrentPublisher = createPublisher(writer, {
      generateId: deterministicIds('concurrent-publisher'),
    });
    let concurrentSnapshot;
    const publisher = createPublisher(database, {
      getServiceState() {
        if (!writeCommitted) {
          acceptNormalInbound(writer, normalEnvelope('concurrent-writer'), {
            now: () => '2026-07-20T08:00:04Z',
            generateId: deterministicIds('concurrent-writer'),
          });
          writeCommitted = true;
          concurrentSnapshot = concurrentPublisher.publish();
        }
        return {};
      },
    });

    expect(() => publisher.publish()).toThrow(/database is locked/i);
    expect(writeCommitted).toBe(true);
    expect(concurrentSnapshot).toMatchObject({
      snapshot_version: 1,
      turns: { items: [expect.any(Object)] },
      outbox: { items: [expect.any(Object)] },
    });

    const after = publisher.publish();
    expect(after.snapshot_version).toBe(2);
    expect(after.turns.items).toHaveLength(1);
    expect(after.outbox.items).toHaveLength(1);
    writer.close();
    database.close();
  });

  test('uses a fixed batch-query count as durable history grows', () => {
    const { database } = openDatabase();
    const instrumented = instrumentPrepares(database);
    const publisher = createPublisher(instrumented);

    instrumented.resetPrepareCount();
    publisher.publish();
    const emptyPrepareCount = instrumented.prepareCount;

    seedUnknownRuntimeState(database);
    instrumented.resetPrepareCount();
    publisher.publish();
    expect(instrumented.prepareCount).toBe(emptyPrepareCount);
    database.close();
  });

  test('projects every durable aggregate, unknown states, and diagnostic-only identities', () => {
    const { database } = openDatabase();
    const publisher = createPublisher(database);
    seedUnknownRuntimeState(database);

    const snapshot = publisher.publish();
    expect(validateObservabilitySnapshot(snapshot).known).toEqual(snapshot);
    expect(snapshot.service.health).toBe('degraded');
    expect(snapshot.executors.items).toEqual([
      expect.objectContaining({
        conversation_id: 'conversation-observability-A',
        provider: 'codex',
        provider_native_id: 'native-thread-observability-A',
        health: 'degraded',
        resident: false,
        evictable: false,
        active_turn_id: 'turn-observability-A',
        wait_reason: 'interaction_delivery_unknown',
        runtime_identity: {
          diagnostic_only: true,
          pid: 4242,
          pgid: 4242,
          process_start_time: '2026-07-20T07:59:58Z',
        },
      }),
    ]);
    expect(snapshot.turns.items).toEqual([
      expect.objectContaining({
        turn_id: 'turn-observability-A',
        state: 'recovering',
        side_effect_status: 'unknown',
        error: expect.objectContaining({ code: 'interaction_answer_delivery_unknown' }),
      }),
    ]);
    expect(snapshot.interactions.items).toEqual([
      expect.objectContaining({
        interaction_id: 'interaction-observability-A',
        state: 'delivery_unknown',
        handoff_state: 'delivery_unknown',
        authorized_subject_summary: { actor_count: 1, capability_count: 1 },
      }),
    ]);
    expect(snapshot.workspace_leases.items).toEqual([
      expect.objectContaining({
        holder_background_work_id: 'background-work-observability-A',
        waiter_count: 0,
      }),
    ]);
    expect(snapshot.outbox).toMatchObject({
      complete: true,
      items: [{ channel: 'feishu', status: 'retry_wait', count: 1, oldest_age_seconds: 8 }],
      retry_count: 1,
      dead_letter_count: 0,
    });
    expect(snapshot.audit_summary.items).toContainEqual({
      category: 'permission',
      count: 1,
      last_committed_at: '2026-07-20T08:00:02Z',
    });
    expect(snapshot.audit_summary.items).toContainEqual({
      category: 'provider_stop',
      count: 1,
      last_committed_at: '2026-07-20T08:00:03Z',
    });
    expect(JSON.stringify(snapshot)).not.toContain('sk-private-answer-value');

    database.prepare(`
      UPDATE runtime_lineages SET provider_native_id = 'native-thread-different'
      WHERE lineage_id = 'lineage-observability-A'
    `).run();
    database.prepare(`
      UPDATE runtime_provider_attempts SET runtime_evidence_json = ?
      WHERE attempt_id = 'attempt-observability-A'
    `).run(JSON.stringify({
      runtime_instance_id: 'provider-runtime-different',
      handle_kind: 'codex_app_server_connection',
      controllable: false,
      process: {
        pid: 9999,
        pgid: null,
        started_at: '2026-07-20T08:00:04Z',
        diagnostic_only: true,
      },
    }));
    const diagnosticChanged = publisher.publish();
    expect(diagnosticChanged.executors.items[0]).toMatchObject({
      provider_native_id: 'native-thread-different',
      health: 'degraded',
      runtime_identity: { diagnostic_only: true, pid: 9999 },
    });

    database.prepare(`
      UPDATE runtime_turns SET state = 'running'
      WHERE turn_id = 'turn-observability-A'
    `).run();
    database.prepare(`
      UPDATE runtime_interactions SET state = 'cancelled', handoff_state = 'cancelled'
      WHERE interaction_id = 'interaction-observability-A'
    `).run();
    database.prepare(`
      UPDATE runtime_interaction_handoffs SET state = 'cancelled'
      WHERE handoff_id = 'handoff-observability-A'
    `).run();
    database.prepare(`
      UPDATE runtime_provider_attempts
      SET state = 'running', side_effect_status = 'none', error_json = NULL
      WHERE attempt_id = 'attempt-observability-A'
    `).run();
    database.prepare(`
      UPDATE runtime_normalized_events SET event_json = ?
      WHERE event_id = 'event-observability-A'
    `).run(JSON.stringify({
      event_id: 'event-observability-A',
      phase: 'running',
      payload: {},
      error: null,
    }));
    const identityOnly = publisher.publish();
    expect(identityOnly.executors.items[0]).toMatchObject({
      provider_native_id: 'native-thread-different',
      health: 'unknown',
      runtime_identity: { diagnostic_only: true, pid: 9999 },
    });

    database.prepare(`
      UPDATE runtime_provider_attempts SET runtime_evidence_json = ?
      WHERE attempt_id = 'attempt-observability-A'
    `).run(JSON.stringify({
      runtime_instance_id: 'provider-runtime-different',
      handle_kind: 'codex_app_server_connection',
      controllable: true,
      process: {
        pid: 9999,
        pgid: null,
        started_at: '2026-07-20T08:00:04Z',
        diagnostic_only: true,
      },
    }));
    const jointlyHealthy = publisher.publish();
    expect(jointlyHealthy.executors.items[0].health).toBe('healthy');

    database.prepare(`
      UPDATE runtime_executor_leases SET lease_expires_at = '2026-07-20T09:00:00+02:00'
      WHERE conversation_id = 'conversation-observability-A'
    `).run();
    const offsetExpired = publisher.publish();
    expect(offsetExpired.executors.items[0].health).toBe('unknown');
    database.close();
  });

  test('keeps required/null/major/minor compatibility at the public producer seam', () => {
    const { database } = openDatabase();
    const publisher = createPublisher(database);
    seedUnknownRuntimeState(database);
    const snapshot = publisher.publish();

    const missing = structuredClone(snapshot);
    delete missing.audit_summary;
    expect(() => validateObservabilitySnapshot(missing)).toThrow();

    const nullable = structuredClone(snapshot);
    nullable.turns.items[0].lineage_id = null;
    nullable.executors.items[0].provider_native_id = null;
    expect(validateObservabilitySnapshot(nullable).known).toMatchObject({
      turns: { items: [expect.objectContaining({ lineage_id: null })] },
      executors: { items: [expect.objectContaining({ provider_native_id: null })] },
    });

    const unsupported = structuredClone(snapshot);
    unsupported.contract_version = '2.0';
    expect(() => validateObservabilitySnapshot(unsupported)).toThrow();

    const additive = structuredClone(snapshot);
    additive.contract_version = '1.1';
    additive.presentation_hint = 'compact';
    const validated = validateObservabilitySnapshot(additive);
    expect(validated.extensions).toEqual({ presentation_hint: 'compact' });
    expect(validated.forwarded.presentation_hint).toBe('compact');
    database.close();
  });

  test('fails closed with one unified error when a collection is partial', () => {
    const { database } = openDatabase();
    const publisher = createPublisher(database);
    database.exec('ALTER TABLE runtime_outbox RENAME TO runtime_outbox_unavailable');

    const snapshot = publisher.publish();
    expect(snapshot.service.health).toBe('degraded');
    expect(snapshot.outbox).toMatchObject({
      complete: false,
      items: [],
      retry_count: 0,
      dead_letter_count: 0,
      error: expect.objectContaining({ code: 'observability_degraded' }),
    });
    expect(snapshot.outbox.error).toBe(snapshot.error);
    expect(snapshot.turns).toMatchObject({ complete: true, items: [], error: null });
    expect(validateObservabilitySnapshot(snapshot).known.error.code).toBe('observability_degraded');
    database.close();
  });

  test('marks an unsafe durable detail incomplete instead of leaking or presenting idle', () => {
    const { database } = openDatabase();
    const publisher = createPublisher(database);
    seedUnknownRuntimeState(database);
    const unsafeError = {
      code: 'provider_context_invalid',
      category: 'provider',
      retryable: false,
      side_effect_status: 'unknown',
      user_message: 'Leaked bearer secret should be blocked.',
      authorization: 'Bearer private-observability-secret',
      occurred_at: '2026-07-20T08:00:04Z',
    };
    database.prepare(`
      UPDATE runtime_provider_attempts SET error_json = ?
      WHERE attempt_id = 'attempt-observability-A'
    `).run(JSON.stringify(unsafeError));
    database.prepare(`
      UPDATE runtime_normalized_events SET event_json = ?
      WHERE event_id = 'event-observability-A'
    `).run(JSON.stringify({
      event_id: 'event-observability-A',
      phase: 'recovering',
      payload: { side_effect_status: 'unknown' },
      error: unsafeError,
    }));

    const snapshot = publisher.publish();
    expect(snapshot.turns).toMatchObject({
      complete: false,
      items: [],
      error: expect.objectContaining({ code: 'observability_degraded' }),
    });
    expect(snapshot.error).toBe(snapshot.turns.error);
    expect(snapshot.service.health).toBe('degraded');
    expect(JSON.stringify(snapshot)).not.toContain('private-observability-secret');
    database.close();
  });
});
