import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { afterEach, describe, expect, test } from '@jest/globals';
import Database from '../skills/comm-bridge/node_modules/better-sqlite3/lib/index.js';

import {
  resolveObservabilitySnapshotUpdate,
  validateObservabilitySnapshot,
} from '../contracts/public/index.js';
import { createExecutorService } from '../runtime/executor/service.js';
import { createRuntimeSnapshotPublisher } from '../runtime/observability/snapshot-publisher.js';

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
      INSERT INTO runtime_executor_residents (
        conversation_id, bot_id, provider, owner_service_instance_id,
        owner_epoch, owner_expires_at, admitted_at, last_used_at
      ) VALUES (?, 'bot-A', 'codex', ?, 9, ?, ?, ?)
    `).run(
      'conversation-observability-A',
      'core-service-observability-A',
      '2026-07-20T08:00:15Z',
      '2026-07-20T07:59:30Z',
      '2026-07-20T08:00:03Z',
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

    const snapshot = service.publishObservabilitySnapshot();
    expect(validateObservabilitySnapshot(snapshot).known).toMatchObject({
      core_service_instance_id: 'core-service-production-surface',
      snapshot_version: 1,
      service: {
        host_id: 'core-service-production-surface',
        health: 'healthy',
      },
    });

    await service.close();
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
