import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { afterEach, describe, expect, test } from '@jest/globals';
import Database from '../skills/comm-bridge/node_modules/better-sqlite3/lib/index.js';

import { createIdempotencyKey } from '../contracts/public/index.js';
import { createOutboxService } from '../runtime/delivery/outbox-service.js';
import { createExecutorStore } from '../runtime/persistence/executor-store.js';
import { acceptQueuedInbound as acceptNormalInbound } from '../runtime/persistence/inbound-acceptance.js';
import { deliveredResult } from './helpers/delivered-result.js';

const inboundFixture = JSON.parse(fs.readFileSync(
  new URL('../contracts/public/fixtures/inbound-envelope-v1.json', import.meta.url),
  'utf8',
));
const temporaryDirectories = [];

function deterministicIds(namespace) {
  const counts = new Map();
  return (kind) => {
    const next = (counts.get(kind) ?? 0) + 1;
    counts.set(kind, next);
    return `${kind}-${namespace}-${next}`;
  };
}

function openTestDatabase() {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'zylos-retention-cleanup-'));
  temporaryDirectories.push(directory);
  const databasePath = path.join(directory, 'c4.db');
  return { database: new Database(databasePath), databasePath };
}

function canonicalJson(value) {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(',')}]`;
  if (value !== null && typeof value === 'object') {
    return `{${Object.keys(value).sort().map((key) => (
      `${JSON.stringify(key)}:${canonicalJson(value[key])}`
    )).join(',')}}`;
  }
  return JSON.stringify(value);
}

function envelope(suffix, occurredAt) {
  const fixture = inboundFixture.valid.find(
    ({ name }) => name === 'authenticated_dm_with_attachment',
  ).document;
  const document = structuredClone(fixture);
  document.inbound_event_id = `evt-retention-${suffix}`;
  document.trace_id = `trace-retention-${suffix}`;
  document.message_id = `message-retention-${suffix}`;
  document.occurred_at = occurredAt;
  document.received_at = occurredAt;
  document.idempotency_key = createIdempotencyKey('inbound', {
    region: document.region,
    tenant_id: document.tenant_id,
    channel: document.channel,
    bot_id: document.bot_id,
    inbound_event_id: document.inbound_event_id,
  });
  return document;
}

function createTerminalTurn(database, suffix, anchor, { deliverOutbox = true } = {}) {
  const accepted = acceptNormalInbound(database, envelope(suffix, anchor), {
    now: () => anchor,
    generateId: deterministicIds(`inbound-${suffix}`),
  });
  const store = createExecutorStore({
    database,
    provider: 'claude',
    serviceInstanceId: `executor-retention-${suffix}`,
    now: () => anchor,
    generateId: deterministicIds(`executor-${suffix}`),
  });
  const context = store.claimNextQueuedTurn();
  store.recordProviderRuntimeEvidence(context, {
    runtime_instance_id: `runtime-retention-${suffix}`,
    handle_kind: 'claude_sdk_query',
    controllable: true,
  });
  store.recordProviderEventDiagnostic(context, {
    type: 'provider_raw_event',
    provider_event_id: `provider-event-retention-${suffix}`,
  }, { reasonCode: 'retention_fixture' });
  store.transitionTurn(context, 'starting', 'running');
  store.appendAdapterEvent(context, {
    kind: 'text_snapshot',
    payload: { text: `final output ${suffix}`, end_offset: `final output ${suffix}`.length },
    provider_native_id: null,
  });
  store.transitionTurn(context, 'running', 'completed');
  if (deliverOutbox) {
    const outbox = createOutboxService({
      database,
      serviceInstanceId: `delivery-retention-${suffix}`,
      now: () => anchor,
      generateId: deterministicIds(`delivery-${suffix}`),
      throttleMs: 0,
    });
    for (;;) {
      const command = outbox.claimNext();
      if (command === null) break;
      outbox.recordResult(deliveredResult(command, anchor));
    }
  }
  return { accepted, context, store };
}

function createUnfinishedTurn(database, suffix, anchor) {
  const document = envelope(suffix, anchor);
  document.chat_id = `chat-retention-${suffix}`;
  const accepted = acceptNormalInbound(database, document, {
    now: () => anchor,
    generateId: deterministicIds(`inbound-${suffix}`),
  });
  const store = createExecutorStore({
    database,
    provider: 'claude',
    serviceInstanceId: `executor-retention-${suffix}`,
    now: () => anchor,
    generateId: deterministicIds(`executor-${suffix}`),
  });
  const context = store.claimNextQueuedTurn();
  store.transitionTurn(context, 'starting', 'running');
  const outbox = createOutboxService({
    database,
    serviceInstanceId: `delivery-retention-${suffix}`,
    now: () => anchor,
    generateId: deterministicIds(`delivery-${suffix}`),
    throttleMs: 0,
  });
  for (;;) {
    const command = outbox.claimNext();
    if (command === null) break;
    outbox.recordResult(deliveredResult(command, anchor));
  }
  return { accepted, context, store };
}

function countEvents(database, turnId) {
  return database.prepare(`
    SELECT COUNT(*) AS count FROM runtime_normalized_events WHERE turn_id = ?
  `).get(turnId).count;
}

afterEach(() => {
  for (const directory of temporaryDirectories.splice(0)) {
    fs.rmSync(directory, { recursive: true, force: true });
  }
});

describe('runtime retention cleanup', () => {
  test('deletes raw event detail exactly at its persisted 7x24h expiry boundary', () => {
    const { database } = openTestDatabase();
    const before = createTerminalTurn(database, 'before', '2026-07-10T11:59:59.999Z');
    const equal = createTerminalTurn(database, 'equal', '2026-07-10T12:00:00.000Z');
    const after = createTerminalTurn(database, 'after', '2026-07-10T12:00:00.001Z');
    const cleanup = createExecutorStore({
      database,
      provider: 'claude',
      serviceInstanceId: 'executor-retention-sweep',
      now: () => '2026-07-17T12:00:00.000Z',
      generateId: deterministicIds('retention-sweep'),
    });

    const dryRun = cleanup.runRetentionCleanup({ dryRun: true, maxLagMs: 60_000 });
    expect(dryRun.candidates.filter(({ record_kind: kind }) => kind === 'normalized_event'))
      .toHaveLength(countEvents(database, before.accepted.turn_id)
        + countEvents(database, equal.accepted.turn_id));

    const result = cleanup.runRetentionCleanup({ dryRun: false, maxLagMs: 60_000 });
    expect(result.candidates).toEqual(dryRun.candidates);
    expect(result.deleted_by_kind.normalized_event).toBe(
      dryRun.candidates.filter(({ record_kind: kind }) => kind === 'normalized_event').length,
    );
    expect(countEvents(database, before.accepted.turn_id)).toBe(0);
    expect(countEvents(database, equal.accepted.turn_id)).toBe(0);
    expect(countEvents(database, after.accepted.turn_id)).toBeGreaterThan(0);
    database.close();
  });

  test('keeps raw expiry fixed at 7x24h while terminal detail uses terminal_at plus 30x24h', () => {
    const { database } = openTestDatabase();
    const terminal = createTerminalTurn(database, 'classification-overlap', '2026-07-31T23:59:59.500Z');
    const retention = database.prepare(`
      SELECT record_kind, retention_class, anchor_at, expires_at, disposal_kind
      FROM runtime_retention_entries
      WHERE turn_id = ?
      ORDER BY record_kind, record_id
    `).all(terminal.accepted.turn_id);

    expect(retention.filter(({ record_kind: kind }) => kind === 'normalized_event'))
      .toEqual(expect.arrayContaining([expect.objectContaining({
        retention_class: 'raw_detail_7d',
        anchor_at: '2026-07-31T23:59:59.500Z',
        expires_at: '2026-08-07T23:59:59.500Z',
        disposal_kind: 'delete',
      })]));
    expect(retention).toEqual(expect.arrayContaining([
      expect.objectContaining({
        record_kind: 'intermediate_projection',
        retention_class: 'raw_detail_7d',
        expires_at: '2026-08-07T23:59:59.500Z',
      }),
      expect.objectContaining({
        record_kind: 'provider_raw_event',
        retention_class: 'raw_detail_7d',
        anchor_at: '2026-07-31T23:59:59.500Z',
        expires_at: '2026-08-07T23:59:59.500Z',
      }),
      expect.objectContaining({
        record_kind: 'terminal_projection',
        retention_class: 'terminal_detail_30d',
        anchor_at: '2026-07-31T23:59:59.500Z',
        expires_at: '2026-08-30T23:59:59.500Z',
      }),
      expect.objectContaining({
        record_kind: 'provider_attempt_detail',
        retention_class: 'terminal_detail_30d',
        expires_at: '2026-08-30T23:59:59.500Z',
        disposal_kind: 'redact',
      }),
      expect.objectContaining({
        record_kind: 'terminal_outbox',
        retention_class: 'terminal_detail_30d',
        expires_at: '2026-08-30T23:59:59.500Z',
      }),
    ]));

    const summary = database.prepare(`
      SELECT terminal_state, terminal_at, final_text, lineage_id, mapping_id
      FROM runtime_compact_turn_summaries WHERE turn_id = ?
    `).get(terminal.accepted.turn_id);
    expect(summary).toMatchObject({
      terminal_state: 'completed',
      terminal_at: '2026-07-31T23:59:59.500Z',
      final_text: 'final output classification-overlap',
      lineage_id: terminal.accepted.lineage_id,
    });
    expect(summary.mapping_id).toEqual(expect.any(String));
    const immutableEntry = database.prepare(`
      SELECT record_kind, record_id FROM runtime_retention_entries
      WHERE turn_id = ? ORDER BY record_kind, record_id LIMIT 1
    `).get(terminal.accepted.turn_id);
    expect(() => database.prepare(`
      UPDATE runtime_retention_entries SET expires_at = '2099-01-01T00:00:00.000Z'
      WHERE record_kind = ? AND record_id = ?
    `).run(immutableEntry.record_kind, immutableEntry.record_id)).toThrow(/immutable/);
    database.close();
  });

  test('compacts terminal detail atomically without breaking durable facts or foreign keys', () => {
    const { database } = openTestDatabase();
    const terminal = createTerminalTurn(database, 'terminal-detail', '2026-07-01T04:05:06.789Z');
    database.prepare(`
      INSERT INTO runtime_workspace_leases (
        workspace_lease_id, workspace_root, mode, holder_service_instance_id,
        holder_conversation_id, holder_turn_id, lease_epoch, lease_expires_at,
        state, acquired_at, updated_at, released_at
      ) VALUES (?, ?, 'writable', ?, ?, ?, 77, ?, 'released', ?, ?, ?)
    `).run(
      'workspace-lease-terminal-detail',
      '/tmp/zylos-retention-terminal-detail',
      'executor-terminal-detail',
      terminal.accepted.conversation_id,
      terminal.accepted.turn_id,
      '2026-07-01T04:05:16.789Z',
      '2026-07-01T04:05:06.789Z',
      '2026-07-01T04:05:06.789Z',
      '2026-07-01T04:05:06.789Z',
    );
    database.prepare(`
      INSERT INTO runtime_workspace_background_work (
        background_work_id, workspace_lease_id, holder_turn_id,
        provider_task_id, state, started_at, ended_at, error_json
      ) VALUES (?, ?, ?, ?, 'completed', ?, ?, NULL)
    `).run(
      'background-work-terminal-detail',
      'workspace-lease-terminal-detail',
      terminal.accepted.turn_id,
      'provider-task-terminal-detail',
      '2026-07-01T04:05:06.789Z',
      '2026-07-01T04:05:06.789Z',
    );
    expect(database.prepare(`
      SELECT record_kind, retention_class, expires_at
      FROM runtime_retention_entries
      WHERE record_id IN ('background-work-terminal-detail', 'workspace-lease-terminal-detail')
      ORDER BY record_kind
    `).all()).toEqual([
      {
        record_kind: 'terminal_background_work',
        retention_class: 'terminal_detail_30d',
        expires_at: '2026-07-31T04:05:06.789Z',
      },
      {
        record_kind: 'terminal_workspace_lease',
        retention_class: 'terminal_detail_30d',
        expires_at: '2026-07-31T04:05:06.789Z',
      },
    ]);
    const cleanup = createExecutorStore({
      database,
      provider: 'claude',
      serviceInstanceId: 'executor-retention-terminal-detail-sweep',
      now: () => '2026-07-31T04:05:06.789Z',
      generateId: deterministicIds('terminal-detail-sweep'),
    });

    const result = cleanup.runRetentionCleanup({ dryRun: false, maxLagMs: 60_000 });
    expect(result.deleted_by_kind).toMatchObject({
      normalized_event: expect.any(Number),
      intermediate_projection: expect.any(Number),
      terminal_projection: 1,
      provider_attempt_detail: 1,
      terminal_outbox: expect.any(Number),
      terminal_turn_queue: 1,
      terminal_background_work: 1,
      terminal_workspace_lease: 1,
    });
    expect(database.prepare(`
      SELECT COUNT(*) AS count FROM runtime_projection_snapshots WHERE turn_id = ?
    `).get(terminal.accepted.turn_id)).toEqual({ count: 0 });
    expect(database.prepare(`
      SELECT COUNT(*) AS count FROM runtime_outbox WHERE turn_id = ?
    `).get(terminal.accepted.turn_id)).toEqual({ count: 0 });
    expect(database.prepare(`
      SELECT COUNT(*) AS count FROM runtime_turn_queue WHERE turn_id = ?
    `).get(terminal.accepted.turn_id)).toEqual({ count: 0 });
    expect(database.prepare(`
      SELECT COUNT(*) AS count FROM runtime_workspace_background_work WHERE holder_turn_id = ?
    `).get(terminal.accepted.turn_id)).toEqual({ count: 0 });
    expect(database.prepare(`
      SELECT COUNT(*) AS count FROM runtime_workspace_leases WHERE holder_turn_id = ?
    `).get(terminal.accepted.turn_id)).toEqual({ count: 0 });
    expect(database.prepare(`
      SELECT state, runtime_instance_id, runtime_evidence_json, error_json
      FROM runtime_provider_attempts WHERE turn_id = ?
    `).get(terminal.accepted.turn_id)).toEqual({
      state: 'completed',
      runtime_instance_id: null,
      runtime_evidence_json: null,
      error_json: null,
    });

    expect(database.prepare(`
      SELECT final_text FROM runtime_compact_turn_summaries WHERE turn_id = ?
    `).get(terminal.accepted.turn_id)).toEqual({ final_text: 'final output terminal-detail' });
    expect(database.prepare(`
      SELECT COUNT(*) AS count FROM runtime_lineages WHERE lineage_id = ?
    `).get(terminal.accepted.lineage_id)).toEqual({ count: 1 });
    expect(database.prepare(`
      SELECT COUNT(*) AS count FROM runtime_message_mappings WHERE turn_id = ?
    `).get(terminal.accepted.turn_id).count).toBeGreaterThan(0);
    expect(database.prepare(`
      SELECT COUNT(*) AS count FROM runtime_inbound_events WHERE inbound_event_id = ?
    `).get(`evt-retention-terminal-detail`)).toEqual({ count: 1 });
    expect(database.pragma('foreign_key_check')).toEqual([]);
    expect(database.prepare(`
      SELECT COUNT(*) AS count FROM runtime_retention_deletion_audit WHERE sweep_id = ?
    `).get(result.sweep_id)).toEqual({ count: result.disposed_count });

    const replay = cleanup.runRetentionCleanup({ dryRun: false, maxLagMs: 60_000 });
    expect(replay).toMatchObject({ candidate_count: 0, disposed_count: 0 });
    database.close();
  });

  test('deletes security and operations audit at committed_at plus 180x24h with immutable deletion audit', () => {
    const { database } = openTestDatabase();
    const terminal = createTerminalTurn(database, 'security-audit', '2026-01-01T00:00:00.000Z');
    database.prepare(`
      INSERT INTO runtime_permission_audit (
        audit_id, action, actor_id, source, scope_json, policy_revision,
        reason, redacted_context_json, turn_id, grant_id, control_id,
        action_ref, committed_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, ?)
    `).run(
      'permission-audit-retention',
      'permission_rejected',
      'actor-retention',
      'platform_original',
      '{}',
      1,
      'retention fixture',
      '{}',
      '2026-01-01T00:00:00.000Z',
    );
    database.prepare(`
      INSERT INTO runtime_operations_audit (
        audit_id, caller_namespace, control_id, action, outcome,
        subject_type, subject_id, capability, grant_id, policy_id,
        policy_version, target_json, expected_version_json,
        previous_target_version, target_version, reason, error_json, committed_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, ?, NULL, ?)
    `).run(
      'operations-audit-retention',
      'retention.fixture',
      'control-retention',
      'inspect',
      'completed',
      'service',
      'subject-retention',
      'runtime.inspect',
      'grant-retention',
      'policy-retention',
      1,
      '{"aggregate_type":"service","service_instance_id":"service-retention"}',
      'retention fixture',
      '2026-01-01T00:00:00.000Z',
    );
    database.prepare(`
      INSERT INTO runtime_permission_action_decisions (
        decision_id, turn_id, action_ref, action_kind, outcome, basis_kind,
        grant_id, checked_policy_revision, checked_at
      ) VALUES (?, ?, ?, ?, 'requires_approval', 'default_safe', NULL, 1, ?)
    `).run(
      'permission-decision-retention',
      terminal.accepted.turn_id,
      'action-ref-retention',
      'workspace_write',
      '2026-01-01T00:00:00.000Z',
    );
    database.prepare(`
      INSERT INTO runtime_interactions (
        interaction_id, conversation_id, turn_id, lineage_id, parent_type,
        parent_id, ordinal, state, version, handoff_state, handoff_version,
        request_json, created_at, updated_at
      ) VALUES (?, ?, ?, ?, 'provider_turn', ?, 1, 'answered', 3,
        'accepted', 2, '{}', ?, ?)
    `).run(
      'interaction-audit-retention-parent',
      terminal.accepted.conversation_id,
      terminal.accepted.turn_id,
      terminal.accepted.lineage_id,
      terminal.accepted.turn_id,
      '2026-01-01T00:00:00.000Z',
      '2026-01-01T00:00:00.000Z',
    );
    database.prepare(`
      INSERT INTO runtime_interaction_handoffs (
        handoff_id, interaction_id, answer_id, state, parent_type,
        provider_attempt_id, handoff_attempt_id, handoff_attempt_no,
        lease_epoch, record_json, created_at, updated_at
      ) VALUES (?, ?, ?, 'accepted', 'provider_turn', ?, ?, 1, 1, '{}', ?, ?)
    `).run(
      'handoff-audit-retention-parent',
      'interaction-audit-retention-parent',
      'answer-audit-retention-parent',
      terminal.context.attempt.attempt_id,
      'handoff-attempt-audit-retention',
      '2026-01-01T00:00:00.000Z',
      '2026-01-01T00:00:00.000Z',
    );
    database.prepare(`
      INSERT INTO runtime_interaction_audit (
        audit_id, interaction_id, handoff_id, outcome, provider_attempt_id,
        lease_epoch, acknowledgement_json, created_at
      ) VALUES (?, ?, ?, 'accepted', ?, 1, '{}', ?)
    `).run(
      'interaction-audit-retention',
      'interaction-audit-retention-parent',
      'handoff-audit-retention-parent',
      terminal.context.attempt.attempt_id,
      '2026-01-01T00:00:00.000Z',
    );
    database.prepare(`
      INSERT INTO runtime_operations_idempotency_conflicts (
        caller_namespace, control_id, request_hash, result_json, audit_id, committed_at
      ) VALUES (?, ?, ?, '{}', ?, ?)
    `).run(
      'retention.fixture',
      'control-retention-conflict',
      'request-hash-retention',
      'operations-audit-retention',
      '2026-01-01T00:00:00.000Z',
    );

    expect(database.prepare(`
      SELECT record_kind, retention_class, anchor_at, expires_at
      FROM runtime_retention_entries
      WHERE record_kind IN (
        'permission_audit', 'operations_audit', 'permission_action_decision',
        'interaction_audit', 'operations_idempotency_conflict'
      )
      ORDER BY record_kind
    `).all()).toEqual([
      {
        record_kind: 'interaction_audit',
        retention_class: 'security_audit_180d',
        anchor_at: '2026-01-01T00:00:00.000Z',
        expires_at: '2026-06-30T00:00:00.000Z',
      },
      {
        record_kind: 'operations_audit',
        retention_class: 'security_audit_180d',
        anchor_at: '2026-01-01T00:00:00.000Z',
        expires_at: '2026-06-30T00:00:00.000Z',
      },
      {
        record_kind: 'operations_idempotency_conflict',
        retention_class: 'security_audit_180d',
        anchor_at: '2026-01-01T00:00:00.000Z',
        expires_at: '2026-06-30T00:00:00.000Z',
      },
      {
        record_kind: 'permission_action_decision',
        retention_class: 'security_audit_180d',
        anchor_at: '2026-01-01T00:00:00.000Z',
        expires_at: '2026-06-30T00:00:00.000Z',
      },
      {
        record_kind: 'permission_audit',
        retention_class: 'security_audit_180d',
        anchor_at: '2026-01-01T00:00:00.000Z',
        expires_at: '2026-06-30T00:00:00.000Z',
      },
    ]);

    const cleanup = createExecutorStore({
      database,
      provider: 'claude',
      serviceInstanceId: 'executor-retention-security-audit-sweep',
      now: () => '2026-06-30T00:00:00.000Z',
      generateId: deterministicIds('security-audit-sweep'),
    });
    const result = cleanup.runRetentionCleanup({ dryRun: false, maxLagMs: 60_000 });
    expect(result.deleted_by_kind).toMatchObject({
      permission_audit: 1,
      operations_audit: 1,
      permission_action_decision: 1,
      interaction_audit: 1,
      operations_idempotency_conflict: 1,
    });
    expect(database.prepare(`
      SELECT COUNT(*) AS count FROM runtime_permission_audit
      WHERE audit_id = 'permission-audit-retention'
    `).get()).toEqual({ count: 0 });
    expect(database.prepare(`
      SELECT COUNT(*) AS count FROM runtime_operations_audit
      WHERE audit_id = 'operations-audit-retention'
    `).get()).toEqual({ count: 0 });
    const deletionAuditId = database.prepare(`
      SELECT audit_id FROM runtime_retention_deletion_audit
      WHERE record_kind = 'permission_audit' AND record_id = 'permission-audit-retention'
    `).get().audit_id;
    expect(() => database.prepare(`
      UPDATE runtime_retention_deletion_audit SET deleted_at = deleted_at WHERE audit_id = ?
    `).run(deletionAuditId)).toThrow(/immutable/);
    expect(() => database.prepare(`
      DELETE FROM runtime_retention_deletion_audit WHERE audit_id = ?
    `).run(deletionAuditId)).toThrow(/immutable/);
    database.close();
  });

  test('retries a real SQLite busy sweep and reports max-lag without changing expiry', () => {
    const { database, databasePath } = openTestDatabase();
    createTerminalTurn(database, 'busy-retry', '2026-07-01T00:00:00.000Z');
    const blocker = new Database(databasePath);
    let retryObserved = false;
    const cleanup = createExecutorStore({
      database,
      provider: 'claude',
      serviceInstanceId: 'executor-retention-busy-retry-sweep',
      now: () => '2026-07-08T00:01:00.000Z',
      generateId: deterministicIds('busy-retry-sweep'),
      retentionCleanupSleep(delayMs, retryNo) {
        expect(delayMs).toBe(10);
        expect(retryNo).toBe(1);
        retryObserved = true;
        blocker.exec('ROLLBACK');
      },
    });
    database.pragma('busy_timeout = 1');
    blocker.pragma('journal_mode = WAL');
    blocker.exec('BEGIN IMMEDIATE');

    const expiryBefore = database.prepare(`
      SELECT expires_at FROM runtime_retention_entries
      WHERE record_kind = 'normalized_event'
      ORDER BY expires_at LIMIT 1
    `).get().expires_at;
    const result = cleanup.runRetentionCleanup({ dryRun: false, maxLagMs: 30_000 });

    expect(retryObserved).toBe(true);
    expect(result).toMatchObject({
      busy_retry_count: 1,
      observed_lag_ms: 60_000,
      max_lag_ms: 30_000,
      max_lag_exceeded: true,
      alert: {
        code: 'retention_cleanup_max_lag_exceeded',
        retryable: false,
      },
    });
    expect(database.prepare(`
      SELECT busy_retry_count, observed_lag_ms, max_lag_exceeded
      FROM runtime_retention_sweeps WHERE sweep_id = ?
    `).get(result.sweep_id)).toEqual({
      busy_retry_count: 1,
      observed_lag_ms: 60_000,
      max_lag_exceeded: 1,
    });
    expect(database.prepare(`
      SELECT expires_at FROM runtime_retention_entries
      WHERE record_kind = 'normalized_event'
      ORDER BY expires_at LIMIT 1
    `).get().expires_at).toBe(expiryBefore);
    blocker.close();
    database.close();
  });

  test('treats a concurrent winning sweep as an idempotent no-op after busy retry', () => {
    const { database, databasePath } = openTestDatabase();
    createTerminalTurn(database, 'concurrent-sweep', '2026-07-01T00:00:00.000Z');
    const blocker = new Database(databasePath);
    const winnerDatabase = new Database(databasePath);
    const winner = createExecutorStore({
      database: winnerDatabase,
      provider: 'claude',
      serviceInstanceId: 'executor-retention-concurrent-winner',
      now: () => '2026-07-08T00:01:00.000Z',
      generateId: deterministicIds('concurrent-winner'),
    });
    let winningResult;
    const contender = createExecutorStore({
      database,
      provider: 'claude',
      serviceInstanceId: 'executor-retention-concurrent-contender',
      now: () => '2026-07-08T00:01:00.000Z',
      generateId: deterministicIds('concurrent-contender'),
      retentionCleanupSleep() {
        blocker.exec('ROLLBACK');
        winningResult = winner.runRetentionCleanup({ dryRun: false, maxLagMs: 30_000 });
      },
    });
    database.pragma('busy_timeout = 1');
    blocker.pragma('journal_mode = WAL');
    blocker.exec('BEGIN IMMEDIATE');

    const result = contender.runRetentionCleanup({ dryRun: false, maxLagMs: 30_000 });

    expect(winningResult.disposed_count).toBeGreaterThan(0);
    expect(result).toMatchObject({
      busy_retry_count: 1,
      candidate_count: 0,
      disposed_count: 0,
      observed_lag_ms: 0,
      max_lag_exceeded: false,
      alert: null,
    });
    expect(winnerDatabase.prepare(`
      SELECT COUNT(*) AS count FROM runtime_retention_sweeps
    `).get()).toEqual({ count: 2 });
    blocker.close();
    winnerDatabase.close();
    database.close();
  });

  test('deletion audit hashes a versioned canonical serialization of the complete source row', () => {
    const { database } = openTestDatabase();
    const terminal = createTerminalTurn(
      database,
      'canonical-digest',
      '2026-07-01T00:00:00.000Z',
    );
    const sourceRow = database.prepare(`
      SELECT * FROM runtime_normalized_events
      WHERE turn_id = ? ORDER BY event_sequence LIMIT 1
    `).get(terminal.accepted.turn_id);
    const expectedDigest = crypto.createHash('sha256').update(canonicalJson({
      digest_schema_version: 1,
      record_kind: 'normalized_event',
      row: sourceRow,
    })).digest('hex');
    const payloadOnlyDigest = crypto.createHash('sha256')
      .update(sourceRow.event_json)
      .digest('hex');
    const cleanup = createExecutorStore({
      database,
      provider: 'claude',
      serviceInstanceId: 'executor-retention-canonical-digest',
      now: () => '2026-07-08T00:00:00.000Z',
      generateId: deterministicIds('canonical-digest'),
    });

    cleanup.runRetentionCleanup({ dryRun: false, maxLagMs: 60_000 });

    const audit = database.prepare(`
      SELECT content_digest_version, content_sha256
      FROM runtime_retention_deletion_audit
      WHERE record_kind = 'normalized_event' AND record_id = ?
    `).get(sourceRow.event_id);
    expect(audit.content_digest_version).toBe(1);
    expect(audit.content_sha256).toBe(expectedDigest);
    expect(audit.content_sha256).not.toBe(payloadOnlyDigest);
    database.close();
  });

  test('persists expiry across a real SQLite close and reopen without sliding at restart', () => {
    const { database, databasePath } = openTestDatabase();
    const terminal = createTerminalTurn(database, 'restart', '2026-07-31T23:59:59.999Z');
    const beforeRestart = database.prepare(`
      SELECT record_kind, record_id, anchor_at, expires_at
      FROM runtime_retention_entries WHERE turn_id = ? ORDER BY record_kind, record_id
    `).all(terminal.accepted.turn_id);
    database.close();

    const reopened = new Database(databasePath);
    const cleanup = createExecutorStore({
      database: reopened,
      provider: 'claude',
      serviceInstanceId: 'executor-retention-restart',
      now: () => '2026-08-07T23:59:59.999Z',
      generateId: deterministicIds('retention-restart'),
    });
    expect(reopened.prepare(`
      SELECT record_kind, record_id, anchor_at, expires_at
      FROM runtime_retention_entries WHERE turn_id = ? ORDER BY record_kind, record_id
    `).all(terminal.accepted.turn_id)).toEqual(beforeRestart);
    expect(cleanup.runRetentionCleanup({ dryRun: true, maxLagMs: 60_000 }).candidates)
      .toEqual(expect.arrayContaining(beforeRestart.filter(
        ({ expires_at: expiresAt }) => expiresAt === '2026-08-07T23:59:59.999Z',
      ).map(({ record_kind: recordKind, record_id: recordId }) => expect.objectContaining({
        record_kind: recordKind,
        record_id: recordId,
      }))));
    reopened.close();
  });

  test('protects unfinished, blocking, delivery-pending, unknown-side-effect, and background work', () => {
    const { database } = openTestDatabase();
    const blocking = createTerminalTurn(database, 'protected-blocking', '2026-01-01T00:00:00.000Z');
    const unknown = createTerminalTurn(database, 'protected-unknown', '2026-01-01T00:00:00.000Z');
    const background = createTerminalTurn(database, 'protected-background', '2026-01-01T00:00:00.000Z');
    const unfinished = createUnfinishedTurn(database, 'protected-unfinished', '2026-01-01T00:00:00.000Z');
    const pendingOutbox = createTerminalTurn(
      database,
      'protected-outbox',
      '2026-01-01T00:00:00.000Z',
      { deliverOutbox: false },
    );
    const unknownOutbox = createTerminalTurn(
      database,
      'protected-unknown-outbox',
      '2026-01-01T00:00:00.000Z',
      { deliverOutbox: false },
    );
    database.prepare(`
      UPDATE runtime_outbox SET status = 'delivery_unknown' WHERE turn_id = ?
    `).run(unknownOutbox.accepted.turn_id);
    database.prepare(`
      UPDATE runtime_provider_attempts SET side_effect_status = 'unknown'
      WHERE turn_id = ?
    `).run(unknown.accepted.turn_id);
    database.prepare(`
      INSERT INTO runtime_interactions (
        interaction_id, conversation_id, turn_id, lineage_id, parent_type,
        parent_id, ordinal, state, version, handoff_state, handoff_version,
        request_json, created_at, updated_at
      ) VALUES (?, ?, ?, ?, 'provider_turn', ?, 1, 'answer_committed', 2,
        'pending', 1, '{}', ?, ?)
    `).run(
      'interaction-protected-blocking',
      blocking.accepted.conversation_id,
      blocking.accepted.turn_id,
      blocking.accepted.lineage_id,
      blocking.accepted.turn_id,
      '2026-01-01T00:00:00.000Z',
      '2026-01-01T00:00:00.000Z',
    );
    database.prepare(`
      INSERT INTO runtime_interaction_handoffs (
        handoff_id, interaction_id, answer_id, state, parent_type,
        provider_attempt_id, handoff_attempt_id, handoff_attempt_no,
        lease_epoch, record_json, created_at, updated_at
      ) VALUES (?, ?, ?, 'pending', 'provider_turn', ?, NULL, NULL, 1, '{}', ?, ?)
    `).run(
      'handoff-protected-blocking',
      'interaction-protected-blocking',
      'answer-protected-blocking',
      blocking.context.attempt.attempt_id,
      '2026-01-01T00:00:00.000Z',
      '2026-01-01T00:00:00.000Z',
    );
    database.prepare(`
      INSERT INTO runtime_workspace_leases (
        workspace_lease_id, workspace_root, mode, holder_service_instance_id,
        holder_conversation_id, holder_turn_id, lease_epoch, lease_expires_at,
        state, acquired_at, updated_at, released_at
      ) VALUES (?, ?, 'writable', ?, ?, ?, 99, ?, 'active', ?, ?, NULL)
    `).run(
      'workspace-lease-protected-background',
      '/tmp/zylos-retention-protected-background',
      'executor-protected-background',
      background.accepted.conversation_id,
      background.accepted.turn_id,
      '2027-01-01T00:00:00.000Z',
      '2026-01-01T00:00:00.000Z',
      '2026-01-01T00:00:00.000Z',
    );
    database.prepare(`
      INSERT INTO runtime_workspace_background_work (
        background_work_id, workspace_lease_id, holder_turn_id,
        provider_task_id, state, started_at, ended_at, error_json
      ) VALUES (?, ?, ?, ?, 'active', ?, NULL, NULL)
    `).run(
      'background-work-protected',
      'workspace-lease-protected-background',
      background.accepted.turn_id,
      'provider-task-protected',
      '2026-01-01T00:00:00.000Z',
    );

    const cleanup = createExecutorStore({
      database,
      provider: 'claude',
      serviceInstanceId: 'executor-retention-protection-sweep',
      now: () => '2027-01-01T00:00:00.000Z',
      generateId: deterministicIds('protection-sweep'),
    });
    const protectedTurnIds = [
      blocking.accepted.turn_id,
      pendingOutbox.accepted.turn_id,
      unknownOutbox.accepted.turn_id,
      unknown.accepted.turn_id,
      background.accepted.turn_id,
      unfinished.accepted.turn_id,
    ];
    const dryRun = cleanup.runRetentionCleanup({ dryRun: true, maxLagMs: 60_000 });
    expect(dryRun.candidates.filter(({ turn_id: turnId }) => protectedTurnIds.includes(turnId)))
      .toEqual([]);
    for (const turnId of protectedTurnIds) {
      expect(database.prepare(`
        SELECT COUNT(*) AS count FROM runtime_normalized_events WHERE turn_id = ?
      `).get(turnId).count).toBeGreaterThan(0);
    }
    database.close();
  });

  test('rolls source deletion, deletion audit, ledger, and sweep back together on SQLite failure', () => {
    const { database } = openTestDatabase();
    const terminal = createTerminalTurn(database, 'rollback', '2026-07-01T00:00:00.000Z');
    const before = {
      events: countEvents(database, terminal.accepted.turn_id),
      entries: database.prepare(`
        SELECT COUNT(*) AS count FROM runtime_retention_entries
        WHERE turn_id = ? AND disposed_at IS NULL
      `).get(terminal.accepted.turn_id).count,
    };
    database.exec(`
      CREATE TRIGGER retention_fixture_fail_delete
      BEFORE DELETE ON runtime_projection_snapshots
      WHEN OLD.turn_id = '${terminal.accepted.turn_id}'
      BEGIN
        SELECT RAISE(ABORT, 'retention fixture injected failure');
      END;
    `);
    const cleanup = createExecutorStore({
      database,
      provider: 'claude',
      serviceInstanceId: 'executor-retention-rollback-sweep',
      now: () => '2026-08-01T00:00:00.000Z',
      generateId: deterministicIds('rollback-sweep'),
    });

    expect(() => cleanup.runRetentionCleanup({ dryRun: false, maxLagMs: 60_000 }))
      .toThrow(/injected failure/);
    expect(countEvents(database, terminal.accepted.turn_id)).toBe(before.events);
    expect(database.prepare(`
      SELECT COUNT(*) AS count FROM runtime_retention_entries
      WHERE turn_id = ? AND disposed_at IS NULL
    `).get(terminal.accepted.turn_id)).toEqual({ count: before.entries });
    expect(database.prepare(`
      SELECT COUNT(*) AS count FROM runtime_retention_deletion_audit
    `).get()).toEqual({ count: 0 });
    expect(database.prepare(`
      SELECT COUNT(*) AS count FROM runtime_retention_sweeps
    `).get()).toEqual({ count: 0 });
    database.close();
  });
});
