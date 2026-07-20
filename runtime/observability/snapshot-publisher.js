import crypto from 'node:crypto';

import {
  validateContractError,
  validateObservabilitySnapshot,
  validatePublicFixtureSafety,
} from '../../contracts/public/index.js';
import { initializeRuntimePersistence } from '../persistence/schema.js';

function defaultGenerateId(kind) {
  return `${kind}-${crypto.randomUUID()}`;
}

function requireNonEmptyString(name, value) {
  if (typeof value !== 'string' || value.length === 0) {
    throw new TypeError(`${name} must be a non-empty string`);
  }
  return value;
}

function requireTimestamp(name, value) {
  if (typeof value !== 'string' || !Number.isFinite(Date.parse(value))) {
    throw new TypeError(`${name} must be an RFC 3339 timestamp`);
  }
  return value;
}

function parseJson(value, label) {
  if (value === null || value === undefined) return null;
  try {
    return JSON.parse(value);
  } catch {
    throw new TypeError(`${label} is not valid JSON`);
  }
}

function projectPublicError(value, label) {
  if (value === null || value === undefined) return null;
  const parsed = typeof value === 'string' ? parseJson(value, label) : value;
  const projected = validateContractError(parsed);
  validatePublicFixtureSafety(projected);
  return Object.freeze(projected);
}

function publicItem(value) {
  validatePublicFixtureSafety(value);
  return Object.freeze(value);
}

function strongestSideEffectStatus(...statuses) {
  if (statuses.includes('unknown')) return 'unknown';
  if (statuses.includes('known')) return 'known';
  return 'none';
}

function createDegradedError(snapshotId, occurredAt) {
  return Object.freeze({
    code: 'observability_degraded',
    category: 'storage',
    retryable: true,
    side_effect_status: 'none',
    user_message: 'The snapshot is partial; unavailable collections are marked incomplete.',
    detail_ref: `diagnostic:${snapshotId}`,
    occurred_at: occurredAt,
  });
}

function emptyCollection(name, error) {
  if (name === 'outbox') {
    return Object.freeze({
      complete: false,
      items: Object.freeze([]),
      retry_count: 0,
      dead_letter_count: 0,
      error,
    });
  }
  return Object.freeze({ complete: false, items: Object.freeze([]), error });
}

function collect(name, collector, failures) {
  try {
    return collector();
  } catch {
    failures.push(name);
    return null;
  }
}

function latestLineage(database, conversationId, activeLineageId) {
  if (activeLineageId !== null) {
    return database.prepare(`
      SELECT lineage_id, provider, provider_native_id
      FROM runtime_lineages
      WHERE lineage_id = ? AND conversation_id = ?
    `).get(activeLineageId, conversationId);
  }
  return database.prepare(`
    SELECT lineage_id, provider, provider_native_id
    FROM runtime_lineages
    WHERE conversation_id = ? AND provider IS NOT NULL
    ORDER BY is_default DESC, created_at DESC, lineage_id DESC
    LIMIT 1
  `).get(conversationId);
}

function latestAttempt(database, conversationId, turnId) {
  if (turnId !== null) {
    return database.prepare(`
      SELECT * FROM runtime_provider_attempts
      WHERE turn_id = ?
      ORDER BY attempt_no DESC
      LIMIT 1
    `).get(turnId);
  }
  return database.prepare(`
    SELECT * FROM runtime_provider_attempts
    WHERE conversation_id = ?
    ORDER BY updated_at DESC, attempt_no DESC
    LIMIT 1
  `).get(conversationId);
}

function runtimeIdentity(attempt) {
  const evidence = parseJson(attempt?.runtime_evidence_json, 'runtime evidence');
  const process = evidence?.process ?? null;
  if (process !== null && process.diagnostic_only !== true) {
    throw new TypeError('runtime process identity must be diagnostic-only');
  }
  return publicItem({
    diagnostic_only: true,
    pid: process?.pid ?? null,
    pgid: process?.pgid ?? null,
    process_start_time: process?.started_at ?? null,
  });
}

function exactActiveFence(row, attempt, serviceInstanceId, generatedAt) {
  if (attempt === undefined || attempt === null) return false;
  const evidence = parseJson(attempt.runtime_evidence_json, 'runtime evidence');
  return row.lease_owner === serviceInstanceId
    && row.lease_turn_id === row.active_turn_id
    && row.lease_attempt_id === attempt.attempt_id
    && row.lease_attempt_no === attempt.attempt_no
    && row.lease_epoch === attempt.lease_epoch
    && row.lease_expires_at !== null
    && row.lease_expires_at > generatedAt
    && attempt.service_instance_id === serviceInstanceId
    && evidence?.controllable === true
    && typeof attempt.last_provider_event_at === 'string';
}

function collectExecutors(database, serviceInstanceId, generatedAt) {
  const residents = database.prepare(`
    SELECT resident.*, conversation.region, conversation.tenant_id,
      conversation.chat_type, conversation.chat_id,
      conversation.native_thread_or_topic_id,
      active.turn_id AS active_turn_id, active.lineage_id AS active_lineage_id,
      active.state AS active_state, active.turn_version AS active_turn_version,
      lease.lease_owner, lease.lease_epoch, lease.turn_id AS lease_turn_id,
      lease.attempt_id AS lease_attempt_id, lease.attempt_no AS lease_attempt_no,
      lease.lease_expires_at
    FROM runtime_executor_residents AS resident
    JOIN runtime_conversations AS conversation
      ON conversation.conversation_id = resident.conversation_id
    LEFT JOIN runtime_turns AS active
      ON active.conversation_id = resident.conversation_id
      AND active.state IN ('starting', 'running', 'waiting_user', 'redirecting', 'recovering')
    LEFT JOIN runtime_executor_leases AS lease
      ON lease.conversation_id = resident.conversation_id
    ORDER BY resident.conversation_id
  `).all();

  const items = residents.map((row) => {
    const activeTurnId = row.active_turn_id ?? null;
    const lineage = latestLineage(database, row.conversation_id, row.active_lineage_id ?? null);
    const attempt = latestAttempt(database, row.conversation_id, activeTurnId);
    if (!lineage || !attempt || typeof attempt.executor_instance_id !== 'string') {
      throw new TypeError('resident executor identity is incomplete');
    }
    const queue = database.prepare(`
      SELECT COUNT(*) AS count
      FROM runtime_turn_queue
      WHERE conversation_id = ? AND status = 'queued'
    `).get(row.conversation_id).count;
    const queuedWait = database.prepare(`
      SELECT wait_reason
      FROM runtime_turn_queue
      WHERE conversation_id = ? AND status = 'queued'
      ORDER BY priority DESC, queue_sequence ASC
      LIMIT 1
    `).get(row.conversation_id)?.wait_reason ?? null;
    const lastEvent = database.prepare(`
      SELECT event_id, persisted_at
      FROM runtime_normalized_events AS event
      JOIN runtime_turns AS turn ON turn.turn_id = event.turn_id
      WHERE turn.conversation_id = ?
      ORDER BY event.persisted_at DESC, event.event_sequence DESC
      LIMIT 1
    `).get(row.conversation_id);
    const unknownInteraction = activeTurnId === null ? undefined : database.prepare(`
      SELECT 1
      FROM runtime_interactions
      WHERE turn_id = ? AND (state = 'delivery_unknown' OR handoff_state = 'delivery_unknown')
      LIMIT 1
    `).get(activeTurnId);
    const unknownRecovery = activeTurnId === null ? undefined : database.prepare(`
      SELECT 1
      FROM runtime_execution_recoveries
      WHERE turn_id = ? AND side_effect_status = 'unknown'
      LIMIT 1
    `).get(activeTurnId);
    const blocking = activeTurnId === null ? undefined : database.prepare(`
      SELECT 1
      FROM runtime_interactions
      WHERE turn_id = ?
        AND state IN ('pending', 'answer_committed', 'answer_delivering', 'delivery_unknown')
      LIMIT 1
    `).get(activeTurnId);
    const exactFence = exactActiveFence(row, attempt, serviceInstanceId, generatedAt);
    const sideEffectUnknown = attempt.side_effect_status === 'unknown'
      || unknownInteraction !== undefined
      || unknownRecovery !== undefined;
    let health;
    if (sideEffectUnknown || ['redirecting', 'recovering'].includes(row.active_state)) {
      health = 'degraded';
    } else if (['starting', 'running'].includes(row.active_state)) {
      health = exactFence ? 'healthy' : 'unknown';
    } else if (row.active_state === 'waiting_user') {
      health = exactFence && blocking !== undefined ? 'healthy' : 'unknown';
    } else {
      health = row.owner_service_instance_id === serviceInstanceId
        && row.owner_expires_at !== null
        && row.owner_expires_at > generatedAt
        ? 'healthy'
        : 'unknown';
    }
    const blockingWork = database.prepare(`
      SELECT 1
      FROM runtime_turns
      WHERE conversation_id = ?
        AND state IN ('queued', 'starting', 'running', 'waiting_user', 'redirecting', 'recovering')
      UNION ALL
      SELECT 1
      FROM runtime_workspace_leases AS workspace
      LEFT JOIN runtime_workspace_background_work AS background
        ON background.workspace_lease_id = workspace.workspace_lease_id
      WHERE workspace.holder_conversation_id = ?
        AND workspace.state IN ('active', 'uncertain')
        AND (background.background_work_id IS NULL OR background.state IN ('active', 'unknown'))
      LIMIT 1
    `).get(row.conversation_id, row.conversation_id);
    const waitReason = unknownInteraction !== undefined
      ? 'interaction_delivery_unknown'
      : unknownRecovery !== undefined
        ? 'recovery_decision'
        : queuedWait;
    return publicItem({
      conversation_key: {
        region: row.region,
        tenant_id: row.tenant_id,
        bot_id: row.bot_id,
        chat_type: row.chat_type,
        chat_id: row.chat_id,
        native_thread_or_topic_id: row.native_thread_or_topic_id,
      },
      conversation_id: row.conversation_id,
      executor_version: Math.max(
        1,
        row.owner_epoch,
        row.active_turn_version ?? 0,
        attempt.attempt_no ?? 0,
      ),
      provider: row.provider,
      lineage_id: lineage.lineage_id,
      provider_native_id: lineage.provider_native_id,
      executor_instance_id: attempt.executor_instance_id,
      health,
      resident: true,
      evictable: blockingWork === undefined,
      active_turn_id: activeTurnId,
      queue_length: queue,
      wait_reason: waitReason,
      last_event_id: lastEvent?.event_id ?? null,
      last_event_at: lastEvent?.persisted_at ?? null,
      lease: {
        owner: row.lease_owner ?? null,
        expires_at: row.lease_expires_at ?? null,
        epoch: row.lease_epoch ?? 0,
      },
      runtime_identity: runtimeIdentity(attempt),
    });
  });
  return Object.freeze({ complete: true, items: Object.freeze(items), error: null });
}

function queuePositions(database) {
  const positions = new Map();
  const counts = new Map();
  const rows = database.prepare(`
    SELECT conversation_id, turn_id
    FROM runtime_turn_queue
    WHERE status = 'queued'
    ORDER BY conversation_id, priority DESC, queue_sequence ASC
  `).all();
  for (const row of rows) {
    const position = (counts.get(row.conversation_id) ?? 0) + 1;
    counts.set(row.conversation_id, position);
    positions.set(row.turn_id, position);
  }
  return positions;
}

function collectTurns(database) {
  const positions = queuePositions(database);
  const rows = database.prepare(`
    SELECT * FROM runtime_turns ORDER BY created_at, turn_id
  `).all();
  const items = rows.map((row) => {
    const attempts = database.prepare(`
      SELECT * FROM runtime_provider_attempts
      WHERE turn_id = ?
      ORDER BY attempt_no DESC
    `).all(row.turn_id);
    const latestEventRow = database.prepare(`
      SELECT event_json
      FROM runtime_normalized_events
      WHERE turn_id = ?
      ORDER BY event_sequence DESC
      LIMIT 1
    `).get(row.turn_id);
    const latestEvent = parseJson(latestEventRow?.event_json, 'normalized event');
    const executionRecovery = database.prepare(`
      SELECT side_effect_status, error_json
      FROM runtime_execution_recoveries
      WHERE turn_id = ?
    `).get(row.turn_id);
    const stopIncident = database.prepare(`
      SELECT side_effect_status, error_json
      FROM runtime_provider_stop_incidents
      WHERE turn_id = ?
    `).get(row.turn_id);
    const errorSource = latestEvent?.error
      ?? attempts[0]?.error_json
      ?? executionRecovery?.error_json
      ?? stopIncident?.error_json
      ?? null;
    const error = projectPublicError(errorSource, 'turn error');
    const sideEffectStatus = strongestSideEffectStatus(
      ...attempts.map(({ side_effect_status: status }) => status),
      executionRecovery?.side_effect_status,
      stopIncident?.side_effect_status,
      error?.side_effect_status,
    );
    const recoveryOfTurnId = latestEvent?.payload?.recovery_of_turn_id ?? null;
    return publicItem({
      turn_id: row.turn_id,
      conversation_id: row.conversation_id,
      lineage_id: row.lineage_id,
      turn_version: row.turn_version,
      state: row.state,
      phase: latestEvent?.phase ?? row.state,
      attempt_count: attempts.length,
      retry_count: Math.max(0, attempts.length - 1),
      queue_position: positions.get(row.turn_id) ?? null,
      recovery_of_turn_id: recoveryOfTurnId,
      side_effect_status: sideEffectStatus,
      error,
    });
  });
  return Object.freeze({ complete: true, items: Object.freeze(items), error: null });
}

function collectInteractions(database) {
  const rows = database.prepare(`
    SELECT interaction.*, handoff.record_json AS handoff_json
    FROM runtime_interactions AS interaction
    LEFT JOIN runtime_interaction_handoffs AS handoff
      ON handoff.interaction_id = interaction.interaction_id
    ORDER BY interaction.created_at, interaction.interaction_id
  `).all();
  const items = rows.map((row) => {
    const request = parseJson(row.request_json, 'interaction request');
    const handoff = parseJson(row.handoff_json, 'interaction handoff');
    if (!Array.isArray(request?.authorized_subjects)) {
      throw new TypeError('interaction authorized subjects are unavailable');
    }
    const subjects = request.authorized_subjects;
    return publicItem({
      interaction_id: row.interaction_id,
      conversation_id: row.conversation_id,
      turn_id: row.turn_id,
      blocking_ordinal: row.ordinal,
      kind: request?.kind,
      state: row.state,
      interaction_version: row.version,
      handoff_state: row.handoff_state,
      handoff_deadline_at: handoff?.handoff_deadline_at ?? null,
      authorized_subject_summary: {
        actor_count: subjects.filter(({ type }) => type === 'actor').length,
        capability_count: subjects.filter(({ type }) => type === 'capability').length,
      },
      created_at: row.created_at,
      expires_at: request?.expires_at,
    });
  });
  return Object.freeze({ complete: true, items: Object.freeze(items), error: null });
}

function collectWorkspaceLeases(database) {
  const waiters = database.prepare(`
    SELECT wait_detail_json
    FROM runtime_turn_queue
    WHERE status = 'queued' AND wait_reason = 'workspace_lease'
      AND wait_detail_json IS NOT NULL
  `).all().map(({ wait_detail_json: detail }) => parseJson(detail, 'workspace waiter'));
  const rows = database.prepare(`
    SELECT *
    FROM runtime_workspace_leases
    WHERE state IN ('active', 'uncertain')
    ORDER BY workspace_root, lease_epoch, workspace_lease_id
  `).all();
  const items = rows.map((row) => {
    const background = database.prepare(`
      SELECT background_work_id
      FROM runtime_workspace_background_work
      WHERE workspace_lease_id = ? AND state IN ('active', 'unknown')
      ORDER BY background_work_id
      LIMIT 1
    `).get(row.workspace_lease_id);
    return publicItem({
      workspace_root: row.workspace_root,
      mode: row.mode === 'writable' ? 'write' : 'read',
      holder_conversation_id: row.holder_conversation_id,
      holder_turn_id: row.holder_turn_id,
      holder_background_work_id: background?.background_work_id ?? null,
      expires_at: row.lease_expires_at,
      epoch: row.lease_epoch,
      waiter_count: waiters.filter(
        (detail) => detail?.holder_turn_id === row.holder_turn_id,
      ).length,
    });
  });
  return Object.freeze({ complete: true, items: Object.freeze(items), error: null });
}

function collectOutbox(database, generatedAt) {
  const rows = database.prepare(`
    SELECT status, command_json, created_at
    FROM runtime_outbox
    WHERE status IN ('pending', 'delivering', 'retry_wait', 'dead_letter')
    ORDER BY created_at, outbox_id
  `).all();
  const grouped = new Map();
  for (const row of rows) {
    const command = parseJson(row.command_json, 'outbox command');
    const channel = command?.target?.channel;
    requireNonEmptyString('outbox channel', channel);
    const key = `${channel}\u0000${row.status}`;
    const age = Math.max(
      0,
      Math.floor((Date.parse(generatedAt) - Date.parse(row.created_at)) / 1000),
    );
    const current = grouped.get(key) ?? {
      channel,
      status: row.status,
      count: 0,
      oldest_age_seconds: 0,
    };
    current.count += 1;
    current.oldest_age_seconds = Math.max(current.oldest_age_seconds, age);
    grouped.set(key, current);
  }
  const items = [...grouped.values()]
    .sort((left, right) => {
      if (left.channel !== right.channel) return left.channel < right.channel ? -1 : 1;
      if (left.status === right.status) return 0;
      return left.status < right.status ? -1 : 1;
    })
    .map(publicItem);
  return Object.freeze({
    complete: true,
    items: Object.freeze(items),
    retry_count: rows.filter(({ status }) => status === 'retry_wait').length,
    dead_letter_count: rows.filter(({ status }) => status === 'dead_letter').length,
    error: null,
  });
}

function collectAuditSummary(database) {
  const queries = Object.freeze([
    ['permission', `SELECT committed_at FROM runtime_permission_audit`],
    ['interaction', `SELECT created_at AS committed_at FROM runtime_interaction_audit`],
    ['recovery', `
      SELECT created_at AS committed_at FROM runtime_execution_recoveries
      UNION ALL
      SELECT created_at AS committed_at FROM runtime_reply_mapping_recoveries
    `],
    ['runtime_control', `
      SELECT committed_at FROM runtime_stop_controls
      UNION ALL
      SELECT committed_at FROM runtime_steer_controls
    `],
    ['provider_diagnostic', `
      SELECT observed_at AS committed_at FROM runtime_provider_event_diagnostics
    `],
  ]);
  const items = queries.flatMap(([category, source]) => {
    const row = database.prepare(`
      SELECT COUNT(*) AS count, MAX(committed_at) AS last_committed_at
      FROM (${source})
    `).get();
    return row.count === 0 ? [] : [publicItem({ category, ...row })];
  });
  return Object.freeze({ complete: true, items: Object.freeze(items), error: null });
}

function emptyCompleteSections() {
  return {
    executors: { complete: true, items: [], error: null },
    turns: { complete: true, items: [], error: null },
    interactions: { complete: true, items: [], error: null },
    workspace_leases: { complete: true, items: [], error: null },
    outbox: {
      complete: true,
      items: [],
      retry_count: 0,
      dead_letter_count: 0,
      error: null,
    },
    audit_summary: { complete: true, items: [], error: null },
  };
}

function validateSection({
  name,
  value,
  instance,
  snapshotId,
  generatedAt,
}) {
  const sections = emptyCompleteSections();
  sections[name] = value;
  validateObservabilitySnapshot({
    contract: 'zylos.observability-snapshot',
    contract_version: '1.0',
    snapshot_id: snapshotId,
    core_service_instance_id: instance.service_instance_id,
    generated_at: generatedAt,
    snapshot_version: instance.snapshot_version,
    service: {
      complete: true,
      service_version: instance.service_version,
      health: 'healthy',
      maintenance: false,
      draining: false,
      reconciling: false,
      host_id: instance.host_id,
      service_instance_id: instance.service_instance_id,
      started_at: instance.started_at,
      last_reconciliation_at: instance.last_reconciliation_at,
      error: null,
    },
    ...sections,
    error: null,
  }, { occurredAt: generatedAt });
  return value;
}

function deriveServiceHealth(sections, serviceDegraded) {
  if (serviceDegraded || Object.values(sections).some((section) => section.complete === false)) {
    return 'degraded';
  }
  if (sections.executors.items.some(({ health }) => ['degraded', 'unknown'].includes(health))) {
    return 'degraded';
  }
  if (sections.turns.items.some(
    ({ state, side_effect_status: status }) => state === 'recovering' || status === 'unknown',
  )) {
    return 'degraded';
  }
  if (sections.outbox.dead_letter_count > 0) return 'degraded';
  return 'healthy';
}

export function createRuntimeSnapshotPublisher({
  database,
  serviceInstanceId,
  hostId = serviceInstanceId,
  startedAt,
  now = () => new Date().toISOString(),
  generateId = defaultGenerateId,
  getServiceState = () => ({}),
}) {
  if (!database || typeof database.transaction !== 'function') {
    throw new TypeError('database must be a better-sqlite3 connection');
  }
  requireNonEmptyString('serviceInstanceId', serviceInstanceId);
  requireNonEmptyString('hostId', hostId);
  if (typeof now !== 'function') throw new TypeError('now must be a function');
  if (typeof generateId !== 'function') throw new TypeError('generateId must be a function');
  if (typeof getServiceState !== 'function') {
    throw new TypeError('getServiceState must be a function');
  }
  const registeredAt = requireTimestamp('startedAt', startedAt ?? now());
  initializeRuntimePersistence(database);
  database.prepare(`
    INSERT OR IGNORE INTO runtime_observability_instances (
      service_instance_id, host_id, started_at, service_version,
      snapshot_version, last_reconciliation_at, created_at, updated_at
    ) VALUES (?, ?, ?, 1, 0, NULL, ?, ?)
  `).run(serviceInstanceId, hostId, registeredAt, registeredAt, registeredAt);
  const registered = database.prepare(`
    SELECT host_id FROM runtime_observability_instances WHERE service_instance_id = ?
  `).get(serviceInstanceId);
  if (registered.host_id !== hostId) {
    throw new TypeError('serviceInstanceId is already registered to another host identity');
  }

  function recordReconciliation() {
    const reconciledAt = requireTimestamp('reconciliation time', now());
    const updated = database.prepare(`
      UPDATE runtime_observability_instances
      SET last_reconciliation_at = ?, service_version = service_version + 1, updated_at = ?
      WHERE service_instance_id = ?
    `).run(reconciledAt, reconciledAt, serviceInstanceId);
    if (updated.changes !== 1) throw new Error('observability service instance is unavailable');
    return Object.freeze({ recorded_at: reconciledAt });
  }

  function publish() {
    const publishTransaction = database.transaction(() => {
      const generatedAt = requireTimestamp('snapshot time', now());
      const snapshotId = requireNonEmptyString(
        'snapshot id',
        generateId('observability-snapshot'),
      );
      const advanced = database.prepare(`
        UPDATE runtime_observability_instances
        SET snapshot_version = snapshot_version + 1, updated_at = ?
        WHERE service_instance_id = ?
      `).run(generatedAt, serviceInstanceId);
      if (advanced.changes !== 1) throw new Error('observability service instance is unavailable');
      const instance = database.prepare(`
        SELECT * FROM runtime_observability_instances WHERE service_instance_id = ?
      `).get(serviceInstanceId);
      const failures = [];
      const collected = {
        executors: collect(
          'executors',
          () => collectExecutors(database, serviceInstanceId, generatedAt),
          failures,
        ),
        turns: collect('turns', () => collectTurns(database), failures),
        interactions: collect('interactions', () => collectInteractions(database), failures),
        workspace_leases: collect(
          'workspace_leases',
          () => collectWorkspaceLeases(database),
          failures,
        ),
        outbox: collect('outbox', () => collectOutbox(database, generatedAt), failures),
        audit_summary: collect(
          'audit_summary',
          () => collectAuditSummary(database),
          failures,
        ),
      };
      for (const [name, value] of Object.entries(collected)) {
        if (value === null) continue;
        try {
          validateSection({ name, value, instance, snapshotId, generatedAt });
        } catch {
          failures.push(name);
          collected[name] = null;
        }
      }
      let serviceState = {};
      let serviceComplete = true;
      try {
        serviceState = getServiceState() ?? {};
      } catch {
        failures.push('service');
        serviceComplete = false;
      }
      const error = failures.length === 0 ? null : createDegradedError(snapshotId, generatedAt);
      const sections = Object.fromEntries(Object.entries(collected).map(([name, value]) => [
        name,
        value ?? emptyCollection(name, error),
      ]));
      const service = Object.freeze({
        complete: serviceComplete,
        service_version: instance.service_version,
        health: deriveServiceHealth(
          sections,
          !serviceComplete || serviceState.degraded === true,
        ),
        maintenance: serviceState.maintenance === true,
        draining: serviceState.draining === true,
        reconciling: serviceState.reconciling === true,
        host_id: instance.host_id,
        service_instance_id: instance.service_instance_id,
        started_at: instance.started_at,
        last_reconciliation_at: instance.last_reconciliation_at,
        error: serviceComplete ? null : error,
      });
      const snapshot = {
        contract: 'zylos.observability-snapshot',
        contract_version: '1.0',
        snapshot_id: snapshotId,
        core_service_instance_id: serviceInstanceId,
        generated_at: generatedAt,
        snapshot_version: instance.snapshot_version,
        service,
        ...sections,
        error,
      };
      return validateObservabilitySnapshot(snapshot, { occurredAt: generatedAt }).forwarded;
    });
    return publishTransaction.immediate();
  }

  return Object.freeze({ publish, recordReconciliation });
}
