import crypto from 'node:crypto';

import {
  TERMINAL_TURN_STATES,
  validateContractError,
  validateObservabilitySnapshot,
  validatePublicFixtureSafety,
  validateRfc3339Timestamp,
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
  return validateRfc3339Timestamp(name, value);
}

function timestampIsAfter(name, value, referenceName, referenceValue) {
  requireTimestamp(name, value);
  requireTimestamp(referenceName, referenceValue);
  return Date.parse(value) > Date.parse(referenceValue);
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

function firstBy(rows, key) {
  const result = new Map();
  for (const row of rows) {
    const value = row[key];
    if (!result.has(value)) result.set(value, row);
  }
  return result;
}

function groupBy(rows, key) {
  const result = new Map();
  for (const row of rows) {
    const value = row[key];
    const group = result.get(value) ?? [];
    group.push(row);
    result.set(value, group);
  }
  return result;
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
    && timestampIsAfter(
      'executor lease expiry',
      row.lease_expires_at,
      'snapshot time',
      generatedAt,
    )
    && attempt.service_instance_id === serviceInstanceId
    && evidence?.controllable === true
    && typeof attempt.last_provider_event_at === 'string';
}

function collectExecutors(database, serviceInstanceId, generatedAt) {
  const candidates = database.prepare(`
    WITH executor_conversations AS (
      SELECT conversation_id FROM runtime_executor_residents
      UNION
      SELECT conversation_id FROM runtime_provider_attempts
    )
    SELECT conversation.conversation_id, conversation.region, conversation.tenant_id,
      conversation.bot_id AS conversation_bot_id,
      conversation.chat_type, conversation.chat_id,
      conversation.native_thread_or_topic_id,
      resident.conversation_id AS resident_conversation_id,
      resident.provider AS resident_provider,
      resident.owner_service_instance_id, resident.owner_epoch, resident.owner_expires_at,
      active.turn_id AS active_turn_id, active.lineage_id AS active_lineage_id,
      active.state AS active_state, active.turn_version AS active_turn_version,
      lease.lease_owner, lease.lease_epoch, lease.turn_id AS lease_turn_id,
      lease.attempt_id AS lease_attempt_id, lease.attempt_no AS lease_attempt_no,
      lease.lease_expires_at
    FROM executor_conversations AS candidate
    JOIN runtime_conversations AS conversation
      ON conversation.conversation_id = candidate.conversation_id
    LEFT JOIN runtime_executor_residents AS resident
      ON resident.conversation_id = candidate.conversation_id
    LEFT JOIN runtime_turns AS active
      ON active.conversation_id = candidate.conversation_id
      AND active.state IN ('starting', 'running', 'waiting_user', 'redirecting', 'recovering')
    LEFT JOIN runtime_executor_leases AS lease
      ON lease.conversation_id = candidate.conversation_id
    ORDER BY candidate.conversation_id
  `).all();
  const lineages = database.prepare(`
    SELECT lineage_id, conversation_id, provider, provider_native_id
    FROM runtime_lineages
    ORDER BY conversation_id, is_default DESC, created_at DESC, lineage_id DESC
  `).all();
  const lineageById = new Map(lineages.map((lineage) => [lineage.lineage_id, lineage]));
  const latestLineageByConversation = firstBy(lineages, 'conversation_id');
  const attempts = database.prepare(`
    SELECT * FROM runtime_provider_attempts
    ORDER BY conversation_id, updated_at DESC, attempt_no DESC
  `).all();
  const latestAttemptByConversation = firstBy(attempts, 'conversation_id');
  const attemptsByTurn = groupBy(attempts, 'turn_id');
  const queuedRows = database.prepare(`
    SELECT conversation_id, wait_reason
    FROM runtime_turn_queue
    WHERE status = 'queued'
    ORDER BY conversation_id, priority DESC, queue_sequence ASC
  `).all();
  const queueByConversation = groupBy(queuedRows, 'conversation_id');
  const latestEvents = firstBy(database.prepare(`
    SELECT turn.conversation_id, event.event_id, event.persisted_at
    FROM runtime_normalized_events AS event
    JOIN runtime_turns AS turn ON turn.turn_id = event.turn_id
    ORDER BY turn.conversation_id, event.persisted_at DESC, event.event_sequence DESC
  `).all(), 'conversation_id');
  const interactionStateByTurn = new Map(database.prepare(`
    SELECT turn_id,
      MAX(CASE WHEN state = 'delivery_unknown' OR handoff_state = 'delivery_unknown'
        THEN 1 ELSE 0 END) AS delivery_unknown,
      MAX(CASE WHEN state IN (
        'pending', 'answer_committed', 'answer_delivering', 'delivery_unknown'
      ) THEN 1 ELSE 0 END) AS blocking
    FROM runtime_interactions
    WHERE turn_id IS NOT NULL
    GROUP BY turn_id
  `).all().map((row) => [row.turn_id, row]));
  const unknownRecoveryTurns = new Set(database.prepare(`
    SELECT turn_id FROM runtime_execution_recoveries WHERE side_effect_status = 'unknown'
  `).all().map(({ turn_id: turnId }) => turnId));
  const backgroundWorkConversations = new Set(database.prepare(`
    SELECT DISTINCT workspace.holder_conversation_id
    FROM runtime_workspace_leases AS workspace
    LEFT JOIN runtime_workspace_background_work AS background
      ON background.workspace_lease_id = workspace.workspace_lease_id
    WHERE workspace.state IN ('active', 'uncertain')
      AND (background.background_work_id IS NULL OR background.state IN ('active', 'unknown'))
  `).all().map(({ holder_conversation_id: conversationId }) => conversationId));

  const items = candidates.map((row) => {
    const activeTurnId = row.active_turn_id ?? null;
    const lineage = row.active_lineage_id === null
      ? latestLineageByConversation.get(row.conversation_id)
      : lineageById.get(row.active_lineage_id);
    const attempt = activeTurnId === null
      ? latestAttemptByConversation.get(row.conversation_id)
      : attemptsByTurn.get(activeTurnId)?.[0];
    if (!lineage || !attempt || typeof attempt.executor_instance_id !== 'string') {
      throw new TypeError('executor identity is incomplete');
    }
    const queue = queueByConversation.get(row.conversation_id) ?? [];
    const lastEvent = latestEvents.get(row.conversation_id);
    const interactionState = interactionStateByTurn.get(activeTurnId);
    const unknownInteraction = interactionState?.delivery_unknown === 1;
    const unknownRecovery = unknownRecoveryTurns.has(activeTurnId);
    const blocking = interactionState?.blocking === 1;
    const exactFence = exactActiveFence(row, attempt, serviceInstanceId, generatedAt);
    const sideEffectUnknown = activeTurnId !== null && (
      attempt.side_effect_status === 'unknown'
      || unknownInteraction
      || unknownRecovery
    );
    let health;
    if (sideEffectUnknown || ['redirecting', 'recovering'].includes(row.active_state)) {
      health = 'degraded';
    } else if (['starting', 'running'].includes(row.active_state)) {
      health = exactFence ? 'healthy' : 'unknown';
    } else if (row.active_state === 'waiting_user') {
      health = exactFence && blocking ? 'healthy' : 'unknown';
    } else {
      health = row.resident_conversation_id !== null
        && row.owner_service_instance_id === serviceInstanceId
        && row.owner_expires_at !== null
        && timestampIsAfter(
          'resident owner expiry',
          row.owner_expires_at,
          'snapshot time',
          generatedAt,
        )
        ? 'healthy'
        : 'offline';
    }
    const resident = row.resident_conversation_id !== null;
    const blockingWork = activeTurnId !== null
      || queue.length > 0
      || backgroundWorkConversations.has(row.conversation_id);
    const waitReason = unknownInteraction
      ? 'interaction_delivery_unknown'
      : unknownRecovery
        ? 'recovery_decision'
        : queue[0]?.wait_reason ?? null;
    return publicItem({
      conversation_key: {
        region: row.region,
        tenant_id: row.tenant_id,
        bot_id: row.conversation_bot_id,
        chat_type: row.chat_type,
        chat_id: row.chat_id,
        native_thread_or_topic_id: row.native_thread_or_topic_id,
      },
      conversation_id: row.conversation_id,
      executor_version: Math.max(
        1,
        row.owner_epoch ?? 0,
        row.active_turn_version ?? 0,
        attempt.attempt_no ?? 0,
      ),
      provider: attempt.provider ?? lineage.provider ?? row.resident_provider,
      lineage_id: lineage.lineage_id,
      provider_native_id: lineage.provider_native_id,
      executor_instance_id: attempt.executor_instance_id,
      health,
      resident,
      evictable: resident && !blockingWork,
      active_turn_id: activeTurnId,
      queue_length: queue.length,
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
  const attemptsByTurn = groupBy(database.prepare(`
    SELECT * FROM runtime_provider_attempts
    ORDER BY turn_id, attempt_no DESC
  `).all(), 'turn_id');
  const latestEventByTurn = firstBy(database.prepare(`
    SELECT turn_id, event_json
    FROM runtime_normalized_events
    ORDER BY turn_id, event_sequence DESC
  `).all(), 'turn_id');
  const executionRecoveryByTurn = new Map(database.prepare(`
    SELECT turn_id, side_effect_status, error_json
    FROM runtime_execution_recoveries
  `).all().map((row) => [row.turn_id, row]));
  const stopIncidentByTurn = new Map(database.prepare(`
    SELECT turn_id, side_effect_status, error_json
    FROM runtime_provider_stop_incidents
  `).all().map((row) => [row.turn_id, row]));
  const inputGroupByTurn = new Map(database.prepare(`
    SELECT execution_turn_id, input_group_id, state, member_count, collect_until
    FROM runtime_input_groups
  `).all().map((row) => [row.execution_turn_id, row]));
  const items = rows.map((row) => {
    const attempts = attemptsByTurn.get(row.turn_id) ?? [];
    const latestEventRow = latestEventByTurn.get(row.turn_id);
    const latestEvent = parseJson(latestEventRow?.event_json, 'normalized event');
    const executionRecovery = executionRecoveryByTurn.get(row.turn_id);
    const stopIncident = stopIncidentByTurn.get(row.turn_id);
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
    const inputGroup = inputGroupByTurn.get(row.turn_id);
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
      ...(inputGroup === undefined ? {} : {
        input_group_id: inputGroup.input_group_id,
        input_group_state: inputGroup.state === 'collecting'
          ? 'input_settling'
          : inputGroup.state,
        input_group_member_count: inputGroup.member_count,
        input_group_supplement_count: inputGroup.member_count - 1,
        input_group_collect_until: inputGroup.collect_until,
      }),
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
  const waiterCounts = new Map();
  for (const { wait_detail_json: detail } of database.prepare(`
    SELECT wait_detail_json
    FROM runtime_turn_queue
    WHERE status = 'queued' AND wait_reason = 'workspace_lease'
      AND wait_detail_json IS NOT NULL
  `).all()) {
    const holderTurnId = parseJson(detail, 'workspace waiter')?.holder_turn_id;
    if (typeof holderTurnId === 'string') {
      waiterCounts.set(holderTurnId, (waiterCounts.get(holderTurnId) ?? 0) + 1);
    }
  }
  const rows = database.prepare(`
    SELECT *
    FROM runtime_workspace_leases
    WHERE state IN ('active', 'uncertain')
    ORDER BY workspace_root, lease_epoch, workspace_lease_id
  `).all();
  const backgroundByLease = firstBy(database.prepare(`
    SELECT workspace_lease_id, background_work_id
    FROM runtime_workspace_background_work
    WHERE state IN ('active', 'unknown')
    ORDER BY workspace_lease_id, background_work_id
  `).all(), 'workspace_lease_id');
  const items = rows.map((row) => {
    const background = backgroundByLease.get(row.workspace_lease_id);
    return publicItem({
      workspace_root: row.workspace_root,
      mode: row.mode === 'writable' ? 'write' : 'read',
      holder_conversation_id: row.holder_conversation_id,
      holder_turn_id: row.holder_turn_id,
      holder_background_work_id: background?.background_work_id ?? null,
      expires_at: row.lease_expires_at,
      epoch: row.lease_epoch,
      waiter_count: waiterCounts.get(row.holder_turn_id) ?? 0,
    });
  });
  return Object.freeze({ complete: true, items: Object.freeze(items), error: null });
}

function collectOutbox(database, generatedAt) {
  const rows = database.prepare(`
    SELECT outbox.status, outbox.command_json, outbox.created_at,
      outbox.lease_expires_at, outbox.lease_expires_epoch_ms,
      outbox.pre_action_fenced_at, outbox.last_error_json,
      reconciliation.state AS reconciliation_state,
      reconciliation.last_error_code AS reconciliation_error_code
    FROM runtime_outbox AS outbox
    LEFT JOIN runtime_turns AS turn ON turn.turn_id = outbox.turn_id
    LEFT JOIN runtime_outbox_reconciliations AS reconciliation
      ON reconciliation.outbox_id = outbox.outbox_id
      AND reconciliation.reconciliation_epoch = (
        SELECT MAX(candidate.reconciliation_epoch)
        FROM runtime_outbox_reconciliations AS candidate
        WHERE candidate.outbox_id = outbox.outbox_id
      )
    WHERE outbox.status IN (
      'pending', 'delivering', 'retry_wait', 'dead_letter', 'delivery_unknown'
    )
      AND NOT (
        outbox.status = 'dead_letter'
        AND turn.state IN (${TERMINAL_TURN_STATES.map((state) => `'${state}'`).join(', ')})
      )
    ORDER BY outbox.created_at, outbox.outbox_id
  `).all();
  const grouped = new Map();
  for (const row of rows) {
    const command = parseJson(row.command_json, 'outbox command');
    const lastError = parseJson(row.last_error_json, 'outbox last error');
    const channel = command?.target?.channel;
    requireNonEmptyString('outbox channel', channel);
    const staleDelivering = row.status === 'delivering'
      && row.pre_action_fenced_at !== null
      && row.lease_expires_epoch_ms !== null
      && row.lease_expires_epoch_ms <= Date.parse(generatedAt);
    const status = staleDelivering ? 'delivery_unknown' : row.status;
    const key = `${channel}\u0000${status}`;
    const age = Math.max(
      0,
      Math.floor((Date.parse(generatedAt) - Date.parse(row.created_at)) / 1000),
    );
    const current = grouped.get(key) ?? {
      channel,
      status,
      count: 0,
      oldest_age_seconds: 0,
    };
    if (status === 'delivery_unknown') {
      const reconciliationState = row.reconciliation_state
        ?? (
          staleDelivering
            ? command.operation === 'update_main' ? 'required' : 'not_reconcilable'
            : 'not_applicable'
        );
      const errorCode = row.reconciliation_error_code
        ?? lastError?.code
        ?? (
          staleDelivering
            ? command.operation === 'update_main'
              ? 'delivery_reconciliation_required'
              : 'delivery_side_effect_unknown_fail_closed'
            : 'delivery_claim_authority_unverifiable'
        );
      const staleAge = staleDelivering
        ? Math.max(0, Math.floor(
          (Date.parse(generatedAt) - row.lease_expires_epoch_ms) / 1000,
        ))
        : 0;
      current.stale_delivering_age_seconds = Math.max(
        current.stale_delivering_age_seconds ?? 0,
        staleAge,
      );
      current.reconciliation_state = current.reconciliation_state === undefined
        || current.reconciliation_state === reconciliationState
        ? reconciliationState
        : 'multiple';
      current.error_code = current.error_code === undefined
        || current.error_code === errorCode
        ? errorCode
        : 'multiple_delivery_errors';
    }
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
  const items = database.prepare(`
    SELECT category, COUNT(*) AS count, MAX(committed_at) AS last_committed_at
    FROM (
      SELECT 'permission' AS category, committed_at FROM runtime_permission_audit
      UNION ALL
      SELECT 'interaction', created_at FROM runtime_interaction_audit
      UNION ALL
      SELECT 'recovery', created_at FROM runtime_execution_recoveries
      UNION ALL
      SELECT 'recovery', created_at FROM runtime_reply_mapping_recoveries
      UNION ALL
      SELECT 'runtime_control', committed_at FROM runtime_stop_controls
      UNION ALL
      SELECT 'runtime_control', committed_at FROM runtime_steer_controls
      UNION ALL
      SELECT 'operations_control', committed_at FROM runtime_operations_audit
      UNION ALL
      SELECT 'provider_diagnostic', observed_at FROM runtime_provider_event_diagnostics
      UNION ALL
      SELECT 'scheduler', committed_at FROM runtime_scheduler_occurrences
      UNION ALL
      SELECT 'provider_stop', created_at FROM runtime_provider_stop_incidents
    )
    GROUP BY category
    ORDER BY category
  `).all().map(publicItem);
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

function deriveServiceHealth(sections, serviceDegraded, serviceOffline) {
  if (serviceOffline) return 'offline';
  if (serviceDegraded || Object.values(sections).some((section) => section.complete === false)) {
    return 'degraded';
  }
  if (sections.executors.items.some(({ health }) => ['degraded', 'unknown'].includes(health))) {
    return 'degraded';
  }
  if (sections.turns.items.some(
    ({ state, side_effect_status: status }) => (
      state === 'recovering'
      || (status === 'unknown' && !TERMINAL_TURN_STATES.includes(state))
    ),
  )) {
    return 'degraded';
  }
  if (sections.outbox.dead_letter_count > 0) return 'degraded';
  if (sections.outbox.items.some(({ status }) => status === 'delivery_unknown')) {
    return 'degraded';
  }
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
  let registered = database.prepare(`
    SELECT * FROM runtime_observability_instances WHERE service_instance_id = ?
  `).get(serviceInstanceId);
  if (registered === undefined) {
    database.prepare(`
      INSERT INTO runtime_observability_instances (
        service_instance_id, host_id, started_at, service_version,
        snapshot_version, last_reconciliation_at, created_at, updated_at
      ) VALUES (?, ?, ?, 1, 0, NULL, ?, ?)
    `).run(serviceInstanceId, hostId, registeredAt, registeredAt, registeredAt);
    registered = database.prepare(`
      SELECT * FROM runtime_observability_instances WHERE service_instance_id = ?
    `).get(serviceInstanceId);
  }
  requireTimestamp('registered started_at', registered.started_at);
  requireTimestamp('registered created_at', registered.created_at);
  requireTimestamp('registered updated_at', registered.updated_at);
  if (registered.last_reconciliation_at !== null) {
    requireTimestamp('registered last_reconciliation_at', registered.last_reconciliation_at);
  }
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
    const generatedAt = requireTimestamp('snapshot time', now());
    const snapshotId = requireNonEmptyString(
      'snapshot id',
      generateId('observability-snapshot'),
    );
    const readSnapshot = database.transaction(() => {
      const instance = database.prepare(`
        SELECT * FROM runtime_observability_instances WHERE service_instance_id = ?
      `).get(serviceInstanceId);
      if (instance === undefined) throw new Error('observability service instance is unavailable');
      requireTimestamp('registered started_at', instance.started_at);
      requireTimestamp('registered updated_at', instance.updated_at);
      if (instance.last_reconciliation_at !== null) {
        requireTimestamp('registered last_reconciliation_at', instance.last_reconciliation_at);
      }
      const failures = [];
      const collected = {
        executors: collect(
          'executors',
          () => collectExecutors(database, serviceInstanceId, generatedAt),
          failures,
        ),
        turns: collect('turns', () => collectTurns(database), failures),
      };
      let serviceState = {};
      let serviceComplete = true;
      try {
        serviceState = getServiceState() ?? {};
      } catch {
        failures.push('service');
        serviceComplete = false;
      }
      Object.assign(collected, {
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
      });
      const validationInstance = {
        ...instance,
        snapshot_version: Math.max(1, instance.snapshot_version + 1),
      };
      for (const [name, value] of Object.entries(collected)) {
        if (value === null) continue;
        try {
          validateSection({
            name,
            value,
            instance: validationInstance,
            snapshotId,
            generatedAt,
          });
        } catch {
          failures.push(name);
          collected[name] = null;
        }
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
          serviceState.offline === true,
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
      const advanced = database.prepare(`
        UPDATE runtime_observability_instances
        SET snapshot_version = snapshot_version + 1, updated_at = ?
        WHERE service_instance_id = ?
      `).run(generatedAt, serviceInstanceId);
      if (advanced.changes !== 1) throw new Error('observability service instance is unavailable');
      const snapshotVersion = database.prepare(`
        SELECT snapshot_version FROM runtime_observability_instances
        WHERE service_instance_id = ?
      `).get(serviceInstanceId).snapshot_version;
      return { service, sections, error, snapshotVersion };
    }).deferred();

    const snapshot = {
      contract: 'zylos.observability-snapshot',
      contract_version: '1.0',
      snapshot_id: snapshotId,
      core_service_instance_id: serviceInstanceId,
      generated_at: generatedAt,
      snapshot_version: readSnapshot.snapshotVersion,
      service: readSnapshot.service,
      ...readSnapshot.sections,
      error: readSnapshot.error,
    };
    return validateObservabilitySnapshot(snapshot, { occurredAt: generatedAt }).forwarded;
  }

  return Object.freeze({ publish, recordReconciliation });
}
