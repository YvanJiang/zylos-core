import path from 'node:path';

import {
  admitNormalizedEvent,
  canonicalizeJson,
  ContractKernelError,
  createContractError,
  createIdempotencyKey,
  createPayloadHash,
  createNormalizedEventStreamState,
  INTERACTION_ANSWER_SCHEMA_V1,
  resolveIdempotencyReplay,
  resolveProvisionalMappingBinding,
  validateInteractionAnswer,
  validateInteractionAnswerAgainstRequest,
  validateInteractionAnswerResult,
  validateInteractionAnswerResultReplay,
  validateInteractionHandoff,
  validateInteractionHandoffTransition,
  validateInteractionRequest,
  validateInteractionTransition,
  validateNormalizedEvent,
  validateDeliveryCommand,
  validateInboundEnvelope,
} from '../../contracts/public/index.js';
import { bindPermissionToAcceptedTurnInTransaction } from '../permissions/permission-service.js';
import { createDeliveryLaneKey } from './delivery-lane-key.js';
import {
  buildInitialDeliveryCommand,
  buildLifecycleEvent,
  encodeConversationKey,
  INBOUND_ENVELOPE_KNOWN_FIELDS,
} from './inbound-acceptance.js';
import { initializeMainProjection, stageMainProjection } from './main-projection.js';
import { initializeRuntimePersistence } from './schema.js';
import { createRetentionCleanup } from './retention-cleanup.js';
import {
  createWorkspaceLeaseCoordinator,
  normalizeWorkspaceRoot,
} from '../workspace/lease-coordinator.js';
import { BLOCKING_UPGRADE_STATES_SQL } from '../migration/upgrade-state.js';

const RELEASE_FENCE_PREDICATE_SQL = `
  (
    NOT EXISTS (
      SELECT 1 FROM runtime_executor_service_instances AS revoked_service
      WHERE revoked_service.service_instance_id = ?
        AND revoked_service.revoked_at IS NOT NULL
    )
    AND (
      NOT EXISTS (
      SELECT 1 FROM runtime_active_release_fences AS fence
      WHERE fence.scope_kind = 'installation'
        OR (fence.scope_kind = 'bot' AND fence.bot_id = conversation.bot_id)
      )
      OR EXISTS (
      SELECT 1
      FROM runtime_active_release_fences AS fence
      JOIN runtime_executor_service_instances AS service
        ON service.service_instance_id = ?
       AND service.upgrade_id = fence.upgrade_id
       AND service.release_ref = fence.release_ref
       AND service.revoked_at IS NULL
      WHERE fence.scope_key = (
        SELECT applicable.scope_key
        FROM runtime_active_release_fences AS applicable
        WHERE applicable.scope_kind = 'installation'
          OR (applicable.scope_kind = 'bot' AND applicable.bot_id = conversation.bot_id)
        ORDER BY CASE applicable.scope_kind WHEN 'bot' THEN 0 ELSE 1 END,
          applicable.generation DESC
        LIMIT 1
      )
      )
    )
  )
`;

const CANONICAL_TRANSITIONS = Object.freeze({
  queued: Object.freeze(['starting']),
  starting: Object.freeze(['running', 'recovering', 'stopped', 'failed']),
  running: Object.freeze([
    'waiting_user',
    'redirecting',
    'recovering',
    'completed',
    'stopped',
    'failed',
  ]),
  redirecting: Object.freeze(['interrupted']),
  waiting_user: Object.freeze(['running', 'recovering', 'stopped', 'timed_out', 'failed']),
  recovering: Object.freeze([
    'starting',
    'running',
    'waiting_user',
    'stopped',
    'failed',
    'interrupted',
    'timed_out',
  ]),
});

const TERMINAL_STATES = new Set([
  'completed',
  'stopped',
  'cancelled',
  'failed',
  'interrupted',
  'timed_out',
]);
const WORKSPACE_NOTIFICATION_BARRIER_CODES = new Set([
  'provider_background_work_unknown',
  'workspace_lease_expired',
  'workspace_lease_orphaned',
]);

const ANSWER_CONFLICT_CODES = new Set([
  'idempotency_conflict',
  'interaction_already_answered',
  'interaction_out_of_order',
  'stale_attempt',
  'turn_terminal',
  'version_conflict',
]);

const BLOCKING_INTERACTION_STATES_SQL = [
  'pending',
  'answer_committed',
  'answer_delivering',
  'delivery_unknown',
].map((state) => `'${state}'`).join(', ');

export class ExecutorPersistenceError extends Error {
  constructor(code, message) {
    super(message);
    this.name = 'ExecutorPersistenceError';
    this.code = code;
  }
}

function conflict(code, message) {
  throw new ExecutorPersistenceError(code, message);
}

function translateContractError(error) {
  if (error instanceof ContractKernelError) {
    conflict(error.contractError.code, error.contractError.user_message);
  }
  throw error;
}

function validateAndHashInteractionAnswer(answer, occurredAt) {
  try {
    const validated = validateInteractionAnswer(answer, { occurredAt });
    return createPayloadHash(answer, {
      scope: 'interaction',
      knownFields: INTERACTION_ANSWER_SCHEMA_V1.requiredFields,
      extensionFields: Object.keys(validated.extensions),
    });
  } catch (error) {
    translateContractError(error);
  }
}

function requestScopeFromTurn(turn) {
  const envelope = JSON.parse(turn.envelope_json);
  return {
    region: envelope.region,
    tenant_id: envelope.tenant_id,
    channel: envelope.channel,
    bot_id: envelope.bot_id,
    chat_id: envelope.chat_id,
    native_thread_or_topic_id: envelope.native_thread_or_topic_id,
  };
}

function assertMainCardReplyMapping(database, answer, request, replyToMessageId) {
  if (answer.source !== 'main_card_reply') return;
  if (typeof replyToMessageId !== 'string' || replyToMessageId.length === 0) {
    conflict('mapping_missing', 'A main-card reply requires its trusted reply-to message ID.');
  }
  const context = answer.source_context;
  const mapping = database.prepare(`
    SELECT conversation_id, turn_id, lineage_id, binding_state
    FROM runtime_message_mappings
    WHERE region = ? AND tenant_id = ? AND channel = ? AND bot_id = ?
      AND platform_message_id = ?
  `).get(
    context.region,
    context.tenant_id,
    context.channel,
    context.bot_id,
    replyToMessageId,
  );
  const isPendingRecoveryReply = request.parent_type === 'recovery_control'
    && request.lineage_id === null
    && mapping?.binding_state === 'pending'
    && mapping?.lineage_id === null
    && mapping?.conversation_id === request.conversation_id
    && mapping?.turn_id === request.turn_id;
  if (isPendingRecoveryReply) return;
  if (
    !mapping
    || mapping.binding_state !== 'bound'
    || mapping.conversation_id !== request.conversation_id
    || mapping.turn_id !== request.turn_id
    || mapping.lineage_id !== request.lineage_id
  ) {
    conflict('mapping_missing', 'The replied-to message is not bound to this interaction turn.');
  }
}

function sameFence(row, fence) {
  return row.attempt_id === fence.attempt_id
    && row.attempt_no === fence.attempt_no
    && row.lease_epoch === fence.lease_epoch;
}

function loadTurn(database, turnId) {
  const turn = database.prepare(`
    SELECT
      turn.turn_id,
      turn.conversation_id,
      turn.lineage_id,
      turn.state,
      turn.turn_version,
      turn.attempt_id,
      turn.attempt_no,
      turn.lease_epoch,
      turn.provider_input_json,
      turn.redirected_from_turn_id,
      lineage.provider,
      lineage.provider_native_id,
      inbound.envelope_json
    FROM runtime_turns AS turn
    JOIN runtime_inbound_events AS inbound
      ON inbound.inbound_event_id = turn.inbound_event_id
    LEFT JOIN runtime_lineages AS lineage
      ON lineage.lineage_id = turn.lineage_id
     AND lineage.conversation_id = turn.conversation_id
    WHERE turn.turn_id = ?
  `).get(turnId);
  if (!turn) conflict('turn_not_found', `Turn ${turnId} does not exist.`);
  return turn;
}

function loadStreamState(database, turnId) {
  let state = createNormalizedEventStreamState();
  const rows = database.prepare(`
    SELECT event_json
    FROM runtime_normalized_events
    WHERE turn_id = ?
    ORDER BY event_sequence ASC
  `).all(turnId);
  for (const { event_json: eventJson } of rows) {
    state = admitNormalizedEvent(state, JSON.parse(eventJson));
  }
  return state;
}

function loadLastEvent(database, turnId) {
  return database.prepare(`
    SELECT event_id, event_sequence
    FROM runtime_normalized_events
    WHERE turn_id = ?
    ORDER BY event_sequence DESC
    LIMIT 1
  `).get(turnId);
}

function assertActiveFence(database, turn, fence, serviceInstanceId) {
  if (!sameFence(turn, fence)) {
    conflict('stale_attempt', 'The turn no longer matches this provider attempt fence.');
  }
  const lease = database.prepare(`
    SELECT lease_owner, lease_epoch, turn_id, attempt_id, attempt_no
    FROM runtime_executor_leases
    WHERE conversation_id = ?
  `).get(turn.conversation_id);
  if (
    !lease
    || lease.lease_owner !== serviceInstanceId
    || lease.turn_id !== turn.turn_id
    || !sameFence(lease, fence)
  ) {
    conflict('stale_attempt', 'The executor lease no longer matches this provider attempt fence.');
  }
}

function assertBoundProviderNativeId(turn, provider, providerNativeId) {
  if (
    turn.provider !== provider
    || turn.provider_native_id !== providerNativeId
  ) {
    conflict(
      'provider_context_invalid',
      'Provider output cannot reference a native ID before the lineage binding is durable.',
    );
  }
}

function buildEvent({
  turn,
  lastEvent,
  fence,
  provider,
  descriptor,
  occurredAt,
  generateId,
}) {
  const envelope = JSON.parse(turn.envelope_json);
  const event = {
    contract: 'zylos.normalized-event',
    contract_version: '1.0',
    event_id: generateId('event'),
    trace_id: envelope.trace_id,
    conversation_id: turn.conversation_id,
    turn_id: turn.turn_id,
    lineage_id: turn.lineage_id,
    event_sequence: lastEvent.event_sequence + 1,
    turn_version: turn.turn_version + 1,
    attempt_id: fence?.attempt_id ?? null,
    attempt_no: fence?.attempt_no ?? null,
    lease_epoch: fence?.lease_epoch ?? null,
    kind: descriptor.kind,
    phase: descriptor.phase,
    occurred_at: occurredAt,
    persisted_at: occurredAt,
    provider,
    provider_native_id: descriptor.provider_native_id ?? null,
    payload: descriptor.payload,
    causation_event_id: lastEvent.event_id,
    error: descriptor.error ?? null,
  };
  validateNormalizedEvent(event);
  return event;
}

function persistEvent(database, turn, event, generateId) {
  const streamState = loadStreamState(database, turn.turn_id);
  admitNormalizedEvent(streamState, event);
  database.prepare(`
    INSERT INTO runtime_normalized_events (
      event_id, turn_id, event_sequence, turn_version, event_json, persisted_at
    ) VALUES (?, ?, ?, ?, ?, ?)
  `).run(
    event.event_id,
    turn.turn_id,
    event.event_sequence,
    event.turn_version,
    JSON.stringify(event),
    event.persisted_at,
  );
  stageMainProjection(database, turn, event, { generateId });
}

function commitTurnEvent(database, {
  turn,
  event,
  fence,
  nextState,
  staleMessage,
  generateId,
}) {
  const updated = database.prepare(`
    UPDATE runtime_turns
    SET state = ?, turn_version = ?, committed_at = ?
    WHERE turn_id = ? AND state = ?
      AND attempt_id = ? AND attempt_no = ? AND lease_epoch = ?
  `).run(
    nextState,
    event.turn_version,
    event.persisted_at,
    turn.turn_id,
    turn.state,
    fence.attempt_id,
    fence.attempt_no,
    fence.lease_epoch,
  );
  if (updated.changes !== 1) conflict('stale_attempt', staleMessage);
  persistEvent(database, turn, event, generateId);
}

function transitionInTransaction(database, {
  turnId,
  fromState,
  toState,
  fence,
  provider,
  serviceInstanceId,
  occurredAt,
  generateId,
  reasonCode = `executor_${toState}`,
  error = null,
  requireActiveLease = true,
  retainLease = false,
}) {
  if (!CANONICAL_TRANSITIONS[fromState]?.includes(toState)) {
    conflict('illegal_transition', `Canonical transition ${fromState} -> ${toState} is not allowed.`);
  }
  const turn = loadTurn(database, turnId);
  if (turn.state !== fromState) {
    conflict(
      'illegal_transition',
      `Turn ${turnId} is ${turn.state}; expected ${fromState} before ${toState}.`,
    );
  }
  if (requireActiveLease) assertActiveFence(database, turn, fence, serviceInstanceId);
  else if (!sameFence(turn, fence)) {
    conflict('stale_attempt', 'The turn no longer matches this provider attempt fence.');
  }
  const event = buildEvent({
    turn,
    lastEvent: loadLastEvent(database, turnId),
    fence,
    provider,
    descriptor: {
      kind: 'turn_state_changed',
      phase: toState,
      payload: {
        from_state: fromState,
        to_state: toState,
        reason_code: reasonCode,
      },
      provider_native_id: turn.provider_native_id,
      error: error === null ? null : {
        ...error,
        occurred_at: error.occurred_at ?? occurredAt,
      },
    },
    occurredAt,
    generateId,
  });
  commitTurnEvent(database, {
    turn,
    event,
    fence,
    nextState: toState,
    staleMessage: 'The canonical turn transition lost its attempt fence.',
    generateId,
  });
  // Interaction timeout must retain the writer lease until the executor service
  // has proved that the suspended provider iterator stopped.
  if (TERMINAL_STATES.has(toState)) {
    const terminalQueueEntry = database.prepare(`
      UPDATE runtime_turn_queue
      SET status = ?
      WHERE turn_id = ? AND status = 'claimed'
    `).run(toState, turnId);
    if (terminalQueueEntry.changes !== 1) {
      conflict('stale_attempt', 'The canonical terminal transition lost its queue claim.');
    }
    if (toState === 'timed_out' || retainLease) return event;
    const released = database.prepare(`
      UPDATE runtime_executor_leases
      SET
        lease_owner = NULL,
        turn_id = NULL,
        attempt_id = NULL,
        attempt_no = NULL,
        lease_expires_at = NULL,
        updated_at = ?
      WHERE conversation_id = ? AND lease_owner = ? AND turn_id = ?
        AND attempt_id = ? AND attempt_no = ? AND lease_epoch = ?
    `).run(
      occurredAt,
      turn.conversation_id,
      serviceInstanceId,
      turnId,
      fence.attempt_id,
      fence.attempt_no,
      fence.lease_epoch,
    );
    if (released.changes !== 1) {
      conflict('stale_attempt', 'The canonical terminal transition lost its executor lease.');
    }
  }
  return event;
}

function transitionAttemptlessInTransaction(database, {
  turnId,
  fromState,
  toState,
  occurredAt,
  generateId,
  reasonCode,
  error = null,
}) {
  if (!CANONICAL_TRANSITIONS[fromState]?.includes(toState)) {
    conflict('illegal_transition', `Canonical transition ${fromState} -> ${toState} is not allowed.`);
  }
  const turn = loadTurn(database, turnId);
  if (
    turn.state !== fromState
    || turn.attempt_id !== null
    || turn.attempt_no !== null
    || turn.lease_epoch !== null
  ) {
    conflict('stale_attempt', 'The attemptless recovery transition lost its turn fence.');
  }
  const event = buildEvent({
    turn,
    lastEvent: loadLastEvent(database, turnId),
    fence: null,
    provider: null,
    descriptor: {
      kind: 'turn_state_changed',
      phase: toState,
      payload: {
        from_state: fromState,
        to_state: toState,
        reason_code: reasonCode,
      },
      provider_native_id: null,
      error,
    },
    occurredAt,
    generateId,
  });
  const updated = database.prepare(`
    UPDATE runtime_turns
    SET state = ?, turn_version = ?, committed_at = ?
    WHERE turn_id = ? AND state = ?
      AND attempt_id IS NULL AND attempt_no IS NULL AND lease_epoch IS NULL
  `).run(
    toState,
    event.turn_version,
    event.persisted_at,
    turn.turn_id,
    fromState,
  );
  if (updated.changes !== 1) {
    conflict('stale_attempt', 'The attemptless recovery transition changed concurrently.');
  }
  persistEvent(database, turn, event, generateId);
  return event;
}

function appendAttemptlessEventInTransaction(database, {
  turnId,
  state,
  descriptor,
  occurredAt,
  generateId,
}) {
  const turn = loadTurn(database, turnId);
  if (
    turn.state !== state
    || turn.attempt_id !== null
    || turn.attempt_no !== null
    || turn.lease_epoch !== null
  ) {
    conflict('stale_attempt', 'The attemptless recovery event lost its turn fence.');
  }
  const event = buildEvent({
    turn,
    lastEvent: loadLastEvent(database, turnId),
    fence: null,
    provider: null,
    descriptor,
    occurredAt,
    generateId,
  });
  const updated = database.prepare(`
    UPDATE runtime_turns
    SET turn_version = ?, committed_at = ?
    WHERE turn_id = ? AND state = ? AND turn_version = ?
      AND attempt_id IS NULL AND attempt_no IS NULL AND lease_epoch IS NULL
  `).run(
    event.turn_version,
    event.persisted_at,
    turn.turn_id,
    state,
    turn.turn_version,
  );
  if (updated.changes !== 1) {
    conflict('stale_attempt', 'The attemptless recovery event changed concurrently.');
  }
  persistEvent(database, turn, event, generateId);
  return event;
}

function persistInteractionLifecycleEventInTransaction(database, {
  turn,
  state,
  descriptor,
  fence,
  provider,
  occurredAt,
  generateId,
  staleMessage,
}) {
  if (turn.attempt_id === null) {
    if (turn.lineage_id === null) return { turn_version: turn.turn_version };
    return appendAttemptlessEventInTransaction(database, {
      turnId: turn.turn_id,
      state,
      descriptor,
      occurredAt,
      generateId,
    });
  }
  const event = buildEvent({
    turn,
    lastEvent: loadLastEvent(database, turn.turn_id),
    fence,
    provider,
    descriptor,
    occurredAt,
    generateId,
  });
  commitTurnEvent(database, {
    turn,
    event,
    fence,
    nextState: state,
    staleMessage,
    generateId,
  });
  return event;
}

export function createExecutorStore({
  database,
  provider,
  serviceInstanceId,
  now,
  generateId,
  leaseDurationMs = 10_000,
  residentLeaseDurationMs = 60_000,
  workspaceLeaseDurationMs = 10_000,
  interactionTimeoutMs = 10 * 60_000,
  retentionCleanupSleep,
}) {
  initializeRuntimePersistence(database);
  const workspaceLeases = createWorkspaceLeaseCoordinator({
    database,
    serviceInstanceId,
    now,
    generateId,
    leaseDurationMs: workspaceLeaseDurationMs,
  });
  const retentionCleanup = createRetentionCleanup({
    database,
    now,
    generateId,
    ...(retentionCleanupSleep === undefined ? {} : { sleep: retentionCleanupSleep }),
  });

  function assertResidentOwner(conversationId, expectedEpoch = null) {
    if (provider !== 'claude') return;
    const resident = database.prepare(`
      SELECT owner_service_instance_id, owner_epoch
      FROM runtime_executor_residents
      WHERE conversation_id = ? AND provider = 'claude'
    `).get(conversationId);
    if (!resident && expectedEpoch === null) return;
    if (
      !resident
      || resident.owner_service_instance_id !== serviceInstanceId
      || expectedEpoch !== null && resident.owner_epoch !== expectedEpoch
    ) {
      conflict('stale_attempt', 'The resident executor owner no longer matches this fence.');
    }
  }

  function assertTurnContextFence(turn, turnContext) {
    assertActiveFence(database, turn, turnContext.attempt, serviceInstanceId);
    assertResidentOwner(turn.conversation_id, turnContext.resident?.owner_epoch ?? null);
    if (turnContext.workspace !== null && turnContext.workspace !== undefined) {
      workspaceLeases.assertCurrent(turnContext.workspace);
    }
  }

  function assertTurnContextIsolationFence(turn, turnContext) {
    assertActiveFence(database, turn, turnContext.attempt, serviceInstanceId);
    assertResidentOwner(turn.conversation_id, turnContext.resident?.owner_epoch ?? null);
    if (turnContext.workspace !== null && turnContext.workspace !== undefined) {
      workspaceLeases.assertHeldOrUncertain(turnContext.workspace);
    }
  }

  function matchesInteractionHandoffDelivery({
    delivery,
    request,
    handoff,
    handoffVersion,
  }) {
    return delivery?.request?.interaction_id === request.interaction_id
      && delivery?.request?.version === request.version
      && delivery?.request?.runtime_fence?.provider_attempt_id
        === request.runtime_fence?.provider_attempt_id
      && delivery?.request?.runtime_fence?.lease_epoch === request.runtime_fence?.lease_epoch
      && delivery?.handoff_version === handoffVersion
      && delivery?.handoff?.handoff_id === handoff.handoff_id
      && delivery?.handoff?.provider_attempt_id === handoff.provider_attempt_id
      && delivery?.handoff?.lease_epoch === handoff.lease_epoch
      && delivery?.handoff?.handoff_attempt_id === handoff.handoff_attempt_id
      && delivery?.handoff?.handoff_attempt_no === handoff.handoff_attempt_no;
  }

  function assertPairedInteractionHandoffCas(
    interactionUpdate,
    handoffUpdate,
    code,
    message,
  ) {
    if (interactionUpdate.changes !== 1 || handoffUpdate.changes !== 1) {
      conflict(code, message);
    }
  }

  function rejectedInteractionAnswerResult(answer, persistenceError) {
    const interaction = typeof answer?.interaction_id === 'string'
      ? database.prepare(`
        SELECT request_json
        FROM runtime_interactions
        WHERE interaction_id = ?
      `).get(answer.interaction_id)
      : null;
    const request = interaction ? JSON.parse(interaction.request_json) : null;
    const turn = request?.turn_id === null || request === null
      ? null
      : database.prepare(`
        SELECT state, turn_version
        FROM runtime_turns
        WHERE turn_id = ?
      `).get(request.turn_id);
    let code = persistenceError.code;
    if (code === 'interaction_not_found') code = 'not_found';
    if (code === 'illegal_transition' && turn && turn.state !== 'waiting_user') {
      code = 'turn_terminal';
    }
    const category = code === 'interaction_actor_forbidden'
      ? 'authorization'
      : ANSWER_CONFLICT_CODES.has(code) || code === 'interaction_expired'
        ? 'conflict'
        : 'validation';
    const rejectedAt = now();
    const result = {
      contract: 'zylos.interaction-answer-result',
      contract_version: '1.0',
      trace_id: answer?.trace_id ?? null,
      interaction_id: answer?.interaction_id ?? null,
      answer_id: answer?.answer_id ?? null,
      idempotency_key: answer?.idempotency_key ?? null,
      status: ANSWER_CONFLICT_CODES.has(code) ? 'conflict' : 'rejected',
      interaction_state: request?.state ?? null,
      interaction_version: request?.version ?? null,
      handoff_state: 'not_applicable',
      handoff_id: null,
      turn_id: request?.turn_id ?? null,
      turn_version: turn?.turn_version ?? null,
      control_id: request?.control_id ?? null,
      error: createContractError({
        code,
        category,
        userMessage: persistenceError.message,
        occurredAt: rejectedAt,
      }),
      received_at: null,
      committed_at: null,
    };
    try {
      validateInteractionAnswerResult(result, { occurredAt: rejectedAt });
    } catch {
      throw persistenceError;
    }
    return result;
  }

  function rebuildExecutorCache() {
    const rows = database.prepare(`
      SELECT conversation_id, turn_id, status, wait_reason, wait_detail_json,
        queue_sequence, priority
      FROM runtime_turn_queue
      WHERE status IN ('queued', 'claimed')
      ORDER BY conversation_id ASC, priority DESC, queue_sequence ASC
    `).all();
    const executors = new Map();
    for (const row of rows) {
      let projection = executors.get(row.conversation_id);
      if (!projection) {
        projection = {
          conversation_id: row.conversation_id,
          active_turn_id: null,
          queued_turn_ids: [],
          wait_reason: null,
        };
        executors.set(row.conversation_id, projection);
      }
      if (row.status === 'claimed') projection.active_turn_id = row.turn_id;
      else {
        projection.queued_turn_ids.push(row.turn_id);
        if (projection.wait_reason === null && row.wait_reason !== null) {
          projection.wait_reason = row.wait_reason;
          if (row.wait_detail_json !== null) {
            projection.wait_detail = JSON.parse(row.wait_detail_json);
          }
        }
      }
    }
    return [...executors.values()];
  }

  function listPendingInteractionDeadlines() {
    return database.prepare(`
      SELECT candidate.request_json
      FROM runtime_interactions AS candidate
      WHERE candidate.state = 'pending'
        AND candidate.parent_type != 'security_control'
        AND NOT EXISTS (
          SELECT 1
          FROM runtime_interactions AS blocker
          WHERE blocker.turn_id = candidate.turn_id
            AND blocker.ordinal < candidate.ordinal
            AND blocker.state IN (${BLOCKING_INTERACTION_STATES_SQL})
        )
      ORDER BY json_extract(candidate.request_json, '$.expires_at'), candidate.interaction_id
    `).all().map(({ request_json: requestJson }) => {
      const request = JSON.parse(requestJson);
      return {
        interaction_id: request.interaction_id,
        interaction_version: request.version,
        expires_at: request.expires_at,
      };
    });
  }

  function listClaimableQueuedTurns() {
    const claimableAt = now();
    return database.prepare(`
      SELECT
        turn.turn_id,
        turn.conversation_id,
        conversation.bot_id,
        turn.created_at,
        queue.queue_sequence
      FROM runtime_turn_queue AS queue
      JOIN runtime_turns AS turn ON turn.turn_id = queue.turn_id
      JOIN runtime_conversations AS conversation
        ON conversation.conversation_id = turn.conversation_id
      WHERE queue.status = 'queued'
        AND NOT EXISTS (
          SELECT 1 FROM runtime_upgrade_runs AS upgrade
          WHERE upgrade.state IN (${BLOCKING_UPGRADE_STATES_SQL})
            AND (
              upgrade.scope_kind = 'installation'
              OR (upgrade.scope_kind = 'bot' AND upgrade.bot_id = conversation.bot_id)
            )
        )
        AND ${RELEASE_FENCE_PREDICATE_SQL}
        AND turn.lineage_id IS NOT NULL
        AND (
          turn.state = 'queued'
          OR (
            turn.state = 'recovering'
            AND (
              EXISTS (
                SELECT 1
                FROM runtime_reply_mapping_recoveries AS recovery
                WHERE recovery.turn_id = turn.turn_id
                  AND recovery.state = 'bound'
                  AND recovery.bound_lineage_id = turn.lineage_id
              )
              OR EXISTS (
                SELECT 1
                FROM runtime_provider_attempts AS provider_attempt
                WHERE provider_attempt.turn_id = turn.turn_id
                  AND provider_attempt.attempt_id = turn.attempt_id
                  AND provider_attempt.attempt_no = turn.attempt_no
                  AND provider_attempt.lease_epoch = turn.lease_epoch
                  AND provider_attempt.state = 'retry_wait'
                  AND provider_attempt.side_effect_status = 'none'
                  AND provider_attempt.next_retry_at <= ?
              )
            )
          )
        )
        AND NOT EXISTS (
          SELECT 1
          FROM runtime_turn_queue AS earlier
          WHERE earlier.conversation_id = queue.conversation_id
            AND (
              earlier.priority > queue.priority
              OR (
                earlier.priority = queue.priority
                AND earlier.queue_sequence < queue.queue_sequence
              )
            )
            AND earlier.status IN ('queued', 'claimed')
        )
        AND NOT EXISTS (
          SELECT 1
          FROM runtime_executor_leases AS lease
          WHERE lease.conversation_id = queue.conversation_id
            AND lease.lease_owner IS NOT NULL
        )
        AND NOT EXISTS (
          SELECT 1
          FROM runtime_workspace_leases AS workspace
          WHERE workspace.holder_turn_id = turn.turn_id
            AND workspace.state IN ('active', 'uncertain')
        )
      ORDER BY turn.created_at ASC, turn.conversation_id ASC, queue.queue_sequence ASC
    `).all(serviceInstanceId, serviceInstanceId, claimableAt);
  }

  function claimNextReplyMappingRecoveryNotice() {
    const claim = database.transaction(() => {
      const claimAt = now();
      const existing = database.prepare(`
        SELECT recovery.turn_id, recovery.notice_event_sequence,
          queue.wait_reason,
          EXISTS (
            SELECT 1
            FROM runtime_projection_snapshots AS projection
            JOIN runtime_outbox AS outbox
              ON outbox.outbox_id = projection.materialized_outbox_id
            WHERE projection.turn_id = recovery.turn_id
              AND projection.event_sequence_through >= recovery.notice_event_sequence
              AND outbox.status = 'delivered'
          ) AS notice_delivered
        FROM runtime_reply_mapping_recoveries AS recovery
        JOIN runtime_turns AS turn ON turn.turn_id = recovery.turn_id
        JOIN runtime_turn_queue AS queue ON queue.turn_id = recovery.turn_id
        JOIN runtime_conversations AS conversation
          ON conversation.conversation_id = turn.conversation_id
        WHERE recovery.state IN (
            'notice_pending',
            'native_recovery_claimed',
            'native_recovery_not_applicable'
          )
          AND (
            (recovery.state = 'notice_pending' AND turn.state = 'starting')
            OR (
              recovery.state IN ('native_recovery_claimed', 'native_recovery_not_applicable')
              AND turn.state = 'recovering'
            )
          )
          AND turn.lineage_id IS NULL
          AND turn.attempt_id IS NULL AND turn.attempt_no IS NULL
          AND turn.lease_epoch IS NULL
          AND queue.status = 'claimed'
          AND (
            recovery.state = 'notice_pending'
            OR recovery.native_recovery_claim_expires_at IS NULL
            OR recovery.native_recovery_claim_expires_at <= ?
          )
          AND NOT EXISTS (
            SELECT 1 FROM runtime_upgrade_runs AS upgrade
            WHERE upgrade.state IN (${BLOCKING_UPGRADE_STATES_SQL})
              AND (
                upgrade.scope_kind = 'installation'
                OR (upgrade.scope_kind = 'bot' AND upgrade.bot_id = conversation.bot_id)
              )
          )
          AND ${RELEASE_FENCE_PREDICATE_SQL}
        ORDER BY turn.created_at ASC, turn.conversation_id ASC,
          queue.queue_sequence ASC
        LIMIT 1
      `).get(claimAt, serviceInstanceId, serviceInstanceId);
      if (existing) {
        return {
          status: 'lineage_resolution_pending',
          turn_id: existing.turn_id,
          wait_reason: existing.wait_reason,
          notice_event_sequence: existing.notice_event_sequence,
          notice_delivered: existing.notice_delivered === 1,
        };
      }

      const candidate = database.prepare(`
        SELECT recovery.turn_id
        FROM runtime_reply_mapping_recoveries AS recovery
        JOIN runtime_turns AS turn ON turn.turn_id = recovery.turn_id
        JOIN runtime_turn_queue AS queue ON queue.turn_id = recovery.turn_id
        JOIN runtime_conversations AS conversation
          ON conversation.conversation_id = turn.conversation_id
        WHERE recovery.state = 'queued'
          AND turn.state = 'queued' AND turn.lineage_id IS NULL
          AND turn.attempt_id IS NULL AND turn.attempt_no IS NULL
          AND turn.lease_epoch IS NULL
          AND queue.status = 'queued'
          AND NOT EXISTS (
            SELECT 1
            FROM runtime_turn_queue AS earlier
            WHERE earlier.conversation_id = queue.conversation_id
              AND (
                earlier.priority > queue.priority
                OR (
                  earlier.priority = queue.priority
                  AND earlier.queue_sequence < queue.queue_sequence
                )
              )
              AND earlier.status IN ('queued', 'claimed')
          )
          AND NOT EXISTS (
            SELECT 1
            FROM runtime_executor_leases AS lease
            WHERE lease.conversation_id = queue.conversation_id
              AND lease.lease_owner IS NOT NULL
          )
          AND NOT EXISTS (
            SELECT 1 FROM runtime_upgrade_runs AS upgrade
            WHERE upgrade.state IN (${BLOCKING_UPGRADE_STATES_SQL})
              AND (
                upgrade.scope_kind = 'installation'
                OR (upgrade.scope_kind = 'bot' AND upgrade.bot_id = conversation.bot_id)
              )
          )
          AND ${RELEASE_FENCE_PREDICATE_SQL}
        ORDER BY turn.created_at ASC, turn.conversation_id ASC,
          queue.priority DESC, queue.queue_sequence ASC
        LIMIT 1
      `).get(serviceInstanceId, serviceInstanceId);
      if (!candidate) return null;

      const claimedAt = now();
      const queueUpdate = database.prepare(`
        UPDATE runtime_turn_queue
        SET status = 'claimed', wait_reason = 'reply_mapping_notice_delivery'
        WHERE turn_id = ? AND status = 'queued'
          AND wait_reason = 'lineage_resolution_pending'
      `).run(candidate.turn_id);
      if (queueUpdate.changes !== 1) {
        conflict('stale_attempt', 'The reply-mapping recovery queue claim changed concurrently.');
      }
      const event = transitionAttemptlessInTransaction(database, {
        turnId: candidate.turn_id,
        fromState: 'queued',
        toState: 'starting',
        occurredAt: claimedAt,
        generateId,
        reasonCode: 'reply_mapping_recovery_notice',
      });
      const recoveryUpdate = database.prepare(`
        UPDATE runtime_reply_mapping_recoveries
        SET state = 'notice_pending', notice_event_sequence = ?, updated_at = ?
        WHERE turn_id = ? AND state = 'queued' AND notice_event_sequence IS NULL
      `).run(event.event_sequence, claimedAt, candidate.turn_id);
      if (recoveryUpdate.changes !== 1) {
        conflict('stale_attempt', 'The reply-mapping notice fence changed concurrently.');
      }
      return {
        status: 'lineage_resolution_pending',
        turn_id: candidate.turn_id,
        wait_reason: 'reply_mapping_notice_delivery',
        notice_event_sequence: event.event_sequence,
        notice_delivered: false,
      };
    });
    return claim.immediate();
  }

  function beginReplyMappingRecovery(turnId, { allowNativeRecovery }) {
    const begin = database.transaction(() => {
      const recovery = database.prepare(`
        SELECT recovery.*, turn.conversation_id, turn.state AS turn_state,
          turn.lineage_id AS turn_lineage_id, queue.status AS queue_status,
          queue.wait_reason,
          candidate.provider AS candidate_provider,
          candidate.provider_native_id AS candidate_provider_native_id
        FROM runtime_reply_mapping_recoveries AS recovery
        JOIN runtime_turns AS turn ON turn.turn_id = recovery.turn_id
        JOIN runtime_turn_queue AS queue ON queue.turn_id = recovery.turn_id
        LEFT JOIN runtime_lineages AS candidate
          ON candidate.lineage_id = recovery.candidate_lineage_id
         AND candidate.conversation_id = turn.conversation_id
        WHERE recovery.turn_id = ?
      `).get(turnId);
      if (!recovery) conflict('mapping_missing', `Reply-mapping recovery ${turnId} does not exist.`);
      if (recovery.state === 'bound') {
        return {
          status: 'bound',
          recovery_id: recovery.recovery_id,
          turn_id: recovery.turn_id,
          lineage_id: recovery.bound_lineage_id,
        };
      }
      if (recovery.state === 'native_recovery_claimed') {
        const claimedAt = now();
        if (
          recovery.native_recovery_owner_service_instance_id !== null
          && recovery.native_recovery_claim_expires_at !== null
          && recovery.native_recovery_claim_expires_at > claimedAt
        ) {
          return {
            status: 'native_recovery_in_flight',
            recovery_id: recovery.recovery_id,
            turn_id: recovery.turn_id,
            reason: recovery.reason,
          };
        }
        const takeover = database.prepare(`
          UPDATE runtime_reply_mapping_recoveries
          SET native_recovery_owner_service_instance_id = ?,
            native_recovery_claim_expires_at = ?, updated_at = ?
          WHERE recovery_id = ? AND state = 'native_recovery_claimed'
            AND native_recovery_attempt_id = ?
            AND (
              native_recovery_claim_expires_at IS NULL
              OR native_recovery_claim_expires_at <= ?
            )
        `).run(
          serviceInstanceId,
          residentOwnerExpiresAt(claimedAt),
          claimedAt,
          recovery.recovery_id,
          recovery.native_recovery_attempt_id,
          claimedAt,
        );
        if (takeover.changes !== 1) {
          conflict('stale_attempt', 'The native recovery ownership fence changed concurrently.');
        }
        return {
          status: 'native_recovery_lost',
          recovery_id: recovery.recovery_id,
          turn_id: recovery.turn_id,
          reason: recovery.reason,
          candidate_lineage_id: recovery.candidate_lineage_id,
          native_recovery_attempt_id: recovery.native_recovery_attempt_id,
          native_recovery_attempt_no: 1,
        };
      }
      if (recovery.state === 'native_recovery_not_applicable') {
        const claimedAt = now();
        if (
          recovery.native_recovery_owner_service_instance_id !== null
          && recovery.native_recovery_claim_expires_at !== null
          && recovery.native_recovery_claim_expires_at > claimedAt
        ) {
          return {
            status: 'native_recovery_in_flight',
            recovery_id: recovery.recovery_id,
            turn_id: recovery.turn_id,
            reason: recovery.reason,
          };
        }
        const takeover = database.prepare(`
          UPDATE runtime_reply_mapping_recoveries
          SET native_recovery_owner_service_instance_id = ?,
            native_recovery_claim_expires_at = ?, updated_at = ?
          WHERE recovery_id = ? AND state = 'native_recovery_not_applicable'
            AND (
              native_recovery_claim_expires_at IS NULL
              OR native_recovery_claim_expires_at <= ?
            )
        `).run(
          serviceInstanceId,
          residentOwnerExpiresAt(claimedAt),
          claimedAt,
          recovery.recovery_id,
          claimedAt,
        );
        if (takeover.changes !== 1) {
          conflict('stale_attempt', 'The fallback recovery ownership fence changed concurrently.');
        }
        return {
          status: 'native_recovery_not_applicable',
          recovery_id: recovery.recovery_id,
          turn_id: recovery.turn_id,
          reason: recovery.reason,
          candidate_lineage_id: recovery.candidate_lineage_id,
          side_effect_status: recovery.side_effect_status,
        };
      }
      if (recovery.state !== 'notice_pending') {
        conflict('illegal_transition', `Reply-mapping recovery is ${recovery.state}.`);
      }
      const delivered = database.prepare(`
        SELECT projection.materialized_outbox_id AS outbox_id
        FROM runtime_projection_snapshots AS projection
        JOIN runtime_outbox AS outbox
          ON outbox.outbox_id = projection.materialized_outbox_id
        WHERE projection.turn_id = ?
          AND projection.event_sequence_through >= ?
          AND outbox.status = 'delivered'
        ORDER BY projection.event_sequence_through ASC
        LIMIT 1
      `).get(recovery.turn_id, recovery.notice_event_sequence);
      if (!delivered) {
        conflict(
          'notification_pending',
          'Reply-mapping recovery must wait for a delivered user notice.',
        );
      }
      if (
        recovery.turn_state !== 'starting'
        || recovery.turn_lineage_id !== null
        || recovery.queue_status !== 'claimed'
      ) {
        conflict('stale_attempt', 'The delivered reply-mapping notice lost its turn fence.');
      }
      const occurredAt = now();
      transitionAttemptlessInTransaction(database, {
        turnId: recovery.turn_id,
        fromState: 'starting',
        toState: 'recovering',
        occurredAt,
        generateId,
        reasonCode: 'reply_mapping_recovery_started',
      });
      if (recovery.side_effect_status === 'unknown') {
        const turn = loadTurn(database, recovery.turn_id);
        const envelope = JSON.parse(turn.envelope_json);
        if (envelope.actor?.authenticated !== true) {
          conflict('authorization_denied', 'Recovery decisions require an authenticated actor.');
        }
        const interaction = {
          contract: 'zylos.interaction-request',
          contract_version: '1.0',
          trace_id: envelope.trace_id,
          interaction_id: generateId('interaction'),
          conversation_id: recovery.conversation_id,
          turn_id: recovery.turn_id,
          lineage_id: null,
          control_id: recovery.recovery_id,
          parent_type: 'recovery_control',
          tool_use_id: null,
          ordinal: 1,
          kind: 'recovery_decision',
          prompt: 'Associated work may have unknown side effects. Continue with a new recovery lineage?',
          choices: [
            { choice_id: 'approve', label: 'Continue recovery' },
            { choice_id: 'deny', label: 'Stop recovery' },
          ],
          authorized_subjects: [{
            type: 'actor',
            actor_id: envelope.actor.actor_id,
          }],
          allowed_sources: [
            'main_card_reply',
            ...(['feishu', 'lark'].includes(envelope.channel) ? ['card_action'] : []),
          ],
          runtime_fence: null,
          state: 'pending',
          version: 1,
          handoff_state: 'not_started',
          created_at: occurredAt,
          expires_at: new Date(
            Date.parse(occurredAt) + interactionTimeoutMs,
          ).toISOString(),
          card_delivery_id: null,
        };
        validateInteractionRequest(interaction, { occurredAt });
        database.prepare(`
          INSERT INTO runtime_interactions (
            interaction_id, conversation_id, turn_id, lineage_id, parent_type, parent_id,
            ordinal, state, version, handoff_state, handoff_version, request_json,
            created_at, updated_at
          ) VALUES (?, ?, ?, NULL, 'recovery_control', ?, 1, 'pending', 1,
            'not_started', NULL, ?, ?, ?)
        `).run(
          interaction.interaction_id,
          interaction.conversation_id,
          interaction.turn_id,
          interaction.control_id,
          JSON.stringify(interaction),
          occurredAt,
          occurredAt,
        );
        appendAttemptlessEventInTransaction(database, {
          turnId: recovery.turn_id,
          state: 'recovering',
          occurredAt,
          generateId,
          descriptor: {
            kind: 'recovery_waiting_decision',
            phase: 'recovering',
            payload: {
              recovery_id: recovery.recovery_id,
              recovery_of_turn_id: null,
              recovery_of_lineage_id: recovery.candidate_lineage_id,
              side_effect_status: 'unknown',
              interaction_id: interaction.interaction_id,
              ordinal: interaction.ordinal,
              interaction_version: interaction.version,
              handoff_version: null,
              kind: interaction.kind,
              prompt: interaction.prompt,
              choices: structuredClone(interaction.choices),
              allowed_sources: [...interaction.allowed_sources],
            },
            error: createContractError({
              code: 'side_effect_unknown',
              category: 'provider',
              retryable: false,
              sideEffectStatus: 'unknown',
              userMessage: 'Recovery is waiting for an authorized decision.',
              occurredAt,
            }),
          },
        });
        database.prepare(`
          UPDATE runtime_turn_queue
          SET wait_reason = 'reply_mapping_recovery_decision'
          WHERE turn_id = ? AND status = 'claimed'
        `).run(recovery.turn_id);
        database.prepare(`
          UPDATE runtime_reply_mapping_recoveries
          SET state = 'waiting_decision', updated_at = ?
          WHERE recovery_id = ? AND state = 'notice_pending'
        `).run(occurredAt, recovery.recovery_id);
        return {
          status: 'waiting_decision',
          recovery_id: recovery.recovery_id,
          turn_id: recovery.turn_id,
          reason: recovery.reason,
          side_effect_status: 'unknown',
          interaction_id: interaction.interaction_id,
        };
      }

      const canAttemptNative = allowNativeRecovery === true
        && recovery.candidate_lineage_id !== null
        && recovery.candidate_provider === provider
        && recovery.candidate_provider_native_id !== null;
      if (!canAttemptNative) {
        const claimed = database.prepare(`
          UPDATE runtime_reply_mapping_recoveries
          SET state = 'native_recovery_not_applicable',
            native_recovery_status = 'not_applicable',
            native_recovery_owner_service_instance_id = ?,
            native_recovery_claim_expires_at = ?, updated_at = ?
          WHERE recovery_id = ? AND state = 'notice_pending'
            AND native_recovery_attempt_count = 0
        `).run(
          serviceInstanceId,
          residentOwnerExpiresAt(occurredAt),
          occurredAt,
          recovery.recovery_id,
        );
        if (claimed.changes !== 1) {
          conflict('version_conflict', 'The fallback recovery claim changed concurrently.');
        }
        database.prepare(`
          UPDATE runtime_turn_queue
          SET wait_reason = 'reply_mapping_recovery_binding'
          WHERE turn_id = ? AND status = 'claimed'
        `).run(recovery.turn_id);
        return {
          status: 'native_recovery_not_applicable',
          recovery_id: recovery.recovery_id,
          turn_id: recovery.turn_id,
          reason: recovery.reason,
          candidate_lineage_id: recovery.candidate_lineage_id,
          side_effect_status: recovery.side_effect_status,
        };
      }

      const nativeRecoveryAttemptId = generateId('native-recovery-attempt');
      const claimed = database.prepare(`
        UPDATE runtime_reply_mapping_recoveries
        SET state = 'native_recovery_claimed',
          native_recovery_status = 'claimed',
          native_recovery_attempt_count = 1,
          native_recovery_attempt_id = ?,
          native_recovery_owner_service_instance_id = ?,
          native_recovery_claim_expires_at = ?, updated_at = ?
        WHERE recovery_id = ? AND state = 'notice_pending'
          AND native_recovery_attempt_count = 0
      `).run(
        nativeRecoveryAttemptId,
        serviceInstanceId,
        residentOwnerExpiresAt(occurredAt),
        occurredAt,
        recovery.recovery_id,
      );
      if (claimed.changes !== 1) {
        conflict('version_conflict', 'The native reply-mapping recovery attempt was already claimed.');
      }
      database.prepare(`
        UPDATE runtime_turn_queue
        SET wait_reason = 'reply_mapping_native_recovery'
        WHERE turn_id = ? AND status = 'claimed'
      `).run(recovery.turn_id);
      return {
        status: 'native_recovery_claimed',
        recovery_id: recovery.recovery_id,
        turn_id: recovery.turn_id,
        reason: recovery.reason,
        side_effect_status: recovery.side_effect_status,
        native_recovery_attempt_id: nativeRecoveryAttemptId,
        native_recovery_attempt_no: 1,
        candidate: {
          lineage_id: recovery.candidate_lineage_id,
          provider: recovery.candidate_provider,
          provider_native_id: recovery.candidate_provider_native_id,
        },
      };
    });
    return begin.immediate();
  }

  function completeReplyMappingRecovery(claim, nativeResult) {
    const complete = database.transaction(() => {
      const completedAt = now();
      const recovery = database.prepare(`
        SELECT recovery.*, turn.conversation_id, turn.state AS turn_state,
          turn.lineage_id AS turn_lineage_id
        FROM runtime_reply_mapping_recoveries AS recovery
        JOIN runtime_turns AS turn ON turn.turn_id = recovery.turn_id
        WHERE recovery.recovery_id = ? AND recovery.turn_id = ?
      `).get(claim?.recovery_id, claim?.turn_id);
      if (!recovery) conflict('mapping_missing', 'The reply-mapping recovery claim is stale.');
      if (recovery.state === 'bound') {
        if (claim?.lineage_id === recovery.bound_lineage_id || claim?.lineage_id === undefined) {
          return {
            status: 'duplicate',
            turn_id: recovery.turn_id,
            lineage_id: recovery.bound_lineage_id,
          };
        }
        conflict('version_conflict', 'A bound reply mapping cannot change lineage.');
      }
      if (
        ['rejected', 'failed'].includes(recovery.state)
        && ['cancelled', 'stopped', 'failed'].includes(recovery.turn_state)
      ) {
        return {
          status: 'stopped',
          recovery_id: recovery.recovery_id,
          turn_id: recovery.turn_id,
        };
      }
      if (
        !['native_recovery_claimed', 'native_recovery_not_applicable'].includes(recovery.state)
        || recovery.turn_state !== 'recovering'
        || recovery.turn_lineage_id !== null
      ) {
        conflict('stale_attempt', 'The reply-mapping recovery lost its binding fence.');
      }

      if (recovery.native_recovery_owner_service_instance_id !== serviceInstanceId) {
        conflict('stale_attempt', 'The reply-mapping recovery owner no longer matches this fence.');
      }
      if (
        recovery.native_recovery_claim_expires_at === null
        || recovery.native_recovery_claim_expires_at <= completedAt
      ) {
        conflict('stale_attempt', 'The reply-mapping recovery claim lease expired.');
      }
      if (
        claim?.recovery_id !== recovery.recovery_id
        || claim?.turn_id !== recovery.turn_id
      ) {
        conflict('stale_attempt', 'The reply-mapping recovery claim identity is stale.');
      }

      let nativeRecovered = false;
      if (recovery.state === 'native_recovery_claimed') {
        if (
          claim?.native_recovery_attempt_id !== recovery.native_recovery_attempt_id
          || claim?.native_recovery_attempt_no !== 1
        ) {
          conflict('stale_attempt', 'The native recovery claim attempt is stale.');
        }
        if (!['recovered', 'failed', 'lost'].includes(nativeResult?.status)) {
          conflict('provider_context_invalid', 'Native recovery returned an invalid terminal status.');
        }
        if (
          nativeResult.recovery_id !== recovery.recovery_id
          || nativeResult.native_recovery_attempt_id !== recovery.native_recovery_attempt_id
          || nativeResult.native_recovery_attempt_no !== 1
        ) {
          conflict('provider_context_invalid', 'Native recovery returned a mismatched attempt fence.');
        }
        if (nativeResult.status === 'recovered') {
          if (
            nativeResult.lineage_id !== recovery.candidate_lineage_id
            || nativeResult.provider !== provider
            || typeof nativeResult.provider_native_id !== 'string'
            || nativeResult.provider_native_id.length === 0
            || nativeResult.side_effect_status !== 'none'
          ) {
            conflict('provider_context_invalid', 'Native recovery returned a different lineage identity.');
          }
          nativeRecovered = true;
        }
      } else {
        if (!['not_applicable', 'authorized_fallback'].includes(nativeResult?.status)) {
          conflict('provider_context_invalid', 'Fallback recovery returned an invalid terminal status.');
        }
        if (nativeResult.recovery_id !== recovery.recovery_id) {
          conflict('provider_context_invalid', 'Fallback recovery returned a mismatched recovery identity.');
        }
      }

      let lineageId;
      let recoveryProviderInput = null;
      if (nativeRecovered) {
        const candidate = database.prepare(`
          SELECT lineage_id, provider, provider_native_id
          FROM runtime_lineages
          WHERE lineage_id = ? AND conversation_id = ?
        `).get(recovery.candidate_lineage_id, recovery.conversation_id);
        if (
          !candidate
          || candidate.provider !== nativeResult.provider
          || candidate.provider_native_id !== nativeResult.provider_native_id
        ) {
          conflict('provider_context_invalid', 'Native recovery returned a different lineage identity.');
        }
        database.prepare(`
          UPDATE runtime_lineages
          SET provider_native_state = 'valid'
          WHERE lineage_id = ? AND conversation_id = ?
            AND provider = ? AND provider_native_id = ?
        `).run(
          candidate.lineage_id,
          recovery.conversation_id,
          nativeResult.provider,
          nativeResult.provider_native_id,
        );
        lineageId = candidate.lineage_id;
      } else {
        lineageId = generateId('lineage');
        database.prepare(`
          INSERT INTO runtime_lineages (
            lineage_id, conversation_id, lineage_kind, is_default, created_at,
            recovery_of_lineage_id
          ) VALUES (?, ?, 'recovery', 0, ?, ?)
        `).run(
          lineageId,
          recovery.conversation_id,
          completedAt,
          recovery.candidate_lineage_id,
        );
        const recoveryTurn = database.prepare(`
          SELECT turn.queue_sequence, inbound.envelope_json
          FROM runtime_turns AS turn
          JOIN runtime_inbound_events AS inbound
            ON inbound.inbound_event_id = turn.inbound_event_id
          WHERE turn.turn_id = ? AND turn.conversation_id = ?
        `).get(recovery.turn_id, recovery.conversation_id);
        const currentEnvelope = JSON.parse(recoveryTurn.envelope_json);
        const priorRows = database.prepare(`
          SELECT inbound.envelope_json
          FROM runtime_turns AS turn
          JOIN runtime_inbound_events AS inbound
            ON inbound.inbound_event_id = turn.inbound_event_id
          WHERE turn.conversation_id = ? AND turn.queue_sequence < ?
          ORDER BY turn.queue_sequence DESC
          LIMIT 6
        `).all(recovery.conversation_id, recoveryTurn.queue_sequence).reverse();
        const priorTexts = priorRows
          .map(({ envelope_json: envelopeJson }) => JSON.parse(envelopeJson).content?.text)
          .filter((text) => typeof text === 'string' && text.length > 0);
        const currentText = currentEnvelope.content?.text ?? '';
        const handoffText = [
          '[Zylos recovery handoff]',
          'The native provider lineage could not be recovered uniquely. Continue in this new lineage.',
          'Existing Zylos memory and runtime instructions remain authoritative.',
          ...(priorTexts.length > 0
            ? ['Recent durable C4 context:', ...priorTexts.map((text) => `- ${text}`)]
            : []),
          'Current user request:',
          currentText,
        ].join('\n').slice(0, 16_000);
        recoveryProviderInput = {
          kind: 'text',
          text: handoffText,
          attachments: Array.isArray(currentEnvelope.content?.attachments)
            ? structuredClone(currentEnvelope.content.attachments)
            : [],
        };
      }

      const lane = database.prepare(`
        SELECT lane_key, mapping_json
        FROM runtime_delivery_lanes
        WHERE turn_id = ?
      `).get(recovery.turn_id);
      if (!lane) conflict('mapping_missing', 'The provisional delivery lane does not exist.');
      const currentLaneMapping = JSON.parse(lane.mapping_json);
      const binding = resolveProvisionalMappingBinding(currentLaneMapping, {
        authority: 'core',
        mapping_id: currentLaneMapping.mapping_id,
        expected_mapping_version: currentLaneMapping.mapping_version,
        lineage_id: lineageId,
      }, { occurredAt: completedAt });
      if (binding.status !== 'bound') {
        conflict(binding.error?.code ?? 'version_conflict', binding.error?.user_message ?? 'Mapping binding failed.');
      }

      const messageRows = database.prepare(`
        SELECT mapping_id, conversation_id, turn_id, lineage_id,
          binding_state, mapping_version, reason
        FROM runtime_message_mappings
        WHERE turn_id = ? AND binding_state = 'pending' AND lineage_id IS NULL
      `).all(recovery.turn_id);
      if (messageRows.length === 0) {
        conflict('notification_pending', 'No delivered provisional mapping is available to bind.');
      }
      for (const mapping of messageRows) {
        const mapped = resolveProvisionalMappingBinding(mapping, {
          authority: 'core',
          mapping_id: mapping.mapping_id,
          expected_mapping_version: mapping.mapping_version,
          lineage_id: lineageId,
        }, { occurredAt: completedAt });
        if (mapped.status !== 'bound') {
          conflict(mapped.error?.code ?? 'version_conflict', mapped.error?.user_message ?? 'Mapping binding failed.');
        }
        const updated = database.prepare(`
          UPDATE runtime_message_mappings
          SET lineage_id = ?, binding_state = 'bound', mapping_version = ?
          WHERE mapping_id = ? AND lineage_id IS NULL
            AND binding_state = 'pending' AND mapping_version = ?
        `).run(
          lineageId,
          mapped.mapping.mapping_version,
          mapping.mapping_id,
          mapping.mapping_version,
        );
        if (updated.changes !== 1) {
          conflict('version_conflict', 'A provisional platform mapping changed concurrently.');
        }
      }

      const turnUpdate = database.prepare(`
        UPDATE runtime_turns
        SET lineage_id = ?, provider_input_json = ?, committed_at = ?
        WHERE turn_id = ? AND state = 'recovering' AND lineage_id IS NULL
          AND provider_input_json IS NULL
          AND attempt_id IS NULL AND attempt_no IS NULL AND lease_epoch IS NULL
      `).run(
        lineageId,
        recoveryProviderInput === null ? null : JSON.stringify(recoveryProviderInput),
        completedAt,
        recovery.turn_id,
      );
      const laneUpdate = database.prepare(`
        UPDATE runtime_delivery_lanes
        SET mapping_json = ?, updated_at = ?
        WHERE lane_key = ? AND mapping_json = ?
      `).run(
        JSON.stringify(binding.mapping),
        completedAt,
        lane.lane_key,
        lane.mapping_json,
      );
      if (turnUpdate.changes !== 1 || laneUpdate.changes !== 1) {
        conflict('version_conflict', 'The provisional turn or delivery projection changed concurrently.');
      }

      appendAttemptlessEventInTransaction(database, {
        turnId: recovery.turn_id,
        state: 'recovering',
        occurredAt: completedAt,
        generateId,
        descriptor: {
          kind: 'recovery_finished',
          phase: 'recovering',
          payload: {
            recovery_id: recovery.recovery_id,
            recovery_of_turn_id: null,
            recovery_of_lineage_id: recovery.candidate_lineage_id,
            side_effect_status: recovery.side_effect_status,
          },
        },
      });

      const pendingCommands = database.prepare(`
        SELECT outbox_id, command_json
        FROM runtime_outbox
        WHERE turn_id = ? AND status IN ('pending', 'retry_wait')
      `).all(recovery.turn_id);
      for (const row of pendingCommands) {
        const command = JSON.parse(row.command_json);
        if (command.mapping?.binding_state !== 'pending') continue;
        const commandBinding = resolveProvisionalMappingBinding(command.mapping, {
          authority: 'core',
          mapping_id: command.mapping.mapping_id,
          expected_mapping_version: command.mapping.mapping_version,
          lineage_id: lineageId,
        }, { occurredAt: completedAt });
        if (commandBinding.status !== 'bound') {
          conflict(
            commandBinding.error?.code ?? 'version_conflict',
            commandBinding.error?.user_message ?? 'Pending delivery binding failed.',
          );
        }
        const nextCommand = { ...command, mapping: commandBinding.mapping };
        validateDeliveryCommand(nextCommand, { occurredAt: completedAt });
        const updated = database.prepare(`
          UPDATE runtime_outbox
          SET command_json = ?, updated_at = ?
          WHERE outbox_id = ? AND command_json = ?
            AND status IN ('pending', 'retry_wait')
        `).run(JSON.stringify(nextCommand), completedAt, row.outbox_id, row.command_json);
        if (updated.changes !== 1) {
          conflict('version_conflict', 'A pending delivery projection changed concurrently.');
        }
      }

      const recoveryUpdate = database.prepare(`
        UPDATE runtime_reply_mapping_recoveries
        SET state = 'bound', bound_lineage_id = ?,
          native_recovery_status = ?, native_recovery_result_json = ?, updated_at = ?
        WHERE recovery_id = ? AND state = ? AND bound_lineage_id IS NULL
          AND native_recovery_owner_service_instance_id = ?
          AND native_recovery_claim_expires_at > ?
      `).run(
        lineageId,
        nativeRecovered
          ? 'recovered'
          : (recovery.state === 'native_recovery_claimed'
            ? (nativeResult?.status ?? 'failed')
            : (recovery.native_recovery_status ?? 'not_applicable')),
        JSON.stringify(nativeResult ?? { status: 'not_applicable' }),
        completedAt,
        recovery.recovery_id,
        recovery.state,
        serviceInstanceId,
        completedAt,
      );
      const queueUpdate = database.prepare(`
        UPDATE runtime_turn_queue
        SET status = 'queued', wait_reason = NULL
        WHERE turn_id = ? AND status = 'claimed'
      `).run(recovery.turn_id);
      if (recoveryUpdate.changes !== 1 || queueUpdate.changes !== 1) {
        conflict('version_conflict', 'The reply-mapping binding lost its recovery queue fence.');
      }
      return {
        status: 'bound',
        recovery_id: recovery.recovery_id,
        turn_id: recovery.turn_id,
        lineage_id: lineageId,
        native_recovery_status: nativeRecovered ? 'recovered' : 'fallback_created',
      };
    });
    return complete.immediate();
  }

  function markCapacityWaitInTransaction(turnId, occurredAt) {
    const queued = database.prepare(`
      SELECT turn.turn_id, turn.conversation_id, queue.wait_reason
      FROM runtime_turns AS turn
      JOIN runtime_turn_queue AS queue ON queue.turn_id = turn.turn_id
      WHERE turn.turn_id = ? AND turn.state = 'queued' AND queue.status = 'queued'
    `).get(turnId);
    if (!queued) return null;
    const result = {
      conversation_id: queued.conversation_id,
      turn_id: queued.turn_id,
      wait_reason: 'executor_capacity',
    };
    if (queued.wait_reason === 'executor_capacity') return result;

    const turn = loadTurn(database, turnId);
    const event = buildEvent({
      turn,
      lastEvent: loadLastEvent(database, turnId),
      fence: null,
      provider: null,
      descriptor: {
        kind: 'turn_state_changed',
        phase: 'queued',
        payload: {
          from_state: 'queued',
          to_state: 'queued',
          reason_code: 'executor_capacity',
        },
      },
      occurredAt,
      generateId,
    });
    const turnUpdate = database.prepare(`
      UPDATE runtime_turns
      SET turn_version = ?, committed_at = ?
      WHERE turn_id = ? AND state = 'queued' AND turn_version = ?
    `).run(event.turn_version, occurredAt, turnId, turn.turn_version);
    const queueUpdate = database.prepare(`
      UPDATE runtime_turn_queue
      SET wait_reason = 'executor_capacity', wait_detail_json = NULL
      WHERE turn_id = ? AND status = 'queued' AND wait_reason IS NOT 'executor_capacity'
    `).run(turnId);
    if (turnUpdate.changes !== 1 || queueUpdate.changes !== 1) {
      conflict('stale_attempt', 'The capacity-wait projection changed concurrently.');
    }
    persistEvent(database, turn, event, generateId);
    return result;
  }

  function markWorkspaceWaitInTransaction(turnId, wait, occurredAt) {
    const queued = database.prepare(`
      SELECT turn.turn_id, turn.conversation_id, turn.turn_version,
        queue.wait_reason, queue.wait_detail_json
      FROM runtime_turns AS turn
      JOIN runtime_turn_queue AS queue ON queue.turn_id = turn.turn_id
      WHERE turn.turn_id = ? AND turn.state = 'queued' AND queue.status = 'queued'
    `).get(turnId);
    if (!queued) return null;
    const waitDetail = {
      workspace_root: wait.workspace_root,
      mode: wait.mode,
      conflicting_workspace_root: wait.conflicting_workspace_root,
      holder_conversation_id: wait.holder_conversation_id,
      holder_turn_id: wait.holder_turn_id,
      holder_background_work_ids: wait.holder_background_work_ids,
      recovery_required: wait.recovery_required === true,
    };
    const waitDetailJson = JSON.stringify(waitDetail);
    if (
      queued.wait_reason === 'workspace_lease'
      && queued.wait_detail_json === waitDetailJson
    ) {
      return {
        status: 'workspace_wait',
        conversation_id: queued.conversation_id,
        turn_id: queued.turn_id,
        wait_reason: 'workspace_lease',
        wait_detail: waitDetail,
      };
    }
    const turn = loadTurn(database, turnId);
    const event = buildEvent({
      turn,
      lastEvent: loadLastEvent(database, turnId),
      fence: null,
      provider: null,
      descriptor: {
        kind: 'turn_state_changed',
        phase: 'queued',
        payload: {
          from_state: 'queued',
          to_state: 'queued',
          reason_code: 'workspace_lease',
        },
      },
      occurredAt,
      generateId,
    });
    const turnUpdate = database.prepare(`
      UPDATE runtime_turns
      SET turn_version = ?, committed_at = ?
      WHERE turn_id = ? AND state = 'queued' AND turn_version = ?
    `).run(event.turn_version, occurredAt, turnId, turn.turn_version);
    const queueUpdate = database.prepare(`
      UPDATE runtime_turn_queue
      SET wait_reason = 'workspace_lease', wait_detail_json = ?
      WHERE turn_id = ? AND status = 'queued'
    `).run(waitDetailJson, turnId);
    if (turnUpdate.changes !== 1 || queueUpdate.changes !== 1) {
      conflict('stale_attempt', 'The workspace-wait projection changed concurrently.');
    }
    persistEvent(database, turn, event, generateId);
    return {
      status: 'workspace_wait',
      conversation_id: queued.conversation_id,
      turn_id: queued.turn_id,
      wait_reason: 'workspace_lease',
      wait_detail: waitDetail,
    };
  }

  function isResidentConversation(conversationId) {
    return database.prepare(`
      SELECT owner_service_instance_id, owner_epoch, owner_expires_at
      FROM runtime_executor_residents
      WHERE conversation_id = ? AND provider = 'claude'
    `).get(conversationId) ?? null;
  }

  function residentOwnerExpiresAt(ownedAt) {
    const timestamp = Date.parse(ownedAt);
    if (!Number.isFinite(timestamp)) {
      throw new TypeError('now must return an ISO timestamp for resident ownership');
    }
    return new Date(timestamp + residentLeaseDurationMs).toISOString();
  }

  function heartbeatOwnedResidents() {
    const heartbeatAt = now();
    const expiresAt = residentOwnerExpiresAt(heartbeatAt);
    const residentHeartbeat = provider === 'claude'
      ? database.prepare(`
        UPDATE runtime_executor_residents
        SET owner_expires_at = ?
        WHERE provider = 'claude' AND owner_service_instance_id = ?
      `).run(expiresAt, serviceInstanceId).changes
      : 0;
    const recoveryHeartbeat = database.prepare(`
      UPDATE runtime_reply_mapping_recoveries
      SET native_recovery_claim_expires_at = ?, updated_at = ?
      WHERE state IN ('native_recovery_claimed', 'native_recovery_not_applicable')
        AND native_recovery_owner_service_instance_id = ?
    `).run(expiresAt, heartbeatAt, serviceInstanceId).changes;
    return residentHeartbeat + recoveryHeartbeat;
  }

  function renewOwnedTurnLeases(turnContexts) {
    if (!Array.isArray(turnContexts)) {
      throw new TypeError('turnContexts must be an array');
    }
    const renew = database.transaction(() => {
      const renewedAt = now();
      const leaseExpiresAt = new Date(
        Date.parse(renewedAt) + leaseDurationMs,
      ).toISOString();
      let renewed = 0;
      const statement = database.prepare(`
        UPDATE runtime_executor_leases
        SET lease_expires_at = ?, updated_at = ?
        WHERE conversation_id = ? AND lease_owner = ? AND turn_id = ?
          AND attempt_id = ? AND attempt_no = ? AND lease_epoch = ?
          AND EXISTS (
            SELECT 1 FROM runtime_turns
            WHERE runtime_turns.turn_id = runtime_executor_leases.turn_id
              AND runtime_turns.state IN (
                'starting', 'running', 'waiting_user', 'redirecting', 'recovering'
              )
          )
      `);
      for (const context of turnContexts) {
        const fence = context?.attempt;
        if (
          typeof context?.conversation_id !== 'string'
          || typeof context?.turn_id !== 'string'
          || typeof fence?.attempt_id !== 'string'
          || !Number.isSafeInteger(fence?.attempt_no)
          || !Number.isSafeInteger(fence?.lease_epoch)
        ) {
          throw new TypeError('each turn context must carry its exact durable attempt fence');
        }
        renewed += statement.run(
          leaseExpiresAt,
          renewedAt,
          context.conversation_id,
          serviceInstanceId,
          context.turn_id,
          fence.attempt_id,
          fence.attempt_no,
          fence.lease_epoch,
        ).changes;
        database.prepare(`
          UPDATE runtime_provider_attempts
          SET last_lease_renewed_at = ?, updated_at = ?
          WHERE attempt_id = ? AND turn_id = ? AND attempt_no = ? AND lease_epoch = ?
            AND service_instance_id = ? AND executor_instance_id = ?
            AND state IN ('starting', 'running', 'recovering')
        `).run(
          renewedAt,
          renewedAt,
          fence.attempt_id,
          context.turn_id,
          fence.attempt_no,
          fence.lease_epoch,
          serviceInstanceId,
          context.executor_instance_id,
        );
      }
      return renewed;
    });
    return renew.immediate();
  }

  function reconcileExpiredResidents() {
    if (provider !== 'claude') return 0;
    const reconciledAt = now();
    const released = database.prepare(`
      DELETE FROM runtime_executor_residents
      WHERE provider = 'claude'
        AND (
          owner_service_instance_id IS NULL
          OR (
            owner_service_instance_id != ?
            AND (owner_expires_at IS NULL OR owner_expires_at <= ?)
          )
        )
        AND NOT EXISTS (
          SELECT 1
          FROM runtime_turns
          WHERE runtime_turns.conversation_id = runtime_executor_residents.conversation_id
            AND state IN (
              'queued', 'starting', 'running', 'waiting_user',
              'redirecting', 'recovering', 'retrying'
            )
        )
    `).run(serviceInstanceId, reconciledAt);
    return released.changes;
  }

  function residentCountForBot(botId) {
    return database.prepare(`
      SELECT COUNT(*) AS count
      FROM runtime_executor_residents
      WHERE bot_id = ? AND provider = 'claude'
    `).get(botId).count;
  }

  function reserveNextExecutor({
    maxResidentExecutorsPerBot,
    markCapacityWait = true,
    workspaceAccessByConversation = null,
  }) {
    const reserve = database.transaction(() => {
      reconcileExpiredResidents();
      const claimable = listClaimableQueuedTurns();
      if (claimable.length === 0) return { status: 'idle' };
      const workspaceWaits = [];
      const candidates = [];
      for (const candidate of claimable) {
        const workspaceAccess = workspaceAccessByConversation?.get(candidate.conversation_id);
        if (!workspaceAccess) {
          candidates.push(candidate);
          continue;
        }
        if (provider === 'claude') {
          const resident = isResidentConversation(candidate.conversation_id);
          const checkedAt = now();
          const hasCapacity = resident?.owner_service_instance_id === serviceInstanceId
            || (resident && (!resident.owner_expires_at || resident.owner_expires_at <= checkedAt))
            || (!resident && residentCountForBot(candidate.bot_id) < maxResidentExecutorsPerBot);
          if (!hasCapacity) {
            candidates.push(candidate);
            continue;
          }
        }
        const availability = workspaceLeases.inspect(workspaceAccess);
        if (availability.status === 'wait') {
          const wait = markWorkspaceWaitInTransaction(candidate.turn_id, availability, now());
          if (wait) workspaceWaits.push(wait);
        } else {
          candidates.push(candidate);
        }
      }
      if (candidates.length === 0) {
        return workspaceWaits[0] ?? { status: 'idle' };
      }
      if (provider !== 'claude') {
        const selected = candidates[0];
        const workspaceAccess = workspaceAccessByConversation?.get(selected.conversation_id);
        const workspace = workspaceAccess ? workspaceLeases.acquire({
          workspace_root: workspaceAccess.workspace_root,
          mode: workspaceAccess.mode,
          holder_conversation_id: selected.conversation_id,
          holder_turn_id: selected.turn_id,
        }) : null;
        if (workspace?.status === 'wait') {
          return markWorkspaceWaitInTransaction(selected.turn_id, workspace, now());
        }
        return {
          status: 'ready',
          conversation_id: selected.conversation_id,
          workspace,
        };
      }

      let selected = candidates.find((candidate) => {
        const resident = isResidentConversation(candidate.conversation_id);
        return resident?.owner_service_instance_id === serviceInstanceId;
      });
      if (!selected) {
        const takeoverAt = now();
        for (const candidate of candidates) {
          const resident = isResidentConversation(candidate.conversation_id);
          if (!resident || (resident.owner_expires_at && resident.owner_expires_at > takeoverAt)) {
            continue;
          }
          const taken = database.prepare(`
            UPDATE runtime_executor_residents
            SET owner_service_instance_id = ?, owner_epoch = owner_epoch + 1,
              owner_expires_at = ?, last_used_at = ?
            WHERE conversation_id = ? AND provider = 'claude'
              AND owner_epoch = ?
              AND (owner_expires_at IS NULL OR owner_expires_at <= ?)
          `).run(
            serviceInstanceId,
            residentOwnerExpiresAt(takeoverAt),
            takeoverAt,
            candidate.conversation_id,
            resident.owner_epoch,
            takeoverAt,
          );
          if (taken.changes === 1) {
            selected = candidate;
            break;
          }
        }
      }
      if (!selected) {
        for (const candidate of candidates) {
          if (isResidentConversation(candidate.conversation_id)) continue;
          if (residentCountForBot(candidate.bot_id) >= maxResidentExecutorsPerBot) continue;
          const admittedAt = now();
          database.prepare(`
            INSERT INTO runtime_executor_residents (
              conversation_id, bot_id, provider, owner_service_instance_id,
              owner_epoch, owner_expires_at, admitted_at, last_used_at
            ) VALUES (?, ?, 'claude', ?, 1, ?, ?, ?)
          `).run(
            candidate.conversation_id,
            candidate.bot_id,
            serviceInstanceId,
            residentOwnerExpiresAt(admittedAt),
            admittedAt,
            admittedAt,
          );
          selected = candidate;
          break;
        }
      }
      if (selected) {
        const workspaceAccess = workspaceAccessByConversation?.get(selected.conversation_id);
        const workspace = workspaceAccess ? workspaceLeases.acquire({
          workspace_root: workspaceAccess.workspace_root,
          mode: workspaceAccess.mode,
          holder_conversation_id: selected.conversation_id,
          holder_turn_id: selected.turn_id,
        }) : null;
        if (workspace?.status === 'wait') {
          return markWorkspaceWaitInTransaction(selected.turn_id, workspace, now());
        }
        const usedAt = now();
        database.prepare(`
          UPDATE runtime_executor_residents
          SET last_used_at = ?, owner_expires_at = ?
          WHERE conversation_id = ? AND provider = 'claude'
            AND owner_service_instance_id = ?
        `).run(
          usedAt,
          residentOwnerExpiresAt(usedAt),
          selected.conversation_id,
          serviceInstanceId,
        );
        return {
          status: 'ready',
          conversation_id: selected.conversation_id,
          workspace,
        };
      }

      const waits = [];
      const blocked = [];
      for (const candidate of candidates) {
        if (candidate.conversation_id === selected?.conversation_id) continue;
        const resident = isResidentConversation(candidate.conversation_id);
        if (resident?.owner_service_instance_id === serviceInstanceId) continue;
        if (resident) {
          blocked.push(candidate);
          if (markCapacityWait || selected) {
            const wait = markCapacityWaitInTransaction(candidate.turn_id, now());
            if (wait) waits.push(wait);
          }
          continue;
        }
        if (residentCountForBot(candidate.bot_id) < maxResidentExecutorsPerBot) continue;
        blocked.push(candidate);
        if (markCapacityWait || selected) {
          const wait = markCapacityWaitInTransaction(candidate.turn_id, now());
          if (wait) waits.push(wait);
        }
      }

      if (waits.length > 0) return { status: 'capacity_wait', ...waits[0] };
      if (blocked.length > 0) {
        return {
          status: 'capacity_wait',
          conversation_id: blocked[0].conversation_id,
          turn_id: blocked[0].turn_id,
          wait_reason: 'executor_capacity',
        };
      }
      return { status: 'idle' };
    });
    return reserve.immediate();
  }

  function claimNextQueuedTurn({
    conversationId = null,
    requireResident = false,
    workspaceAccess = null,
    workspaceLease = null,
  } = {}) {
    const claim = database.transaction(() => {
      const claimableAt = now();
      const turn = database.prepare(`
        SELECT turn.turn_id
        FROM runtime_turn_queue AS queue
        JOIN runtime_turns AS turn ON turn.turn_id = queue.turn_id
        JOIN runtime_conversations AS conversation
          ON conversation.conversation_id = turn.conversation_id
        WHERE queue.status = 'queued'
          AND NOT EXISTS (
            SELECT 1 FROM runtime_upgrade_runs AS upgrade
            WHERE upgrade.state IN (${BLOCKING_UPGRADE_STATES_SQL})
              AND (
                upgrade.scope_kind = 'installation'
                OR (upgrade.scope_kind = 'bot' AND upgrade.bot_id = conversation.bot_id)
              )
          )
          AND ${RELEASE_FENCE_PREDICATE_SQL}
          AND turn.lineage_id IS NOT NULL
          AND (
            turn.state = 'queued'
            OR (
              turn.state = 'recovering'
              AND (
                EXISTS (
                  SELECT 1
                  FROM runtime_reply_mapping_recoveries AS recovery
                  WHERE recovery.turn_id = turn.turn_id
                    AND recovery.state = 'bound'
                    AND recovery.bound_lineage_id = turn.lineage_id
                )
                OR EXISTS (
                  SELECT 1
                  FROM runtime_provider_attempts AS provider_attempt
                  WHERE provider_attempt.turn_id = turn.turn_id
                    AND provider_attempt.attempt_id = turn.attempt_id
                    AND provider_attempt.attempt_no = turn.attempt_no
                    AND provider_attempt.lease_epoch = turn.lease_epoch
                    AND provider_attempt.state = 'retry_wait'
                    AND provider_attempt.side_effect_status = 'none'
                    AND provider_attempt.next_retry_at <= ?
                )
              )
            )
          )
          AND (? IS NULL OR queue.conversation_id = ?)
          AND NOT EXISTS (
            SELECT 1
            FROM runtime_turn_queue AS earlier
            WHERE earlier.conversation_id = queue.conversation_id
              AND (
                earlier.priority > queue.priority
                OR (
                  earlier.priority = queue.priority
                  AND earlier.queue_sequence < queue.queue_sequence
                )
              )
              AND earlier.status IN ('queued', 'claimed')
          )
          AND NOT EXISTS (
            SELECT 1
            FROM runtime_executor_leases AS lease
            WHERE lease.conversation_id = queue.conversation_id
              AND lease.lease_owner IS NOT NULL
          )
        ORDER BY turn.created_at ASC, turn.conversation_id ASC, queue.queue_sequence ASC
        LIMIT 1
      `).get(
        serviceInstanceId, serviceInstanceId, claimableAt, conversationId, conversationId,
      );
      if (!turn) return null;

      const claimedAt = now();
      const current = loadTurn(database, turn.turn_id);
      const retryAttempt = (current.state === 'recovering'
        ? database.prepare(`
          SELECT retry_backoff_ms, next_retry_at, error_json
          FROM runtime_provider_attempts
          WHERE turn_id = ? AND attempt_id = ? AND attempt_no = ? AND lease_epoch = ?
            AND state = 'retry_wait' AND side_effect_status = 'none'
            AND next_retry_at <= ?
        `).get(
          current.turn_id,
          current.attempt_id,
          current.attempt_no,
          current.lease_epoch,
          claimedAt,
        )
        : null) ?? null;
      const resident = provider === 'claude'
        ? isResidentConversation(current.conversation_id)
        : null;
      if (
        provider === 'claude'
        && requireResident
        && resident === null
      ) {
        conflict('stale_attempt', 'The queued turn lost its resident executor reservation.');
      }
      if (
        resident !== null
        && (
          resident.owner_service_instance_id !== serviceInstanceId
          || resident.owner_expires_at === null
          || resident.owner_expires_at <= claimedAt
        )
      ) {
        conflict('stale_attempt', 'The queued turn lost its resident executor reservation.');
      }
      const workspace = workspaceLease === null
        ? (workspaceAccess === null ? null : workspaceLeases.acquire({
          workspace_root: workspaceAccess.workspace_root,
          mode: workspaceAccess.mode,
          holder_conversation_id: current.conversation_id,
          holder_turn_id: current.turn_id,
        }))
        : workspaceLeases.assertCurrent(workspaceLease);
      if (workspace?.status === 'wait') {
        return markWorkspaceWaitInTransaction(current.turn_id, workspace, claimedAt);
      }
      if (
        workspace !== null
        && (
          workspace.holder_conversation_id !== current.conversation_id
          || workspace.holder_turn_id !== current.turn_id
          || workspaceAccess !== null && (
            workspace.workspace_root !== workspaceAccess.workspace_root
            || workspace.mode !== workspaceAccess.mode
          )
        )
      ) {
        conflict('stale_workspace_lease', 'The workspace reservation does not match this turn.');
      }
      const existingLease = database.prepare(`
        SELECT lease_epoch
        FROM runtime_executor_leases
        WHERE conversation_id = ?
      `).get(current.conversation_id);
      const fence = {
        attempt_id: generateId('attempt'),
        attempt_no: retryAttempt === null ? 1 : current.attempt_no + 1,
        lease_epoch: (existingLease?.lease_epoch ?? 0) + 1,
      };
      const executorInstanceId = generateId('executor');
      const leaseExpiresAt = new Date(
        Date.parse(claimedAt) + leaseDurationMs,
      ).toISOString();
      const leaseWrite = database.prepare(`
        INSERT INTO runtime_executor_leases (
          conversation_id, lease_owner, lease_epoch, turn_id, attempt_id,
          attempt_no, lease_expires_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(conversation_id) DO UPDATE SET
          lease_owner = excluded.lease_owner,
          lease_epoch = excluded.lease_epoch,
          turn_id = excluded.turn_id,
          attempt_id = excluded.attempt_id,
          attempt_no = excluded.attempt_no,
          lease_expires_at = excluded.lease_expires_at,
          updated_at = excluded.updated_at
        WHERE runtime_executor_leases.lease_owner IS NULL
      `).run(
        current.conversation_id,
        serviceInstanceId,
        fence.lease_epoch,
        current.turn_id,
        fence.attempt_id,
        fence.attempt_no,
        leaseExpiresAt,
        claimedAt,
      );
      if (leaseWrite.changes !== 1) {
        conflict('stale_attempt', 'The executor lease was claimed concurrently.');
      }
      const turnUpdate = database.prepare(`
        UPDATE runtime_turns
        SET attempt_id = ?, attempt_no = ?, lease_epoch = ?
        WHERE turn_id = ? AND state = ?
          AND (
            (? = 0 AND attempt_id IS NULL AND attempt_no IS NULL)
            OR (
              ? = 1 AND attempt_id = ? AND attempt_no = ? AND lease_epoch = ?
            )
          )
      `).run(
        fence.attempt_id,
        fence.attempt_no,
        fence.lease_epoch,
        current.turn_id,
        current.state,
        retryAttempt === null ? 0 : 1,
        retryAttempt === null ? 0 : 1,
        current.attempt_id,
        current.attempt_no,
        current.lease_epoch,
      );
      const queueUpdate = database.prepare(`
        UPDATE runtime_turn_queue
        SET status = 'claimed', wait_reason = NULL, wait_detail_json = NULL
        WHERE turn_id = ? AND status = 'queued'
      `).run(current.turn_id);
      if (turnUpdate.changes !== 1 || queueUpdate.changes !== 1) {
        conflict('stale_attempt', 'The durable queue entry was claimed concurrently.');
      }
      database.prepare(`
        INSERT INTO runtime_provider_attempts (
          attempt_id, turn_id, conversation_id, attempt_no, lease_epoch,
          provider, service_instance_id, executor_instance_id, state,
          last_lease_renewed_at, started_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'starting', ?, ?, ?)
      `).run(
        fence.attempt_id,
        current.turn_id,
        current.conversation_id,
        fence.attempt_no,
        fence.lease_epoch,
        provider,
        serviceInstanceId,
        executorInstanceId,
        claimedAt,
        claimedAt,
        claimedAt,
      );
      if (retryAttempt !== null) {
        const retryEvent = buildEvent({
          turn: loadTurn(database, current.turn_id),
          lastEvent: loadLastEvent(database, current.turn_id),
          fence,
          provider,
          descriptor: {
            kind: 'retry_attempt_started',
            phase: 'recovering',
            payload: {
              retry_no: current.attempt_no,
              attempt_no: fence.attempt_no,
              backoff_ms: retryAttempt.retry_backoff_ms,
              reason_code: 'provider_transient',
            },
            provider_native_id: current.provider_native_id,
            error: retryAttempt.error_json === null
              ? null
              : JSON.parse(retryAttempt.error_json),
          },
          occurredAt: claimedAt,
          generateId,
        });
        commitTurnEvent(database, {
          turn: loadTurn(database, current.turn_id),
          event: retryEvent,
          fence,
          nextState: 'recovering',
          staleMessage: 'The retry attempt start lost its provider attempt fence.',
          generateId,
        });
      }
      transitionInTransaction(database, {
        turnId: current.turn_id,
        fromState: current.state,
        toState: 'starting',
        fence,
        provider,
        serviceInstanceId,
        occurredAt: claimedAt,
        generateId,
      });
      const envelope = JSON.parse(current.envelope_json);
      if (current.provider !== null && current.provider !== provider) {
        conflict(
          'provider_context_invalid',
          'The persisted lineage belongs to a different provider adapter.',
        );
      }
      return {
        conversation_id: current.conversation_id,
        turn_id: current.turn_id,
        lineage_id: current.lineage_id,
        provider_native_id: current.provider_native_id,
        trace_id: envelope.trace_id,
        input: current.provider_input_json === null
          ? envelope.content
          : JSON.parse(current.provider_input_json),
        interaction: {
          authorized_subjects: envelope.actor?.authenticated === true
            && typeof envelope.actor.actor_id === 'string'
            ? [{ type: 'actor', actor_id: envelope.actor.actor_id }]
            : [],
          allowed_sources: ['main_card_reply', ...(
            ['feishu', 'lark'].includes(envelope.channel) ? ['card_action'] : []
          )],
        },
        lineage: {
          provider_native_id: current.provider_native_id,
        },
        interaction_authority: envelope.actor?.authenticated === true
          ? [{ type: 'actor', actor_id: envelope.actor.actor_id }]
          : [],
        resident: resident === null ? null : Object.freeze({
          owner_epoch: resident.owner_epoch,
        }),
        workspace,
        executor_instance_id: executorInstanceId,
        attempt: fence,
      };
    });
    return claim.immediate();
  }

  function scheduleProviderRetry(turnContext, error, backoffMs) {
    if (
      error?.retryable !== true
      || error.side_effect_status !== 'none'
      || !Number.isSafeInteger(backoffMs)
      || backoffMs < 0
      || turnContext.attempt.attempt_no > 3
    ) {
      conflict('provider_context_invalid', 'Provider retry requires proven no-side-effect eligibility.');
    }
    const schedule = database.transaction(() => {
      const occurredAt = now();
      let turn = loadTurn(database, turnContext.turn_id);
      assertTurnContextFence(turn, turnContext);
      assertActiveFence(database, turn, turnContext.attempt, serviceInstanceId);
      if (!['starting', 'running'].includes(turn.state)) {
        conflict('illegal_transition', `Provider retry is invalid while turn is ${turn.state}.`);
      }
      recordProviderEventActivityInTransaction(turnContext, occurredAt);
      transitionInTransaction(database, {
        turnId: turn.turn_id,
        fromState: turn.state,
        toState: 'recovering',
        fence: turnContext.attempt,
        provider,
        serviceInstanceId,
        occurredAt,
        generateId,
        reasonCode: 'provider_retry_scheduled',
      });
      turn = loadTurn(database, turn.turn_id);
      const retryNo = turnContext.attempt.attempt_no;
      const event = buildEvent({
        turn,
        lastEvent: loadLastEvent(database, turn.turn_id),
        fence: turnContext.attempt,
        provider,
        descriptor: {
          kind: 'retry_scheduled',
          phase: 'recovering',
          payload: {
            retry_no: retryNo,
            attempt_no: retryNo + 1,
            backoff_ms: backoffMs,
            reason_code: 'provider_transient',
          },
          provider_native_id: turn.provider_native_id,
          error,
        },
        occurredAt,
        generateId,
      });
      commitTurnEvent(database, {
        turn,
        event,
        fence: turnContext.attempt,
        nextState: 'recovering',
        staleMessage: 'The retry schedule lost its provider attempt fence.',
        generateId,
      });
      const nextRetryAt = new Date(Date.parse(occurredAt) + backoffMs).toISOString();
      const attemptUpdate = database.prepare(`
        UPDATE runtime_provider_attempts
        SET state = 'retry_wait', side_effect_status = 'none', error_json = ?,
          retry_backoff_ms = ?, next_retry_at = ?, updated_at = ?
        WHERE turn_id = ? AND attempt_id = ? AND attempt_no = ? AND lease_epoch = ?
          AND service_instance_id = ? AND state IN ('starting', 'running')
      `).run(
        JSON.stringify(error),
        backoffMs,
        nextRetryAt,
        occurredAt,
        turn.turn_id,
        turnContext.attempt.attempt_id,
        turnContext.attempt.attempt_no,
        turnContext.attempt.lease_epoch,
        serviceInstanceId,
      );
      const queueUpdate = database.prepare(`
        UPDATE runtime_turn_queue
        SET status = 'queued', wait_reason = 'provider_retry'
        WHERE turn_id = ? AND status = 'claimed'
      `).run(turn.turn_id);
      if (turnContext.workspace) workspaceLeases.release(turnContext.workspace);
      const leaseUpdate = database.prepare(`
        UPDATE runtime_executor_leases
        SET lease_owner = NULL, turn_id = NULL, attempt_id = NULL,
          attempt_no = NULL, lease_expires_at = NULL, updated_at = ?
        WHERE conversation_id = ? AND lease_owner = ? AND turn_id = ?
          AND attempt_id = ? AND attempt_no = ? AND lease_epoch = ?
      `).run(
        occurredAt,
        turn.conversation_id,
        serviceInstanceId,
        turn.turn_id,
        turnContext.attempt.attempt_id,
        turnContext.attempt.attempt_no,
        turnContext.attempt.lease_epoch,
      );
      if (attemptUpdate.changes !== 1 || queueUpdate.changes !== 1 || leaseUpdate.changes !== 1) {
        conflict('stale_attempt', 'The provider retry schedule changed concurrently.');
      }
      return Object.freeze({
        retry_no: retryNo,
        next_attempt_no: retryNo + 1,
        backoff_ms: backoffMs,
        next_retry_at: nextRetryAt,
      });
    });
    return schedule.immediate();
  }

  function exhaustProviderRetries(turnContext, error) {
    if (
      error?.retryable !== true
      || error.side_effect_status !== 'none'
      || turnContext.attempt.attempt_no !== 4
    ) {
      conflict('provider_context_invalid', 'Retry exhaustion requires the fourth safe attempt.');
    }
    const exhaust = database.transaction(() => {
      const occurredAt = now();
      const turn = loadTurn(database, turnContext.turn_id);
      assertTurnContextFence(turn, turnContext);
      assertActiveFence(database, turn, turnContext.attempt, serviceInstanceId);
      if (!['starting', 'running'].includes(turn.state)) {
        conflict('illegal_transition', `Retry exhaustion is invalid while turn is ${turn.state}.`);
      }
      recordProviderEventActivityInTransaction(turnContext, occurredAt);
      const event = buildEvent({
        turn,
        lastEvent: loadLastEvent(database, turn.turn_id),
        fence: turnContext.attempt,
        provider,
        descriptor: {
          kind: 'retry_exhausted',
          phase: turn.state,
          payload: {
            retry_no: 3,
            attempt_no: 4,
            backoff_ms: 0,
            reason_code: 'provider_transient',
          },
          provider_native_id: turn.provider_native_id,
          error,
        },
        occurredAt,
        generateId,
      });
      commitTurnEvent(database, {
        turn,
        event,
        fence: turnContext.attempt,
        nextState: turn.state,
        staleMessage: 'Retry exhaustion lost its provider attempt fence.',
        generateId,
      });
      transitionInTransaction(database, {
        turnId: turn.turn_id,
        fromState: turn.state,
        toState: 'failed',
        fence: turnContext.attempt,
        provider,
        serviceInstanceId,
        occurredAt,
        generateId,
        reasonCode: 'provider_retry_exhausted',
        error,
      });
      if (turnContext.workspace) workspaceLeases.release(turnContext.workspace);
      const attemptUpdate = database.prepare(`
        UPDATE runtime_provider_attempts
        SET state = 'failed', side_effect_status = 'none', error_json = ?, updated_at = ?
        WHERE turn_id = ? AND attempt_id = ? AND attempt_no = 4 AND lease_epoch = ?
          AND service_instance_id = ? AND state IN ('starting', 'running')
      `).run(
        JSON.stringify(error),
        occurredAt,
        turn.turn_id,
        turnContext.attempt.attempt_id,
        turnContext.attempt.lease_epoch,
        serviceInstanceId,
      );
      if (attemptUpdate.changes !== 1) {
        conflict('stale_attempt', 'Retry exhaustion lost its durable attempt record.');
      }
      return event;
    });
    return exhaust.immediate();
  }

  function persistDeliveryUnknownInTransaction({
    turn,
    request,
    handoff,
    handoffVersion,
    fence,
    occurredAt,
    error,
    reasonCode,
    recoverParent,
    requireActiveLease = true,
    auditContext = {},
  }) {
    if (error.side_effect_status !== 'unknown') {
      conflict('provider_context_invalid', 'Unknown delivery requires unknown provider side effects.');
    }
    validateInteractionTransition({
      from: 'answer_delivering',
      to: 'delivery_unknown',
      sendStarted: true,
      occurredAt,
    });
    validateInteractionHandoffTransition({
      from: 'delivering',
      to: 'delivery_unknown',
      sendStarted: true,
      occurredAt,
    });
    const updatedRequest = {
      ...request,
      state: 'delivery_unknown',
      version: request.version + 1,
      handoff_state: 'delivery_unknown',
    };
    const updatedHandoff = {
      ...handoff,
      state: 'delivery_unknown',
      reason_code: reasonCode,
      error,
      side_effect_status: 'unknown',
    };
    validateInteractionRequest(updatedRequest, { occurredAt });
    validateInteractionHandoff(updatedHandoff, { occurredAt });
    const interactionUpdate = database.prepare(`
      UPDATE runtime_interactions
      SET state = 'delivery_unknown', version = ?,
        handoff_state = 'delivery_unknown', handoff_version = ?,
        request_json = ?, updated_at = ?
      WHERE interaction_id = ? AND state = 'answer_delivering' AND version = ?
    `).run(
      updatedRequest.version,
      handoffVersion,
      JSON.stringify(updatedRequest),
      occurredAt,
      request.interaction_id,
      request.version,
    );
    const handoffUpdate = database.prepare(`
      UPDATE runtime_interaction_handoffs
      SET state = 'delivery_unknown', record_json = ?, updated_at = ?
      WHERE handoff_id = ? AND state = 'delivering'
        AND handoff_attempt_id = ? AND handoff_attempt_no = ?
        AND provider_attempt_id = ? AND lease_epoch = ?
    `).run(
      JSON.stringify(updatedHandoff),
      occurredAt,
      handoff.handoff_id,
      handoff.handoff_attempt_id,
      handoff.handoff_attempt_no,
      handoff.provider_attempt_id,
      handoff.lease_epoch,
    );
    assertPairedInteractionHandoffCas(
      interactionUpdate,
      handoffUpdate,
      'stale_attempt',
      'The unknown interaction delivery lost its durable fence.',
    );

    let currentTurn = turn;
    if (recoverParent) {
      transitionInTransaction(database, {
        turnId: turn.turn_id,
        fromState: 'waiting_user',
        toState: 'recovering',
        fence,
        provider,
        serviceInstanceId,
        occurredAt,
        generateId,
        reasonCode: 'interaction_answer_delivery_unknown',
        requireActiveLease,
      });
      currentTurn = loadTurn(database, turn.turn_id);
    }
    const event = buildEvent({
      turn: currentTurn,
      lastEvent: loadLastEvent(database, currentTurn.turn_id),
      fence,
      provider,
      descriptor: {
        kind: 'interaction_answer_delivery_unknown',
        phase: currentTurn.state,
        payload: {
          interaction_id: request.interaction_id,
          ordinal: request.ordinal,
          interaction_version: updatedRequest.version,
          handoff_version: handoffVersion,
          state: 'delivery_unknown',
          handoff_state: 'delivery_unknown',
        },
        error,
      },
      occurredAt,
      generateId,
    });
    commitTurnEvent(database, {
      turn: currentTurn,
      event,
      fence,
      nextState: currentTurn.state,
      staleMessage: 'The unknown interaction delivery lost its provider attempt fence.',
      generateId,
    });
    const auditId = generateId('audit');
    database.prepare(`
      INSERT INTO runtime_interaction_audit (
        audit_id, interaction_id, handoff_id, outcome, provider_attempt_id,
        lease_epoch, acknowledgement_json, created_at
      ) VALUES (?, ?, ?, 'delivery_unknown', ?, ?, ?, ?)
    `).run(
      auditId,
      request.interaction_id,
      handoff.handoff_id,
      handoff.provider_attempt_id,
      handoff.lease_epoch,
      JSON.stringify({
        status: 'delivery_unknown',
        reason_code: reasonCode,
        ...auditContext,
        error,
      }),
      occurredAt,
    );
    return {
      auditId,
      event,
      turn: loadTurn(database, currentTurn.turn_id),
      updatedHandoff,
      updatedRequest,
    };
  }

  function settleBlockingInteractionsForStop(turnContext, turn, cancelledAt) {
    const rows = database.prepare(`
      SELECT interaction.request_json, interaction.handoff_version,
        handoff.record_json AS handoff_json
      FROM runtime_interactions AS interaction
      LEFT JOIN runtime_interaction_handoffs AS handoff
        ON handoff.interaction_id = interaction.interaction_id
      WHERE interaction.turn_id = ?
        AND interaction.state IN (
          'pending', 'answer_committed', 'answer_delivering', 'delivery_unknown'
        )
      ORDER BY interaction.ordinal ASC
    `).all(turn.turn_id);
    let currentTurn = turn;
    for (const row of rows) {
      const request = JSON.parse(row.request_json);
      if (request.state === 'delivery_unknown') continue;
      const handoff = row.handoff_json === null ? null : JSON.parse(row.handoff_json);
      const sendStarted = handoff?.last_send_started_at !== null
        && handoff?.last_send_started_at !== undefined;
      if (request.state === 'answer_delivering' && sendStarted) {
        const error = createContractError({
          code: 'interaction_answer_delivery_unknown',
          category: 'provider',
          retryable: false,
          sideEffectStatus: 'unknown',
          userMessage: 'The answer send started before the parent was stopped; acknowledgement is unknown.',
          occurredAt: cancelledAt,
        });
        const unknown = persistDeliveryUnknownInTransaction({
          turn: currentTurn,
          request,
          handoff,
          handoffVersion: row.handoff_version + 1,
          fence: turnContext.attempt,
          occurredAt: cancelledAt,
          error,
          reasonCode: 'parent_stopped_after_send_started',
          recoverParent: false,
        });
        currentTurn = unknown.turn;
        continue;
      }
      validateInteractionTransition({
        from: request.state,
        to: 'cancelled',
        sendStarted,
        occurredAt: cancelledAt,
      });
      let handoffVersion = row.handoff_version;
      const updatedRequest = {
        ...request,
        state: 'cancelled',
        version: request.version + 1,
        terminal_reason: 'parent_stopped',
      };
      if (handoff !== null) {
        validateInteractionHandoffTransition({
          from: handoff.state,
          to: 'cancelled',
          sendStarted,
          occurredAt: cancelledAt,
        });
        const updatedHandoff = {
          ...handoff,
          state: 'cancelled',
          reason_code: 'parent_stopped',
        };
        validateInteractionHandoff(updatedHandoff, { occurredAt: cancelledAt });
        const handoffUpdate = database.prepare(`
          UPDATE runtime_interaction_handoffs
          SET state = 'cancelled', record_json = ?, updated_at = ?
          WHERE handoff_id = ? AND state = ?
        `).run(
          JSON.stringify(updatedHandoff),
          cancelledAt,
          handoff.handoff_id,
          handoff.state,
        );
        if (handoffUpdate.changes !== 1) {
          conflict('stale_attempt', 'Turn cancellation lost its pending interaction handoff.');
        }
        updatedRequest.handoff_state = 'cancelled';
        handoffVersion += 1;
        database.prepare(`
          INSERT INTO runtime_interaction_audit (
            audit_id, interaction_id, handoff_id, outcome, provider_attempt_id,
            lease_epoch, acknowledgement_json, created_at
          ) VALUES (?, ?, ?, 'cancelled', ?, ?, ?, ?)
        `).run(
          generateId('audit'),
          request.interaction_id,
          handoff.handoff_id,
          handoff.provider_attempt_id,
          handoff.lease_epoch,
          JSON.stringify({ status: 'cancelled', reason_code: 'parent_stopped' }),
          cancelledAt,
        );
      }
      validateInteractionRequest(updatedRequest, { occurredAt: cancelledAt });
      const interactionUpdate = database.prepare(`
        UPDATE runtime_interactions
        SET state = 'cancelled', version = ?, handoff_state = ?, handoff_version = ?,
          request_json = ?, updated_at = ?
        WHERE interaction_id = ? AND state = ? AND version = ?
      `).run(
        updatedRequest.version,
        updatedRequest.handoff_state,
        handoffVersion,
        JSON.stringify(updatedRequest),
        cancelledAt,
        request.interaction_id,
        request.state,
        request.version,
      );
      if (interactionUpdate.changes !== 1) {
        conflict('stale_attempt', 'Turn cancellation lost its pending interaction fence.');
      }
      const event = buildEvent({
        turn: currentTurn,
        lastEvent: loadLastEvent(database, currentTurn.turn_id),
        fence: turnContext.attempt,
        provider,
        descriptor: {
          kind: 'interaction_cancelled',
          phase: currentTurn.state,
          payload: {
            interaction_id: request.interaction_id,
            ordinal: request.ordinal,
            interaction_version: updatedRequest.version,
            handoff_version: handoffVersion,
          },
        },
        occurredAt: cancelledAt,
        generateId,
      });
      commitTurnEvent(database, {
        turn: currentTurn,
        event,
        fence: turnContext.attempt,
        nextState: currentTurn.state,
        staleMessage: 'Turn cancellation lost its provider attempt fence.',
        generateId,
      });
      currentTurn = loadTurn(database, currentTurn.turn_id);
    }
    return currentTurn;
  }

  function transitionTurn(
    turnContext,
    fromState,
    toState,
    {
      allowUncertainWorkspace = false,
      error = null,
      reasonCode = null,
      recovery = null,
      providerEventObserved = false,
    } = {},
  ) {
    const transition = database.transaction(() => {
      let turn = loadTurn(database, turnContext.turn_id);
      if (allowUncertainWorkspace) assertTurnContextIsolationFence(turn, turnContext);
      else assertTurnContextFence(turn, turnContext);
      const occurredAt = now();
      if (providerEventObserved) {
        recordProviderEventActivityInTransaction(turnContext, occurredAt);
      }
      if (toState === 'stopped') {
        turn = settleBlockingInteractionsForStop(turnContext, turn, occurredAt);
      }
      let event = transitionInTransaction(database, {
        turnId: turnContext.turn_id,
        fromState,
        toState,
        fence: turnContext.attempt,
        provider,
        serviceInstanceId,
        occurredAt,
        generateId,
        error,
        reasonCode: reasonCode ?? undefined,
      });
      if (recovery !== null) {
        if (toState !== 'recovering' || recovery?.error === null || !recovery?.error) {
          conflict(
            'validation_error',
            'A recovery event requires a recovering transition and public error.',
          );
        }
        turn = loadTurn(database, turnContext.turn_id);
        const recoveryId = generateId('recovery');
        event = buildEvent({
          turn,
          lastEvent: loadLastEvent(database, turn.turn_id),
          fence: turnContext.attempt,
          provider,
          descriptor: {
            kind: 'recovery_started',
            phase: 'recovering',
            payload: {
              recovery_id: recoveryId,
              recovery_of_turn_id: turn.turn_id,
              recovery_of_lineage_id: turn.lineage_id,
              side_effect_status: recovery.error.side_effect_status,
            },
            error: {
              ...recovery.error,
              occurred_at: recovery.error.occurred_at ?? occurredAt,
            },
          },
          occurredAt,
          generateId,
        });
        commitTurnEvent(database, {
          turn,
          event,
          fence: turnContext.attempt,
          nextState: 'recovering',
          staleMessage: 'The recovery notification lost its provider attempt fence.',
          generateId,
        });
        if (recovery.waitForDecision === true) {
          turn = loadTurn(database, turnContext.turn_id);
          event = buildEvent({
            turn,
            lastEvent: loadLastEvent(database, turn.turn_id),
            fence: turnContext.attempt,
            provider,
            descriptor: {
              kind: 'recovery_waiting_decision',
              phase: 'recovering',
              payload: {
                recovery_id: recoveryId,
                recovery_of_turn_id: turn.turn_id,
                recovery_of_lineage_id: turn.lineage_id,
                side_effect_status: recovery.error.side_effect_status,
              },
              error: {
                ...recovery.error,
                occurred_at: recovery.error.occurred_at ?? occurredAt,
              },
            },
            occurredAt,
            generateId,
          });
          commitTurnEvent(database, {
            turn,
            event,
            fence: turnContext.attempt,
            nextState: 'recovering',
            staleMessage: 'The recovery decision boundary lost its provider attempt fence.',
            generateId,
          });
        }
      }
      if (TERMINAL_STATES.has(toState) && toState !== 'timed_out' && turnContext.workspace) {
        workspaceLeases.release(turnContext.workspace);
      }
      if (toState === 'failed' && error?.code === 'provider_context_invalid') {
        database.prepare(`
          UPDATE runtime_lineages
          SET provider_native_state = 'invalid'
          WHERE lineage_id = ? AND conversation_id = ?
          AND provider = ? AND provider_native_id IS NOT NULL
        `).run(turn.lineage_id, turn.conversation_id, provider);
      }
      database.prepare(`
        UPDATE runtime_provider_attempts
        SET state = ?, side_effect_status = ?, error_json = ?, updated_at = ?
        WHERE attempt_id = ? AND turn_id = ? AND attempt_no = ? AND lease_epoch = ?
          AND service_instance_id = ?
      `).run(
        toState,
        error?.side_effect_status ?? 'none',
        error === null ? null : JSON.stringify(error),
        occurredAt,
        turnContext.attempt.attempt_id,
        turnContext.turn_id,
        turnContext.attempt.attempt_no,
        turnContext.attempt.lease_epoch,
        serviceInstanceId,
      );
      return event;
    });
    return transition.immediate();
  }

  function validateSteerEnvelope(envelope) {
    let validated;
    try {
      validated = validateInboundEnvelope(envelope);
    } catch (error) {
      translateContractError(error);
    }
    if (
      validated.forwarded.source.kind !== 'platform_original'
      || validated.forwarded.actor.type !== 'user'
      || validated.forwarded.actor.authenticated !== true
    ) {
      conflict(
        'steer_precondition_failed',
        '/steer requires an authenticated original user message.',
      );
    }
    const { content } = validated.forwarded;
    if (
      content.kind !== 'text'
      || content.attachments.length !== 0
      || typeof content.text !== 'string'
    ) {
      conflict(
        'steer_precondition_failed',
        '/steer must occupy the complete original plain-text message.',
      );
    }
    const match = /^\/steer[\t ]+([\s\S]*\S)[\t ]*$/u.exec(content.text);
    if (!match) {
      conflict(
        'steer_precondition_failed',
        '/steer requires non-empty supplemental text and no surrounding content.',
      );
    }
    const payloadHash = createPayloadHash(envelope, {
      scope: 'inbound',
      knownFields: INBOUND_ENVELOPE_KNOWN_FIELDS,
      extensionFields: Object.keys(validated.extensions),
    });
    return {
      envelope: validated.forwarded,
      payloadHash,
      supplement: match[1].trim(),
    };
  }

  function sideEffectStatusForProviderDescriptor(descriptor, fallbackKind = null) {
    const event = descriptor?.type === 'normalized_event'
      ? descriptor.event
      : descriptor;
    const kind = event?.kind ?? fallbackKind;
    return typeof kind === 'string' && kind.startsWith('tool_')
      ? event?.payload?.side_effect_status
      : null;
  }

  function steerSideEffectStatus(turnId, attempt = null) {
    let status = 'none';
    for (const { event_json: eventJson } of database.prepare(`
      SELECT event_json
      FROM runtime_normalized_events
      WHERE turn_id = ?
      ORDER BY event_sequence ASC
    `).all(turnId)) {
      const event = JSON.parse(eventJson);
      const eventStatus = sideEffectStatusForProviderDescriptor(event);
      if (eventStatus === 'unknown') return 'unknown';
      if (eventStatus === 'known') status = 'known';
    }
    const diagnosticRows = attempt
      ? database.prepare(`
        SELECT event_kind, descriptor_json
        FROM runtime_provider_event_diagnostics
        WHERE turn_id = ? AND attempt_id = ? AND attempt_no = ? AND lease_epoch = ?
        ORDER BY observed_at ASC, diagnostic_id ASC
      `).all(
        turnId,
        attempt.attempt_id,
        attempt.attempt_no,
        attempt.lease_epoch,
      )
      : [];
    for (const row of diagnosticRows) {
      const eventStatus = sideEffectStatusForProviderDescriptor(
        JSON.parse(row.descriptor_json),
        row.event_kind,
      );
      if (eventStatus === 'unknown') return 'unknown';
      if (eventStatus === 'known') status = 'known';
    }
    return status;
  }

  function rejectedSteerResult(request, persistenceError, winner = null, {
    committedAt = null,
    reconciliationRequired = false,
    interactionId = null,
  } = {}) {
    const rejectedAt = committedAt ?? now();
    const turn = typeof request?.turn_id === 'string'
      ? database.prepare(`
        SELECT turn_id, lineage_id, state, turn_version
        FROM runtime_turns
        WHERE turn_id = ?
      `).get(request.turn_id)
      : null;
    const code = persistenceError.code ?? 'validation_error';
    const category = code === 'unauthenticated'
      ? 'authentication'
      : ([
        'idempotency_conflict',
        'steer_precondition_failed',
        'steer_reconciliation_required',
        'turn_terminal',
      ].includes(code)
          ? 'conflict'
          : 'validation');
    return {
      status: 'rejected',
      steer_id: request?.steer_id ?? null,
      conversation_id: request?.conversation_id ?? null,
      winner,
      old_turn: turn ? {
        turn_id: turn.turn_id,
        state: turn.state,
        turn_version: turn.turn_version,
        lineage_id: turn.lineage_id,
      } : null,
      priority_turn: {
        status: 'not_created',
        turn_id: null,
        state: null,
        lineage_id: turn?.lineage_id ?? null,
        redirected_from_turn_id: turn?.turn_id ?? null,
      },
      provider_stop_status: 'not_applicable',
      lease_released: turn === null || ![
        'starting',
        'running',
        'waiting_user',
        'redirecting',
        'recovering',
      ].includes(turn.state),
      error: createContractError({
        code,
        category,
        retryable: false,
        sideEffectStatus: 'none',
        userMessage: persistenceError.message,
        occurredAt: rejectedAt,
      }),
      committed_at: committedAt,
      updated_at: rejectedAt,
      deduplicated: false,
      ...(reconciliationRequired ? {
        reconciliation_required: true,
        blocking_interaction_id: interactionId,
      } : {}),
    };
  }

  function persistSteerRequestInTransaction(request, result, winnerControlId, committedAt) {
    database.prepare(`
      INSERT INTO runtime_steer_requests (
        steer_id, conversation_id, target_turn_id, inbound_event_id,
        winner_control_id, request_json, result_json, committed_at, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      request.steer_id,
      request.conversation_id,
      request.turn_id,
      request.inbound_event_id,
      winnerControlId,
      canonicalizeJson(request),
      JSON.stringify(result),
      committedAt,
      result.updated_at,
    );
  }

  function replayOrConflictSteerRequestInTransaction(request) {
    const row = database.prepare(`
      SELECT steer_id, conversation_id, target_turn_id, winner_control_id,
        request_json, result_json
      FROM runtime_steer_requests
      WHERE steer_id = ? OR inbound_event_id = ?
      ORDER BY CASE WHEN steer_id = ? THEN 0 ELSE 1 END
      LIMIT 1
    `).get(request.steer_id, request.inbound_event_id, request.steer_id);
    if (!row) return null;
    if (
      row.steer_id !== request.steer_id
      || row.conversation_id !== request.conversation_id
      || row.target_turn_id !== request.turn_id
      || canonicalizeJson(JSON.parse(row.request_json)) !== canonicalizeJson(request)
    ) {
      return rejectedSteerResult(
        request,
        new ExecutorPersistenceError(
          'idempotency_conflict',
          'The steer ID or inbound event was already used by another request.',
        ),
        controlWinnerForTurn(request.turn_id),
      );
    }
    const resultRow = row.winner_control_id === null
      ? row
      : database.prepare(`
        SELECT result_json FROM runtime_steer_controls WHERE steer_id = ?
      `).get(row.winner_control_id);
    if (!resultRow) {
      conflict('version_conflict', 'The steer request lost its durable winner control.');
    }
    return { ...JSON.parse(resultRow.result_json), deduplicated: true };
  }

  function controlWinnerForTurn(turnId) {
    const steer = database.prepare(`
      SELECT result_json
      FROM runtime_steer_controls
      WHERE target_turn_id = ?
    `).get(turnId);
    if (steer) return JSON.parse(steer.result_json).winner;
    const stop = database.prepare(`
      SELECT stop_id
      FROM runtime_stop_controls
      WHERE active_turn_id = ?
      ORDER BY committed_at ASC
      LIMIT 1
    `).get(turnId);
    return stop ? {
      control: 'stop',
      control_id: stop.stop_id,
      turn_id: turnId,
    } : null;
  }

  function beginSteer({
    conversation_id: conversationId,
    turn_id: turnId,
    steer_id: steerId,
    envelope,
  }) {
    if (typeof conversationId !== 'string' || conversationId.length === 0) {
      throw new TypeError('conversation_id must be a non-empty string');
    }
    if (typeof turnId !== 'string' || turnId.length === 0) {
      throw new TypeError('turn_id must be a non-empty string');
    }
    if (typeof steerId !== 'string' || steerId.length === 0) {
      throw new TypeError('steer_id must be a non-empty string');
    }
    let steerEnvelope;
    try {
      steerEnvelope = validateSteerEnvelope(envelope);
    } catch (error) {
      if (error instanceof ExecutorPersistenceError) {
        return rejectedSteerResult({
          conversation_id: conversationId,
          turn_id: turnId,
          steer_id: steerId,
        }, error);
      }
      throw error;
    }
    const begin = database.transaction(() => {
      const request = {
        conversation_id: conversationId,
        turn_id: turnId,
        steer_id: steerId,
        inbound_event_id: steerEnvelope.envelope.inbound_event_id,
        idempotency_key: steerEnvelope.envelope.idempotency_key,
        payload_hash: steerEnvelope.payloadHash,
        supplement: steerEnvelope.supplement,
      };
      const replay = replayOrConflictSteerRequestInTransaction(request);
      if (replay) return replay;

      const turn = loadTurn(database, turnId);
      if (turn.conversation_id !== conversationId) {
        conflict('steer_precondition_failed', 'The steer target is outside this conversation.');
      }
      const conversation = database.prepare(`
        SELECT conversation_key
        FROM runtime_conversations
        WHERE conversation_id = ?
      `).get(conversationId);
      if (conversation?.conversation_key !== encodeConversationKey(steerEnvelope.envelope)) {
        conflict('steer_precondition_failed', 'The steer message targets another conversation.');
      }
      const committedAt = now();
      const existingInbound = database.prepare(`
        SELECT inbound_event_id
        FROM runtime_inbound_events
        WHERE inbound_event_id = ? OR idempotency_key = ?
        LIMIT 1
      `).get(request.inbound_event_id, request.idempotency_key);
      if (existingInbound) {
        return rejectedSteerResult(
          request,
          new ExecutorPersistenceError(
            'idempotency_conflict',
            'The inbound event was already committed outside this steer request.',
          ),
          controlWinnerForTurn(turnId),
        );
      }
      database.prepare(`
        INSERT INTO runtime_inbound_events (
          inbound_event_id, idempotency_key, conversation_id, message_id,
          payload_hash, envelope_json, received_at, committed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      `).run(
        steerEnvelope.envelope.inbound_event_id,
        steerEnvelope.envelope.idempotency_key,
        conversationId,
        steerEnvelope.envelope.message_id,
        steerEnvelope.payloadHash,
        JSON.stringify(steerEnvelope.envelope),
        steerEnvelope.envelope.received_at,
        committedAt,
      );
      if (turn.state !== 'running') {
        const rejected = rejectedSteerResult(request, new ExecutorPersistenceError(
          TERMINAL_STATES.has(turn.state) ? 'turn_terminal' : 'steer_precondition_failed',
          `/steer is only accepted while the canonical turn is running; it is ${turn.state}.`,
        ), controlWinnerForTurn(turnId), { committedAt });
        persistSteerRequestInTransaction(request, rejected, null, committedAt);
        return rejected;
      }
      const blockingInteraction = database.prepare(`
        SELECT interaction_id
        FROM runtime_interactions
        WHERE turn_id = ? AND state IN (${BLOCKING_INTERACTION_STATES_SQL})
        LIMIT 1
      `).get(turnId);
      if (blockingInteraction) {
        transitionInTransaction(database, {
          turnId,
          fromState: 'running',
          toState: 'recovering',
          fence: {
            attempt_id: turn.attempt_id,
            attempt_no: turn.attempt_no,
            lease_epoch: turn.lease_epoch,
          },
          provider,
          serviceInstanceId,
          occurredAt: committedAt,
          generateId,
          reasonCode: 'steer_interaction_projection_inconsistent',
        });
        const rejected = rejectedSteerResult(
          request,
          new ExecutorPersistenceError(
            'steer_reconciliation_required',
            '/steer found conflicting durable interaction ownership; provider work was fenced for reconciliation.',
          ),
          controlWinnerForTurn(turnId),
          {
            committedAt,
            reconciliationRequired: true,
            interactionId: blockingInteraction.interaction_id,
          },
        );
        persistSteerRequestInTransaction(request, rejected, null, committedAt);
        return rejected;
      }
      const redirectEvent = transitionInTransaction(database, {
        turnId,
        fromState: 'running',
        toState: 'redirecting',
        fence: {
          attempt_id: turn.attempt_id,
          attempt_no: turn.attempt_no,
          lease_epoch: turn.lease_epoch,
        },
        provider,
        serviceInstanceId,
        occurredAt: committedAt,
        generateId,
        reasonCode: 'steer_requested',
      });
      const result = {
        status: 'redirecting',
        steer_id: steerId,
        conversation_id: conversationId,
        winner: {
          control: 'steer',
          control_id: steerId,
          turn_id: turnId,
        },
        old_turn: {
          turn_id: turnId,
          previous_state: 'running',
          previous_version: turn.turn_version,
          state: 'redirecting',
          turn_version: redirectEvent.turn_version,
          side_effect_status: steerSideEffectStatus(turnId, {
            attempt_id: turn.attempt_id,
            attempt_no: turn.attempt_no,
            lease_epoch: turn.lease_epoch,
          }),
          attempt: {
            attempt_id: turn.attempt_id,
            attempt_no: turn.attempt_no,
            lease_epoch: turn.lease_epoch,
          },
        },
        priority_turn: {
          status: 'not_created',
          turn_id: null,
          state: null,
          lineage_id: turn.lineage_id,
          redirected_from_turn_id: turnId,
        },
        provider_stop_status: 'pending',
        lease_released: false,
        error: null,
        committed_at: committedAt,
        updated_at: committedAt,
        deduplicated: false,
      };
      database.prepare(`
        INSERT INTO runtime_steer_controls (
          steer_id, conversation_id, target_turn_id, inbound_event_id,
          request_json, result_json, committed_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      `).run(
        steerId,
        conversationId,
        turnId,
        steerEnvelope.envelope.inbound_event_id,
        JSON.stringify(request),
        JSON.stringify(result),
        committedAt,
        committedAt,
      );
      persistSteerRequestInTransaction(request, result, steerId, committedAt);
      return result;
    });
    return begin.immediate();
  }

  function finalizeSteerStopBarriersInTransaction(
    steerResult,
    providerStopStatus,
    leaseReleased,
    updatedAt,
  ) {
    const stopRows = database.prepare(`
      SELECT stop_id, result_json
      FROM runtime_stop_controls
      WHERE active_turn_id = ?
      ORDER BY committed_at ASC, stop_id ASC
    `).all(steerResult.old_turn.turn_id);
    for (const row of stopRows) {
      const currentStop = JSON.parse(row.result_json);
      if (currentStop.active_turn?.state !== 'redirecting') continue;
      const updatedStop = {
        ...currentStop,
        status: leaseReleased ? 'barrier_completed' : 'barrier_recovery_required',
        active_turn: {
          ...currentStop.active_turn,
          state: 'interrupted',
          turn_version: steerResult.old_turn.turn_version,
        },
        steering: {
          ...(currentStop.steering ?? {}),
          steer_id: steerResult.steer_id,
          winner: steerResult.winner,
          stop_barrier_id: steerResult.stop_barrier_id,
          priority_turn: { ...steerResult.priority_turn },
        },
        provider_stop_status: providerStopStatus,
        lease_released: leaseReleased,
        provider_stop_updated_at: updatedAt,
      };
      const write = database.prepare(`
        UPDATE runtime_stop_controls
        SET result_json = ?
        WHERE stop_id = ? AND result_json = ?
      `).run(JSON.stringify(updatedStop), row.stop_id, row.result_json);
      if (write.changes !== 1) {
        conflict('version_conflict', 'The steer stop-barrier projection lost its durable CAS.');
      }
    }
  }

  function recordSteerReconciliationOutcome(steerResult, providerIsolated) {
    if (typeof providerIsolated !== 'boolean') {
      throw new TypeError('providerIsolated must be a boolean');
    }
    const record = database.transaction(() => {
      const row = database.prepare(`
        SELECT result_json
        FROM runtime_steer_requests
        WHERE steer_id = ? AND conversation_id = ? AND target_turn_id = ?
          AND winner_control_id IS NULL
      `).get(
        steerResult?.steer_id,
        steerResult?.conversation_id,
        steerResult?.old_turn?.turn_id,
      );
      if (!row) {
        conflict('steer_precondition_failed', 'The reconciliation request is not durable.');
      }
      const current = JSON.parse(row.result_json);
      if (current.reconciliation_required !== true) {
        conflict('steer_precondition_failed', 'The steer request does not require reconciliation.');
      }
      if (current.reconciliation) {
        return { ...current, deduplicated: steerResult.deduplicated === true };
      }
      const updatedAt = now();
      const updated = {
        ...current,
        reconciliation: {
          status: providerIsolated
            ? 'provider_isolated_manual_recovery_required'
            : 'provider_isolation_unproven',
          provider_isolated: providerIsolated,
        },
        updated_at: updatedAt,
        deduplicated: false,
      };
      const write = database.prepare(`
        UPDATE runtime_steer_requests
        SET result_json = ?, updated_at = ?
        WHERE steer_id = ? AND result_json = ? AND winner_control_id IS NULL
      `).run(
        JSON.stringify(updated),
        updatedAt,
        current.steer_id,
        row.result_json,
      );
      if (write.changes !== 1) {
        conflict('version_conflict', 'The steer reconciliation outcome lost its durable CAS.');
      }
      return { ...updated, deduplicated: steerResult.deduplicated === true };
    });
    return record.immediate();
  }

  function completeSteer(steerResult, providerStopStatus) {
    if (!['confirmed', 'isolated'].includes(providerStopStatus)) {
      throw new TypeError('providerStopStatus must prove provider isolation');
    }
    const complete = database.transaction(() => {
      const row = database.prepare(`
        SELECT request_json, result_json, stop_barrier_id
        FROM runtime_steer_controls
        WHERE steer_id = ? AND conversation_id = ? AND target_turn_id = ?
      `).get(
        steerResult?.steer_id,
        steerResult?.conversation_id,
        steerResult?.old_turn?.turn_id,
      );
      if (!row) conflict('steer_precondition_failed', 'The durable steer control does not exist.');
      const current = JSON.parse(row.result_json);
      if (current.status !== 'redirecting') {
        return { ...current, deduplicated: steerResult.deduplicated === true };
      }
      const request = JSON.parse(row.request_json);
      const oldTurn = loadTurn(database, current.old_turn.turn_id);
      if (oldTurn.state !== 'redirecting') {
        conflict('version_conflict', 'The steer winner lost its redirecting turn fence.');
      }
      const completedAt = now();
      const sideEffectStatus = steerSideEffectStatus(
        oldTurn.turn_id,
        current.old_turn.attempt,
      );
      if (sideEffectStatus === 'unknown') {
        const error = createContractError({
          code: 'side_effect_unknown',
          category: 'provider',
          retryable: false,
          sideEffectStatus: 'unknown',
          userMessage: 'A late provider event reported an unknown side effect; steering work is blocked pending manual recovery.',
          occurredAt: completedAt,
        });
        const terminalEvent = transitionInTransaction(database, {
          turnId: oldTurn.turn_id,
          fromState: 'redirecting',
          toState: 'interrupted',
          fence: current.old_turn.attempt,
          provider,
          serviceInstanceId,
          occurredAt: completedAt,
          generateId,
          reasonCode: 'steer_side_effect_unknown',
          error,
          retainLease: true,
        });
        const updated = {
          ...current,
          status: 'failed',
          old_turn: {
            ...current.old_turn,
            state: 'interrupted',
            turn_version: terminalEvent.turn_version,
            side_effect_status: 'unknown',
          },
          priority_turn: {
            ...current.priority_turn,
            status: 'blocked_recovery',
            turn_id: null,
            state: null,
          },
          provider_stop_status: providerStopStatus,
          lease_released: false,
          error,
          updated_at: completedAt,
          deduplicated: false,
        };
        const write = database.prepare(`
          UPDATE runtime_steer_controls
          SET result_json = ?, updated_at = ?
          WHERE steer_id = ? AND result_json = ? AND priority_turn_id IS NULL
        `).run(
          JSON.stringify(updated),
          completedAt,
          current.steer_id,
          row.result_json,
        );
        if (write.changes !== 1) {
          conflict('version_conflict', 'The side-effect-unknown steer lost its durable CAS.');
        }
        finalizeSteerStopBarriersInTransaction(
          updated,
          providerStopStatus,
          false,
          completedAt,
        );
        const incident = markProviderStopUnknownInTransaction(
          {
            turn_id: oldTurn.turn_id,
            attempt: current.old_turn.attempt,
          },
          providerStopStatus,
        );
        const durableUpdated = { ...updated, incident };
        const incidentWrite = database.prepare(`
          UPDATE runtime_steer_controls
          SET result_json = ?
          WHERE steer_id = ? AND result_json = ?
        `).run(
          JSON.stringify(durableUpdated),
          current.steer_id,
          JSON.stringify(updated),
        );
        if (incidentWrite.changes !== 1) {
          conflict('version_conflict', 'The side-effect-unknown incident lost its steer CAS.');
        }
        return {
          ...durableUpdated,
          deduplicated: steerResult.deduplicated === true,
        };
      }
      const interruptionError = createContractError({
        code: 'turn_interrupted',
        category: 'conflict',
        retryable: false,
        sideEffectStatus,
        userMessage: 'The running turn was interrupted by /steer; completed side effects were not rolled back.',
        occurredAt: completedAt,
      });
      const terminalEvent = transitionInTransaction(database, {
        turnId: oldTurn.turn_id,
        fromState: 'redirecting',
        toState: 'interrupted',
        fence: current.old_turn.attempt,
        provider,
        serviceInstanceId,
        occurredAt: completedAt,
        generateId,
        reasonCode: 'steer_redirected',
        error: interruptionError,
      });
      workspaceLeases.releaseTurnAfterIsolation(oldTurn.turn_id, {
        reason: 'steer_provider_isolation_confirmed',
      });

      if (row.stop_barrier_id !== null) {
        const updated = {
          ...current,
          status: 'completed',
          stop_barrier_id: row.stop_barrier_id,
          old_turn: {
            ...current.old_turn,
            state: 'interrupted',
            turn_version: terminalEvent.turn_version,
            side_effect_status: sideEffectStatus,
          },
          priority_turn: {
            ...current.priority_turn,
            status: 'not_created',
            turn_id: null,
            state: null,
            stop_barrier_id: row.stop_barrier_id,
          },
          provider_stop_status: providerStopStatus,
          lease_released: true,
          error: null,
          updated_at: completedAt,
          deduplicated: false,
        };
        const write = database.prepare(`
          UPDATE runtime_steer_controls
          SET result_json = ?, updated_at = ?
          WHERE steer_id = ? AND result_json = ? AND priority_turn_id IS NULL
            AND stop_barrier_id = ?
        `).run(
          JSON.stringify(updated),
          completedAt,
          current.steer_id,
          row.result_json,
          row.stop_barrier_id,
        );
        if (write.changes !== 1) {
          conflict('version_conflict', 'The stopped steer completion lost its durable CAS.');
        }
        finalizeSteerStopBarriersInTransaction(
          updated,
          providerStopStatus,
          true,
          completedAt,
        );
        return { ...updated, deduplicated: steerResult.deduplicated === true };
      }

      database.prepare(`
        UPDATE runtime_conversations
        SET last_queue_sequence = last_queue_sequence + 1
        WHERE conversation_id = ?
      `).run(current.conversation_id);
      const queueSequence = database.prepare(`
        SELECT last_queue_sequence
        FROM runtime_conversations
        WHERE conversation_id = ?
      `).get(current.conversation_id).last_queue_sequence;
      const priorityTurnId = generateId('turn');
      const envelopeRow = database.prepare(`
        SELECT envelope_json
        FROM runtime_inbound_events
        WHERE inbound_event_id = ?
      `).get(request.inbound_event_id);
      const envelope = JSON.parse(envelopeRow.envelope_json);
      const providerInput = {
        kind: 'text',
        text: request.supplement,
        attachments: [],
      };
      database.prepare(`
        INSERT INTO runtime_turns (
          turn_id, conversation_id, lineage_id, inbound_event_id, state,
          turn_version, queue_sequence, provider_input_json,
          redirected_from_turn_id, created_at, committed_at
        ) VALUES (?, ?, ?, ?, 'queued', 2, ?, ?, ?, ?, ?)
      `).run(
        priorityTurnId,
        current.conversation_id,
        oldTurn.lineage_id,
        request.inbound_event_id,
        queueSequence,
        JSON.stringify(providerInput),
        oldTurn.turn_id,
        completedAt,
        completedAt,
      );
      database.prepare(`
        INSERT INTO runtime_turn_queue (
          conversation_id, queue_sequence, turn_id, status, priority, enqueued_at
        ) VALUES (?, ?, ?, 'queued', 1, ?)
      `).run(current.conversation_id, queueSequence, priorityTurnId, completedAt);
      bindPermissionToAcceptedTurnInTransaction(database, {
        turnId: priorityTurnId,
        actorId: envelope.actor.actor_id,
        conversationId: current.conversation_id,
        acceptedAt: completedAt,
        generateId,
      });

      const receivedEvent = buildLifecycleEvent({
        eventId: generateId('event'),
        traceId: envelope.trace_id,
        conversationId: current.conversation_id,
        turnId: priorityTurnId,
        lineageId: oldTurn.lineage_id,
        eventSequence: 1,
        turnVersion: 1,
        phase: 'received',
        occurredAt: envelope.received_at,
        persistedAt: completedAt,
        fromState: null,
        reasonCode: 'steer_committed',
        causationEventId: terminalEvent.event_id,
      });
      const queuedEvent = buildLifecycleEvent({
        eventId: generateId('event'),
        traceId: envelope.trace_id,
        conversationId: current.conversation_id,
        turnId: priorityTurnId,
        lineageId: oldTurn.lineage_id,
        eventSequence: 2,
        turnVersion: 2,
        phase: 'queued',
        occurredAt: completedAt,
        persistedAt: completedAt,
        fromState: 'received',
        reasonCode: 'steer_priority_queued',
        causationEventId: receivedEvent.event_id,
      });
      for (const event of [receivedEvent, queuedEvent]) {
        validateNormalizedEvent(event);
        database.prepare(`
          INSERT INTO runtime_normalized_events (
            event_id, turn_id, event_sequence, turn_version, event_json, persisted_at
          ) VALUES (?, ?, ?, ?, ?, ?)
        `).run(
          event.event_id,
          priorityTurnId,
          event.event_sequence,
          event.turn_version,
          JSON.stringify(event),
          completedAt,
        );
      }
      const deliveryCommand = buildInitialDeliveryCommand({
        envelope,
        traceId: envelope.trace_id,
        conversationId: current.conversation_id,
        turnId: priorityTurnId,
        lineageId: oldTurn.lineage_id,
        committedAt: completedAt,
        generateId,
        text: 'Steering request received.',
      });
      validateDeliveryCommand(deliveryCommand);
      const laneKey = initializeMainProjection(database, deliveryCommand);
      database.prepare(`
        INSERT INTO runtime_outbox (
          outbox_id, delivery_id, aggregate_type, aggregate_id, turn_id, control_id,
          lane_key, predecessor_delivery_id, aggregate_version, status, command_json,
          priority, supersedable, terminal, next_attempt_at, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, NULL, ?, NULL, 1, 'pending', ?, ?, 0, 0, ?, ?, ?)
      `).run(
        deliveryCommand.outbox_id,
        deliveryCommand.delivery_id,
        deliveryCommand.aggregate_type,
        deliveryCommand.aggregate_id,
        priorityTurnId,
        laneKey,
        JSON.stringify(deliveryCommand),
        deliveryCommand.priority,
        deliveryCommand.not_before,
        completedAt,
        completedAt,
      );
      stageMainProjection(database, { turn_id: priorityTurnId }, queuedEvent, { generateId });

      const updated = {
        ...current,
        status: 'completed',
        old_turn: {
          ...current.old_turn,
          state: 'interrupted',
          turn_version: terminalEvent.turn_version,
          side_effect_status: sideEffectStatus,
        },
        priority_turn: {
          status: 'queued',
          turn_id: priorityTurnId,
          state: 'queued',
          lineage_id: oldTurn.lineage_id,
          redirected_from_turn_id: oldTurn.turn_id,
          queue_sequence: queueSequence,
          priority: 1,
        },
        provider_stop_status: providerStopStatus,
        lease_released: true,
        error: null,
        updated_at: completedAt,
        deduplicated: false,
      };
      const write = database.prepare(`
        UPDATE runtime_steer_controls
        SET priority_turn_id = ?, result_json = ?, updated_at = ?
        WHERE steer_id = ? AND result_json = ? AND priority_turn_id IS NULL
      `).run(
        priorityTurnId,
        JSON.stringify(updated),
        completedAt,
        current.steer_id,
        row.result_json,
      );
      if (write.changes !== 1) {
        conflict('version_conflict', 'The steer completion lost its durable CAS.');
      }
      return { ...updated, deduplicated: steerResult.deduplicated === true };
    });
    return complete.immediate();
  }

  function failSteer(steerResult, providerStopStatus) {
    if (!['uncertain', 'terminal_unconfirmed'].includes(providerStopStatus)) {
      throw new TypeError('providerStopStatus must describe unproven provider isolation');
    }
    const fail = database.transaction(() => {
      const row = database.prepare(`
        SELECT result_json
        FROM runtime_steer_controls
        WHERE steer_id = ? AND conversation_id = ? AND target_turn_id = ?
      `).get(
        steerResult?.steer_id,
        steerResult?.conversation_id,
        steerResult?.old_turn?.turn_id,
      );
      if (!row) conflict('steer_precondition_failed', 'The durable steer control does not exist.');
      const current = JSON.parse(row.result_json);
      if (current.status !== 'redirecting') {
        return { ...current, deduplicated: steerResult.deduplicated === true };
      }
      const turn = loadTurn(database, current.old_turn.turn_id);
      if (turn.state !== 'redirecting') {
        conflict('version_conflict', 'The failed steer lost its redirecting turn fence.');
      }
      const failedAt = now();
      const error = createContractError({
        code: 'side_effect_unknown',
        category: 'provider',
        retryable: false,
        sideEffectStatus: 'unknown',
        userMessage: 'Provider interruption could not be confirmed; steering work is blocked pending manual recovery.',
        occurredAt: failedAt,
      });
      const terminalEvent = transitionInTransaction(database, {
        turnId: turn.turn_id,
        fromState: 'redirecting',
        toState: 'interrupted',
        fence: current.old_turn.attempt,
        provider,
        serviceInstanceId,
        occurredAt: failedAt,
        generateId,
        reasonCode: 'steer_provider_stop_unknown',
        error,
        retainLease: true,
      });
      const updated = {
        ...current,
        status: 'failed',
        old_turn: {
          ...current.old_turn,
          state: 'interrupted',
          turn_version: terminalEvent.turn_version,
          side_effect_status: 'unknown',
        },
        priority_turn: {
          ...current.priority_turn,
          status: 'blocked_recovery',
          turn_id: null,
          state: null,
        },
        provider_stop_status: providerStopStatus,
        lease_released: false,
        error,
        updated_at: failedAt,
        deduplicated: false,
      };
      const write = database.prepare(`
        UPDATE runtime_steer_controls
        SET result_json = ?, updated_at = ?
        WHERE steer_id = ? AND result_json = ? AND priority_turn_id IS NULL
      `).run(
        JSON.stringify(updated),
        failedAt,
        current.steer_id,
        row.result_json,
      );
      if (write.changes !== 1) {
        conflict('version_conflict', 'The failed steer lost its durable CAS.');
      }
      finalizeSteerStopBarriersInTransaction(
        updated,
        providerStopStatus,
        false,
        failedAt,
      );
      const incident = markProviderStopUnknownInTransaction(
        {
          turn_id: turn.turn_id,
          attempt: current.old_turn.attempt,
        },
        providerStopStatus,
      );
      const durableUpdated = { ...updated, incident };
      const incidentWrite = database.prepare(`
        UPDATE runtime_steer_controls
        SET result_json = ?
        WHERE steer_id = ? AND result_json = ?
      `).run(
        JSON.stringify(durableUpdated),
        current.steer_id,
        JSON.stringify(updated),
      );
      if (incidentWrite.changes !== 1) {
        conflict('version_conflict', 'The failed steer incident lost its durable CAS.');
      }
      return {
        ...durableUpdated,
        deduplicated: steerResult.deduplicated === true,
      };
    });
    return fail.immediate();
  }

  function cancelQueuedTurnInTransaction(
    turnId,
    cancelledAt,
    reasonCode = 'conversation_stopped',
  ) {
    const turn = loadTurn(database, turnId);
    if (turn.state !== 'queued' || turn.attempt_id !== null) {
      conflict('version_conflict', 'Only an unclaimed queued turn can be cancelled by stop.');
    }
    const event = buildEvent({
      turn,
      lastEvent: loadLastEvent(database, turn.turn_id),
      fence: null,
      provider: null,
      descriptor: {
        kind: 'turn_state_changed',
        phase: 'cancelled',
        payload: {
          from_state: 'queued',
          to_state: 'cancelled',
          reason_code: reasonCode,
        },
      },
      occurredAt: cancelledAt,
      generateId,
    });
    const turnUpdate = database.prepare(`
      UPDATE runtime_turns
      SET state = 'cancelled', turn_version = ?, committed_at = ?
      WHERE turn_id = ? AND state = 'queued' AND turn_version = ?
        AND attempt_id IS NULL AND attempt_no IS NULL AND lease_epoch IS NULL
    `).run(event.turn_version, cancelledAt, turn.turn_id, turn.turn_version);
    const queueUpdate = database.prepare(`
      UPDATE runtime_turn_queue
      SET status = 'cancelled', wait_reason = NULL
      WHERE turn_id = ? AND status = 'queued'
    `).run(turn.turn_id);
    if (turnUpdate.changes !== 1 || queueUpdate.changes !== 1) {
      conflict('version_conflict', 'The stop cutoff lost a queued turn compare-and-swap.');
    }
    const recoveryUpdate = database.prepare(`
      UPDATE runtime_reply_mapping_recoveries
      SET state = 'rejected', native_recovery_status = 'stopped',
        native_recovery_owner_service_instance_id = NULL,
        native_recovery_claim_expires_at = NULL, updated_at = ?
      WHERE turn_id = ? AND state = 'queued' AND bound_lineage_id IS NULL
    `).run(cancelledAt, turn.turn_id);
    if (turn.lineage_id === null && recoveryUpdate.changes !== 1) {
      conflict('version_conflict', 'The stop cutoff lost its queued recovery fence.');
    }
    persistEvent(database, turn, event, generateId);
    return event;
  }

  function clearUnstartedQueue({
    conversation_id: conversationId,
    through_queue_sequence: throughQueueSequence,
    expected_queue_version: expectedQueueVersion,
  }) {
    if (typeof conversationId !== 'string' || conversationId.length === 0) {
      throw new TypeError('conversation_id must be a non-empty string');
    }
    if (!Number.isSafeInteger(throughQueueSequence) || throughQueueSequence < 1) {
      throw new TypeError('through_queue_sequence must be a positive safe integer');
    }
    if (!Number.isSafeInteger(expectedQueueVersion) || expectedQueueVersion < 1) {
      throw new TypeError('expected_queue_version must be a positive safe integer');
    }
    const clear = database.transaction(() => {
      const conversation = database.prepare(`
        SELECT queue_version FROM runtime_conversations WHERE conversation_id = ?
      `).get(conversationId);
      if (!conversation) {
        conflict('not_found', `Conversation ${conversationId} does not exist.`);
      }
      if (conversation.queue_version !== expectedQueueVersion) {
        conflict('version_conflict', 'The queue aggregate version changed before clear.');
      }
      const clearedAt = now();
      const acquired = database.prepare(`
        UPDATE runtime_conversations
        SET queue_version = queue_version + 1
        WHERE conversation_id = ? AND queue_version = ?
      `).run(conversationId, expectedQueueVersion);
      if (acquired.changes !== 1) {
        conflict('version_conflict', 'The queue aggregate lost its clear CAS.');
      }
      const rows = database.prepare(`
        SELECT queue.turn_id
        FROM runtime_turn_queue AS queue
        JOIN runtime_turns AS turn ON turn.turn_id = queue.turn_id
        WHERE queue.conversation_id = ?
          AND queue.queue_sequence <= ?
          AND queue.status = 'queued'
          AND turn.state = 'queued'
          AND turn.attempt_id IS NULL
        ORDER BY queue.queue_sequence ASC
      `).all(conversationId, throughQueueSequence);
      const clearedTurnIds = [];
      for (const { turn_id: turnId } of rows) {
        cancelQueuedTurnInTransaction(turnId, clearedAt, 'queue_cleared_by_operator');
        clearedTurnIds.push(turnId);
      }
      const committed = database.prepare(`
        SELECT queue_version FROM runtime_conversations WHERE conversation_id = ?
      `).get(conversationId);
      return {
        previous_version: expectedQueueVersion,
        queue_version: committed.queue_version,
        cleared_turn_ids: clearedTurnIds,
        through_queue_sequence: throughQueueSequence,
        committed_at: clearedAt,
      };
    });
    return clear.immediate();
  }

  function cancelUpgradeImportedTurns(upgradeId) {
    if (typeof upgradeId !== 'string' || upgradeId.length === 0) {
      throw new TypeError('upgradeId must be a non-empty string');
    }
    const cancel = database.transaction(() => {
      const run = database.prepare(`
        SELECT state FROM runtime_upgrade_runs WHERE upgrade_id = ?
      `).get(upgradeId);
      if (!run || run.state !== 'rollback_required') {
        conflict('illegal_transition', 'Upgrade-import cancellation requires rollback_required.');
      }
      const rows = database.prepare(`
        SELECT migrated_turn_id AS turn_id
        FROM runtime_legacy_migration_records
        WHERE upgrade_id = ? AND migrated_turn_id IS NOT NULL AND imported_by_upgrade = 1
        ORDER BY created_at, legacy_kind, legacy_record_id
      `).all(upgradeId);
      for (const { turn_id: turnId } of rows) {
        const turn = loadTurn(database, turnId);
        const currentQueue = database.prepare(`
          SELECT status, wait_reason FROM runtime_turn_queue WHERE turn_id = ?
        `).get(turnId);
        if (turn.state === 'queued' && currentQueue?.status === 'cancelled'
          && currentQueue.wait_reason === 'upgrade_rollback_parked') continue;
        if (turn.state !== 'queued' || turn.attempt_id !== null) {
          conflict('version_conflict', 'Only unexecuted upgrade imports can be parked for retry.');
        }
        const queueUpdate = database.prepare(`
          UPDATE runtime_turn_queue
          SET status = 'cancelled', wait_reason = 'upgrade_rollback_parked'
          WHERE turn_id = ? AND status = 'queued'
        `).run(turnId);
        if (queueUpdate.changes !== 1) {
          conflict('version_conflict', 'Upgrade import parking lost its durable CAS.');
        }
      }
      return Object.freeze(rows.map(({ turn_id: turnId }) => turnId));
    });
    return cancel.immediate();
  }

  function requeueRolledBackUpgradeImportedTurn({
    previous_upgrade_id: previousUpgradeId,
    current_upgrade_id: currentUpgradeId,
    turn_id: turnId,
  }) {
    for (const [name, value] of Object.entries({ previousUpgradeId, currentUpgradeId, turnId })) {
      if (typeof value !== 'string' || value.length === 0) {
        throw new TypeError(`${name} must be a non-empty string`);
      }
    }
    const requeue = database.transaction(() => {
      const ownership = database.prepare(`
        SELECT previous.state AS previous_state, current.state AS current_state,
          legacy.imported_by_upgrade
        FROM runtime_legacy_migration_records AS legacy
        JOIN runtime_upgrade_runs AS previous ON previous.upgrade_id = legacy.upgrade_id
        JOIN runtime_upgrade_runs AS current ON current.upgrade_id = ?
        WHERE legacy.upgrade_id = ? AND legacy.migrated_turn_id = ?
      `).get(currentUpgradeId, previousUpgradeId, turnId);
      if (!ownership || ownership.previous_state !== 'rolled_back'
        || ownership.current_state !== 'migrating' || ownership.imported_by_upgrade !== 1) {
        conflict('illegal_transition', 'Legacy turn adoption requires exact rolled-back ownership.');
      }
      const turn = loadTurn(database, turnId);
      const queue = database.prepare(`
        SELECT status FROM runtime_turn_queue WHERE turn_id = ?
      `).get(turnId);
      const attempts = database.prepare(`
        SELECT COUNT(*) AS count FROM runtime_provider_attempts WHERE turn_id = ?
      `).get(turnId).count;
      if (turn.state !== 'queued' || queue?.status !== 'cancelled'
        || turn.attempt_id !== null || turn.attempt_no !== null || turn.lease_epoch !== null
        || attempts !== 0) {
        conflict(
          'version_conflict',
          'Rolled-back legacy turn was executed or changed and cannot be adopted.',
        );
      }
      const adoptedAt = now();
      const queueUpdate = database.prepare(`
        UPDATE runtime_turn_queue
        SET status = 'queued', wait_reason = 'maintenance'
        WHERE turn_id = ? AND status = 'cancelled'
          AND wait_reason = 'upgrade_rollback_parked'
      `).run(turnId);
      if (queueUpdate.changes !== 1) {
        conflict('version_conflict', 'Legacy turn adoption lost its durable CAS.');
      }
      return Object.freeze({ turn_id: turnId, adopted: true, adopted_at: adoptedAt });
    });
    return requeue.immediate();
  }

  function synchronizeSteerReconciliationWithStopInTransaction(stopResult) {
    const activeTurn = stopResult?.active_turn;
    if (!activeTurn?.turn_id) return;
    const requestRows = database.prepare(`
      SELECT steer_id, result_json
      FROM runtime_steer_requests
      WHERE target_turn_id = ? AND winner_control_id IS NULL
    `).all(activeTurn.turn_id);
    for (const row of requestRows) {
      const current = JSON.parse(row.result_json);
      if (current.reconciliation_required !== true) continue;
      const providerIsolated = stopResult.lease_released === true
        && ['confirmed', 'isolated'].includes(stopResult.provider_stop_status);
      const updated = {
        ...current,
        winner: {
          control: 'stop',
          control_id: stopResult.stop_id,
          turn_id: activeTurn.turn_id,
        },
        stop_id: stopResult.stop_id,
        old_turn: {
          ...current.old_turn,
          state: activeTurn.state,
          turn_version: activeTurn.turn_version,
        },
        provider_stop_status: stopResult.provider_stop_status,
        lease_released: stopResult.lease_released === true,
        reconciliation: {
          status: 'superseded_by_stop',
          provider_isolated: providerIsolated,
        },
        updated_at: stopResult.provider_stop_updated_at ?? now(),
        deduplicated: false,
      };
      const write = database.prepare(`
        UPDATE runtime_steer_requests
        SET result_json = ?, updated_at = ?
        WHERE steer_id = ? AND result_json = ? AND winner_control_id IS NULL
      `).run(
        JSON.stringify(updated),
        updated.updated_at,
        row.steer_id,
        row.result_json,
      );
      if (write.changes !== 1) {
        conflict('version_conflict', 'The stop-superseded steer result lost its CAS.');
      }
    }
  }

  function stopConversation({
    conversation_id: conversationId,
    stop_id: stopId,
    target_turn_id: targetTurnId = null,
    expected_turn_version: expectedTurnVersion = null,
    clear_unstarted_queue: clearUnstartedQueue = true,
  }) {
    if (typeof conversationId !== 'string' || conversationId.length === 0) {
      throw new TypeError('conversation_id must be a non-empty string');
    }
    if (typeof stopId !== 'string' || stopId.length === 0) {
      throw new TypeError('stop_id must be a non-empty string');
    }
    if ((targetTurnId === null) !== (expectedTurnVersion === null)) {
      throw new TypeError('target_turn_id and expected_turn_version must be provided together');
    }
    if (targetTurnId !== null && (typeof targetTurnId !== 'string' || targetTurnId.length === 0)) {
      throw new TypeError('target_turn_id must be a non-empty string');
    }
    if (expectedTurnVersion !== null
      && (!Number.isSafeInteger(expectedTurnVersion) || expectedTurnVersion < 1)) {
      throw new TypeError('expected_turn_version must be a positive safe integer');
    }
    if (typeof clearUnstartedQueue !== 'boolean') {
      throw new TypeError('clear_unstarted_queue must be a boolean');
    }
    // The IMMEDIATE transaction is the linearization point shared with inbound
    // acceptance: its queue sequence snapshot is the durable stop cutoff.
    const stop = database.transaction(() => {
      const existing = database.prepare(`
        SELECT conversation_id, result_json
        FROM runtime_stop_controls
        WHERE stop_id = ?
      `).get(stopId);
      if (existing) {
        if (existing.conversation_id !== conversationId) {
          conflict('idempotency_conflict', 'The stop ID belongs to another conversation.');
        }
        return { ...JSON.parse(existing.result_json), deduplicated: true };
      }
      const conversation = database.prepare(`
        SELECT last_queue_sequence
        FROM runtime_conversations
        WHERE conversation_id = ?
      `).get(conversationId);
      if (!conversation) {
        conflict('conversation_not_found', `Conversation ${conversationId} does not exist.`);
      }
      const stoppedAt = now();
      const cutoff = conversation.last_queue_sequence;
      const activeRow = database.prepare(`
        SELECT turn_id, turn_version
        FROM runtime_turns
        WHERE conversation_id = ?
          AND state IN ('starting', 'running', 'waiting_user', 'redirecting', 'recovering')
        LIMIT 1
      `).get(conversationId);
      if (targetTurnId !== null && (
        !activeRow
        || activeRow.turn_id !== targetTurnId
        || activeRow.turn_version !== expectedTurnVersion
      )) {
        conflict('version_conflict', 'The exact active turn changed before stop.');
      }
      let activeTurn = null;
      let attemptlessRecoveryStopped = false;
      let steering = null;
      let activeSteerUpdate = null;
      if (activeRow) {
        let turn = loadTurn(database, activeRow.turn_id);
        const previousState = turn.state;
        const turnContext = {
          conversation_id: turn.conversation_id,
          turn_id: turn.turn_id,
          attempt: {
            attempt_id: turn.attempt_id,
            attempt_no: turn.attempt_no,
            lease_epoch: turn.lease_epoch,
          },
        };
        const attemptlessRecovery = turn.lineage_id === null
          && turn.attempt_id === null
          && database.prepare(`
            SELECT 1
            FROM runtime_reply_mapping_recoveries
            WHERE turn_id = ?
              AND state IN (
                'notice_pending', 'native_recovery_claimed',
                'native_recovery_not_applicable', 'waiting_decision'
              )
              AND bound_lineage_id IS NULL
          `).get(turn.turn_id) !== undefined;
        if (attemptlessRecovery) {
          const interactionRows = database.prepare(`
            SELECT interaction_id, state, version, request_json
            FROM runtime_interactions
            WHERE turn_id = ? AND parent_type = 'recovery_control'
              AND state = 'pending'
          `).all(turn.turn_id);
          for (const interaction of interactionRows) {
            const request = JSON.parse(interaction.request_json);
            const cancelledRequest = {
              ...request,
              state: 'cancelled',
              version: request.version + 1,
            };
            validateInteractionRequest(cancelledRequest, { occurredAt: stoppedAt });
            const cancelled = database.prepare(`
              UPDATE runtime_interactions
              SET state = 'cancelled', version = ?, request_json = ?, updated_at = ?
              WHERE interaction_id = ? AND state = 'pending' AND version = ?
            `).run(
              cancelledRequest.version,
              JSON.stringify(cancelledRequest),
              stoppedAt,
              interaction.interaction_id,
              interaction.version,
            );
            if (cancelled.changes !== 1) {
              conflict('version_conflict', 'The stop lost its recovery decision fence.');
            }
          }
          const terminalEvent = transitionAttemptlessInTransaction(database, {
            turnId: turn.turn_id,
            fromState: previousState,
            toState: 'stopped',
            occurredAt: stoppedAt,
            generateId,
            reasonCode: 'conversation_stopped',
          });
          const queueUpdate = database.prepare(`
            UPDATE runtime_turn_queue
            SET status = 'stopped', wait_reason = NULL
            WHERE turn_id = ? AND status = 'claimed'
          `).run(turn.turn_id);
          const recoveryUpdate = database.prepare(`
            UPDATE runtime_reply_mapping_recoveries
            SET state = 'rejected', native_recovery_status = 'stopped',
              native_recovery_owner_service_instance_id = NULL,
              native_recovery_claim_expires_at = NULL, updated_at = ?
            WHERE turn_id = ?
              AND state IN (
                'notice_pending', 'native_recovery_claimed',
                'native_recovery_not_applicable', 'waiting_decision'
              )
              AND bound_lineage_id IS NULL
          `).run(stoppedAt, turn.turn_id);
          if (queueUpdate.changes !== 1 || recoveryUpdate.changes !== 1) {
            conflict('version_conflict', 'The stop lost its active recovery fence.');
          }
          activeTurn = {
            turn_id: turn.turn_id,
            previous_state: previousState,
            previous_version: turn.turn_version,
            state: 'stopped',
            turn_version: terminalEvent.turn_version,
            attempt: { ...turnContext.attempt },
          };
          attemptlessRecoveryStopped = true;
        } else if (previousState === 'redirecting') {
          const steerRow = database.prepare(`
            SELECT steer_id, stop_barrier_id, result_json
            FROM runtime_steer_controls
            WHERE target_turn_id = ?
          `).get(turn.turn_id);
          if (!steerRow) {
            conflict('version_conflict', 'Redirecting turn has no durable steer winner.');
          }
          const steerResult = JSON.parse(steerRow.result_json);
          activeTurn = {
            turn_id: turn.turn_id,
            previous_state: previousState,
            previous_version: turn.turn_version,
            state: 'redirecting',
            turn_version: turn.turn_version,
            attempt: { ...turnContext.attempt },
          };
          steering = {
            steer_id: steerRow.steer_id,
            winner: steerResult.winner,
            stop_barrier_id: steerRow.stop_barrier_id ?? stopId,
            priority_turn: {
              ...steerResult.priority_turn,
              stop_barrier_id: steerRow.stop_barrier_id ?? stopId,
            },
          };
        } else {
          turn = settleBlockingInteractionsForStop(turnContext, turn, stoppedAt);
          if (previousState === 'recovering') {
            database.prepare(`
              UPDATE runtime_execution_recoveries
              SET state = 'stopped', recovery_version = recovery_version + 1, updated_at = ?
              WHERE turn_id = ? AND state IN ('waiting_decision', 'authorized')
            `).run(stoppedAt, turn.turn_id);
          }
          const terminalEvent = transitionInTransaction(database, {
            turnId: turn.turn_id,
            fromState: previousState,
            toState: 'stopped',
            fence: turnContext.attempt,
            provider,
            serviceInstanceId,
            occurredAt: stoppedAt,
            generateId,
            reasonCode: 'conversation_stopped',
            retainLease: true,
          });
          activeTurn = {
            turn_id: turn.turn_id,
            previous_state: previousState,
            previous_version: turn.turn_version,
            state: 'stopped',
            turn_version: terminalEvent.turn_version,
            attempt: { ...turnContext.attempt },
          };
          const steerRow = database.prepare(`
            SELECT steer_id, target_turn_id, result_json
            FROM runtime_steer_controls
            WHERE priority_turn_id = ?
          `).get(turn.turn_id);
          if (steerRow) {
            const steerResult = JSON.parse(steerRow.result_json);
            const updatedSteerResult = {
              ...steerResult,
              stop_barrier_id: stopId,
              priority_turn: {
                ...steerResult.priority_turn,
                status: 'stopped',
                state: 'stopped',
                stop_barrier_id: stopId,
              },
              updated_at: stoppedAt,
            };
            activeSteerUpdate = { steerRow, updatedSteerResult };
            steering = {
              steer_id: steerRow.steer_id,
              winner: steerResult.winner,
              priority_turn: updatedSteerResult.priority_turn,
            };
          }
        }
      }
      const queuedRows = clearUnstartedQueue ? database.prepare(`
        SELECT queue.turn_id
        FROM runtime_turn_queue AS queue
        JOIN runtime_turns AS turn ON turn.turn_id = queue.turn_id
        WHERE queue.conversation_id = ?
          AND queue.queue_sequence <= ?
          AND queue.status = 'queued'
          AND turn.state = 'queued'
        ORDER BY queue.queue_sequence ASC
      `).all(conversationId, cutoff) : [];
      const cancelledTurnIds = [];
      for (const { turn_id: turnId } of queuedRows) {
        cancelQueuedTurnInTransaction(turnId, stoppedAt);
        cancelledTurnIds.push(turnId);
      }
      let queuedSteerUpdate = null;
      if (cancelledTurnIds.length > 0) {
        const placeholders = cancelledTurnIds.map(() => '?').join(', ');
        const steerRow = database.prepare(`
          SELECT steer_id, target_turn_id, result_json
          FROM runtime_steer_controls
          WHERE priority_turn_id IN (${placeholders})
          ORDER BY committed_at DESC
          LIMIT 1
        `).get(...cancelledTurnIds);
        if (steerRow) {
          const steerResult = JSON.parse(steerRow.result_json);
          const updatedSteerResult = {
            ...steerResult,
            stop_barrier_id: stopId,
            priority_turn: {
              ...steerResult.priority_turn,
              status: 'cancelled',
              state: 'cancelled',
              stop_barrier_id: stopId,
            },
            updated_at: stoppedAt,
          };
          queuedSteerUpdate = { steerRow, updatedSteerResult };
          steering = {
            steer_id: steerRow.steer_id,
            winner: steerResult.winner,
            priority_turn: updatedSteerResult.priority_turn,
          };
        }
      }
      const status = activeTurn?.state === 'redirecting'
        ? 'barrier_applied'
        : activeTurn
          ? 'stopped'
        : (cancelledTurnIds.length > 0 ? 'queue_cleared' : 'noop');
      const result = {
        status,
        stop_id: stopId,
        conversation_id: conversationId,
        stop_cutoff_queue_sequence: cutoff,
        active_turn: activeTurn,
        cancelled_turn_ids: cancelledTurnIds,
        steering,
        provider_stop_status: activeTurn?.state === 'stopped' && !attemptlessRecoveryStopped
          ? 'pending'
          : 'not_applicable',
        lease_released: activeTurn === null || attemptlessRecoveryStopped,
        provider_stop_updated_at: stoppedAt,
        committed_at: stoppedAt,
        deduplicated: false,
      };
      database.prepare(`
        INSERT INTO runtime_stop_controls (
          stop_id, conversation_id, stop_cutoff_queue_sequence,
          active_turn_id, result_json, committed_at
        ) VALUES (?, ?, ?, ?, ?, ?)
      `).run(
        stopId,
        conversationId,
        cutoff,
        activeTurn?.turn_id ?? null,
        JSON.stringify(result),
        stoppedAt,
      );
      synchronizeSteerReconciliationWithStopInTransaction(result);
      if (
        activeTurn?.state === 'redirecting'
        && steering.stop_barrier_id === stopId
      ) {
        const steerRow = database.prepare(`
          SELECT result_json
          FROM runtime_steer_controls
          WHERE steer_id = ? AND target_turn_id = ?
        `).get(steering.steer_id, activeTurn.turn_id);
        const steerResult = JSON.parse(steerRow.result_json);
        const updatedSteerResult = {
          ...steerResult,
          stop_barrier_id: stopId,
          priority_turn: {
            ...steerResult.priority_turn,
            stop_barrier_id: stopId,
          },
          updated_at: stoppedAt,
        };
        const barrier = database.prepare(`
          UPDATE runtime_steer_controls
          SET stop_barrier_id = ?, result_json = ?, updated_at = ?
          WHERE steer_id = ? AND target_turn_id = ?
            AND stop_barrier_id IS NULL AND result_json = ?
        `).run(
          stopId,
          JSON.stringify(updatedSteerResult),
          stoppedAt,
          steering.steer_id,
          activeTurn.turn_id,
          steerRow.result_json,
        );
        if (barrier.changes !== 1) {
          conflict('version_conflict', 'The stop barrier lost the steer control CAS.');
        }
      } else if (queuedSteerUpdate) {
        const { steerRow, updatedSteerResult } = queuedSteerUpdate;
        const barrier = database.prepare(`
          UPDATE runtime_steer_controls
          SET stop_barrier_id = ?, result_json = ?, updated_at = ?
          WHERE steer_id = ? AND target_turn_id = ?
            AND stop_barrier_id IS NULL AND result_json = ?
        `).run(
          stopId,
          JSON.stringify(updatedSteerResult),
          stoppedAt,
          steerRow.steer_id,
          steerRow.target_turn_id,
          steerRow.result_json,
        );
        if (barrier.changes !== 1) {
          conflict('version_conflict', 'The queued steer stop barrier lost its durable CAS.');
        }
      } else if (activeSteerUpdate) {
        const { steerRow, updatedSteerResult } = activeSteerUpdate;
        const barrier = database.prepare(`
          UPDATE runtime_steer_controls
          SET stop_barrier_id = ?, result_json = ?, updated_at = ?
          WHERE steer_id = ? AND target_turn_id = ?
            AND stop_barrier_id IS NULL AND result_json = ?
        `).run(
          stopId,
          JSON.stringify(updatedSteerResult),
          stoppedAt,
          steerRow.steer_id,
          steerRow.target_turn_id,
          steerRow.result_json,
        );
        if (barrier.changes !== 1) {
          conflict('version_conflict', 'The active steer stop barrier lost its durable CAS.');
        }
      }
      return result;
    });
    return stop.immediate();
  }

  function loadStopProviderOutcomeInTransaction(stopResult) {
    if (!stopResult || typeof stopResult.stop_id !== 'string') {
      throw new TypeError('stopResult must identify a durable stop control');
    }
    const row = database.prepare(`
      SELECT conversation_id, result_json
      FROM runtime_stop_controls
      WHERE stop_id = ?
    `).get(stopResult.stop_id);
    if (!row) conflict('stop_not_found', `Stop ${stopResult.stop_id} does not exist.`);
    if (row.conversation_id !== stopResult.conversation_id) {
      conflict('idempotency_conflict', 'The stop ID belongs to another conversation.');
    }
    const current = JSON.parse(row.result_json);
    if (current.active_turn?.turn_id !== stopResult.active_turn?.turn_id) {
      conflict('version_conflict', 'The provider stop outcome lost its active turn fence.');
    }
    const currentStatus = current.provider_stop_status
      ?? (current.active_turn ? 'pending' : 'not_applicable');
    return { current, currentStatus, row };
  }

  function updateStopProviderOutcomeInTransaction(
    stopResult,
    providerStopStatus,
    leaseReleased,
  ) {
    if (!['confirmed', 'isolated', 'uncertain', 'terminal_unconfirmed'].includes(
      providerStopStatus,
    )) {
      throw new TypeError('providerStopStatus must be a canonical provider stop outcome');
    }
    if (typeof leaseReleased !== 'boolean') {
      throw new TypeError('leaseReleased must be a boolean');
    }
    const { current, currentStatus, row } = loadStopProviderOutcomeInTransaction(stopResult);
    if (currentStatus !== 'pending') {
      synchronizeSteerReconciliationWithStopInTransaction(current);
      return { ...current, deduplicated: stopResult.deduplicated === true };
    }
    const updated = {
      ...current,
      provider_stop_status: providerStopStatus,
      lease_released: leaseReleased,
      provider_stop_updated_at: now(),
      deduplicated: false,
    };
    const write = database.prepare(`
      UPDATE runtime_stop_controls
      SET result_json = ?
      WHERE stop_id = ? AND conversation_id = ? AND result_json = ?
    `).run(
      JSON.stringify(updated),
      stopResult.stop_id,
      stopResult.conversation_id,
      row.result_json,
    );
    if (write.changes !== 1) {
      conflict('version_conflict', 'The provider stop outcome lost its durable CAS.');
    }
    synchronizeSteerReconciliationWithStopInTransaction(updated);
    return { ...updated, deduplicated: stopResult.deduplicated === true };
  }

  function releaseStoppedExecutorLeaseInTransaction(stopResult) {
    const activeTurn = stopResult?.active_turn;
    if (!activeTurn) return false;
    const turn = loadTurn(database, activeTurn.turn_id);
    if (turn.state !== 'stopped') {
      conflict('turn_terminal', 'The stopped turn no longer has stopped authority.');
    }
    workspaceLeases.releaseTurnAfterIsolation(turn.turn_id, {
      reason: 'stop_provider_isolation_confirmed',
    });
    const lease = database.prepare(`
      SELECT lease_owner, turn_id, attempt_id, attempt_no, lease_epoch
      FROM runtime_executor_leases
      WHERE conversation_id = ?
    `).get(turn.conversation_id);
    if (!lease || lease.lease_owner === null) return true;
    const released = database.prepare(`
      UPDATE runtime_executor_leases
      SET lease_owner = NULL, turn_id = NULL, attempt_id = NULL,
        attempt_no = NULL, lease_expires_at = NULL, updated_at = ?
      WHERE conversation_id = ? AND lease_owner = ? AND turn_id = ?
        AND attempt_id = ? AND attempt_no = ? AND lease_epoch = ?
    `).run(
      now(),
      turn.conversation_id,
      serviceInstanceId,
      turn.turn_id,
      turn.attempt_id,
      turn.attempt_no,
      turn.lease_epoch,
    );
    if (released.changes !== 1) {
      conflict('stale_attempt', 'The stopped turn lost its executor lease fence.');
    }
    return true;
  }

  function recordStopProviderOutcome(stopResult, providerStopStatus) {
    const record = database.transaction(() => {
      const { current, currentStatus } = loadStopProviderOutcomeInTransaction(stopResult);
      if (currentStatus !== 'pending') {
        return { ...current, deduplicated: stopResult.deduplicated === true };
      }
      const leaseReleased = releaseStoppedExecutorLeaseInTransaction(stopResult);
      return updateStopProviderOutcomeInTransaction(
        stopResult,
        providerStopStatus,
        leaseReleased,
      );
    });
    return record.immediate();
  }

  function assertCurrentFence(turnContext) {
    const turn = loadTurn(database, turnContext.turn_id);
    assertTurnContextFence(turn, turnContext);
    return Object.freeze({ state: turn.state });
  }

  function assertRecoverableFence(turnContext) {
    const turn = loadTurn(database, turnContext.turn_id);
    assertTurnContextIsolationFence(turn, turnContext);
    return Object.freeze({ state: turn.state });
  }

  function isWorkspaceRecoveryRequired(turnContext) {
    return Boolean(turnContext.workspace)
      && workspaceLeases.isRecoveryRequired(turnContext.workspace);
  }

  function isRecoveryNotificationDelivered(turnContext) {
    const turn = loadTurn(database, turnContext.turn_id);
    if (turn.state === 'timed_out') {
      const timedOutTerminal = database.prepare(`
        SELECT event_sequence
        FROM runtime_normalized_events
        WHERE turn_id = ?
          AND json_extract(event_json, '$.kind') = 'turn_state_changed'
          AND json_extract(event_json, '$.payload.to_state') = 'timed_out'
        ORDER BY event_sequence DESC
        LIMIT 1
      `).get(turnContext.turn_id);
      if (!timedOutTerminal) return false;
      return database.prepare(`
        SELECT 1
        FROM runtime_outbox
        WHERE turn_id = ? AND aggregate_type = 'turn_main' AND status = 'delivered'
          AND CAST(json_extract(command_json, '$.event_sequence_through') AS INTEGER) >= ?
        LIMIT 1
      `).get(turnContext.turn_id, timedOutTerminal.event_sequence) !== undefined;
    }
    const recovery = database.prepare(`
      SELECT event_sequence, json_extract(event_json, '$.error.code') AS error_code
      FROM runtime_normalized_events
      WHERE turn_id = ?
        AND json_extract(event_json, '$.kind') IN (
          'recovery_started', 'recovery_waiting_decision'
        )
      ORDER BY event_sequence DESC
      LIMIT 1
    `).get(turnContext.turn_id);
    if (!recovery) return true;
    if (!WORKSPACE_NOTIFICATION_BARRIER_CODES.has(recovery.error_code)) return true;
    return database.prepare(`
      SELECT 1
      FROM runtime_outbox
      WHERE turn_id = ? AND aggregate_type = 'turn_main' AND status = 'delivered'
        AND CAST(json_extract(command_json, '$.event_sequence_through') AS INTEGER) >= ?
      LIMIT 1
    `).get(turnContext.turn_id, recovery.event_sequence) !== undefined;
  }

  function claimOrphanedWorkspaceRecoveries() {
    const recoveries = [];
    for (const candidate of workspaceLeases.listRecoveryCandidates()) {
      const claim = database.transaction(() => {
        const claimedAt = now();
        const turn = loadTurn(database, candidate.holder_turn_id);
        if (!['starting', 'running', 'waiting_user', 'recovering', 'timed_out'].includes(turn.state)) {
          return null;
        }
        const executorLease = database.prepare(`
          SELECT * FROM runtime_executor_leases
          WHERE conversation_id = ? AND turn_id = ?
        `).get(turn.conversation_id, turn.turn_id);
        if (
          !executorLease
          || executorLease.lease_owner !== candidate.holder_service_instance_id
          || executorLease.attempt_id !== turn.attempt_id
          || executorLease.attempt_no !== turn.attempt_no
          || executorLease.lease_epoch !== turn.lease_epoch
          || executorLease.lease_expires_at > claimedAt
        ) return null;

        let resident = provider === 'claude'
          ? isResidentConversation(turn.conversation_id)
          : null;
        if (
          resident
          && resident.owner_service_instance_id !== null
          && resident.owner_service_instance_id !== candidate.holder_service_instance_id
        ) return null;
        if (resident?.owner_expires_at && resident.owner_expires_at > claimedAt) return null;

        const executorClaimed = database.prepare(`
          UPDATE runtime_executor_leases
          SET lease_owner = ?, lease_expires_at = ?, updated_at = ?
          WHERE conversation_id = ? AND turn_id = ? AND lease_owner = ?
            AND attempt_id = ? AND attempt_no = ? AND lease_epoch = ?
            AND lease_expires_at <= ?
        `).run(
          serviceInstanceId,
          new Date(Date.parse(claimedAt) + leaseDurationMs).toISOString(),
          claimedAt,
          turn.conversation_id,
          turn.turn_id,
          candidate.holder_service_instance_id,
          turn.attempt_id,
          turn.attempt_no,
          turn.lease_epoch,
          claimedAt,
        );
        if (executorClaimed.changes !== 1) return null;

        if (resident) {
          const residentClaimed = database.prepare(`
            UPDATE runtime_executor_residents
            SET owner_service_instance_id = ?, owner_epoch = owner_epoch + 1,
              owner_expires_at = ?, last_used_at = ?
            WHERE conversation_id = ? AND provider = 'claude' AND owner_epoch = ?
              AND (
                owner_service_instance_id IS NULL
                OR (
                  owner_service_instance_id = ?
                  AND (owner_expires_at IS NULL OR owner_expires_at <= ?)
                )
              )
          `).run(
            serviceInstanceId,
            residentOwnerExpiresAt(claimedAt),
            claimedAt,
            turn.conversation_id,
            resident.owner_epoch,
            candidate.holder_service_instance_id,
            claimedAt,
          );
          if (residentClaimed.changes !== 1) {
            conflict('stale_attempt', 'The orphaned resident recovery lost its owner fence.');
          }
          resident = isResidentConversation(turn.conversation_id);
        }

        const workspace = workspaceLeases.adoptUncertainForRecovery(candidate);
        const attempt = Object.freeze({
          attempt_id: turn.attempt_id,
          attempt_no: turn.attempt_no,
          lease_epoch: turn.lease_epoch,
        });
        const envelope = JSON.parse(turn.envelope_json);
        const recoveryContext = Object.freeze({
          conversation_id: turn.conversation_id,
          turn_id: turn.turn_id,
          lineage_id: turn.lineage_id,
          provider_native_id: turn.provider_native_id,
          trace_id: envelope.trace_id,
          input: envelope.content,
          interaction_authority: envelope.actor?.authenticated === true
            ? [{ type: 'actor', actor_id: envelope.actor.actor_id }]
            : [],
          resident: resident === null ? null : Object.freeze({
            owner_epoch: resident.owner_epoch,
          }),
          workspace,
          attempt,
        });
        if (turn.state === 'timed_out') {
          return recoveryContext;
        }
        if (!['recovering', 'timed_out'].includes(turn.state)) {
          transitionInTransaction(database, {
            turnId: turn.turn_id,
            fromState: turn.state,
            toState: 'recovering',
            fence: attempt,
            provider,
            serviceInstanceId,
            occurredAt: claimedAt,
            generateId,
            reasonCode: 'workspace_lease_orphaned',
          });
        }
        const recoveringTurn = loadTurn(database, turn.turn_id);
        const recoveryError = {
          code: 'workspace_lease_orphaned',
          category: 'conflict',
          retryable: false,
          side_effect_status: 'unknown',
          user_message: 'Workspace ownership expired during a service restart.',
          occurred_at: claimedAt,
        };
        const recoveryId = generateId('recovery');
        let event = buildEvent({
          turn: recoveringTurn,
          lastEvent: loadLastEvent(database, turn.turn_id),
          fence: attempt,
          provider,
          descriptor: {
            kind: 'recovery_started',
            phase: 'recovering',
            payload: {
              recovery_id: recoveryId,
              recovery_of_turn_id: turn.turn_id,
              recovery_of_lineage_id: turn.lineage_id,
              side_effect_status: 'unknown',
            },
            error: recoveryError,
          },
          occurredAt: claimedAt,
          generateId,
        });
        commitTurnEvent(database, {
          turn: recoveringTurn,
          event,
          fence: attempt,
          nextState: recoveringTurn.state,
          staleMessage: 'The orphaned workspace recovery lost its adopted fence.',
          generateId,
        });
        const waitingTurn = loadTurn(database, turn.turn_id);
        event = buildEvent({
          turn: waitingTurn,
          lastEvent: loadLastEvent(database, turn.turn_id),
          fence: attempt,
          provider,
          descriptor: {
            kind: 'recovery_waiting_decision',
            phase: 'recovering',
            payload: {
              recovery_id: recoveryId,
              recovery_of_turn_id: turn.turn_id,
              recovery_of_lineage_id: turn.lineage_id,
              side_effect_status: 'unknown',
            },
            error: recoveryError,
          },
          occurredAt: claimedAt,
          generateId,
        });
        commitTurnEvent(database, {
          turn: waitingTurn,
          event,
          fence: attempt,
          nextState: waitingTurn.state,
          staleMessage: 'The orphaned recovery decision boundary lost its adopted fence.',
          generateId,
        });
        return recoveryContext;
      });
      const recovered = claim.immediate();
      if (recovered) recoveries.push(recovered);
    }
    return recoveries;
  }

  function assertWorkspaceWritable(turnContext, approvalFence = undefined) {
    const turn = loadTurn(database, turnContext.turn_id);
    assertTurnContextFence(turn, turnContext);
    if (!turnContext.workspace) {
      conflict('stale_workspace_lease', 'The turn has no workspace lease fence.');
    }
    const workspace = workspaceLeases.assertWritable(turnContext.workspace);
    if (approvalFence === undefined) return workspace;
    if (!approvalFence || typeof approvalFence !== 'object' || Array.isArray(approvalFence)) {
      conflict('provider_context_invalid', 'A Codex write approval requires a complete fence.');
    }
    const approvalWorkspace = approvalFence.workspace;
    const providerAttempt = approvalFence.provider_attempt;
    if (
      !['item/commandExecution/requestApproval', 'item/fileChange/requestApproval']
        .includes(approvalFence.action_kind)
      || typeof approvalFence.connection_id !== 'string'
      || approvalFence.connection_id.length === 0
      || approvalFence.conversation_id !== turn.conversation_id
      || approvalFence.core_turn_id !== turn.turn_id
      || approvalFence.lineage_id !== turn.lineage_id
      || approvalFence.executor_instance_id !== turnContext.executor_instance_id
      || approvalFence.provider_thread_id !== turn.provider_native_id
      || typeof approvalFence.provider_turn_id !== 'string'
      || approvalFence.provider_turn_id.length === 0
      || typeof approvalFence.provider_item_id !== 'string'
      || approvalFence.provider_item_id.length === 0
      || approvalFence.provider_approval_id !== null
        && (typeof approvalFence.provider_approval_id !== 'string'
          || approvalFence.provider_approval_id.length === 0)
      || approvalFence.environment_id !== null
      || !providerAttempt
      || !sameFence(providerAttempt, turnContext.attempt)
      || !approvalWorkspace
      || approvalWorkspace.workspace_lease_id !== workspace.workspace_lease_id
      || approvalWorkspace.workspace_root !== workspace.workspace_root
      || approvalWorkspace.mode !== 'writable'
      || approvalWorkspace.holder_service_instance_id !== serviceInstanceId
      || approvalWorkspace.holder_conversation_id !== turn.conversation_id
      || approvalWorkspace.holder_turn_id !== turn.turn_id
      || approvalWorkspace.lease_epoch !== workspace.lease_epoch
    ) {
      conflict(
        'provider_context_invalid',
        'The Codex write approval does not match its durable turn, owner, or workspace fence.',
      );
    }
    let canonicalCwd;
    try {
      canonicalCwd = normalizeWorkspaceRoot(approvalFence.cwd, {
        base: workspace.workspace_root,
      });
    } catch {
      conflict('provider_context_invalid', 'The Codex write approval has an invalid working directory.');
    }
    if (canonicalCwd !== workspace.workspace_root) {
      conflict('provider_context_invalid', 'The Codex write approval changed its workspace cwd.');
    }
    if (!Array.isArray(approvalFence.write_paths) || approvalFence.write_paths.length === 0) {
      conflict('provider_context_invalid', 'The Codex write approval has no bounded write path.');
    }
    for (const writePath of approvalFence.write_paths) {
      let canonicalWritePath;
      try {
        canonicalWritePath = normalizeWorkspaceRoot(writePath, { base: workspace.workspace_root });
      } catch {
        conflict('provider_context_invalid', 'The Codex write approval has an invalid write path.');
      }
      if (
        canonicalWritePath !== workspace.workspace_root
        && !canonicalWritePath.startsWith(`${workspace.workspace_root}${path.sep}`)
      ) {
        conflict('provider_context_invalid', 'The Codex write approval escaped its workspace root.');
      }
    }
    const checkedAt = now();
    const writer = database.prepare(`
      SELECT lease_expires_at
      FROM runtime_executor_leases
      WHERE conversation_id = ? AND lease_owner = ? AND turn_id = ?
        AND attempt_id = ? AND attempt_no = ? AND lease_epoch = ?
    `).get(
      turn.conversation_id,
      serviceInstanceId,
      turn.turn_id,
      turnContext.attempt.attempt_id,
      turnContext.attempt.attempt_no,
      turnContext.attempt.lease_epoch,
    );
    const attempt = database.prepare(`
      SELECT state
      FROM runtime_provider_attempts
      WHERE attempt_id = ? AND turn_id = ? AND attempt_no = ? AND lease_epoch = ?
        AND service_instance_id = ? AND executor_instance_id = ?
    `).get(
      turnContext.attempt.attempt_id,
      turn.turn_id,
      turnContext.attempt.attempt_no,
      turnContext.attempt.lease_epoch,
      serviceInstanceId,
      turnContext.executor_instance_id,
    );
    if (
      !writer
      || writer.lease_expires_at <= checkedAt
      || !attempt
      || !['starting', 'running', 'waiting_user'].includes(attempt.state)
    ) {
      conflict('stale_attempt', 'The Codex write approval lost its current writer ownership.');
    }
    return Object.freeze({
      status: 'current',
      conversation_id: turn.conversation_id,
      turn_id: turn.turn_id,
      executor_instance_id: turnContext.executor_instance_id,
      provider_native_id: turn.provider_native_id,
      provider_turn_id: approvalFence.provider_turn_id,
      provider_item_id: approvalFence.provider_item_id,
      workspace_lease_id: workspace.workspace_lease_id,
      workspace_lease_epoch: workspace.lease_epoch,
      checked_at: checkedAt,
    });
  }

  function recordProviderEventActivityInTransaction(turnContext, observedAt) {
    if (!Number.isFinite(Date.parse(observedAt))) {
      throw new TypeError('provider event activity requires an ISO timestamp');
    }
    const updated = database.prepare(`
      UPDATE runtime_provider_attempts
      SET last_provider_event_at = ?, updated_at = ?
      WHERE attempt_id = ? AND turn_id = ? AND attempt_no = ? AND lease_epoch = ?
        AND service_instance_id = ? AND executor_instance_id = ?
        AND state IN ('starting', 'running', 'waiting_user')
    `).run(
      observedAt,
      observedAt,
      turnContext.attempt.attempt_id,
      turnContext.turn_id,
      turnContext.attempt.attempt_no,
      turnContext.attempt.lease_epoch,
      serviceInstanceId,
      turnContext.executor_instance_id,
    );
    if (updated.changes !== 1) {
      conflict('stale_attempt', 'Provider event activity lost its durable attempt fence.');
    }
  }

  function recordProviderEventActivity(turnContext) {
    const record = database.transaction(() => {
      const observedAt = now();
      const turn = loadTurn(database, turnContext.turn_id);
      assertActiveFence(database, turn, turnContext.attempt, serviceInstanceId);
      recordProviderEventActivityInTransaction(turnContext, observedAt);
      return Object.freeze({ status: 'recorded', observed_at: observedAt });
    });
    return record.immediate();
  }

  function bindProviderNativeId(turnContext, providerNativeId) {
    if (typeof providerNativeId !== 'string' || providerNativeId.trim().length === 0) {
      throw new TypeError('providerNativeId must be a non-empty string');
    }
    const bind = database.transaction(() => {
      const turn = loadTurn(database, turnContext.turn_id);
      assertTurnContextFence(turn, turnContext);
      if (!['starting', 'running'].includes(turn.state)) {
        conflict('illegal_transition', `Lineage binding is invalid while turn is ${turn.state}.`);
      }
      if (turn.lineage_id === null) {
        conflict('lineage_resolution_pending', 'A provider native ID requires a bound lineage.');
      }
      const boundAt = now();
      recordProviderEventActivityInTransaction(turnContext, boundAt);
      if (turn.provider !== null || turn.provider_native_id !== null) {
        if (turn.provider === provider && turn.provider_native_id === providerNativeId) {
          database.prepare(`
            UPDATE runtime_lineages
            SET provider_native_state = 'valid'
            WHERE lineage_id = ? AND conversation_id = ?
              AND provider = ? AND provider_native_id = ?
          `).run(
            turn.lineage_id,
            turn.conversation_id,
            provider,
            providerNativeId,
          );
          return Object.freeze({
            provider,
            provider_native_id: providerNativeId,
            newly_bound: false,
          });
        }
        conflict(
          'provider_context_invalid',
          'The lineage is already bound to a different provider native ID.',
        );
      }
      const updated = database.prepare(`
        UPDATE runtime_lineages
        SET provider = ?, provider_native_id = ?, provider_native_id_bound_at = ?,
          provider_native_state = 'valid'
        WHERE lineage_id = ? AND conversation_id = ?
          AND provider IS NULL AND provider_native_id IS NULL
      `).run(
        provider,
        providerNativeId,
        boundAt,
        turn.lineage_id,
        turn.conversation_id,
      );
      if (updated.changes !== 1) {
        conflict('version_conflict', 'The provider lineage binding changed concurrently.');
      }
      return Object.freeze({
        provider,
        provider_native_id: providerNativeId,
        newly_bound: true,
      });
    });
    return bind.immediate();
  }

  function appendAdapterEvent(turnContext, descriptor) {
    if (descriptor.kind === 'turn_state_changed') {
      conflict('illegal_transition', 'Provider adapters cannot author canonical state transitions.');
    }
    const append = database.transaction(() => {
      const occurredAt = now();
      let turn = loadTurn(database, turnContext.turn_id);
      assertTurnContextFence(turn, turnContext);
      const restoreWaitingUser = turn.state === 'waiting_user';
      if (restoreWaitingUser) {
        transitionInTransaction(database, {
          turnId: turnContext.turn_id,
          fromState: 'waiting_user',
          toState: 'running',
          fence: turnContext.attempt,
          provider,
          serviceInstanceId,
          occurredAt,
          generateId,
        });
        turn = loadTurn(database, turnContext.turn_id);
      }
      if (turn.state !== 'running') {
        conflict('illegal_transition', `Adapter output is invalid while turn is ${turn.state}.`);
      }
      const descriptorNativeId = descriptor.provider_native_id ?? null;
      if (turn.provider_native_id !== null || descriptorNativeId !== null) {
        assertBoundProviderNativeId(turn, provider, descriptorNativeId);
      }
      recordProviderEventActivityInTransaction(turnContext, occurredAt);
      const event = buildEvent({
        turn,
        lastEvent: loadLastEvent(database, turn.turn_id),
        fence: turnContext.attempt,
        provider,
        descriptor: {
          ...descriptor,
          phase: 'running',
        },
        occurredAt,
        generateId,
      });
      commitTurnEvent(database, {
        turn,
        event,
        fence: turnContext.attempt,
        nextState: 'running',
        staleMessage: 'The adapter event lost its provider attempt fence.',
        generateId,
      });
      if (restoreWaitingUser) {
        transitionInTransaction(database, {
          turnId: turnContext.turn_id,
          fromState: 'running',
          toState: 'waiting_user',
          fence: turnContext.attempt,
          provider,
          serviceInstanceId,
          occurredAt,
          generateId,
        });
      }
      return event;
    });
    return append.immediate();
  }

  function recordProviderEventDiagnostic(
    turnContext,
    descriptor,
    { reasonCode = 'stale_attempt' } = {},
  ) {
    if (!turnContext || typeof turnContext.turn_id !== 'string') {
      throw new TypeError('turnContext must identify a turn');
    }
    if (typeof reasonCode !== 'string' || reasonCode.length === 0) {
      throw new TypeError('reasonCode must be a non-empty string');
    }
    const turn = loadTurn(database, turnContext.turn_id);
    const eventKind = descriptor?.type === 'normalized_event'
      ? descriptor.event?.kind
      : (descriptor?.kind ?? descriptor?.type ?? 'provider_event');
    const diagnostic = {
      diagnostic_id: generateId('provider-event-diagnostic'),
      turn_id: turn.turn_id,
      conversation_id: turn.conversation_id,
      provider,
      attempt_id: turnContext.attempt?.attempt_id ?? null,
      attempt_no: turnContext.attempt?.attempt_no ?? null,
      lease_epoch: turnContext.attempt?.lease_epoch ?? null,
      current_turn_state: turn.state,
      event_kind: typeof eventKind === 'string' && eventKind.length > 0
        ? eventKind
        : 'provider_event',
      reason_code: reasonCode,
      descriptor_json: JSON.stringify(descriptor ?? null),
      observed_at: now(),
    };
    database.prepare(`
      INSERT INTO runtime_provider_event_diagnostics (
        diagnostic_id, turn_id, conversation_id, provider,
        attempt_id, attempt_no, lease_epoch, current_turn_state,
        event_kind, reason_code, descriptor_json, observed_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      diagnostic.diagnostic_id,
      diagnostic.turn_id,
      diagnostic.conversation_id,
      diagnostic.provider,
      diagnostic.attempt_id,
      diagnostic.attempt_no,
      diagnostic.lease_epoch,
      diagnostic.current_turn_state,
      diagnostic.event_kind,
      diagnostic.reason_code,
      diagnostic.descriptor_json,
      diagnostic.observed_at,
    );
    return Object.freeze(diagnostic);
  }

  function recordProviderRuntimeEvidence(turnContext, evidence) {
    if (!evidence || typeof evidence !== 'object' || Array.isArray(evidence)) {
      throw new TypeError('runtime evidence must be an object');
    }
    if (
      typeof evidence.runtime_instance_id !== 'string'
      || evidence.runtime_instance_id.length === 0
      || typeof evidence.handle_kind !== 'string'
      || evidence.handle_kind.length === 0
      || typeof evidence.controllable !== 'boolean'
    ) {
      throw new TypeError('runtime evidence requires identity, handle kind, and controllability');
    }
    if (Object.hasOwn(evidence, 'process')) {
      const process = evidence.process;
      if (
        !process
        || typeof process !== 'object'
        || Array.isArray(process)
        || !Number.isSafeInteger(process.pid)
        || process.pid <= 0
        || !(process.pgid === null || Number.isSafeInteger(process.pgid) && process.pgid > 0)
        || typeof process.started_at !== 'string'
        || !Number.isFinite(Date.parse(process.started_at))
        || process.diagnostic_only !== true
      ) {
        throw new TypeError('process runtime evidence must be complete and diagnostic-only');
      }
    }
    const recordedAt = now();
    const turn = loadTurn(database, turnContext.turn_id);
    assertActiveFence(database, turn, turnContext.attempt, serviceInstanceId);
    const updated = database.prepare(`
      UPDATE runtime_provider_attempts
      SET runtime_instance_id = ?, runtime_evidence_json = ?,
        updated_at = ?
      WHERE attempt_id = ? AND turn_id = ? AND attempt_no = ? AND lease_epoch = ?
        AND service_instance_id = ? AND executor_instance_id = ?
        AND state IN ('starting', 'running')
    `).run(
      evidence.runtime_instance_id,
      JSON.stringify(evidence),
      recordedAt,
      turnContext.attempt.attempt_id,
      turnContext.turn_id,
      turnContext.attempt.attempt_no,
      turnContext.attempt.lease_epoch,
      serviceInstanceId,
      turnContext.executor_instance_id,
    );
    if (updated.changes !== 1) {
      conflict('stale_attempt', 'Runtime evidence lost its provider attempt identity fence.');
    }
    return Object.freeze({ status: 'recorded', recorded_at: recordedAt });
  }

  function isConversationEvictable(conversationId) {
    if (typeof conversationId !== 'string' || conversationId.length === 0) {
      throw new TypeError('conversationId must be a non-empty string');
    }
    const blockingTurn = database.prepare(`
      SELECT 1
      FROM runtime_turns
      WHERE conversation_id = ?
        AND state IN (
          'queued', 'starting', 'running', 'waiting_user',
          'redirecting', 'recovering', 'retrying'
        )
      LIMIT 1
    `).get(conversationId);
    return blockingTurn === undefined
      && !workspaceLeases.hasBlockingBackgroundWork(conversationId);
  }

  function evictIdleExecutor({
    conversation_id: conversationId,
    executor_instance_id: executorInstanceId,
    expected_executor_version: expectedExecutorVersion,
  }) {
    if (typeof conversationId !== 'string' || conversationId.length === 0) {
      throw new TypeError('conversation_id must be a non-empty string');
    }
    if (typeof executorInstanceId !== 'string' || executorInstanceId.length === 0) {
      throw new TypeError('executor_instance_id must be a non-empty string');
    }
    if (!Number.isSafeInteger(expectedExecutorVersion) || expectedExecutorVersion < 1) {
      throw new TypeError('expected_executor_version must be a positive safe integer');
    }
    const evict = database.transaction(() => {
      const resident = database.prepare(`
        SELECT conversation_id, provider, owner_service_instance_id, owner_epoch
        FROM runtime_executor_residents
        WHERE conversation_id = ? AND provider = ?
      `).get(conversationId, provider);
      const attempt = database.prepare(`
        SELECT executor_instance_id, attempt_no
        FROM runtime_provider_attempts
        WHERE conversation_id = ?
        ORDER BY updated_at DESC, attempt_no DESC
        LIMIT 1
      `).get(conversationId);
      const active = database.prepare(`
        SELECT turn_version FROM runtime_turns
        WHERE conversation_id = ?
          AND state IN ('starting', 'running', 'waiting_user', 'redirecting', 'recovering')
        LIMIT 1
      `).get(conversationId);
      if (!resident || attempt?.executor_instance_id !== executorInstanceId) {
        conflict('not_found', 'The canonical executor target is not resident.');
      }
      const executorVersion = Math.max(
        1,
        resident.owner_epoch ?? 0,
        active?.turn_version ?? 0,
        attempt.attempt_no ?? 0,
      );
      if (executorVersion !== expectedExecutorVersion) {
        conflict('version_conflict', 'The executor aggregate changed before eviction.');
      }
      if (!isConversationEvictable(conversationId)) {
        conflict('version_conflict', 'The executor is no longer idle and evictable.');
      }
      const removed = database.prepare(`
        DELETE FROM runtime_executor_residents
        WHERE conversation_id = ? AND provider = ?
          AND owner_epoch = ?
          AND (owner_service_instance_id = ? OR owner_service_instance_id IS NULL)
      `).run(
        conversationId,
        provider,
        resident.owner_epoch,
        serviceInstanceId,
      );
      if (removed.changes !== 1) {
        conflict('version_conflict', 'The executor resident fence changed before eviction.');
      }
      return {
        evicted: true,
        executor_instance_id: executorInstanceId,
        previous_version: executorVersion,
        executor_version: executorVersion + 1,
      };
    });
    return evict.immediate();
  }

  function decideRecovery({
    recovery_id: recoveryId,
    conversation_id: conversationId,
    turn_id: turnId,
    expected_recovery_version: expectedRecoveryVersion,
    decision,
  }) {
    if (typeof recoveryId !== 'string' || recoveryId.length === 0
      || typeof conversationId !== 'string' || conversationId.length === 0
      || typeof turnId !== 'string' || turnId.length === 0) {
      throw new TypeError('recovery_id, conversation_id, and turn_id are required strings');
    }
    if (!Number.isSafeInteger(expectedRecoveryVersion) || expectedRecoveryVersion < 1) {
      throw new TypeError('expected_recovery_version must be a positive safe integer');
    }
    if (!['confirmed', 'rejected'].includes(decision)) {
      throw new TypeError('decision must be confirmed or rejected');
    }
    const decide = database.transaction(() => {
      const executionRecovery = database.prepare(`
        SELECT recovery.*, interaction.request_json,
          interaction.state AS interaction_state,
          interaction.version AS interaction_version,
          'execution' AS recovery_kind
        FROM runtime_execution_recoveries AS recovery
        JOIN runtime_interactions AS interaction
          ON interaction.interaction_id = recovery.interaction_id
        JOIN runtime_turns AS turn ON turn.turn_id = recovery.turn_id
        WHERE recovery.recovery_id = ? AND recovery.turn_id = ?
          AND turn.conversation_id = ?
      `).get(recoveryId, turnId, conversationId);
      const recovery = executionRecovery ?? database.prepare(`
        SELECT recovery.*, interaction.interaction_id, interaction.request_json,
          interaction.state AS interaction_state,
          interaction.version AS interaction_version,
          'reply_mapping' AS recovery_kind
        FROM runtime_reply_mapping_recoveries AS recovery
        JOIN runtime_interactions AS interaction
          ON interaction.parent_type = 'recovery_control'
          AND interaction.parent_id = recovery.recovery_id
        JOIN runtime_turns AS turn ON turn.turn_id = recovery.turn_id
        WHERE recovery.recovery_id = ? AND recovery.turn_id = ?
          AND turn.conversation_id = ?
      `).get(recoveryId, turnId, conversationId);
      if (!recovery) conflict('not_found', 'The recovery aggregate does not exist.');
      if (recovery.state !== 'waiting_decision'
        || recovery.recovery_version !== expectedRecoveryVersion) {
        conflict('version_conflict', 'The recovery decision lost its aggregate CAS.');
      }
      if (recovery.interaction_state !== 'pending') {
        conflict('version_conflict', 'The recovery interaction is no longer pending.');
      }
      const decidedAt = now();
      const request = JSON.parse(recovery.request_json);
      const closedRequest = { ...request, state: 'cancelled', version: request.version + 1 };
      validateInteractionRequest(closedRequest, { occurredAt: decidedAt });
      const interactionUpdate = database.prepare(`
        UPDATE runtime_interactions
        SET state = 'cancelled', version = ?, request_json = ?, updated_at = ?
        WHERE interaction_id = ? AND state = 'pending' AND version = ?
      `).run(
        closedRequest.version,
        JSON.stringify(closedRequest),
        decidedAt,
        recovery.interaction_id,
        recovery.interaction_version,
      );
      const recoveryState = decision === 'confirmed'
        ? (recovery.recovery_kind === 'execution' ? 'authorized' : 'native_recovery_not_applicable')
        : (recovery.recovery_kind === 'execution' ? 'stopped' : 'rejected');
      const recoveryUpdate = recovery.recovery_kind === 'execution'
        ? database.prepare(`
          UPDATE runtime_execution_recoveries
          SET state = ?, recovery_version = recovery_version + 1, updated_at = ?
          WHERE recovery_id = ? AND turn_id = ? AND state = 'waiting_decision'
            AND recovery_version = ?
        `).run(
          recoveryState,
          decidedAt,
          recoveryId,
          turnId,
          expectedRecoveryVersion,
        )
        : database.prepare(`
          UPDATE runtime_reply_mapping_recoveries
          SET state = ?, recovery_version = recovery_version + 1,
            native_recovery_status = ?,
            native_recovery_owner_service_instance_id = ?,
            native_recovery_claim_expires_at = ?, updated_at = ?
          WHERE recovery_id = ? AND turn_id = ? AND state = 'waiting_decision'
            AND bound_lineage_id IS NULL AND recovery_version = ?
        `).run(
          recoveryState,
          decision === 'confirmed' ? 'authorized_fallback' : 'rejected',
          decision === 'confirmed' ? serviceInstanceId : null,
          decision === 'confirmed' ? residentOwnerExpiresAt(decidedAt) : null,
          decidedAt,
          recoveryId,
          turnId,
          expectedRecoveryVersion,
        );
      if (interactionUpdate.changes !== 1 || recoveryUpdate.changes !== 1) {
        conflict('version_conflict', 'The recovery decision lost its durable CAS.');
      }
      if (decision === 'rejected') {
        if (recovery.recovery_kind === 'execution') {
          const turn = loadTurn(database, turnId);
          transitionInTransaction(database, {
            turnId,
            fromState: 'recovering',
            toState: 'stopped',
            fence: {
              attempt_id: recovery.attempt_id,
              attempt_no: recovery.attempt_no,
              lease_epoch: recovery.lease_epoch,
            },
            provider,
            serviceInstanceId,
            occurredAt: decidedAt,
            generateId,
            reasonCode: 'execution_recovery_stopped_by_operator',
            requireActiveLease: false,
            retainLease: true,
          });
          database.prepare(`
            UPDATE runtime_provider_attempts
            SET state = 'stopped', side_effect_status = 'unknown', updated_at = ?
            WHERE turn_id = ? AND attempt_id = ? AND attempt_no = ? AND lease_epoch = ?
          `).run(
            decidedAt,
            turn.turn_id,
            recovery.attempt_id,
            recovery.attempt_no,
            recovery.lease_epoch,
          );
        } else {
          transitionAttemptlessInTransaction(database, {
            turnId,
            fromState: 'recovering',
            toState: 'stopped',
            occurredAt: decidedAt,
            generateId,
            reasonCode: 'reply_mapping_recovery_rejected',
          });
        }
        database.prepare(`
          UPDATE runtime_turn_queue
          SET status = 'stopped', wait_reason = NULL
          WHERE turn_id = ? AND status = 'claimed'
        `).run(turnId);
      } else if (recovery.recovery_kind === 'execution') {
        database.prepare(`
          UPDATE runtime_turn_queue
          SET wait_reason = 'execution_recovery_authorized'
          WHERE turn_id = ? AND status = 'claimed'
        `).run(turnId);
      } else {
        const queued = database.prepare(`
          UPDATE runtime_turn_queue
          SET wait_reason = 'reply_mapping_recovery_binding'
          WHERE turn_id = ? AND status = 'claimed'
            AND wait_reason = 'reply_mapping_recovery_decision'
        `).run(turnId);
        if (queued.changes !== 1) {
          conflict('version_conflict', 'The reply-mapping recovery lost its queue fence.');
        }
        completeReplyMappingRecovery({ recovery_id: recoveryId, turn_id: turnId }, {
          status: 'authorized_fallback',
          recovery_id: recoveryId,
          side_effect_status: 'unknown',
        });
      }
      const recoveryTable = recovery.recovery_kind === 'execution'
        ? 'runtime_execution_recoveries'
        : 'runtime_reply_mapping_recoveries';
      const versionRow = database.prepare(`
        SELECT recovery_version FROM ${recoveryTable}
        WHERE recovery_id = ?
      `).get(recoveryId);
      return {
        decision,
        recovery_turn_id: null,
        previous_version: expectedRecoveryVersion,
        recovery_version: versionRow.recovery_version,
      };
    });
    return decide.immediate();
  }

  function startWorkspaceBackgroundWork(turnContext, providerTaskId) {
    const turn = loadTurn(database, turnContext.turn_id);
    assertTurnContextFence(turn, turnContext);
    return workspaceLeases.startBackgroundWork(turnContext.workspace, {
      provider_task_id: providerTaskId,
    });
  }

  function finishWorkspaceBackgroundWork(turnContext, providerTaskId, outcome) {
    const turn = loadTurn(database, turnContext.turn_id);
    assertTurnContextFence(turn, turnContext);
    return workspaceLeases.finishBackgroundWork(turnContext.workspace, {
      provider_task_id: providerTaskId,
      outcome,
    });
  }

  function releaseExecutorResident(conversationId, ownerEpoch = null) {
    if (typeof conversationId !== 'string' || conversationId.length === 0) {
      throw new TypeError('conversationId must be a non-empty string');
    }
    const released = database.prepare(`
      DELETE FROM runtime_executor_residents
      WHERE conversation_id = ? AND provider = ?
        AND (owner_service_instance_id = ? OR owner_service_instance_id IS NULL)
        AND (? IS NULL OR owner_epoch = ?)
        AND NOT EXISTS (
          SELECT 1
          FROM runtime_turns
          WHERE conversation_id = ?
            AND state IN (
              'queued', 'starting', 'running', 'waiting_user',
              'redirecting', 'recovering', 'retrying'
            )
        )
    `).run(
      conversationId,
      provider,
      serviceInstanceId,
      ownerEpoch,
      ownerEpoch,
      conversationId,
    );
    return released.changes === 1;
  }

  function requestInteraction(turnContext, descriptor) {
    const request = database.transaction(() => {
      const requestedAt = now();
      let turn = loadTurn(database, turnContext.turn_id);
      if (!['running', 'waiting_user'].includes(turn.state)) {
        conflict('illegal_transition', `Interaction requests are invalid while turn is ${turn.state}.`);
      }
      assertTurnContextFence(turn, turnContext);
      const ordinal = database.prepare(`
        SELECT COALESCE(MAX(ordinal), 0) + 1 AS ordinal
        FROM runtime_interactions
        WHERE turn_id = ?
      `).get(turn.turn_id).ordinal;
      const envelope = JSON.parse(turn.envelope_json);
      const interaction = {
        contract: 'zylos.interaction-request',
        contract_version: '1.0',
        trace_id: envelope.trace_id,
        interaction_id: generateId('interaction'),
        conversation_id: turn.conversation_id,
        turn_id: turn.turn_id,
        lineage_id: turn.lineage_id,
        control_id: null,
        parent_type: 'provider_turn',
        tool_use_id: descriptor.tool_use_id ?? null,
        ordinal,
        kind: descriptor.kind,
        prompt: descriptor.prompt,
        choices: descriptor.choices,
        authorized_subjects: descriptor.authorized_subjects,
        allowed_sources: descriptor.allowed_sources,
        runtime_fence: {
          provider_attempt_id: turnContext.attempt.attempt_id,
          lease_epoch: turnContext.attempt.lease_epoch,
          provider_interaction_ref: descriptor.provider_interaction_ref,
        },
        state: 'pending',
        version: 1,
        handoff_state: 'not_started',
        created_at: requestedAt,
        expires_at: new Date(
          Date.parse(requestedAt) + interactionTimeoutMs,
        ).toISOString(),
        card_delivery_id: null,
      };
      validateInteractionRequest(interaction, { occurredAt: requestedAt });
      recordProviderEventActivityInTransaction(turnContext, requestedAt);
      database.prepare(`
        INSERT INTO runtime_interactions (
          interaction_id, conversation_id, turn_id, lineage_id, parent_type, parent_id, ordinal,
          state, version, handoff_state, handoff_version, request_json,
          created_at, updated_at
        ) VALUES (?, ?, ?, ?, 'provider_turn', ?, ?, 'pending', 1, 'not_started', NULL, ?, ?, ?)
      `).run(
        interaction.interaction_id,
        interaction.conversation_id,
        interaction.turn_id,
        interaction.lineage_id,
        interaction.turn_id,
        interaction.ordinal,
        JSON.stringify(interaction),
        requestedAt,
        requestedAt,
      );
      if (turn.state === 'running') {
        transitionInTransaction(database, {
          turnId: turn.turn_id,
          fromState: 'running',
          toState: 'waiting_user',
          fence: turnContext.attempt,
          provider,
          serviceInstanceId,
          occurredAt: requestedAt,
          generateId,
          reasonCode: 'interaction_requested',
        });
        turn = loadTurn(database, turn.turn_id);
      }
      const event = buildEvent({
        turn,
        lastEvent: loadLastEvent(database, turn.turn_id),
        fence: turnContext.attempt,
        provider,
        descriptor: {
          kind: 'interaction_requested',
          phase: 'waiting_user',
          payload: {
            interaction_id: interaction.interaction_id,
            ordinal: interaction.ordinal,
            interaction_version: interaction.version,
            handoff_version: null,
            kind: interaction.kind,
            prompt: interaction.prompt,
            choices: structuredClone(interaction.choices),
            allowed_sources: [...interaction.allowed_sources],
          },
        },
        occurredAt: requestedAt,
        generateId,
      });
      commitTurnEvent(database, {
        turn,
        event,
        fence: turnContext.attempt,
        nextState: 'waiting_user',
        staleMessage: 'The interaction request lost its provider attempt fence.',
        generateId,
      });
      return interaction;
    });
    return request.immediate();
  }

  function commitInteractionAnswer(answer, { replyToMessageId = null } = {}) {
    const commit = database.transaction(() => {
      const committedAt = now();
      const payloadHash = validateAndHashInteractionAnswer(answer, committedAt);
      const existingByKey = database.prepare(`
        SELECT answer_id, idempotency_key, payload_hash, answer_json, result_json
        FROM runtime_interaction_answers
        WHERE idempotency_key = ?
      `).get(answer.idempotency_key);
      const existingByAnswerId = database.prepare(`
        SELECT idempotency_key
        FROM runtime_interaction_answers
        WHERE answer_id = ?
      `).get(answer.answer_id);
      if (
        existingByAnswerId
        && existingByAnswerId.idempotency_key !== answer.idempotency_key
      ) {
        conflict('idempotency_conflict', 'The answer ID was already used by another answer.');
      }
      const existingAnswer = existingByKey;
      if (existingAnswer) {
        let existingPayloadHash = existingAnswer.payload_hash;
        if (existingPayloadHash === null) {
          const storedAnswer = JSON.parse(existingAnswer.answer_json);
          existingPayloadHash = validateAndHashInteractionAnswer(storedAnswer, committedAt);
        }
        const replay = resolveIdempotencyReplay({
          idempotency_key: existingAnswer.idempotency_key,
          payload_hash: existingPayloadHash,
        }, {
          idempotency_key: answer.idempotency_key,
          payload_hash: payloadHash,
        }, { occurredAt: committedAt });
        if (replay.status === 'conflict') {
          conflict(replay.error.code, replay.error.user_message);
        }
        const firstResult = JSON.parse(existingAnswer.result_json);
        const duplicateResult = {
          ...firstResult,
          trace_id: answer.trace_id,
          status: 'duplicate',
        };
        try {
          validateInteractionAnswerResultReplay(firstResult, duplicateResult, {
            occurredAt: committedAt,
          });
        } catch (error) {
          translateContractError(error);
        }
        return duplicateResult;
      }
      const row = database.prepare(`
        SELECT request_json
        FROM runtime_interactions
        WHERE interaction_id = ?
      `).get(answer.interaction_id);
      if (!row) conflict('interaction_not_found', `Interaction ${answer.interaction_id} does not exist.`);
      const request = JSON.parse(row.request_json);
      if (request.state === 'expired') {
        conflict('interaction_expired', 'The interaction has expired.');
      }
      if (
        request.state === 'cancelled'
        && request.terminal_reason === 'parent_timed_out'
      ) {
        conflict('turn_terminal', 'The parent turn timed out before this answer arrived.');
      }
      if (request.state !== 'pending') {
        conflict('interaction_already_answered', 'Only a pending interaction can accept an answer.');
      }
      const turn = loadTurn(database, request.turn_id);
      const interactions = database.prepare(`
        SELECT request_json
        FROM runtime_interactions
        WHERE turn_id = ?
        ORDER BY ordinal ASC
      `).all(request.turn_id).map(({ request_json: requestJson }) => JSON.parse(requestJson));
      try {
        validateInteractionAnswerAgainstRequest(answer, request, {
          interactions,
          requestScope: requestScopeFromTurn(turn),
          actorCapabilities: [],
          occurredAt: committedAt,
        });
      } catch (error) {
        translateContractError(error);
      }
      assertMainCardReplyMapping(database, answer, request, replyToMessageId);
      const isProviderHandoff = request.parent_type === 'provider_turn';
      const expectedTurnState = isProviderHandoff ? 'waiting_user' : 'recovering';
      if (turn.state !== expectedTurnState) {
        conflict('illegal_transition', `Interaction answers are invalid while turn is ${turn.state}.`);
      }
      const fence = {
        attempt_id: turn.attempt_id,
        attempt_no: turn.attempt_no,
        lease_epoch: turn.lease_epoch,
      };
      if (isProviderHandoff) {
        if (
          request.runtime_fence.provider_attempt_id !== fence.attempt_id
          || request.runtime_fence.lease_epoch !== fence.lease_epoch
        ) {
          conflict('stale_attempt', 'The interaction no longer matches the current runtime fence.');
        }
        assertActiveFence(database, turn, fence, serviceInstanceId);
        assertResidentOwner(turn.conversation_id);
      } else if (request.parent_type !== 'recovery_control') {
        conflict('illegal_transition', 'This executor store cannot answer security control interactions.');
      } else {
        const executionRecovery = database.prepare(`
          SELECT state, notice_event_sequence
          FROM runtime_execution_recoveries
          WHERE recovery_id = ? AND turn_id = ? AND interaction_id = ?
        `).get(request.control_id, request.turn_id, request.interaction_id);
        if (executionRecovery) {
          if (executionRecovery.state !== 'waiting_decision') {
            conflict('interaction_already_answered', 'Execution recovery is no longer awaiting a decision.');
          }
          const deliveredNotice = database.prepare(`
            SELECT 1
            FROM runtime_projection_snapshots AS projection
            JOIN runtime_outbox AS outbox
              ON outbox.outbox_id = projection.materialized_outbox_id
            WHERE projection.turn_id = ?
              AND projection.event_sequence_through >= ?
              AND outbox.status = 'delivered'
            LIMIT 1
          `).get(request.turn_id, executionRecovery.notice_event_sequence);
          if (!deliveredNotice) {
            conflict(
              'side_effect_unknown',
              'The execution recovery decision must wait for its delivered user notice.',
            );
          }
        }
      }

      validateInteractionTransition({
        from: 'pending',
        to: 'answer_committed',
        occurredAt: committedAt,
      });
      const handoffId = generateId('handoff');
      const updatedRequest = {
        ...request,
        state: 'answer_committed',
        version: request.version + 1,
        handoff_state: 'pending',
      };
      validateInteractionRequest(updatedRequest, { occurredAt: committedAt });
      const handoff = {
        handoff_id: handoffId,
        interaction_id: request.interaction_id,
        answer_id: answer.answer_id,
        parent_type: request.parent_type,
        state: 'pending',
        provider_attempt_id: isProviderHandoff ? fence.attempt_id : null,
        handoff_attempt_id: null,
        handoff_attempt_no: null,
        lease_epoch: isProviderHandoff ? fence.lease_epoch : null,
        claimed_by: null,
        claimed_at: null,
        last_send_started_at: null,
        provider_acked_at: null,
        handoff_deadline_at: request.expires_at,
        reason_code: null,
        error: null,
        side_effect_status: 'none',
      };
      validateInteractionHandoff(handoff, { occurredAt: committedAt });

      const interactionUpdate = database.prepare(`
        UPDATE runtime_interactions
        SET state = 'answer_committed', version = ?, handoff_state = 'pending',
          handoff_version = 1, request_json = ?, updated_at = ?
        WHERE interaction_id = ? AND state = 'pending' AND version = ?
      `).run(
        updatedRequest.version,
        JSON.stringify(updatedRequest),
        committedAt,
        request.interaction_id,
        request.version,
      );
      if (interactionUpdate.changes !== 1) {
        conflict('version_conflict', 'The interaction answer lost its state/version compare-and-swap.');
      }
      database.prepare(`
        INSERT INTO runtime_interaction_handoffs (
          handoff_id, interaction_id, answer_id, state, parent_type, provider_attempt_id,
          handoff_attempt_id, handoff_attempt_no, lease_epoch, record_json,
          created_at, updated_at
        ) VALUES (?, ?, ?, 'pending', ?, ?, NULL, NULL, ?, ?, ?, ?)
      `).run(
        handoff.handoff_id,
        handoff.interaction_id,
        handoff.answer_id,
        handoff.parent_type,
        handoff.provider_attempt_id,
        handoff.lease_epoch,
        JSON.stringify(handoff),
        committedAt,
        committedAt,
      );

      const eventDescriptor = {
          kind: 'interaction_answer_committed',
          phase: expectedTurnState,
          payload: {
            interaction_id: request.interaction_id,
            ordinal: request.ordinal,
            interaction_version: updatedRequest.version,
            handoff_version: 1,
          },
        };
      const event = persistInteractionLifecycleEventInTransaction(database, {
        turn,
        state: expectedTurnState,
        descriptor: eventDescriptor,
        fence,
        provider,
        occurredAt: committedAt,
        generateId,
        staleMessage: 'The interaction answer commit lost its turn fence.',
      });
      const result = {
        contract: 'zylos.interaction-answer-result',
        contract_version: '1.0',
        trace_id: answer.trace_id,
        interaction_id: request.interaction_id,
        answer_id: answer.answer_id,
        idempotency_key: answer.idempotency_key,
        status: 'accepted',
        interaction_state: 'answer_committed',
        interaction_version: updatedRequest.version,
        handoff_state: 'pending',
        handoff_id: handoff.handoff_id,
        turn_id: turn.turn_id,
        turn_version: event.turn_version,
        control_id: request.control_id,
        error: null,
        received_at: committedAt,
        committed_at: committedAt,
      };
      validateInteractionAnswerResult(result, { occurredAt: committedAt });
      database.prepare(`
        INSERT INTO runtime_interaction_answers (
          answer_id, interaction_id, idempotency_key, payload_hash, answer_json, result_json,
          committed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
      `).run(
        answer.answer_id,
        answer.interaction_id,
        answer.idempotency_key,
        payloadHash,
        JSON.stringify(answer),
        JSON.stringify(result),
        committedAt,
      );
      return result;
    });
    try {
      return commit.immediate();
    } catch (error) {
      if (!(error instanceof ExecutorPersistenceError)) throw error;
      return rejectedInteractionAnswerResult(answer, error);
    }
  }

  function resolveInteractionTarget({
    region,
    tenantId,
    channel,
    botId,
    platformMessageId,
    mappingId = null,
    interactionId = null,
  } = {}) {
    for (const [name, value] of [
      ['region', region],
      ['tenantId', tenantId],
      ['channel', channel],
      ['botId', botId],
      ['platformMessageId', platformMessageId],
    ]) {
      if (typeof value !== 'string' || value.length === 0) {
        throw new TypeError(`${name} must be a non-empty string`);
      }
    }
    if (mappingId !== null && (typeof mappingId !== 'string' || mappingId.length === 0)) {
      throw new TypeError('mappingId must be null or a non-empty string');
    }
    if (
      interactionId !== null
      && (typeof interactionId !== 'string' || interactionId.length === 0)
    ) {
      throw new TypeError('interactionId must be null or a non-empty string');
    }

    const row = database.prepare(`
      SELECT mapping.*, lane.target_json
      FROM runtime_message_mappings AS mapping
      LEFT JOIN runtime_delivery_lanes AS lane
        ON lane.turn_id = mapping.turn_id
      WHERE mapping.region = ?
        AND mapping.tenant_id = ?
        AND mapping.channel = ?
        AND mapping.bot_id = ?
        AND mapping.platform_message_id = ?
    `).get(region, tenantId, channel, botId, platformMessageId);
    if (!row || (mappingId !== null && row.mapping_id !== mappingId)) {
      conflict('mapping_missing', 'The current channel card mapping was not found.');
    }

    let target;
    if (row.target_json !== null) {
      target = JSON.parse(row.target_json);
    } else {
      const commandRow = database.prepare(`
        SELECT command_json
        FROM runtime_outbox
        WHERE json_extract(command_json, '$.mapping.mapping_id') = ?
        ORDER BY created_at DESC
        LIMIT 1
      `).get(row.mapping_id);
      if (!commandRow) {
        conflict('mapping_corrupt', 'The current channel card target was not found.');
      }
      target = JSON.parse(commandRow.command_json).target;
    }
    if (
      !target
      || target.region !== region
      || target.tenant_id !== tenantId
      || target.channel !== channel
      || target.bot_id !== botId
    ) {
      conflict('mapping_corrupt', 'The current channel card target scope is inconsistent.');
    }

    const interactionRows = row.turn_id === null
      ? database.prepare(`
          SELECT request_json
          FROM runtime_interactions
          WHERE conversation_id = ? AND parent_type = 'security_control'
          ORDER BY ordinal, created_at
        `).all(row.conversation_id)
      : database.prepare(`
          SELECT request_json
          FROM runtime_interactions
          WHERE turn_id = ?
          ORDER BY ordinal, created_at
        `).all(row.turn_id);
    const interactions = interactionRows.map(
      ({ request_json: requestJson }) => JSON.parse(requestJson),
    );
    if (
      interactionId !== null
      && !interactions.some((interaction) => interaction.interaction_id === interactionId)
    ) {
      conflict('interaction_not_found', 'The card action is not correlated to this mapping.');
    }

    return {
      platform_message_id: platformMessageId,
      mapping: {
        mapping_id: row.mapping_id,
        conversation_id: row.conversation_id,
        turn_id: row.turn_id,
        lineage_id: row.lineage_id,
        binding_state: row.binding_state,
        reason: row.reason,
        mapping_version: row.mapping_version,
      },
      request_scope: {
        region: target.region,
        tenant_id: target.tenant_id,
        channel: target.channel,
        bot_id: target.bot_id,
        chat_id: target.chat_id,
        native_thread_or_topic_id: target.native_thread_or_topic_id,
      },
      interactions,
    };
  }

  function finalizeExecutionRecoveryTimeoutInTransaction(
    recovery,
    expirationError,
    expiredAt,
  ) {
    const recoveryUpdate = database.prepare(`
      UPDATE runtime_execution_recoveries
      SET state = 'stopped', recovery_version = recovery_version + 1, updated_at = ?
      WHERE recovery_id = ? AND turn_id = ? AND state = 'waiting_decision'
        AND recovery_version = ?
    `).run(
      expiredAt,
      recovery.recovery_id,
      recovery.turn_id,
      recovery.recovery_version,
    );
    if (recoveryUpdate.changes !== 1) {
      conflict('version_conflict', 'The expired execution recovery changed concurrently.');
    }
    const fence = {
      attempt_id: recovery.attempt_id,
      attempt_no: recovery.attempt_no,
      lease_epoch: recovery.lease_epoch,
    };
    const terminalEvent = transitionInTransaction(database, {
      turnId: recovery.turn_id,
      fromState: 'recovering',
      toState: 'timed_out',
      fence,
      provider,
      serviceInstanceId,
      occurredAt: expiredAt,
      generateId,
      reasonCode: 'execution_recovery_decision_expired',
      error: expirationError,
      requireActiveLease: false,
      retainLease: true,
    });
    database.prepare(`
      UPDATE runtime_provider_attempts
      SET state = 'stopped', side_effect_status = 'unknown', updated_at = ?
      WHERE turn_id = ? AND attempt_id = ? AND attempt_no = ? AND lease_epoch = ?
    `).run(
      expiredAt,
      recovery.turn_id,
      recovery.attempt_id,
      recovery.attempt_no,
      recovery.lease_epoch,
    );
    database.prepare(`
      UPDATE runtime_turn_queue
      SET wait_reason = NULL
      WHERE turn_id = ? AND status = 'timed_out'
    `).run(recovery.turn_id);
    return { terminalEvent, fence };
  }

  function reconcileExpiredExecutionRecoveries() {
    const reconcile = database.transaction(() => {
      const reconciledAt = now();
      const expirationError = createContractError({
        code: 'interaction_expired',
        category: 'conflict',
        userMessage: 'The recovery decision expired before an answer was committed.',
        occurredAt: reconciledAt,
      });
      const candidates = database.prepare(`
        SELECT recovery.*
        FROM runtime_execution_recoveries AS recovery
        JOIN runtime_interactions AS interaction
          ON interaction.interaction_id = recovery.interaction_id
        JOIN runtime_turns AS turn ON turn.turn_id = recovery.turn_id
        WHERE recovery.state = 'waiting_decision'
          AND interaction.state = 'expired'
          AND turn.state = 'recovering'
        ORDER BY recovery.created_at, recovery.recovery_id
      `).all();
      for (const recovery of candidates) {
        finalizeExecutionRecoveryTimeoutInTransaction(
          recovery,
          expirationError,
          reconciledAt,
        );
      }
      return Object.freeze({
        reconciled: candidates.length,
        turn_ids: Object.freeze(candidates.map(({ turn_id: turnId }) => turnId)),
      });
    });
    return reconcile.immediate();
  }

  function expireInteraction(expiration) {
    const expire = database.transaction(() => {
      const expiredAt = now();
      if (
        !expiration
        || typeof expiration.interaction_id !== 'string'
        || expiration.interaction_id.length === 0
        || !Number.isSafeInteger(expiration.interaction_version)
        || expiration.interaction_version <= 0
      ) {
        conflict(
          'validation_error',
          'Interaction expiration requires an interaction ID and positive expected version.',
        );
      }
      const row = database.prepare(`
        SELECT request_json
        FROM runtime_interactions
        WHERE interaction_id = ?
      `).get(expiration.interaction_id);
      if (!row) {
        conflict(
          'interaction_not_found',
          `Interaction ${expiration.interaction_id} does not exist.`,
        );
      }
      const request = JSON.parse(row.request_json);
      if (request.state === 'expired') {
        const turn = loadTurn(database, request.turn_id);
        return {
          status: 'expired',
          newly_expired: false,
          interaction_id: request.interaction_id,
          interaction_version: request.version,
          turn_id: request.turn_id,
          turn_state: turn.state,
          turn_version: turn.turn_version,
          attempt: {
            attempt_id: turn.attempt_id,
            attempt_no: turn.attempt_no,
            lease_epoch: turn.lease_epoch,
          },
        };
      }
      if (request.state !== 'pending') {
        return {
          status: 'not_pending',
          interaction_id: request.interaction_id,
          interaction_state: request.state,
          interaction_version: request.version,
          turn_id: request.turn_id,
        };
      }
      if (request.version !== expiration.interaction_version) {
        conflict('version_conflict', 'The deadline does not match the current interaction version.');
      }
      if (Date.parse(expiredAt) < Date.parse(request.expires_at)) {
        return {
          status: 'not_due',
          interaction_id: request.interaction_id,
          interaction_version: request.version,
          expires_at: request.expires_at,
          turn_id: request.turn_id,
        };
      }

      const blockingInteraction = database.prepare(`
        SELECT interaction_id
        FROM runtime_interactions
        WHERE turn_id = ?
          AND state IN (${BLOCKING_INTERACTION_STATES_SQL})
        ORDER BY ordinal ASC
        LIMIT 1
      `).get(request.turn_id);
      if (blockingInteraction?.interaction_id !== request.interaction_id) {
        return {
          status: 'not_current',
          interaction_id: request.interaction_id,
          interaction_version: request.version,
          blocking_interaction_id: blockingInteraction?.interaction_id ?? null,
          turn_id: request.turn_id,
        };
      }

      const turn = loadTurn(database, request.turn_id);
      const isProviderInteraction = request.parent_type === 'provider_turn';
      const expectedTurnState = isProviderInteraction ? 'waiting_user' : 'recovering';
      if (turn.state !== expectedTurnState) {
        conflict('illegal_transition', `Interaction timeout is invalid while turn is ${turn.state}.`);
      }
      const fence = {
        attempt_id: turn.attempt_id,
        attempt_no: turn.attempt_no,
        lease_epoch: turn.lease_epoch,
      };
      if (isProviderInteraction) {
        assertActiveFence(database, turn, fence, serviceInstanceId);
      } else if (request.parent_type !== 'recovery_control') {
        conflict('illegal_transition', 'This executor store cannot expire security controls.');
      }
      validateInteractionTransition({
        from: 'pending',
        to: 'expired',
        occurredAt: expiredAt,
      });
      const updatedRequest = {
        ...request,
        state: 'expired',
        version: request.version + 1,
      };
      validateInteractionRequest(updatedRequest, { occurredAt: expiredAt });
      const interactionUpdate = database.prepare(`
        UPDATE runtime_interactions
        SET state = 'expired', version = ?, request_json = ?, updated_at = ?
        WHERE interaction_id = ? AND state = 'pending' AND version = ?
      `).run(
        updatedRequest.version,
        JSON.stringify(updatedRequest),
        expiredAt,
        request.interaction_id,
        expiration.interaction_version,
      );
      if (interactionUpdate.changes !== 1) {
        conflict('version_conflict', 'The interaction deadline lost its state/version compare-and-swap.');
      }

      const expirationError = createContractError({
        code: 'interaction_expired',
        category: 'conflict',
        userMessage: 'The interaction expired before an answer was committed.',
        occurredAt: expiredAt,
      });
      const replyMappingRecovery = !isProviderInteraction
        ? database.prepare(`
          SELECT recovery_id
          FROM runtime_reply_mapping_recoveries
          WHERE recovery_id = ? AND turn_id = ? AND state = 'waiting_decision'
            AND bound_lineage_id IS NULL
        `).get(request.control_id, request.turn_id)
        : null;
      const executionRecovery = !isProviderInteraction
        ? database.prepare(`
          SELECT *
          FROM runtime_execution_recoveries
          WHERE interaction_id = ? AND turn_id = ? AND state = 'waiting_decision'
        `).get(request.interaction_id, request.turn_id)
        : null;
      if (replyMappingRecovery) {
        const recoveryUpdate = database.prepare(`
          UPDATE runtime_reply_mapping_recoveries
          SET state = 'rejected', native_recovery_status = 'expired', updated_at = ?
          WHERE recovery_id = ? AND state = 'waiting_decision'
            AND bound_lineage_id IS NULL
        `).run(expiredAt, replyMappingRecovery.recovery_id);
        if (recoveryUpdate.changes !== 1) {
          conflict('version_conflict', 'The expired recovery decision changed concurrently.');
        }
        const terminalEvent = transitionAttemptlessInTransaction(database, {
          turnId: request.turn_id,
          fromState: 'recovering',
          toState: 'stopped',
          occurredAt: expiredAt,
          generateId,
          reasonCode: 'reply_mapping_recovery_decision_expired',
        });
        const queueUpdate = database.prepare(`
          UPDATE runtime_turn_queue
          SET status = 'stopped', wait_reason = NULL
          WHERE turn_id = ? AND status = 'claimed'
            AND wait_reason = 'reply_mapping_recovery_decision'
        `).run(request.turn_id);
        if (queueUpdate.changes !== 1) {
          conflict('stale_attempt', 'The expired recovery decision lost its queue fence.');
        }
        return {
          status: 'expired',
          newly_expired: true,
          interaction_id: request.interaction_id,
          interaction_version: updatedRequest.version,
          turn_id: request.turn_id,
          turn_state: 'stopped',
          turn_version: terminalEvent.turn_version,
          attempt: fence,
        };
      }
      const expirationEvent = buildEvent({
        turn,
        lastEvent: loadLastEvent(database, turn.turn_id),
        fence,
        provider,
        descriptor: {
          kind: 'interaction_expired',
          phase: expectedTurnState,
          payload: {
            interaction_id: request.interaction_id,
            ordinal: request.ordinal,
            interaction_version: updatedRequest.version,
            handoff_version: null,
          },
          error: expirationError,
        },
        occurredAt: expiredAt,
        generateId,
      });
      commitTurnEvent(database, {
        turn,
        event: expirationEvent,
        fence,
        nextState: expectedTurnState,
        staleMessage: 'The interaction deadline lost its durable turn fence.',
        generateId,
      });
      if (!isProviderInteraction) {
        if (executionRecovery) {
          const { terminalEvent, fence: recoveryFence }
            = finalizeExecutionRecoveryTimeoutInTransaction(
              executionRecovery,
              expirationError,
              expiredAt,
            );
          return {
            status: 'expired',
            newly_expired: true,
            interaction_id: request.interaction_id,
            interaction_version: updatedRequest.version,
            turn_id: request.turn_id,
            turn_state: 'timed_out',
            turn_version: terminalEvent.turn_version,
            attempt: recoveryFence,
            execution_recovery_timed_out: true,
          };
        }
        return {
          status: 'expired',
          newly_expired: true,
          interaction_id: request.interaction_id,
          interaction_version: updatedRequest.version,
          turn_id: request.turn_id,
          turn_state: 'recovering',
          turn_version: expirationEvent.turn_version,
          attempt: fence,
        };
      }
      let currentTurn = loadTurn(database, turn.turn_id);
      const siblingRows = database.prepare(`
        SELECT request_json
        FROM runtime_interactions
        WHERE turn_id = ? AND interaction_id != ? AND state = 'pending'
        ORDER BY ordinal ASC
      `).all(turn.turn_id, request.interaction_id);
      for (const { request_json: siblingJson } of siblingRows) {
        const sibling = JSON.parse(siblingJson);
        validateInteractionTransition({
          from: 'pending',
          to: 'cancelled',
          occurredAt: expiredAt,
        });
        const cancelledSibling = {
          ...sibling,
          state: 'cancelled',
          version: sibling.version + 1,
          terminal_reason: 'parent_timed_out',
        };
        validateInteractionRequest(cancelledSibling, { occurredAt: expiredAt });
        const siblingUpdate = database.prepare(`
          UPDATE runtime_interactions
          SET state = 'cancelled', version = ?, request_json = ?, updated_at = ?
          WHERE interaction_id = ? AND state = 'pending' AND version = ?
        `).run(
          cancelledSibling.version,
          JSON.stringify(cancelledSibling),
          expiredAt,
          sibling.interaction_id,
          sibling.version,
        );
        if (siblingUpdate.changes !== 1) {
          conflict('version_conflict', 'The parent timeout lost a sibling interaction fence.');
        }
        const cancellationEvent = buildEvent({
          turn: currentTurn,
          lastEvent: loadLastEvent(database, currentTurn.turn_id),
          fence,
          provider,
          descriptor: {
            kind: 'interaction_cancelled',
            phase: 'waiting_user',
            payload: {
              interaction_id: sibling.interaction_id,
              ordinal: sibling.ordinal,
              interaction_version: cancelledSibling.version,
              handoff_version: null,
            },
          },
          occurredAt: expiredAt,
          generateId,
        });
        commitTurnEvent(database, {
          turn: currentTurn,
          event: cancellationEvent,
          fence,
          nextState: 'waiting_user',
          staleMessage: 'The parent timeout lost its sibling cancellation fence.',
          generateId,
        });
        currentTurn = loadTurn(database, turn.turn_id);
      }
      const terminalEvent = transitionInTransaction(database, {
        turnId: turn.turn_id,
        fromState: 'waiting_user',
        toState: 'timed_out',
        fence,
        provider,
        serviceInstanceId,
        occurredAt: expiredAt,
        generateId,
        reasonCode: 'interaction_expired',
        error: expirationError,
      });
      return {
        status: 'expired',
        newly_expired: true,
        interaction_id: request.interaction_id,
        interaction_version: updatedRequest.version,
        turn_id: request.turn_id,
        turn_state: 'timed_out',
        turn_version: terminalEvent.turn_version,
        attempt: fence,
      };
    });
    return expire.immediate();
  }

  function markProviderStopUnknownInTransaction(
    terminalContext,
    providerStopStatus,
    stopResult = null,
  ) {
      if (typeof providerStopStatus !== 'string' || providerStopStatus.length === 0) {
        throw new TypeError('providerStopStatus must be a non-empty string');
      }
      if (!terminalContext || typeof terminalContext.turn_id !== 'string') {
        throw new TypeError('terminalContext must identify a turn and attempt');
      }
      const existing = database.prepare(`
        SELECT incident_id, provider_stop_status, side_effect_status, disposition, outbox_id
        FROM runtime_provider_stop_incidents
        WHERE turn_id = ?
      `).get(terminalContext.turn_id);
      if (existing) {
        const stoppedResult = stopResult
          ? updateStopProviderOutcomeInTransaction(
            stopResult,
            existing.provider_stop_status,
            false,
          )
          : null;
        return {
          status: 'manual_recovery_required',
          ...existing,
          ...(stoppedResult ? { stop_result: stoppedResult } : {}),
        };
      }

      const occurredAt = now();
      const turn = loadTurn(database, terminalContext.turn_id);
      if (!['timed_out', 'stopped', 'interrupted'].includes(turn.state)) {
        conflict('illegal_transition', 'Provider stop uncertainty requires a terminal stopped turn.');
      }
      const expectedAttempt = terminalContext.attempt;
      if (
        expectedAttempt?.attempt_id !== turn.attempt_id
        || expectedAttempt?.attempt_no !== turn.attempt_no
        || expectedAttempt?.lease_epoch !== turn.lease_epoch
      ) {
        conflict('stale_attempt', 'Provider stop uncertainty lost its terminal attempt fence.');
      }
      assertActiveFence(database, turn, expectedAttempt, serviceInstanceId);
      const lane = database.prepare(`
        SELECT target_json, mapping_json
        FROM runtime_delivery_lanes
        WHERE turn_id = ?
      `).get(turn.turn_id);
      if (!lane) {
        conflict('provider_context_invalid', 'The timed-out turn has no durable delivery lane.');
      }

      const incidentId = generateId('provider-stop-incident');
      const outboxId = generateId('outbox');
      const deliveryId = generateId('delivery');
      const providerIsolationProven = ['confirmed', 'isolated'].includes(providerStopStatus);
      const error = createContractError({
        code: 'side_effect_unknown',
        category: 'provider',
        retryable: false,
        sideEffectStatus: 'unknown',
        userMessage: providerIsolationProven
          ? 'Provider execution stopped, but a late event reported an unknown side effect; manual recovery is required.'
          : 'Provider stop could not be confirmed; manual recovery is required.',
        occurredAt,
      });
      const target = JSON.parse(lane.target_json);
      const mapping = {
        ...JSON.parse(lane.mapping_json),
        mapping_id: generateId('mapping'),
      };
      const command = {
        contract: 'zylos.delivery-command',
        contract_version: target.chat_type === 'thread' ? '1.1' : '1.0',
        outbox_id: outboxId,
        delivery_id: deliveryId,
        trace_id: generateId('delivery-trace'),
        delivery_attempt_id: generateId('delivery-attempt'),
        delivery_attempt_no: 1,
        outbox_lease_epoch: 1,
        target,
        aggregate_type: 'text_notice',
        aggregate_id: `${turn.turn_id}-provider-stop-${incidentId}`,
        operation: 'send_text',
        aggregate_version: 1,
        event_sequence_through: turn.turn_version,
        idempotency_key: createIdempotencyKey('delivery', {
          channel: target.channel,
          target,
          delivery_id: deliveryId,
        }),
        render_model: {
          title: 'Zylos',
          phase: turn.state,
          text: providerIsolationProven
            ? 'Steering stopped the provider, but a late event reported an unknown side effect. Manual recovery is required.'
            : turn.state === 'timed_out'
            ? 'Execution timed out, but provider stop could not be confirmed. Manual recovery is required.'
            : turn.state === 'interrupted'
              ? 'Steering interrupted the turn, but provider termination could not be confirmed. Manual recovery is required.'
              : 'Execution was stopped, but provider termination could not be confirmed. Manual recovery is required.',
          error,
          tools: [],
          interactions: [],
          terminal: true,
          user_action_required: true,
        },
        mapping,
        target_platform_message_id: null,
        predecessor_delivery_id: null,
        expected_platform_version: null,
        priority: 100,
        not_before: occurredAt,
        created_at: occurredAt,
      };
      validateDeliveryCommand(command, { occurredAt });
      const laneKey = createDeliveryLaneKey(command);
      database.prepare(`
        INSERT INTO runtime_outbox (
          outbox_id, delivery_id, aggregate_type, aggregate_id, turn_id, control_id,
          lane_key, predecessor_delivery_id, aggregate_version, status, command_json,
          priority, supersedable, terminal, next_attempt_at, created_at, updated_at
        ) VALUES (?, ?, 'text_notice', ?, ?, NULL, ?, NULL, 1, 'pending', ?, 100, 0, 1, ?, ?, ?)
      `).run(
        outboxId,
        deliveryId,
        command.aggregate_id,
        turn.turn_id,
        laneKey,
        JSON.stringify(command),
        occurredAt,
        occurredAt,
        occurredAt,
      );
      database.prepare(`
        INSERT INTO runtime_provider_stop_incidents (
          incident_id, turn_id, attempt_id, attempt_no, lease_epoch,
          provider_stop_status, side_effect_status, disposition, error_json,
          outbox_id, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, 'unknown', 'manual_recovery_required', ?, ?, ?)
      `).run(
        incidentId,
        turn.turn_id,
        turn.attempt_id,
        turn.attempt_no,
        turn.lease_epoch,
        providerStopStatus,
        JSON.stringify(error),
        outboxId,
        occurredAt,
      );
      const stoppedResult = stopResult
        ? updateStopProviderOutcomeInTransaction(stopResult, providerStopStatus, false)
        : null;
      return {
        status: 'manual_recovery_required',
        incident_id: incidentId,
        provider_stop_status: providerStopStatus,
        side_effect_status: 'unknown',
        disposition: 'manual_recovery_required',
        outbox_id: outboxId,
        ...(stoppedResult ? { stop_result: stoppedResult } : {}),
      };
  }

  function markProviderStopUnknown(terminalContext, providerStopStatus, stopResult = null) {
    const markUnknown = database.transaction(() => {
      return markProviderStopUnknownInTransaction(
        terminalContext,
        providerStopStatus,
        stopResult,
      );
    });
    return markUnknown.immediate();
  }

  function releaseTimedOutExecutorLease(expiration) {
    const release = database.transaction(() => {
      if (
        expiration?.status !== 'expired'
        || typeof expiration.turn_id !== 'string'
        || !expiration.attempt
      ) {
        conflict('validation_error', 'A persisted timeout result is required to release its lease.');
      }
      const turn = loadTurn(database, expiration.turn_id);
      if (turn.state !== 'timed_out' || !sameFence(turn, expiration.attempt)) {
        conflict('stale_attempt', 'The timed-out turn no longer matches the expiration fence.');
      }
      const lease = database.prepare(`
        SELECT lease_owner
        FROM runtime_executor_leases
        WHERE conversation_id = ?
      `).get(turn.conversation_id);
      if (!lease || lease.lease_owner === null) return false;
      assertActiveFence(database, turn, expiration.attempt, serviceInstanceId);
      const releasedAt = now();
      const released = database.prepare(`
        UPDATE runtime_executor_leases
        SET lease_owner = NULL, turn_id = NULL, attempt_id = NULL,
          attempt_no = NULL, lease_expires_at = NULL, updated_at = ?
        WHERE conversation_id = ? AND lease_owner = ? AND turn_id = ?
          AND attempt_id = ? AND attempt_no = ? AND lease_epoch = ?
      `).run(
        releasedAt,
        turn.conversation_id,
        serviceInstanceId,
        turn.turn_id,
        expiration.attempt.attempt_id,
        expiration.attempt.attempt_no,
        expiration.attempt.lease_epoch,
      );
      if (released.changes !== 1) {
        conflict('stale_attempt', 'The timed-out turn lost its executor lease fence.');
      }
      workspaceLeases.releaseTurnAfterIsolation(turn.turn_id, {
        reason: 'timed_out_provider_isolation_confirmed',
      });
      return true;
    });
    return release.immediate();
  }

  function releaseRecoveringExecutorOwnership(turnContext) {
    const release = database.transaction(() => {
      const releasedAt = now();
      const turn = loadTurn(database, turnContext.turn_id);
      assertActiveFence(database, turn, turnContext.attempt, serviceInstanceId);
      assertResidentOwner(turn.conversation_id, turnContext.resident?.owner_epoch ?? null);
      if (turnContext.workspace) {
        workspaceLeases.assertHeldOrUncertain(turnContext.workspace);
      }
      if (!['recovering', 'timed_out'].includes(turn.state)) {
        conflict(
          'illegal_transition',
          `Turn ${turn.turn_id} is ${turn.state}; expected recovering or timed_out before ownership release.`,
        );
      }
      if (turnContext.workspace && !isRecoveryNotificationDelivered(turnContext)) {
        conflict(
          'recovery_notification_pending',
          'Workspace recovery must wait for a delivered user notification.',
        );
      }
      const workspaceReleased = turnContext.workspace
        ? workspaceLeases.releaseAfterIsolation(turnContext.workspace, {
          reason: 'provider_isolation_confirmed',
        })
        : null;
      const releasedLease = database.prepare(`
        UPDATE runtime_executor_leases
        SET lease_owner = NULL, turn_id = NULL, attempt_id = NULL,
          attempt_no = NULL, lease_expires_at = NULL, updated_at = ?
        WHERE conversation_id = ? AND lease_owner = ? AND turn_id = ?
          AND attempt_id = ? AND attempt_no = ? AND lease_epoch = ?
      `).run(
        releasedAt,
        turn.conversation_id,
        serviceInstanceId,
        turn.turn_id,
        turnContext.attempt.attempt_id,
        turnContext.attempt.attempt_no,
        turnContext.attempt.lease_epoch,
      );
      if (releasedLease.changes !== 1) {
        conflict('stale_attempt', 'The recovering turn lost its executor lease fence.');
      }

      let residentReleased = false;
      if (provider === 'claude') {
        const resident = isResidentConversation(turn.conversation_id);
        if (resident) {
          const expectedEpoch = turnContext.resident?.owner_epoch ?? resident.owner_epoch;
          if (
            resident.owner_service_instance_id !== serviceInstanceId
            || resident.owner_epoch !== expectedEpoch
          ) {
            conflict('stale_attempt', 'The recovering resident owner no longer matches this fence.');
          }
          const releasedResident = database.prepare(`
            UPDATE runtime_executor_residents
            SET owner_service_instance_id = NULL, owner_expires_at = NULL,
              last_used_at = ?
            WHERE conversation_id = ? AND provider = 'claude'
              AND owner_service_instance_id = ? AND owner_epoch = ?
          `).run(
            releasedAt,
            turn.conversation_id,
            serviceInstanceId,
            expectedEpoch,
          );
          if (releasedResident.changes !== 1) {
            conflict('stale_attempt', 'The recovering resident release lost its owner fence.');
          }
          residentReleased = true;
        }
      }
      const reconciliationRows = database.prepare(`
        SELECT steer_id, result_json
        FROM runtime_steer_requests
        WHERE target_turn_id = ? AND winner_control_id IS NULL
      `).all(turn.turn_id);
      for (const row of reconciliationRows) {
        const current = JSON.parse(row.result_json);
        if (current.reconciliation_required !== true) continue;
        const updated = {
          ...current,
          lease_released: true,
          reconciliation: {
            status: 'provider_isolated_manual_recovery_required',
            provider_isolated: true,
          },
          updated_at: releasedAt,
          deduplicated: false,
        };
        const requestUpdate = database.prepare(`
          UPDATE runtime_steer_requests
          SET result_json = ?, updated_at = ?
          WHERE steer_id = ? AND result_json = ? AND winner_control_id IS NULL
        `).run(
          JSON.stringify(updated),
          releasedAt,
          row.steer_id,
          row.result_json,
        );
        if (requestUpdate.changes !== 1) {
          conflict('version_conflict', 'The steer reconciliation lease result lost its CAS.');
        }
      }
      return {
        lease_released: true,
        resident_released: residentReleased,
        workspace_released: workspaceReleased !== null,
      };
    });
    return release.immediate();
  }

  function claimInteractionHandoff(handoffId) {
    const claim = database.transaction(() => {
      const claimedAt = now();
      const row = database.prepare(`
        SELECT interaction.handoff_version, interaction.request_json,
          answer.answer_json, handoff.record_json
        FROM runtime_interaction_handoffs AS handoff
        JOIN runtime_interactions AS interaction
          ON interaction.interaction_id = handoff.interaction_id
        JOIN runtime_interaction_answers AS answer
          ON answer.answer_id = handoff.answer_id
        WHERE handoff.handoff_id = ?
      `).get(handoffId);
      if (!row) conflict('handoff_not_found', `Interaction handoff ${handoffId} does not exist.`);
      const request = JSON.parse(row.request_json);
      const answer = JSON.parse(row.answer_json);
      const handoff = JSON.parse(row.record_json);
      if (
        request.state !== 'answer_committed'
        || !['pending', 'retry_wait'].includes(handoff.state)
      ) {
        conflict('illegal_transition', 'Only a committed answer with a pending or retry-wait handoff can be claimed.');
      }
      const turn = loadTurn(database, request.turn_id);
      const isProviderHandoff = request.parent_type === 'provider_turn';
      const expectedTurnState = isProviderHandoff ? 'waiting_user' : 'recovering';
      if (turn.state !== expectedTurnState) {
        conflict('illegal_transition', `Interaction handoff is invalid while turn is ${turn.state}.`);
      }
      const fence = {
        attempt_id: turn.attempt_id,
        attempt_no: turn.attempt_no,
        lease_epoch: turn.lease_epoch,
      };
      if (isProviderHandoff) {
        if (
          request.runtime_fence?.provider_attempt_id !== fence.attempt_id
          || request.runtime_fence?.lease_epoch !== fence.lease_epoch
          || handoff.provider_attempt_id !== fence.attempt_id
          || handoff.lease_epoch !== fence.lease_epoch
        ) {
          conflict('stale_attempt', 'The pending handoff no longer matches the current runtime fence.');
        }
        assertActiveFence(database, turn, fence, serviceInstanceId);
        assertResidentOwner(turn.conversation_id);
      } else if (
        request.parent_type !== 'recovery_control'
        || request.runtime_fence !== null
        || handoff.parent_type !== 'recovery_control'
        || handoff.provider_attempt_id !== null
        || handoff.lease_epoch !== null
      ) {
        conflict('stale_attempt', 'The recovery control handoff lost its Core-owned fence.');
      }
      validateInteractionTransition({
        from: 'answer_committed',
        to: 'answer_delivering',
        occurredAt: claimedAt,
      });
      validateInteractionHandoffTransition({
        from: handoff.state,
        to: 'delivering',
        occurredAt: claimedAt,
      });

      const updatedRequest = {
        ...request,
        state: 'answer_delivering',
        version: request.version + 1,
        handoff_state: 'delivering',
      };
      const updatedHandoff = {
        ...handoff,
        state: 'delivering',
        handoff_attempt_id: generateId('handoff-attempt'),
        handoff_attempt_no: handoff.state === 'pending'
          ? 1
          : handoff.handoff_attempt_no + 1,
        claimed_by: serviceInstanceId,
        claimed_at: claimedAt,
        last_send_started_at: null,
        provider_acked_at: null,
        reason_code: null,
        error: null,
        side_effect_status: 'none',
      };
      validateInteractionRequest(updatedRequest, { occurredAt: claimedAt });
      validateInteractionHandoff(updatedHandoff, { occurredAt: claimedAt });

      const nextHandoffVersion = row.handoff_version + 1;
      const interactionUpdate = database.prepare(`
        UPDATE runtime_interactions
        SET state = 'answer_delivering', version = ?, handoff_state = 'delivering',
          handoff_version = ?, request_json = ?, updated_at = ?
        WHERE interaction_id = ? AND state = 'answer_committed' AND version = ?
          AND handoff_state = ? AND handoff_version = ?
      `).run(
        updatedRequest.version,
        nextHandoffVersion,
        JSON.stringify(updatedRequest),
        claimedAt,
        request.interaction_id,
        request.version,
        handoff.state,
        row.handoff_version,
      );
      const handoffUpdate = handoff.state === 'pending'
        ? database.prepare(`
          UPDATE runtime_interaction_handoffs
          SET state = 'delivering', handoff_attempt_id = ?, handoff_attempt_no = 1,
            record_json = ?, updated_at = ?
          WHERE handoff_id = ? AND state = 'pending'
            AND handoff_attempt_id IS NULL AND handoff_attempt_no IS NULL
            AND provider_attempt_id IS ? AND lease_epoch IS ?
        `).run(
          updatedHandoff.handoff_attempt_id,
          JSON.stringify(updatedHandoff),
          claimedAt,
          handoff.handoff_id,
          handoff.provider_attempt_id,
          handoff.lease_epoch,
        )
        : database.prepare(`
          UPDATE runtime_interaction_handoffs
          SET state = 'delivering', handoff_attempt_id = ?, handoff_attempt_no = ?,
            record_json = ?, updated_at = ?
          WHERE handoff_id = ? AND state = 'retry_wait'
            AND handoff_attempt_id = ? AND handoff_attempt_no = ?
            AND provider_attempt_id IS ? AND lease_epoch IS ?
        `).run(
          updatedHandoff.handoff_attempt_id,
          updatedHandoff.handoff_attempt_no,
          JSON.stringify(updatedHandoff),
          claimedAt,
          handoff.handoff_id,
          handoff.handoff_attempt_id,
          handoff.handoff_attempt_no,
          handoff.provider_attempt_id,
          handoff.lease_epoch,
        );
      assertPairedInteractionHandoffCas(
        interactionUpdate,
        handoffUpdate,
        'version_conflict',
        'The interaction handoff claim lost its state/version fence.',
      );

      const eventDescriptor = {
          kind: 'interaction_answer_handoff_started',
          phase: expectedTurnState,
          payload: {
            interaction_id: request.interaction_id,
            ordinal: request.ordinal,
            interaction_version: updatedRequest.version,
            handoff_version: nextHandoffVersion,
          },
        };
      persistInteractionLifecycleEventInTransaction(database, {
        turn,
        state: expectedTurnState,
        descriptor: eventDescriptor,
        fence,
        provider,
        occurredAt: claimedAt,
        generateId,
        staleMessage: 'The interaction handoff claim lost its durable turn fence.',
      });
      return {
        request: updatedRequest,
        answer,
        handoff: updatedHandoff,
        handoff_version: nextHandoffVersion,
      };
    });
    return claim.immediate();
  }

  function markInteractionHandoffPreSendFailure(delivery, error) {
    const markFailure = database.transaction(() => {
      const occurredAt = now();
      if (!['none', 'unknown'].includes(error?.side_effect_status)) {
        conflict(
          'provider_context_invalid',
          'A preparation failure must report none or unknown provider side effects.',
        );
      }
      const safeToRetry = error.retryable === true && error.side_effect_status === 'none';
      const row = database.prepare(`
        SELECT interaction.handoff_version, interaction.request_json,
          handoff.record_json
        FROM runtime_interaction_handoffs AS handoff
        JOIN runtime_interactions AS interaction
          ON interaction.interaction_id = handoff.interaction_id
        WHERE handoff.handoff_id = ?
      `).get(delivery?.handoff?.handoff_id);
      if (!row) {
        conflict('handoff_not_found', 'The claimed interaction handoff does not exist.');
      }
      const request = JSON.parse(row.request_json);
      const handoff = JSON.parse(row.record_json);
      const turn = loadTurn(database, request.turn_id);
      const fence = {
        attempt_id: turn.attempt_id,
        attempt_no: turn.attempt_no,
        lease_epoch: turn.lease_epoch,
      };
      if (
        request.state !== 'answer_delivering'
        || handoff.state !== 'delivering'
        || handoff.claimed_by !== serviceInstanceId
        || handoff.last_send_started_at !== null
        || request.runtime_fence?.provider_attempt_id !== fence.attempt_id
        || request.runtime_fence?.lease_epoch !== fence.lease_epoch
        || handoff.provider_attempt_id !== fence.attempt_id
        || handoff.lease_epoch !== fence.lease_epoch
        || !matchesInteractionHandoffDelivery({
          delivery,
          request,
          handoff,
          handoffVersion: row.handoff_version,
        })
      ) {
        conflict('stale_attempt', 'The pre-send failure does not match the current interaction handoff fence.');
      }
      if (turn.state !== 'waiting_user') {
        conflict('illegal_transition', `Pre-send failure is invalid while turn is ${turn.state}.`);
      }
      assertActiveFence(database, turn, fence, serviceInstanceId);
      assertResidentOwner(turn.conversation_id);
      validateInteractionTransition({
        from: 'answer_delivering',
        to: safeToRetry ? 'answer_committed' : 'cancelled',
        sendStarted: false,
        occurredAt,
      });
      validateInteractionHandoffTransition({
        from: 'delivering',
        to: safeToRetry ? 'retry_wait' : 'cancelled',
        sendStarted: false,
        safeToRetry,
        occurredAt,
      });

      const updatedRequest = {
        ...request,
        state: safeToRetry ? 'answer_committed' : 'cancelled',
        version: request.version + 1,
        handoff_state: safeToRetry ? 'retry_wait' : 'cancelled',
        ...(safeToRetry ? {} : { terminal_reason: 'pre_send_non_retryable_failure' }),
      };
      const updatedHandoff = {
        ...handoff,
        state: safeToRetry ? 'retry_wait' : 'cancelled',
        reason_code: safeToRetry ? 'pre_send_failure' : 'pre_send_non_retryable_failure',
        error: structuredClone(error),
        side_effect_status: error.side_effect_status,
      };
      validateInteractionRequest(updatedRequest, { occurredAt });
      validateInteractionHandoff(updatedHandoff, { occurredAt });
      const nextHandoffVersion = row.handoff_version + 1;
      const interactionUpdate = database.prepare(`
        UPDATE runtime_interactions
        SET state = ?, version = ?, handoff_state = ?,
          handoff_version = ?, request_json = ?, updated_at = ?
        WHERE interaction_id = ? AND state = 'answer_delivering' AND version = ?
          AND handoff_state = 'delivering' AND handoff_version = ?
      `).run(
        updatedRequest.state,
        updatedRequest.version,
        updatedRequest.handoff_state,
        nextHandoffVersion,
        JSON.stringify(updatedRequest),
        occurredAt,
        request.interaction_id,
        request.version,
        row.handoff_version,
      );
      const handoffUpdate = database.prepare(`
        UPDATE runtime_interaction_handoffs
        SET state = ?, record_json = ?, updated_at = ?
        WHERE handoff_id = ? AND state = 'delivering'
          AND handoff_attempt_id = ? AND handoff_attempt_no = ?
          AND provider_attempt_id = ? AND lease_epoch = ?
      `).run(
        updatedHandoff.state,
        JSON.stringify(updatedHandoff),
        occurredAt,
        handoff.handoff_id,
        handoff.handoff_attempt_id,
        handoff.handoff_attempt_no,
        handoff.provider_attempt_id,
        handoff.lease_epoch,
      );
      assertPairedInteractionHandoffCas(
        interactionUpdate,
        handoffUpdate,
        'stale_attempt',
        'The pre-send failure lost its interaction handoff fence.',
      );
      const auditId = generateId('audit');
      database.prepare(`
        INSERT INTO runtime_interaction_audit (
          audit_id, interaction_id, handoff_id, outcome, provider_attempt_id,
          lease_epoch, acknowledgement_json, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      `).run(
        auditId,
        request.interaction_id,
        handoff.handoff_id,
        safeToRetry ? 'retry_wait' : 'cancelled_pre_send',
        handoff.provider_attempt_id,
        handoff.lease_epoch,
        JSON.stringify({
          status: safeToRetry ? 'retry_wait' : 'cancelled_pre_send',
          handoff_attempt_id: handoff.handoff_attempt_id,
          handoff_attempt_no: handoff.handoff_attempt_no,
          error,
        }),
        occurredAt,
      );
      let turnState = 'waiting_user';
      if (!safeToRetry) {
        const cancellationEvent = buildEvent({
          turn,
          lastEvent: loadLastEvent(database, turn.turn_id),
          fence,
          provider,
          descriptor: {
            kind: 'interaction_cancelled',
            phase: 'waiting_user',
            provider_native_id: turn.provider_native_id,
            payload: {
              interaction_id: request.interaction_id,
              ordinal: request.ordinal,
              interaction_version: updatedRequest.version,
              handoff_version: nextHandoffVersion,
            },
          },
          occurredAt,
          generateId,
        });
        commitTurnEvent(database, {
          turn,
          event: cancellationEvent,
          fence,
          nextState: 'waiting_user',
          staleMessage: 'The pre-send cancellation lost its provider attempt fence.',
          generateId,
        });
        transitionInTransaction(database, {
          turnId: turn.turn_id,
          fromState: 'waiting_user',
          toState: 'recovering',
          fence,
          provider,
          serviceInstanceId,
          occurredAt,
          generateId,
          reasonCode: 'pre_send_non_retryable_failure',
        });
        const recoveringTurn = loadTurn(database, turn.turn_id);
        const recoveryEvent = buildEvent({
          turn: recoveringTurn,
          lastEvent: loadLastEvent(database, turn.turn_id),
          fence,
          provider,
          descriptor: {
            kind: 'recovery_started',
            phase: 'recovering',
            provider_native_id: recoveringTurn.provider_native_id,
            payload: {
              recovery_id: generateId('recovery'),
              recovery_of_turn_id: turn.turn_id,
              recovery_of_lineage_id: turn.lineage_id,
              side_effect_status: error.side_effect_status,
            },
            error,
          },
          occurredAt,
          generateId,
        });
        commitTurnEvent(database, {
          turn: recoveringTurn,
          event: recoveryEvent,
          fence,
          nextState: 'recovering',
          staleMessage: 'The pre-send recovery notification lost its provider attempt fence.',
          generateId,
        });
        turnState = 'recovering';
      }
      return {
        status: safeToRetry ? 'retry_wait' : 'recovering',
        interaction_id: request.interaction_id,
        interaction_state: updatedRequest.state,
        handoff_id: handoff.handoff_id,
        handoff_state: updatedHandoff.state,
        handoff_version: nextHandoffVersion,
        audit_id: auditId,
        turn_id: turn.turn_id,
        turn_state: turnState,
        request: updatedRequest,
        handoff: updatedHandoff,
      };
    });
    return markFailure.immediate();
  }

  function markInteractionHandoffSendStarted(delivery) {
    const markStarted = database.transaction(() => {
      const sendStartedAt = now();
      const row = database.prepare(`
        SELECT interaction.handoff_version, interaction.request_json,
          handoff.record_json
        FROM runtime_interaction_handoffs AS handoff
        JOIN runtime_interactions AS interaction
          ON interaction.interaction_id = handoff.interaction_id
        WHERE handoff.handoff_id = ?
      `).get(delivery?.handoff?.handoff_id);
      if (!row) {
        conflict('handoff_not_found', 'The claimed interaction handoff does not exist.');
      }
      const request = JSON.parse(row.request_json);
      const handoff = JSON.parse(row.record_json);
      const turn = loadTurn(database, request.turn_id);
      const fence = {
        attempt_id: turn.attempt_id,
        attempt_no: turn.attempt_no,
        lease_epoch: turn.lease_epoch,
      };
      const isProviderHandoff = request.parent_type === 'provider_turn';
      if (
        request.state !== 'answer_delivering'
        || handoff.state !== 'delivering'
        || handoff.claimed_by !== serviceInstanceId
        || handoff.last_send_started_at !== null
        || (isProviderHandoff && (
          request.runtime_fence?.provider_attempt_id !== fence.attempt_id
          || request.runtime_fence?.lease_epoch !== fence.lease_epoch
          || handoff.provider_attempt_id !== fence.attempt_id
          || handoff.lease_epoch !== fence.lease_epoch
        ))
        || (!isProviderHandoff && (
          request.parent_type !== 'recovery_control'
          || request.runtime_fence !== null
          || handoff.parent_type !== 'recovery_control'
          || handoff.provider_attempt_id !== null
          || handoff.lease_epoch !== null
        ))
        || !matchesInteractionHandoffDelivery({
          delivery,
          request,
          handoff,
          handoffVersion: row.handoff_version,
        })
      ) {
        conflict('stale_attempt', 'The send start does not match the current interaction handoff fence.');
      }
      const expectedTurnState = isProviderHandoff ? 'waiting_user' : 'recovering';
      if (turn.state !== expectedTurnState) {
        conflict('illegal_transition', `Interaction send is invalid while turn is ${turn.state}.`);
      }
      if (isProviderHandoff) {
        assertActiveFence(database, turn, fence, serviceInstanceId);
        assertResidentOwner(turn.conversation_id);
      }

      const updatedHandoff = {
        ...handoff,
        last_send_started_at: sendStartedAt,
      };
      validateInteractionHandoff(updatedHandoff, { occurredAt: sendStartedAt });
      const nextHandoffVersion = row.handoff_version + 1;
      const interactionUpdate = database.prepare(`
        UPDATE runtime_interactions
        SET handoff_version = ?, updated_at = ?
        WHERE interaction_id = ? AND state = 'answer_delivering'
          AND handoff_state = 'delivering' AND handoff_version = ?
      `).run(
        nextHandoffVersion,
        sendStartedAt,
        request.interaction_id,
        row.handoff_version,
      );
      const handoffUpdate = database.prepare(`
        UPDATE runtime_interaction_handoffs
        SET record_json = ?, updated_at = ?
        WHERE handoff_id = ? AND state = 'delivering'
          AND handoff_attempt_id = ? AND handoff_attempt_no = ?
          AND provider_attempt_id IS ? AND lease_epoch IS ?
      `).run(
        JSON.stringify(updatedHandoff),
        sendStartedAt,
        handoff.handoff_id,
        handoff.handoff_attempt_id,
        handoff.handoff_attempt_no,
        handoff.provider_attempt_id,
        handoff.lease_epoch,
      );
      assertPairedInteractionHandoffCas(
        interactionUpdate,
        handoffUpdate,
        'stale_attempt',
        'The send start lost its interaction handoff fence.',
      );
      return {
        ...delivery,
        request,
        handoff: updatedHandoff,
        handoff_version: nextHandoffVersion,
      };
    });
    return markStarted.immediate();
  }

  function markInteractionHandoffDeliveryUnknown(
    delivery,
    providerError = null,
    { expiredLeaseRecovery = false } = {},
  ) {
    const markUnknown = database.transaction(() => {
      const occurredAt = now();
      const row = database.prepare(`
        SELECT interaction.handoff_version, interaction.request_json,
          handoff.record_json
        FROM runtime_interaction_handoffs AS handoff
        JOIN runtime_interactions AS interaction
          ON interaction.interaction_id = handoff.interaction_id
        WHERE handoff.handoff_id = ?
      `).get(delivery?.handoff?.handoff_id);
      if (!row) {
        conflict('handoff_not_found', 'The delivering interaction handoff does not exist.');
      }
      const request = JSON.parse(row.request_json);
      const handoff = JSON.parse(row.record_json);
      const turn = loadTurn(database, request.turn_id);
      const fence = {
        attempt_id: turn.attempt_id,
        attempt_no: turn.attempt_no,
        lease_epoch: turn.lease_epoch,
      };
      const expiredLease = expiredLeaseRecovery
        ? database.prepare(`
          SELECT lease_owner, turn_id, attempt_id, attempt_no, lease_epoch, lease_expires_at
          FROM runtime_executor_leases
          WHERE conversation_id = ?
        `).get(turn.conversation_id)
        : null;
      const matchesExpiredLease = expiredLeaseRecovery
        && expiredLease
        && expiredLease.turn_id === turn.turn_id
        && expiredLease.attempt_id === fence.attempt_id
        && expiredLease.attempt_no === fence.attempt_no
        && expiredLease.lease_epoch === fence.lease_epoch
        && expiredLease.lease_expires_at !== null
        && expiredLease.lease_expires_at <= occurredAt;
      if (
        request.state !== 'answer_delivering'
        || handoff.state !== 'delivering'
        || (!expiredLeaseRecovery && handoff.claimed_by !== serviceInstanceId)
        || handoff.last_send_started_at === null
        || request.runtime_fence?.provider_attempt_id !== fence.attempt_id
        || request.runtime_fence?.lease_epoch !== fence.lease_epoch
        || handoff.provider_attempt_id !== fence.attempt_id
        || handoff.lease_epoch !== fence.lease_epoch
        || !matchesInteractionHandoffDelivery({
          delivery,
          request,
          handoff,
          handoffVersion: row.handoff_version,
        })
        || (expiredLeaseRecovery && !matchesExpiredLease)
      ) {
        conflict('stale_attempt', 'The unknown delivery result lost its handoff fence.');
      }
      if (turn.state !== 'waiting_user') {
        conflict('illegal_transition', `Interaction delivery is unknown while turn is ${turn.state}.`);
      }
      if (!expiredLeaseRecovery) {
        assertActiveFence(database, turn, fence, serviceInstanceId);
        assertResidentOwner(turn.conversation_id);
      }
      const error = providerError === null
        ? createContractError({
          code: 'interaction_answer_delivery_unknown',
          category: 'provider',
          retryable: false,
          sideEffectStatus: 'unknown',
          userMessage: 'The answer may have reached the provider, but acknowledgement is unknown.',
          occurredAt,
        })
        : createContractError({
          code: providerError.code,
          category: providerError.category,
          retryable: providerError.retryable,
          sideEffectStatus: providerError.side_effect_status,
          userMessage: providerError.user_message,
          occurredAt: providerError.occurred_at ?? occurredAt,
        });
      const unknown = persistDeliveryUnknownInTransaction({
        turn,
        request,
        handoff,
        handoffVersion: row.handoff_version + 1,
        fence,
        occurredAt,
        error,
        reasonCode: 'send_started_ack_missing',
        recoverParent: true,
        requireActiveLease: !expiredLeaseRecovery,
        auditContext: {
          recovery_source: expiredLeaseRecovery ? 'expired_writer_lease' : 'live_send_failure',
          handoff_id: handoff.handoff_id,
          handoff_attempt_id: handoff.handoff_attempt_id,
          handoff_attempt_no: handoff.handoff_attempt_no,
          provider_attempt_id: handoff.provider_attempt_id,
          lease_epoch: handoff.lease_epoch,
          last_send_started_at: handoff.last_send_started_at,
        },
      });
      return {
        status: 'delivery_unknown',
        interaction_id: request.interaction_id,
        handoff_id: handoff.handoff_id,
        audit_id: unknown.auditId,
        turn_id: turn.turn_id,
        turn_state: 'recovering',
        turn_version: unknown.event.turn_version,
      };
    });
    return markUnknown.immediate();
  }

  function reconcileExpiredStartedInteractionHandoffs() {
    const reconciledAt = now();
    const candidates = database.prepare(`
      SELECT interaction.handoff_version, interaction.request_json, handoff.record_json
      FROM runtime_interactions AS interaction
      JOIN runtime_interaction_handoffs AS handoff
        ON handoff.interaction_id = interaction.interaction_id
      JOIN runtime_turns AS turn ON turn.turn_id = interaction.turn_id
      JOIN runtime_executor_leases AS lease
        ON lease.conversation_id = interaction.conversation_id
      WHERE interaction.state = 'answer_delivering'
        AND interaction.handoff_state = 'delivering'
        AND handoff.state = 'delivering'
        AND handoff.parent_type = 'provider_turn'
        AND json_extract(handoff.record_json, '$.last_send_started_at') IS NOT NULL
        AND turn.state = 'waiting_user'
        AND lease.turn_id = turn.turn_id
        AND lease.attempt_id = turn.attempt_id
        AND lease.attempt_no = turn.attempt_no
        AND lease.lease_epoch = turn.lease_epoch
        AND lease.lease_expires_at IS NOT NULL
        AND lease.lease_expires_at <= ?
      ORDER BY interaction.created_at, interaction.interaction_id
    `).all(reconciledAt);
    const results = [];
    for (const row of candidates) {
      const request = JSON.parse(row.request_json);
      const handoff = JSON.parse(row.record_json);
      try {
        results.push(markInteractionHandoffDeliveryUnknown({
          request,
          handoff,
          handoff_version: row.handoff_version,
        }, null, { expiredLeaseRecovery: true }));
      } catch (error) {
        if (
          error instanceof ExecutorPersistenceError
          && ['stale_attempt', 'illegal_transition'].includes(error.code)
        ) {
          continue;
        }
        throw error;
      }
    }
    return results;
  }

  function acknowledgeInteractionHandoff(acknowledgement, { holdForPermission = false } = {}) {
    const acknowledge = database.transaction(() => {
      const acknowledgedAt = now();
      if (!['accepted', 'deny'].includes(acknowledgement?.status)) {
        conflict('invalid_acknowledgement', 'The happy-path handler acknowledgement must be accepted or deny.');
      }
      const row = database.prepare(`
        SELECT interaction.handoff_version, interaction.request_json,
          handoff.record_json
        FROM runtime_interaction_handoffs AS handoff
        JOIN runtime_interactions AS interaction
          ON interaction.interaction_id = handoff.interaction_id
        WHERE handoff.handoff_id = ?
      `).get(acknowledgement.handoff_id);
      if (!row) {
        conflict('handoff_not_found', `Interaction handoff ${acknowledgement.handoff_id} does not exist.`);
      }
      const request = JSON.parse(row.request_json);
      const handoff = JSON.parse(row.record_json);
      const turn = loadTurn(database, request.turn_id);
      const fence = {
        attempt_id: turn.attempt_id,
        attempt_no: turn.attempt_no,
        lease_epoch: turn.lease_epoch,
      };
      const isProviderHandoff = request.parent_type === 'provider_turn';
      const matchesCurrentHandoff = request.state === 'answer_delivering'
        && handoff.state === 'delivering'
        && handoff.claimed_by === serviceInstanceId
        && handoff.last_send_started_at !== null
        && (isProviderHandoff
          ? request.runtime_fence?.provider_attempt_id === fence.attempt_id
            && request.runtime_fence?.lease_epoch === fence.lease_epoch
            && handoff.provider_attempt_id === fence.attempt_id
            && handoff.lease_epoch === fence.lease_epoch
          : request.parent_type === 'recovery_control'
            && request.runtime_fence === null
            && handoff.parent_type === 'recovery_control'
            && handoff.provider_attempt_id === null
            && handoff.lease_epoch === null
            && turn.state === 'recovering')
        && acknowledgement.provider_attempt_id === handoff.provider_attempt_id
        && acknowledgement.handoff_attempt_id === handoff.handoff_attempt_id
        && acknowledgement.handoff_attempt_no === handoff.handoff_attempt_no
        && acknowledgement.lease_epoch === handoff.lease_epoch;
      if (!matchesCurrentHandoff) {
        const auditId = generateId('audit');
        database.prepare(`
          INSERT INTO runtime_interaction_audit (
            audit_id, interaction_id, handoff_id, outcome, provider_attempt_id,
            lease_epoch, acknowledgement_json, created_at
          ) VALUES (?, ?, ?, 'late_ack_ignored', ?, ?, ?, ?)
        `).run(
          auditId,
          request.interaction_id,
          handoff.handoff_id,
          handoff.provider_attempt_id,
          handoff.lease_epoch,
          JSON.stringify({
            status: 'late_ack_ignored',
            received_acknowledgement: acknowledgement,
            current_interaction_state: request.state,
            current_interaction_version: request.version,
            current_handoff_state: handoff.state,
            current_handoff_attempt_id: handoff.handoff_attempt_id,
            current_handoff_attempt_no: handoff.handoff_attempt_no,
          }),
          acknowledgedAt,
        );
        return {
          status: 'ignored',
          code: 'stale_attempt',
          audit_id: auditId,
          interaction_id: request.interaction_id,
          handoff_id: handoff.handoff_id,
        };
      }
      if (isProviderHandoff) {
        assertActiveFence(database, turn, fence, serviceInstanceId);
        assertResidentOwner(turn.conversation_id);
      }
      validateInteractionTransition({
        from: 'answer_delivering',
        to: 'answered',
        sendStarted: true,
        occurredAt: acknowledgedAt,
      });
      validateInteractionHandoffTransition({
        from: 'delivering',
        to: 'accepted',
        sendStarted: true,
        occurredAt: acknowledgedAt,
      });

      const updatedRequest = {
        ...request,
        state: 'answered',
        version: request.version + 1,
        handoff_state: 'accepted',
      };
      const updatedHandoff = {
        ...handoff,
        state: 'accepted',
        provider_acked_at: acknowledgedAt,
        side_effect_status: 'known',
      };
      validateInteractionRequest(updatedRequest, { occurredAt: acknowledgedAt });
      validateInteractionHandoff(updatedHandoff, { occurredAt: acknowledgedAt });
      const nextHandoffVersion = row.handoff_version + 1;
      const interactionUpdate = database.prepare(`
        UPDATE runtime_interactions
        SET state = 'answered', version = ?, handoff_state = 'accepted',
          handoff_version = ?, request_json = ?, updated_at = ?
        WHERE interaction_id = ? AND state = 'answer_delivering' AND version = ?
          AND handoff_state = 'delivering' AND handoff_version = ?
      `).run(
        updatedRequest.version,
        nextHandoffVersion,
        JSON.stringify(updatedRequest),
        acknowledgedAt,
        request.interaction_id,
        request.version,
        row.handoff_version,
      );
      const handoffUpdate = database.prepare(`
        UPDATE runtime_interaction_handoffs
        SET state = 'accepted', record_json = ?, updated_at = ?
        WHERE handoff_id = ? AND state = 'delivering'
          AND handoff_attempt_id = ? AND handoff_attempt_no = ?
          AND provider_attempt_id IS ? AND lease_epoch IS ?
      `).run(
        JSON.stringify(updatedHandoff),
        acknowledgedAt,
        handoff.handoff_id,
        handoff.handoff_attempt_id,
        handoff.handoff_attempt_no,
        handoff.provider_attempt_id,
        handoff.lease_epoch,
      );
      assertPairedInteractionHandoffCas(
        interactionUpdate,
        handoffUpdate,
        'stale_attempt',
        'The handler acknowledgement lost its interaction/handoff fence.',
      );
      const auditId = generateId('audit');
      database.prepare(`
        INSERT INTO runtime_interaction_audit (
          audit_id, interaction_id, handoff_id, outcome, provider_attempt_id,
          lease_epoch, acknowledgement_json, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      `).run(
        auditId,
        request.interaction_id,
        handoff.handoff_id,
        acknowledgement.status,
        handoff.provider_attempt_id,
        handoff.lease_epoch,
        JSON.stringify(acknowledgement),
        acknowledgedAt,
      );

      const hasNextBlockingInteraction = database.prepare(`
        SELECT 1
        FROM runtime_interactions
        WHERE turn_id = ? AND interaction_id != ?
          AND state IN (${BLOCKING_INTERACTION_STATES_SQL})
        LIMIT 1
      `).get(turn.turn_id, request.interaction_id) !== undefined;
      const remainsBlocked = !isProviderHandoff
        || hasNextBlockingInteraction
        || holdForPermission;
      let currentTurn = turn;
      if (isProviderHandoff && !remainsBlocked) {
        transitionInTransaction(database, {
          turnId: turn.turn_id,
          fromState: 'waiting_user',
          toState: 'running',
          fence,
          provider,
          serviceInstanceId,
          occurredAt: acknowledgedAt,
          generateId,
          reasonCode: 'interaction_answered',
        });
        currentTurn = loadTurn(database, turn.turn_id);
      }
      const currentPhase = isProviderHandoff
        ? (remainsBlocked ? 'waiting_user' : 'running')
        : 'recovering';
      const eventDescriptor = {
          kind: 'interaction_answered',
          phase: currentPhase,
          payload: {
            interaction_id: request.interaction_id,
            ordinal: request.ordinal,
            interaction_version: updatedRequest.version,
            handoff_version: nextHandoffVersion,
            state: 'answered',
            handoff_state: 'accepted',
          },
        };
      const event = persistInteractionLifecycleEventInTransaction(database, {
        turn: currentTurn,
        state: currentPhase,
        descriptor: eventDescriptor,
        fence,
        provider,
        occurredAt: acknowledgedAt,
        generateId,
        staleMessage: 'The handler acknowledgement lost its durable turn fence.',
      });
      return {
        status: acknowledgement.status,
        interaction_id: request.interaction_id,
        handoff_id: handoff.handoff_id,
        audit_id: auditId,
        resumed: isProviderHandoff && !remainsBlocked,
        turn_state: currentPhase,
        turn_version: event.turn_version,
      };
    });
    const result = acknowledge.immediate();
    if (result.status === 'ignored') {
      const error = new ExecutorPersistenceError(
        result.code,
        'The handler acknowledgement does not match the delivering handoff fence.',
      );
      error.audit_id = result.audit_id;
      throw error;
    }
    return result;
  }

  function completeRecoveryControlHandoff(delivery) {
    const complete = database.transaction(() => {
      if (
        delivery?.request?.parent_type !== 'recovery_control'
        || delivery?.handoff?.parent_type !== 'recovery_control'
      ) {
        conflict(
          'provider_context_invalid',
          'Only a Core-owned recovery control may use atomic local completion.',
        );
      }
      const sendingDelivery = markInteractionHandoffSendStarted(delivery);
      const acknowledgement = acknowledgeInteractionHandoff({
        status: 'accepted',
        handoff_id: sendingDelivery.handoff.handoff_id,
        provider_attempt_id: null,
        handoff_attempt_id: sendingDelivery.handoff.handoff_attempt_id,
        handoff_attempt_no: sendingDelivery.handoff.handoff_attempt_no,
        lease_epoch: null,
      });
      const replyMappingRecovery = database.prepare(`
        SELECT recovery_id, turn_id, state
        FROM runtime_reply_mapping_recoveries
        WHERE recovery_id = ? AND turn_id = ?
      `).get(
        delivery.request.control_id,
        delivery.request.turn_id,
      );
      const executionRecovery = database.prepare(`
        SELECT recovery_id, turn_id, attempt_id, attempt_no, lease_epoch, state
        FROM runtime_execution_recoveries
        WHERE recovery_id = ? AND turn_id = ? AND interaction_id = ?
      `).get(
        delivery.request.control_id,
        delivery.request.turn_id,
        delivery.request.interaction_id,
      );
      let recoveryResolution = null;
      if (replyMappingRecovery) {
        if (
          delivery.request.kind !== 'recovery_decision'
          || replyMappingRecovery.state !== 'waiting_decision'
        ) {
          conflict('stale_attempt', 'The reply-mapping recovery decision lost its recovery fence.');
        }
        const decision = delivery.answer.value?.kind === 'decision'
          ? delivery.answer.value.decision
          : delivery.answer.value?.choice_id;
        if (!['approve', 'deny'].includes(decision)) {
          conflict('provider_context_invalid', 'A recovery decision must approve or deny.');
        }
        const decidedAt = now();
        if (decision === 'deny') {
          const recoveryUpdate = database.prepare(`
            UPDATE runtime_reply_mapping_recoveries
            SET state = 'rejected', native_recovery_status = 'rejected', updated_at = ?
            WHERE recovery_id = ? AND state = 'waiting_decision'
              AND bound_lineage_id IS NULL
          `).run(decidedAt, replyMappingRecovery.recovery_id);
          if (recoveryUpdate.changes !== 1) {
            conflict('version_conflict', 'The recovery rejection changed concurrently.');
          }
          transitionAttemptlessInTransaction(database, {
            turnId: replyMappingRecovery.turn_id,
            fromState: 'recovering',
            toState: 'stopped',
            occurredAt: decidedAt,
            generateId,
            reasonCode: 'reply_mapping_recovery_rejected',
          });
          const queueUpdate = database.prepare(`
            UPDATE runtime_turn_queue
            SET status = 'stopped', wait_reason = NULL
            WHERE turn_id = ? AND status = 'claimed'
          `).run(replyMappingRecovery.turn_id);
          if (queueUpdate.changes !== 1) {
            conflict('stale_attempt', 'The recovery rejection lost its queue fence.');
          }
          recoveryResolution = {
            status: 'rejected',
            recovery_id: replyMappingRecovery.recovery_id,
            turn_id: replyMappingRecovery.turn_id,
          };
        } else {
          const recoveryUpdate = database.prepare(`
            UPDATE runtime_reply_mapping_recoveries
            SET state = 'native_recovery_not_applicable',
              native_recovery_status = 'authorized_fallback',
              native_recovery_owner_service_instance_id = ?,
              native_recovery_claim_expires_at = ?, updated_at = ?
            WHERE recovery_id = ? AND state = 'waiting_decision'
              AND native_recovery_attempt_count = 0 AND bound_lineage_id IS NULL
          `).run(
            serviceInstanceId,
            residentOwnerExpiresAt(decidedAt),
            decidedAt,
            replyMappingRecovery.recovery_id,
          );
          const queueUpdate = database.prepare(`
            UPDATE runtime_turn_queue
            SET wait_reason = 'reply_mapping_recovery_binding'
            WHERE turn_id = ? AND status = 'claimed'
              AND wait_reason = 'reply_mapping_recovery_decision'
          `).run(replyMappingRecovery.turn_id);
          if (recoveryUpdate.changes !== 1 || queueUpdate.changes !== 1) {
            conflict('version_conflict', 'The recovery confirmation lost its decision fence.');
          }
          recoveryResolution = completeReplyMappingRecovery({
            recovery_id: replyMappingRecovery.recovery_id,
            turn_id: replyMappingRecovery.turn_id,
          }, {
            status: 'authorized_fallback',
            recovery_id: replyMappingRecovery.recovery_id,
            side_effect_status: 'unknown',
          });
        }
      } else if (executionRecovery) {
        if (
          delivery.request.kind !== 'recovery_decision'
          || executionRecovery.state !== 'waiting_decision'
        ) {
          conflict('stale_attempt', 'The execution recovery decision lost its recovery fence.');
        }
        const decision = delivery.answer.value?.decision;
        if (!['approve', 'deny'].includes(decision)) {
          conflict('provider_context_invalid', 'An execution recovery decision must approve or deny.');
        }
        const decidedAt = now();
        const recoveryState = decision === 'approve' ? 'authorized' : 'stopped';
        const recoveryUpdate = database.prepare(`
          UPDATE runtime_execution_recoveries
          SET state = ?, updated_at = ?
          WHERE recovery_id = ? AND turn_id = ? AND state = 'waiting_decision'
            AND attempt_id = ? AND attempt_no = ? AND lease_epoch = ?
        `).run(
          recoveryState,
          decidedAt,
          executionRecovery.recovery_id,
          executionRecovery.turn_id,
          executionRecovery.attempt_id,
          executionRecovery.attempt_no,
          executionRecovery.lease_epoch,
        );
        if (recoveryUpdate.changes !== 1) {
          conflict('version_conflict', 'The execution recovery decision changed concurrently.');
        }
        if (decision === 'deny') {
          transitionInTransaction(database, {
            turnId: executionRecovery.turn_id,
            fromState: 'recovering',
            toState: 'stopped',
            fence: {
              attempt_id: executionRecovery.attempt_id,
              attempt_no: executionRecovery.attempt_no,
              lease_epoch: executionRecovery.lease_epoch,
            },
            provider,
            serviceInstanceId,
            occurredAt: decidedAt,
            generateId,
            reasonCode: 'execution_recovery_stopped_by_user',
            requireActiveLease: false,
            retainLease: true,
          });
          database.prepare(`
            UPDATE runtime_provider_attempts
            SET state = 'stopped', side_effect_status = 'unknown', updated_at = ?
            WHERE turn_id = ? AND attempt_id = ? AND attempt_no = ? AND lease_epoch = ?
          `).run(
            decidedAt,
            executionRecovery.turn_id,
            executionRecovery.attempt_id,
            executionRecovery.attempt_no,
            executionRecovery.lease_epoch,
          );
        }
        recoveryResolution = {
          status: recoveryState,
          recovery_id: executionRecovery.recovery_id,
          turn_id: executionRecovery.turn_id,
          automatic_replay: false,
          lease_released: false,
        };
      }
      return { acknowledgement, sendingDelivery, recovery_resolution: recoveryResolution };
    });
    return complete.immediate();
  }

  function beginInteractionHandoff(handoffId) {
    const begin = database.transaction(() => {
      const delivery = claimInteractionHandoff(handoffId);
      if (delivery.request.parent_type !== 'recovery_control') {
        return { acknowledgement: null, delivery };
      }
      const completed = completeRecoveryControlHandoff(delivery);
      return { ...completed, delivery: null };
    });
    return begin.immediate();
  }

  function getInteractionHandoffForRecovery(handoffId) {
    const row = database.prepare(`
      SELECT interaction.handoff_version, interaction.request_json,
        answer.answer_json, handoff.record_json
      FROM runtime_interaction_handoffs AS handoff
      JOIN runtime_interactions AS interaction
        ON interaction.interaction_id = handoff.interaction_id
      JOIN runtime_interaction_answers AS answer
        ON answer.answer_id = handoff.answer_id
      WHERE handoff.handoff_id = ?
    `).get(handoffId);
    if (!row) conflict('handoff_not_found', `Interaction handoff ${handoffId} does not exist.`);
    const request = JSON.parse(row.request_json);
    const answer = JSON.parse(row.answer_json);
    const handoff = JSON.parse(row.record_json);
    const turn = loadTurn(database, request.turn_id);
    if (
      request.state !== 'delivery_unknown'
      || request.handoff_state !== 'delivery_unknown'
      || handoff.state !== 'delivery_unknown'
      || turn.state !== 'recovering'
    ) {
      conflict('illegal_transition', 'Only a recovering delivery-unknown handoff can be reconciled.');
    }
    if (
      request.runtime_fence?.provider_attempt_id !== handoff.provider_attempt_id
      || request.runtime_fence?.lease_epoch !== handoff.lease_epoch
      || turn.attempt_id !== handoff.provider_attempt_id
      || turn.lease_epoch !== handoff.lease_epoch
    ) {
      conflict('stale_attempt', 'The delivery-unknown handoff lost its interaction runtime fence.');
    }
    validateInteractionRequest(request);
    validateInteractionHandoff(handoff);
    return {
      request,
      answer,
      handoff,
      handoff_version: row.handoff_version,
    };
  }

  function assertInteractionHandoffRecoveryNoticeDelivered(delivery) {
    const current = getInteractionHandoffForRecovery(delivery?.handoff?.handoff_id);
    if (!matchesInteractionHandoffDelivery({
      delivery,
      request: current.request,
      handoff: current.handoff,
      handoffVersion: current.handoff_version,
    })) {
      conflict('stale_attempt', 'The recovery notice check lost its handoff fence.');
    }
    const unknownEvent = database.prepare(`
      SELECT event_sequence, event_json
      FROM runtime_normalized_events
      WHERE turn_id = ?
      ORDER BY event_sequence DESC
    `).all(current.request.turn_id).find(({ event_json: eventJson }) => {
      const event = JSON.parse(eventJson);
      return event.kind === 'interaction_answer_delivery_unknown'
        && event.payload?.interaction_id === current.request.interaction_id
        && event.payload?.handoff_version === current.handoff_version;
    });
    if (!unknownEvent) {
      conflict('notification_pending', 'The delivery-unknown user notice is not durable.');
    }
    const delivered = database.prepare(`
      SELECT projection.materialized_outbox_id AS outbox_id,
        projection.event_sequence_through
      FROM runtime_projection_snapshots AS projection
      JOIN runtime_outbox AS outbox
        ON outbox.outbox_id = projection.materialized_outbox_id
      WHERE projection.turn_id = ?
        AND projection.event_sequence_through >= ?
        AND outbox.status = 'delivered'
      ORDER BY projection.event_sequence_through ASC
      LIMIT 1
    `).get(current.request.turn_id, unknownEvent.event_sequence);
    if (!delivered) {
      conflict(
        'notification_pending',
        'Recovery must wait until a user-visible delivery-unknown notice is delivered.',
      );
    }
    return {
      outbox_id: delivered.outbox_id,
      event_sequence_through: delivered.event_sequence_through,
    };
  }

  function acknowledgeInteractionHandoffFromQuery(delivery, proof) {
    const acknowledge = database.transaction(() => {
      const acknowledgedAt = now();
      const row = database.prepare(`
        SELECT interaction.handoff_version, interaction.request_json,
          handoff.record_json
        FROM runtime_interaction_handoffs AS handoff
        JOIN runtime_interactions AS interaction
          ON interaction.interaction_id = handoff.interaction_id
        WHERE handoff.handoff_id = ?
      `).get(delivery?.handoff?.handoff_id);
      if (!row) conflict('handoff_not_found', 'The delivery-unknown handoff does not exist.');
      const request = JSON.parse(row.request_json);
      const handoff = JSON.parse(row.record_json);
      const turn = loadTurn(database, request.turn_id);
      if (
        proof?.status !== 'accepted'
        || proof.read_only !== true
        || proof.idempotent !== true
        || typeof proof.accepted_at !== 'string'
        || Number.isNaN(Date.parse(proof.accepted_at))
        || Date.parse(proof.accepted_at) < Date.parse(handoff.last_send_started_at)
        || typeof proof.evidence_ref !== 'string'
        || proof.evidence_ref.length === 0
        || proof.reason_code !== null
      ) {
        conflict(
          'provider_context_invalid',
          'Delivery-unknown acceptance requires read-only idempotent provider proof.',
        );
      }
      if (
        request.state !== 'delivery_unknown'
        || handoff.state !== 'delivery_unknown'
        || turn.state !== 'recovering'
        || request.runtime_fence?.provider_attempt_id !== handoff.provider_attempt_id
        || request.runtime_fence?.lease_epoch !== handoff.lease_epoch
        || turn.attempt_id !== handoff.provider_attempt_id
        || turn.lease_epoch !== handoff.lease_epoch
        || !matchesInteractionHandoffDelivery({
          delivery,
          request,
          handoff,
          handoffVersion: row.handoff_version,
        })
        || proof.handoff_id !== handoff.handoff_id
        || proof.handoff_attempt_id !== handoff.handoff_attempt_id
        || proof.handoff_attempt_no !== handoff.handoff_attempt_no
        || proof.provider_attempt_id !== handoff.provider_attempt_id
        || proof.lease_epoch !== handoff.lease_epoch
      ) {
        conflict('stale_attempt', 'The acceptance proof does not match the delivery-unknown handoff fence.');
      }
      validateInteractionTransition({
        from: 'delivery_unknown',
        to: 'answered',
        sendStarted: true,
        acknowledgementProven: true,
        occurredAt: acknowledgedAt,
      });
      validateInteractionHandoffTransition({
        from: 'delivery_unknown',
        to: 'accepted',
        sendStarted: true,
        acknowledgementProven: true,
        occurredAt: acknowledgedAt,
      });

      const updatedRequest = {
        ...request,
        state: 'answered',
        version: request.version + 1,
        handoff_state: 'accepted',
      };
      const updatedHandoff = {
        ...handoff,
        state: 'accepted',
        provider_acked_at: proof.accepted_at,
        reason_code: null,
        error: null,
        side_effect_status: 'known',
      };
      validateInteractionRequest(updatedRequest, { occurredAt: acknowledgedAt });
      validateInteractionHandoff(updatedHandoff, { occurredAt: acknowledgedAt });
      const nextHandoffVersion = row.handoff_version + 1;
      const interactionUpdate = database.prepare(`
        UPDATE runtime_interactions
        SET state = 'answered', version = ?, handoff_state = 'accepted',
          handoff_version = ?, request_json = ?, updated_at = ?
        WHERE interaction_id = ? AND state = 'delivery_unknown' AND version = ?
          AND handoff_state = 'delivery_unknown' AND handoff_version = ?
      `).run(
        updatedRequest.version,
        nextHandoffVersion,
        JSON.stringify(updatedRequest),
        acknowledgedAt,
        request.interaction_id,
        request.version,
        row.handoff_version,
      );
      const handoffUpdate = database.prepare(`
        UPDATE runtime_interaction_handoffs
        SET state = 'accepted', record_json = ?, updated_at = ?
        WHERE handoff_id = ? AND state = 'delivery_unknown'
          AND handoff_attempt_id = ? AND handoff_attempt_no = ?
          AND provider_attempt_id = ? AND lease_epoch = ?
      `).run(
        JSON.stringify(updatedHandoff),
        acknowledgedAt,
        handoff.handoff_id,
        handoff.handoff_attempt_id,
        handoff.handoff_attempt_no,
        handoff.provider_attempt_id,
        handoff.lease_epoch,
      );
      assertPairedInteractionHandoffCas(
        interactionUpdate,
        handoffUpdate,
        'stale_attempt',
        'The acceptance proof lost its delivery-unknown fence.',
      );
      const auditId = generateId('audit');
      database.prepare(`
        INSERT INTO runtime_interaction_audit (
          audit_id, interaction_id, handoff_id, outcome, provider_attempt_id,
          lease_epoch, acknowledgement_json, created_at
        ) VALUES (?, ?, ?, 'accepted_via_query', ?, ?, ?, ?)
      `).run(
        auditId,
        request.interaction_id,
        handoff.handoff_id,
        handoff.provider_attempt_id,
        handoff.lease_epoch,
        JSON.stringify({
          ...proof,
          acknowledgement_source: 'read_only_idempotent_query',
        }),
        acknowledgedAt,
      );
      const fence = {
        attempt_id: turn.attempt_id,
        attempt_no: turn.attempt_no,
        lease_epoch: turn.lease_epoch,
      };
      const event = buildEvent({
        turn,
        lastEvent: loadLastEvent(database, turn.turn_id),
        fence,
        provider,
        descriptor: {
          kind: 'interaction_answered',
          phase: 'recovering',
          provider_native_id: turn.provider_native_id,
          payload: {
            interaction_id: request.interaction_id,
            ordinal: request.ordinal,
            interaction_version: updatedRequest.version,
            handoff_version: nextHandoffVersion,
            state: 'answered',
            handoff_state: 'accepted',
          },
        },
        occurredAt: acknowledgedAt,
        generateId,
      });
      commitTurnEvent(database, {
        turn,
        event,
        fence,
        nextState: 'recovering',
        staleMessage: 'The acceptance proof lost its recovering turn fence.',
        generateId,
      });
      return {
        status: 'accepted',
        acknowledgement_source: 'read_only_idempotent_query',
        interaction_id: request.interaction_id,
        handoff_id: handoff.handoff_id,
        handoff_version: nextHandoffVersion,
        audit_id: auditId,
        turn_id: turn.turn_id,
        turn_state: 'recovering',
        turn_version: event.turn_version,
      };
    });
    return acknowledge.immediate();
  }

  function recordInteractionHandoffQueryUnproven(delivery, proof) {
    const record = database.transaction(() => {
      const occurredAt = now();
      const current = getInteractionHandoffForRecovery(delivery?.handoff?.handoff_id);
      if (
        proof?.status !== 'unknown'
        || proof.read_only !== true
        || proof.idempotent !== true
        || proof.handoff_id !== current.handoff.handoff_id
        || proof.handoff_attempt_id !== current.handoff.handoff_attempt_id
        || proof.handoff_attempt_no !== current.handoff.handoff_attempt_no
        || proof.provider_attempt_id !== current.handoff.provider_attempt_id
        || proof.lease_epoch !== current.handoff.lease_epoch
        || proof.accepted_at !== null
        || proof.evidence_ref !== null
        || typeof proof.reason_code !== 'string'
        || proof.reason_code.length === 0
      ) {
        conflict(
          'provider_context_invalid',
          'An unproven acceptance query must be read-only, idempotent, and match the handoff fence.',
        );
      }
      if (!matchesInteractionHandoffDelivery({
        delivery,
        request: current.request,
        handoff: current.handoff,
        handoffVersion: current.handoff_version,
      })) {
        conflict('stale_attempt', 'The unproven query result lost its handoff fence.');
      }
      const auditId = generateId('audit');
      database.prepare(`
        INSERT INTO runtime_interaction_audit (
          audit_id, interaction_id, handoff_id, outcome, provider_attempt_id,
          lease_epoch, acknowledgement_json, created_at
        ) VALUES (?, ?, ?, 'query_unproven', ?, ?, ?, ?)
      `).run(
        auditId,
        current.request.interaction_id,
        current.handoff.handoff_id,
        current.handoff.provider_attempt_id,
        current.handoff.lease_epoch,
        JSON.stringify(proof),
        occurredAt,
      );
      return {
        status: 'unknown',
        interaction_id: current.request.interaction_id,
        handoff_id: current.handoff.handoff_id,
        audit_id: auditId,
        turn_id: current.request.turn_id,
        turn_state: 'recovering',
      };
    });
    return record.immediate();
  }

  function resolveInteractionHandoffDisposition(delivery, disposition, authorization) {
    const resolve = database.transaction(() => {
      const occurredAt = now();
      const current = getInteractionHandoffForRecovery(delivery?.handoff?.handoff_id);
      const { request, handoff } = current;
      const turn = loadTurn(database, request.turn_id);
      const dispositionKeys = disposition && typeof disposition === 'object'
        ? Object.keys(disposition).sort()
        : [];
      if (
        dispositionKeys.length !== 2
        || dispositionKeys[0] !== 'action'
        || dispositionKeys[1] !== 'replacement_interaction'
        || !['terminate', 'establish_interaction'].includes(disposition.action)
        || (disposition.action === 'terminate' && disposition.replacement_interaction !== null)
        || (disposition.action === 'establish_interaction'
          && (!disposition.replacement_interaction
            || typeof disposition.replacement_interaction !== 'object'
            || Array.isArray(disposition.replacement_interaction)
            || Object.keys(disposition.replacement_interaction).sort().join(',')
              !== 'allowed_sources,authorized_subjects,choices,kind,prompt'))
      ) {
        conflict('provider_context_invalid', 'Invalid interaction handoff disposition.');
      }
      const expectedScope = {
        conversation_id: request.conversation_id,
        turn_id: request.turn_id,
        handoff_id: handoff.handoff_id,
        action: disposition.action,
        replacement_interaction: disposition.replacement_interaction,
      };
      const authorizationKeys = authorization && typeof authorization === 'object'
        ? Object.keys(authorization).sort()
        : [];
      const scopeKeys = authorization?.scope && typeof authorization.scope === 'object'
        ? Object.keys(authorization.scope).sort()
        : [];
      if (
        authorizationKeys.join(',')
          !== 'actor_id,authorized,authorized_at,capability,decision_id,policy_id,policy_version,scope'
        || authorization.authorized !== true
        || typeof authorization.decision_id !== 'string'
        || authorization.decision_id.length === 0
        || typeof authorization.actor_id !== 'string'
        || authorization.actor_id.length === 0
        || authorization.capability !== 'interaction.handoff.resolve'
        || typeof authorization.policy_id !== 'string'
        || authorization.policy_id.length === 0
        || !Number.isSafeInteger(authorization.policy_version)
        || authorization.policy_version < 1
        || typeof authorization.authorized_at !== 'string'
        || Number.isNaN(Date.parse(authorization.authorized_at))
        || scopeKeys.join(',')
          !== 'action,conversation_id,handoff_id,replacement_interaction,turn_id'
        || canonicalizeJson(authorization.scope) !== canonicalizeJson(expectedScope)
      ) {
        conflict('authorization_denied', 'A trusted scoped authorization is required.');
      }
      if (!matchesInteractionHandoffDelivery({
        delivery,
        request,
        handoff,
        handoffVersion: current.handoff_version,
      })) {
        conflict('stale_attempt', 'The authorized disposition lost its handoff fence.');
      }

      let replacement = null;
      if (disposition.action === 'establish_interaction') {
        const lease = database.prepare(`
          SELECT lease_owner, turn_id, attempt_id, attempt_no, lease_epoch, lease_expires_at
          FROM runtime_executor_leases
          WHERE conversation_id = ?
        `).get(turn.conversation_id);
        const releasedLease = lease
          && lease.lease_epoch === handoff.lease_epoch
          && lease.lease_owner === null
          && lease.turn_id === null
          && lease.attempt_id === null
          && lease.attempt_no === null;
        const expiredFencedLease = lease
          && lease.lease_epoch === handoff.lease_epoch
          && lease.turn_id === turn.turn_id
          && lease.attempt_id === handoff.provider_attempt_id
          && lease.attempt_no === turn.attempt_no
          && lease.lease_expires_at !== null
          && lease.lease_expires_at <= occurredAt;
        if (!releasedLease && !expiredFencedLease) {
          conflict(
            'recovery_isolation_pending',
            'A new recovery interaction requires a released or expired prior writer lease.',
          );
        }
        const descriptor = disposition.replacement_interaction;
        const controlId = generateId('recovery-control');
        replacement = {
          contract: 'zylos.interaction-request',
          contract_version: '1.0',
          trace_id: request.trace_id,
          interaction_id: generateId('interaction'),
          conversation_id: request.conversation_id,
          turn_id: request.turn_id,
          lineage_id: request.lineage_id,
          control_id: controlId,
          parent_type: 'recovery_control',
          tool_use_id: null,
          ordinal: 1,
          kind: descriptor.kind,
          prompt: descriptor.prompt,
          choices: structuredClone(descriptor.choices),
          authorized_subjects: structuredClone(descriptor.authorized_subjects),
          allowed_sources: [...descriptor.allowed_sources],
          runtime_fence: null,
          state: 'pending',
          version: 1,
          handoff_state: 'not_started',
          created_at: occurredAt,
          expires_at: new Date(
            Date.parse(occurredAt) + interactionTimeoutMs,
          ).toISOString(),
          card_delivery_id: null,
        };
        validateInteractionRequest(replacement, { occurredAt });
        database.prepare(`
          INSERT INTO runtime_interactions (
            interaction_id, conversation_id, turn_id, lineage_id, parent_type, parent_id, ordinal,
            state, version, handoff_state, handoff_version, request_json,
            created_at, updated_at
          ) VALUES (?, ?, ?, ?, 'recovery_control', ?, 1, 'pending', 1,
            'not_started', NULL, ?, ?, ?)
        `).run(
          replacement.interaction_id,
          replacement.conversation_id,
          replacement.turn_id,
          replacement.lineage_id,
          replacement.control_id,
          JSON.stringify(replacement),
          occurredAt,
          occurredAt,
        );
      }

      validateInteractionTransition({
        from: 'delivery_unknown',
        to: 'cancelled',
        sendStarted: true,
        occurredAt,
      });
      validateInteractionHandoffTransition({
        from: 'delivery_unknown',
        to: 'cancelled',
        sendStarted: true,
        occurredAt,
      });
      const updatedRequest = {
        ...request,
        state: 'cancelled',
        version: request.version + 1,
        handoff_state: 'cancelled',
        terminal_reason: disposition.action === 'terminate'
          ? 'authorized_delivery_unknown_termination'
          : 'authorized_delivery_unknown_replacement',
      };
      const updatedHandoff = {
        ...handoff,
        state: 'cancelled',
        reason_code: updatedRequest.terminal_reason,
      };
      validateInteractionRequest(updatedRequest, { occurredAt });
      validateInteractionHandoff(updatedHandoff, { occurredAt });
      const nextHandoffVersion = current.handoff_version + 1;
      const interactionUpdate = database.prepare(`
        UPDATE runtime_interactions
        SET state = 'cancelled', version = ?, handoff_state = 'cancelled',
          handoff_version = ?, request_json = ?, updated_at = ?
        WHERE interaction_id = ? AND state = 'delivery_unknown' AND version = ?
          AND handoff_state = 'delivery_unknown' AND handoff_version = ?
      `).run(
        updatedRequest.version,
        nextHandoffVersion,
        JSON.stringify(updatedRequest),
        occurredAt,
        request.interaction_id,
        request.version,
        current.handoff_version,
      );
      const handoffUpdate = database.prepare(`
        UPDATE runtime_interaction_handoffs
        SET state = 'cancelled', record_json = ?, updated_at = ?
        WHERE handoff_id = ? AND state = 'delivery_unknown'
          AND handoff_attempt_id = ? AND handoff_attempt_no = ?
          AND provider_attempt_id = ? AND lease_epoch = ?
      `).run(
        JSON.stringify(updatedHandoff),
        occurredAt,
        handoff.handoff_id,
        handoff.handoff_attempt_id,
        handoff.handoff_attempt_no,
        handoff.provider_attempt_id,
        handoff.lease_epoch,
      );
      assertPairedInteractionHandoffCas(
        interactionUpdate,
        handoffUpdate,
        'stale_attempt',
        'The authorized disposition lost its durable handoff fence.',
      );
      const auditId = generateId('audit');
      database.prepare(`
        INSERT INTO runtime_interaction_audit (
          audit_id, interaction_id, handoff_id, outcome, provider_attempt_id,
          lease_epoch, acknowledgement_json, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      `).run(
        auditId,
        request.interaction_id,
        handoff.handoff_id,
        disposition.action === 'terminate'
          ? 'authorized_termination'
          : 'authorized_interaction_established',
        handoff.provider_attempt_id,
        handoff.lease_epoch,
        JSON.stringify({
          disposition,
          authorization,
          previous_request: request,
          previous_handoff: handoff,
          replacement_interaction: replacement,
        }),
        occurredAt,
      );
      const fence = {
        attempt_id: turn.attempt_id,
        attempt_no: turn.attempt_no,
        lease_epoch: turn.lease_epoch,
      };
      let currentTurn = turn;
      const cancellationEvent = buildEvent({
        turn: currentTurn,
        lastEvent: loadLastEvent(database, turn.turn_id),
        fence,
        provider,
        descriptor: {
          kind: 'interaction_cancelled',
          phase: currentTurn.state,
          provider_native_id: currentTurn.provider_native_id,
          payload: {
            interaction_id: request.interaction_id,
            ordinal: request.ordinal,
            interaction_version: updatedRequest.version,
            handoff_version: nextHandoffVersion,
            replacement_interaction_id: replacement?.interaction_id ?? null,
          },
        },
        occurredAt,
        generateId,
      });
      commitTurnEvent(database, {
        turn: currentTurn,
        event: cancellationEvent,
        fence,
        nextState: currentTurn.state,
        staleMessage: 'The authorized disposition lost its recovering turn fence.',
        generateId,
      });
      let finalEvent = cancellationEvent;
      if (replacement !== null) {
        currentTurn = loadTurn(database, turn.turn_id);
        finalEvent = buildEvent({
          turn: currentTurn,
          lastEvent: loadLastEvent(database, turn.turn_id),
          fence,
          provider,
          descriptor: {
            kind: 'interaction_requested',
            phase: 'recovering',
            provider_native_id: currentTurn.provider_native_id,
            payload: {
              interaction_id: replacement.interaction_id,
              ordinal: replacement.ordinal,
              interaction_version: replacement.version,
              handoff_version: null,
              kind: replacement.kind,
              prompt: replacement.prompt,
              choices: structuredClone(replacement.choices),
              allowed_sources: [...replacement.allowed_sources],
            },
          },
          occurredAt,
          generateId,
        });
        commitTurnEvent(database, {
          turn: currentTurn,
          event: finalEvent,
          fence,
          nextState: 'recovering',
          staleMessage: 'The authorized replacement interaction lost its turn fence.',
          generateId,
        });
      }
      return {
        status: disposition.action === 'terminate' ? 'terminated' : 'interaction_established',
        interaction_id: request.interaction_id,
        handoff_id: handoff.handoff_id,
        replacement_interaction_id: replacement?.interaction_id ?? null,
        handoff_version: nextHandoffVersion,
        audit_id: auditId,
        turn_id: turn.turn_id,
        turn_state: 'recovering',
        turn_version: finalEvent.turn_version,
      };
    });
    return resolve.immediate();
  }

  function persistExecutionRecoveryDecisionInTransaction(
    turnContext,
    error,
    occurredAt,
    recoveryKind,
    { requireActiveLease = true } = {},
  ) {
    const existing = database.prepare(`
      SELECT recovery_id, interaction_id, state, notice_event_sequence
      FROM runtime_execution_recoveries WHERE turn_id = ?
    `).get(turnContext.turn_id);
    if (existing) return existing;
    const recoveringTurn = loadTurn(database, turnContext.turn_id);
    if (requireActiveLease) assertTurnContextFence(recoveringTurn, turnContext);
    else if (!sameFence(recoveringTurn, turnContext.attempt)) {
      conflict('stale_attempt', 'Execution recovery lost its durable attempt fence.');
    }
    if (recoveringTurn.state !== 'recovering') {
      conflict('illegal_transition', 'Execution recovery decisions require a recovering turn.');
    }
    const recoveryId = generateId('recovery');
    const recoveryEvent = buildEvent({
      turn: recoveringTurn,
      lastEvent: loadLastEvent(database, recoveringTurn.turn_id),
      fence: turnContext.attempt,
      provider,
      descriptor: {
        kind: 'recovery_started',
        phase: 'recovering',
        provider_native_id: recoveringTurn.provider_native_id,
        payload: {
          recovery_id: recoveryId,
          recovery_of_turn_id: recoveringTurn.turn_id,
          recovery_of_lineage_id: recoveringTurn.lineage_id,
          side_effect_status: 'unknown',
        },
        error,
      },
      occurredAt,
      generateId,
    });
    commitTurnEvent(database, {
      turn: recoveringTurn,
      event: recoveryEvent,
      fence: turnContext.attempt,
      nextState: 'recovering',
      staleMessage: 'The provider failure lost its recovery fence.',
      generateId,
    });
    const currentTurn = loadTurn(database, recoveringTurn.turn_id);
    const envelope = JSON.parse(currentTurn.envelope_json);
    if (envelope.actor?.authenticated !== true) {
      conflict('authorization_denied', 'Execution recovery decisions require an authenticated actor.');
    }
    const interaction = {
      contract: 'zylos.interaction-request',
      contract_version: '1.0',
      trace_id: envelope.trace_id,
      interaction_id: generateId('interaction'),
      conversation_id: currentTurn.conversation_id,
      turn_id: currentTurn.turn_id,
      lineage_id: currentTurn.lineage_id,
      control_id: recoveryId,
      parent_type: 'recovery_control',
      tool_use_id: null,
      ordinal: 1,
      kind: 'recovery_decision',
      prompt: 'Provider work may still be running or may have unknown side effects. Choose how to recover.',
      choices: [
        { choice_id: 'approve', label: 'Authorize recovery' },
        { choice_id: 'deny', label: 'Stop this turn' },
      ],
      authorized_subjects: [{ type: 'actor', actor_id: envelope.actor.actor_id }],
      allowed_sources: [
        'main_card_reply',
        ...(['feishu', 'lark'].includes(envelope.channel) ? ['card_action'] : []),
      ],
      runtime_fence: null,
      state: 'pending',
      version: 1,
      handoff_state: 'not_started',
      created_at: occurredAt,
      expires_at: new Date(Date.parse(occurredAt) + interactionTimeoutMs).toISOString(),
      card_delivery_id: null,
    };
    validateInteractionRequest(interaction, { occurredAt });
    database.prepare(`
      INSERT INTO runtime_interactions (
        interaction_id, conversation_id, turn_id, lineage_id, parent_type, parent_id,
        ordinal, state, version, handoff_state, handoff_version, request_json,
        created_at, updated_at
      ) VALUES (?, ?, ?, ?, 'recovery_control', ?, 1, 'pending', 1,
        'not_started', NULL, ?, ?, ?)
    `).run(
      interaction.interaction_id,
      interaction.conversation_id,
      interaction.turn_id,
      interaction.lineage_id,
      interaction.control_id,
      JSON.stringify(interaction),
      occurredAt,
      occurredAt,
    );
    const waitingEvent = buildEvent({
      turn: currentTurn,
      lastEvent: loadLastEvent(database, currentTurn.turn_id),
      fence: turnContext.attempt,
      provider,
      descriptor: {
        kind: 'recovery_waiting_decision',
        phase: 'recovering',
        provider_native_id: currentTurn.provider_native_id,
        payload: {
          recovery_id: recoveryId,
          recovery_of_turn_id: currentTurn.turn_id,
          recovery_of_lineage_id: currentTurn.lineage_id,
          side_effect_status: 'unknown',
          interaction_id: interaction.interaction_id,
          ordinal: interaction.ordinal,
          interaction_version: interaction.version,
          handoff_version: null,
          kind: interaction.kind,
          prompt: interaction.prompt,
          choices: structuredClone(interaction.choices),
          allowed_sources: [...interaction.allowed_sources],
        },
        error,
      },
      occurredAt,
      generateId,
    });
    commitTurnEvent(database, {
      turn: currentTurn,
      event: waitingEvent,
      fence: turnContext.attempt,
      nextState: 'recovering',
      staleMessage: 'The provider failure lost its recovery-decision fence.',
      generateId,
    });
    const attempt = database.prepare(`
      SELECT service_instance_id, executor_instance_id, runtime_instance_id
      FROM runtime_provider_attempts
      WHERE turn_id = ? AND attempt_id = ? AND attempt_no = ? AND lease_epoch = ?
    `).get(
      currentTurn.turn_id,
      turnContext.attempt.attempt_id,
      turnContext.attempt.attempt_no,
      turnContext.attempt.lease_epoch,
    );
    database.prepare(`
      INSERT INTO runtime_execution_recoveries (
        recovery_id, turn_id, attempt_id, attempt_no, lease_epoch, provider,
        prior_service_instance_id, prior_executor_instance_id, prior_runtime_instance_id,
        recovery_kind, state, side_effect_status, notice_event_sequence,
        interaction_id, error_json, created_at, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'waiting_decision', 'unknown', ?, ?, ?, ?, ?)
    `).run(
      recoveryId,
      currentTurn.turn_id,
      turnContext.attempt.attempt_id,
      turnContext.attempt.attempt_no,
      turnContext.attempt.lease_epoch,
      provider,
      attempt?.service_instance_id ?? null,
      attempt?.executor_instance_id ?? null,
      attempt?.runtime_instance_id ?? null,
      recoveryKind,
      waitingEvent.event_sequence,
      interaction.interaction_id,
      JSON.stringify(error),
      occurredAt,
      occurredAt,
    );
    database.prepare(`
      UPDATE runtime_provider_attempts
      SET state = 'recovering', side_effect_status = 'unknown', error_json = ?, updated_at = ?
      WHERE turn_id = ? AND attempt_id = ? AND attempt_no = ? AND lease_epoch = ?
        AND state IN ('starting', 'running')
    `).run(
      JSON.stringify(error),
      occurredAt,
      currentTurn.turn_id,
      turnContext.attempt.attempt_id,
      turnContext.attempt.attempt_no,
      turnContext.attempt.lease_epoch,
    );
    database.prepare(`
      UPDATE runtime_turn_queue
      SET wait_reason = 'execution_recovery_decision'
      WHERE turn_id = ? AND status = 'claimed'
    `).run(currentTurn.turn_id);
    return {
      recovery_id: recoveryId,
      interaction_id: interaction.interaction_id,
      state: 'waiting_decision',
      notice_event_sequence: waitingEvent.event_sequence,
    };
  }

  function reconcileNonterminalTurns(
    controlledTurnContexts = [],
    recoveryKind = 'sweep_reconciliation',
    { controlledStartingGraceMs = 0 } = {},
  ) {
    if (!Array.isArray(controlledTurnContexts)) {
      throw new TypeError('controlledTurnContexts must be an array');
    }
    if (!['startup_reconciliation', 'sweep_reconciliation'].includes(recoveryKind)) {
      throw new TypeError('recoveryKind must identify startup or sweep reconciliation');
    }
    if (!Number.isFinite(controlledStartingGraceMs) || controlledStartingGraceMs < 0) {
      throw new TypeError('controlledStartingGraceMs must be a non-negative finite number');
    }
    const controlled = new Map(controlledTurnContexts.map((context) => [context.turn_id, context]));
    const reconcile = database.transaction(() => {
      const reconciledAt = now();
      const candidates = database.prepare(`
        SELECT turn.turn_id, turn.conversation_id, turn.state, turn.attempt_id,
          turn.attempt_no, turn.lease_epoch,
          attempt.service_instance_id, attempt.executor_instance_id,
          attempt.runtime_instance_id, attempt.runtime_evidence_json,
          attempt.last_provider_event_at, attempt.started_at,
          lease.lease_owner, lease.turn_id AS lease_turn_id,
          lease.attempt_id AS lease_attempt_id, lease.attempt_no AS lease_attempt_no,
          lease.lease_epoch AS lease_epoch_current, lease.lease_expires_at,
          EXISTS (
            SELECT 1 FROM runtime_interactions AS interaction
            WHERE interaction.turn_id = turn.turn_id
              AND interaction.state IN (${BLOCKING_INTERACTION_STATES_SQL})
          ) AS has_blocking_interaction
        FROM runtime_turns AS turn
        JOIN runtime_lineages AS lineage ON lineage.lineage_id = turn.lineage_id
        LEFT JOIN runtime_provider_attempts AS attempt
          ON attempt.turn_id = turn.turn_id
         AND attempt.attempt_id = turn.attempt_id
         AND attempt.attempt_no = turn.attempt_no
         AND attempt.lease_epoch = turn.lease_epoch
        LEFT JOIN runtime_executor_leases AS lease
          ON lease.conversation_id = turn.conversation_id
        WHERE attempt.provider = ?
          AND turn.state IN ('starting', 'running', 'waiting_user', 'redirecting', 'recovering')
          AND turn.attempt_id IS NOT NULL
          AND NOT EXISTS (
            SELECT 1 FROM runtime_execution_recoveries AS recovery
            WHERE recovery.turn_id = turn.turn_id
          )
        ORDER BY turn.created_at, turn.turn_id
      `).all(provider);
      const results = [];
      const reconciledAtMs = Date.parse(reconciledAt);
      for (const candidate of candidates) {
        if (['redirecting', 'recovering'].includes(candidate.state)) {
          results.push({
            turn_id: candidate.turn_id,
            status: 'specialized_recovery_retained',
          });
          continue;
        }
        const context = controlled.get(candidate.turn_id);
        let evidence = null;
        try {
          evidence = candidate.runtime_evidence_json === null
            ? null
            : JSON.parse(candidate.runtime_evidence_json);
        } catch {
          evidence = null;
        }
        const exactLocalFence = context
          && context.attempt.attempt_id === candidate.attempt_id
          && context.attempt.attempt_no === candidate.attempt_no
          && context.attempt.lease_epoch === candidate.lease_epoch;
        const exactLeaseFence = candidate.service_instance_id === candidate.lease_owner
          && candidate.lease_turn_id === candidate.turn_id
          && candidate.lease_attempt_id === candidate.attempt_id
          && candidate.lease_attempt_no === candidate.attempt_no
          && candidate.lease_epoch_current === candidate.lease_epoch
          && candidate.lease_expires_at !== null
          && candidate.lease_expires_at > reconciledAt;
        const exactLocalIdentity = candidate.service_instance_id === serviceInstanceId
          && candidate.lease_owner === serviceInstanceId
          && exactLocalFence
          && context.executor_instance_id === candidate.executor_instance_id;
        const durableRuntimeEvidence = evidence !== null
          && evidence.runtime_instance_id === candidate.runtime_instance_id
          && typeof evidence.runtime_instance_id === 'string'
          && evidence.runtime_instance_id.length > 0
          && typeof evidence.handle_kind === 'string'
          && evidence.handle_kind.length > 0
          && evidence.controllable === true;
        const providerEventAtMs = Date.parse(candidate.last_provider_event_at);
        const validProviderEventActivity = typeof candidate.last_provider_event_at === 'string'
          && Number.isFinite(providerEventAtMs)
          && providerEventAtMs <= reconciledAtMs;
        const attemptStartedAtMs = Date.parse(candidate.started_at);
        const controlledStartingWithinGrace = recoveryKind === 'sweep_reconciliation'
          && candidate.state === 'starting'
          && exactLocalIdentity
          && exactLeaseFence
          && Number.isFinite(attemptStartedAtMs)
          && reconciledAtMs >= attemptStartedAtMs
          && reconciledAtMs - attemptStartedAtMs < controlledStartingGraceMs;
        if (
          controlledStartingWithinGrace
          || (
            exactLocalIdentity
            && exactLeaseFence
            && durableRuntimeEvidence
            && validProviderEventActivity
          )
        ) {
          results.push({ turn_id: candidate.turn_id, status: 'healthy' });
          continue;
        }
        if (
          exactLeaseFence
          && candidate.lease_owner !== serviceInstanceId
        ) {
          results.push({ turn_id: candidate.turn_id, status: 'foreign_lease_retained' });
          continue;
        }
        if (candidate.state === 'waiting_user' && candidate.has_blocking_interaction === 1) {
          results.push({
            turn_id: candidate.turn_id,
            status: 'specialized_recovery_retained',
          });
          continue;
        }
        const turnContext = {
          turn_id: candidate.turn_id,
          conversation_id: candidate.conversation_id,
          executor_instance_id: candidate.executor_instance_id,
          attempt: {
            attempt_id: candidate.attempt_id,
            attempt_no: candidate.attempt_no,
            lease_epoch: candidate.lease_epoch,
          },
        };
        transitionInTransaction(database, {
          turnId: candidate.turn_id,
          fromState: candidate.state,
          toState: 'recovering',
          fence: turnContext.attempt,
          provider,
          serviceInstanceId,
          occurredAt: reconciledAt,
          generateId,
          reasonCode: 'executor_reconciliation_uncertain',
          requireActiveLease: false,
        });
        const error = createContractError({
          code: 'side_effect_unknown',
          category: 'provider',
          retryable: false,
          sideEffectStatus: 'unknown',
          userMessage: 'Executor ownership or provider runtime isolation could not be proven.',
          occurredAt: reconciledAt,
        });
        const recovery = persistExecutionRecoveryDecisionInTransaction(
          turnContext,
          error,
          reconciledAt,
          recoveryKind,
          { requireActiveLease: false },
        );
        results.push({
          turn_id: candidate.turn_id,
          status: 'waiting_decision',
          recovery_id: recovery.recovery_id,
          interaction_id: recovery.interaction_id,
        });
      }
      return Object.freeze({
        inspected: candidates.length,
        healthy: results.filter(({ status }) => status === 'healthy').length,
        waiting_decision: results.filter(({ status }) => status === 'waiting_decision').length,
        results: Object.freeze(results.map((result) => Object.freeze(result))),
      });
    });
    return reconcile.immediate();
  }

  function markProviderFailure(turnContext, error) {
    const markFailure = database.transaction(() => {
      const occurredAt = now();
      let turn = loadTurn(database, turnContext.turn_id);
      assertActiveFence(database, turn, turnContext.attempt, serviceInstanceId);
      if (['starting', 'running'].includes(turn.state)) {
        recordProviderEventActivityInTransaction(turnContext, occurredAt);
        const fromState = turn.state;
        transitionInTransaction(database, {
          turnId: turn.turn_id,
          fromState,
          toState: 'recovering',
          fence: turnContext.attempt,
          provider,
          serviceInstanceId,
          occurredAt,
          generateId,
          reasonCode: 'provider_connection_lost',
        });
        const recovery = persistExecutionRecoveryDecisionInTransaction(
          turnContext,
          error,
          occurredAt,
          'provider_failure',
        );
        return {
          status: 'recovering',
          turn_id: turn.turn_id,
          turn_state: 'recovering',
          previous_turn_state: fromState,
          cancelled_interaction_ids: [],
          ...recovery,
        };
      }
      if (turn.state !== 'waiting_user') {
        return { status: 'not_active', turn_id: turn.turn_id, turn_state: turn.state };
      }
      const blockingEntries = database.prepare(`
        SELECT interaction.request_json, handoff.record_json AS handoff_json
        FROM runtime_interactions AS interaction
        LEFT JOIN runtime_interaction_handoffs AS handoff
          ON handoff.interaction_id = interaction.interaction_id
        WHERE interaction.turn_id = ?
          AND interaction.state IN (${BLOCKING_INTERACTION_STATES_SQL})
        ORDER BY interaction.ordinal ASC
      `).all(turn.turn_id).map(({ request_json: requestJson, handoff_json: handoffJson }) => ({
        request: JSON.parse(requestJson),
        handoff: handoffJson === null ? null : JSON.parse(handoffJson),
      }));
      if (blockingEntries.length === 0) {
        return { status: 'handoff_in_progress', turn_id: turn.turn_id, turn_state: turn.state };
      }
      for (const { request, handoff } of blockingEntries) {
        const pendingRequest = request.state === 'pending' && handoff === null;
        const committedUnsentHandoff = request.state === 'answer_committed'
          && request.handoff_state === 'pending'
          && handoff?.state === 'pending'
          && handoff.last_send_started_at === null;
        if (!pendingRequest && !committedUnsentHandoff) {
          return { status: 'handoff_in_progress', turn_id: turn.turn_id, turn_state: turn.state };
        }
        if (
          handoff !== null
          && (handoff.provider_attempt_id !== turnContext.attempt.attempt_id
            || handoff.lease_epoch !== turnContext.attempt.lease_epoch)
        ) {
          conflict('stale_attempt', 'The unsent interaction handoff lost its provider failure fence.');
        }
      }
      recordProviderEventActivityInTransaction(turnContext, occurredAt);
      const cancelledInteractionIds = [];
      const cancelledHandoffIds = [];
      for (const { request, handoff } of blockingEntries) {
        validateInteractionTransition({
          from: request.state,
          to: 'cancelled',
          sendStarted: false,
          occurredAt,
        });
        if (handoff !== null) {
          validateInteractionHandoffTransition({
            from: 'pending',
            to: 'cancelled',
            sendStarted: false,
            occurredAt,
          });
        }
        const handoffVersion = handoff === null ? null : 2;
        const cancelledRequest = {
          ...request,
          state: 'cancelled',
          version: request.version + 1,
          handoff_state: handoff === null ? 'not_started' : 'cancelled',
          terminal_reason: 'provider_connection_lost',
        };
        validateInteractionRequest(cancelledRequest, { occurredAt });
        const updated = handoff === null
          ? database.prepare(`
            UPDATE runtime_interactions
            SET state = 'cancelled', version = ?, handoff_state = 'not_started',
              handoff_version = NULL, request_json = ?, updated_at = ?
            WHERE interaction_id = ? AND state = 'pending' AND version = ?
          `).run(
            cancelledRequest.version,
            JSON.stringify(cancelledRequest),
            occurredAt,
            request.interaction_id,
            request.version,
          )
          : database.prepare(`
            UPDATE runtime_interactions
            SET state = 'cancelled', version = ?, handoff_state = 'cancelled',
              handoff_version = 2, request_json = ?, updated_at = ?
            WHERE interaction_id = ? AND state = 'answer_committed' AND version = ?
              AND handoff_state = 'pending' AND handoff_version = 1
          `).run(
            cancelledRequest.version,
            JSON.stringify(cancelledRequest),
            occurredAt,
            request.interaction_id,
            request.version,
          );
        if (updated.changes !== 1) {
          conflict('version_conflict', 'The provider failure lost its interaction fence.');
        }
        if (handoff !== null) {
          const cancelledHandoff = {
            ...handoff,
            state: 'cancelled',
            reason_code: 'provider_connection_lost',
            error: null,
            side_effect_status: 'none',
          };
          validateInteractionHandoff(cancelledHandoff, { occurredAt });
          const handoffUpdate = database.prepare(`
            UPDATE runtime_interaction_handoffs
            SET state = 'cancelled', record_json = ?, updated_at = ?
            WHERE handoff_id = ? AND state = 'pending'
              AND handoff_attempt_id IS NULL AND handoff_attempt_no IS NULL
              AND provider_attempt_id = ? AND lease_epoch = ?
          `).run(
            JSON.stringify(cancelledHandoff),
            occurredAt,
            handoff.handoff_id,
            handoff.provider_attempt_id,
            handoff.lease_epoch,
          );
          if (handoffUpdate.changes !== 1) {
            conflict('version_conflict', 'The provider failure lost its unsent handoff fence.');
          }
          const auditId = generateId('audit');
          database.prepare(`
            INSERT INTO runtime_interaction_audit (
              audit_id, interaction_id, handoff_id, outcome, provider_attempt_id,
              lease_epoch, acknowledgement_json, created_at
            ) VALUES (?, ?, ?, 'cancelled', ?, ?, ?, ?)
          `).run(
            auditId,
            request.interaction_id,
            handoff.handoff_id,
            handoff.provider_attempt_id,
            handoff.lease_epoch,
            JSON.stringify({
              status: 'cancelled',
              reason_code: 'provider_connection_lost',
              provider_attempt_id: handoff.provider_attempt_id,
              lease_epoch: handoff.lease_epoch,
            }),
            occurredAt,
          );
          cancelledHandoffIds.push(handoff.handoff_id);
        }
        const cancellationEvent = buildEvent({
          turn,
          lastEvent: loadLastEvent(database, turn.turn_id),
          fence: turnContext.attempt,
          provider,
          descriptor: {
            kind: 'interaction_cancelled',
            phase: 'waiting_user',
            provider_native_id: turn.provider_native_id,
            payload: {
              interaction_id: request.interaction_id,
              ordinal: request.ordinal,
              interaction_version: cancelledRequest.version,
              handoff_version: handoffVersion,
            },
          },
          occurredAt,
          generateId,
        });
        commitTurnEvent(database, {
          turn,
          event: cancellationEvent,
          fence: turnContext.attempt,
          nextState: 'waiting_user',
          staleMessage: 'The provider failure lost its interaction cancellation fence.',
          generateId,
        });
        cancelledInteractionIds.push(request.interaction_id);
        turn = loadTurn(database, turn.turn_id);
      }
      transitionInTransaction(database, {
        turnId: turn.turn_id,
        fromState: 'waiting_user',
        toState: 'recovering',
        fence: turnContext.attempt,
        provider,
        serviceInstanceId,
        occurredAt,
        generateId,
        reasonCode: 'provider_connection_lost',
      });
      const recovery = persistExecutionRecoveryDecisionInTransaction(
        turnContext,
        error,
        occurredAt,
        'provider_failure',
      );
      return {
        status: 'recovering',
        turn_id: turn.turn_id,
        turn_state: 'recovering',
        cancelled_interaction_ids: cancelledInteractionIds,
        cancelled_handoff_ids: cancelledHandoffIds,
        ...recovery,
      };
    });
    return markFailure.immediate();
  }

  function resumeTurnAfterPermission(turnContext) {
    const resume = database.transaction(() => {
      const turn = loadTurn(database, turnContext.turn_id);
      assertTurnContextFence(turn, turnContext);
      if (turn.state !== 'waiting_user') {
        return { resumed: turn.state === 'running', turn_state: turn.state };
      }
      const hasBlockingInteraction = database.prepare(`
        SELECT 1
        FROM runtime_interactions
        WHERE turn_id = ?
          AND state IN ('pending', 'answer_committed', 'answer_delivering', 'delivery_unknown')
        LIMIT 1
      `).get(turn.turn_id) !== undefined;
      if (hasBlockingInteraction) {
        return { resumed: false, turn_state: 'waiting_user' };
      }
      transitionInTransaction(database, {
        turnId: turn.turn_id,
        fromState: 'waiting_user',
        toState: 'running',
        fence: turnContext.attempt,
        provider,
        serviceInstanceId,
        occurredAt: now(),
        generateId,
        reasonCode: 'permission_resolved',
      });
      return { resumed: true, turn_state: 'running' };
    });
    return resume.immediate();
  }

  return Object.freeze({
    acknowledgeInteractionHandoff,
    acknowledgeInteractionHandoffFromQuery,
    appendAdapterEvent,
    assertInteractionHandoffRecoveryNoticeDelivered,
    assertCurrentFence,
    assertRecoverableFence,
    assertWorkspaceWritable,
    beginSteer,
    beginInteractionHandoff,
    beginReplyMappingRecovery,
    bindProviderNativeId,
    claimNextReplyMappingRecoveryNotice,
    claimNextQueuedTurn,
    claimOrphanedWorkspaceRecoveries,
    claimInteractionHandoff,
    clearUnstartedQueue,
    cancelUpgradeImportedTurns,
    requeueRolledBackUpgradeImportedTurn,
    completeSteer,
    commitInteractionAnswer,
    completeReplyMappingRecovery,
    markProviderFailure,
    markProviderStopUnknown,
    heartbeatOwnedResidents,
    heartbeatOwnedWorkspaceLeases: workspaceLeases.heartbeatOwned,
    renewOwnedTurnLeases,
    isConversationEvictable,
    isRecoveryNotificationDelivered,
    isWorkspaceRecoveryRequired,
    markInteractionHandoffDeliveryUnknown,
    markInteractionHandoffPreSendFailure,
    markInteractionHandoffSendStarted,
    resolveInteractionTarget,
    releaseExecutorResident,
    releaseWorkspaceReservation: workspaceLeases.release,
    releaseRecoveringExecutorOwnership,
    reconcileExpiredExecutionRecoveries,
    reconcileExpiredResidents,
    recordProviderEventDiagnostic,
    recordProviderEventActivity,
    recordProviderRuntimeEvidence,
    recordSteerReconciliationOutcome,
    recordStopProviderOutcome,
    expireInteraction,
    finishWorkspaceBackgroundWork,
    listPendingInteractionDeadlines,
    listActiveWorkspaceLeases: workspaceLeases.listActive,
    listWorkspaceLeaseObservability: workspaceLeases.listObservability,
    listWorkspaceReservationCandidates: listClaimableQueuedTurns,
    exhaustProviderRetries,
    evictIdleExecutor,
    decideRecovery,
    failSteer,
    getInteractionHandoffForRecovery,
    recordInteractionHandoffQueryUnproven,
    reconcileExpiredStartedInteractionHandoffs,
    resolveInteractionHandoffDisposition,
    rebuildExecutorCache,
    reconcileNonterminalTurns,
    releaseTimedOutExecutorLease,
    requestInteraction,
    runRetentionCleanup: retentionCleanup.run,
    resumeTurnAfterPermission,
    reserveNextExecutor,
    startWorkspaceBackgroundWork,
    scheduleProviderRetry,
    stopConversation,
    transitionTurn,
  });
}
