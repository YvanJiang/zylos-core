import {
  admitNormalizedEvent,
  ContractKernelError,
  createContractError,
  createIdempotencyKey,
  createPayloadHash,
  createNormalizedEventStreamState,
  TERMINAL_TURN_STATES,
  INTERACTION_ANSWER_SCHEMA_V1,
  resolveIdempotencyReplay,
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
} from '../../contracts/public/index.js';
import { createDeliveryLaneKey } from './delivery-lane-key.js';
import { stageMainProjection } from './main-projection.js';
import { initializeRuntimePersistence } from './schema.js';

const CANONICAL_TRANSITIONS = Object.freeze({
  queued: ['starting'],
  starting: ['running', 'recovering', 'failed'],
  running: ['waiting_user', 'recovering', 'completed', 'failed'],
  waiting_user: ['running', 'recovering', 'timed_out'],
  recovering: [],
});

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
      lineage.provider AS lineage_provider,
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
  assertActiveFence(database, turn, fence, serviceInstanceId);
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
      error,
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
  if (TERMINAL_TURN_STATES.includes(toState) && toState !== 'timed_out') {
    const completedQueueEntry = database.prepare(`
      UPDATE runtime_turn_queue
      SET status = ?
      WHERE turn_id = ? AND status = 'claimed'
    `).run(toState, turnId);
    if (completedQueueEntry.changes !== 1) {
      conflict('stale_attempt', 'The canonical terminal transition lost its queue claim.');
    }
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

export function createExecutorStore({
  database,
  provider,
  serviceInstanceId,
  now,
  generateId,
  leaseDurationMs = 10_000,
  interactionTimeoutMs = 10 * 60_000,
}) {
  initializeRuntimePersistence(database);

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
      SELECT conversation_id, turn_id, status, wait_reason, queue_sequence
      FROM runtime_turn_queue
      WHERE status IN ('queued', 'claimed')
      ORDER BY conversation_id ASC, queue_sequence ASC
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
        projection.wait_reason ??= row.wait_reason;
      }
    }
    return [...executors.values()];
  }

  function listPendingInteractionDeadlines() {
    return database.prepare(`
      SELECT candidate.request_json
      FROM runtime_interactions AS candidate
      WHERE candidate.state = 'pending'
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
      WHERE queue.status = 'queued' AND turn.state = 'queued'
        AND NOT EXISTS (
          SELECT 1
          FROM runtime_turn_queue AS earlier
          WHERE earlier.conversation_id = queue.conversation_id
            AND earlier.queue_sequence < queue.queue_sequence
            AND earlier.status IN ('queued', 'claimed')
        )
        AND NOT EXISTS (
          SELECT 1
          FROM runtime_executor_leases AS lease
          WHERE lease.conversation_id = queue.conversation_id
            AND lease.lease_owner IS NOT NULL
        )
      ORDER BY turn.created_at ASC, turn.conversation_id ASC, queue.queue_sequence ASC
    `).all();
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
      SET wait_reason = 'executor_capacity'
      WHERE turn_id = ? AND status = 'queued' AND wait_reason IS NOT 'executor_capacity'
    `).run(turnId);
    if (turnUpdate.changes !== 1 || queueUpdate.changes !== 1) {
      conflict('stale_attempt', 'The capacity-wait projection changed concurrently.');
    }
    persistEvent(database, turn, event, generateId);
    return result;
  }

  function isResidentConversation(conversationId) {
    return Boolean(database.prepare(`
      SELECT 1
      FROM runtime_executor_residents
      WHERE conversation_id = ? AND provider = 'claude'
    `).get(conversationId));
  }

  function residentCountForBot(botId) {
    return database.prepare(`
      SELECT COUNT(*) AS count
      FROM runtime_executor_residents
      WHERE bot_id = ? AND provider = 'claude'
    `).get(botId).count;
  }

  function reserveNextExecutor({ maxResidentExecutorsPerBot }) {
    const reserve = database.transaction(() => {
      const candidates = listClaimableQueuedTurns();
      if (candidates.length === 0) return { status: 'idle' };
      if (provider !== 'claude') {
        return {
          status: 'ready',
          conversation_id: candidates[0].conversation_id,
        };
      }

      let selected = candidates.find(
        (candidate) => isResidentConversation(candidate.conversation_id),
      );
      if (!selected) {
        for (const candidate of candidates) {
          if (residentCountForBot(candidate.bot_id) >= maxResidentExecutorsPerBot) continue;
          const admittedAt = now();
          database.prepare(`
            INSERT INTO runtime_executor_residents (
              conversation_id, bot_id, provider, admitted_at, last_used_at
            ) VALUES (?, ?, 'claude', ?, ?)
          `).run(candidate.conversation_id, candidate.bot_id, admittedAt, admittedAt);
          selected = candidate;
          break;
        }
      } else {
        database.prepare(`
          UPDATE runtime_executor_residents
          SET last_used_at = ?
          WHERE conversation_id = ? AND provider = 'claude'
        `).run(now(), selected.conversation_id);
      }

      const waits = [];
      for (const candidate of candidates) {
        if (candidate.conversation_id === selected?.conversation_id) continue;
        if (isResidentConversation(candidate.conversation_id)) continue;
        if (residentCountForBot(candidate.bot_id) < maxResidentExecutorsPerBot) continue;
        const wait = markCapacityWaitInTransaction(candidate.turn_id, now());
        if (wait) waits.push(wait);
      }

      if (selected) {
        return { status: 'ready', conversation_id: selected.conversation_id };
      }
      return waits.length > 0 ? { status: 'capacity_wait', ...waits[0] } : { status: 'idle' };
    });
    return reserve.immediate();
  }

  function claimNextQueuedTurn({ conversationId = null } = {}) {
    const claim = database.transaction(() => {
      const turn = database.prepare(`
        SELECT turn.turn_id
        FROM runtime_turn_queue AS queue
        JOIN runtime_turns AS turn ON turn.turn_id = queue.turn_id
        WHERE queue.status = 'queued' AND turn.state = 'queued'
          AND (? IS NULL OR queue.conversation_id = ?)
          AND NOT EXISTS (
            SELECT 1
            FROM runtime_turn_queue AS earlier
            WHERE earlier.conversation_id = queue.conversation_id
              AND earlier.queue_sequence < queue.queue_sequence
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
      `).get(conversationId, conversationId);
      if (!turn) return null;

      const claimedAt = now();
      const current = loadTurn(database, turn.turn_id);
      const existingLease = database.prepare(`
        SELECT lease_epoch
        FROM runtime_executor_leases
        WHERE conversation_id = ?
      `).get(current.conversation_id);
      const fence = {
        attempt_id: generateId('attempt'),
        attempt_no: 1,
        lease_epoch: (existingLease?.lease_epoch ?? 0) + 1,
      };
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
        WHERE turn_id = ? AND state = 'queued' AND attempt_id IS NULL
      `).run(
        fence.attempt_id,
        fence.attempt_no,
        fence.lease_epoch,
        current.turn_id,
      );
      const queueUpdate = database.prepare(`
        UPDATE runtime_turn_queue
        SET status = 'claimed', wait_reason = NULL
        WHERE turn_id = ? AND status = 'queued'
      `).run(current.turn_id);
      if (turnUpdate.changes !== 1 || queueUpdate.changes !== 1) {
        conflict('stale_attempt', 'The durable queue entry was claimed concurrently.');
      }
      transitionInTransaction(database, {
        turnId: current.turn_id,
        fromState: 'queued',
        toState: 'starting',
        fence,
        provider,
        serviceInstanceId,
        occurredAt: claimedAt,
        generateId,
      });
      const envelope = JSON.parse(current.envelope_json);
      if (current.lineage_provider !== null && current.lineage_provider !== provider) {
        conflict(
          'provider_context_invalid',
          'The persisted lineage belongs to a different provider adapter.',
        );
      }
      return {
        conversation_id: current.conversation_id,
        turn_id: current.turn_id,
        lineage_id: current.lineage_id,
        trace_id: envelope.trace_id,
        input: envelope.content,
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
        attempt: fence,
      };
    });
    return claim.immediate();
  }

  function transitionTurn(turnContext, fromState, toState, { reasonCode, error } = {}) {
    const transition = database.transaction(() => transitionInTransaction(database, {
      turnId: turnContext.turn_id,
      fromState,
      toState,
      fence: turnContext.attempt,
      provider,
      serviceInstanceId,
      occurredAt: now(),
      generateId,
      reasonCode,
      error,
    }));
    return transition.immediate();
  }

  function appendAdapterEvent(turnContext, descriptor) {
    if (descriptor.kind === 'turn_state_changed') {
      conflict('illegal_transition', 'Provider adapters cannot author canonical state transitions.');
    }
    const append = database.transaction(() => {
      const turn = loadTurn(database, turnContext.turn_id);
      assertActiveFence(database, turn, turnContext.attempt, serviceInstanceId);
      if (turn.state !== 'running') {
        conflict('illegal_transition', `Adapter output is invalid while turn is ${turn.state}.`);
      }
      const descriptorNativeId = descriptor.provider_native_id ?? null;
      if (
        (turn.provider_native_id !== null || descriptorNativeId !== null)
        && (
          turn.lineage_provider !== provider
          || turn.provider_native_id !== descriptorNativeId
        )
      ) {
        conflict(
          'provider_context_invalid',
          'Provider output does not match the atomically bound lineage.',
        );
      }
      const event = buildEvent({
        turn,
        lastEvent: loadLastEvent(database, turn.turn_id),
        fence: turnContext.attempt,
        provider,
        descriptor: {
          ...descriptor,
          phase: 'running',
        },
        occurredAt: now(),
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
      return event;
    });
    return append.immediate();
  }

  function bindProviderNativeId(turnContext, providerNativeId) {
    if (typeof providerNativeId !== 'string' || providerNativeId.trim().length === 0) {
      throw new TypeError('providerNativeId must be a non-empty string');
    }
    const bind = database.transaction(() => {
      const turn = loadTurn(database, turnContext.turn_id);
      assertActiveFence(database, turn, turnContext.attempt, serviceInstanceId);
      if (!['starting', 'running'].includes(turn.state)) {
        conflict('illegal_transition', `Lineage binding is invalid while turn is ${turn.state}.`);
      }
      if (
        turn.lineage_provider === provider
        && turn.provider_native_id === providerNativeId
      ) {
        return Object.freeze({
          provider,
          provider_native_id: providerNativeId,
          newly_bound: false,
        });
      }
      if (turn.lineage_provider !== null || turn.provider_native_id !== null) {
        conflict(
          'provider_context_invalid',
          'The lineage is already bound to a different provider context.',
        );
      }
      const boundAt = now();
      const result = database.prepare(`
        UPDATE runtime_lineages
        SET provider = ?, provider_native_id = ?, provider_native_id_bound_at = ?
        WHERE lineage_id = ? AND conversation_id = ?
          AND provider IS NULL AND provider_native_id IS NULL
          AND provider_native_id_bound_at IS NULL
      `).run(
        provider,
        providerNativeId,
        boundAt,
        turn.lineage_id,
        turn.conversation_id,
      );
      if (result.changes !== 1) {
        conflict('provider_context_invalid', 'The provider lineage binding changed concurrently.');
      }
      return Object.freeze({
        provider,
        provider_native_id: providerNativeId,
        newly_bound: true,
      });
    });
    return bind.immediate();
  }

  function requestInteraction(turnContext, descriptor) {
    const request = database.transaction(() => {
      const requestedAt = now();
      let turn = loadTurn(database, turnContext.turn_id);
      if (!['running', 'waiting_user'].includes(turn.state)) {
        conflict('illegal_transition', `Interaction requests are invalid while turn is ${turn.state}.`);
      }
      assertActiveFence(database, turn, turnContext.attempt, serviceInstanceId);
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
      database.prepare(`
        INSERT INTO runtime_interactions (
          interaction_id, conversation_id, turn_id, lineage_id, ordinal,
          state, version, handoff_state, handoff_version, request_json,
          created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, 'pending', 1, 'not_started', NULL, ?, ?, ?)
      `).run(
        interaction.interaction_id,
        interaction.conversation_id,
        interaction.turn_id,
        interaction.lineage_id,
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
      if (turn.state !== 'waiting_user') {
        conflict('illegal_transition', `Interaction answers are invalid while turn is ${turn.state}.`);
      }
      const fence = {
        attempt_id: turn.attempt_id,
        attempt_no: turn.attempt_no,
        lease_epoch: turn.lease_epoch,
      };
      if (
        request.runtime_fence.provider_attempt_id !== fence.attempt_id
        || request.runtime_fence.lease_epoch !== fence.lease_epoch
      ) {
        conflict('stale_attempt', 'The interaction no longer matches the current runtime fence.');
      }
      assertActiveFence(database, turn, fence, serviceInstanceId);

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
        parent_type: 'provider_turn',
        state: 'pending',
        provider_attempt_id: fence.attempt_id,
        handoff_attempt_id: null,
        handoff_attempt_no: null,
        lease_epoch: fence.lease_epoch,
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
          handoff_id, interaction_id, answer_id, state, provider_attempt_id,
          handoff_attempt_id, handoff_attempt_no, lease_epoch, record_json,
          created_at, updated_at
        ) VALUES (?, ?, ?, 'pending', ?, NULL, NULL, ?, ?, ?, ?)
      `).run(
        handoff.handoff_id,
        handoff.interaction_id,
        handoff.answer_id,
        handoff.provider_attempt_id,
        handoff.lease_epoch,
        JSON.stringify(handoff),
        committedAt,
        committedAt,
      );

      const event = buildEvent({
        turn,
        lastEvent: loadLastEvent(database, turn.turn_id),
        fence,
        provider,
        descriptor: {
          kind: 'interaction_answer_committed',
          phase: 'waiting_user',
          payload: {
            interaction_id: request.interaction_id,
            ordinal: request.ordinal,
            interaction_version: updatedRequest.version,
            handoff_version: 1,
          },
        },
        occurredAt: committedAt,
        generateId,
      });
      commitTurnEvent(database, {
        turn,
        event,
        fence,
        nextState: 'waiting_user',
        staleMessage: 'The interaction answer commit lost its provider attempt fence.',
        generateId,
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
        control_id: null,
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
      if (turn.state !== 'waiting_user') {
        conflict('illegal_transition', `Interaction timeout is invalid while turn is ${turn.state}.`);
      }
      const fence = {
        attempt_id: turn.attempt_id,
        attempt_no: turn.attempt_no,
        lease_epoch: turn.lease_epoch,
      };
      assertActiveFence(database, turn, fence, serviceInstanceId);
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
      const expirationEvent = buildEvent({
        turn,
        lastEvent: loadLastEvent(database, turn.turn_id),
        fence,
        provider,
        descriptor: {
          kind: 'interaction_expired',
          phase: 'waiting_user',
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
        nextState: 'waiting_user',
        staleMessage: 'The interaction deadline lost its provider attempt fence.',
        generateId,
      });
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
      const queueUpdate = database.prepare(`
        UPDATE runtime_turn_queue
        SET status = 'timed_out', wait_reason = NULL
        WHERE turn_id = ? AND status = 'claimed'
      `).run(turn.turn_id);
      if (queueUpdate.changes !== 1) {
        conflict('stale_attempt', 'The interaction deadline lost its durable queue claim.');
      }
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

  function markTimedOutProviderStopUnknown(expiration, providerStopStatus) {
    const markUnknown = database.transaction(() => {
      if (typeof providerStopStatus !== 'string' || providerStopStatus.length === 0) {
        throw new TypeError('providerStopStatus must be a non-empty string');
      }
      const existing = database.prepare(`
        SELECT incident_id, provider_stop_status, side_effect_status, disposition, outbox_id
        FROM runtime_provider_stop_incidents
        WHERE turn_id = ?
      `).get(expiration.turn_id);
      if (existing) return { status: 'manual_recovery_required', ...existing };

      const occurredAt = now();
      const turn = loadTurn(database, expiration.turn_id);
      if (turn.state !== 'timed_out') {
        conflict('illegal_transition', 'Provider stop uncertainty requires a timed-out turn.');
      }
      if (
        expiration.attempt?.attempt_id !== turn.attempt_id
        || expiration.attempt?.attempt_no !== turn.attempt_no
        || expiration.attempt?.lease_epoch !== turn.lease_epoch
      ) {
        conflict('stale_attempt', 'Provider stop uncertainty lost its timeout attempt fence.');
      }
      assertActiveFence(database, turn, expiration.attempt, serviceInstanceId);
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
      const error = createContractError({
        code: 'side_effect_unknown',
        category: 'provider',
        retryable: false,
        sideEffectStatus: 'unknown',
        userMessage: 'Provider stop could not be confirmed; manual recovery is required.',
        occurredAt,
      });
      const target = JSON.parse(lane.target_json);
      const mapping = {
        ...JSON.parse(lane.mapping_json),
        mapping_id: generateId('mapping'),
      };
      const command = {
        contract: 'zylos.delivery-command',
        contract_version: '1.0',
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
          phase: 'timed_out',
          text: 'Execution timed out, but provider stop could not be confirmed. Manual recovery is required.',
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
      return {
        status: 'manual_recovery_required',
        incident_id: incidentId,
        provider_stop_status: providerStopStatus,
        side_effect_status: 'unknown',
        disposition: 'manual_recovery_required',
        outbox_id: outboxId,
      };
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
      return true;
    });
    return release.immediate();
  }

  function claimInteractionHandoff(handoffId) {
    const claim = database.transaction(() => {
      const claimedAt = now();
      const row = database.prepare(`
        SELECT interaction.request_json, answer.answer_json, handoff.record_json
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
      if (request.state !== 'answer_committed' || handoff.state !== 'pending') {
        conflict('illegal_transition', 'Only a committed answer with a pending handoff can be claimed.');
      }
      const turn = loadTurn(database, request.turn_id);
      if (turn.state !== 'waiting_user') {
        conflict('illegal_transition', `Interaction handoff is invalid while turn is ${turn.state}.`);
      }
      const fence = {
        attempt_id: turn.attempt_id,
        attempt_no: turn.attempt_no,
        lease_epoch: turn.lease_epoch,
      };
      if (
        handoff.provider_attempt_id !== fence.attempt_id
        || handoff.lease_epoch !== fence.lease_epoch
      ) {
        conflict('stale_attempt', 'The pending handoff no longer matches the current runtime fence.');
      }
      assertActiveFence(database, turn, fence, serviceInstanceId);
      validateInteractionTransition({
        from: 'answer_committed',
        to: 'answer_delivering',
        occurredAt: claimedAt,
      });
      validateInteractionHandoffTransition({
        from: 'pending',
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
        handoff_attempt_no: 1,
        claimed_by: serviceInstanceId,
        claimed_at: claimedAt,
        last_send_started_at: claimedAt,
      };
      validateInteractionRequest(updatedRequest, { occurredAt: claimedAt });
      validateInteractionHandoff(updatedHandoff, { occurredAt: claimedAt });

      const interactionUpdate = database.prepare(`
        UPDATE runtime_interactions
        SET state = 'answer_delivering', version = ?, handoff_state = 'delivering',
          handoff_version = 2, request_json = ?, updated_at = ?
        WHERE interaction_id = ? AND state = 'answer_committed' AND version = ?
      `).run(
        updatedRequest.version,
        JSON.stringify(updatedRequest),
        claimedAt,
        request.interaction_id,
        request.version,
      );
      const handoffUpdate = database.prepare(`
        UPDATE runtime_interaction_handoffs
        SET state = 'delivering', handoff_attempt_id = ?, handoff_attempt_no = 1,
          record_json = ?, updated_at = ?
        WHERE handoff_id = ? AND state = 'pending' AND handoff_attempt_id IS NULL
      `).run(
        updatedHandoff.handoff_attempt_id,
        JSON.stringify(updatedHandoff),
        claimedAt,
        handoff.handoff_id,
      );
      if (interactionUpdate.changes !== 1 || handoffUpdate.changes !== 1) {
        conflict('version_conflict', 'The interaction handoff claim lost its state/version fence.');
      }

      const event = buildEvent({
        turn,
        lastEvent: loadLastEvent(database, turn.turn_id),
        fence,
        provider,
        descriptor: {
          kind: 'interaction_answer_handoff_started',
          phase: 'waiting_user',
          payload: {
            interaction_id: request.interaction_id,
            ordinal: request.ordinal,
            interaction_version: updatedRequest.version,
            handoff_version: 2,
          },
        },
        occurredAt: claimedAt,
        generateId,
      });
      commitTurnEvent(database, {
        turn,
        event,
        fence,
        nextState: 'waiting_user',
        staleMessage: 'The interaction handoff claim lost its provider attempt fence.',
        generateId,
      });
      return {
        request: updatedRequest,
        answer,
        handoff: updatedHandoff,
      };
    });
    return claim.immediate();
  }

  function acknowledgeInteractionHandoff(acknowledgement) {
    const acknowledge = database.transaction(() => {
      const acknowledgedAt = now();
      if (!['accepted', 'deny'].includes(acknowledgement?.status)) {
        conflict('invalid_acknowledgement', 'The happy-path handler acknowledgement must be accepted or deny.');
      }
      const row = database.prepare(`
        SELECT interaction.request_json, handoff.record_json
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
      if (
        request.state !== 'answer_delivering'
        || handoff.state !== 'delivering'
        || acknowledgement.provider_attempt_id !== handoff.provider_attempt_id
        || acknowledgement.handoff_attempt_id !== handoff.handoff_attempt_id
        || acknowledgement.handoff_attempt_no !== handoff.handoff_attempt_no
        || acknowledgement.lease_epoch !== handoff.lease_epoch
      ) {
        conflict('stale_attempt', 'The handler acknowledgement does not match the delivering handoff fence.');
      }
      assertActiveFence(database, turn, fence, serviceInstanceId);
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
      const interactionUpdate = database.prepare(`
        UPDATE runtime_interactions
        SET state = 'answered', version = ?, handoff_state = 'accepted',
          handoff_version = 3, request_json = ?, updated_at = ?
        WHERE interaction_id = ? AND state = 'answer_delivering' AND version = ?
      `).run(
        updatedRequest.version,
        JSON.stringify(updatedRequest),
        acknowledgedAt,
        request.interaction_id,
        request.version,
      );
      const handoffUpdate = database.prepare(`
        UPDATE runtime_interaction_handoffs
        SET state = 'accepted', record_json = ?, updated_at = ?
        WHERE handoff_id = ? AND state = 'delivering'
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
      if (interactionUpdate.changes !== 1 || handoffUpdate.changes !== 1) {
        conflict('stale_attempt', 'The handler acknowledgement lost its interaction/handoff fence.');
      }
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

      const hasNextBlocking = database.prepare(`
        SELECT 1
        FROM runtime_interactions
        WHERE turn_id = ? AND interaction_id != ?
          AND state IN (${BLOCKING_INTERACTION_STATES_SQL})
        LIMIT 1
      `).get(turn.turn_id, request.interaction_id) !== undefined;
      let currentTurn = turn;
      if (!hasNextBlocking) {
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
      const event = buildEvent({
        turn: currentTurn,
        lastEvent: loadLastEvent(database, turn.turn_id),
        fence,
        provider,
        descriptor: {
          kind: 'interaction_answered',
          phase: hasNextBlocking ? 'waiting_user' : 'running',
          payload: {
            interaction_id: request.interaction_id,
            ordinal: request.ordinal,
            interaction_version: updatedRequest.version,
            handoff_version: 3,
            state: 'answered',
            handoff_state: 'accepted',
          },
        },
        occurredAt: acknowledgedAt,
        generateId,
      });
      commitTurnEvent(database, {
        turn: currentTurn,
        event,
        fence,
        nextState: hasNextBlocking ? 'waiting_user' : 'running',
        staleMessage: 'The handler acknowledgement lost its provider attempt fence.',
        generateId,
      });
      return {
        status: acknowledgement.status,
        interaction_id: request.interaction_id,
        handoff_id: handoff.handoff_id,
        audit_id: auditId,
        resumed: !hasNextBlocking,
        turn_state: hasNextBlocking ? 'waiting_user' : 'running',
        turn_version: event.turn_version,
      };
    });
    return acknowledge.immediate();
  }

  function markProviderFailure(turnContext, error) {
    const markFailure = database.transaction(() => {
      const occurredAt = now();
      let turn = loadTurn(database, turnContext.turn_id);
      assertActiveFence(database, turn, turnContext.attempt, serviceInstanceId);
      if (['starting', 'running'].includes(turn.state)) {
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
        const recoveringTurn = loadTurn(database, turn.turn_id);
        const recoveryEvent = buildEvent({
          turn: recoveringTurn,
          lastEvent: loadLastEvent(database, turn.turn_id),
          fence: turnContext.attempt,
          provider,
          descriptor: {
            kind: 'recovery_started',
            phase: 'recovering',
            provider_native_id: recoveringTurn.provider_native_id,
            payload: {
              recovery_id: generateId('recovery'),
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
        return {
          status: 'recovering',
          turn_id: turn.turn_id,
          turn_state: 'recovering',
          previous_turn_state: fromState,
          cancelled_interaction_ids: [],
        };
      }
      if (turn.state !== 'waiting_user') {
        return { status: 'not_active', turn_id: turn.turn_id, turn_state: turn.state };
      }
      const blockingRequests = database.prepare(`
        SELECT request_json
        FROM runtime_interactions
        WHERE turn_id = ? AND state IN (${BLOCKING_INTERACTION_STATES_SQL})
        ORDER BY ordinal ASC
      `).all(turn.turn_id).map(({ request_json: requestJson }) => JSON.parse(requestJson));
      if (
        blockingRequests.length === 0
        || blockingRequests.some((request) => request.state !== 'pending')
      ) {
        return { status: 'handoff_in_progress', turn_id: turn.turn_id, turn_state: turn.state };
      }
      const cancelledInteractionIds = [];
      for (const request of blockingRequests) {
        validateInteractionTransition({
          from: 'pending',
          to: 'cancelled',
          occurredAt,
        });
        const cancelledRequest = {
          ...request,
          state: 'cancelled',
          version: request.version + 1,
          handoff_state: 'not_started',
          terminal_reason: 'provider_connection_lost',
        };
        validateInteractionRequest(cancelledRequest, { occurredAt });
        const updated = database.prepare(`
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
        );
        if (updated.changes !== 1) {
          conflict('version_conflict', 'The provider failure lost its interaction fence.');
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
              handoff_version: null,
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
      const recoveringTurn = loadTurn(database, turn.turn_id);
      const recoveryEvent = buildEvent({
        turn: recoveringTurn,
        lastEvent: loadLastEvent(database, turn.turn_id),
        fence: turnContext.attempt,
        provider,
        descriptor: {
          kind: 'recovery_started',
          phase: 'recovering',
          provider_native_id: recoveringTurn.provider_native_id,
          payload: {
            recovery_id: generateId('recovery'),
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
      return {
        status: 'recovering',
        turn_id: turn.turn_id,
        turn_state: 'recovering',
        cancelled_interaction_ids: cancelledInteractionIds,
      };
    });
    return markFailure.immediate();
  }

  function markInteractionHandoffDeliveryUnknown(delivery, error) {
    const markUnknown = database.transaction(() => {
      const occurredAt = now();
      const { request, handoff } = delivery;
      const row = database.prepare(`
        SELECT interaction.request_json, durable_handoff.record_json
        FROM runtime_interactions AS interaction
        JOIN runtime_interaction_handoffs AS durable_handoff
          ON durable_handoff.interaction_id = interaction.interaction_id
        WHERE durable_handoff.handoff_id = ?
      `).get(handoff.handoff_id);
      if (!row) conflict('handoff_not_found', `Interaction handoff ${handoff.handoff_id} does not exist.`);
      const persistedRequest = JSON.parse(row.request_json);
      const persistedHandoff = JSON.parse(row.record_json);
      const turn = loadTurn(database, persistedRequest.turn_id);
      const fence = {
        attempt_id: turn.attempt_id,
        attempt_no: turn.attempt_no,
        lease_epoch: turn.lease_epoch,
      };
      if (
        persistedRequest.state !== 'answer_delivering'
        || persistedHandoff.state !== 'delivering'
        || request.interaction_id !== persistedRequest.interaction_id
        || handoff.handoff_attempt_id !== persistedHandoff.handoff_attempt_id
        || handoff.handoff_attempt_no !== persistedHandoff.handoff_attempt_no
        || handoff.provider_attempt_id !== persistedHandoff.provider_attempt_id
        || handoff.lease_epoch !== persistedHandoff.lease_epoch
        || error?.side_effect_status !== 'unknown'
      ) {
        conflict('stale_attempt', 'The uncertain provider delivery does not match the current handoff fence.');
      }
      assertActiveFence(database, turn, fence, serviceInstanceId);
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
        ...persistedRequest,
        state: 'delivery_unknown',
        version: persistedRequest.version + 1,
        handoff_state: 'delivery_unknown',
      };
      const updatedHandoff = {
        ...persistedHandoff,
        state: 'delivery_unknown',
        reason_code: 'provider_ack_uncertain',
        error,
        side_effect_status: 'unknown',
      };
      validateInteractionRequest(updatedRequest, { occurredAt });
      validateInteractionHandoff(updatedHandoff, { occurredAt });
      const interactionUpdate = database.prepare(`
        UPDATE runtime_interactions
        SET state = 'delivery_unknown', version = ?, handoff_state = 'delivery_unknown',
          handoff_version = 3, request_json = ?, updated_at = ?
        WHERE interaction_id = ? AND state = 'answer_delivering' AND version = ?
      `).run(
        updatedRequest.version,
        JSON.stringify(updatedRequest),
        occurredAt,
        persistedRequest.interaction_id,
        persistedRequest.version,
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
        persistedHandoff.handoff_id,
        persistedHandoff.handoff_attempt_id,
        persistedHandoff.handoff_attempt_no,
        persistedHandoff.provider_attempt_id,
        persistedHandoff.lease_epoch,
      );
      if (interactionUpdate.changes !== 1 || handoffUpdate.changes !== 1) {
        conflict('stale_attempt', 'The uncertain provider delivery lost its handoff fence.');
      }
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
      });
      const recoveringTurn = loadTurn(database, turn.turn_id);
      const event = buildEvent({
        turn: recoveringTurn,
        lastEvent: loadLastEvent(database, turn.turn_id),
        fence,
        provider,
        descriptor: {
          kind: 'interaction_answer_delivery_unknown',
          phase: 'recovering',
          payload: {
            interaction_id: updatedRequest.interaction_id,
            ordinal: updatedRequest.ordinal,
            interaction_version: updatedRequest.version,
            handoff_version: 3,
          },
          provider_native_id: recoveringTurn.provider_native_id,
          error,
        },
        occurredAt,
        generateId,
      });
      commitTurnEvent(database, {
        turn: recoveringTurn,
        event,
        fence,
        nextState: 'recovering',
        staleMessage: 'The delivery-unknown event lost its provider attempt fence.',
        generateId,
      });
      return {
        status: 'delivery_unknown',
        interaction_id: updatedRequest.interaction_id,
        handoff_id: updatedHandoff.handoff_id,
        turn_id: turn.turn_id,
        turn_state: 'recovering',
        turn_version: event.turn_version,
      };
    });
    return markUnknown.immediate();
  }

  return Object.freeze({
    acknowledgeInteractionHandoff,
    appendAdapterEvent,
    bindProviderNativeId,
    claimNextQueuedTurn,
    claimInteractionHandoff,
    commitInteractionAnswer,
    markProviderFailure,
    markInteractionHandoffDeliveryUnknown,
    markTimedOutProviderStopUnknown,
    expireInteraction,
    listPendingInteractionDeadlines,
    rebuildExecutorCache,
    releaseTimedOutExecutorLease,
    requestInteraction,
    reserveNextExecutor,
    transitionTurn,
  });
}
