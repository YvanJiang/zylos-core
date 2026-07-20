import crypto from 'node:crypto';

import {
  canonicalizeJson,
  ContractKernelError,
  createContractError,
  createIdempotencyKey,
  createPayloadHash,
  DELIVERY_COMMAND_CURRENT_VERSION,
  resolveIdempotencyReplay,
  validateDeliveryCommand,
  validateDeliveryMapping,
  validateInboundEnvelope,
  validateInboundResult,
  validateNormalizedEvent,
} from '../../contracts/public/index.js';
import {
  initializeMainProjection,
  stageMainProjection,
} from './main-projection.js';
import { initializeRuntimePersistence } from './schema.js';
import {
  acceptPermissionCommandInTransaction,
  bindPermissionToAcceptedTurnInTransaction,
  DEFAULT_PERMISSION_CONFIRMATION_TIMEOUT_MS,
  DEFAULT_PERMISSION_MAX_TIMED_DURATION_MS,
  parsePermissionCommand,
} from '../permissions/permission-service.js';

export const INBOUND_ENVELOPE_KNOWN_FIELDS = Object.freeze([
  'contract',
  'contract_version',
  'inbound_event_id',
  'idempotency_key',
  'trace_id',
  'occurred_at',
  'received_at',
  'region',
  'tenant_id',
  'channel',
  'bot_id',
  'chat_type',
  'chat_id',
  'native_thread_or_topic_id',
  'message_id',
  'actor',
  'content',
  'reply',
  'source',
]);

export const DEFAULT_MAX_QUEUED_TURNS = 5;

function defaultGenerateId(kind) {
  return `${kind}-${crypto.randomUUID()}`;
}

export function encodeConversationKey(envelope) {
  return canonicalizeJson([
    envelope.region,
    envelope.tenant_id,
    envelope.bot_id,
    envelope.chat_type,
    envelope.chat_id,
    envelope.native_thread_or_topic_id,
  ]);
}

export { initializeRuntimePersistence };

export function buildLifecycleEvent({
  eventId,
  traceId,
  conversationId,
  turnId,
  lineageId,
  eventSequence,
  turnVersion,
  phase,
  occurredAt,
  persistedAt,
  fromState,
  reasonCode,
  causationEventId,
  error = null,
}) {
  return {
    contract: 'zylos.normalized-event',
    contract_version: '1.0',
    event_id: eventId,
    trace_id: traceId,
    conversation_id: conversationId,
    turn_id: turnId,
    lineage_id: lineageId,
    event_sequence: eventSequence,
    turn_version: turnVersion,
    attempt_id: null,
    attempt_no: null,
    lease_epoch: null,
    kind: 'turn_state_changed',
    phase,
    occurred_at: occurredAt,
    persisted_at: persistedAt,
    provider: null,
    provider_native_id: null,
    payload: {
      from_state: fromState,
      to_state: phase,
      reason_code: reasonCode,
    },
    causation_event_id: causationEventId,
    error,
  };
}

export function buildInitialDeliveryCommand({
  envelope,
  traceId,
  conversationId,
  turnId,
  lineageId,
  committedAt,
  generateId,
  aggregateVersion = 1,
  eventSequenceThrough = 1,
  phase = 'received',
  text = 'Message received.',
  error = null,
  terminal = false,
  operation = 'create_main',
  mappingId = null,
  mappingReason = null,
}) {
  const outboxId = generateId('outbox');
  const deliveryId = generateId('delivery');
  const target = {
    region: envelope.region,
    tenant_id: envelope.tenant_id,
    channel: envelope.channel,
    bot_id: envelope.bot_id,
    chat_type: envelope.chat_type,
    chat_id: envelope.chat_id,
    native_thread_or_topic_id: envelope.native_thread_or_topic_id,
    native_thread_root_message_id: envelope.chat_type === 'thread'
      ? envelope.reply.root_message_id
      : null,
    native_thread_reply_target_message_id: envelope.chat_type === 'thread'
      ? envelope.message_id
      : null,
  };
  return {
    contract: 'zylos.delivery-command',
    contract_version: DELIVERY_COMMAND_CURRENT_VERSION,
    outbox_id: outboxId,
    delivery_id: deliveryId,
    trace_id: traceId,
    delivery_attempt_id: generateId('delivery-attempt'),
    delivery_attempt_no: 1,
    outbox_lease_epoch: 1,
    target,
    aggregate_type: 'turn_main',
    aggregate_id: turnId,
    operation,
    aggregate_version: aggregateVersion,
    event_sequence_through: eventSequenceThrough,
    idempotency_key: createIdempotencyKey('delivery', {
      channel: envelope.channel,
      target,
      delivery_id: deliveryId,
    }),
    render_model: {
      title: 'Zylos',
      phase,
      text,
      error,
      tools: [],
      interactions: [],
      terminal,
      user_action_required: false,
    },
    mapping: {
      mapping_id: mappingId ?? generateId('mapping'),
      conversation_id: conversationId,
      turn_id: turnId,
      lineage_id: lineageId,
      binding_state: lineageId === null ? 'pending' : 'bound',
      mapping_version: 1,
      ...(lineageId === null ? { reason: mappingReason } : {}),
    },
    target_platform_message_id: null,
    predecessor_delivery_id: null,
    expected_platform_version: null,
    priority: 10,
    not_before: committedAt,
    created_at: committedAt,
  };
}

function resolveNormalLineage(database, envelope, conversationId, committedAt, generateId) {
  const replyToMessageId = envelope.reply.reply_to_message_id;
  if (replyToMessageId !== null) {
    const boundRecoveries = database.prepare(`
      SELECT DISTINCT recovery.bound_lineage_id AS lineage_id,
        lineage.provider_native_state
      FROM runtime_reply_mapping_recoveries AS recovery
      JOIN runtime_turns AS turn ON turn.turn_id = recovery.turn_id
      JOIN runtime_lineages AS lineage
        ON lineage.lineage_id = recovery.bound_lineage_id
       AND lineage.conversation_id = turn.conversation_id
      WHERE turn.conversation_id = ?
        AND recovery.source_platform_message_id = ?
        AND recovery.state = 'bound'
      LIMIT 2
    `).all(conversationId, replyToMessageId);
    const invalidRecoveredLineageId = boundRecoveries.length === 1
      && boundRecoveries[0].provider_native_state === 'invalid'
      ? boundRecoveries[0].lineage_id
      : null;
    if (boundRecoveries.length === 1 && invalidRecoveredLineageId === null) {
      return {
        lineage_id: boundRecoveries[0].lineage_id,
        recovery: null,
        pending_turn: null,
      };
    }

    const pending = database.prepare(`
      SELECT recovery.turn_id, turn.turn_version
      FROM runtime_reply_mapping_recoveries AS recovery
      JOIN runtime_turns AS turn ON turn.turn_id = recovery.turn_id
      WHERE turn.conversation_id = ?
        AND recovery.state NOT IN ('bound', 'rejected', 'failed')
        AND (
          recovery.source_platform_message_id = ?
          OR EXISTS (
            SELECT 1
            FROM runtime_message_mappings AS mapping
            WHERE mapping.turn_id = recovery.turn_id
              AND mapping.region = ? AND mapping.tenant_id = ?
              AND mapping.channel = ? AND mapping.bot_id = ?
              AND mapping.platform_message_id = ?
          )
        )
      ORDER BY recovery.created_at ASC
      LIMIT 1
    `).get(
      conversationId,
      replyToMessageId,
      envelope.region,
      envelope.tenant_id,
      envelope.channel,
      envelope.bot_id,
      replyToMessageId,
    );
    if (pending) {
      return {
        lineage_id: null,
        recovery: null,
        pending_turn: pending,
      };
    }

    const mapped = database.prepare(`
      SELECT mapping.*,
        lineage.conversation_id AS lineage_conversation_id,
        lineage.provider_native_state,
        turn.conversation_id AS turn_conversation_id,
        turn.lineage_id AS turn_lineage_id
      FROM runtime_message_mappings AS mapping
      LEFT JOIN runtime_lineages AS lineage
        ON lineage.lineage_id = mapping.lineage_id
      LEFT JOIN runtime_turns AS turn
        ON turn.turn_id = mapping.turn_id
      WHERE mapping.region = ?
        AND mapping.tenant_id = ?
        AND mapping.channel = ?
        AND mapping.bot_id = ?
        AND mapping.platform_message_id = ?
    `).get(
      envelope.region,
      envelope.tenant_id,
      envelope.channel,
      envelope.bot_id,
      replyToMessageId,
    );
    let reason = null;
    if (!mapped) {
      reason = invalidRecoveredLineageId === null
        ? 'mapping_missing'
        : 'provider_lineage_invalid';
    } else {
      const publicMapping = {
        mapping_id: mapped.mapping_id,
        conversation_id: mapped.conversation_id,
        turn_id: mapped.turn_id,
        lineage_id: mapped.lineage_id,
        binding_state: mapped.binding_state,
        mapping_version: mapped.mapping_version,
        reason: mapped.reason,
      };
      try {
        validateDeliveryMapping(publicMapping, { occurredAt: envelope.received_at });
      } catch {
        reason = 'mapping_corrupt';
      }
      if (
        reason === null
        && (
          mapped.conversation_id !== conversationId
          || mapped.turn_id === null
          || mapped.turn_conversation_id !== conversationId
          || (
            mapped.lineage_id !== null
            && mapped.lineage_conversation_id !== conversationId
          )
          || (
            mapped.binding_state === 'bound'
            && mapped.turn_lineage_id !== mapped.lineage_id
          )
        )
      ) {
        reason = 'mapping_corrupt';
      }
      if (reason === null && mapped.binding_state !== 'bound') {
        reason = 'mapping_unbound';
      }
      if (reason === null && mapped.provider_native_state === 'invalid') {
        reason = 'provider_lineage_invalid';
      }
      if (reason === null) return { lineage_id: mapped.lineage_id, recovery: null };
    }

    const conversationLineages = database.prepare(`
      SELECT lineage_id
      FROM runtime_lineages
      WHERE conversation_id = ?
      ORDER BY created_at ASC, lineage_id ASC
      LIMIT 2
    `).all(conversationId);
    const exactInvalidCandidate = reason === 'provider_lineage_invalid'
      ? (invalidRecoveredLineageId ?? (
        mapped?.lineage_id !== null
        && mapped?.lineage_id !== undefined
        && mapped.lineage_conversation_id === conversationId
          ? mapped.lineage_id
          : null
      ))
      : null;
    const candidateLineageId = exactInvalidCandidate
      ?? (conversationLineages.length === 1 ? conversationLineages[0].lineage_id : null);
    const unknownAssociatedWork = database.prepare(`
      SELECT 1
      FROM runtime_normalized_events AS event
      JOIN runtime_turns AS turn ON turn.turn_id = event.turn_id
      WHERE turn.conversation_id = ?
        AND (
          (? IS NOT NULL AND turn.turn_id = ?)
          OR (? IS NOT NULL AND turn.lineage_id = ?)
        )
        AND (
          json_extract(event.event_json, '$.payload.side_effect_status') = 'unknown'
          OR json_extract(event.event_json, '$.error.side_effect_status') = 'unknown'
        )
      LIMIT 1
    `).get(
      conversationId,
      mapped?.turn_id ?? null,
      mapped?.turn_id ?? null,
      candidateLineageId,
      candidateLineageId,
    );
    return {
      lineage_id: null,
      pending_turn: null,
      recovery: {
        reason,
        candidate_lineage_id: candidateLineageId,
        side_effect_status: unknownAssociatedWork ? 'unknown' : 'none',
      },
    };
  }

  let lineage = database.prepare(`
    SELECT lineage_id
    FROM runtime_lineages
    WHERE conversation_id = ? AND is_default = 1
  `).get(conversationId);
  if (!lineage) {
    lineage = { lineage_id: generateId('lineage') };
    database.prepare(`
      INSERT INTO runtime_lineages (
        lineage_id, conversation_id, lineage_kind, is_default, created_at
      ) VALUES (?, ?, ?, 1, ?)
    `).run(
      lineage.lineage_id,
      conversationId,
      envelope.source.kind === 'scheduler' && envelope.chat_type === 'synthetic'
        ? 'scheduler'
        : 'normal',
      committedAt,
    );
  }
  return { ...lineage, recovery: null, pending_turn: null };
}

export function acceptNormalInbound(
  database,
  envelope,
  {
    now = () => new Date().toISOString(),
    generateId = defaultGenerateId,
    maxQueuedTurns = DEFAULT_MAX_QUEUED_TURNS,
    initialDeliveryOperation = 'create_main',
    permissionMaxTimedDurationMs = DEFAULT_PERMISSION_MAX_TIMED_DURATION_MS,
    permissionConfirmationTimeoutMs = DEFAULT_PERMISSION_CONFIRMATION_TIMEOUT_MS,
  } = {},
) {
  if (!Number.isSafeInteger(maxQueuedTurns) || maxQueuedTurns <= 0) {
    throw new TypeError('maxQueuedTurns must be a positive safe integer');
  }
  if (!['create_main', 'send_text'].includes(initialDeliveryOperation)) {
    throw new TypeError('initialDeliveryOperation must be create_main or send_text');
  }
  if (!Number.isSafeInteger(permissionMaxTimedDurationMs) || permissionMaxTimedDurationMs <= 0) {
    throw new TypeError('permissionMaxTimedDurationMs must be a positive safe integer');
  }
  if (
    !Number.isSafeInteger(permissionConfirmationTimeoutMs)
    || permissionConfirmationTimeoutMs <= 0
  ) {
    throw new TypeError('permissionConfirmationTimeoutMs must be a positive safe integer');
  }
  const validated = validateInboundEnvelope(envelope);
  if (!validated.forwarded.actor.authenticated) {
    throw new ContractKernelError(createContractError({
      code: 'unauthenticated',
      category: 'authentication',
      userMessage: 'Inbound delivery facts must come from authenticated channel ingress.',
      occurredAt: envelope.received_at,
    }));
  }
  const payloadHash = createPayloadHash(envelope, {
    scope: 'inbound',
    knownFields: INBOUND_ENVELOPE_KNOWN_FIELDS,
    extensionFields: [
      ...Object.keys(validated.extensions),
      ...(Object.hasOwn(validated.forwarded, 'schedule') ? ['schedule'] : []),
      ...(Object.hasOwn(validated.forwarded, 'legacy') ? ['legacy'] : []),
    ],
  });
  initializeRuntimePersistence(database);
  const permissionCommand = parsePermissionCommand(validated.forwarded, {
    maxTimedDurationMs: permissionMaxTimedDurationMs,
  });

  const commit = database.transaction(() => {
    const existingIdempotency = database.prepare(`
      SELECT idempotency_key, payload_hash, first_result_json
      FROM runtime_inbound_idempotency
      WHERE idempotency_key = ?
    `).get(envelope.idempotency_key);
    const replay = resolveIdempotencyReplay(existingIdempotency, {
      idempotency_key: envelope.idempotency_key,
      payload_hash: payloadHash,
    }, { occurredAt: envelope.received_at });
    if (replay.status === 'duplicate') {
      const originalResult = JSON.parse(existingIdempotency.first_result_json);
      const replayedResult = {
        ...originalResult,
        trace_id: envelope.trace_id,
        deduplicated: true,
      };
      validateInboundResult(replayedResult);
      return replayedResult;
    }
    if (replay.status === 'conflict') {
      const rejectedResult = {
        contract: 'zylos.inbound-result',
        contract_version: '1.0',
        trace_id: envelope.trace_id,
        inbound_event_id: envelope.inbound_event_id,
        idempotency_key: envelope.idempotency_key,
        status: 'rejected',
        conversation_id: null,
        turn_id: null,
        lineage_id: null,
        control_id: null,
        turn_version: null,
        lineage_resolution_state: 'not_applicable',
        deduplicated: false,
        error: replay.error,
        committed_at: null,
      };
      validateInboundResult(rejectedResult);
      return rejectedResult;
    }

    const committedAt = now();
    const conversationKey = encodeConversationKey(validated.forwarded);
    let conversation = database.prepare(`
      SELECT conversation_id
      FROM runtime_conversations
      WHERE conversation_key = ?
    `).get(conversationKey);

    if (!conversation) {
      conversation = { conversation_id: generateId('conversation') };
      database.prepare(`
        INSERT INTO runtime_conversations (
          conversation_id, conversation_key, region, tenant_id, bot_id,
          chat_type, chat_id, native_thread_or_topic_id, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
      `).run(
        conversation.conversation_id,
        conversationKey,
        envelope.region,
        envelope.tenant_id,
        envelope.bot_id,
        envelope.chat_type,
        envelope.chat_id,
        envelope.native_thread_or_topic_id,
        committedAt,
      );
    }

    if (permissionCommand !== null) {
      const permissionResult = acceptPermissionCommandInTransaction(database, {
        envelope: validated.forwarded,
        command: permissionCommand,
        conversationId: conversation.conversation_id,
        payloadHash,
        committedAt,
        generateId,
        confirmationTimeoutMs: permissionConfirmationTimeoutMs,
      });
      validateInboundResult(permissionResult);
      database.prepare(`
        INSERT INTO runtime_inbound_idempotency (
          idempotency_key, inbound_event_id, payload_hash, first_result_json, committed_at
        ) VALUES (?, ?, ?, ?, ?)
      `).run(
        envelope.idempotency_key,
        envelope.inbound_event_id,
        payloadHash,
        JSON.stringify(permissionResult),
        committedAt,
      );
      return permissionResult;
    }

    const lineage = resolveNormalLineage(
      database,
      envelope,
      conversation.conversation_id,
      committedAt,
      generateId,
    );

    if (lineage.pending_turn !== null && lineage.pending_turn !== undefined) {
      database.prepare(`
        INSERT INTO runtime_inbound_events (
          inbound_event_id, idempotency_key, conversation_id, message_id,
          payload_hash, envelope_json, received_at, committed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      `).run(
        envelope.inbound_event_id,
        envelope.idempotency_key,
        conversation.conversation_id,
        envelope.message_id,
        payloadHash,
        JSON.stringify(validated.forwarded),
        envelope.received_at,
        committedAt,
      );
      const result = {
        contract: 'zylos.inbound-result',
        contract_version: '1.0',
        trace_id: envelope.trace_id,
        inbound_event_id: envelope.inbound_event_id,
        idempotency_key: envelope.idempotency_key,
        status: 'rejected',
        conversation_id: conversation.conversation_id,
        turn_id: lineage.pending_turn.turn_id,
        lineage_id: null,
        control_id: null,
        turn_version: lineage.pending_turn.turn_version,
        lineage_resolution_state: 'pending_recovery',
        deduplicated: false,
        error: createContractError({
          code: 'lineage_resolution_pending',
          category: 'conflict',
          userMessage: 'The replied-to message is waiting for safe lineage recovery.',
          occurredAt: committedAt,
        }),
        committed_at: committedAt,
      };
      validateInboundResult(result);
      database.prepare(`
        INSERT INTO runtime_inbound_idempotency (
          idempotency_key, inbound_event_id, payload_hash, first_result_json, committed_at
        ) VALUES (?, ?, ?, ?, ?)
      `).run(
        envelope.idempotency_key,
        envelope.inbound_event_id,
        payloadHash,
        JSON.stringify(result),
        committedAt,
      );
      return result;
    }

    const queuedTurnCount = database.prepare(`
      SELECT COUNT(*) AS count
      FROM runtime_turn_queue
      WHERE conversation_id = ? AND status = 'queued' AND priority = 0
    `).get(conversation.conversation_id).count;
    const pendingRecovery = lineage.recovery !== null;
    const queueFull = !pendingRecovery && queuedTurnCount >= maxQueuedTurns;
    const queueFullError = queueFull
      ? createContractError({
        code: 'queue_full',
        category: 'capacity',
        retryable: true,
        userMessage: 'The conversation queue is full.',
        occurredAt: committedAt,
      })
      : null;

    const turnId = generateId('turn');
    database.prepare(`
      UPDATE runtime_conversations
      SET last_queue_sequence = last_queue_sequence + 1
      WHERE conversation_id = ?
    `).run(conversation.conversation_id);
    const queueSequence = database.prepare(`
      SELECT last_queue_sequence
      FROM runtime_conversations
      WHERE conversation_id = ?
    `).get(conversation.conversation_id).last_queue_sequence;

    database.prepare(`
      INSERT INTO runtime_inbound_events (
        inbound_event_id, idempotency_key, conversation_id, message_id,
        payload_hash, envelope_json, received_at, committed_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      envelope.inbound_event_id,
      envelope.idempotency_key,
      conversation.conversation_id,
      envelope.message_id,
      payloadHash,
      JSON.stringify(validated.forwarded),
      envelope.received_at,
      committedAt,
    );

    database.prepare(`
      INSERT INTO runtime_turns (
        turn_id, conversation_id, lineage_id, inbound_event_id, state,
        turn_version, queue_sequence, created_at, committed_at
      ) VALUES (?, ?, ?, ?, ?, 2, ?, ?, ?)
    `).run(
      turnId,
      conversation.conversation_id,
      lineage.lineage_id,
      envelope.inbound_event_id,
      queueFull ? 'failed' : 'queued',
      queueSequence,
      committedAt,
      committedAt,
    );
    if (!queueFull) {
      database.prepare(`
        INSERT INTO runtime_turn_queue (
          conversation_id, queue_sequence, turn_id, status, wait_reason, enqueued_at
        ) VALUES (?, ?, ?, 'queued', ?, ?)
      `).run(
        conversation.conversation_id,
        queueSequence,
        turnId,
        pendingRecovery ? 'lineage_resolution_pending' : null,
        committedAt,
      );
      bindPermissionToAcceptedTurnInTransaction(database, {
        turnId,
        actorId: envelope.actor.actor_id,
        conversationId: conversation.conversation_id,
        acceptedAt: committedAt,
        generateId,
      });
    }

    const receivedEvent = buildLifecycleEvent({
      eventId: generateId('event'),
      traceId: envelope.trace_id,
      conversationId: conversation.conversation_id,
      turnId,
      lineageId: lineage.lineage_id,
      eventSequence: 1,
      turnVersion: 1,
      phase: 'received',
      occurredAt: envelope.received_at,
      persistedAt: committedAt,
      fromState: null,
      reasonCode: 'inbound_committed',
      causationEventId: null,
    });
    const admissionEvent = buildLifecycleEvent({
      eventId: generateId('event'),
      traceId: envelope.trace_id,
      conversationId: conversation.conversation_id,
      turnId,
      lineageId: lineage.lineage_id,
      eventSequence: 2,
      turnVersion: 2,
      phase: queueFull ? 'failed' : 'queued',
      occurredAt: committedAt,
      persistedAt: committedAt,
      fromState: 'received',
      reasonCode: queueFull ? 'queue_full' : 'queue_sequence_allocated',
      causationEventId: receivedEvent.event_id,
      error: queueFullError,
    });
    for (const event of [receivedEvent, admissionEvent]) {
      validateNormalizedEvent(event);
      database.prepare(`
        INSERT INTO runtime_normalized_events (
          event_id, turn_id, event_sequence, turn_version, event_json, persisted_at
        ) VALUES (?, ?, ?, ?, ?, ?)
      `).run(
        event.event_id,
        turnId,
        event.event_sequence,
        event.turn_version,
        JSON.stringify(event),
        committedAt,
      );
    }

    const recoveryMappingId = pendingRecovery ? generateId('mapping') : null;
    const deliveryCommand = buildInitialDeliveryCommand({
      envelope,
      traceId: envelope.trace_id,
      conversationId: conversation.conversation_id,
      turnId,
      lineageId: lineage.lineage_id,
      committedAt,
      generateId,
      aggregateVersion: queueFull ? 2 : 1,
      eventSequenceThrough: queueFull ? 2 : 1,
      phase: queueFull ? 'failed' : 'received',
      text: queueFull
        ? queueFullError.user_message
        : (envelope.source.kind === 'scheduler'
          ? (envelope.schedule.notification_text ?? 'Scheduled occurrence queued.')
          : 'Message received.'),
      error: queueFullError,
      terminal: queueFull,
      operation: initialDeliveryOperation,
      mappingId: recoveryMappingId,
      mappingReason: lineage.recovery?.reason ?? null,
    });
    validateDeliveryCommand(deliveryCommand);
    const laneKey = initializeMainProjection(database, deliveryCommand);
    database.prepare(`
      INSERT INTO runtime_outbox (
        outbox_id, delivery_id, aggregate_type, aggregate_id, turn_id, control_id,
        lane_key, predecessor_delivery_id, aggregate_version, status, command_json,
        priority, supersedable, terminal, next_attempt_at, created_at, updated_at
      ) VALUES (?, ?, ?, ?, ?, NULL, ?, NULL, ?, 'pending', ?, ?, 0, ?, ?, ?, ?)
    `).run(
      deliveryCommand.outbox_id,
      deliveryCommand.delivery_id,
      deliveryCommand.aggregate_type,
      deliveryCommand.aggregate_id,
      turnId,
      laneKey,
      deliveryCommand.aggregate_version,
      JSON.stringify(deliveryCommand),
      deliveryCommand.priority,
      deliveryCommand.render_model.terminal ? 1 : 0,
      deliveryCommand.not_before,
      committedAt,
      committedAt,
    );
    if (!queueFull) {
      stageMainProjection(database, { turn_id: turnId }, admissionEvent, { generateId });
    }
    if (pendingRecovery) {
      database.prepare(`
        INSERT INTO runtime_reply_mapping_recoveries (
          recovery_id, turn_id, mapping_id, source_platform_message_id,
          reason, candidate_lineage_id, side_effect_status, state,
          native_recovery_attempt_count, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, 'queued', 0, ?, ?)
      `).run(
        generateId('mapping-recovery'),
        turnId,
        recoveryMappingId,
        envelope.reply.reply_to_message_id,
        lineage.recovery.reason,
        lineage.recovery.candidate_lineage_id,
        lineage.recovery.side_effect_status,
        committedAt,
        committedAt,
      );
    }

    const result = {
      contract: 'zylos.inbound-result',
      contract_version: '1.0',
      trace_id: envelope.trace_id,
      inbound_event_id: envelope.inbound_event_id,
      idempotency_key: envelope.idempotency_key,
      status: queueFull ? 'rejected' : 'accepted',
      conversation_id: conversation.conversation_id,
      turn_id: turnId,
      lineage_id: lineage.lineage_id,
      control_id: null,
      turn_version: 2,
      lineage_resolution_state: pendingRecovery ? 'pending_recovery' : 'bound',
      deduplicated: false,
      error: queueFullError,
      committed_at: committedAt,
    };
    validateInboundResult(result);
    if (validated.forwarded.source.kind === 'scheduler') {
      database.prepare(`
        INSERT INTO runtime_scheduler_occurrences (
          schedule_id, occurrence_id, task_id, bound_conversation, conversation_id,
          turn_id, status, envelope_json, committed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
      `).run(
        validated.forwarded.schedule.schedule_id,
        validated.forwarded.schedule.occurrence_id,
        validated.forwarded.schedule.task_id,
        validated.forwarded.schedule.bound_conversation ? 1 : 0,
        conversation.conversation_id,
        turnId,
        result.status,
        JSON.stringify(validated.forwarded),
        committedAt,
      );
    }
    database.prepare(`
      INSERT INTO runtime_inbound_idempotency (
        idempotency_key, inbound_event_id, payload_hash, first_result_json, committed_at
      ) VALUES (?, ?, ?, ?, ?)
    `).run(
      envelope.idempotency_key,
      envelope.inbound_event_id,
      payloadHash,
      JSON.stringify(result),
      committedAt,
    );
    return result;
  });

  return commit.immediate();
}
