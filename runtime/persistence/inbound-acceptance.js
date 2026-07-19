import crypto from 'node:crypto';

import {
  canonicalizeJson,
  ContractKernelError,
  createContractError,
  createIdempotencyKey,
  createPayloadHash,
  resolveIdempotencyReplay,
  validateDeliveryCommand,
  validateInboundEnvelope,
  validateInboundResult,
  validateNormalizedEvent,
} from '../../contracts/public/index.js';
import {
  initializeMainProjection,
  stageMainProjection,
} from './main-projection.js';
import { initializeRuntimePersistence } from './schema.js';

const INBOUND_ENVELOPE_KNOWN_FIELDS = Object.freeze([
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

function buildLifecycleEvent({
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

function buildInitialDeliveryCommand({
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
  };
  return {
    contract: 'zylos.delivery-command',
    contract_version: '1.0',
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
      mapping_id: generateId('mapping'),
      conversation_id: conversationId,
      turn_id: turnId,
      lineage_id: lineageId,
      binding_state: 'bound',
      mapping_version: 1,
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
    const mapped = database.prepare(`
      SELECT mapping.lineage_id
      FROM runtime_message_mappings AS mapping
      JOIN runtime_lineages AS lineage
        ON lineage.lineage_id = mapping.lineage_id
       AND lineage.conversation_id = mapping.conversation_id
      WHERE mapping.region = ?
        AND mapping.tenant_id = ?
        AND mapping.channel = ?
        AND mapping.bot_id = ?
        AND mapping.platform_message_id = ?
        AND mapping.conversation_id = ?
        AND mapping.binding_state = 'bound'
    `).get(
      envelope.region,
      envelope.tenant_id,
      envelope.channel,
      envelope.bot_id,
      replyToMessageId,
      conversationId,
    );
    if (!mapped) {
      throw new ContractKernelError(createContractError({
        code: 'lineage_resolution_pending',
        category: 'conflict',
        userMessage: 'The replied-to message does not have a bound lineage in this conversation.',
        occurredAt: envelope.received_at,
      }));
    }
    return mapped;
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
      ) VALUES (?, ?, 'normal', 1, ?)
    `).run(lineage.lineage_id, conversationId, committedAt);
  }
  return lineage;
}

export function acceptNormalInbound(
  database,
  envelope,
  {
    now = () => new Date().toISOString(),
    generateId = defaultGenerateId,
    maxQueuedTurns = DEFAULT_MAX_QUEUED_TURNS,
    initialDeliveryOperation = 'create_main',
  } = {},
) {
  if (!Number.isSafeInteger(maxQueuedTurns) || maxQueuedTurns <= 0) {
    throw new TypeError('maxQueuedTurns must be a positive safe integer');
  }
  if (!['create_main', 'send_text'].includes(initialDeliveryOperation)) {
    throw new TypeError('initialDeliveryOperation must be create_main or send_text');
  }
  const validated = validateInboundEnvelope(envelope);
  const payloadHash = createPayloadHash(envelope, {
    scope: 'inbound',
    knownFields: INBOUND_ENVELOPE_KNOWN_FIELDS,
    extensionFields: Object.keys(validated.extensions),
  });
  initializeRuntimePersistence(database);

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

    const lineage = resolveNormalLineage(
      database,
      envelope,
      conversation.conversation_id,
      committedAt,
      generateId,
    );

    const queuedTurnCount = database.prepare(`
      SELECT COUNT(*) AS count
      FROM runtime_turn_queue
      WHERE conversation_id = ? AND status = 'queued'
    `).get(conversation.conversation_id).count;
    const queueFull = queuedTurnCount >= maxQueuedTurns;
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
          conversation_id, queue_sequence, turn_id, status, enqueued_at
        ) VALUES (?, ?, ?, 'queued', ?)
      `).run(conversation.conversation_id, queueSequence, turnId, committedAt);
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
      text: queueFull ? queueFullError.user_message : 'Message received.',
      error: queueFullError,
      terminal: queueFull,
      operation: initialDeliveryOperation,
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
      lineage_resolution_state: 'bound',
      deduplicated: false,
      error: queueFullError,
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
  });

  return commit.immediate();
}
