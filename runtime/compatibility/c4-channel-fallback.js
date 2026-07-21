import {
  createIdempotencyKey,
  validateDeliveryCommand,
  validateDeliveryResult,
  validateInboundEnvelope,
} from '../../contracts/public/index.js';
import { acceptNormalInbound } from '../persistence/inbound-acceptance.js';

export function createCompatibilityEnvelope(message, {
  receivedAt = message?.received_at,
  traceId = message?.trace_id,
} = {}) {
  const envelope = {
    contract: 'zylos.inbound-envelope',
    contract_version: '1.0',
    inbound_event_id: message?.inbound_event_id,
    idempotency_key: createIdempotencyKey('inbound', {
      region: message?.region,
      tenant_id: message?.tenant_id,
      channel: message?.channel,
      bot_id: message?.bot_id,
      inbound_event_id: message?.inbound_event_id,
    }),
    trace_id: traceId,
    occurred_at: message?.occurred_at,
    received_at: receivedAt,
    region: message?.region,
    tenant_id: message?.tenant_id,
    channel: message?.channel,
    bot_id: message?.bot_id,
    chat_type: message?.chat_type,
    chat_id: message?.chat_id,
    native_thread_or_topic_id: message?.native_thread_or_topic_id ?? null,
    message_id: message?.message_id,
    actor: structuredClone(message?.actor),
    content: structuredClone(message?.content),
    reply: structuredClone(message?.reply ?? {
      root_message_id: null,
      parent_message_id: null,
      reply_to_message_id: null,
    }),
    source: {
      kind: 'platform_original',
      source_ref: message?.source_ref ?? null,
    },
  };
  return validateInboundEnvelope(envelope).forwarded;
}

export function acceptCompatibilityInbound(database, message, options = {}) {
  const envelope = createCompatibilityEnvelope(message, {
    receivedAt: message?.received_at ?? options.now?.(),
    traceId: message?.trace_id ?? options.generateId?.('trace'),
  });
  return acceptNormalInbound(database, envelope, {
    ...options,
    initialDeliveryOperation: 'send_text',
  });
}

const PHASE_LABELS = Object.freeze({
  received: 'Received',
  completed: 'Completed',
  failed: 'Failed',
  waiting_user: 'Action required',
});

function renderText(command) {
  const { render_model: renderModel } = command;
  const actionRequired = renderModel.user_action_required
    || renderModel.phase === 'waiting_user';
  const label = actionRequired
    ? PHASE_LABELS.waiting_user
    : (PHASE_LABELS[renderModel.phase] ?? 'Update');
  const latestInteraction = renderModel.interactions.at(-1);
  const interactionPrompt = typeof latestInteraction?.prompt === 'string'
    && latestInteraction.prompt.length > 0
    ? latestInteraction.prompt
    : null;
  const actionText = interactionPrompt
    ?? (renderModel.interactions.length > 0 ? renderModel.text : null)
    ?? 'Your input is required.';
  const body = actionRequired
    ? actionText
    : (renderModel.error?.user_message ?? renderModel.text ?? 'Status updated.');
  const replyInstruction = actionRequired
    ? '\nReply to this message so Zylos can route your response.'
    : '';
  return `${label}\n${body}${replyInstruction}`;
}

export function createChannelNeutralTextRenderer({
  sendText,
  beforeSend,
  now = () => new Date().toISOString(),
}) {
  if (typeof sendText !== 'function') {
    throw new TypeError('sendText must be a function');
  }
  if (typeof now !== 'function') {
    throw new TypeError('now must be a function');
  }
  if (typeof beforeSend !== 'function') {
    throw new TypeError('beforeSend must be a function');
  }

  async function deliver(command) {
    const validated = validateDeliveryCommand(command).forwarded;
    if (validated.operation === 'update_main') {
      throw new TypeError('A channel-neutral text renderer cannot update a platform message');
    }
    beforeSend(validated);
    const sent = await sendText(Object.freeze({
      target: structuredClone(validated.target),
      text: renderText(validated),
      delivery_id: validated.delivery_id,
      idempotency_key: validated.idempotency_key,
      reply_to_platform_message_id: null,
    }));
    if (
      !sent
      || typeof sent.platform_message_id !== 'string'
      || sent.platform_message_id.length === 0
    ) {
      throw new TypeError('sendText must return a non-empty platform_message_id');
    }
    const deliveredAt = now();
    const result = {
      contract: 'zylos.delivery-result',
      contract_version: '1.0',
      trace_id: validated.trace_id,
      outbox_id: validated.outbox_id,
      delivery_id: validated.delivery_id,
      idempotency_key: validated.idempotency_key,
      delivery_attempt_id: validated.delivery_attempt_id,
      delivery_attempt_no: validated.delivery_attempt_no,
      outbox_lease_epoch: validated.outbox_lease_epoch,
      mapping_id: validated.mapping.mapping_id,
      operation: validated.operation,
      aggregate_version: validated.aggregate_version,
      status: 'delivered',
      platform_message_id: sent.platform_message_id,
      applied_platform_version: null,
      delivered_at: deliveredAt,
      error: null,
      renderer_capabilities: {
        supports_update: false,
        supports_actions: false,
        supports_platform_idempotency: false,
        supports_platform_version: false,
      },
      result_at: deliveredAt,
    };
    validateDeliveryResult(result, { command: validated, occurredAt: deliveredAt });
    return result;
  }

  return Object.freeze({ deliver });
}
