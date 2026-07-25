import { canonicalizeJson } from '../../contracts/public/index.js';

export const DEFAULT_INPUT_COALESCING_POLICY = Object.freeze({
  enabled: true,
  enabledChannels: Object.freeze(['feishu']),
  quietWindowMs: 10_000,
  maxOpenWindowMs: 120_000,
  maxMembers: 20,
});

function positiveSafeInteger(name, value) {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new TypeError(`${name} must be a positive safe integer`);
  }
  return value;
}

export function resolveInputCoalescingPolicy(policy = {}) {
  if (policy === null || typeof policy !== 'object' || Array.isArray(policy)) {
    throw new TypeError('inputCoalescingPolicy must be an object');
  }
  const enabled = policy.enabled ?? DEFAULT_INPUT_COALESCING_POLICY.enabled;
  if (typeof enabled !== 'boolean') {
    throw new TypeError('inputCoalescingPolicy.enabled must be a boolean');
  }
  const enabledChannels = policy.enabledChannels
    ?? DEFAULT_INPUT_COALESCING_POLICY.enabledChannels;
  if (
    !Array.isArray(enabledChannels)
    || enabledChannels.length === 0
    || enabledChannels.some((channel) => typeof channel !== 'string' || channel.length === 0)
    || new Set(enabledChannels).size !== enabledChannels.length
  ) {
    throw new TypeError('inputCoalescingPolicy.enabledChannels must contain unique channels');
  }
  return Object.freeze({
    enabled,
    enabledChannels: Object.freeze([...enabledChannels]),
    quietWindowMs: positiveSafeInteger(
      'inputCoalescingPolicy.quietWindowMs',
      policy.quietWindowMs ?? DEFAULT_INPUT_COALESCING_POLICY.quietWindowMs,
    ),
    maxOpenWindowMs: positiveSafeInteger(
      'inputCoalescingPolicy.maxOpenWindowMs',
      policy.maxOpenWindowMs ?? DEFAULT_INPUT_COALESCING_POLICY.maxOpenWindowMs,
    ),
    maxMembers: positiveSafeInteger(
      'inputCoalescingPolicy.maxMembers',
      policy.maxMembers ?? DEFAULT_INPUT_COALESCING_POLICY.maxMembers,
    ),
  });
}

function isControlLikeText(envelope) {
  const content = envelope.content;
  if (
    content?.kind !== 'text'
    || !Array.isArray(content.attachments)
    || content.attachments.length !== 0
    || typeof content.text !== 'string'
  ) {
    return false;
  }
  return /^\/(?:permission|steer|stop)(?:\s|$)/u.test(content.text.trim());
}

export function shouldCoalesceInbound(envelope, policy) {
  return policy.enabled === true
    && policy.enabledChannels.includes(envelope.channel)
    && envelope.source?.kind === 'platform_original'
    && envelope.actor?.authenticated === true
    && envelope.actor?.type === 'user'
    && !isControlLikeText(envelope);
}

function addMilliseconds(timestamp, milliseconds) {
  const epochMs = Date.parse(timestamp);
  if (!Number.isFinite(epochMs)) throw new TypeError('timestamp must be RFC3339');
  return new Date(epochMs + milliseconds).toISOString();
}

function earlierTimestamp(left, right) {
  return Date.parse(left) <= Date.parse(right) ? left : right;
}

export function initialInputGroupDeadline(committedAt, policy) {
  const maxCollectUntil = addMilliseconds(committedAt, policy.maxOpenWindowMs);
  return Object.freeze({
    collectUntil: earlierTimestamp(
      addMilliseconds(committedAt, policy.quietWindowMs),
      maxCollectUntil,
    ),
    maxCollectUntil,
  });
}

export function nextInputGroupDeadline(group, committedAt, policy, nextMemberCount) {
  if (nextMemberCount >= policy.maxMembers) return committedAt;
  return earlierTimestamp(
    addMilliseconds(committedAt, policy.quietWindowMs),
    group.max_collect_until,
  );
}

export function resolveInputRoutingIntent(database, envelope) {
  const replyToMessageId = envelope.reply?.reply_to_message_id ?? null;
  if (replyToMessageId === null) {
    const value = Object.freeze({ kind: 'unscoped' });
    return Object.freeze({ value, key: canonicalizeJson(value), explicit: false });
  }
  const mappings = database.prepare(`
    SELECT mapping_id, lineage_id, binding_state
    FROM runtime_message_mappings
    WHERE region = ? AND tenant_id = ? AND channel = ? AND bot_id = ?
      AND platform_message_id = ?
    ORDER BY mapping_version DESC, mapping_id ASC
    LIMIT 2
  `).all(
    envelope.region,
    envelope.tenant_id,
    envelope.channel,
    envelope.bot_id,
    replyToMessageId,
  );
  const mapping = mappings.length === 1 ? mappings[0] : null;
  const value = Object.freeze({
    kind: 'reply',
    reply_to_message_id: replyToMessageId,
    mapping_id: mapping?.mapping_id ?? null,
    lineage_id: mapping?.binding_state === 'bound' ? mapping.lineage_id : null,
  });
  return Object.freeze({ value, key: canonicalizeJson(value), explicit: true });
}

export function findAppendableInputGroup(database, {
  originConversationId,
  actorId,
  routingIntent,
  committedAt,
  maxMembers,
}) {
  const rows = database.prepare(`
    SELECT input_group.*
    FROM runtime_input_groups AS input_group
    JOIN runtime_background_tasks AS task
      ON task.background_task_id = input_group.background_task_id
    JOIN runtime_turns AS execution
      ON execution.turn_id = input_group.execution_turn_id
    JOIN runtime_turn_queue AS queue
      ON queue.turn_id = input_group.execution_turn_id
    WHERE input_group.origin_conversation_id = ?
      AND input_group.actor_id = ?
      AND input_group.state = 'collecting'
      AND julianday(input_group.collect_until) > julianday(?)
      AND input_group.member_count < ?
      AND task.state = 'queued'
      AND execution.state = 'queued'
      AND queue.status = 'queued'
      AND (? = 0 OR input_group.routing_intent_key = ?)
    ORDER BY input_group.opened_at ASC, input_group.input_group_id ASC
    LIMIT 2
  `).all(
    originConversationId,
    actorId,
    committedAt,
    maxMembers,
    routingIntent.explicit ? 1 : 0,
    routingIntent.key,
  );
  return rows.length === 1 ? rows[0] : null;
}

export function insertInputGroupMember(database, {
  inputGroupId,
  memberOrdinal,
  originConversationId,
  inboundEventId,
  dispatchTurnId,
  envelope,
  committedAt,
}) {
  database.prepare(`
    INSERT INTO runtime_input_group_members (
      input_group_id, member_ordinal, origin_conversation_id,
      inbound_event_id, dispatch_turn_id, message_id, actor_json,
      occurred_at, content_json, reply_json, committed_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).run(
    inputGroupId,
    memberOrdinal,
    originConversationId,
    inboundEventId,
    dispatchTurnId,
    envelope.message_id,
    JSON.stringify(envelope.actor),
    envelope.occurred_at,
    JSON.stringify(envelope.content),
    JSON.stringify(envelope.reply),
    committedAt,
  );
}

export function materializeInputGroupProviderInput(database, inputGroupId) {
  const members = database.prepare(`
    SELECT member_ordinal, message_id, actor_json, occurred_at, content_json, reply_json
    FROM runtime_input_group_members
    WHERE input_group_id = ?
    ORDER BY occurred_at ASC, member_ordinal ASC
  `).all(inputGroupId).map((row) => Object.freeze({
    message_id: row.message_id,
    actor: JSON.parse(row.actor_json),
    occurred_at: row.occurred_at,
    content: JSON.parse(row.content_json),
    reply: JSON.parse(row.reply_json),
  }));
  if (members.length === 0) {
    throw new Error(`Input group ${inputGroupId} has no durable members.`);
  }
  const text = [
    '[Zylos collected input messages]',
    ...members.map((member, index) => (
      `Message ${index + 1}:\n${canonicalizeJson(member)}`
    )),
  ].join('\n\n');
  return Object.freeze({
    kind: 'text',
    text,
    attachments: Object.freeze(members.flatMap(
      (member) => structuredClone(member.content.attachments ?? []),
    )),
    input_group_id: inputGroupId,
    messages: Object.freeze(members),
  });
}
