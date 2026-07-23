import crypto from 'node:crypto';

import {
  ContractKernelError,
  createContractError,
  createIdempotencyKey,
  createPayloadHash,
  validateDeliveryCommand,
  validateInteractionAnswerAgainstRequest,
  validateInteractionAnswerResult,
  validateInteractionHandoff,
  validateInteractionRequest,
  validateNormalizedEvent,
} from '../../contracts/public/index.js';
import { resolveDeliveryCommandVersionForTarget } from '../persistence/delivery-target-identity.js';
import { initializeRuntimePersistence } from '../persistence/schema.js';

export const DEFAULT_PERMISSION_MAX_TIMED_DURATION_MS = 30 * 24 * 60 * 60 * 1_000;
export const DEFAULT_PERMISSION_CONFIRMATION_TIMEOUT_MS = 10 * 60 * 1_000;

const DURATION_MULTIPLIERS = Object.freeze({
  s: 1_000,
  m: 60_000,
  h: 3_600_000,
  d: 86_400_000,
});
const BOT_MANAGER_ROLES = new Set(['bot_owner', 'bot_admin', 'tenant_admin']);
const CONVERSATION_MANAGER_ROLES = new Set([
  'group_owner',
  'bot_owner',
  'bot_admin',
  'tenant_admin',
]);

function defaultGenerateId(kind) {
  return `${kind}-${crypto.randomUUID()}`;
}

function addMilliseconds(timestamp, milliseconds) {
  return new Date(Date.parse(timestamp) + milliseconds).toISOString();
}

function isManager(actor, roleSet) {
  return actor.roles.some((role) => roleSet.has(role));
}

export function parsePermissionCommand(envelope, {
  maxTimedDurationMs = DEFAULT_PERMISSION_MAX_TIMED_DURATION_MS,
} = {}) {
  if (
    envelope?.actor?.type !== 'user'
    || envelope.actor.authenticated !== true
    || envelope?.source?.kind !== 'platform_original'
    || envelope?.content?.kind !== 'text'
    || !Array.isArray(envelope.content.attachments)
    || envelope.content.attachments.length !== 0
    || typeof envelope.content.text !== 'string'
  ) {
    return null;
  }

  const commandText = envelope.content.text.trim();
  if (commandText === '/permission trusted') {
    return Object.freeze({
      kind: 'next_turn',
      command_text: commandText,
      duration_ms: null,
      policy_error: null,
    });
  }
  if (commandText === '/permission trusted --bot') {
    return Object.freeze({
      kind: 'persistent_bot',
      command_text: commandText,
      duration_ms: null,
      policy_error: null,
    });
  }
  if (commandText === '/permission safe') {
    return Object.freeze({
      kind: 'safe',
      command_text: commandText,
      duration_ms: null,
      policy_error: null,
    });
  }

  const durationMatch = /^\/permission trusted ([1-9][0-9]*)([smhd])$/.exec(commandText);
  if (!durationMatch) return null;
  const magnitude = Number(durationMatch[1]);
  const durationMs = magnitude * DURATION_MULTIPLIERS[durationMatch[2]];
  if (!Number.isSafeInteger(durationMs)) return null;
  return Object.freeze({
    kind: 'timed_conversation',
    command_text: commandText,
    duration_ms: durationMs,
    policy_error: durationMs > maxTimedDurationMs
      ? 'permission_duration_exceeds_policy'
      : null,
  });
}

function currentRevision(database) {
  return database.prepare(`
    SELECT current_revision
    FROM runtime_permission_revision_sequence
    WHERE singleton_id = 1
  `).get().current_revision;
}

function nextRevision(database) {
  database.prepare(`
    UPDATE runtime_permission_revision_sequence
    SET current_revision = current_revision + 1
    WHERE singleton_id = 1
  `).run();
  return currentRevision(database);
}

function permissionScope(envelope, conversationId, scopeKind = 'conversation') {
  return {
    scope_kind: scopeKind,
    region: envelope.region,
    tenant_id: envelope.tenant_id,
    bot_id: envelope.bot_id,
    conversation_id: scopeKind === 'conversation' ? conversationId : null,
  };
}

function insertAudit(database, {
  generateId,
  action,
  actorId,
  source,
  scope,
  policyRevision,
  reason,
  committedAt,
  turnId = null,
  grantId = null,
  controlId = null,
  actionRef = null,
  redactedContext = {},
}) {
  const auditId = generateId('permission-audit');
  database.prepare(`
    INSERT INTO runtime_permission_audit (
      audit_id, action, actor_id, source, scope_json, policy_revision,
      reason, redacted_context_json, turn_id, grant_id, control_id,
      action_ref, committed_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).run(
    auditId,
    action,
    actorId,
    source,
    JSON.stringify(scope),
    policyRevision,
    reason,
    JSON.stringify(redactedContext),
    turnId,
    grantId,
    controlId,
    actionRef,
    committedAt,
  );
  return auditId;
}

function deliveryTargetFromEnvelope(envelope) {
  return {
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
    ...(envelope.channel === 'feishu' ? {
      reply_target_message_id: envelope.message_id,
      mention_actor_id: (
        envelope.actor.type === 'user'
        && ['group', 'thread'].includes(envelope.chat_type)
      )
        ? envelope.actor.actor_id
        : null,
    } : {}),
  };
}

function enqueueSecurityNotice(database, {
  envelope,
  conversationId,
  controlId,
  text,
  committedAt,
  generateId,
  interaction = null,
}) {
  const target = deliveryTargetFromEnvelope(envelope);
  const outboxId = generateId('outbox');
  const deliveryId = generateId('delivery');
  const command = {
    contract: 'zylos.delivery-command',
    contract_version: resolveDeliveryCommandVersionForTarget(target),
    outbox_id: outboxId,
    delivery_id: deliveryId,
    trace_id: envelope.trace_id,
    delivery_attempt_id: generateId('delivery-attempt'),
    delivery_attempt_no: 1,
    outbox_lease_epoch: 1,
    target,
    aggregate_type: 'security_notice',
    aggregate_id: controlId,
    operation: 'send_text',
    aggregate_version: 1,
    event_sequence_through: null,
    idempotency_key: createIdempotencyKey('delivery', {
      channel: envelope.channel,
      target,
      delivery_id: deliveryId,
    }),
    render_model: {
      title: 'Zylos permissions',
      phase: interaction === null ? 'permission_changed' : 'permission_confirmation_required',
      text,
      error: null,
      tools: [],
      interactions: interaction === null ? [] : [interaction],
      terminal: true,
      user_action_required: interaction !== null,
    },
    mapping: {
      mapping_id: generateId('mapping'),
      conversation_id: conversationId,
      turn_id: null,
      lineage_id: null,
      binding_state: 'not_applicable',
      mapping_version: 1,
    },
    target_platform_message_id: null,
    predecessor_delivery_id: null,
    expected_platform_version: null,
    priority: 100,
    not_before: committedAt,
    created_at: committedAt,
  };
  validateDeliveryCommand(command, { occurredAt: committedAt });
  database.prepare(`
    INSERT INTO runtime_outbox (
      outbox_id, delivery_id, aggregate_type, aggregate_id, turn_id, control_id,
      lane_key, predecessor_delivery_id, aggregate_version, status, command_json,
      priority, supersedable, terminal, next_attempt_at, created_at, updated_at
    ) VALUES (?, ?, 'security_notice', ?, NULL, ?, NULL, NULL, 1, 'pending',
      ?, ?, 0, 1, ?, ?, ?)
  `).run(
    command.outbox_id,
    command.delivery_id,
    controlId,
    controlId,
    JSON.stringify(command),
    command.priority,
    committedAt,
    committedAt,
    committedAt,
  );
  return command;
}

function commandError(code, userMessage, committedAt, category = 'authorization') {
  return createContractError({
    code,
    category,
    retryable: false,
    sideEffectStatus: 'none',
    userMessage,
    occurredAt: committedAt,
  });
}

function permissionControlOutcome({
  controlId,
  status,
  actionKind,
  actorId,
  source,
  policyRevision,
  grantId,
  reason,
  resolvedAt,
  error = null,
}) {
  return {
    record: 'zylos.permission-control-outcome',
    record_version: '1.0',
    control_id: controlId,
    status,
    action_kind: actionKind,
    actor_id: actorId,
    source,
    policy_revision: policyRevision,
    grant_id: grantId,
    reason,
    error,
    resolved_at: resolvedAt,
  };
}

function createGrant(database, {
  envelope,
  conversationId,
  command,
  committedAt,
  generateId = defaultGenerateId,
}) {
  const grantId = generateId('permission-grant');
  const revision = nextRevision(database);
  const expiresAt = command.duration_ms === null
    ? null
    : addMilliseconds(committedAt, command.duration_ms);
  database.prepare(`
    INSERT INTO runtime_permission_grants (
      grant_id, grant_kind, state, region, tenant_id, bot_id, conversation_id,
      issued_by_actor_id, source, policy_revision, issued_at, expires_at,
      consumed_by_turn_id, consumed_at, revoked_at, expired_at, notice_target_json
    ) VALUES (?, ?, 'active', ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, ?)
  `).run(
    grantId,
    command.kind,
    envelope.region,
    envelope.tenant_id,
    envelope.bot_id,
    command.kind === 'persistent_bot' ? null : conversationId,
    envelope.actor.actor_id,
    envelope.source.kind,
    revision,
    committedAt,
    expiresAt,
    JSON.stringify(deliveryTargetFromEnvelope(envelope)),
  );
  return { grantId, revision, expiresAt };
}

function createConversationBarrier(database, {
  envelope,
  conversationId,
  committedAt,
  generateId,
}) {
  const revision = nextRevision(database);
  const revocationId = generateId('permission-revocation');
  database.prepare(`
    INSERT INTO runtime_permission_revocations (
      revocation_id, scope_kind, region, tenant_id, bot_id, conversation_id,
      actor_id, source, policy_revision, reason, committed_at
    ) VALUES (?, 'conversation', ?, ?, ?, ?, ?, ?, ?, 'safe_command', ?)
  `).run(
    revocationId,
    envelope.region,
    envelope.tenant_id,
    envelope.bot_id,
    conversationId,
    envelope.actor.actor_id,
    envelope.source.kind,
    revision,
    committedAt,
  );
  database.prepare(`
    UPDATE runtime_permission_grants
    SET state = 'revoked', revoked_at = ?
    WHERE conversation_id = ?
      AND grant_kind IN ('next_turn', 'timed_conversation')
      AND state = 'active'
      AND policy_revision < ?
  `).run(committedAt, conversationId, revision);
  return { revision, revocationId };
}

function confirmationRequestScope(envelope) {
  return {
    region: envelope.region,
    tenant_id: envelope.tenant_id,
    channel: envelope.channel,
    bot_id: envelope.bot_id,
    chat_id: envelope.chat_id,
    native_thread_or_topic_id: envelope.native_thread_or_topic_id,
  };
}

function createPendingConfirmation(database, {
  envelope,
  conversationId,
  controlId,
  actionKind,
  targetGrantId = null,
  expectedCommand,
  committedAt,
  generateId,
  confirmationTimeoutMs,
}) {
  const interactionId = generateId('interaction');
  const expiresAt = addMilliseconds(committedAt, confirmationTimeoutMs);
  const request = {
    contract: 'zylos.interaction-request',
    contract_version: '1.0',
    trace_id: envelope.trace_id,
    interaction_id: interactionId,
    conversation_id: conversationId,
    turn_id: null,
    lineage_id: null,
    control_id: controlId,
    parent_type: 'security_control',
    tool_use_id: null,
    ordinal: 1,
    kind: 'permission_approval',
    prompt: actionKind === 'grant_persistent_bot'
      ? 'Confirm persistent trusted mode for this bot.'
      : 'Confirm revoking persistent trusted mode for this bot.',
    choices: [],
    authorized_subjects: [{ type: 'actor', actor_id: envelope.actor.actor_id }],
    allowed_sources: ['card_action', 'magic_command_repeat'],
    runtime_fence: null,
    state: 'pending',
    version: 1,
    handoff_state: 'not_started',
    created_at: committedAt,
    expires_at: expiresAt,
    card_delivery_id: null,
  };
  validateInteractionRequest(request, { occurredAt: committedAt });
  database.prepare(`
    INSERT INTO runtime_interactions (
      interaction_id, conversation_id, turn_id, lineage_id, parent_type, parent_id,
      ordinal, state, version, handoff_state, handoff_version, request_json,
      created_at, updated_at
    ) VALUES (?, ?, NULL, NULL, 'security_control', ?, 1, 'pending', 1,
      'not_started', NULL, ?, ?, ?)
  `).run(
    interactionId,
    conversationId,
    controlId,
    JSON.stringify(request),
    committedAt,
    committedAt,
  );
  database.prepare(`
    INSERT INTO runtime_permission_confirmations (
      control_id, interaction_id, action_kind, expected_command, actor_id,
      region, tenant_id, bot_id, conversation_id, target_grant_id,
      requested_policy_revision, status, expires_at, resolved_at,
      effect_policy_revision
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, NULL, NULL)
  `).run(
    controlId,
    interactionId,
    actionKind,
    expectedCommand,
    envelope.actor.actor_id,
    envelope.region,
    envelope.tenant_id,
    envelope.bot_id,
    conversationId,
    targetGrantId,
    currentRevision(database),
    expiresAt,
  );
  database.prepare(`
    UPDATE runtime_permission_controls
    SET interaction_id = ?, status = 'pending_confirmation'
    WHERE control_id = ?
  `).run(interactionId, controlId);
  return request;
}

function interactionErrorResult(answer, error, request = null) {
  const result = {
    contract: 'zylos.interaction-answer-result',
    contract_version: '1.0',
    trace_id: answer.trace_id,
    interaction_id: answer.interaction_id,
    answer_id: answer.answer_id,
    idempotency_key: answer.idempotency_key,
    status: 'rejected',
    interaction_state: request?.state ?? null,
    interaction_version: request?.version ?? null,
    handoff_state: 'not_applicable',
    handoff_id: null,
    turn_id: null,
    turn_version: null,
    control_id: request?.control_id ?? null,
    error,
    received_at: null,
    committed_at: null,
  };
  validateInteractionAnswerResult(result, { occurredAt: answer.answered_at });
  return result;
}

function applyConfirmedPolicy(database, {
  confirmation,
  envelope,
  actor,
  source,
  decision,
  committedAt,
  generateId,
}) {
  if (decision === 'deny') {
    return {
      effectRevision: currentRevision(database),
      grantId: null,
      action: 'permission_confirmation_denied',
      reason: 'actor_denied',
    };
  }
  if (!isManager(actor, BOT_MANAGER_ROLES)) {
    throw new ContractKernelError(commandError(
      'forbidden',
      'The confirming actor no longer has bot permission authority.',
      committedAt,
    ));
  }
  if (confirmation.action_kind === 'grant_persistent_bot') {
    const grant = createGrant(database, {
      envelope: { ...envelope, actor, source: { kind: source } },
      conversationId: confirmation.conversation_id,
      command: {
        kind: 'persistent_bot',
        command_text: confirmation.expected_command,
        duration_ms: null,
      },
      committedAt,
      generateId,
    });
    return {
      effectRevision: grant.revision,
      grantId: grant.grantId,
      action: 'permission_confirmation_approved',
      reason: 'persistent_bot_granted',
    };
  }

  const effectRevision = nextRevision(database);
  database.prepare(`
    INSERT INTO runtime_permission_revocations (
      revocation_id, scope_kind, region, tenant_id, bot_id, conversation_id,
      actor_id, source, policy_revision, reason, committed_at
    ) VALUES (?, 'bot', ?, ?, ?, NULL, ?, ?, ?, 'safe_bot_confirmation', ?)
  `).run(
    generateId('permission-revocation'),
    confirmation.region,
    confirmation.tenant_id,
    confirmation.bot_id,
    actor.actor_id,
    source,
    effectRevision,
    committedAt,
  );
  const revoked = database.prepare(`
    UPDATE runtime_permission_grants
    SET state = 'revoked', revoked_at = ?
    WHERE grant_id = ? AND grant_kind = 'persistent_bot' AND state = 'active'
      AND region = ? AND tenant_id = ? AND bot_id = ?
      AND policy_revision < ?
  `).run(
    committedAt,
    confirmation.target_grant_id,
    confirmation.region,
    confirmation.tenant_id,
    confirmation.bot_id,
    effectRevision,
  );
  if (revoked.changes !== 1) {
    throw new ContractKernelError(commandError(
      'version_conflict',
      'The targeted persistent bot grant is no longer current.',
      committedAt,
      'conflict',
    ));
  }
  return {
    effectRevision,
    grantId: null,
    action: 'permission_bot_revoked',
    reason: 'safe_bot_confirmation',
  };
}

function invalidateStaleConfirmation(database, {
  confirmation,
  request,
  envelope,
  committedAt,
  generateId,
}) {
  const invalidatedRequest = {
    ...request,
    state: 'expired',
    version: request.version + 1,
  };
  validateInteractionRequest(invalidatedRequest, { occurredAt: committedAt });
  database.prepare(`
    UPDATE runtime_permission_confirmations
    SET status = 'expired', resolved_at = ?
    WHERE control_id = ? AND status = 'pending'
  `).run(committedAt, confirmation.control_id);
  database.prepare(`
    UPDATE runtime_interactions
    SET state = 'expired', version = ?, request_json = ?, updated_at = ?
    WHERE interaction_id = ? AND state = 'pending' AND version = ?
  `).run(
    invalidatedRequest.version,
    JSON.stringify(invalidatedRequest),
    committedAt,
    request.interaction_id,
    request.version,
  );
  const revision = currentRevision(database);
  const error = commandError(
    'version_conflict',
    'Bot permission policy changed; issue a fresh command to confirm the current policy.',
    committedAt,
    'conflict',
  );
  const finalOutcome = permissionControlOutcome({
    controlId: confirmation.control_id,
    status: 'invalidated',
    actionKind: confirmation.action_kind,
    actorId: confirmation.actor_id,
    source: 'core_policy_recheck',
    policyRevision: revision,
    grantId: confirmation.target_grant_id,
    reason: 'policy_revision_changed',
    resolvedAt: committedAt,
    error,
  });
  database.prepare(`
    UPDATE runtime_permission_controls
    SET status = 'completed', final_result_json = ?
    WHERE control_id = ?
  `).run(JSON.stringify(finalOutcome), confirmation.control_id);
  insertAudit(database, {
    generateId,
    action: 'permission_confirmation_invalidated',
    actorId: confirmation.actor_id,
    source: 'core_policy_recheck',
    scope: permissionScope(envelope, confirmation.conversation_id, 'bot'),
    policyRevision: revision,
    reason: 'policy_revision_changed',
    committedAt,
    grantId: confirmation.target_grant_id,
    controlId: confirmation.control_id,
    redactedContext: { action_kind: confirmation.action_kind },
  });
  return Object.freeze({ error, request: invalidatedRequest });
}

function resolveConfirmationInTransaction(database, {
  answer,
  request,
  confirmation,
  envelope,
  committedAt,
  generateId,
}) {
  const payloadHash = createPayloadHash(answer, {
    scope: 'interaction',
    knownFields: Object.keys(answer),
  });
  const existing = database.prepare(`
    SELECT payload_hash, result_json
    FROM runtime_interaction_answers
    WHERE idempotency_key = ?
  `).get(answer.idempotency_key);
  if (existing) {
    if (existing.payload_hash !== payloadHash) {
      return interactionErrorResult(answer, commandError(
        'idempotency_conflict',
        'The interaction idempotency key was reused with a different answer.',
        answer.answered_at,
        'conflict',
      ), request);
    }
    const first = JSON.parse(existing.result_json);
    const duplicate = { ...first, trace_id: answer.trace_id, status: 'duplicate' };
    validateInteractionAnswerResult(duplicate, { occurredAt: answer.answered_at });
    return duplicate;
  }

  if (confirmation.status !== 'pending' || confirmation.expires_at <= committedAt) {
    return interactionErrorResult(answer, commandError(
      'interaction_expired',
      'The permission confirmation has expired.',
      answer.answered_at,
      'conflict',
    ), request);
  }
  const revisionMatches = currentRevision(database) === confirmation.requested_policy_revision;
  const targetStateMatches = confirmation.action_kind === 'revoke_persistent_bot'
    ? database.prepare(`
      SELECT grant_id
      FROM runtime_permission_grants
      WHERE grant_id = ? AND grant_kind = 'persistent_bot' AND state = 'active'
        AND region = ? AND tenant_id = ? AND bot_id = ?
    `).get(
      confirmation.target_grant_id,
      confirmation.region,
      confirmation.tenant_id,
      confirmation.bot_id,
    ) !== undefined
    : database.prepare(`
      SELECT grant_id
      FROM runtime_permission_grants
      WHERE grant_kind = 'persistent_bot' AND state = 'active'
        AND region = ? AND tenant_id = ? AND bot_id = ?
      LIMIT 1
    `).get(
      confirmation.region,
      confirmation.tenant_id,
      confirmation.bot_id,
    ) === undefined;
  if (!revisionMatches || !targetStateMatches) {
    const invalidated = invalidateStaleConfirmation(database, {
      confirmation,
      request,
      envelope,
      committedAt,
      generateId,
    });
    return interactionErrorResult(answer, invalidated.error, invalidated.request);
  }
  validateInteractionAnswerAgainstRequest(answer, request, {
    interactions: [request],
    requestScope: confirmationRequestScope(envelope),
    actorCapabilities: [],
    occurredAt: committedAt,
  });
  const effect = applyConfirmedPolicy(database, {
    confirmation,
    envelope,
    actor: answer.actor,
    source: answer.source,
    decision: answer.value.decision,
    committedAt,
    generateId,
  });
  const handoffId = generateId('handoff');
  const result = {
    contract: 'zylos.interaction-answer-result',
    contract_version: '1.0',
    trace_id: answer.trace_id,
    interaction_id: answer.interaction_id,
    answer_id: answer.answer_id,
    idempotency_key: answer.idempotency_key,
    status: 'accepted',
    interaction_state: 'answer_committed',
    interaction_version: request.version + 1,
    handoff_state: 'pending',
    handoff_id: handoffId,
    turn_id: null,
    turn_version: null,
    control_id: request.control_id,
    error: null,
    received_at: answer.answered_at,
    committed_at: committedAt,
  };
  validateInteractionAnswerResult(result, { occurredAt: committedAt });
  const handoff = {
    handoff_id: handoffId,
    interaction_id: request.interaction_id,
    answer_id: answer.answer_id,
    parent_type: 'security_control',
    state: 'accepted',
    provider_attempt_id: null,
    handoff_attempt_id: generateId('handoff-attempt'),
    handoff_attempt_no: 1,
    lease_epoch: null,
    claimed_by: 'core-permission-service',
    claimed_at: committedAt,
    last_send_started_at: committedAt,
    provider_acked_at: committedAt,
    handoff_deadline_at: request.expires_at,
    reason_code: effect.reason,
    error: null,
    side_effect_status: 'known',
  };
  validateInteractionHandoff(handoff, { occurredAt: committedAt });
  const resolvedRequest = {
    ...request,
    state: 'answered',
    version: request.version + 1,
    handoff_state: 'accepted',
    handoff_version: 1,
  };
  validateInteractionRequest(resolvedRequest, { occurredAt: committedAt });

  database.prepare(`
    INSERT INTO runtime_interaction_answers (
      answer_id, interaction_id, idempotency_key, payload_hash,
      answer_json, result_json, committed_at
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
  database.prepare(`
    INSERT INTO runtime_interaction_handoffs (
      handoff_id, interaction_id, answer_id, state, parent_type,
      provider_attempt_id, handoff_attempt_id, handoff_attempt_no, lease_epoch,
      record_json, created_at, updated_at
    ) VALUES (?, ?, ?, 'accepted', 'security_control', NULL, ?, 1, NULL, ?, ?, ?)
  `).run(
    handoffId,
    answer.interaction_id,
    answer.answer_id,
    handoff.handoff_attempt_id,
    JSON.stringify(handoff),
    committedAt,
    committedAt,
  );
  database.prepare(`
    UPDATE runtime_interactions
    SET state = 'answered', version = ?, handoff_state = 'accepted',
      handoff_version = 1, request_json = ?, updated_at = ?
    WHERE interaction_id = ? AND state = 'pending' AND version = ?
  `).run(
    resolvedRequest.version,
    JSON.stringify(resolvedRequest),
    committedAt,
    request.interaction_id,
    request.version,
  );
  database.prepare(`
    UPDATE runtime_permission_confirmations
    SET status = ?, resolved_at = ?, effect_policy_revision = ?
    WHERE control_id = ? AND status = 'pending'
  `).run(
    answer.value.decision === 'approve' ? 'approved' : 'denied',
    committedAt,
    effect.effectRevision === 0 ? null : effect.effectRevision,
    confirmation.control_id,
  );
  const finalOutcome = permissionControlOutcome({
    controlId: confirmation.control_id,
    status: answer.value.decision === 'approve' ? 'approved' : 'denied',
    actionKind: confirmation.action_kind,
    actorId: answer.actor.actor_id,
    source: answer.source,
    policyRevision: effect.effectRevision,
    grantId: effect.grantId ?? confirmation.target_grant_id,
    reason: effect.reason,
    resolvedAt: committedAt,
  });
  database.prepare(`
    UPDATE runtime_permission_controls
    SET status = 'completed', policy_revision = ?, grant_id = COALESCE(?, grant_id),
      final_result_json = ?
    WHERE control_id = ?
  `).run(
    effect.effectRevision === 0 ? null : effect.effectRevision,
    effect.grantId,
    JSON.stringify(finalOutcome),
    confirmation.control_id,
  );
  database.prepare(`
    INSERT INTO runtime_interaction_audit (
      audit_id, interaction_id, handoff_id, outcome, provider_attempt_id,
      lease_epoch, acknowledgement_json, created_at
    ) VALUES (?, ?, ?, 'accepted', NULL, NULL, ?, ?)
  `).run(
    generateId('interaction-audit'),
    request.interaction_id,
    handoffId,
    JSON.stringify({
      source: answer.source,
      effect_policy_revision: effect.effectRevision,
      side_effect_status: 'known',
    }),
    committedAt,
  );
  insertAudit(database, {
    generateId,
    action: effect.action,
    actorId: answer.actor.actor_id,
    source: answer.source,
    scope: permissionScope(
      envelope,
      confirmation.conversation_id,
      'bot',
    ),
    policyRevision: effect.effectRevision,
    reason: effect.reason,
    committedAt,
    grantId: effect.grantId,
    controlId: confirmation.control_id,
    redactedContext: { decision: answer.value.decision },
  });
  return result;
}

export function acceptPermissionCommandInTransaction(database, {
  envelope,
  command,
  conversationId,
  payloadHash,
  committedAt,
  generateId,
  confirmationTimeoutMs = DEFAULT_PERMISSION_CONFIRMATION_TIMEOUT_MS,
}) {
  const controlId = generateId('permission-control');
  database.prepare(`
    INSERT INTO runtime_inbound_events (
      inbound_event_id, idempotency_key, conversation_id, message_id,
      payload_hash, envelope_json, received_at, committed_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
  `).run(
    envelope.inbound_event_id,
    envelope.idempotency_key,
    conversationId,
    envelope.message_id,
    payloadHash,
    JSON.stringify(envelope),
    envelope.received_at,
    committedAt,
  );

  let status = 'completed';
  let error = null;
  let grantId = null;
  let revision = currentRevision(database);
  let noticeText;
  let auditAction;
  let auditReason;
  let confirmationAction = null;
  let confirmationTargetGrantId = null;
  let interaction = null;

  const pendingRepeat = database.prepare(`
    SELECT confirmation.*, interaction.request_json
    FROM runtime_permission_confirmations AS confirmation
    JOIN runtime_interactions AS interaction
      ON interaction.interaction_id = confirmation.interaction_id
    WHERE confirmation.status = 'pending'
      AND confirmation.expected_command = ?
      AND confirmation.actor_id = ?
      AND confirmation.region = ?
      AND confirmation.tenant_id = ?
      AND confirmation.bot_id = ?
      AND confirmation.conversation_id = ?
      AND confirmation.expires_at > ?
    ORDER BY confirmation.expires_at ASC
    LIMIT 1
  `).get(
    command.command_text,
    envelope.actor.actor_id,
    envelope.region,
    envelope.tenant_id,
    envelope.bot_id,
    conversationId,
    committedAt,
  );

  if (pendingRepeat) {
    const request = JSON.parse(pendingRepeat.request_json);
    const sourceEventId = envelope.inbound_event_id;
    const answer = {
      contract: 'zylos.interaction-answer',
      contract_version: '1.0',
      trace_id: envelope.trace_id,
      interaction_id: request.interaction_id,
      interaction_version: request.version,
      answer_id: generateId('interaction-answer'),
      source_event_or_action_id: sourceEventId,
      actor: envelope.actor,
      source_context: {
        ...confirmationRequestScope(envelope),
        platform_message_or_action_id: sourceEventId,
      },
      source: 'magic_command_repeat',
      value: { kind: 'decision', decision: 'approve' },
      answered_at: committedAt,
      idempotency_key: createIdempotencyKey('interaction', {
        interaction_id: request.interaction_id,
        source_event_or_action_id: sourceEventId,
      }),
    };
    let confirmationResult;
    try {
      confirmationResult = resolveConfirmationInTransaction(database, {
        answer,
        request,
        confirmation: pendingRepeat,
        envelope,
        committedAt,
        generateId,
      });
    } catch (confirmationError) {
      if (!(confirmationError instanceof ContractKernelError)) throw confirmationError;
      confirmationResult = interactionErrorResult(
        answer,
        confirmationError.contractError,
        request,
      );
    }
    if (confirmationResult.status === 'accepted') {
      const resolved = database.prepare(`
        SELECT confirmation.effect_policy_revision, control.grant_id
        FROM runtime_permission_confirmations AS confirmation
        JOIN runtime_permission_controls AS control
          ON control.control_id = confirmation.control_id
        WHERE confirmation.control_id = ?
      `).get(pendingRepeat.control_id);
      revision = resolved.effect_policy_revision ?? currentRevision(database);
      grantId = resolved.grant_id;
      noticeText = pendingRepeat.action_kind === 'grant_persistent_bot'
        ? 'Persistent trusted mode is active for this bot.'
        : 'Persistent trusted mode has been revoked for this bot.';
      auditAction = 'permission_confirmation_repeat_accepted';
      auditReason = pendingRepeat.action_kind;
    } else {
      status = 'rejected';
      error = confirmationResult.error;
      revision = currentRevision(database);
      noticeText = error.user_message;
      auditAction = 'permission_confirmation_rejected';
      auditReason = error.code;
    }
  } else if (command.policy_error !== null) {
    status = 'rejected';
    error = commandError(
      command.policy_error,
      'The requested trusted duration exceeds the configured permission policy.',
      committedAt,
      'validation',
    );
    noticeText = error.user_message;
    auditAction = 'permission_rejected';
    auditReason = command.policy_error;
  } else if (
    command.kind === 'timed_conversation'
    && envelope.chat_type !== 'dm'
    && !isManager(envelope.actor, CONVERSATION_MANAGER_ROLES)
  ) {
    status = 'rejected';
    error = commandError(
      'forbidden',
      'This actor cannot enable timed trusted mode for this conversation.',
      committedAt,
    );
    noticeText = error.user_message;
    auditAction = 'permission_forbidden';
    auditReason = 'role_scope_restriction';
  } else if (command.kind === 'persistent_bot') {
    if (!isManager(envelope.actor, BOT_MANAGER_ROLES)) {
      status = 'rejected';
      error = commandError(
        'forbidden',
        'This actor cannot change persistent bot permission policy.',
        committedAt,
      );
      noticeText = error.user_message;
      auditAction = 'permission_forbidden';
      auditReason = 'role_scope_restriction';
    } else {
      const activeBotGrant = database.prepare(`
        SELECT grant_id, policy_revision
        FROM runtime_permission_grants
        WHERE grant_kind = 'persistent_bot' AND state = 'active'
          AND region = ? AND tenant_id = ? AND bot_id = ?
        LIMIT 1
      `).get(envelope.region, envelope.tenant_id, envelope.bot_id);
      if (activeBotGrant) {
        grantId = activeBotGrant.grant_id;
        revision = currentRevision(database);
        noticeText = 'Persistent trusted mode is already active for this bot.';
        auditAction = 'permission_policy_unchanged';
        auditReason = 'persistent_bot_already_active';
      } else {
        status = 'pending_confirmation';
        noticeText = 'Persistent trusted mode requires confirmation by the same actor.';
        auditAction = 'permission_confirmation_pending';
        auditReason = 'persistent_bot_double_confirmation_required';
        confirmationAction = 'grant_persistent_bot';
      }
    }
  } else if (command.kind === 'safe') {
    const mayManageBot = isManager(envelope.actor, BOT_MANAGER_ROLES);
    const activeBotGrant = mayManageBot && database.prepare(`
      SELECT grant_id
      FROM runtime_permission_grants
      WHERE grant_kind = 'persistent_bot' AND state = 'active'
        AND region = ? AND tenant_id = ? AND bot_id = ?
      LIMIT 1
    `).get(envelope.region, envelope.tenant_id, envelope.bot_id);
    ({ revision } = createConversationBarrier(database, {
      envelope,
      conversationId,
      committedAt,
      generateId,
    }));
    noticeText = 'Safe mode is active for this conversation.';
    auditAction = 'permission_safe_applied';
    auditReason = 'safe_command';
    if (activeBotGrant) {
      status = 'pending_confirmation';
      confirmationAction = 'revoke_persistent_bot';
      confirmationTargetGrantId = activeBotGrant.grant_id;
      noticeText += ' Confirm separately to revoke persistent trusted mode for this bot.';
    }
  } else {
    const grant = createGrant(database, {
      envelope,
      conversationId,
      command,
      committedAt,
      generateId,
    });
    grantId = grant.grantId;
    revision = grant.revision;
    noticeText = command.kind === 'next_turn'
      ? 'Trusted mode will apply to your next accepted executable turn in this conversation.'
      : `Trusted mode is active for this conversation until ${grant.expiresAt}.`;
    auditAction = 'permission_granted';
    auditReason = command.kind;
  }

  const result = {
    contract: 'zylos.inbound-result',
    contract_version: '1.0',
    trace_id: envelope.trace_id,
    inbound_event_id: envelope.inbound_event_id,
    idempotency_key: envelope.idempotency_key,
    status: error === null ? 'accepted' : 'rejected',
    conversation_id: conversationId,
    turn_id: null,
    lineage_id: null,
    control_id: controlId,
    turn_version: null,
    lineage_resolution_state: 'not_applicable',
    deduplicated: false,
    error,
    committed_at: committedAt,
  };
  const finalOutcome = status === 'pending_confirmation'
    ? null
    : permissionControlOutcome({
      controlId,
      status: status === 'rejected' ? 'rejected' : 'completed',
      actionKind: command.kind,
      actorId: envelope.actor.actor_id,
      source: envelope.source.kind,
      policyRevision: revision,
      grantId,
      reason: auditReason,
      resolvedAt: committedAt,
      error,
    });
  database.prepare(`
    INSERT INTO runtime_permission_controls (
      control_id, inbound_event_id, conversation_id, command_kind, command_text,
      actor_id, source, status, policy_revision, grant_id, interaction_id,
      result_json, final_result_json, committed_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?)
  `).run(
    controlId,
    envelope.inbound_event_id,
    conversationId,
    command.kind,
    command.command_text,
    envelope.actor.actor_id,
    envelope.source.kind,
    status,
    revision === 0 ? null : revision,
    grantId,
    JSON.stringify(result),
    finalOutcome === null ? null : JSON.stringify(finalOutcome),
    committedAt,
  );
  if (confirmationAction !== null) {
    interaction = createPendingConfirmation(database, {
      envelope,
      conversationId,
      controlId,
      actionKind: confirmationAction,
      targetGrantId: confirmationTargetGrantId,
      expectedCommand: command.command_text,
      committedAt,
      generateId,
      confirmationTimeoutMs,
    });
  }
  insertAudit(database, {
    generateId,
    action: auditAction,
    actorId: envelope.actor.actor_id,
    source: envelope.source.kind,
    scope: permissionScope(
      envelope,
      conversationId,
      command.kind === 'persistent_bot' ? 'bot' : 'conversation',
    ),
    policyRevision: revision,
    reason: auditReason,
    committedAt,
    grantId,
    controlId,
    redactedContext: { command_kind: command.kind },
  });
  enqueueSecurityNotice(database, {
    envelope,
    conversationId,
    controlId,
    text: noticeText,
    committedAt,
    generateId,
    interaction,
  });
  return result;
}

function latestBarrierRevision(database, turn, grant) {
  return database.prepare(`
    SELECT COALESCE(MAX(policy_revision), 0) AS revision
    FROM runtime_permission_revocations
    WHERE region = ? AND tenant_id = ? AND bot_id = ?
      AND policy_revision > ?
      AND (
        scope_kind = 'bot'
        OR (scope_kind = 'conversation' AND conversation_id = ?)
      )
  `).get(
    turn.region,
    turn.tenant_id,
    turn.bot_id,
    grant.policy_revision,
    turn.conversation_id,
  ).revision;
}

export function bindPermissionToAcceptedTurnInTransaction(database, {
  turnId,
  actorId,
  conversationId,
  acceptedAt,
  generateId = defaultGenerateId,
}) {
  const revision = currentRevision(database);
  const conversation = database.prepare(`
    SELECT region, tenant_id, bot_id
    FROM runtime_conversations
    WHERE conversation_id = ?
  `).get(conversationId);
  const candidates = database.prepare(`
    SELECT *
    FROM runtime_permission_grants
    WHERE state = 'active'
      AND (
        (grant_kind = 'next_turn' AND conversation_id = ? AND issued_by_actor_id = ?)
        OR (grant_kind = 'timed_conversation' AND conversation_id = ? AND expires_at > ?)
        OR (grant_kind = 'persistent_bot' AND conversation_id IS NULL
          AND region = ? AND tenant_id = ? AND bot_id = ?)
      )
    ORDER BY CASE grant_kind
      WHEN 'next_turn' THEN 1
      WHEN 'timed_conversation' THEN 2
      ELSE 3
    END, policy_revision ASC
  `).all(
    conversationId,
    actorId,
    conversationId,
    acceptedAt,
    conversation.region,
    conversation.tenant_id,
    conversation.bot_id,
  );
  const turnScope = { ...conversation, conversation_id: conversationId };
  const grant = candidates.find((candidate) => (
    latestBarrierRevision(database, turnScope, candidate) === 0
  ));
  if (grant?.grant_kind === 'next_turn') {
    const consumed = database.prepare(`
      UPDATE runtime_permission_grants
      SET state = 'consumed', consumed_by_turn_id = ?, consumed_at = ?
      WHERE grant_id = ? AND state = 'active'
    `).run(turnId, acceptedAt, grant.grant_id);
    if (consumed.changes !== 1) throw new Error('Next-turn permission consumption lost atomicity.');
    insertAudit(database, {
      generateId,
      action: 'permission_consumed',
      actorId,
      source: 'core_inbound',
      scope: {
        scope_kind: 'conversation',
        region: conversation.region,
        tenant_id: conversation.tenant_id,
        bot_id: conversation.bot_id,
        conversation_id: conversationId,
      },
      policyRevision: revision,
      reason: 'next_turn_first_accepted',
      committedAt: acceptedAt,
      turnId,
      grantId: grant.grant_id,
      redactedContext: { grant_kind: grant.grant_kind },
    });
  }
  const basisKind = grant?.grant_kind ?? 'default_safe';
  database.prepare(`
    INSERT INTO runtime_turn_permissions (
      turn_id, actor_id, mode, basis_kind, grant_id, grant_policy_revision,
      accepted_policy_revision, accepted_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
  `).run(
    turnId,
    actorId,
    grant ? 'trusted' : 'safe',
    basisKind,
    grant?.grant_id ?? null,
    grant?.policy_revision ?? null,
    revision,
    acceptedAt,
  );
}

function protectedActionDecision(database, {
  turnId,
  actionRef,
  actionKind,
  checkedAt,
  generateId,
}) {
  const row = database.prepare(`
    SELECT permission.*, turn.conversation_id,
      conversation.region, conversation.tenant_id, conversation.bot_id,
      grant.grant_kind, grant.state AS grant_state, grant.expires_at,
      grant.consumed_by_turn_id, grant.policy_revision
    FROM runtime_turn_permissions AS permission
    JOIN runtime_turns AS turn ON turn.turn_id = permission.turn_id
    JOIN runtime_conversations AS conversation
      ON conversation.conversation_id = turn.conversation_id
    LEFT JOIN runtime_permission_grants AS grant ON grant.grant_id = permission.grant_id
    WHERE permission.turn_id = ?
  `).get(turnId);
  if (!row) throw new Error(`Turn ${turnId} has no durable permission basis.`);

  const revision = currentRevision(database);
  let trusted = row.mode === 'trusted';
  let reason = trusted ? 'grant_current' : 'default_safe';
  if (trusted) {
    if (latestBarrierRevision(database, row, row) > 0) {
      trusted = false;
      reason = 'revocation_barrier';
    } else if (row.grant_kind === 'next_turn') {
      trusted = row.grant_state === 'consumed' && row.consumed_by_turn_id === turnId;
      if (!trusted) reason = 'next_turn_fence_mismatch';
    } else if (row.grant_kind === 'timed_conversation') {
      trusted = row.grant_state === 'active' && row.expires_at > checkedAt;
      if (!trusted) reason = row.expires_at <= checkedAt ? 'grant_expired' : 'grant_inactive';
    } else {
      trusted = row.grant_state === 'active';
      if (!trusted) reason = 'grant_inactive';
    }
  }
  const outcome = trusted ? 'trusted' : 'requires_approval';
  database.prepare(`
    INSERT INTO runtime_permission_action_decisions (
      decision_id, turn_id, action_ref, action_kind, outcome, basis_kind,
      grant_id, checked_policy_revision, checked_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).run(
    generateId('permission-decision'),
    turnId,
    actionRef,
    actionKind,
    outcome,
    row.basis_kind,
    row.grant_id,
    revision,
    checkedAt,
  );
  insertAudit(database, {
    generateId,
    action: trusted ? 'protected_action_allowed' : 'protected_action_recheck_required',
    actorId: row.actor_id,
    source: 'core_runtime',
    scope: {
      scope_kind: 'conversation',
      region: row.region,
      tenant_id: row.tenant_id,
      bot_id: row.bot_id,
      conversation_id: row.conversation_id,
    },
    policyRevision: revision,
    reason,
    committedAt: checkedAt,
    turnId,
    grantId: row.grant_id,
    actionRef,
    redactedContext: { action_kind: actionKind },
  });
  return Object.freeze({
    trusted,
    basis_kind: row.basis_kind,
    grant_id: row.grant_id,
    checked_policy_revision: revision,
    reason,
  });
}

function envelopeForStoredTarget(target, {
  traceId,
  actorId,
  source = 'core_runtime',
}) {
  return {
    trace_id: traceId,
    region: target.region,
    tenant_id: target.tenant_id,
    channel: target.channel,
    bot_id: target.bot_id,
    chat_type: target.chat_type,
    chat_id: target.chat_id,
    native_thread_or_topic_id: target.native_thread_or_topic_id,
    message_id: target.chat_type === 'thread'
      ? target.native_thread_reply_target_message_id
      : traceId,
    reply: {
      root_message_id: target.chat_type === 'thread'
        ? target.native_thread_root_message_id
        : null,
    },
    actor: {
      type: 'user',
      actor_id: actorId,
      authenticated: true,
      roles: [],
    },
    source: { kind: source },
  };
}

function expireTimedGrantInTransaction(database, grant, {
  expiredAt,
  generateId,
}) {
  const updated = database.prepare(`
    UPDATE runtime_permission_grants
    SET state = 'expired', expired_at = ?
    WHERE grant_id = ? AND state = 'active' AND expires_at <= ?
  `).run(expiredAt, grant.grant_id, expiredAt);
  if (updated.changes !== 1) return false;
  const expiryRevision = nextRevision(database);
  const scope = {
    scope_kind: 'conversation',
    region: grant.region,
    tenant_id: grant.tenant_id,
    bot_id: grant.bot_id,
    conversation_id: grant.conversation_id,
  };
  const auditId = insertAudit(database, {
    generateId,
    action: 'permission_expired',
    actorId: grant.issued_by_actor_id,
    source: 'core_timer',
    scope,
    policyRevision: expiryRevision,
    reason: 'timed_grant_deadline',
    committedAt: expiredAt,
    grantId: grant.grant_id,
    redactedContext: { grant_kind: grant.grant_kind },
  });

  const turns = database.prepare(`
    SELECT turn.*, lineage.provider, lineage.provider_native_id
    FROM runtime_turn_permissions AS permission
    JOIN runtime_turns AS turn ON turn.turn_id = permission.turn_id
    LEFT JOIN runtime_lineages AS lineage ON lineage.lineage_id = turn.lineage_id
    WHERE permission.grant_id = ?
  `).all(grant.grant_id);
  for (const turn of turns) {
    const lastEvent = database.prepare(`
      SELECT event_id, event_sequence
      FROM runtime_normalized_events
      WHERE turn_id = ?
      ORDER BY event_sequence DESC
      LIMIT 1
    `).get(turn.turn_id);
    const event = {
      contract: 'zylos.normalized-event',
      contract_version: '1.0',
      event_id: generateId('event'),
      trace_id: generateId('permission-trace'),
      conversation_id: turn.conversation_id,
      turn_id: turn.turn_id,
      lineage_id: turn.lineage_id,
      event_sequence: lastEvent.event_sequence + 1,
      turn_version: turn.turn_version + 1,
      attempt_id: turn.attempt_id,
      attempt_no: turn.attempt_no,
      lease_epoch: turn.lease_epoch,
      kind: 'permission_expired',
      phase: turn.state,
      occurred_at: expiredAt,
      persisted_at: expiredAt,
      provider: turn.provider,
      provider_native_id: turn.provider_native_id,
      payload: {
        scope,
        actor_id: grant.issued_by_actor_id,
        audit_id: auditId,
      },
      causation_event_id: lastEvent.event_id,
      error: null,
    };
    validateNormalizedEvent(event, { occurredAt: expiredAt });
    const turnUpdate = database.prepare(`
      UPDATE runtime_turns
      SET turn_version = ?
      WHERE turn_id = ? AND turn_version = ?
    `).run(event.turn_version, turn.turn_id, turn.turn_version);
    if (turnUpdate.changes !== 1) {
      throw new Error('Permission expiry lost the turn-version fence.');
    }
    database.prepare(`
      INSERT INTO runtime_normalized_events (
        event_id, turn_id, event_sequence, turn_version, event_json, persisted_at
      ) VALUES (?, ?, ?, ?, ?, ?)
    `).run(
      event.event_id,
      event.turn_id,
      event.event_sequence,
      event.turn_version,
      JSON.stringify(event),
      expiredAt,
    );
  }

  const target = JSON.parse(grant.notice_target_json);
  enqueueSecurityNotice(database, {
    envelope: envelopeForStoredTarget(target, {
      traceId: generateId('permission-trace'),
      actorId: grant.issued_by_actor_id,
    }),
    conversationId: grant.conversation_id,
    controlId: generateId('permission-expiry-control'),
    text: 'Timed trusted mode has expired; safe-mode checks now apply.',
    committedAt: expiredAt,
    generateId,
  });
  return true;
}

function expireConfirmationInTransaction(database, confirmation, {
  expiredAt,
  generateId,
}) {
  const request = JSON.parse(confirmation.request_json);
  const updatedRequest = {
    ...request,
    state: 'expired',
    version: request.version + 1,
  };
  validateInteractionRequest(updatedRequest, { occurredAt: expiredAt });
  const updated = database.prepare(`
    UPDATE runtime_permission_confirmations
    SET status = 'expired', resolved_at = ?
    WHERE control_id = ? AND status = 'pending' AND expires_at <= ?
  `).run(expiredAt, confirmation.control_id, expiredAt);
  if (updated.changes !== 1) return false;
  database.prepare(`
    UPDATE runtime_interactions
    SET state = 'expired', version = ?, request_json = ?, updated_at = ?
    WHERE interaction_id = ? AND state = 'pending'
  `).run(
    updatedRequest.version,
    JSON.stringify(updatedRequest),
    expiredAt,
    request.interaction_id,
  );
  const envelope = JSON.parse(confirmation.envelope_json);
  const revision = currentRevision(database);
  const finalOutcome = permissionControlOutcome({
    controlId: confirmation.control_id,
    status: 'expired',
    actionKind: confirmation.action_kind,
    actorId: confirmation.actor_id,
    source: 'core_timer',
    policyRevision: revision,
    grantId: confirmation.target_grant_id,
    reason: 'confirmation_deadline',
    resolvedAt: expiredAt,
  });
  database.prepare(`
    UPDATE runtime_permission_controls
    SET status = 'completed', final_result_json = ?
    WHERE control_id = ?
  `).run(JSON.stringify(finalOutcome), confirmation.control_id);
  insertAudit(database, {
    generateId,
    action: 'permission_confirmation_expired',
    actorId: confirmation.actor_id,
    source: 'core_timer',
    scope: permissionScope(envelope, confirmation.conversation_id, 'bot'),
    policyRevision: revision,
    reason: 'confirmation_deadline',
    committedAt: expiredAt,
    controlId: confirmation.control_id,
    redactedContext: { action_kind: confirmation.action_kind },
  });
  enqueueSecurityNotice(database, {
    envelope,
    conversationId: confirmation.conversation_id,
    controlId: generateId('permission-expiry-control'),
    text: 'The bot permission confirmation expired without changing bot-wide policy.',
    committedAt: expiredAt,
    generateId,
  });
  return true;
}

export function createPermissionService({
  database,
  now = () => new Date().toISOString(),
  generateId = defaultGenerateId,
} = {}) {
  if (!database) throw new TypeError('database is required');
  initializeRuntimePersistence(database);

  function authorizeProtectedAction({ turn_id: turnId, action_ref: actionRef, action_kind: actionKind }) {
    if (typeof turnId !== 'string' || typeof actionRef !== 'string' || typeof actionKind !== 'string') {
      throw new TypeError('turn_id, action_ref, and action_kind are required strings');
    }
    const apply = database.transaction(() => protectedActionDecision(database, {
      turnId,
      actionRef,
      actionKind,
      checkedAt: now(),
      generateId,
    }));
    return apply.immediate();
  }

  function submitConfirmation(answer) {
    const apply = database.transaction(() => {
      const row = database.prepare(`
        SELECT interaction.request_json, confirmation.*,
          inbound.envelope_json
        FROM runtime_interactions AS interaction
        JOIN runtime_permission_confirmations AS confirmation
          ON confirmation.interaction_id = interaction.interaction_id
        JOIN runtime_permission_controls AS control
          ON control.control_id = confirmation.control_id
        JOIN runtime_inbound_events AS inbound
          ON inbound.inbound_event_id = control.inbound_event_id
        WHERE interaction.interaction_id = ?
      `).get(answer.interaction_id);
      if (!row) {
        return interactionErrorResult(answer, commandError(
          'not_found',
          'The permission confirmation does not exist.',
          answer.answered_at,
          'validation',
        ));
      }
      return resolveConfirmationInTransaction(database, {
        answer,
        request: JSON.parse(row.request_json),
        confirmation: row,
        envelope: JSON.parse(row.envelope_json),
        committedAt: now(),
        generateId,
      });
    });
    try {
      return apply.immediate();
    } catch (error) {
      if (!(error instanceof ContractKernelError)) throw error;
      const requestRow = database.prepare(`
        SELECT request_json
        FROM runtime_interactions
        WHERE interaction_id = ?
      `).get(answer.interaction_id);
      return interactionErrorResult(
        answer,
        error.contractError,
        requestRow ? JSON.parse(requestRow.request_json) : null,
      );
    }
  }

  function handlesInteraction(interactionId) {
    if (typeof interactionId !== 'string') return false;
    return database.prepare(`
      SELECT 1 AS present
      FROM runtime_permission_confirmations
      WHERE interaction_id = ?
    `).get(interactionId)?.present === 1;
  }

  function expireDue() {
    const apply = database.transaction(() => {
      const expiredAt = now();
      const grants = database.prepare(`
        SELECT *
        FROM runtime_permission_grants
        WHERE grant_kind = 'timed_conversation' AND state = 'active'
          AND expires_at <= ?
        ORDER BY expires_at, grant_id
      `).all(expiredAt);
      let grantsExpired = 0;
      for (const grant of grants) {
        if (expireTimedGrantInTransaction(database, grant, { expiredAt, generateId })) {
          grantsExpired += 1;
        }
      }
      const confirmations = database.prepare(`
        SELECT confirmation.*, interaction.request_json, inbound.envelope_json
        FROM runtime_permission_confirmations AS confirmation
        JOIN runtime_interactions AS interaction
          ON interaction.interaction_id = confirmation.interaction_id
        JOIN runtime_permission_controls AS control
          ON control.control_id = confirmation.control_id
        JOIN runtime_inbound_events AS inbound
          ON inbound.inbound_event_id = control.inbound_event_id
        WHERE confirmation.status = 'pending' AND confirmation.expires_at <= ?
        ORDER BY confirmation.expires_at, confirmation.control_id
      `).all(expiredAt);
      let confirmationsExpired = 0;
      for (const confirmation of confirmations) {
        if (expireConfirmationInTransaction(database, confirmation, {
          expiredAt,
          generateId,
        })) {
          confirmationsExpired += 1;
        }
      }
      return Object.freeze({
        grants_expired: grantsExpired,
        confirmations_expired: confirmationsExpired,
      });
    });
    return apply.immediate();
  }

  return Object.freeze({
    authorizeProtectedAction,
    expireDue,
    handlesInteraction,
    submitConfirmation,
  });
}
