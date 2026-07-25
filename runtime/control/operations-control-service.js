import crypto from 'node:crypto';

import {
  canonicalizeJson,
  createContractError,
  validateControlRequest,
  validateControlResult,
  validatePublicFixtureSafety,
} from '../../contracts/public/index.js';
import { initializeRuntimePersistence } from '../persistence/schema.js';

const ACTION_CAPABILITIES = Object.freeze({
  inspect: 'runtime.inspect',
  stop_active_turn: 'turn.stop',
  clear_unstarted_queue: 'queue.clear',
  reconcile: 'service.reconcile',
  evict_idle_executor: 'executor.evict',
  confirm_recovery: 'recovery.decide',
  reject_recovery: 'recovery.decide',
});

function defaultGenerateId(kind) {
  return `${kind}-${crypto.randomUUID()}`;
}

function requireText(name, value) {
  if (typeof value !== 'string' || value.length === 0) {
    throw new TypeError(`${name} must be a non-empty string`);
  }
  return value;
}

function requireTimestamp(name, value) {
  requireText(name, value);
  if (!Number.isFinite(Date.parse(value))) throw new TypeError(`${name} must be an RFC 3339 timestamp`);
  return value;
}

function normalizePolicy(policy) {
  if (!policy || typeof policy !== 'object' || Array.isArray(policy)) {
    throw new TypeError('deploymentPolicy must be an object');
  }
  requireText('deploymentPolicy.policy_id', policy.policy_id);
  if (!Number.isSafeInteger(policy.policy_version) || policy.policy_version < 1) {
    throw new TypeError('deploymentPolicy.policy_version must be a positive safe integer');
  }
  if (!Array.isArray(policy.grants)) throw new TypeError('deploymentPolicy.grants must be an array');
  const grants = policy.grants.map((grant, index) => {
    if (!grant || typeof grant !== 'object' || Array.isArray(grant)) {
      throw new TypeError(`deploymentPolicy.grants[${index}] must be an object`);
    }
    requireText(`deploymentPolicy.grants[${index}].grant_id`, grant.grant_id);
    requireText(`deploymentPolicy.grants[${index}].subject.type`, grant.subject?.type);
    requireText(`deploymentPolicy.grants[${index}].subject.subject_id`, grant.subject?.subject_id);
    requireText(`deploymentPolicy.grants[${index}].capability`, grant.capability);
    if (!['active', 'revoked'].includes(grant.state)) {
      throw new TypeError(`deploymentPolicy.grants[${index}].state must be active or revoked`);
    }
    if (grant.expires_at !== null) requireTimestamp(
      `deploymentPolicy.grants[${index}].expires_at`,
      grant.expires_at,
    );
    return structuredClone(grant);
  });
  return Object.freeze({
    policy_id: policy.policy_id,
    policy_version: policy.policy_version,
    grants: Object.freeze(grants),
  });
}

function normalizeTrustedTransport(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new TypeError('trusted transport context is required');
  }
  if (!['dashboard_session', 'service_credential', 'platform_admin_bridge'].includes(value.source)) {
    throw new TypeError('trusted transport source is unsupported');
  }
  if (!['user', 'service'].includes(value.verified_subject?.type)) {
    throw new TypeError('trusted transport subject type must be user or service');
  }
  requireText('verified subject ID', value.verified_subject.subject_id);
  if (!Array.isArray(value.verified_subject.roles)) {
    throw new TypeError('trusted transport subject roles must be an array');
  }
  value.verified_subject.roles.forEach((role) => requireText('verified subject role', role));
  requireText('trusted policy ID', value.authorization_policy_id);
  if (!Number.isSafeInteger(value.authorization_policy_version)
    || value.authorization_policy_version < 1) {
    throw new TypeError('trusted policy version must be a positive safe integer');
  }
  requireTimestamp('trusted authentication time', value.authenticated_at);
  return structuredClone(value);
}

function policyHash(policy) {
  return crypto.createHash('sha256').update(canonicalizeJson(policy)).digest('hex');
}

function attemptHash(request, trusted) {
  const { actor: _actor, auth_context: _authContext, trace_id: _traceId, ...business } = request;
  return crypto.createHash('sha256').update(canonicalizeJson({
    request: business,
    verified_subject: trusted.verified_subject,
    trusted_policy_id: trusted.authorization_policy_id,
    trusted_policy_version: trusted.authorization_policy_version,
  })).digest('hex');
}

function scopeCovers(scope, authority) {
  if (scope.region !== authority.region || scope.tenant_id !== authority.tenant_id) return false;
  switch (scope.scope_type) {
    case 'tenant':
      return true;
    case 'bot':
      return authority.bot_id !== null && scope.bot_id === authority.bot_id;
    case 'conversation':
      return scope.bot_id === authority.bot_id
        && scope.conversation_id === authority.conversation_id;
    case 'service':
      return authority.aggregate_type === 'service'
        && scope.service_instance_id === authority.aggregate_id;
    case 'recovery':
      return authority.aggregate_type === 'recovery'
        && scope.bot_id === authority.bot_id
        && scope.conversation_id === authority.conversation_id
        && scope.recovery_id === authority.aggregate_id;
    default:
      return false;
  }
}

function targetAggregateId(target) {
  return target.service_instance_id
    ?? target.recovery_id
    ?? target.executor_instance_id
    ?? target.turn_id
    ?? target.conversation_id;
}

function scopeCouldCoverMissingTarget(scope, target) {
  switch (scope.scope_type) {
    case 'tenant':
      return true;
    case 'bot':
      return target.aggregate_type !== 'service';
    case 'conversation':
      return target.conversation_id === scope.conversation_id;
    case 'service':
      return target.aggregate_type === 'service'
        && target.service_instance_id === scope.service_instance_id;
    case 'recovery':
      return target.aggregate_type === 'recovery'
        && target.recovery_id === scope.recovery_id
        && target.conversation_id === scope.conversation_id;
    default:
      return false;
  }
}

function missingTargetAuthority(request, grant) {
  return {
    aggregate_type: request.target.aggregate_type,
    aggregate_id: targetAggregateId(request.target),
    version: null,
    region: grant.scope.region,
    tenant_id: grant.scope.tenant_id,
    bot_id: grant.scope.bot_id,
    conversation_id: request.target.conversation_id ?? grant.scope.conversation_id,
    snapshot: null,
  };
}

function loadConversationAuthority(database, conversationId) {
  const row = database.prepare(`
    SELECT conversation_id, region, tenant_id, bot_id, last_queue_sequence
    FROM runtime_conversations WHERE conversation_id = ?
  `).get(conversationId);
  if (!row) return null;
  return {
    aggregate_type: 'conversation',
    aggregate_id: row.conversation_id,
    version: 1,
    region: row.region,
    tenant_id: row.tenant_id,
    bot_id: row.bot_id,
    conversation_id: row.conversation_id,
    snapshot: {
      aggregate_type: 'conversation',
      conversation_id: row.conversation_id,
      region: row.region,
      tenant_id: row.tenant_id,
      bot_id: row.bot_id,
      queue_length: database.prepare(`
        SELECT COUNT(*) AS count FROM runtime_turn_queue
        WHERE conversation_id = ? AND status = 'queued'
      `).get(row.conversation_id).count,
    },
  };
}

function loadAuthority(database, request, serviceInstanceId, serviceNamespace) {
  if (request.action === 'inspect' && request.target.aggregate_type === 'conversation') {
    return loadConversationAuthority(database, request.target.conversation_id);
  }
  if (request.target.aggregate_type === 'queue') {
    const authority = loadConversationAuthority(database, request.target.conversation_id);
    if (!authority) return null;
    const row = database.prepare(`
      SELECT queue_version FROM runtime_conversations WHERE conversation_id = ?
    `).get(request.target.conversation_id);
    return {
      ...authority,
      aggregate_type: 'queue',
      version: row.queue_version,
      snapshot: {
        aggregate_type: 'queue',
        conversation_id: request.target.conversation_id,
        queue_version: row.queue_version,
      },
    };
  }
  if (request.target.aggregate_type === 'turn') {
    const row = database.prepare(`
      SELECT turn.turn_id, turn.conversation_id, turn.turn_version, turn.state,
        conversation.region, conversation.tenant_id, conversation.bot_id
      FROM runtime_turns AS turn
      JOIN runtime_conversations AS conversation
        ON conversation.conversation_id = turn.conversation_id
      WHERE turn.turn_id = ? AND turn.conversation_id = ?
    `).get(request.target.turn_id, request.target.conversation_id);
    if (!row) return null;
    return {
      aggregate_type: 'turn',
      aggregate_id: row.turn_id,
      version: row.turn_version,
      region: row.region,
      tenant_id: row.tenant_id,
      bot_id: row.bot_id,
      conversation_id: row.conversation_id,
      snapshot: {
        aggregate_type: 'turn',
        conversation_id: row.conversation_id,
        turn_id: row.turn_id,
        turn_version: row.turn_version,
        state: row.state,
      },
    };
  }
  if (request.target.aggregate_type === 'executor') {
    const resident = database.prepare(`
      SELECT resident.owner_epoch, conversation.region, conversation.tenant_id,
        conversation.bot_id
      FROM runtime_executor_residents AS resident
      JOIN runtime_conversations AS conversation
        ON conversation.conversation_id = resident.conversation_id
      WHERE resident.conversation_id = ?
    `).get(request.target.conversation_id);
    const attempt = database.prepare(`
      SELECT executor_instance_id, attempt_no
      FROM runtime_provider_attempts
      WHERE conversation_id = ?
      ORDER BY updated_at DESC, attempt_no DESC
      LIMIT 1
    `).get(request.target.conversation_id);
    const active = database.prepare(`
      SELECT turn_version FROM runtime_turns
      WHERE conversation_id = ?
        AND state IN ('starting', 'running', 'waiting_user', 'redirecting', 'recovering')
      LIMIT 1
    `).get(request.target.conversation_id);
    if (!resident || attempt?.executor_instance_id !== request.target.executor_instance_id) {
      return null;
    }
    const executorVersion = Math.max(
      1,
      resident.owner_epoch ?? 0,
      active?.turn_version ?? 0,
      attempt.attempt_no ?? 0,
    );
    return {
      aggregate_type: 'executor',
      aggregate_id: attempt.executor_instance_id,
      version: executorVersion,
      region: resident.region,
      tenant_id: resident.tenant_id,
      bot_id: resident.bot_id,
      conversation_id: request.target.conversation_id,
      snapshot: {
        aggregate_type: 'executor',
        conversation_id: request.target.conversation_id,
        executor_instance_id: attempt.executor_instance_id,
        executor_version: executorVersion,
      },
    };
  }
  if (request.target.aggregate_type === 'recovery') {
    const execution = database.prepare(`
      SELECT recovery.recovery_id, recovery.recovery_version, recovery.state,
        interaction.interaction_id, turn.turn_id, turn.conversation_id,
        conversation.region, conversation.tenant_id, conversation.bot_id,
        interaction.request_json
      FROM runtime_execution_recoveries AS recovery
      JOIN runtime_turns AS turn ON turn.turn_id = recovery.turn_id
      JOIN runtime_conversations AS conversation
        ON conversation.conversation_id = turn.conversation_id
      JOIN runtime_interactions AS interaction
        ON interaction.interaction_id = recovery.interaction_id
      WHERE recovery.recovery_id = ? AND turn.turn_id = ? AND turn.conversation_id = ?
    `).get(
      request.target.recovery_id,
      request.target.turn_id,
      request.target.conversation_id,
    );
    const row = execution ?? database.prepare(`
      SELECT recovery.recovery_id, recovery.recovery_version, recovery.state,
        interaction.interaction_id, turn.turn_id, turn.conversation_id,
        conversation.region, conversation.tenant_id, conversation.bot_id,
        interaction.request_json
      FROM runtime_reply_mapping_recoveries AS recovery
      JOIN runtime_turns AS turn ON turn.turn_id = recovery.turn_id
      JOIN runtime_conversations AS conversation
        ON conversation.conversation_id = turn.conversation_id
      JOIN runtime_interactions AS interaction
        ON interaction.parent_type = 'recovery_control'
        AND interaction.parent_id = recovery.recovery_id
      WHERE recovery.recovery_id = ? AND turn.turn_id = ? AND turn.conversation_id = ?
    `).get(
      request.target.recovery_id,
      request.target.turn_id,
      request.target.conversation_id,
    );
    if (!row) return null;
    const recoveryRequest = JSON.parse(row.request_json);
    return {
      aggregate_type: 'recovery',
      aggregate_id: row.recovery_id,
      version: row.recovery_version,
      region: row.region,
      tenant_id: row.tenant_id,
      bot_id: row.bot_id,
      conversation_id: row.conversation_id,
      authorized_subjects: recoveryRequest.authorized_subjects,
      snapshot: {
        aggregate_type: 'recovery',
        recovery_id: row.recovery_id,
        conversation_id: row.conversation_id,
        turn_id: row.turn_id,
        recovery_version: row.recovery_version,
        state: row.state,
      },
    };
  }
  if (request.target.aggregate_type === 'service'
    && request.target.service_instance_id === serviceInstanceId) {
    const row = database.prepare(`
      SELECT service_instance_id, service_version
      FROM runtime_observability_instances WHERE service_instance_id = ?
    `).get(serviceInstanceId);
    if (!row) return null;
    return {
      aggregate_type: 'service',
      aggregate_id: serviceInstanceId,
      version: row.service_version,
      region: serviceNamespace?.region ?? null,
      tenant_id: serviceNamespace?.tenant_id ?? null,
      bot_id: null,
      conversation_id: null,
      snapshot: { aggregate_type: 'service', service_instance_id: serviceInstanceId },
    };
  }
  return null;
}

function normalizeRequest(request, trusted, policy, grant, authority) {
  const normalized = structuredClone(request);
  normalized.actor = {
    type: trusted.verified_subject.type,
    actor_id: trusted.verified_subject.subject_id,
    authenticated: true,
    roles: [...trusted.verified_subject.roles],
    capabilities: [{
      capability: grant.capability,
      scope: structuredClone(grant.scope),
      policy_id: policy.policy_id,
      policy_version: policy.policy_version,
      grant_id: grant.grant_id,
      expires_at: grant.expires_at,
    }],
  };
  normalized.auth_context = {
    source: trusted.source,
    auth_subject_id: trusted.verified_subject.subject_id,
    tenant_id: authority.tenant_id,
    bot_id: authority.bot_id,
    authorization_policy_id: policy.policy_id,
    authorization_policy_version: policy.policy_version,
    authenticated_at: trusted.authenticated_at,
  };
  return normalized;
}

export function createOperationsControlService({
  database,
  serviceInstanceId,
  deploymentPolicy,
  runtimeStore = null,
  serviceNamespace = null,
  now = () => new Date().toISOString(),
  generateId = defaultGenerateId,
} = {}) {
  if (!database || typeof database.transaction !== 'function') {
    throw new TypeError('database must be a better-sqlite3 connection');
  }
  requireText('serviceInstanceId', serviceInstanceId);
  if (typeof now !== 'function') throw new TypeError('now must be a function');
  if (typeof generateId !== 'function') throw new TypeError('generateId must be a function');
  if (serviceNamespace !== null && (
    typeof serviceNamespace !== 'object'
    || Array.isArray(serviceNamespace)
    || typeof serviceNamespace.region !== 'string'
    || serviceNamespace.region.length === 0
    || typeof serviceNamespace.tenant_id !== 'string'
    || serviceNamespace.tenant_id.length === 0
  )) {
    throw new TypeError('serviceNamespace must contain region and tenant_id');
  }
  initializeRuntimePersistence(database);
  const policy = normalizePolicy(deploymentPolicy);
  const registeredAt = requireTimestamp('policy registration time', now());
  const artifactHash = policyHash(policy);
  const existingPolicy = database.prepare(`
    SELECT artifact_hash FROM runtime_operations_policies
    WHERE policy_id = ? AND policy_version = ?
  `).get(policy.policy_id, policy.policy_version);
  if (existingPolicy && existingPolicy.artifact_hash !== artifactHash) {
    throw new TypeError('deployment policy content changed without a version change');
  }
  database.prepare(`
    INSERT OR IGNORE INTO runtime_operations_policies (
      policy_id, policy_version, artifact_hash, policy_json, registered_at
    ) VALUES (?, ?, ?, ?, ?)
  `).run(
    policy.policy_id,
    policy.policy_version,
    artifactHash,
    canonicalizeJson(policy),
    registeredAt,
  );

  function persistForbidden(request, trusted, committedAt, userMessage) {
    const requestHash = attemptHash(request, trusted);
    const existing = database.prepare(`
      SELECT request_hash, latest_result_json
      FROM runtime_operations_controls
      WHERE caller_namespace = ? AND control_id = ?
    `).get(request.caller_namespace, request.control_id);
    if (existing) {
      if (existing.request_hash !== requestHash) {
        const recorded = database.prepare(`
          SELECT result_json FROM runtime_operations_idempotency_conflicts
          WHERE caller_namespace = ? AND control_id = ? AND request_hash = ?
        `).get(request.caller_namespace, request.control_id, requestHash);
        if (recorded) return JSON.parse(recorded.result_json);
        const auditId = generateId('operations-audit');
        const error = createContractError({
          code: 'idempotency_conflict',
          category: 'conflict',
          retryable: false,
          userMessage: 'The control ID was reused with a different business payload.',
          occurredAt: committedAt,
        });
        const conflictResult = validateControlResult({
          contract: 'zylos.control-result',
          contract_version: '1.0',
          trace_id: request.trace_id,
          caller_namespace: request.caller_namespace,
          control_id: request.control_id,
          control_result_version: 1,
          status: 'conflict',
          target: structuredClone(request.target),
          previous_target_version: null,
          target_version: null,
          audit_id: auditId,
          result: null,
          error,
          accepted_at: null,
          completed_at: committedAt,
        }, { occurredAt: committedAt, action: request.action }).forwarded;
        database.prepare(`
          INSERT INTO runtime_operations_idempotency_conflicts (
            caller_namespace, control_id, request_hash, result_json, audit_id, committed_at
          ) VALUES (?, ?, ?, ?, ?, ?)
        `).run(
          request.caller_namespace,
          request.control_id,
          requestHash,
          canonicalizeJson(conflictResult),
          auditId,
          committedAt,
        );
        database.prepare(`
          INSERT INTO runtime_operations_audit (
            audit_id, caller_namespace, control_id, action, outcome,
            subject_type, subject_id, capability, grant_id, policy_id, policy_version,
            target_json, expected_version_json, previous_target_version, target_version,
            reason, error_json, committed_at
          ) VALUES (?, ?, ?, ?, 'conflict', ?, ?, NULL, NULL, ?, ?, ?, ?, NULL, NULL, ?, ?, ?)
        `).run(
          auditId,
          request.caller_namespace,
          request.control_id,
          request.action,
          trusted.verified_subject.type,
          trusted.verified_subject.subject_id,
          trusted.authorization_policy_id,
          trusted.authorization_policy_version,
          canonicalizeJson(request.target),
          request.expected_version === null ? null : canonicalizeJson(request.expected_version),
          request.reason,
          canonicalizeJson(error),
          committedAt,
        );
        return conflictResult;
      }
      return JSON.parse(existing.latest_result_json);
    }
    const auditId = generateId('operations-audit');
    const error = createContractError({
      code: 'forbidden',
      category: 'authorization',
      userMessage,
      occurredAt: committedAt,
    });
    const result = validateControlResult({
      contract: 'zylos.control-result',
      contract_version: '1.0',
      trace_id: request.trace_id,
      caller_namespace: request.caller_namespace,
      control_id: request.control_id,
      control_result_version: 1,
      status: 'forbidden',
      target: structuredClone(request.target),
      previous_target_version: null,
      target_version: null,
      audit_id: auditId,
      result: null,
      error,
      accepted_at: null,
      completed_at: committedAt,
    }, { occurredAt: committedAt, action: request.action }).forwarded;
    const normalizedAttempt = {
      contract: request.contract,
      contract_version: request.contract_version,
      caller_namespace: request.caller_namespace,
      control_id: request.control_id,
      action: request.action,
      target: structuredClone(request.target),
      expected_version: structuredClone(request.expected_version),
      actor: {
        type: trusted.verified_subject.type,
        actor_id: trusted.verified_subject.subject_id,
        authenticated: true,
        roles: [...trusted.verified_subject.roles],
        capabilities: [],
      },
      auth_context: {
        source: trusted.source,
        auth_subject_id: trusted.verified_subject.subject_id,
        authorization_policy_id: trusted.authorization_policy_id,
        authorization_policy_version: trusted.authorization_policy_version,
        authenticated_at: trusted.authenticated_at,
      },
      reason: request.reason,
      created_at: request.created_at,
    };
    database.prepare(`
      INSERT INTO runtime_operations_controls (
        caller_namespace, control_id, action, target_json, request_hash,
        normalized_request_json, control_result_version, latest_result_json,
        audit_id, created_at, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?)
    `).run(
      request.caller_namespace,
      request.control_id,
      request.action,
      canonicalizeJson(request.target),
      requestHash,
      canonicalizeJson(normalizedAttempt),
      canonicalizeJson(result),
      auditId,
      committedAt,
      committedAt,
    );
    database.prepare(`
      INSERT INTO runtime_operations_audit (
        audit_id, caller_namespace, control_id, action, outcome,
        subject_type, subject_id, capability, grant_id, policy_id, policy_version,
        target_json, expected_version_json, previous_target_version, target_version,
        reason, error_json, committed_at
      ) VALUES (?, ?, ?, ?, 'forbidden', ?, ?, NULL, NULL, ?, ?, ?, ?, NULL, NULL, ?, ?, ?)
    `).run(
      auditId,
      request.caller_namespace,
      request.control_id,
      request.action,
      trusted.verified_subject.type,
      trusted.verified_subject.subject_id,
      trusted.authorization_policy_id,
      trusted.authorization_policy_version,
      canonicalizeJson(request.target),
      request.expected_version === null ? null : canonicalizeJson(request.expected_version),
      request.reason,
      canonicalizeJson(error),
      committedAt,
    );
    return result;
  }

  function persistVersionConflict(request, trusted, committedAt, userMessage) {
    return database.transaction(() => {
      const authority = loadAuthority(database, request, serviceInstanceId, serviceNamespace);
      if (!authority) throw new TypeError('control target does not exist');
      const requiredCapability = ACTION_CAPABILITIES[request.action];
      const grant = policy.grants.find((candidate) => (
        candidate.subject.type === trusted.verified_subject.type
        && candidate.subject.subject_id === trusted.verified_subject.subject_id
        && candidate.capability === requiredCapability
        && candidate.state === 'active'
        && (candidate.expires_at === null
          || Date.parse(candidate.expires_at) > Date.parse(committedAt))
        && scopeCovers(candidate.scope, authority)
      ));
      if (!grant) {
        return persistForbidden(
          request,
          trusted,
          committedAt,
          'The verified subject has no current covering operations grant.',
        );
      }
      const normalized = normalizeRequest(request, trusted, policy, grant, authority);
      validateControlRequest(normalized, { occurredAt: committedAt });
      const requestHash = attemptHash(request, trusted);
      const existing = database.prepare(`
        SELECT request_hash, latest_result_json
        FROM runtime_operations_controls
        WHERE caller_namespace = ? AND control_id = ?
      `).get(normalized.caller_namespace, normalized.control_id);
      if (existing) {
        if (existing.request_hash !== requestHash) {
          throw new TypeError('control idempotency key was reused with a different payload');
        }
        return JSON.parse(existing.latest_result_json);
      }
      const auditId = generateId('operations-audit');
      const error = createContractError({
        code: 'version_conflict',
        category: 'conflict',
        retryable: true,
        userMessage,
        occurredAt: committedAt,
      });
      const result = validateControlResult({
        contract: 'zylos.control-result',
        contract_version: '1.0',
        trace_id: normalized.trace_id,
        caller_namespace: normalized.caller_namespace,
        control_id: normalized.control_id,
        control_result_version: 1,
        status: 'conflict',
        target: structuredClone(normalized.target),
        previous_target_version: authority.version,
        target_version: authority.version,
        audit_id: auditId,
        result: null,
        error,
        accepted_at: null,
        completed_at: committedAt,
      }, { occurredAt: committedAt, action: normalized.action }).forwarded;
      database.prepare(`
        INSERT INTO runtime_operations_controls (
          caller_namespace, control_id, action, target_json, request_hash,
          normalized_request_json, control_result_version, latest_result_json,
          audit_id, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?)
      `).run(
        normalized.caller_namespace,
        normalized.control_id,
        normalized.action,
        canonicalizeJson(normalized.target),
        requestHash,
        canonicalizeJson(normalized),
        canonicalizeJson(result),
        auditId,
        committedAt,
        committedAt,
      );
      database.prepare(`
        INSERT INTO runtime_operations_audit (
          audit_id, caller_namespace, control_id, action, outcome,
          subject_type, subject_id, capability, grant_id, policy_id, policy_version,
          target_json, expected_version_json, previous_target_version, target_version,
          reason, error_json, committed_at
        ) VALUES (?, ?, ?, ?, 'conflict', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `).run(
        auditId,
        normalized.caller_namespace,
        normalized.control_id,
        normalized.action,
        trusted.verified_subject.type,
        trusted.verified_subject.subject_id,
        grant.capability,
        grant.grant_id,
        policy.policy_id,
        policy.policy_version,
        canonicalizeJson(normalized.target),
        canonicalizeJson(normalized.expected_version),
        authority.version,
        authority.version,
        normalized.reason,
        canonicalizeJson(error),
        committedAt,
      );
      return result;
    }).immediate();
  }

  function persistNotFound(request, trusted, grant, authority, committedAt) {
    const normalized = normalizeRequest(request, trusted, policy, grant, authority);
    validateControlRequest(normalized, { occurredAt: committedAt });
    const requestHash = attemptHash(request, trusted);
    const existing = database.prepare(`
      SELECT request_hash, latest_result_json
      FROM runtime_operations_controls
      WHERE caller_namespace = ? AND control_id = ?
    `).get(normalized.caller_namespace, normalized.control_id);
    if (existing) {
      if (existing.request_hash !== requestHash) {
        return persistIdempotencyConflict({
          normalized,
          trusted,
          grant,
          authority,
          requestHash,
          committedAt,
        });
      }
      return JSON.parse(existing.latest_result_json);
    }
    const auditId = generateId('operations-audit');
    const error = createContractError({
      code: 'not_found',
      category: 'conflict',
      retryable: false,
      userMessage: 'The canonical operations target does not exist.',
      occurredAt: committedAt,
    });
    const result = validateControlResult({
      contract: 'zylos.control-result',
      contract_version: '1.0',
      trace_id: normalized.trace_id,
      caller_namespace: normalized.caller_namespace,
      control_id: normalized.control_id,
      control_result_version: 1,
      status: 'not_found',
      target: structuredClone(normalized.target),
      previous_target_version: null,
      target_version: null,
      audit_id: auditId,
      result: null,
      error,
      accepted_at: null,
      completed_at: committedAt,
    }, { occurredAt: committedAt, action: normalized.action }).forwarded;
    database.prepare(`
      INSERT INTO runtime_operations_controls (
        caller_namespace, control_id, action, target_json, request_hash,
        normalized_request_json, control_result_version, latest_result_json,
        audit_id, created_at, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?)
    `).run(
      normalized.caller_namespace,
      normalized.control_id,
      normalized.action,
      canonicalizeJson(normalized.target),
      requestHash,
      canonicalizeJson(normalized),
      canonicalizeJson(result),
      auditId,
      committedAt,
      committedAt,
    );
    database.prepare(`
      INSERT INTO runtime_operations_audit (
        audit_id, caller_namespace, control_id, action, outcome,
        subject_type, subject_id, capability, grant_id, policy_id, policy_version,
        target_json, expected_version_json, previous_target_version, target_version,
        reason, error_json, committed_at
      ) VALUES (?, ?, ?, ?, 'not_found', ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?, ?)
    `).run(
      auditId,
      normalized.caller_namespace,
      normalized.control_id,
      normalized.action,
      trusted.verified_subject.type,
      trusted.verified_subject.subject_id,
      grant.capability,
      grant.grant_id,
      policy.policy_id,
      policy.policy_version,
      canonicalizeJson(normalized.target),
      normalized.expected_version === null ? null : canonicalizeJson(normalized.expected_version),
      normalized.reason,
      canonicalizeJson(error),
      committedAt,
    );
    return result;
  }

  function persistIdempotencyConflict({
    normalized,
    trusted,
    grant,
    authority,
    requestHash,
    committedAt,
  }) {
    const existing = database.prepare(`
      SELECT result_json FROM runtime_operations_idempotency_conflicts
      WHERE caller_namespace = ? AND control_id = ? AND request_hash = ?
    `).get(normalized.caller_namespace, normalized.control_id, requestHash);
    if (existing) return JSON.parse(existing.result_json);
    const auditId = generateId('operations-audit');
    const error = createContractError({
      code: 'idempotency_conflict',
      category: 'conflict',
      retryable: false,
      userMessage: 'The control ID was reused with a different business payload.',
      occurredAt: committedAt,
    });
    const result = validateControlResult({
      contract: 'zylos.control-result',
      contract_version: '1.0',
      trace_id: normalized.trace_id,
      caller_namespace: normalized.caller_namespace,
      control_id: normalized.control_id,
      control_result_version: 1,
      status: 'conflict',
      target: structuredClone(normalized.target),
      previous_target_version: authority.version,
      target_version: authority.version,
      audit_id: auditId,
      result: null,
      error,
      accepted_at: null,
      completed_at: committedAt,
    }, { occurredAt: committedAt, action: normalized.action }).forwarded;
    database.prepare(`
      INSERT INTO runtime_operations_idempotency_conflicts (
        caller_namespace, control_id, request_hash, result_json, audit_id, committed_at
      ) VALUES (?, ?, ?, ?, ?, ?)
    `).run(
      normalized.caller_namespace,
      normalized.control_id,
      requestHash,
      canonicalizeJson(result),
      auditId,
      committedAt,
    );
    database.prepare(`
      INSERT INTO runtime_operations_audit (
        audit_id, caller_namespace, control_id, action, outcome,
        subject_type, subject_id, capability, grant_id, policy_id, policy_version,
        target_json, expected_version_json, previous_target_version, target_version,
        reason, error_json, committed_at
      ) VALUES (?, ?, ?, ?, 'conflict', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      auditId,
      normalized.caller_namespace,
      normalized.control_id,
      normalized.action,
      trusted.verified_subject.type,
      trusted.verified_subject.subject_id,
      grant.capability,
      grant.grant_id,
      policy.policy_id,
      policy.policy_version,
      canonicalizeJson(normalized.target),
      normalized.expected_version === null ? null : canonicalizeJson(normalized.expected_version),
      authority.version,
      authority.version,
      normalized.reason,
      canonicalizeJson(error),
      committedAt,
    );
    return result;
  }

  async function execute(request, trustedTransportContext) {
    const trusted = normalizeTrustedTransport(trustedTransportContext);
    const committedAt = requireTimestamp('control time', now());
    try {
      return database.transaction(() => {
        validatePublicFixtureSafety(request, { occurredAt: committedAt });
        if (
          trusted.authorization_policy_id !== policy.policy_id
          || trusted.authorization_policy_version !== policy.policy_version
        ) {
          return persistForbidden(
            request,
            trusted,
            committedAt,
            'The trusted transport policy version is not current.',
          );
        }
        const requiredCapability = ACTION_CAPABILITIES[request.action];
        const eligibleGrant = (candidate) => (
          candidate.subject.type === trusted.verified_subject.type
          && candidate.subject.subject_id === trusted.verified_subject.subject_id
          && candidate.capability === requiredCapability
          && candidate.state === 'active'
          && (candidate.expires_at === null
            || Date.parse(candidate.expires_at) > Date.parse(committedAt))
        );
        const authority = loadAuthority(database, request, serviceInstanceId, serviceNamespace);
        if (!authority) {
          const missingGrant = policy.grants.find((candidate) => (
            eligibleGrant(candidate)
            && scopeCouldCoverMissingTarget(candidate.scope, request.target)
          ));
          if (!missingGrant) {
            return persistForbidden(
              request,
              trusted,
              committedAt,
              'The verified subject has no current covering operations grant.',
            );
          }
          return persistNotFound(
            request,
            trusted,
            missingGrant,
            missingTargetAuthority(request, missingGrant),
            committedAt,
          );
        }
        const grant = policy.grants.find((candidate) => (
          eligibleGrant(candidate) && scopeCovers(candidate.scope, authority)
        ));
        if (!grant) {
          return persistForbidden(
            request,
            trusted,
            committedAt,
            'The verified subject has no current covering operations grant.',
          );
        }
        if (['confirm_recovery', 'reject_recovery'].includes(request.action)) {
          const subjectAuthorized = authority.authorized_subjects.some((subject) => (
            subject.type === 'actor'
              ? subject.actor_id === trusted.verified_subject.subject_id
              : subject.type === 'capability'
                && subject.capability === grant.capability
                && scopeCovers(subject.scope, authority)
          ));
          if (!subjectAuthorized) {
            return persistForbidden(
              request,
              trusted,
              committedAt,
              'The verified subject is not authorized for this recovery decision.',
            );
          }
        }

        const normalized = normalizeRequest(request, trusted, policy, grant, authority);
        validateControlRequest(normalized, { occurredAt: committedAt });
        const requestHash = attemptHash(request, trusted);
      const existing = database.prepare(`
        SELECT request_hash, latest_result_json
        FROM runtime_operations_controls
        WHERE caller_namespace = ? AND control_id = ?
      `).get(normalized.caller_namespace, normalized.control_id);
      if (existing) {
        if (existing.request_hash !== requestHash) {
          return persistIdempotencyConflict({
            normalized,
            trusted,
            grant,
            authority,
            requestHash,
            committedAt,
          });
        }
        return JSON.parse(existing.latest_result_json);
      }

      const auditId = generateId('operations-audit');
      let actionResult = { snapshot: authority.snapshot };
      let previousTargetVersion = authority.version;
      let targetVersion = authority.version;
      let controlStatus = 'completed';
      let completedAt = committedAt;
      if (normalized.action === 'clear_unstarted_queue') {
        if (!runtimeStore || typeof runtimeStore.clearUnstartedQueue !== 'function') {
          throw new TypeError('runtimeStore.clearUnstartedQueue is required for queue clear');
        }
        const cleared = runtimeStore.clearUnstartedQueue({
          conversation_id: normalized.target.conversation_id,
          through_queue_sequence: normalized.target.through_queue_sequence,
          expected_queue_version: normalized.expected_version.version,
        });
        actionResult = {
          cleared_turn_ids: cleared.cleared_turn_ids,
          through_queue_sequence: cleared.through_queue_sequence,
        };
        previousTargetVersion = cleared.previous_version;
        targetVersion = cleared.queue_version;
      } else if (normalized.action === 'stop_active_turn') {
        if (!runtimeStore || typeof runtimeStore.stopConversation !== 'function') {
          throw new TypeError('runtimeStore.stopConversation is required for active-turn stop');
        }
        const stopped = runtimeStore.stopConversation({
          conversation_id: normalized.target.conversation_id,
          stop_id: normalized.control_id,
          target_turn_id: normalized.target.turn_id,
          expected_turn_version: normalized.expected_version.version,
          clear_unstarted_queue: false,
        });
        actionResult = {
          winner: stopped.active_turn?.previous_state === 'redirecting' ? 'steer' : 'stop',
          active_turn_id: stopped.active_turn.turn_id,
          active_turn_version: stopped.active_turn.turn_version,
          priority_turn_created: stopped.steering !== null,
          priority_turn_cancelled: stopped.steering?.priority_turn?.status === 'cancelled',
        };
        previousTargetVersion = stopped.active_turn.previous_version;
        targetVersion = stopped.active_turn.turn_version;
      } else if (normalized.action === 'reconcile') {
        const intentId = generateId('reconciliation-intent');
        database.prepare(`
          INSERT INTO runtime_operations_reconciliation_intents (
            intent_id, service_instance_id, caller_namespace, expected_service_version,
            state, control_id, created_at, updated_at
          ) VALUES (?, ?, ?, ?, 'pending', ?, ?, ?)
        `).run(
          intentId,
          serviceInstanceId,
          normalized.caller_namespace,
          normalized.expected_version.version,
          normalized.control_id,
          committedAt,
          committedAt,
        );
        const advanced = database.prepare(`
          UPDATE runtime_observability_instances
          SET service_version = service_version + 1, updated_at = ?
          WHERE service_instance_id = ? AND service_version = ?
        `).run(
          committedAt,
          serviceInstanceId,
          normalized.expected_version.version,
        );
        if (advanced.changes !== 1) {
          const error = new Error('The service aggregate changed before reconciliation.');
          error.code = 'version_conflict';
          throw error;
        }
        actionResult = { intent_id: intentId, state: 'pending' };
        previousTargetVersion = normalized.expected_version.version;
        targetVersion = normalized.expected_version.version + 1;
        controlStatus = 'accepted';
        completedAt = null;
      } else if (normalized.action === 'evict_idle_executor') {
        if (!runtimeStore
          || typeof runtimeStore.evictIdleExecutor !== 'function'
          || typeof runtimeStore.isConversationEvictable !== 'function') {
          throw new TypeError('runtimeStore eviction controls are required for eviction');
        }
        if (authority.version !== normalized.expected_version.version
          || !runtimeStore.isConversationEvictable(normalized.target.conversation_id)) {
          const error = new Error('The executor aggregate changed before eviction.');
          error.code = 'version_conflict';
          throw error;
        }
        actionResult = {
          evicted: false,
          executor_instance_id: normalized.target.executor_instance_id,
        };
        controlStatus = 'accepted';
        completedAt = null;
      } else if (['confirm_recovery', 'reject_recovery'].includes(normalized.action)) {
        if (!runtimeStore || typeof runtimeStore.decideRecovery !== 'function') {
          throw new TypeError('runtimeStore.decideRecovery is required for recovery decisions');
        }
        const decision = normalized.action === 'confirm_recovery' ? 'confirmed' : 'rejected';
        const decided = runtimeStore.decideRecovery({
          recovery_id: normalized.target.recovery_id,
          conversation_id: normalized.target.conversation_id,
          turn_id: normalized.target.turn_id,
          expected_recovery_version: normalized.expected_version.version,
          decision,
        });
        actionResult = {
          decision,
          recovery_turn_id: decided.recovery_turn_id,
        };
        previousTargetVersion = decided.previous_version;
        targetVersion = decided.recovery_version;
      }
      const result = {
        contract: 'zylos.control-result',
        contract_version: '1.0',
        trace_id: normalized.trace_id,
        caller_namespace: normalized.caller_namespace,
        control_id: normalized.control_id,
        control_result_version: 1,
        status: controlStatus,
        target: structuredClone(normalized.target),
        previous_target_version: previousTargetVersion,
        target_version: targetVersion,
        audit_id: auditId,
        result: actionResult,
        error: null,
        accepted_at: committedAt,
        completed_at: completedAt,
      };
      const forwarded = validateControlResult(result, {
        occurredAt: committedAt,
        action: normalized.action,
      }).forwarded;
      database.prepare(`
        INSERT INTO runtime_operations_controls (
          caller_namespace, control_id, action, target_json, request_hash,
          normalized_request_json, control_result_version, latest_result_json,
          audit_id, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?)
      `).run(
        normalized.caller_namespace,
        normalized.control_id,
        normalized.action,
        canonicalizeJson(normalized.target),
        requestHash,
        canonicalizeJson(normalized),
        canonicalizeJson(forwarded),
        auditId,
        committedAt,
        committedAt,
      );
      database.prepare(`
        INSERT INTO runtime_operations_audit (
          audit_id, caller_namespace, control_id, action, outcome,
          subject_type, subject_id, capability, grant_id, policy_id, policy_version,
          target_json, expected_version_json, previous_target_version, target_version,
          reason, error_json, committed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?)
      `).run(
        auditId,
        normalized.caller_namespace,
        normalized.control_id,
        normalized.action,
        controlStatus,
        trusted.verified_subject.type,
        trusted.verified_subject.subject_id,
        grant.capability,
        grant.grant_id,
        policy.policy_id,
        policy.policy_version,
        canonicalizeJson(normalized.target),
        normalized.expected_version === null ? null : canonicalizeJson(normalized.expected_version),
        previousTargetVersion,
        targetVersion,
        normalized.reason,
        committedAt,
      );
      return forwarded;
      }).immediate();
    } catch (error) {
      if (error?.code === 'version_conflict') {
        return persistVersionConflict(request, trusted, committedAt, error.message);
      }
      throw error;
    }
  }

  function completeEviction(request, providerError = null) {
    const completedAt = requireTimestamp('eviction completion time', now());
    return database.transaction(() => {
      const row = database.prepare(`
        SELECT normalized_request_json, latest_result_json, audit_id
        FROM runtime_operations_controls
        WHERE caller_namespace = ? AND control_id = ? AND action = 'evict_idle_executor'
      `).get(request.caller_namespace, request.control_id);
      if (!row) throw new TypeError('eviction control intent does not exist');
      const normalized = JSON.parse(row.normalized_request_json);
      const current = JSON.parse(row.latest_result_json);
      if (current.status !== 'accepted') return current;
      let status;
      let result;
      let error;
      let targetVersion = current.target_version;
      if (providerError !== null) {
        status = 'failed';
        result = null;
        const unsupported = providerError.code === 'unsupported_capability'
          && providerError.side_effect_status === 'none';
        error = createContractError(unsupported ? {
          code: 'unsupported_capability',
          category: 'internal',
          retryable: false,
          sideEffectStatus: 'none',
          userMessage: 'The provider adapter does not support executor eviction.',
          occurredAt: completedAt,
        } : {
          code: 'side_effect_unknown',
          category: 'provider',
          retryable: false,
          sideEffectStatus: 'unknown',
          userMessage: 'Provider executor eviction could not be proven.',
          occurredAt: completedAt,
        });
      } else {
        const evicted = runtimeStore.evictIdleExecutor({
          conversation_id: normalized.target.conversation_id,
          executor_instance_id: normalized.target.executor_instance_id,
          expected_executor_version: normalized.expected_version.version,
        });
        status = 'completed';
        result = {
          evicted: evicted.evicted,
          executor_instance_id: evicted.executor_instance_id,
        };
        error = null;
        targetVersion = evicted.executor_version;
      }
      const terminal = validateControlResult({
        ...current,
        control_result_version: current.control_result_version + 1,
        status,
        target_version: targetVersion,
        result,
        error,
        completed_at: completedAt,
      }, { occurredAt: completedAt, action: 'evict_idle_executor' }).forwarded;
      const updated = database.prepare(`
        UPDATE runtime_operations_controls
        SET control_result_version = ?, latest_result_json = ?, updated_at = ?
        WHERE caller_namespace = ? AND control_id = ?
          AND control_result_version = ? AND latest_result_json = ?
      `).run(
        terminal.control_result_version,
        canonicalizeJson(terminal),
        completedAt,
        normalized.caller_namespace,
        normalized.control_id,
        current.control_result_version,
        row.latest_result_json,
      );
      if (updated.changes !== 1) {
        const conflictError = new Error('The eviction completion lost its control-result fence.');
        conflictError.code = 'version_conflict';
        throw conflictError;
      }
      database.prepare(`
        UPDATE runtime_operations_audit
        SET outcome = ?, target_version = ?, error_json = ?, committed_at = ?
        WHERE audit_id = ?
      `).run(
        status,
        targetVersion,
        error === null ? null : canonicalizeJson(error),
        completedAt,
        row.audit_id,
      );
      return terminal;
    }).immediate();
  }

  function completeReconciliation(request, reconciliationError = null) {
    const completedAt = requireTimestamp('reconciliation completion time', now());
    return database.transaction(() => {
      const row = database.prepare(`
        SELECT control.normalized_request_json, control.latest_result_json,
          control.audit_id, intent.intent_id, intent.state AS intent_state
        FROM runtime_operations_controls AS control
        JOIN runtime_operations_reconciliation_intents AS intent
          ON intent.service_instance_id = ?
          AND intent.caller_namespace = control.caller_namespace
          AND intent.control_id = control.control_id
        WHERE control.caller_namespace = ? AND control.control_id = ?
          AND control.action = 'reconcile'
      `).get(serviceInstanceId, request.caller_namespace, request.control_id);
      if (!row) throw new TypeError('reconciliation control intent does not exist');
      const normalized = JSON.parse(row.normalized_request_json);
      const current = JSON.parse(row.latest_result_json);
      if (current.status !== 'accepted') return current;
      if (row.intent_state !== 'pending') {
        const conflictError = new Error('The reconciliation intent is no longer pending.');
        conflictError.code = 'version_conflict';
        throw conflictError;
      }
      const service = database.prepare(`
        SELECT service_version FROM runtime_observability_instances
        WHERE service_instance_id = ?
      `).get(serviceInstanceId);
      if (!service) throw new TypeError('observability service instance is unavailable');
      const status = reconciliationError === null ? 'completed' : 'failed';
      const result = reconciliationError === null
        ? { intent_id: row.intent_id, state: 'completed' }
        : null;
      const error = reconciliationError === null ? null : createContractError({
        code: 'side_effect_unknown',
        category: 'internal',
        retryable: true,
        sideEffectStatus: 'unknown',
        userMessage: 'Runtime reconciliation did not reach a proven terminal state.',
        occurredAt: completedAt,
      });
      const terminal = validateControlResult({
        ...current,
        control_result_version: current.control_result_version + 1,
        status,
        target_version: service.service_version,
        result,
        error,
        completed_at: completedAt,
      }, { occurredAt: completedAt, action: 'reconcile' }).forwarded;
      const intentUpdate = database.prepare(`
        UPDATE runtime_operations_reconciliation_intents
        SET state = ?, updated_at = ?
        WHERE intent_id = ? AND service_instance_id = ?
          AND caller_namespace = ? AND control_id = ? AND state = 'pending'
      `).run(
        status === 'completed' ? 'completed' : 'failed',
        completedAt,
        row.intent_id,
        serviceInstanceId,
        normalized.caller_namespace,
        normalized.control_id,
      );
      const controlUpdate = database.prepare(`
        UPDATE runtime_operations_controls
        SET control_result_version = ?, latest_result_json = ?, updated_at = ?
        WHERE caller_namespace = ? AND control_id = ?
          AND control_result_version = ? AND latest_result_json = ?
      `).run(
        terminal.control_result_version,
        canonicalizeJson(terminal),
        completedAt,
        normalized.caller_namespace,
        normalized.control_id,
        current.control_result_version,
        row.latest_result_json,
      );
      if (intentUpdate.changes !== 1 || controlUpdate.changes !== 1) {
        const conflictError = new Error('The reconciliation completion lost its durable fence.');
        conflictError.code = 'version_conflict';
        throw conflictError;
      }
      database.prepare(`
        UPDATE runtime_operations_audit
        SET outcome = ?, target_version = ?, error_json = ?, committed_at = ?
        WHERE audit_id = ?
      `).run(
        status,
        service.service_version,
        error === null ? null : canonicalizeJson(error),
        completedAt,
        row.audit_id,
      );
      return terminal;
    }).immediate();
  }

  return Object.freeze({ completeEviction, completeReconciliation, execute });
}
