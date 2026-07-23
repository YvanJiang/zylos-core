import crypto from 'node:crypto';

import {
  canonicalizeJson,
  createIdempotencyKey,
  validateInboundEnvelope,
  validateDeliveryCommand,
  validatePublicFixtureSafety,
} from '../../contracts/public/index.js';
import { initializeRuntimePersistence } from '../persistence/schema.js';
import { acceptNormalInbound } from '../persistence/inbound-acceptance.js';
import { resolveDeliveryCommandVersionForTarget } from '../persistence/delivery-target-identity.js';
import { createExecutorStore } from '../persistence/executor-store.js';
import {
  acceptScheduledOccurrence,
  createScheduledOccurrenceEnvelope,
  decideScheduledOccurrence,
} from '../scheduler/scheduler-queue.js';

const ACTIVE_TERMINAL_STATES = new Set(['committed', 'rolled_back']);

function requireText(name, value) {
  if (typeof value !== 'string' || value.length === 0) {
    throw new TypeError(`${name} must be a non-empty string`);
  }
  return value;
}

function hashInput(value) {
  return crypto.createHash('sha256').update(canonicalizeJson(value)).digest('hex');
}

function buildLegacyNoticeCommand({ upgradeId, record, classification, committedAt, generateId }) {
  const outboxId = generateId('outbox');
  const deliveryId = generateId('delivery');
  const aggregateId = `legacy-notice-${hashInput({
    upgrade_id: upgradeId,
    kind: record.kind,
    legacy_record_id: record.legacy_record_id,
  }).slice(0, 32)}`;
  const command = {
    contract: 'zylos.delivery-command',
    contract_version: '1.1',
    outbox_id: outboxId,
    delivery_id: deliveryId,
    trace_id: generateId('delivery-trace'),
    delivery_attempt_id: generateId('delivery-attempt'),
    delivery_attempt_no: 1,
    outbox_lease_epoch: 1,
    target: structuredClone(record.notification_target),
    aggregate_type: 'text_notice',
    aggregate_id: aggregateId,
    operation: 'send_text',
    aggregate_version: 1,
    event_sequence_through: null,
    idempotency_key: createIdempotencyKey('delivery', {
      channel: record.notification_target?.channel,
      target: record.notification_target,
      delivery_id: deliveryId,
    }),
    render_model: {
      title: 'Zylos',
      phase: classification.disposition === 'skipped_missed' ? 'missed' : 'interrupted',
      text: classification.disposition === 'skipped_missed'
        ? 'A missed legacy schedule was skipped during upgrade and was not replayed.'
        : 'Legacy work was interrupted during upgrade. Its side effects are unknown, so it will not be replayed automatically.',
      error: null,
      tools: [],
      interactions: [],
      terminal: true,
      user_action_required: classification.disposition !== 'skipped_missed',
    },
    mapping: {
      mapping_id: generateId('mapping'),
      conversation_id: null,
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
  return command;
}

function buildForcedDrainNoticeCommand({ upgradeId, turn, envelope, committedAt, generateId }) {
  const nativeThread = envelope.native_thread_or_topic_id !== null;
  if (nativeThread && envelope.reply.root_message_id === null) {
    throw new Error('Forced drain notice requires the exact native thread root message ID.');
  }
  const outboxId = generateId('outbox');
  const deliveryId = generateId('delivery');
  const isMessageResponse = envelope.source.kind === 'platform_original'
    || (
      envelope.source.kind === 'scheduler'
      && envelope.schedule?.bound_conversation === true
    );
  const target = {
    region: envelope.region,
    tenant_id: envelope.tenant_id,
    channel: envelope.channel,
    bot_id: envelope.bot_id,
    chat_type: envelope.chat_type,
    chat_id: envelope.chat_id,
    native_thread_or_topic_id: envelope.native_thread_or_topic_id,
    native_thread_root_message_id: nativeThread ? envelope.reply.root_message_id : null,
    native_thread_reply_target_message_id: nativeThread ? envelope.message_id : null,
    ...(envelope.channel === 'feishu' && isMessageResponse ? {
      reply_target_message_id: envelope.message_id,
      mention_actor_id: (
        envelope.source.kind === 'platform_original'
        && envelope.actor.type === 'user'
        && ['group', 'thread'].includes(envelope.chat_type)
      )
        ? envelope.actor.actor_id
        : null,
    } : {}),
  };
  const command = {
    contract: 'zylos.delivery-command',
    contract_version: resolveDeliveryCommandVersionForTarget(target),
    outbox_id: outboxId, delivery_id: deliveryId,
    trace_id: generateId('delivery-trace'),
    delivery_attempt_id: generateId('delivery-attempt'), delivery_attempt_no: 1,
    outbox_lease_epoch: 1,
    target,
    aggregate_type: 'text_notice',
    aggregate_id: `upgrade-force-${hashInput({ upgrade_id: upgradeId, turn_id: turn.turn_id })}`,
    operation: 'send_text', aggregate_version: 1, event_sequence_through: null,
    idempotency_key: createIdempotencyKey('delivery', {
      channel: target.channel, target, delivery_id: deliveryId,
    }),
    render_model: {
      title: 'Zylos', phase: 'interrupted',
      text: 'An administrator is ending this active turn so the runtime upgrade can continue.',
      error: null, tools: [], interactions: [], terminal: true, user_action_required: true,
    },
    mapping: {
      mapping_id: generateId('mapping'),
      conversation_id: turn.conversation_id,
      turn_id: turn.turn_id,
      lineage_id: turn.lineage_id,
      binding_state: turn.lineage_id === null ? 'pending' : 'bound',
      mapping_version: 1,
      ...(turn.lineage_id === null ? { reason: 'lineage_resolution_pending' } : {}),
    },
    target_platform_message_id: null, predecessor_delivery_id: null,
    expected_platform_version: null, priority: 100,
    not_before: committedAt, created_at: committedAt,
  };
  validateDeliveryCommand(command, { occurredAt: committedAt });
  return command;
}

function decodeRun(row) {
  if (!row) return null;
  return Object.freeze({
    upgrade_id: row.upgrade_id,
    scope: Object.freeze({ kind: row.scope_kind, bot_id: row.bot_id }),
    from_release: row.from_release,
    to_release: row.to_release,
    state: row.state,
    state_version: row.state_version,
    preflight: JSON.parse(row.preflight_json),
    snapshot: row.snapshot_json === null ? null : JSON.parse(row.snapshot_json),
    migration: row.migration_json === null ? null : JSON.parse(row.migration_json),
    health: row.health_json === null ? null : JSON.parse(row.health_json),
    failure: row.failure_json === null ? null : JSON.parse(row.failure_json),
    maintenance_started_at: row.maintenance_started_at,
    committed_at: row.committed_at,
    rolled_back_at: row.rolled_back_at,
    created_at: row.created_at,
    updated_at: row.updated_at,
  });
}

export function createRuntimeUpgradeService({
  database,
  now = () => new Date().toISOString(),
  generateId = (kind) => `${kind}-${crypto.randomUUID()}`,
}) {
  if (!database || typeof database.transaction !== 'function') {
    throw new TypeError('database must be a better-sqlite3 connection');
  }
  if (typeof now !== 'function' || typeof generateId !== 'function') {
    throw new TypeError('now and generateId must be functions');
  }
  initializeRuntimePersistence(database);

  function load(upgradeId) {
    return database.prepare(`
      SELECT * FROM runtime_upgrade_runs WHERE upgrade_id = ?
    `).get(upgradeId);
  }

  function get(upgradeId) {
    requireText('upgradeId', upgradeId);
    const row = load(upgradeId);
    if (!row) throw new Error(`Runtime upgrade ${upgradeId} does not exist.`);
    return decodeRun(row);
  }

  function recordEvent({ upgradeId, stepKey, fromState, toState, input, result, committedAt }) {
    database.prepare(`
      INSERT INTO runtime_upgrade_events (
        upgrade_id, step_key, from_state, to_state, input_hash, result_json, committed_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?)
    `).run(
      upgradeId,
      stepKey,
      fromState,
      toState,
      hashInput(input),
      JSON.stringify(result),
      committedAt,
    );
  }

  function replayEvent(upgradeId, stepKey, input) {
    const event = database.prepare(`
      SELECT input_hash, result_json FROM runtime_upgrade_events
      WHERE upgrade_id = ? AND step_key = ?
    `).get(upgradeId, stepKey);
    if (!event) return null;
    if (event.input_hash !== hashInput(input)) {
      throw new Error(`Runtime upgrade step ${stepKey} conflicts with its durable input.`);
    }
    return Object.freeze(JSON.parse(event.result_json));
  }

  function preflight(input) {
    if (!input || typeof input !== 'object' || Array.isArray(input)) {
      throw new TypeError('preflight input must be an object');
    }
    const upgradeId = requireText('upgrade_id', input.upgrade_id);
    requireText('from_release', input.from_release);
    requireText('to_release', input.to_release);
    if (!input.scope || !['installation', 'bot'].includes(input.scope.kind)) {
      throw new TypeError('scope.kind must be installation or bot');
    }
    if (input.scope.kind === 'installation' && input.scope.bot_id !== null) {
      throw new TypeError('installation scope bot_id must be null');
    }
    if (input.scope.kind === 'bot') requireText('scope.bot_id', input.scope.bot_id);
    if (!input.checks || typeof input.checks !== 'object' || Array.isArray(input.checks)) {
      throw new TypeError('checks must be an object');
    }
    const requiredChecks = {
      sqlite_integrity: 'ok',
      codex_transport: 'official_app_server_only',
      delivery_contract: 'zylos.delivery-command@1.1',
      workspace_lease_fencing: 'intact',
      retention_cleanup: 'intact',
      normal_runtime_paths: 'new_only',
    };
    for (const [field, expected] of Object.entries(requiredChecks)) {
      if (input.checks[field] !== expected) {
        throw new Error(`Runtime upgrade preflight check ${field} must be ${expected}.`);
      }
    }
    validatePublicFixtureSafety(input.checks);
    const replay = replayEvent(upgradeId, 'preflight', input);
    if (replay) return replay;
    const commit = database.transaction(() => {
      const committedAt = now();
      database.prepare(`
        INSERT INTO runtime_upgrade_runs (
          upgrade_id, scope_kind, bot_id, from_release, to_release, state,
          state_version, preflight_json, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, 'preflight', 1, ?, ?, ?)
      `).run(
        upgradeId,
        input.scope.kind,
        input.scope.bot_id,
        input.from_release,
        input.to_release,
        JSON.stringify(input.checks),
        committedAt,
        committedAt,
      );
      const result = decodeRun(load(upgradeId));
      recordEvent({
        upgradeId, stepKey: 'preflight', fromState: null, toState: 'preflight',
        input, result, committedAt,
      });
      return result;
    });
    return commit.immediate();
  }

  function transition(upgradeId, {
    stepKey,
    expectedState,
    nextState,
    input,
    assignments = '',
    values = [],
  }) {
    requireText('upgradeId', upgradeId);
    const replay = replayEvent(upgradeId, stepKey, input);
    if (replay) return replay;
    const commit = database.transaction(() => {
      const current = load(upgradeId);
      if (!current) throw new Error(`Runtime upgrade ${upgradeId} does not exist.`);
      if (ACTIVE_TERMINAL_STATES.has(current.state)) {
        throw new Error(`Runtime upgrade ${upgradeId} is already ${current.state}.`);
      }
      if (current.state !== expectedState) {
        throw new Error(
          `Runtime upgrade ${upgradeId} must be ${expectedState}, not ${current.state}.`,
        );
      }
      const committedAt = now();
      const updated = database.prepare(`
        UPDATE runtime_upgrade_runs
        SET state = ?, state_version = state_version + 1,
          updated_at = ?${assignments}
        WHERE upgrade_id = ? AND state = ? AND state_version = ?
      `).run(
        nextState,
        committedAt,
        ...values,
        upgradeId,
        expectedState,
        current.state_version,
      );
      if (updated.changes !== 1) {
        throw new Error(`Runtime upgrade ${upgradeId} lost its durable state fence.`);
      }
      const result = decodeRun(load(upgradeId));
      recordEvent({
        upgradeId, stepKey, fromState: expectedState, toState: nextState,
        input, result, committedAt,
      });
      return result;
    });
    return commit.immediate();
  }

  function recordSnapshot(upgradeId, snapshot) {
    if (!snapshot || typeof snapshot !== 'object' || Array.isArray(snapshot)) {
      throw new TypeError('snapshot must be an object');
    }
    requireText('package_release_ref', snapshot.package_release_ref);
    requireText('database_snapshot_ref', snapshot.database_snapshot_ref);
    if (!/^[a-f0-9]{64}$/.test(snapshot.snapshot_sha256)) {
      throw new TypeError('snapshot_sha256 must be lowercase SHA-256');
    }
    validatePublicFixtureSafety(snapshot);
    return transition(upgradeId, {
      stepKey: 'snapshot', expectedState: 'preflight', nextState: 'snapshotted',
      input: snapshot, assignments: ', snapshot_json = ?', values: [JSON.stringify(snapshot)],
    });
  }

  function enterMaintenance(upgradeId) {
    return transition(upgradeId, {
      stepKey: 'maintenance', expectedState: 'snapshotted', nextState: 'maintenance',
      input: Object.freeze({}), assignments: ', maintenance_started_at = ?', values: [now()],
    });
  }

  function fail(upgradeId, failure) {
    requireText('upgradeId', upgradeId);
    if (!failure || typeof failure !== 'object' || Array.isArray(failure)) {
      throw new TypeError('failure must be an object');
    }
    requireText('failure.boundary', failure.boundary);
    requireText('failure.code', failure.code);
    requireText('failure.message', failure.message);
    validatePublicFixtureSafety(failure);
    const replay = replayEvent(upgradeId, 'failure', failure);
    if (replay) return replay;
    const commit = database.transaction(() => {
      const current = load(upgradeId);
      if (!current) throw new Error(`Runtime upgrade ${upgradeId} does not exist.`);
      if (current.state === 'committed') {
        throw new Error(`Runtime upgrade ${upgradeId} is committed and cannot roll back.`);
      }
      if (current.state === 'rolled_back') {
        throw new Error(`Runtime upgrade ${upgradeId} is already rolled_back.`);
      }
      const failureState = current.snapshot_json === null ? 'rolled_back' : 'rollback_required';
      const committedAt = now();
      const updated = database.prepare(`
        UPDATE runtime_upgrade_runs
        SET state = ?, state_version = state_version + 1,
          failure_json = ?, rolled_back_at = ?, updated_at = ?
        WHERE upgrade_id = ? AND state = ? AND state_version = ?
      `).run(
        failureState,
        JSON.stringify(failure),
        failureState === 'rolled_back' ? committedAt : null,
        committedAt,
        upgradeId,
        current.state,
        current.state_version,
      );
      if (updated.changes !== 1) {
        throw new Error(`Runtime upgrade ${upgradeId} lost its durable failure fence.`);
      }
      const result = decodeRun(load(upgradeId));
      recordEvent({
        upgradeId, stepKey: 'failure', fromState: current.state,
        toState: failureState, input: failure, result, committedAt,
      });
      return result;
    });
    return commit.immediate();
  }

  function completeRollback(upgradeId) {
    requireText('upgradeId', upgradeId);
    const input = Object.freeze({ effect_step_key: 'rollback-restore' });
    const replay = replayEvent(upgradeId, 'rollback', input);
    if (replay) return replay;
    const commit = database.transaction(() => {
      const current = load(upgradeId);
      if (!current) throw new Error(`Runtime upgrade ${upgradeId} does not exist.`);
      if (current.state !== 'rollback_required') {
        throw new Error(
          current.state === 'rolled_back'
            ? `Runtime upgrade ${upgradeId} is already rolled_back.`
            : `Runtime upgrade ${upgradeId} must be rollback_required.`,
        );
      }
      const snapshot = JSON.parse(current.snapshot_json);
      const restore = database.prepare(`
        SELECT result_json
        FROM runtime_upgrade_effects
        WHERE upgrade_id = ? AND step_key = 'rollback-restore'
          AND step_id = upgrade_id || ':rollback-restore'
          AND state = 'completed'
      `).get(upgradeId);
      if (!restore) {
        throw new Error('Rollback requires the coordinator durable restore effect.');
      }
      const restoreResult = JSON.parse(restore.result_json);
      if (restoreResult.release_ref !== snapshot.package_release_ref
        || restoreResult.database_snapshot_ref !== snapshot.database_snapshot_ref
        || restoreResult.snapshot_sha256 !== snapshot.snapshot_sha256
        || restoreResult.database_integrity !== 'ok'
        || restoreResult.foreign_key_violations !== 0) {
        throw new Error('Rollback durable restore effect does not match the verified snapshot.');
      }
      const releaseFence = database.prepare(`
        SELECT restored_at FROM runtime_upgrade_release_fence_history WHERE upgrade_id = ?
      `).get(upgradeId);
      if (releaseFence && releaseFence.restored_at === null) {
        throw new Error('Rollback requires the durable release generation fence to be restored.');
      }
      const sourceInvalidation = database.prepare(`
        SELECT state, result_json FROM runtime_upgrade_effects
        WHERE upgrade_id = ? AND step_key = 'legacy-source-invalidate'
      `).get(upgradeId);
      if (sourceInvalidation !== undefined) {
        if (sourceInvalidation.state !== 'completed') {
          throw new Error('Rollback requires completed legacy source invalidation reconciliation.');
        }
        const reconciliation = database.prepare(`
          SELECT 1 FROM runtime_upgrade_events
          WHERE upgrade_id = ? AND step_key IN (
            'legacy-migration', 'legacy-rollback-reconciliation'
          ) LIMIT 1
        `).get(upgradeId);
        if (reconciliation === undefined) {
          throw new Error('Rollback requires durable legacy migration or rollback reconciliation.');
        }
        const sourceProof = JSON.parse(sourceInvalidation.result_json);
        const completedEffects = new Map(database.prepare(`
          SELECT step_key, step_id, result_json FROM runtime_upgrade_effects
          WHERE upgrade_id = ? AND step_key IN (
            'legacy-source-seal', 'legacy-source-restore', 'legacy-source-reconciliation'
          ) AND state = 'completed'
        `).all(upgradeId).map((effect) => [effect.step_key, effect]));
        for (const stepKey of [
          'legacy-source-seal', 'legacy-source-restore', 'legacy-source-reconciliation',
        ]) {
          if (!completedEffects.has(stepKey)) {
            throw new Error(`Rollback requires completed ${stepKey} effect.`);
          }
        }
        const seal = JSON.parse(completedEffects.get('legacy-source-seal').result_json);
        const sourceRestore = JSON.parse(completedEffects.get('legacy-source-restore').result_json);
        const sourceReconciliation = JSON.parse(
          completedEffects.get('legacy-source-reconciliation').result_json,
        );
        if (seal.audit_queue_ref !== sourceProof.audit_queue_ref
          || seal.audit_sha256 !== sourceProof.audit_sha256
          || seal.audit_queue_removed !== true
          || sourceRestore.source_queue_ref !== sourceProof.source_queue_ref
          || sourceRestore.rollback_queue_sha256 !== sourceProof.rollback_queue_sha256
          || sourceRestore.source_queue_restored !== true
          || sourceReconciliation.source_queue_ref !== sourceProof.source_queue_ref
          || sourceReconciliation.source_data_restored !== true
          || sourceReconciliation.legacy_runtime_remained_inactive !== true
          || sourceReconciliation.source_reconciliation_idempotency_key
            !== completedEffects.get('legacy-source-reconciliation').step_id) {
          throw new Error('Rollback legacy source effects do not match invalidation proof.');
        }
        const pendingNotices = database.prepare(`
          SELECT COUNT(*) AS count FROM runtime_legacy_migration_notices
          WHERE upgrade_id = ? AND state = 'pending'
        `).get(upgradeId).count;
        if (pendingNotices > 0) {
          throw new Error('Rollback requires delivered unknown-side-effect notices.');
        }
      }
      const rollbackStore = createExecutorStore({
        database,
        provider: 'claude',
        serviceInstanceId: `upgrade-rollback:${upgradeId}`,
        now,
        generateId,
      });
      rollbackStore.cancelUpgradeImportedTurns(upgradeId);
      const committedAt = now();
      database.prepare(`
        UPDATE runtime_executor_service_instances
        SET revoked_at = ?
        WHERE upgrade_id = ? AND revoked_at IS NULL
      `).run(committedAt, upgradeId);
      const updated = database.prepare(`
        UPDATE runtime_upgrade_runs
        SET state = 'rolled_back', state_version = state_version + 1,
          rolled_back_at = ?, updated_at = ?
        WHERE upgrade_id = ? AND state = 'rollback_required' AND state_version = ?
      `).run(committedAt, committedAt, upgradeId, current.state_version);
      if (updated.changes !== 1) {
        throw new Error(`Runtime upgrade ${upgradeId} lost its durable rollback fence.`);
      }
      const result = decodeRun(load(upgradeId));
      recordEvent({
        upgradeId, stepKey: 'rollback', fromState: 'rollback_required',
        toState: 'rolled_back', input, result, committedAt,
      });
      return result;
    });
    return commit.immediate();
  }

  function activeTurnIds(run) {
    return database.prepare(`
      SELECT turn.turn_id
      FROM runtime_turns AS turn
      JOIN runtime_conversations AS conversation
        ON conversation.conversation_id = turn.conversation_id
      LEFT JOIN runtime_turn_queue AS queue ON queue.turn_id = turn.turn_id
      WHERE turn.state IN ('starting', 'running', 'waiting_user', 'redirecting', 'recovering')
        AND NOT (
          turn.state = 'recovering' AND queue.status = 'cancelled'
          AND queue.wait_reason = 'upgrade_rollback_parked'
        )
        AND (? = 'installation' OR conversation.bot_id = ?)
      ORDER BY turn.created_at, turn.turn_id
    `).all(run.scope_kind, run.bot_id).map(({ turn_id: turnId }) => turnId);
  }

  function drainBlockers(run) {
    const workspaceLeaseIds = database.prepare(`
      SELECT lease.workspace_lease_id
      FROM runtime_workspace_leases AS lease
      JOIN runtime_conversations AS conversation
        ON conversation.conversation_id = lease.holder_conversation_id
      WHERE lease.state IN ('active', 'uncertain')
        AND (? = 'installation' OR conversation.bot_id = ?)
      ORDER BY lease.acquired_at, lease.workspace_lease_id
    `).all(run.scope_kind, run.bot_id).map(({ workspace_lease_id: id }) => id);
    const backgroundWorkIds = database.prepare(`
      SELECT background.background_work_id
      FROM runtime_workspace_background_work AS background
      JOIN runtime_turns AS turn ON turn.turn_id = background.holder_turn_id
      JOIN runtime_conversations AS conversation
        ON conversation.conversation_id = turn.conversation_id
      WHERE background.state IN ('active', 'unknown')
        AND (? = 'installation' OR conversation.bot_id = ?)
      ORDER BY background.started_at, background.background_work_id
    `).all(run.scope_kind, run.bot_id).map(({ background_work_id: id }) => id);
    return Object.freeze({
      active_turn_ids: Object.freeze(activeTurnIds(run)),
      workspace_lease_ids: Object.freeze(workspaceLeaseIds),
      background_work_ids: Object.freeze(backgroundWorkIds),
    });
  }

  function completeDrain(upgradeId) {
    const current = load(upgradeId);
    if (!current) throw new Error(`Runtime upgrade ${upgradeId} does not exist.`);
    const blockers = drainBlockers(current);
    if (Object.values(blockers).some((ids) => ids.length > 0)) {
      const deadlineAt = new Date(
        Date.parse(current.maintenance_started_at) + 600_000,
      ).toISOString();
      if (Date.parse(now()) > Date.parse(deadlineAt)) {
        return fail(upgradeId, {
          boundary: 'drain',
          code: 'drain_timeout',
          message: 'Runtime work did not drain within ten minutes; force was not authorized.',
          ...blockers,
        });
      }
      return Object.freeze({
        status: 'waiting',
        upgrade_id: upgradeId,
        ...blockers,
        deadline_at: deadlineAt,
      });
    }
    return transition(upgradeId, {
      stepKey: 'drain', expectedState: 'maintenance', nextState: 'drained',
      input: blockers,
    });
  }

  function prepareForcedDrain(upgradeId) {
    requireText('upgradeId', upgradeId);
    const commit = database.transaction(() => {
      const run = load(upgradeId);
      if (!run || run.state !== 'maintenance') {
        throw new Error(`Runtime upgrade ${upgradeId} must be maintenance for forced drain.`);
      }
      const turnIds = activeTurnIds(run);
      if (turnIds.length === 0) {
        throw new Error('Forced drain requires at least one active turn.');
      }
      const committedAt = now();
      for (const turnId of turnIds) {
        const existing = database.prepare(`
          SELECT outbox_id, delivery_id FROM runtime_upgrade_force_notices
          WHERE upgrade_id = ? AND turn_id = ?
        `).get(upgradeId, turnId);
        if (existing) continue;
        const row = database.prepare(`
          SELECT turn.turn_id, turn.conversation_id, turn.lineage_id, inbound.envelope_json
          FROM runtime_turns AS turn
          JOIN runtime_inbound_events AS inbound
            ON inbound.inbound_event_id = turn.inbound_event_id
          WHERE turn.turn_id = ?
        `).get(turnId);
        if (!row) throw new Error(`Active turn ${turnId} has no durable inbound target.`);
        const command = buildForcedDrainNoticeCommand({
          upgradeId,
          turn: row,
          envelope: JSON.parse(row.envelope_json),
          committedAt,
          generateId,
        });
        database.prepare(`
          INSERT INTO runtime_outbox (
            outbox_id, delivery_id, aggregate_type, aggregate_id, turn_id, control_id,
            lane_key, predecessor_delivery_id, aggregate_version, status, command_json,
            priority, supersedable, terminal, next_attempt_at, created_at, updated_at
          ) VALUES (?, ?, 'text_notice', ?, ?, NULL, NULL, NULL, 1, 'pending',
            ?, 100, 0, 1, ?, ?, ?)
        `).run(
          command.outbox_id,
          command.delivery_id,
          command.aggregate_id,
          turnId,
          JSON.stringify(command),
          command.not_before,
          committedAt,
          committedAt,
        );
        database.prepare(`
          INSERT INTO runtime_upgrade_force_notices (
            upgrade_id, turn_id, outbox_id, delivery_id, created_at
          ) VALUES (?, ?, ?, ?, ?)
        `).run(upgradeId, turnId, command.outbox_id, command.delivery_id, committedAt);
      }
      return Object.freeze({
        upgrade_id: upgradeId,
        state: run.state,
        notice_turn_ids: Object.freeze([...turnIds]),
      });
    });
    return commit.immediate();
  }

  function completeForcedDrain(upgradeId, controlRefs) {
    requireText('upgradeId', upgradeId);
    if (!Array.isArray(controlRefs) || controlRefs.length === 0) {
      throw new TypeError('controlRefs must be a non-empty array');
    }
    const normalizedRefs = controlRefs.map((ref) => ({
      caller_namespace: requireText('controlRef.caller_namespace', ref?.caller_namespace),
      control_id: requireText('controlRef.control_id', ref?.control_id),
    })).sort((left, right) => (
      `${left.caller_namespace}\u0000${left.control_id}`
        .localeCompare(`${right.caller_namespace}\u0000${right.control_id}`)
    ));
    const current = load(upgradeId);
    if (!current || current.state !== 'maintenance') {
      throw new Error(`Runtime upgrade ${upgradeId} must be maintenance for forced drain.`);
    }
    const notices = database.prepare(`
      SELECT notice.turn_id, notice.delivery_id,
        outbox.status AS outbox_status, outbox.result_json
      FROM runtime_upgrade_force_notices AS notice
      JOIN runtime_outbox AS outbox ON outbox.outbox_id = notice.outbox_id
      WHERE notice.upgrade_id = ?
      ORDER BY notice.turn_id
    `).all(upgradeId);
    if (notices.length === 0 || notices.length !== normalizedRefs.length) {
      throw new Error('Forced drain requires one canonical admin control per notified active turn.');
    }
    const deliveriesByTurnId = new Map();
    for (const notice of notices) {
      if (notice.outbox_status !== 'delivered' || notice.result_json === null) {
        throw new Error('Forced drain must deliver the user notice before stop authorization.');
      }
      const delivery = JSON.parse(notice.result_json);
      if (delivery.status !== 'delivered' || delivery.delivery_id !== notice.delivery_id) {
        throw new Error('Forced drain notice delivery identity is invalid.');
      }
      deliveriesByTurnId.set(notice.turn_id, delivery);
    }
    const controlsByTurnId = new Map();
    for (const ref of normalizedRefs) {
      const control = database.prepare(`
        SELECT control.action, control.normalized_request_json,
          control.latest_result_json, audit.outcome, audit.capability, audit.committed_at
        FROM runtime_operations_controls AS control
        JOIN runtime_operations_audit AS audit ON audit.audit_id = control.audit_id
        WHERE control.caller_namespace = ? AND control.control_id = ?
      `).get(ref.caller_namespace, ref.control_id);
      if (!control) throw new Error('Forced drain control is not in the canonical operations audit.');
      const request = JSON.parse(control.normalized_request_json);
      const result = JSON.parse(control.latest_result_json);
      const turnId = request.target?.turn_id;
      if (control.action !== 'stop_active_turn'
        || control.capability !== 'turn.stop'
        || control.outcome !== 'completed'
        || result.status !== 'completed'
        || result.result?.active_turn_id !== turnId
        || !request.actor?.roles?.some((role) => role === 'admin' || role.endsWith('-admin'))
        || controlsByTurnId.has(turnId)) {
        throw new Error('Forced drain requires one capability-authenticated admin stop per turn.');
      }
      controlsByTurnId.set(turnId, { control, result });
    }
    const authorizedTurnIds = [];
    for (const notice of notices) {
      const delivery = deliveriesByTurnId.get(notice.turn_id);
      const authorized = controlsByTurnId.get(notice.turn_id);
      if (!authorized
        || Date.parse(authorized.control.committed_at) < Date.parse(delivery.delivered_at)) {
        throw new Error(
          'Forced drain requires capability-authenticated admin stop after durable notice delivery.',
        );
      }
      authorizedTurnIds.push(notice.turn_id);
    }
    const blockers = drainBlockers(current);
    if (Object.values(blockers).some((ids) => ids.length > 0)) {
      throw new Error('Forced drain canonical stops have not released every scoped blocker.');
    }
    return transition(upgradeId, {
      stepKey: 'force-drain', expectedState: 'maintenance', nextState: 'drained',
      input: { control_refs: normalizedRefs, notified_turn_ids: authorizedTurnIds },
    });
  }

  function releaseScopeKey(run) {
    return run.scope_kind === 'installation' ? 'installation' : `bot:${run.bot_id}`;
  }

  function isReleaseFenceActive(upgradeId) {
    const run = load(upgradeId);
    if (!run) throw new Error(`Runtime upgrade ${upgradeId} does not exist.`);
    return database.prepare(`
      SELECT 1 FROM runtime_active_release_fences
      WHERE scope_key = ? AND upgrade_id = ? AND release_ref = ?
    `).get(releaseScopeKey(run), upgradeId, run.to_release) !== undefined;
  }

  function activateReleaseFence(upgradeId) {
    requireText('upgradeId', upgradeId);
    const activate = database.transaction(() => {
      const run = load(upgradeId);
      if (!run || run.state !== 'drained') {
        throw new Error(`Runtime upgrade ${upgradeId} must be drained to activate its release fence.`);
      }
      const effect = database.prepare(`
        SELECT result_json FROM runtime_upgrade_effects
        WHERE upgrade_id = ? AND step_key = 'release-activate'
          AND state = 'completed'
      `).get(upgradeId);
      if (!effect) throw new Error('Release fence requires the durable activation effect.');
      const activation = JSON.parse(effect.result_json);
      if (activation.release_ref !== run.to_release) {
        throw new Error('Release activation effect does not match the target release.');
      }
      const scopeKey = releaseScopeKey(run);
      const existingHistory = database.prepare(`
        SELECT target_generation FROM runtime_upgrade_release_fence_history
        WHERE upgrade_id = ?
      `).get(upgradeId);
      if (existingHistory) {
        if (!isReleaseFenceActive(upgradeId)) {
          throw new Error('Durable release fence history conflicts with the active fence.');
        }
        return Object.freeze({
          upgrade_id: upgradeId, scope_key: scopeKey,
          release_ref: run.to_release, generation: existingHistory.target_generation,
        });
      }
      const previous = database.prepare(`
        SELECT * FROM runtime_active_release_fences WHERE scope_key = ?
      `).get(scopeKey);
      const generation = (previous?.generation ?? 0) + 1;
      const activatedAt = now();
      database.prepare(`
        INSERT INTO runtime_upgrade_release_fence_history (
          upgrade_id, scope_key, previous_fence_json, target_generation, activated_at
        ) VALUES (?, ?, ?, ?, ?)
      `).run(
        upgradeId,
        scopeKey,
        previous === undefined ? null : canonicalizeJson(previous),
        generation,
        activatedAt,
      );
      database.prepare(`
        INSERT INTO runtime_active_release_fences (
          scope_key, scope_kind, bot_id, upgrade_id, release_ref, generation, activated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(scope_key) DO UPDATE SET
          scope_kind = excluded.scope_kind,
          bot_id = excluded.bot_id,
          upgrade_id = excluded.upgrade_id,
          release_ref = excluded.release_ref,
          generation = excluded.generation,
          activated_at = excluded.activated_at
      `).run(
        scopeKey, run.scope_kind, run.bot_id, upgradeId, run.to_release, generation, activatedAt,
      );
      return Object.freeze({
        upgrade_id: upgradeId, scope_key: scopeKey,
        release_ref: run.to_release, generation,
      });
    });
    return activate.immediate();
  }

  function restoreReleaseFence(upgradeId) {
    requireText('upgradeId', upgradeId);
    const restore = database.transaction(() => {
      const run = load(upgradeId);
      if (!run || run.state !== 'rollback_required') {
        throw new Error(`Runtime upgrade ${upgradeId} must be rollback_required.`);
      }
      const history = database.prepare(`
        SELECT * FROM runtime_upgrade_release_fence_history WHERE upgrade_id = ?
      `).get(upgradeId);
      if (!history) throw new Error('Rollback requires durable release fence history.');
      if (history.restored_at !== null) return Object.freeze({ restored: true });
      const restoreEffect = database.prepare(`
        SELECT 1 FROM runtime_upgrade_effects
        WHERE upgrade_id = ? AND step_key = 'rollback-restore'
          AND state = 'completed'
      `).get(upgradeId);
      if (!restoreEffect) throw new Error('Release fence restore requires physical restore effect.');
      if (history.previous_fence_json === null) {
        const removed = database.prepare(`
          DELETE FROM runtime_active_release_fences
          WHERE scope_key = ? AND upgrade_id = ? AND generation = ?
        `).run(history.scope_key, upgradeId, history.target_generation);
        if (removed.changes !== 1) throw new Error('Target release fence lost its rollback CAS.');
      } else {
        const previous = JSON.parse(history.previous_fence_json);
        const restored = database.prepare(`
          UPDATE runtime_active_release_fences
          SET scope_kind = ?, bot_id = ?, upgrade_id = ?, release_ref = ?,
            generation = ?, activated_at = ?
          WHERE scope_key = ? AND upgrade_id = ? AND generation = ?
        `).run(
          previous.scope_kind,
          previous.bot_id,
          previous.upgrade_id,
          previous.release_ref,
          previous.generation,
          previous.activated_at,
          history.scope_key,
          upgradeId,
          history.target_generation,
        );
        if (restored.changes !== 1) throw new Error('Target release fence lost its restore CAS.');
      }
      database.prepare(`
        UPDATE runtime_upgrade_release_fence_history SET restored_at = ?
        WHERE upgrade_id = ? AND restored_at IS NULL
      `).run(now(), upgradeId);
      return Object.freeze({ restored: true });
    });
    return restore.immediate();
  }

  function classifyLegacyRecord(record) {
    if (!record || typeof record !== 'object' || Array.isArray(record)) {
      throw new TypeError('legacy record must be an object');
    }
    requireText('legacy record kind', record.kind);
    if (typeof record.legacy_record_id !== 'string') {
      throw new TypeError('legacy_record_id must be a string');
    }
    requireText('legacy_state', record.legacy_state);
    validatePublicFixtureSafety(record);
    if (!['c4', 'global_provider_lineage', 'scheduler', 'runtime_control'].includes(record.kind)) {
      throw new TypeError(`Unsupported legacy record kind ${record.kind}.`);
    }
    if (record.kind === 'c4'
      && (record.legacy_record_id.length === 0
        || /[\u0000-\u001f\u007f]/.test(record.legacy_record_id))) {
      return {
        disposition: 'quarantined_invalid_identity', notice: false,
        audit: { reason: 'legacy_record_id_invalid' },
      };
    }
    requireText('legacy_record_id', record.legacy_record_id);
    if (record.kind === 'runtime_control') {
      return {
        disposition: 'invalidated_audit_only', notice: false,
        audit: { reason: 'legacy_runtime_control_invalidated', source_queue_read_only: true },
      };
    }
    if (record.kind === 'global_provider_lineage') {
      if (!Array.isArray(record.outbound_messages)) {
        throw new TypeError('global provider lineage requires outbound_messages');
      }
      if (!Array.isArray(record.recent_c4_context)
        || record.recent_c4_context.some((entry) => typeof entry !== 'string')) {
        throw new TypeError('global provider lineage requires recent_c4_context strings');
      }
      if (typeof record.memory_handoff !== 'string') {
        throw new TypeError('global provider lineage requires memory_handoff');
      }
      return {
        disposition: 'archived_unmapped', notice: false,
        audit: { mapping_status: 'legacy_unmapped', importable: false },
      };
    }
    if (record.legacy_state === 'running') {
      if (!record.notification_target || typeof record.notification_target !== 'object') {
        throw new TypeError('running legacy work requires notification_target');
      }
      return {
        disposition: 'quarantined_side_effect_unknown', notice: true,
        audit: { side_effect_status: 'unknown', terminal_state: 'interrupted' },
      };
    }
    if (record.kind === 'c4' && record.legacy_state === 'pending') {
      if (record.route === 'ambiguous') {
        return { disposition: 'quarantined_ambiguous', notice: false, audit: { route: 'ambiguous' } };
      }
      if (record.route !== 'unique') throw new TypeError('pending C4 route must be unique or ambiguous');
      if (!record.envelope || record.envelope.legacy?.legacy_record_id !== record.legacy_record_id
        || record.envelope.legacy?.legacy_state !== 'pending') {
        throw new TypeError('pending C4 envelope must carry the exact legacy record identity');
      }
      return { disposition: 'migrated_pending', notice: false, audit: { route: 'unique' } };
    }
    if (record.kind === 'scheduler' && record.legacy_state === 'pending') {
      if (!record.occurrence || typeof record.occurrence !== 'object') {
        throw new TypeError('pending scheduler record requires an occurrence');
      }
      const scheduledAt = Date.parse(requireText('scheduled_for', record.scheduled_for));
      const observedAt = Date.parse(requireText('observed_at', record.observed_at));
      if (!Number.isFinite(scheduledAt) || !Number.isFinite(observedAt)) {
        throw new TypeError('scheduler migration times must be RFC 3339 timestamps');
      }
      if (!Number.isSafeInteger(record.miss_threshold_ms) || record.miss_threshold_ms < 0) {
        throw new TypeError('miss_threshold_ms must be a non-negative safe integer');
      }
      const oneTimeMissed = record.schedule_type === 'one-time'
        && observedAt - scheduledAt > record.miss_threshold_ms;
      const decision = oneTimeMissed
        ? { status: 'skipped', reason: 'missed_occurrence' }
        : decideScheduledOccurrence({
          schedule_type: record.schedule_type,
          scheduled_for: record.scheduled_for,
          now: record.observed_at,
          miss_threshold_ms: record.miss_threshold_ms,
        });
      if (decision.status === 'enqueue') {
        return {
          disposition: 'migrated_scheduler', notice: false,
          audit: { schedule_decision: 'enqueue', permission_mode: 'safe' },
        };
      }
      if (!record.notification_target || typeof record.notification_target !== 'object') {
        throw new TypeError('missed legacy schedule requires notification_target');
      }
      return {
        disposition: 'skipped_missed', notice: true,
        audit: { schedule_decision: 'skipped', reason: decision.reason },
      };
    }
    if (record.legacy_state === 'delivered') {
      return {
        disposition: record.kind === 'c4' ? 'retained_delivered' : 'retained_history',
        notice: false,
        audit: {},
      };
    }
    if (record.legacy_state === 'failed') {
      return {
        disposition: record.kind === 'c4' ? 'retained_failed' : 'retained_history',
        notice: false,
        audit: {},
      };
    }
    return { disposition: 'retained_history', notice: false, audit: {} };
  }

  function analyzeLegacyBatch(batch) {
    if (!batch || typeof batch !== 'object' || Array.isArray(batch)) {
      throw new TypeError('legacy migration batch must be an object');
    }
    requireText('batch_id', batch.batch_id);
    if (!Array.isArray(batch.records)) throw new TypeError('legacy records must be an array');
    let previousLegacyQueueSequence = 0;
    const identities = new Set();
    const analyzed = batch.records.map((record) => {
      const classification = classifyLegacyRecord(record);
      const identity = `${record.kind}\u0000${record.legacy_record_id}`;
      if (identities.has(identity)) {
        throw new TypeError('legacy record identities must be unique within a batch');
      }
      identities.add(identity);
      if (classification.disposition === 'migrated_pending') {
        const validated = validateInboundEnvelope(record.envelope).forwarded;
        if (validated.legacy?.legacy_record_id !== record.legacy_record_id
          || validated.legacy?.legacy_state !== 'pending') {
          throw new TypeError('pending C4 envelope must carry the exact legacy record identity');
        }
        if (!Number.isSafeInteger(record.legacy_queue_sequence)
          || record.legacy_queue_sequence <= previousLegacyQueueSequence) {
          throw new TypeError(
            'pending unique C4 legacy_queue_sequence must be positive and strictly FIFO ordered',
          );
        }
        previousLegacyQueueSequence = record.legacy_queue_sequence;
      }
      if (classification.disposition === 'migrated_scheduler') {
        if (record.permission_mode !== undefined && record.permission_mode !== 'safe') {
          throw new TypeError('legacy scheduler migration permission_mode must be safe');
        }
        createScheduledOccurrenceEnvelope(record.occurrence);
      }
      return Object.freeze({ record, classification });
    });
    return Object.freeze({ batch_hash: hashInput(batch), analyzed: Object.freeze(analyzed) });
  }

  function planLegacyRollbackBatch(batch) {
    const analysis = analyzeLegacyBatch(batch);
    return Object.freeze({
      batch_id: batch.batch_id,
      rollback_reconciled: true,
      record_refs: Object.freeze(analysis.analyzed
        .filter(({ classification }) => ['migrated_pending', 'migrated_scheduler']
          .includes(classification.disposition))
        .map(({ record }) => Object.freeze({
          legacy_kind: record.kind,
          legacy_record_id: record.legacy_record_id,
          payload_hash: hashInput(record),
        }))),
    });
  }

  function reconcileLegacyRollback(upgradeId, batch) {
    requireText('upgradeId', upgradeId);
    const analysis = analyzeLegacyBatch(batch);
    const reconciliationInput = Object.freeze({
      batch_id: batch.batch_id,
      batch_hash: analysis.batch_hash,
      record_count: batch.records.length,
    });
    const replay = replayEvent(
      upgradeId, 'legacy-rollback-reconciliation', reconciliationInput,
    );
    if (replay) return replay;
    const commit = database.transaction(() => {
      const run = load(upgradeId);
      if (!run || run.state !== 'rollback_required') {
        throw new Error(`Runtime upgrade ${upgradeId} must be rollback_required.`);
      }
      const invalidation = database.prepare(`
        SELECT result_json FROM runtime_upgrade_effects
        WHERE upgrade_id = ? AND step_key = 'legacy-source-invalidate' AND state = 'completed'
      `).get(upgradeId);
      const invalidationProof = invalidation === undefined ? null : JSON.parse(invalidation.result_json);
      if (invalidationProof?.batch_id !== batch.batch_id
        || invalidationProof?.batch_hash !== analysis.batch_hash
        || invalidationProof?.source_queue_read_only !== true) {
        throw new Error('Legacy rollback reconciliation requires exact source invalidation proof.');
      }
      const committedAt = now();
      for (const analyzed of analysis.analyzed) {
        const { record } = analyzed;
        const migratable = ['migrated_pending', 'migrated_scheduler']
          .includes(analyzed.classification.disposition);
        const classification = migratable ? {
          disposition: analyzed.classification.disposition === 'migrated_pending'
            ? 'restored_pending' : 'restored_scheduler',
          notice: false,
          audit: { ...analyzed.classification.audit, rollback_restored: true },
        } : analyzed.classification;
        database.prepare(`
          INSERT INTO runtime_legacy_migration_records (
            upgrade_id, legacy_kind, legacy_record_id, legacy_state, disposition,
            payload_hash, audit_json, migrated_turn_id,
            imported_by_upgrade, executable, read_only, created_at
          ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, 0, 0, 1, ?)
        `).run(
          upgradeId, record.kind, record.legacy_record_id, record.legacy_state,
          classification.disposition, hashInput(record),
          JSON.stringify({ batch_id: batch.batch_id, ...classification.audit }), committedAt,
        );
        database.prepare(`
          INSERT INTO runtime_legacy_migration_payloads (
            upgrade_id, legacy_kind, legacy_record_id, payload_json, created_at
          ) VALUES (?, ?, ?, ?, ?)
        `).run(
          upgradeId, record.kind, record.legacy_record_id, canonicalizeJson(record), committedAt,
        );
        if (record.kind === 'runtime_control') {
          database.prepare(`
            INSERT INTO runtime_legacy_migration_audit_payloads (
              upgrade_id, legacy_kind, legacy_record_id, audit_payload_json, created_at
            ) VALUES (?, ?, ?, ?, ?)
          `).run(
            upgradeId, record.kind, record.legacy_record_id, canonicalizeJson(record), committedAt,
          );
        }
        const durableFact = record.kind === 'scheduler'
          ? {
              kind: record.kind,
              legacy_record_id: record.legacy_record_id,
              legacy_state: record.legacy_state,
              disposition: classification.disposition,
              schedule_type: record.schedule_type ?? null,
              scheduled_for: record.scheduled_for ?? null,
              observed_at: record.observed_at ?? null,
              definition: record.definition ?? null,
              history: record.history ?? null,
              occurrence: record.occurrence === undefined ? null : {
                schedule_id: record.occurrence.schedule_id,
                task_id: record.occurrence.task_id,
                occurrence_id: record.occurrence.occurrence_id,
                prompt: record.occurrence.prompt,
                occurred_at: record.occurrence.occurred_at,
                received_at: record.occurrence.received_at,
                region: record.occurrence.region,
                tenant_id: record.occurrence.tenant_id,
                bot_id: record.occurrence.bot_id,
                bound_conversation: record.occurrence.bound_conversation,
              },
            }
          : (record.kind === 'c4' && ['delivered', 'failed'].includes(record.legacy_state)
              ? {
                  kind: record.kind,
                  legacy_record_id: record.legacy_record_id,
                  legacy_state: record.legacy_state,
                  disposition: classification.disposition,
                  conversation_id: record.conversation_id ?? null,
                  history: record.history ?? null,
                  summary: record.summary ?? null,
                }
              : null);
        if (durableFact !== null) {
          database.prepare(`
            INSERT INTO runtime_legacy_migration_durable_facts (
              upgrade_id, legacy_kind, legacy_record_id, fact_json, created_at
            ) VALUES (?, ?, ?, ?, ?)
          `).run(
            upgradeId, record.kind, record.legacy_record_id,
            canonicalizeJson(durableFact), committedAt,
          );
        }
        if (record.kind === 'global_provider_lineage') {
          for (const message of record.outbound_messages) {
            for (const field of [
              'region', 'tenant_id', 'channel', 'bot_id', 'chat_type', 'chat_id',
              'platform_message_id',
            ]) requireText(`outbound_messages.${field}`, message?.[field]);
            if (message.native_thread_or_topic_id !== null
              && (typeof message.native_thread_or_topic_id !== 'string'
                || message.native_thread_or_topic_id.length === 0)) {
              throw new TypeError(
                'outbound_messages.native_thread_or_topic_id must be a string or null',
              );
            }
            database.prepare(`
              INSERT INTO runtime_legacy_unmapped_messages (
                region, tenant_id, channel, bot_id, chat_type, chat_id,
                native_thread_or_topic_id, platform_message_id,
                upgrade_id, legacy_kind, legacy_record_id,
                recent_c4_context_json, memory_handoff, created_at
              ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'global_provider_lineage', ?, ?, ?, ?)
            `).run(
              message.region, message.tenant_id, message.channel, message.bot_id,
              message.chat_type, message.chat_id, message.native_thread_or_topic_id,
              message.platform_message_id, upgradeId, record.legacy_record_id,
              canonicalizeJson(record.recent_c4_context), record.memory_handoff, committedAt,
            );
          }
        }
        if (classification.notice) {
          const noticeCommand = buildLegacyNoticeCommand({
            upgradeId, record, classification, committedAt, generateId,
          });
          database.prepare(`
            INSERT INTO runtime_outbox (
              outbox_id, delivery_id, aggregate_type, aggregate_id, turn_id, control_id,
              lane_key, predecessor_delivery_id, aggregate_version, status, command_json,
              priority, supersedable, terminal, next_attempt_at, created_at, updated_at
            ) VALUES (?, ?, ?, ?, NULL, NULL, NULL, NULL, 1, 'pending', ?, ?, 0, 1, ?, ?, ?)
          `).run(
            noticeCommand.outbox_id, noticeCommand.delivery_id,
            noticeCommand.aggregate_type, noticeCommand.aggregate_id,
            JSON.stringify(noticeCommand), noticeCommand.priority, noticeCommand.not_before,
            committedAt, committedAt,
          );
          database.prepare(`
            INSERT INTO runtime_legacy_migration_notices (
              upgrade_id, legacy_kind, legacy_record_id, outbox_id, delivery_id,
              state, created_at
            ) VALUES (?, ?, ?, ?, ?, 'pending', ?)
          `).run(
            upgradeId, record.kind, record.legacy_record_id,
            noticeCommand.outbox_id, noticeCommand.delivery_id, committedAt,
          );
        }
      }
      const result = decodeRun(load(upgradeId));
      recordEvent({
        upgradeId, stepKey: 'legacy-rollback-reconciliation',
        fromState: 'rollback_required', toState: 'rollback_required',
        input: reconciliationInput, result, committedAt,
      });
      return result;
    });
    return commit.immediate();
  }

  function migrateLegacy(upgradeId, batch) {
    const analysis = analyzeLegacyBatch(batch);
    const batchHash = analysis.batch_hash;
    const migrationInput = Object.freeze({
      batch_id: batch.batch_id,
      batch_hash: batchHash,
      record_count: batch.records.length,
    });
    const invalidation = database.prepare(`
      SELECT result_json FROM runtime_upgrade_effects
      WHERE upgrade_id = ? AND step_key = 'legacy-source-invalidate'
        AND state = 'completed'
    `).get(upgradeId);
    const invalidationProof = invalidation === undefined ? null : JSON.parse(invalidation.result_json);
    if (invalidationProof?.batch_id !== batch.batch_id
      || invalidationProof?.batch_hash !== batchHash
      || invalidationProof?.source_queue_read_only !== true
      || invalidationProof?.legacy_dispatcher_stopped !== true
      || typeof invalidationProof?.legacy_dispatcher_stopped_at !== 'string'
      || typeof invalidationProof?.audit_queue_ref !== 'string'
      || typeof invalidationProof?.audit_sha256 !== 'string') {
      throw new Error('Legacy migration requires exact durable source-queue invalidation proof.');
    }
    const current = get(upgradeId);
    if (current.state === 'drained') {
      transition(upgradeId, {
        stepKey: 'migration-start', expectedState: 'drained', nextState: 'migrating',
        input: { batch_id: batch.batch_id },
      });
    } else if (current.state !== 'migrating' && current.state !== 'health_check') {
      throw new Error(`Runtime upgrade ${upgradeId} cannot migrate from ${current.state}.`);
    }
    const replay = replayEvent(upgradeId, 'legacy-migration', migrationInput);
    if (replay) return replay;
    const commit = database.transaction(() => {
      const run = load(upgradeId);
      if (!run || run.state !== 'migrating') {
        throw new Error(`Runtime upgrade ${upgradeId} must be migrating.`);
      }
      const committedAt = now();
      const counts = { migrated: 0, quarantined: 0, retained: 0, notices_pending: 0 };
      const migrationStore = createExecutorStore({
        database,
        provider: 'claude',
        serviceInstanceId: `upgrade-migration:${upgradeId}`,
        now,
        generateId,
      });
      for (const { record, classification } of analysis.analyzed) {
        let migratedTurnId = null;
        let importedByUpgrade = 0;
        if (['migrated_pending', 'migrated_scheduler'].includes(classification.disposition)) {
          const accepted = classification.disposition === 'migrated_pending'
            ? acceptNormalInbound(database, record.envelope, { now, generateId })
            : acceptScheduledOccurrence(database, record.occurrence, { now, generateId });
          if (accepted.status !== 'accepted') {
            throw new Error(`Legacy C4 ${record.legacy_record_id} was not accepted.`);
          }
          migratedTurnId = accepted.turn_id;
          importedByUpgrade = accepted.deduplicated ? 0 : 1;
          if (accepted.deduplicated) {
            const prior = database.prepare(`
              SELECT legacy.upgrade_id
              FROM runtime_legacy_migration_records AS legacy
              JOIN runtime_upgrade_runs AS prior_run ON prior_run.upgrade_id = legacy.upgrade_id
              WHERE legacy.legacy_kind = ? AND legacy.legacy_record_id = ?
                AND legacy.migrated_turn_id = ? AND legacy.imported_by_upgrade = 1
                AND legacy.payload_hash = ? AND prior_run.state = 'rolled_back'
              ORDER BY prior_run.rolled_back_at DESC, prior_run.created_at DESC
              LIMIT 1
            `).get(
              record.kind,
              record.legacy_record_id,
              migratedTurnId,
              hashInput(record),
            );
            if (prior) {
              migrationStore.requeueRolledBackUpgradeImportedTurn({
                previous_upgrade_id: prior.upgrade_id,
                current_upgrade_id: upgradeId,
                turn_id: migratedTurnId,
              });
              importedByUpgrade = 1;
            } else {
              const duplicateTurn = database.prepare(`
                SELECT state FROM runtime_turns WHERE turn_id = ?
              `).get(migratedTurnId);
              if (['cancelled', 'recovering'].includes(duplicateTurn?.state)) {
                throw new Error(
                  'Cancelled legacy idempotency can only be adopted from exact rolled-back ownership.',
                );
              }
            }
          }
          counts.migrated += 1;
        } else if (classification.disposition.startsWith('quarantined_')) {
          counts.quarantined += 1;
        } else {
          counts.retained += 1;
        }
        database.prepare(`
          INSERT INTO runtime_legacy_migration_records (
            upgrade_id, legacy_kind, legacy_record_id, legacy_state, disposition,
            payload_hash, audit_json, migrated_turn_id,
            imported_by_upgrade, executable, read_only, created_at
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 1, ?)
        `).run(
          upgradeId,
          record.kind,
          record.legacy_record_id,
          record.legacy_state,
          classification.disposition,
          hashInput(record),
          JSON.stringify({ batch_id: batch.batch_id, ...classification.audit }),
          migratedTurnId,
          importedByUpgrade,
          committedAt,
        );
        database.prepare(`
          INSERT INTO runtime_legacy_migration_payloads (
            upgrade_id, legacy_kind, legacy_record_id, payload_json, created_at
          ) VALUES (?, ?, ?, ?, ?)
        `).run(
          upgradeId,
          record.kind,
          record.legacy_record_id,
          canonicalizeJson(record),
          committedAt,
        );
        if (record.kind === 'runtime_control') {
          database.prepare(`
            INSERT INTO runtime_legacy_migration_audit_payloads (
              upgrade_id, legacy_kind, legacy_record_id, audit_payload_json, created_at
            ) VALUES (?, ?, ?, ?, ?)
          `).run(
            upgradeId,
            record.kind,
            record.legacy_record_id,
            canonicalizeJson(record),
            committedAt,
          );
        }
        const durableFact = record.kind === 'scheduler'
          ? {
              kind: record.kind,
              legacy_record_id: record.legacy_record_id,
              legacy_state: record.legacy_state,
              disposition: classification.disposition,
              schedule_type: record.schedule_type ?? null,
              scheduled_for: record.scheduled_for ?? null,
              observed_at: record.observed_at ?? null,
              definition: record.definition ?? null,
              history: record.history ?? null,
              occurrence: record.occurrence === undefined ? null : {
                schedule_id: record.occurrence.schedule_id,
                task_id: record.occurrence.task_id,
                occurrence_id: record.occurrence.occurrence_id,
                prompt: record.occurrence.prompt,
                occurred_at: record.occurrence.occurred_at,
                received_at: record.occurrence.received_at,
                region: record.occurrence.region,
                tenant_id: record.occurrence.tenant_id,
                bot_id: record.occurrence.bot_id,
                bound_conversation: record.occurrence.bound_conversation,
              },
            }
          : (record.kind === 'c4' && ['delivered', 'failed'].includes(record.legacy_state)
              ? {
                  kind: record.kind,
                  legacy_record_id: record.legacy_record_id,
                  legacy_state: record.legacy_state,
                  disposition: classification.disposition,
                  conversation_id: record.conversation_id ?? null,
                  history: record.history ?? null,
                  summary: record.summary ?? null,
                }
              : null);
        if (durableFact !== null) {
          database.prepare(`
            INSERT INTO runtime_legacy_migration_durable_facts (
              upgrade_id, legacy_kind, legacy_record_id, fact_json, created_at
            ) VALUES (?, ?, ?, ?, ?)
          `).run(
            upgradeId,
            record.kind,
            record.legacy_record_id,
            canonicalizeJson(durableFact),
            committedAt,
          );
        }
        if (record.kind === 'global_provider_lineage') {
          for (const message of record.outbound_messages) {
            for (const field of [
              'region', 'tenant_id', 'channel', 'bot_id', 'chat_type', 'chat_id',
              'platform_message_id',
            ]) {
              requireText(`outbound_messages.${field}`, message?.[field]);
            }
            if (message.native_thread_or_topic_id !== null
              && (typeof message.native_thread_or_topic_id !== 'string'
                || message.native_thread_or_topic_id.length === 0)) {
              throw new TypeError(
                'outbound_messages.native_thread_or_topic_id must be a string or null',
              );
            }
            database.prepare(`
              INSERT INTO runtime_legacy_unmapped_messages (
                region, tenant_id, channel, bot_id, chat_type, chat_id,
                native_thread_or_topic_id, platform_message_id,
                upgrade_id, legacy_kind, legacy_record_id,
                recent_c4_context_json, memory_handoff, created_at
              ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'global_provider_lineage', ?, ?, ?, ?)
            `).run(
              message.region,
              message.tenant_id,
              message.channel,
              message.bot_id,
              message.chat_type,
              message.chat_id,
              message.native_thread_or_topic_id,
              message.platform_message_id,
              upgradeId,
              record.legacy_record_id,
              canonicalizeJson(record.recent_c4_context),
              record.memory_handoff,
              committedAt,
            );
          }
        }
        if (classification.notice) {
          const noticeCommand = buildLegacyNoticeCommand({
            upgradeId, record, classification, committedAt, generateId,
          });
          database.prepare(`
            INSERT INTO runtime_outbox (
              outbox_id, delivery_id, aggregate_type, aggregate_id, turn_id, control_id,
              lane_key, predecessor_delivery_id, aggregate_version, status, command_json,
              priority, supersedable, terminal, next_attempt_at, created_at, updated_at
            ) VALUES (?, ?, ?, ?, NULL, NULL, NULL, NULL, 1, 'pending', ?, ?, 0, 1, ?, ?, ?)
          `).run(
            noticeCommand.outbox_id,
            noticeCommand.delivery_id,
            noticeCommand.aggregate_type,
            noticeCommand.aggregate_id,
            JSON.stringify(noticeCommand),
            noticeCommand.priority,
            noticeCommand.not_before,
            committedAt,
            committedAt,
          );
          database.prepare(`
            INSERT INTO runtime_legacy_migration_notices (
              upgrade_id, legacy_kind, legacy_record_id, outbox_id, delivery_id,
              state, created_at
            ) VALUES (?, ?, ?, ?, ?, 'pending', ?)
          `).run(
            upgradeId,
            record.kind,
            record.legacy_record_id,
            noticeCommand.outbox_id,
            noticeCommand.delivery_id,
            committedAt,
          );
          counts.notices_pending += 1;
        }
      }
      const migration = Object.freeze({ batch_id: batch.batch_id, ...counts });
      const updated = database.prepare(`
        UPDATE runtime_upgrade_runs
        SET state = 'health_check', state_version = state_version + 1,
          migration_json = ?, updated_at = ?
        WHERE upgrade_id = ? AND state = 'migrating' AND state_version = ?
      `).run(JSON.stringify(migration), committedAt, upgradeId, run.state_version);
      if (updated.changes !== 1) throw new Error('Legacy migration lost its durable state fence.');
      const result = decodeRun(load(upgradeId));
      recordEvent({
        upgradeId, stepKey: 'legacy-migration', fromState: 'migrating',
        toState: 'health_check', input: migrationInput, result, committedAt,
      });
      return result;
    });
    return commit.immediate();
  }

  function recordNoticeDelivered(upgradeId, legacyKind, legacyRecordId) {
    requireText('upgradeId', upgradeId);
    requireText('legacyKind', legacyKind);
    requireText('legacyRecordId', legacyRecordId);
    const commit = database.transaction(() => {
      const existing = database.prepare(`
        SELECT notice.state, notice.proof_hash, notice.proof_json,
          notice.delivery_id, outbox.status AS outbox_status, outbox.result_json
        FROM runtime_legacy_migration_notices AS notice
        JOIN runtime_outbox AS outbox ON outbox.outbox_id = notice.outbox_id
        WHERE notice.upgrade_id = ? AND notice.legacy_kind = ?
          AND notice.legacy_record_id = ?
      `).get(upgradeId, legacyKind, legacyRecordId);
      if (!existing) throw new Error('Legacy migration notice does not exist.');
      if (existing.state === 'delivered') {
        return Object.freeze(JSON.parse(existing.proof_json));
      }
      if (existing.outbox_status !== 'delivered' || existing.result_json === null) {
        throw new Error('Legacy notice requires authoritative durable outbox delivery.');
      }
      const proof = JSON.parse(existing.result_json);
      if (proof.status !== 'delivered' || proof.delivery_id !== existing.delivery_id) {
        throw new Error('Legacy notice durable outbox delivery identity is invalid.');
      }
      const proofHash = hashInput(proof);
      const updated = database.prepare(`
        UPDATE runtime_legacy_migration_notices
        SET state = 'delivered', proof_hash = ?, proof_json = ?, delivered_at = ?
        WHERE upgrade_id = ? AND legacy_kind = ? AND legacy_record_id = ? AND state = 'pending'
      `).run(
        proofHash, JSON.stringify(proof), proof.delivered_at,
        upgradeId, legacyKind, legacyRecordId,
      );
      if (updated.changes !== 1) throw new Error('Legacy notice lost its durable state fence.');
      return Object.freeze(structuredClone(proof));
    });
    return commit.immediate();
  }

  function recordExecutorHealth(upgradeId, health) {
    if (!health || typeof health !== 'object' || Array.isArray(health)) {
      throw new TypeError('health must be an object');
    }
    requireText('health.service_instance_id', health.service_instance_id);
    if (!Number.isSafeInteger(health.snapshot_version) || health.snapshot_version < 1) {
      throw new TypeError('health.snapshot_version must be a positive safe integer');
    }
    if (health.health !== 'healthy' || health.reconciliation !== 'complete') {
      throw new Error('Executor health must prove healthy reconciliation.');
    }
    validatePublicFixtureSafety(health);
    const pending = database.prepare(`
      SELECT COUNT(*) AS count FROM runtime_legacy_migration_notices
      WHERE upgrade_id = ? AND state = 'pending'
    `).get(upgradeId).count;
    if (pending > 0) {
      throw new Error('Executor health requires delivered notice proof for unknown side effects.');
    }
    const observed = database.prepare(`
      SELECT observation.snapshot_version, observation.last_reconciliation_at,
        service.provider, service.provider_transport, service.release_ref,
        service.upgrade_id, service.started_at, migration.committed_at AS migrated_at
      FROM runtime_observability_instances AS observation
      JOIN runtime_executor_service_instances AS service
        ON service.service_instance_id = observation.service_instance_id
      JOIN runtime_upgrade_runs AS run ON run.upgrade_id = service.upgrade_id
      JOIN runtime_upgrade_events AS migration
        ON migration.upgrade_id = run.upgrade_id AND migration.step_key = 'legacy-migration'
      WHERE observation.service_instance_id = ? AND run.upgrade_id = ?
        AND service.release_ref = run.to_release
    `).get(health.service_instance_id, upgradeId);
    if (!observed || observed.last_reconciliation_at === null
      || observed.snapshot_version !== health.snapshot_version
      || observed.started_at < observed.migrated_at
      || observed.last_reconciliation_at < observed.migrated_at
      || (observed.provider === 'codex'
        && observed.provider_transport !== 'official_app_server')
      || (observed.provider === 'claude'
        && observed.provider_transport !== 'claude_agent_sdk')) {
      throw new Error(
        'Executor health evidence does not match the target release on its official transport.',
      );
    }
    const authoritativeHealth = {
      ...health,
      provider: observed.provider,
      provider_transport: observed.provider_transport,
      release_ref: observed.release_ref,
      started_at: observed.started_at,
      reconciled_at: observed.last_reconciliation_at,
    };
    return transition(upgradeId, {
      stepKey: 'executor-health', expectedState: 'health_check', nextState: 'ready_to_commit',
      input: authoritativeHealth,
      assignments: ', health_json = ?', values: [JSON.stringify(authoritativeHealth)],
    });
  }

  function commitUpgrade(upgradeId) {
    if (!isReleaseFenceActive(upgradeId)) {
      throw new Error('Runtime upgrade commit requires its exact active release generation fence.');
    }
    return transition(upgradeId, {
      stepKey: 'commit', expectedState: 'ready_to_commit', nextState: 'committed',
      input: Object.freeze({}), assignments: ', committed_at = ?', values: [now()],
    });
  }

  return Object.freeze({
    activateReleaseFence,
    completeRollback,
    completeDrain,
    completeForcedDrain,
    commit: commitUpgrade,
    enterMaintenance,
    fail,
    get,
    isReleaseFenceActive,
    migrateLegacy,
    planLegacyRollbackBatch,
    reconcileLegacyRollback,
    preflight,
    prepareForcedDrain,
    recordSnapshot,
    recordExecutorHealth,
    recordNoticeDelivered,
    restoreReleaseFence,
  });
}
