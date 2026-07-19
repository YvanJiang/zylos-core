import {
  createIdempotencyKey,
  DELIVERY_COMMAND_CURRENT_VERSION,
  validateDeliveryCommand,
} from '../../contracts/public/index.js';
import { createDeliveryLaneKey } from './delivery-lane-key.js';
import {
  assertDeliveryLaneIdentity,
  assertDeliveryTargetIdentity,
  parseDurableDeliveryTarget,
} from './delivery-target-identity.js';

const TERMINAL_PHASES = new Set([
  'completed',
  'stopped',
  'cancelled',
  'interrupted',
  'failed',
  'timed_out',
]);

const CRITICAL_PHASES = new Set([
  'waiting_user',
  'redirecting',
  'recovering',
  ...TERMINAL_PHASES,
]);

function addMilliseconds(timestamp, milliseconds) {
  return new Date(Date.parse(timestamp) + milliseconds).toISOString();
}

function laterTimestamp(left, right) {
  return Date.parse(left) >= Date.parse(right) ? left : right;
}

export function initializeMainProjection(database, command) {
  const laneKey = createDeliveryLaneKey(command);
  const existing = database.prepare(`
    SELECT lane_key, turn_id, aggregate_type, target_json
    FROM runtime_delivery_lanes
    WHERE turn_id = ?
  `).get(command.mapping.turn_id);
  if (existing) {
    const durableTarget = parseDurableDeliveryTarget(existing.target_json, {
      occurredAt: command.created_at,
    });
    assertDeliveryLaneIdentity(existing, durableTarget, { occurredAt: command.created_at });
    assertDeliveryTargetIdentity(
      durableTarget,
      command.target,
      { occurredAt: command.created_at },
    );
    return laneKey;
  }
  database.prepare(`
    INSERT INTO runtime_delivery_lanes (
      lane_key, turn_id, aggregate_type, delivery_mode, target_json, mapping_json,
      created_at, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
  `).run(
    laneKey,
    command.mapping.turn_id,
    command.aggregate_type,
    command.operation === 'send_text' ? 'text' : 'main',
    JSON.stringify(command.target),
    JSON.stringify(command.mapping),
    command.created_at,
    command.created_at,
  );
  return laneKey;
}

function projectRenderModel(renderModel, event) {
  let text = renderModel.text;
  let tools = renderModel.tools;
  let interactions = renderModel.interactions;
  let permissions = renderModel.permissions ?? [];
  if (event.kind === 'text_snapshot') {
    text = event.payload.text;
  } else if (event.kind === 'text_delta') {
    const currentText = event.payload.start_offset === 0 ? '' : (renderModel.text ?? '');
    text = `${currentText.slice(0, event.payload.start_offset)}${event.payload.text}`;
  } else if (
    event.kind === 'turn_state_changed'
    && event.payload.reason_code === 'executor_capacity'
  ) {
    text = 'Waiting for executor capacity.';
  } else if (
    event.kind === 'turn_state_changed'
    && event.payload.to_state === 'starting'
    && renderModel.text === 'Waiting for executor capacity.'
  ) {
    text = 'Starting execution.';
  } else if (event.kind.startsWith('tool_')) {
    tools = [...tools, structuredClone(event.payload)];
  } else if (event.kind.startsWith('interaction_')) {
    interactions = [...interactions, structuredClone(event.payload)];
  } else if (event.kind.startsWith('permission_')) {
    permissions = [...permissions, {
      kind: event.kind,
      payload: structuredClone(event.payload),
    }];
  }
  return {
    ...renderModel,
    phase: event.phase,
    text,
    error: event.error,
    tools,
    interactions,
    permissions,
    terminal: TERMINAL_PHASES.has(event.phase),
    user_action_required: event.phase === 'waiting_user'
      || event.kind === 'recovery_waiting_decision',
  };
}

export function isCriticalProjectionEvent(event) {
  return CRITICAL_PHASES.has(event.phase)
    || event.kind.startsWith('interaction_')
    || event.kind.startsWith('permission_')
    || event.kind.startsWith('recovery_')
    || event.kind === 'delivery_degraded'
    || event.payload?.side_effect_status === 'unknown'
    || event.error?.side_effect_status === 'unknown'
    || event.error !== null;
}

function loadLatestRenderModel(database, laneKey) {
  const snapshot = database.prepare(`
    SELECT render_model_json
    FROM runtime_projection_snapshots
    WHERE lane_key = ?
    ORDER BY aggregate_version DESC
    LIMIT 1
  `).get(laneKey);
  if (snapshot) return JSON.parse(snapshot.render_model_json);
  const initial = database.prepare(`
    SELECT command_json
    FROM runtime_outbox
    WHERE lane_key = ? AND aggregate_type = 'turn_main'
    ORDER BY aggregate_version DESC
    LIMIT 1
  `).get(laneKey);
  if (!initial) throw new Error(`Delivery lane ${laneKey} has no main projection.`);
  return JSON.parse(initial.command_json).render_model;
}

function supersedeOrdinaryPending(database, laneKey, updatedAt) {
  const rows = database.prepare(`
    SELECT outbox_id
    FROM runtime_outbox
    WHERE lane_key = ? AND supersedable = 1
      AND status IN ('pending', 'retry_wait')
  `).all(laneKey);
  if (rows.length === 0) return;
  database.prepare(`
    UPDATE runtime_outbox
    SET status = 'superseded', lease_owner = NULL, lease_expires_at = NULL,
      updated_at = ?
    WHERE lane_key = ? AND supersedable = 1
      AND status IN ('pending', 'retry_wait')
  `).run(updatedAt, laneKey);
  const outboxIds = new Set(rows.map(({ outbox_id: outboxId }) => outboxId));
  for (const { projection_id: projectionId, materialized_outbox_id: outboxId } of
    database.prepare(`
      SELECT projection_id, materialized_outbox_id
      FROM runtime_projection_snapshots
      WHERE lane_key = ? AND status = 'materialized'
    `).all(laneKey)) {
    if (outboxIds.has(outboxId)) {
      database.prepare(`
        UPDATE runtime_projection_snapshots
        SET status = 'superseded'
        WHERE projection_id = ?
      `).run(projectionId);
    }
  }
}

function latestLanePredecessor(database, laneKey) {
  return database.prepare(`
    SELECT delivery_id
    FROM runtime_outbox
    WHERE lane_key = ? AND status != 'superseded'
    ORDER BY aggregate_version DESC, created_at DESC, outbox_id DESC
    LIMIT 1
  `).get(laneKey)?.delivery_id ?? null;
}

function findDeadLetteredInitialCreate(database, laneKey) {
  const candidates = database.prepare(`
    SELECT delivery_id, command_json
    FROM runtime_outbox
    WHERE lane_key = ? AND status = 'dead_letter'
      AND predecessor_delivery_id IS NULL
    ORDER BY aggregate_version ASC, created_at ASC
  `).all(laneKey);
  for (const candidate of candidates) {
    try {
      if (JSON.parse(candidate.command_json).operation === 'create_main') return candidate;
    } catch {
      // Invalid historical commands are not eligible fallback predecessors.
    }
  }
  return null;
}

export function materializeNextStagedMainProjection(
  database,
  laneKey,
  {
    generateId,
    throttleMs = 1_500,
  },
) {
  const lane = database.prepare(`
    SELECT *
    FROM runtime_delivery_lanes
    WHERE lane_key = ?
  `).get(laneKey);
  if (!lane) return null;
  const active = database.prepare(`
    SELECT outbox_id
    FROM runtime_outbox
    WHERE lane_key = ? AND status IN ('pending', 'delivering', 'retry_wait')
    LIMIT 1
  `).get(laneKey);
  if (active) return null;

  const textMode = lane.delivery_mode === 'text';
  const createFailed = !textMode && (
    lane.platform_message_id === null || lane.last_delivery_id === null
  );
  const failedCreate = createFailed
    ? findDeadLetteredInitialCreate(database, laneKey)
    : null;
  const snapshot = database.prepare(`
    SELECT *
    FROM runtime_projection_snapshots
    WHERE lane_key = ? AND status = 'staged'
    ORDER BY aggregate_version ASC
    LIMIT 1
  `).get(laneKey);
  if (!snapshot || (!textMode && createFailed && !failedCreate)) return null;

  const target = parseDurableDeliveryTarget(lane.target_json, {
    occurredAt: snapshot.created_at,
  });
  assertDeliveryLaneIdentity(lane, target, { occurredAt: snapshot.created_at });
  const outboxId = generateId('outbox');
  const deliveryId = generateId('delivery');
  const critical = snapshot.critical === 1;
  const fallback = !textMode && failedCreate !== null;
  const mapping = {
    ...JSON.parse(lane.mapping_json),
    ...((fallback || textMode) ? { mapping_id: generateId('mapping') } : {}),
  };
  const predecessorDeliveryId = textMode
    ? null
    : (fallback ? failedCreate.delivery_id : latestLanePredecessor(database, laneKey));
  const notBefore = critical || fallback || lane.last_delivered_at === null
    ? snapshot.created_at
    : laterTimestamp(
      snapshot.created_at,
      addMilliseconds(lane.last_delivered_at, throttleMs),
    );
  const command = {
    contract: 'zylos.delivery-command',
    contract_version: DELIVERY_COMMAND_CURRENT_VERSION,
    outbox_id: outboxId,
    delivery_id: deliveryId,
    trace_id: generateId('delivery-trace'),
    delivery_attempt_id: generateId('delivery-attempt'),
    delivery_attempt_no: 1,
    outbox_lease_epoch: 1,
    target,
    aggregate_type: 'turn_main',
    aggregate_id: lane.turn_id,
    operation: textMode ? 'send_text' : (fallback ? 'send_fallback' : 'update_main'),
    aggregate_version: snapshot.aggregate_version,
    event_sequence_through: snapshot.event_sequence_through,
    idempotency_key: createIdempotencyKey('delivery', {
      channel: target.channel,
      target,
      delivery_id: deliveryId,
    }),
    render_model: JSON.parse(snapshot.render_model_json),
    mapping,
    target_platform_message_id: (fallback || textMode) ? null : lane.platform_message_id,
    predecessor_delivery_id: predecessorDeliveryId,
    expected_platform_version: (fallback || textMode) ? null : lane.applied_platform_version,
    priority: critical ? 100 : 10,
    not_before: notBefore,
    created_at: snapshot.created_at,
  };
  validateDeliveryCommand(command);
  database.prepare(`
    INSERT INTO runtime_outbox (
      outbox_id, delivery_id, aggregate_type, aggregate_id, turn_id, control_id,
      lane_key, predecessor_delivery_id, aggregate_version, status, command_json,
      priority, supersedable, terminal, next_attempt_at, created_at, updated_at
    ) VALUES (?, ?, ?, ?, ?, NULL, ?, ?, ?, 'pending', ?, ?, ?, ?, ?, ?, ?)
  `).run(
    outboxId,
    deliveryId,
    command.aggregate_type,
    command.aggregate_id,
    lane.turn_id,
    laneKey,
    predecessorDeliveryId,
    command.aggregate_version,
    JSON.stringify(command),
    command.priority,
    critical ? 0 : 1,
    snapshot.terminal,
    notBefore,
    snapshot.created_at,
    snapshot.created_at,
  );
  database.prepare(`
    UPDATE runtime_projection_snapshots
    SET status = 'materialized', materialized_outbox_id = ?
    WHERE projection_id = ? AND status = 'staged'
  `).run(outboxId, snapshot.projection_id);
  return command;
}

export function stageMainProjection(database, turn, event, {
  generateId,
  throttleMs = 1_500,
}) {
  const lane = database.prepare(`
    SELECT lane_key, turn_id, aggregate_type, platform_message_id, target_json
    FROM runtime_delivery_lanes
    WHERE turn_id = ?
  `).get(turn.turn_id);
  if (!lane) throw new Error(`Turn ${turn.turn_id} has no delivery lane.`);
  const durableTarget = parseDurableDeliveryTarget(lane.target_json, {
    occurredAt: event.persisted_at,
  });
  assertDeliveryLaneIdentity(lane, durableTarget, { occurredAt: event.persisted_at });

  const renderModel = projectRenderModel(loadLatestRenderModel(database, lane.lane_key), event);
  const critical = isCriticalProjectionEvent(event);
  if (critical) {
    database.prepare(`
      UPDATE runtime_projection_snapshots
      SET status = 'superseded'
      WHERE lane_key = ? AND critical = 0 AND status = 'staged'
    `).run(lane.lane_key);
    supersedeOrdinaryPending(database, lane.lane_key, event.persisted_at);
  } else {
    const pending = database.prepare(`
      SELECT outbox_id, command_json
      FROM runtime_outbox
      WHERE lane_key = ? AND status = 'pending' AND supersedable = 1
      ORDER BY aggregate_version DESC
      LIMIT 1
    `).get(lane.lane_key);
    if (pending) {
      assertDeliveryTargetIdentity(
        durableTarget,
        JSON.parse(pending.command_json).target,
        { occurredAt: event.persisted_at },
      );
      const command = {
        ...JSON.parse(pending.command_json),
        aggregate_version: event.turn_version,
        event_sequence_through: event.event_sequence,
        render_model: renderModel,
      };
      validateDeliveryCommand(command);
      database.prepare(`
        UPDATE runtime_projection_snapshots
        SET status = 'superseded'
        WHERE materialized_outbox_id = ? AND status = 'materialized'
      `).run(pending.outbox_id);
      database.prepare(`
        UPDATE runtime_outbox
        SET aggregate_version = ?, command_json = ?, updated_at = ?
        WHERE outbox_id = ? AND status = 'pending' AND supersedable = 1
      `).run(
        event.turn_version,
        JSON.stringify(command),
        event.persisted_at,
        pending.outbox_id,
      );
      database.prepare(`
        INSERT INTO runtime_projection_snapshots (
          projection_id, lane_key, turn_id, aggregate_version,
          event_sequence_through, render_model_json, critical, terminal,
          status, materialized_outbox_id, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, 0, 0, 'materialized', ?, ?)
      `).run(
        generateId('projection'),
        lane.lane_key,
        turn.turn_id,
        event.turn_version,
        event.event_sequence,
        JSON.stringify(renderModel),
        pending.outbox_id,
        event.persisted_at,
      );
      return;
    }
    database.prepare(`
      UPDATE runtime_projection_snapshots
      SET status = 'superseded'
      WHERE lane_key = ? AND critical = 0 AND status = 'staged'
    `).run(lane.lane_key);
  }

  database.prepare(`
    INSERT INTO runtime_projection_snapshots (
      projection_id, lane_key, turn_id, aggregate_version,
      event_sequence_through, render_model_json, critical, terminal,
      status, materialized_outbox_id, created_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'staged', NULL, ?)
  `).run(
    generateId('projection'),
    lane.lane_key,
    turn.turn_id,
    event.turn_version,
    event.event_sequence,
    JSON.stringify(renderModel),
    critical ? 1 : 0,
    TERMINAL_PHASES.has(event.phase) ? 1 : 0,
    event.persisted_at,
  );
  materializeNextStagedMainProjection(database, lane.lane_key, {
    generateId,
    throttleMs,
  });
}
