import crypto from 'node:crypto';

import {
  createIdempotencyKey,
  validateDeliveryCommand,
  validateDeliveryResult,
} from '../../contracts/public/index.js';
import {
  materializeNextStagedMainProjection,
} from '../persistence/main-projection.js';
import { createDeliveryLaneKey } from '../persistence/delivery-lane-key.js';
import {
  assertDurableDeliveryTarget,
  resolveDeliveryCommandVersionForTarget,
} from '../persistence/delivery-target-identity.js';
import { initializeRuntimePersistence } from '../persistence/schema.js';

const DELIVERY_RETRY_DELAYS_MS = Object.freeze([2_000, 4_000, 8_000]);

function defaultGenerateId(kind) {
  return `${kind}-${crypto.randomUUID()}`;
}

function addMilliseconds(timestamp, milliseconds) {
  return new Date(Date.parse(timestamp) + milliseconds).toISOString();
}

function isCurrentFence(row, result) {
  return row.delivery_attempt_id === result.delivery_attempt_id
    && row.delivery_attempt_no === result.delivery_attempt_no
    && row.outbox_lease_epoch === result.outbox_lease_epoch;
}

function isCurrentIdentity(command, result) {
  return command.trace_id === result.trace_id
    && command.outbox_id === result.outbox_id
    && command.delivery_id === result.delivery_id
    && command.idempotency_key === result.idempotency_key
    && command.mapping.mapping_id === result.mapping_id
    && command.operation === result.operation
    && command.aggregate_version === result.aggregate_version;
}

function persistDeliveredMapping(database, command, result) {
  const mapping = command.mapping;
  const existing = database.prepare(`
    SELECT mapping_id, platform_message_id
    FROM runtime_message_mappings
    WHERE mapping_id = ? OR (
      region = ? AND tenant_id = ? AND channel = ? AND bot_id = ?
      AND platform_message_id = ?
    )
  `).get(
    mapping.mapping_id,
    command.target.region,
    command.target.tenant_id,
    command.target.channel,
    command.target.bot_id,
    result.platform_message_id,
  );
  if (existing) {
    if (
      existing.mapping_id !== mapping.mapping_id
      || existing.platform_message_id !== result.platform_message_id
    ) {
      throw new Error('A delivered platform message conflicts with an existing mapping.');
    }
    return;
  }
  database.prepare(`
    INSERT INTO runtime_message_mappings (
      region, tenant_id, channel, bot_id, platform_message_id,
      conversation_id, turn_id, lineage_id, binding_state, reason,
      mapping_id, mapping_version, created_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).run(
    command.target.region,
    command.target.tenant_id,
    command.target.channel,
    command.target.bot_id,
    result.platform_message_id,
    mapping.conversation_id,
    mapping.turn_id,
    mapping.lineage_id,
    mapping.binding_state,
    mapping.reason ?? null,
    mapping.mapping_id,
    mapping.mapping_version,
    result.delivered_at,
  );
}

function enqueueInitialTextAcknowledgement(database, command, resultAt, generateId) {
  const aggregateId = `${command.aggregate_id}-receipt`;
  const existing = database.prepare(`
    SELECT outbox_id
    FROM runtime_outbox
    WHERE aggregate_type = 'text_notice' AND aggregate_id = ?
    LIMIT 1
  `).get(aggregateId);
  if (existing) return;

  const outboxId = generateId('outbox');
  const deliveryId = generateId('delivery');
  const acknowledgement = {
    contract: 'zylos.delivery-command',
    contract_version: resolveDeliveryCommandVersionForTarget(command.target),
    outbox_id: outboxId,
    delivery_id: deliveryId,
    trace_id: generateId('delivery-trace'),
    delivery_attempt_id: generateId('delivery-attempt'),
    delivery_attempt_no: 1,
    outbox_lease_epoch: 1,
    target: command.target,
    aggregate_type: 'text_notice',
    aggregate_id: aggregateId,
    operation: 'send_text',
    aggregate_version: 1,
    event_sequence_through: command.event_sequence_through,
    idempotency_key: createIdempotencyKey('delivery', {
      channel: command.target.channel,
      target: command.target,
      delivery_id: deliveryId,
    }),
    render_model: {
      title: 'Zylos',
      phase: 'received',
      text: 'Message received. Rich delivery is temporarily unavailable.',
      error: null,
      tools: [],
      interactions: [],
      terminal: false,
      user_action_required: false,
    },
    mapping: {
      ...command.mapping,
      mapping_id: generateId('mapping'),
    },
    target_platform_message_id: null,
    predecessor_delivery_id: null,
    expected_platform_version: null,
    priority: 100,
    not_before: resultAt,
    created_at: resultAt,
  };
  validateDeliveryCommand(acknowledgement, { occurredAt: resultAt });
  const sourceLaneKey = database.prepare(`
    SELECT lane_key
    FROM runtime_outbox
    WHERE outbox_id = ?
  `).get(command.outbox_id)?.lane_key ?? null;
  database.prepare(`
    INSERT INTO runtime_outbox (
      outbox_id, delivery_id, aggregate_type, aggregate_id, turn_id, control_id,
      lane_key, predecessor_delivery_id, aggregate_version, status, command_json,
      priority, supersedable, terminal, next_attempt_at, created_at, updated_at
    ) VALUES (?, ?, ?, ?, ?, NULL, ?, NULL, 1, 'pending', ?, ?, 0, 0, ?, ?, ?)
  `).run(
    outboxId,
    deliveryId,
    acknowledgement.aggregate_type,
    aggregateId,
    command.mapping.turn_id,
    sourceLaneKey ?? createDeliveryLaneKey(acknowledgement),
    JSON.stringify(acknowledgement),
    acknowledgement.priority,
    resultAt,
    resultAt,
    resultAt,
  );
}

function enqueueFinalFallback(database, command, resultAt, generateId) {
  const existing = database.prepare(`
    SELECT command_json
    FROM runtime_outbox
    WHERE predecessor_delivery_id = ? AND status != 'superseded'
  `).all(command.delivery_id).some(({ command_json: commandJson }) => {
    try {
      return JSON.parse(commandJson).operation === 'send_fallback';
    } catch {
      return false;
    }
  });
  if (existing) return;

  const outboxId = generateId('outbox');
  const deliveryId = generateId('delivery');
  const fallback = {
    ...command,
    outbox_id: outboxId,
    delivery_id: deliveryId,
    trace_id: generateId('delivery-trace'),
    delivery_attempt_id: generateId('delivery-attempt'),
    delivery_attempt_no: 1,
    outbox_lease_epoch: 1,
    operation: 'send_fallback',
    idempotency_key: createIdempotencyKey('delivery', {
      channel: command.target.channel,
      target: command.target,
      delivery_id: deliveryId,
    }),
    mapping: {
      ...command.mapping,
      mapping_id: generateId('mapping'),
    },
    target_platform_message_id: null,
    predecessor_delivery_id: command.delivery_id,
    expected_platform_version: null,
    priority: 100,
    not_before: resultAt,
    created_at: resultAt,
  };
  validateDeliveryCommand(fallback, { occurredAt: resultAt });
  const source = database.prepare(`
    SELECT lane_key
    FROM runtime_outbox
    WHERE outbox_id = ?
  `).get(command.outbox_id);
  database.prepare(`
    INSERT INTO runtime_outbox (
      outbox_id, delivery_id, aggregate_type, aggregate_id, turn_id, control_id,
      lane_key, predecessor_delivery_id, aggregate_version, status, command_json,
      priority, supersedable, terminal, next_attempt_at, created_at, updated_at
    ) VALUES (?, ?, ?, ?, ?, NULL, ?, ?, ?, 'pending', ?, ?, 0, 1, ?, ?, ?)
  `).run(
    outboxId,
    deliveryId,
    fallback.aggregate_type,
    fallback.aggregate_id,
    fallback.mapping.turn_id,
    source.lane_key,
    command.delivery_id,
    fallback.aggregate_version,
    JSON.stringify(fallback),
    fallback.priority,
    resultAt,
    resultAt,
    resultAt,
  );
}

export function createOutboxService({
  database,
  renderer,
  serviceInstanceId,
  channel = null,
  targetChatId = null,
  targetRegion = null,
  targetTenantId = null,
  targetBotId = null,
  now = () => new Date().toISOString(),
  generateId = defaultGenerateId,
  leaseDurationMs = 10_000,
  throttleMs = 1_500,
}) {
  if (!database || typeof database.transaction !== 'function') {
    throw new TypeError('database must be a better-sqlite3 connection');
  }
  if (typeof serviceInstanceId !== 'string' || serviceInstanceId.length === 0) {
    throw new TypeError('serviceInstanceId must be a non-empty string');
  }
  if (channel !== null && (typeof channel !== 'string' || channel.length === 0)) {
    throw new TypeError('channel must be a non-empty string or null');
  }
  if (targetChatId !== null
    && (typeof targetChatId !== 'string' || targetChatId.length === 0)) {
    throw new TypeError('targetChatId must be a non-empty string or null');
  }
  for (const [fieldName, value] of [
    ['targetRegion', targetRegion],
    ['targetTenantId', targetTenantId],
    ['targetBotId', targetBotId],
  ]) {
    if (value !== null && (typeof value !== 'string' || value.length === 0)) {
      throw new TypeError(`${fieldName} must be a non-empty string or null`);
    }
  }
  if (typeof generateId !== 'function') {
    throw new TypeError('generateId must be a function');
  }
  if (renderer !== undefined && typeof renderer?.deliver !== 'function') {
    throw new TypeError('renderer.deliver must be a function');
  }
  initializeRuntimePersistence(database);

  function claimNext() {
    const claim = database.transaction(() => {
      const claimedAt = now();
      const row = database.prepare(`
        SELECT candidate.outbox_id, candidate.status, candidate.attempt_count,
          candidate.outbox_lease_epoch, candidate.command_json,
          lane.lane_key AS durable_lane_key, lane.turn_id AS lane_turn_id,
          lane.aggregate_type AS lane_aggregate_type,
          lane.target_json AS lane_target_json
        FROM runtime_outbox AS candidate
        LEFT JOIN runtime_delivery_lanes AS lane
          ON lane.lane_key = candidate.lane_key
        WHERE (
          (
            candidate.status IN ('pending', 'retry_wait')
            AND (candidate.next_attempt_at IS NULL OR candidate.next_attempt_at <= ?)
          ) OR (
            candidate.status = 'delivering'
            AND candidate.lease_expires_at IS NOT NULL
            AND candidate.lease_expires_at <= ?
          )
        )
        AND (
          candidate.predecessor_delivery_id IS NULL
          OR EXISTS (
            SELECT 1
            FROM runtime_outbox AS predecessor
            WHERE predecessor.delivery_id = candidate.predecessor_delivery_id
              AND predecessor.status IN ('delivered', 'superseded', 'dead_letter')
          )
        )
        AND NOT EXISTS (
          SELECT 1
          FROM runtime_outbox AS active
          WHERE active.lane_key = candidate.lane_key
            AND active.status = 'delivering'
            AND active.outbox_id != candidate.outbox_id
            AND active.lease_expires_at > ?
        )
        AND (? IS NULL OR json_extract(candidate.command_json, '$.target.channel') = ?)
        AND (? IS NULL OR json_extract(candidate.command_json, '$.target.chat_id') = ?)
        AND (? IS NULL OR json_extract(candidate.command_json, '$.target.region') = ?)
        AND (? IS NULL OR json_extract(candidate.command_json, '$.target.tenant_id') = ?)
        AND (? IS NULL OR json_extract(candidate.command_json, '$.target.bot_id') = ?)
        ORDER BY candidate.priority DESC, candidate.created_at ASC,
          candidate.aggregate_version ASC, candidate.outbox_id ASC
        LIMIT 1
      `).get(
        claimedAt,
        claimedAt,
        claimedAt,
        channel,
        channel,
        targetChatId,
        targetChatId,
        targetRegion,
        targetRegion,
        targetTenantId,
        targetTenantId,
        targetBotId,
        targetBotId,
      );
      if (!row) return null;

      const deliveryAttemptNo = row.attempt_count + 1;
      const outboxLeaseEpoch = row.outbox_lease_epoch + 1;
      const deliveryAttemptId = generateId('delivery-attempt');
      const command = {
        ...JSON.parse(row.command_json),
        delivery_attempt_id: deliveryAttemptId,
        delivery_attempt_no: deliveryAttemptNo,
        outbox_lease_epoch: outboxLeaseEpoch,
      };
      if (row.lane_target_json !== null) {
        assertDurableDeliveryTarget({
          lane: {
            lane_key: row.durable_lane_key,
            turn_id: row.lane_turn_id,
            aggregate_type: row.lane_aggregate_type,
          },
          targetJson: row.lane_target_json,
          candidateTarget: command.target,
          occurredAt: claimedAt,
        });
      }
      validateDeliveryCommand(command, { occurredAt: claimedAt });
      const leaseExpiresAt = new Date(
        Date.parse(claimedAt) + leaseDurationMs,
      ).toISOString();
      const updated = database.prepare(`
        UPDATE runtime_outbox
        SET status = 'delivering', attempt_count = ?, delivery_attempt_id = ?,
          delivery_attempt_no = ?, outbox_lease_epoch = ?, lease_owner = ?,
          lease_expires_at = ?, last_attempt_at = ?, next_attempt_at = NULL,
          command_json = ?, updated_at = ?
        WHERE outbox_id = ? AND status = ? AND attempt_count = ?
          AND outbox_lease_epoch = ?
      `).run(
        deliveryAttemptNo,
        deliveryAttemptId,
        deliveryAttemptNo,
        outboxLeaseEpoch,
        serviceInstanceId,
        leaseExpiresAt,
        claimedAt,
        JSON.stringify(command),
        claimedAt,
        row.outbox_id,
        row.status,
        row.attempt_count,
        row.outbox_lease_epoch,
      );
      if (updated.changes !== 1) return null;
      return command;
    });
    return claim.immediate();
  }

  function recordResult(result) {
    const apply = database.transaction(() => {
      const row = database.prepare(`
        SELECT status, delivery_attempt_id, delivery_attempt_no, outbox_lease_epoch,
          command_json, result_json, lane.target_json AS lane_target_json
          , lane.lane_key AS durable_lane_key, lane.turn_id AS lane_turn_id
          , lane.aggregate_type AS lane_aggregate_type
        FROM runtime_outbox AS outbox
        LEFT JOIN runtime_delivery_lanes AS lane ON lane.lane_key = outbox.lane_key
        WHERE outbox.outbox_id = ?
      `).get(result.outbox_id);
      if (!row) return { status: 'stale' };
      const command = JSON.parse(row.command_json);
      if (row.lane_target_json !== null) {
        assertDurableDeliveryTarget({
          lane: {
            lane_key: row.durable_lane_key,
            turn_id: row.lane_turn_id,
            aggregate_type: row.lane_aggregate_type,
          },
          targetJson: row.lane_target_json,
          candidateTarget: command.target,
          occurredAt: result.result_at,
        });
      }
      if (row.result_json === JSON.stringify(result)) {
        return { status: 'duplicate', outbox_status: row.status };
      }
      if (
        row.status !== 'delivering'
        || !isCurrentFence(row, result)
        || !isCurrentIdentity(command, result)
      ) {
        return { status: 'stale' };
      }
      validateDeliveryResult(result, { command, occurredAt: result.result_at });
      if (result.status !== 'delivered') {
        if (command.operation === 'create_main') {
          enqueueInitialTextAcknowledgement(database, command, result.result_at, generateId);
        }
        const terminalUpdate = command.operation === 'update_main'
          && command.render_model.terminal;
        const retryDelay = DELIVERY_RETRY_DELAYS_MS[result.delivery_attempt_no - 1];
        const shouldRetry = result.status === 'retryable_failure'
          && retryDelay !== undefined;
        const outboxStatus = result.status === 'obsolete'
          ? 'superseded'
          : (shouldRetry ? 'retry_wait' : 'dead_letter');
        const nextAttemptAt = shouldRetry
          ? addMilliseconds(result.result_at, retryDelay)
          : null;
        const updated = database.prepare(`
          UPDATE runtime_outbox
          SET status = ?, lease_owner = NULL, lease_expires_at = NULL,
            next_attempt_at = ?, result_json = ?, last_error_json = ?, updated_at = ?
          WHERE outbox_id = ? AND status = 'delivering'
            AND delivery_attempt_id = ? AND delivery_attempt_no = ?
            AND outbox_lease_epoch = ?
        `).run(
          outboxStatus,
          nextAttemptAt,
          JSON.stringify(result),
          JSON.stringify(result.error),
          result.result_at,
          result.outbox_id,
          result.delivery_attempt_id,
          result.delivery_attempt_no,
          result.outbox_lease_epoch,
        );
        if (updated.changes !== 1) return { status: 'stale' };
        if (outboxStatus === 'dead_letter' && terminalUpdate) {
          enqueueFinalFallback(database, command, result.result_at, generateId);
        } else if (['dead_letter', 'superseded'].includes(outboxStatus)) {
          const laneKey = database.prepare(`
            SELECT lane_key
            FROM runtime_outbox
            WHERE outbox_id = ?
          `).get(result.outbox_id)?.lane_key;
          if (laneKey !== null && laneKey !== undefined) {
            materializeNextStagedMainProjection(database, laneKey, {
              generateId,
              throttleMs,
            });
          }
        }
        return { status: 'applied', outbox_status: outboxStatus };
      }

      persistDeliveredMapping(database, command, result);
      if (command.aggregate_type === 'turn_main') {
        database.prepare(`
          UPDATE runtime_delivery_lanes
          SET mapping_json = ?, platform_message_id = ?, applied_platform_version = ?,
            last_delivery_id = ?, last_applied_version = ?, last_delivered_at = ?,
            updated_at = ?
          WHERE lane_key = (
            SELECT lane_key FROM runtime_outbox WHERE outbox_id = ?
          )
        `).run(
          JSON.stringify(command.mapping),
          result.platform_message_id,
          result.applied_platform_version,
          command.delivery_id,
          command.aggregate_version,
          result.delivered_at,
          result.result_at,
          result.outbox_id,
        );
      }
      const updated = database.prepare(`
        UPDATE runtime_outbox
        SET status = 'delivered', lease_owner = NULL, lease_expires_at = NULL,
          result_json = ?, last_error_json = NULL, updated_at = ?
        WHERE outbox_id = ? AND status = 'delivering'
          AND delivery_attempt_id = ? AND delivery_attempt_no = ?
          AND outbox_lease_epoch = ?
      `).run(
        JSON.stringify(result),
        result.result_at,
        result.outbox_id,
        result.delivery_attempt_id,
        result.delivery_attempt_no,
        result.outbox_lease_epoch,
      );
      if (updated.changes !== 1) return { status: 'stale' };
      const laneKey = database.prepare(`
        SELECT lane_key
        FROM runtime_outbox
        WHERE outbox_id = ?
      `).get(result.outbox_id)?.lane_key;
      if (laneKey !== null && laneKey !== undefined) {
        materializeNextStagedMainProjection(database, laneKey, {
          generateId,
          throttleMs,
        });
      }
      return { status: 'applied', outbox_status: 'delivered' };
    });
    return apply.immediate();
  }

  async function dispatchNext() {
    if (!renderer) throw new Error('dispatchNext requires a renderer');
    const command = claimNext();
    if (!command) return { status: 'idle' };
    const result = await renderer.deliver(command);
    return recordResult(result);
  }

  return Object.freeze({ claimNext, dispatchNext, recordResult });
}
