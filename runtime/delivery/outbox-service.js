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

function parseAuthorityEpochMs(timestamp) {
  const epochMs = Date.parse(timestamp);
  if (!Number.isFinite(epochMs)) {
    throw new TypeError('now() must return an RFC3339 timestamp');
  }
  return epochMs;
}

function createCommandSnapshotHash(commandJson) {
  return crypto.createHash('sha256').update(commandJson).digest('hex');
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
  expiredClaimRecovery = 'fenced',
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
  if (!['fenced', 'same_delivery_id'].includes(expiredClaimRecovery)) {
    throw new TypeError('expiredClaimRecovery must be fenced or same_delivery_id');
  }
  initializeRuntimePersistence(database);

  function claimNext() {
    const claim = database.transaction(() => {
      const claimedAt = now();
      const claimedAtEpochMs = parseAuthorityEpochMs(claimedAt);
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
            AND (
              candidate.next_attempt_at IS NULL
              OR julianday(candidate.next_attempt_at) <= julianday(?)
            )
          ) OR (
            candidate.status = 'delivering'
            AND candidate.lease_expires_epoch_ms IS NOT NULL
            AND candidate.lease_expires_epoch_ms <= ?
            AND (
              candidate.pre_action_fenced_at IS NULL
              OR ? = 'same_delivery_id'
            )
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
            AND active.lease_expires_epoch_ms > ?
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
        claimedAtEpochMs,
        expiredClaimRecovery,
        claimedAtEpochMs,
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
      const commandJson = JSON.stringify(command);
      const claimedCommandHash = createCommandSnapshotHash(commandJson);
      const leaseExpiresAt = new Date(
        claimedAtEpochMs + leaseDurationMs,
      ).toISOString();
      const leaseExpiresEpochMs = claimedAtEpochMs + leaseDurationMs;
      const updated = database.prepare(`
        UPDATE runtime_outbox
        SET status = 'delivering', attempt_count = ?, delivery_attempt_id = ?,
          delivery_attempt_no = ?, outbox_lease_epoch = ?, lease_owner = ?,
          lease_expires_at = ?, lease_expires_epoch_ms = ?,
          last_attempt_at = ?, next_attempt_at = NULL,
          pre_action_fenced_at = NULL, command_json = ?, claimed_command_hash = ?,
          updated_at = ?
        WHERE outbox_id = ? AND status = ? AND attempt_count = ?
          AND outbox_lease_epoch = ?
      `).run(
        deliveryAttemptNo,
        deliveryAttemptId,
        deliveryAttemptNo,
        outboxLeaseEpoch,
        serviceInstanceId,
        leaseExpiresAt,
        leaseExpiresEpochMs,
        claimedAt,
        commandJson,
        claimedCommandHash,
        claimedAt,
        row.outbox_id,
        row.status,
        row.attempt_count,
        row.outbox_lease_epoch,
      );
      if (updated.changes !== 1) return null;
      database.prepare(`
        INSERT INTO runtime_outbox_claim_snapshots (
          outbox_id, delivery_attempt_id, delivery_attempt_no, outbox_lease_epoch,
          lease_owner, command_json, command_hash, claimed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      `).run(
        command.outbox_id,
        deliveryAttemptId,
        deliveryAttemptNo,
        outboxLeaseEpoch,
        serviceInstanceId,
        commandJson,
        claimedCommandHash,
        claimedAt,
      );
      return command;
    });
    return claim.immediate();
  }

  function assertCurrentClaim(command, { sideEffectBoundary = true } = {}) {
    const commandJson = JSON.stringify(command);
    const claimedCommandHash = createCommandSnapshotHash(commandJson);
    const renew = database.transaction(() => {
      const renewedAt = now();
      const renewedAtEpochMs = parseAuthorityEpochMs(renewedAt);
      const renewedLeaseExpiresEpochMs = renewedAtEpochMs + leaseDurationMs;
      const renewedLeaseExpiresAt = new Date(renewedLeaseExpiresEpochMs).toISOString();
      return database.prepare(`
        UPDATE runtime_outbox
        SET lease_expires_at = ?, lease_expires_epoch_ms = ?,
          pre_action_fenced_at = CASE WHEN ? = 1
            THEN COALESCE(pre_action_fenced_at, ?)
            ELSE pre_action_fenced_at
          END,
          updated_at = ?
        WHERE outbox_id = ? AND status = 'delivering'
          AND delivery_id = ? AND aggregate_type = ? AND aggregate_id = ?
          AND turn_id IS ? AND aggregate_version = ?
          AND delivery_attempt_id = ? AND delivery_attempt_no = ?
          AND outbox_lease_epoch = ? AND lease_owner = ?
          AND command_json = ? AND claimed_command_hash = ?
          AND lease_expires_epoch_ms IS NOT NULL AND lease_expires_epoch_ms > ?
          AND EXISTS (
            SELECT 1 FROM runtime_outbox_claim_snapshots AS snapshot
            WHERE snapshot.outbox_id = runtime_outbox.outbox_id
              AND snapshot.delivery_attempt_id = runtime_outbox.delivery_attempt_id
              AND snapshot.delivery_attempt_no = runtime_outbox.delivery_attempt_no
              AND snapshot.outbox_lease_epoch = runtime_outbox.outbox_lease_epoch
              AND snapshot.lease_owner = runtime_outbox.lease_owner
              AND snapshot.command_json = ? AND snapshot.command_hash = ?
          )
      `).run(
        renewedLeaseExpiresAt,
        renewedLeaseExpiresEpochMs,
        sideEffectBoundary ? 1 : 0,
        renewedAt,
        renewedAt,
        command.outbox_id,
        command.delivery_id,
        command.aggregate_type,
        command.aggregate_id,
        command.mapping.turn_id,
        command.aggregate_version,
        command.delivery_attempt_id,
        command.delivery_attempt_no,
        command.outbox_lease_epoch,
        serviceInstanceId,
        commandJson,
        claimedCommandHash,
        renewedAtEpochMs,
        commandJson,
        claimedCommandHash,
      );
    });
    if (renew.immediate().changes !== 1) {
      const error = new Error('The durable delivery claim is stale.');
      error.code = 'stale_delivery_claim';
      throw error;
    }
    return command;
  }

  function recordResult(result) {
    const apply = database.transaction(() => {
      const appliedAt = now();
      const appliedAtEpochMs = parseAuthorityEpochMs(appliedAt);
      const row = database.prepare(`
        SELECT outbox.status, outbox.delivery_attempt_id, outbox.delivery_attempt_no,
          outbox.outbox_lease_epoch, outbox.lease_owner, outbox.lease_expires_at,
          outbox.lease_expires_epoch_ms, outbox.pre_action_fenced_at,
          outbox.command_json, outbox.claimed_command_hash, outbox.result_json,
          snapshot.command_json AS claim_command_json,
          snapshot.command_hash AS claim_command_hash,
          snapshot.lease_owner AS claim_lease_owner,
          lane.target_json AS lane_target_json
          , lane.lane_key AS durable_lane_key, lane.turn_id AS lane_turn_id
          , lane.aggregate_type AS lane_aggregate_type
        FROM runtime_outbox AS outbox
        LEFT JOIN runtime_delivery_lanes AS lane ON lane.lane_key = outbox.lane_key
        LEFT JOIN runtime_outbox_claim_snapshots AS snapshot
          ON snapshot.outbox_id = outbox.outbox_id
          AND snapshot.delivery_attempt_id = outbox.delivery_attempt_id
          AND snapshot.delivery_attempt_no = outbox.delivery_attempt_no
          AND snapshot.outbox_lease_epoch = outbox.outbox_lease_epoch
        WHERE outbox.outbox_id = ?
      `).get(result.outbox_id);
      if (!row) return { status: 'stale' };
      if (row.claim_command_hash === null
        || createCommandSnapshotHash(row.claim_command_json) !== row.claim_command_hash
        || row.command_json !== row.claim_command_json
        || row.claimed_command_hash !== row.claim_command_hash) {
        return { status: 'stale' };
      }
      let command;
      try {
        command = JSON.parse(row.claim_command_json);
      } catch {
        return { status: 'stale' };
      }
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
      const currentFence = isCurrentFence(row, result);
      const currentIdentity = isCurrentIdentity(command, result);
      if (row.result_json === JSON.stringify(result)) {
        if (!currentFence || !currentIdentity) return { status: 'stale' };
        validateDeliveryResult(result, { command, occurredAt: result.result_at });
        return { status: 'duplicate', outbox_status: row.status };
      }
      if (
        row.status !== 'delivering'
        || row.lease_owner !== serviceInstanceId
        || row.claim_lease_owner !== row.lease_owner
        || row.lease_expires_epoch_ms === null
        || row.lease_expires_epoch_ms <= appliedAtEpochMs
        || !currentFence
        || !currentIdentity
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
            lease_expires_epoch_ms = NULL,
            next_attempt_at = ?, result_json = ?, last_error_json = ?, updated_at = ?
          WHERE outbox_id = ? AND status = 'delivering'
            AND delivery_attempt_id = ? AND delivery_attempt_no = ?
            AND outbox_lease_epoch = ?
            AND lease_owner = ? AND lease_expires_epoch_ms > ?
            AND command_json = ? AND claimed_command_hash = ?
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
          serviceInstanceId,
          appliedAtEpochMs,
          row.claim_command_json,
          row.claim_command_hash,
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
          lease_expires_epoch_ms = NULL,
          result_json = ?, last_error_json = NULL, updated_at = ?
        WHERE outbox_id = ? AND status = 'delivering'
          AND delivery_attempt_id = ? AND delivery_attempt_no = ?
          AND outbox_lease_epoch = ?
          AND lease_owner = ? AND lease_expires_epoch_ms > ?
          AND command_json = ? AND claimed_command_hash = ?
      `).run(
        JSON.stringify(result),
        result.result_at,
        result.outbox_id,
        result.delivery_attempt_id,
        result.delivery_attempt_no,
        result.outbox_lease_epoch,
        serviceInstanceId,
        appliedAtEpochMs,
        row.claim_command_json,
        row.claim_command_hash,
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
    assertCurrentClaim(command, { sideEffectBoundary: false });
    const result = await renderer.deliver(command);
    return recordResult(result);
  }

  return Object.freeze({ claimNext, assertCurrentClaim, dispatchNext, recordResult });
}
