import {
  canonicalizeJson,
  ContractKernelError,
  createContractError,
  DELIVERY_COMMAND_CURRENT_VERSION,
} from '../../contracts/public/index.js';
import { createDeliveryLaneKeyFromIdentity } from './delivery-lane-key.js';

function targetVersionConflict(occurredAt) {
  return new ContractKernelError(createContractError({
    code: 'version_conflict',
    category: 'conflict',
    userMessage: 'The delivery target does not match the durable lane target.',
    occurredAt,
  }));
}

export function parseDurableDeliveryTarget(targetJson, { occurredAt } = {}) {
  try {
    return JSON.parse(targetJson);
  } catch {
    throw targetVersionConflict(occurredAt);
  }
}

export function assertDeliveryTargetIdentity(expected, candidate, { occurredAt } = {}) {
  try {
    if (canonicalizeJson(expected) === canonicalizeJson(candidate)) return;
  } catch {
    // Invalid persisted or candidate target data is a target identity conflict.
  }
  throw targetVersionConflict(occurredAt);
}

export function assertDeliveryLaneIdentity(lane, target, { occurredAt } = {}) {
  let expectedLaneKey;
  try {
    expectedLaneKey = createDeliveryLaneKeyFromIdentity({
      target,
      turnId: lane.turn_id,
      aggregateType: lane.aggregate_type,
    });
  } catch {
    throw targetVersionConflict(occurredAt);
  }
  if (lane.lane_key !== expectedLaneKey) throw targetVersionConflict(occurredAt);
}

export function assertDurableDeliveryTarget({
  lane,
  targetJson,
  candidateTarget,
  occurredAt,
}) {
  const durableTarget = parseDurableDeliveryTarget(targetJson, { occurredAt });
  assertDeliveryLaneIdentity(lane, durableTarget, { occurredAt });
  if (candidateTarget !== undefined) {
    assertDeliveryTargetIdentity(durableTarget, candidateTarget, { occurredAt });
  }
  return durableTarget;
}

export function resolveDeliveryCommandVersionForTarget(target) {
  if (
    Object.hasOwn(target, 'reply_target_message_id')
    || Object.hasOwn(target, 'mention_actor_id')
  ) {
    return DELIVERY_COMMAND_CURRENT_VERSION;
  }
  if (
    Object.hasOwn(target, 'native_thread_root_message_id')
    || Object.hasOwn(target, 'native_thread_reply_target_message_id')
  ) {
    return '1.1';
  }
  return '1.0';
}
