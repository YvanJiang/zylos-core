import crypto from 'node:crypto';

import {
  materializeNextStagedMainProjection,
} from '../persistence/main-projection.js';
import { initializeRuntimePersistence } from '../persistence/schema.js';
import { enqueueFinalFallback } from './outbox-service.js';

const SHA256_PATTERN = /^[a-f0-9]{64}$/;

function defaultGenerateId(kind) {
  return `${kind}-${crypto.randomUUID()}`;
}

function fail(code, message) {
  const error = new Error(message);
  error.code = code;
  throw error;
}

function requireText(name, value) {
  if (typeof value !== 'string' || value.length === 0) {
    throw new TypeError(`${name} must be a non-empty string`);
  }
  return value;
}

function requirePositiveInteger(name, value) {
  if (!Number.isSafeInteger(value) || value < 1) {
    throw new TypeError(`${name} must be a positive safe integer`);
  }
  return value;
}

function requireTimestamp(name, value) {
  requireText(name, value);
  if (!Number.isFinite(Date.parse(value))) {
    throw new TypeError(`${name} must be an RFC3339 timestamp`);
  }
  return value;
}

function requireSha256(name, value) {
  requireText(name, value);
  const normalized = value.toLowerCase();
  if (!SHA256_PATTERN.test(normalized)) {
    throw new TypeError(`${name} must be a SHA-256 hex digest`);
  }
  return normalized;
}

function sha256(value) {
  return crypto.createHash('sha256').update(value).digest('hex');
}

function normalizeExpected(expected) {
  return Object.freeze({
    outbox_id: requireText('expected.outbox_id', expected?.outbox_id),
    delivery_attempt_id: requireText(
      'expected.delivery_attempt_id',
      expected?.delivery_attempt_id,
    ),
    delivery_attempt_no: requirePositiveInteger(
      'expected.delivery_attempt_no',
      expected?.delivery_attempt_no,
    ),
    outbox_lease_epoch: requirePositiveInteger(
      'expected.outbox_lease_epoch',
      expected?.outbox_lease_epoch,
    ),
    claimed_command_hash: requireSha256(
      'expected.claimed_command_hash',
      expected?.claimed_command_hash,
    ),
    aggregate_version: requirePositiveInteger(
      'expected.aggregate_version',
      expected?.aggregate_version,
    ),
  });
}

function normalizeEvidence(evidence) {
  if (evidence?.kind !== 'platform_readback_no_effect') {
    throw new TypeError('evidence.kind must be platform_readback_no_effect');
  }
  if (evidence.query_succeeded !== true || evidence.expected_effect_present !== false) {
    throw new TypeError('evidence must prove a successful readback with no expected effect');
  }
  const normalized = Object.freeze({
    kind: evidence.kind,
    observed_at: requireTimestamp('evidence.observed_at', evidence.observed_at),
    platform_message_id: requireText(
      'evidence.platform_message_id',
      evidence.platform_message_id,
    ),
    query_succeeded: true,
    expected_effect_present: false,
    expected_delivery_hash: requireSha256(
      'evidence.expected_delivery_hash',
      evidence.expected_delivery_hash,
    ),
    observed_delivery_hash: requireSha256(
      'evidence.observed_delivery_hash',
      evidence.observed_delivery_hash,
    ),
  });
  if (normalized.expected_delivery_hash === normalized.observed_delivery_hash) {
    throw new TypeError('evidence hashes must differ when the expected effect is absent');
  }
  return normalized;
}

function normalizeAuthorization(authorization) {
  return Object.freeze({
    actor_id: requireText('authorization.actor_id', authorization?.actor_id),
    authorization_ref: requireText(
      'authorization.authorization_ref',
      authorization?.authorization_ref,
    ),
    reason: requireText('authorization.reason', authorization?.reason),
  });
}

export function reconcileOutboxNoEffect({
  database,
  expected,
  evidence,
  authorization,
  now = () => new Date().toISOString(),
  generateId = defaultGenerateId,
  throttleMs = 1_500,
} = {}) {
  if (!database || typeof database.transaction !== 'function') {
    throw new TypeError('database must be a better-sqlite3 connection');
  }
  if (typeof generateId !== 'function') {
    throw new TypeError('generateId must be a function');
  }
  const normalizedExpected = normalizeExpected(expected);
  const normalizedEvidence = normalizeEvidence(evidence);
  const normalizedAuthorization = normalizeAuthorization(authorization);
  const evidenceJson = JSON.stringify(normalizedEvidence);
  const evidenceHash = sha256(evidenceJson);
  const requestHash = sha256(JSON.stringify({
    expected: normalizedExpected,
    evidence: normalizedEvidence,
    authorization: normalizedAuthorization,
  }));

  initializeRuntimePersistence(database);
  const reconcile = database.transaction(() => {
    const existing = database.prepare(`
      SELECT reconciliation_id, request_hash, terminal_status, replacement_outbox_id
      FROM runtime_outbox_operator_reconciliations
      WHERE outbox_id = ? AND delivery_attempt_id = ?
        AND delivery_attempt_no = ? AND outbox_lease_epoch = ?
    `).get(
      normalizedExpected.outbox_id,
      normalizedExpected.delivery_attempt_id,
      normalizedExpected.delivery_attempt_no,
      normalizedExpected.outbox_lease_epoch,
    );
    if (existing) {
      if (existing.request_hash !== requestHash) {
        fail(
          'outbox_reconciliation_conflict',
          'The delivery attempt was already reconciled with different evidence.',
        );
      }
      return Object.freeze({
        status: 'duplicate',
        outbox_status: existing.terminal_status,
        reconciliation_id: existing.reconciliation_id,
        replacement_outbox_id: existing.replacement_outbox_id,
      });
    }

    const committedAt = requireTimestamp('now()', now());
    const committedAtEpochMs = Date.parse(committedAt);
    const row = database.prepare(`
      SELECT outbox.status, outbox.delivery_attempt_id, outbox.delivery_attempt_no,
        outbox.outbox_lease_epoch, outbox.lease_owner, outbox.lease_expires_at,
        outbox.lease_expires_epoch_ms, outbox.pre_action_fenced_at,
        outbox.command_json, outbox.claimed_command_hash, outbox.aggregate_version,
        outbox.lane_key,
        snapshot.command_json AS snapshot_command_json,
        snapshot.command_hash AS snapshot_command_hash,
        snapshot.lease_owner AS snapshot_lease_owner
      FROM runtime_outbox AS outbox
      LEFT JOIN runtime_outbox_claim_snapshots AS snapshot
        ON snapshot.outbox_id = outbox.outbox_id
        AND snapshot.delivery_attempt_id = outbox.delivery_attempt_id
        AND snapshot.delivery_attempt_no = outbox.delivery_attempt_no
        AND snapshot.outbox_lease_epoch = outbox.outbox_lease_epoch
      WHERE outbox.outbox_id = ?
    `).get(normalizedExpected.outbox_id);
    if (!row) fail('outbox_reconciliation_not_found', 'The outbox record does not exist.');

    const exactFence = row.delivery_attempt_id === normalizedExpected.delivery_attempt_id
      && row.delivery_attempt_no === normalizedExpected.delivery_attempt_no
      && row.outbox_lease_epoch === normalizedExpected.outbox_lease_epoch
      && row.claimed_command_hash === normalizedExpected.claimed_command_hash
      && row.aggregate_version === normalizedExpected.aggregate_version;
    if (!exactFence) {
      fail('outbox_reconciliation_conflict', 'The outbox delivery fence no longer matches.');
    }
    const verifiedSnapshot = row.snapshot_command_json !== null
      && row.snapshot_lease_owner === row.lease_owner
      && row.snapshot_command_json === row.command_json
      && row.snapshot_command_hash === row.claimed_command_hash
      && sha256(row.snapshot_command_json) === row.snapshot_command_hash;
    if (!verifiedSnapshot) {
      fail(
        'outbox_reconciliation_unverifiable',
        'The immutable delivery claim snapshot cannot be verified.',
      );
    }
    const effectiveUnknown = row.status === 'delivery_unknown'
      || (
        row.status === 'delivering'
        && row.pre_action_fenced_at !== null
        && row.lease_expires_epoch_ms !== null
        && row.lease_expires_epoch_ms <= committedAtEpochMs
      );
    if (!effectiveUnknown) {
      fail(
        'outbox_reconciliation_not_unknown',
        'Only an expired, side-effect-fenced delivery may be reconciled.',
      );
    }

    let command;
    try {
      command = JSON.parse(row.snapshot_command_json);
    } catch {
      fail(
        'outbox_reconciliation_unverifiable',
        'The immutable delivery command snapshot is invalid.',
      );
    }
    if (command.operation !== 'update_main') {
      fail(
        'outbox_reconciliation_unsupported_operation',
        'Only a read-back-verifiable main-message update may be reconciled as no effect.',
      );
    }
    if (command.target_platform_message_id !== normalizedEvidence.platform_message_id) {
      fail(
        'outbox_reconciliation_evidence_mismatch',
        'The platform readback does not identify the claimed delivery target.',
      );
    }

    const reconciliationId = generateId('outbox-reconciliation');
    const errorJson = JSON.stringify({
      code: 'authorized_platform_readback_no_effect',
      category: 'channel',
      retryable: false,
      side_effect_status: 'none',
      user_message: 'A read-only platform comparison proved that the attempted update was absent.',
      occurred_at: committedAt,
      reconciliation_id: reconciliationId,
      evidence_hash: evidenceHash,
    });
    const updated = database.prepare(`
      UPDATE runtime_outbox
      SET status = 'superseded', lease_owner = NULL, lease_expires_at = NULL,
        lease_expires_epoch_ms = NULL, next_attempt_at = NULL,
        last_error_json = ?, updated_at = ?
      WHERE outbox_id = ? AND status = ?
        AND delivery_attempt_id = ? AND delivery_attempt_no = ?
        AND outbox_lease_epoch = ? AND lease_owner IS ?
        AND lease_expires_at IS ? AND lease_expires_epoch_ms IS ?
        AND pre_action_fenced_at IS ?
        AND command_json = ? AND claimed_command_hash = ?
        AND aggregate_version = ?
    `).run(
      errorJson,
      committedAt,
      normalizedExpected.outbox_id,
      row.status,
      normalizedExpected.delivery_attempt_id,
      normalizedExpected.delivery_attempt_no,
      normalizedExpected.outbox_lease_epoch,
      row.lease_owner,
      row.lease_expires_at,
      row.lease_expires_epoch_ms,
      row.pre_action_fenced_at,
      row.command_json,
      normalizedExpected.claimed_command_hash,
      normalizedExpected.aggregate_version,
    );
    if (updated.changes !== 1) {
      fail('outbox_reconciliation_conflict', 'The outbox record changed during reconciliation.');
    }

    const replacement = row.lane_key === null
      ? null
      : materializeNextStagedMainProjection(database, row.lane_key, {
        generateId,
        throttleMs,
      });
    database.prepare(`
      INSERT INTO runtime_outbox_operator_reconciliations (
        reconciliation_id, request_hash, outbox_id, delivery_attempt_id,
        delivery_attempt_no, outbox_lease_epoch, decision, previous_status,
        terminal_status, actor_id, authorization_ref, reason, evidence_json,
        evidence_hash, replacement_outbox_id, committed_at
      ) VALUES (?, ?, ?, ?, ?, ?, 'platform_readback_no_effect', ?,
        'superseded', ?, ?, ?, ?, ?, ?, ?)
    `).run(
      reconciliationId,
      requestHash,
      normalizedExpected.outbox_id,
      normalizedExpected.delivery_attempt_id,
      normalizedExpected.delivery_attempt_no,
      normalizedExpected.outbox_lease_epoch,
      row.status,
      normalizedAuthorization.actor_id,
      normalizedAuthorization.authorization_ref,
      normalizedAuthorization.reason,
      evidenceJson,
      evidenceHash,
      replacement?.outbox_id ?? null,
      committedAt,
    );
    return Object.freeze({
      status: 'applied',
      outbox_status: 'superseded',
      reconciliation_id: reconciliationId,
      replacement_outbox_id: replacement?.outbox_id ?? null,
    });
  });
  return reconcile.immediate();
}

export function correctDeliveredOutboxNoEffect({
  database,
  expected,
  evidence,
  authorization,
  now = () => new Date().toISOString(),
  generateId = defaultGenerateId,
} = {}) {
  if (!database || typeof database.transaction !== 'function') {
    throw new TypeError('database must be a better-sqlite3 connection');
  }
  if (typeof generateId !== 'function') {
    throw new TypeError('generateId must be a function');
  }
  const normalizedExpected = normalizeExpected(expected);
  const normalizedEvidence = normalizeEvidence(evidence);
  const normalizedAuthorization = normalizeAuthorization(authorization);
  const evidenceJson = JSON.stringify(normalizedEvidence);
  const evidenceHash = sha256(evidenceJson);
  const requestHash = sha256(JSON.stringify({
    expected: normalizedExpected,
    evidence: normalizedEvidence,
    authorization: normalizedAuthorization,
  }));

  initializeRuntimePersistence(database);
  const correct = database.transaction(() => {
    const existing = database.prepare(`
      SELECT correction_id, request_hash, terminal_status, fallback_outbox_id
      FROM runtime_outbox_delivery_corrections
      WHERE outbox_id = ? AND delivery_attempt_id = ?
        AND delivery_attempt_no = ? AND outbox_lease_epoch = ?
    `).get(
      normalizedExpected.outbox_id,
      normalizedExpected.delivery_attempt_id,
      normalizedExpected.delivery_attempt_no,
      normalizedExpected.outbox_lease_epoch,
    );
    if (existing) {
      if (existing.request_hash !== requestHash) {
        fail(
          'outbox_delivery_correction_conflict',
          'The delivered attempt was already corrected with different evidence.',
        );
      }
      return Object.freeze({
        status: 'duplicate',
        outbox_status: existing.terminal_status,
        correction_id: existing.correction_id,
        fallback_outbox_id: existing.fallback_outbox_id,
      });
    }

    const committedAt = requireTimestamp('now()', now());
    const row = database.prepare(`
      SELECT outbox.status, outbox.delivery_attempt_id, outbox.delivery_attempt_no,
        outbox.outbox_lease_epoch, outbox.command_json, outbox.claimed_command_hash,
        outbox.aggregate_version, outbox.result_json,
        snapshot.command_json AS snapshot_command_json,
        snapshot.command_hash AS snapshot_command_hash
      FROM runtime_outbox AS outbox
      LEFT JOIN runtime_outbox_claim_snapshots AS snapshot
        ON snapshot.outbox_id = outbox.outbox_id
        AND snapshot.delivery_attempt_id = outbox.delivery_attempt_id
        AND snapshot.delivery_attempt_no = outbox.delivery_attempt_no
        AND snapshot.outbox_lease_epoch = outbox.outbox_lease_epoch
      WHERE outbox.outbox_id = ?
    `).get(normalizedExpected.outbox_id);
    if (!row) fail(
      'outbox_delivery_correction_not_found',
      'The delivered outbox record does not exist.',
    );

    const exactFence = row.delivery_attempt_id === normalizedExpected.delivery_attempt_id
      && row.delivery_attempt_no === normalizedExpected.delivery_attempt_no
      && row.outbox_lease_epoch === normalizedExpected.outbox_lease_epoch
      && row.claimed_command_hash === normalizedExpected.claimed_command_hash
      && row.aggregate_version === normalizedExpected.aggregate_version;
    if (!exactFence) {
      fail(
        'outbox_delivery_correction_conflict',
        'The delivered outbox fence no longer matches.',
      );
    }
    const verifiedSnapshot = row.snapshot_command_json !== null
      && row.snapshot_command_json === row.command_json
      && row.snapshot_command_hash === row.claimed_command_hash
      && sha256(row.snapshot_command_json) === row.snapshot_command_hash;
    if (!verifiedSnapshot) {
      fail(
        'outbox_delivery_correction_unverifiable',
        'The immutable delivered command snapshot cannot be verified.',
      );
    }
    if (row.status !== 'delivered' || row.result_json === null) {
      fail(
        'outbox_delivery_correction_not_delivered',
        'Only an exact delivered attempt may be corrected from platform readback.',
      );
    }

    let command;
    let previousResult;
    try {
      command = JSON.parse(row.snapshot_command_json);
      previousResult = JSON.parse(row.result_json);
    } catch {
      fail(
        'outbox_delivery_correction_unverifiable',
        'The delivered command or result is invalid.',
      );
    }
    if (command.operation !== 'update_main' || command.render_model?.terminal !== true) {
      fail(
        'outbox_delivery_correction_unsupported_operation',
        'Only a terminal main-message update may be corrected to its final fallback.',
      );
    }
    if (command.target_platform_message_id !== normalizedEvidence.platform_message_id) {
      fail(
        'outbox_delivery_correction_evidence_mismatch',
        'The platform readback does not identify the delivered target.',
      );
    }
    const deliveredIdentityMatches = previousResult.status === 'delivered'
      && previousResult.outbox_id === command.outbox_id
      && previousResult.delivery_id === command.delivery_id
      && previousResult.delivery_attempt_id === command.delivery_attempt_id
      && previousResult.delivery_attempt_no === command.delivery_attempt_no
      && previousResult.outbox_lease_epoch === command.outbox_lease_epoch
      && previousResult.aggregate_version === command.aggregate_version
      && previousResult.platform_message_id === command.target_platform_message_id;
    if (!deliveredIdentityMatches) {
      fail(
        'outbox_delivery_correction_unverifiable',
        'The recorded delivered result does not match the immutable command.',
      );
    }

    const correctionId = generateId('outbox-delivery-correction');
    const correctedError = {
      code: 'authorized_platform_readback_delivery_absent',
      category: 'channel',
      retryable: false,
      side_effect_status: 'none',
      user_message: 'A read-only platform comparison proved that the recorded update was absent.',
      occurred_at: committedAt,
    };
    const correctedResult = {
      ...previousResult,
      status: 'permanent_failure',
      platform_message_id: command.target_platform_message_id,
      applied_platform_version: null,
      delivered_at: null,
      error: correctedError,
      result_at: committedAt,
    };
    const updated = database.prepare(`
      UPDATE runtime_outbox
      SET status = 'dead_letter', result_json = ?, last_error_json = ?, updated_at = ?
      WHERE outbox_id = ? AND status = 'delivered'
        AND delivery_attempt_id = ? AND delivery_attempt_no = ?
        AND outbox_lease_epoch = ? AND command_json = ?
        AND claimed_command_hash = ? AND aggregate_version = ?
        AND result_json = ?
    `).run(
      JSON.stringify(correctedResult),
      JSON.stringify(correctedError),
      committedAt,
      normalizedExpected.outbox_id,
      normalizedExpected.delivery_attempt_id,
      normalizedExpected.delivery_attempt_no,
      normalizedExpected.outbox_lease_epoch,
      row.command_json,
      normalizedExpected.claimed_command_hash,
      normalizedExpected.aggregate_version,
      row.result_json,
    );
    if (updated.changes !== 1) {
      fail(
        'outbox_delivery_correction_conflict',
        'The delivered outbox record changed during correction.',
      );
    }

    enqueueFinalFallback(database, command, committedAt, generateId);
    const fallback = database.prepare(`
      SELECT outbox_id
      FROM runtime_outbox
      WHERE predecessor_delivery_id = ? AND status = 'pending'
      ORDER BY created_at, outbox_id
      LIMIT 1
    `).get(command.delivery_id);
    if (!fallback) {
      fail(
        'outbox_delivery_correction_conflict',
        'The final fallback could not be materialized.',
      );
    }
    database.prepare(`
      INSERT INTO runtime_outbox_delivery_corrections (
        correction_id, request_hash, outbox_id, delivery_attempt_id,
        delivery_attempt_no, outbox_lease_epoch, decision, previous_status,
        terminal_status, actor_id, authorization_ref, reason, evidence_json,
        evidence_hash, fallback_outbox_id, committed_at
      ) VALUES (?, ?, ?, ?, ?, ?, 'platform_readback_delivery_absent',
        'delivered', 'dead_letter', ?, ?, ?, ?, ?, ?, ?)
    `).run(
      correctionId,
      requestHash,
      normalizedExpected.outbox_id,
      normalizedExpected.delivery_attempt_id,
      normalizedExpected.delivery_attempt_no,
      normalizedExpected.outbox_lease_epoch,
      normalizedAuthorization.actor_id,
      normalizedAuthorization.authorization_ref,
      normalizedAuthorization.reason,
      evidenceJson,
      evidenceHash,
      fallback.outbox_id,
      committedAt,
    );
    return Object.freeze({
      status: 'applied',
      outbox_status: 'dead_letter',
      correction_id: correctionId,
      fallback_outbox_id: fallback.outbox_id,
    });
  });
  return correct.immediate();
}
