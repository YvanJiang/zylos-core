import crypto from 'node:crypto';

const TERMINAL_TURN_STATES = Object.freeze([
  'completed',
  'stopped',
  'cancelled',
  'interrupted',
  'failed',
  'timed_out',
]);
const BUSY_RETRY_DELAYS_MS = Object.freeze([10, 50, 250]);
const ROW_SOURCES = Object.freeze({
  normalized_event: Object.freeze(['runtime_normalized_events', 'event_id']),
  provider_raw_event: Object.freeze(['runtime_provider_event_diagnostics', 'diagnostic_id']),
  intermediate_projection: Object.freeze(['runtime_projection_snapshots', 'projection_id']),
  terminal_projection: Object.freeze(['runtime_projection_snapshots', 'projection_id']),
  provider_attempt_detail: Object.freeze(['runtime_provider_attempts', 'attempt_id']),
  terminal_outbox: Object.freeze(['runtime_outbox', 'outbox_id']),
  terminal_turn_queue: Object.freeze(['runtime_turn_queue', 'turn_id']),
  terminal_background_work: Object.freeze([
    'runtime_workspace_background_work',
    'background_work_id',
  ]),
  terminal_workspace_lease: Object.freeze(['runtime_workspace_leases', 'workspace_lease_id']),
  permission_audit: Object.freeze(['runtime_permission_audit', 'audit_id']),
  operations_audit: Object.freeze(['runtime_operations_audit', 'audit_id']),
  interaction_audit: Object.freeze(['runtime_interaction_audit', 'audit_id']),
  permission_action_decision: Object.freeze([
    'runtime_permission_action_decisions',
    'decision_id',
  ]),
});

function sleepSynchronously(delayMs) {
  const signal = new Int32Array(new SharedArrayBuffer(Int32Array.BYTES_PER_ELEMENT));
  Atomics.wait(signal, 0, 0, delayMs);
}

function isSqliteBusy(error) {
  return error?.code === 'SQLITE_BUSY' || error?.code === 'SQLITE_LOCKED';
}

function normalizeTimestamp(value, fieldName) {
  const timestamp = Date.parse(value);
  if (!Number.isFinite(timestamp)) {
    throw new TypeError(`${fieldName} must be an RFC 3339 timestamp.`);
  }
  return { timestamp, iso: new Date(timestamp).toISOString() };
}

function validateMaxLag(maxLagMs) {
  if (!Number.isSafeInteger(maxLagMs) || maxLagMs < 0) {
    throw new TypeError('maxLagMs must be a non-negative safe integer.');
  }
}

function canonicalJson(value) {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(',')}]`;
  if (value !== null && typeof value === 'object') {
    return `{${Object.keys(value).sort().map((key) => (
      `${JSON.stringify(key)}:${canonicalJson(value[key])}`
    )).join(',')}}`;
  }
  return JSON.stringify(value);
}

function turnIsProtected(database, turnId) {
  if (turnId === null) return false;
  const turn = database.prepare(`
    SELECT state FROM runtime_turns WHERE turn_id = ?
  `).get(turnId);
  if (!turn || !TERMINAL_TURN_STATES.includes(turn.state)) return true;
  if (database.prepare(`
    SELECT 1 FROM runtime_interactions
    WHERE turn_id = ? AND (
      state IN ('pending', 'answer_committed', 'answer_delivering', 'delivery_unknown')
      OR handoff_state IN ('pending', 'preparing', 'sending', 'delivery_unknown')
    ) LIMIT 1
  `).get(turnId)) return true;
  if (database.prepare(`
    SELECT 1 FROM runtime_outbox
    WHERE turn_id = ? AND status IN ('pending', 'delivering', 'retry_wait') LIMIT 1
  `).get(turnId)) return true;
  if (database.prepare(`
    SELECT 1 FROM runtime_provider_attempts
    WHERE turn_id = ? AND side_effect_status = 'unknown' LIMIT 1
  `).get(turnId)) return true;
  if (database.prepare(`
    SELECT 1 FROM runtime_execution_recoveries
    WHERE turn_id = ? AND side_effect_status = 'unknown' LIMIT 1
  `).get(turnId)) return true;
  if (database.prepare(`
    SELECT 1 FROM runtime_provider_stop_incidents
    WHERE turn_id = ? AND side_effect_status = 'unknown' LIMIT 1
  `).get(turnId)) return true;
  if (database.prepare(`
    SELECT 1 FROM runtime_workspace_background_work
    WHERE holder_turn_id = ? AND state IN ('active', 'unknown') LIMIT 1
  `).get(turnId)) return true;
  if (database.prepare(`
    SELECT 1 FROM runtime_workspace_leases
    WHERE holder_turn_id = ? AND state IN ('active', 'uncertain') LIMIT 1
  `).get(turnId)) return true;
  return !database.prepare(`
    SELECT 1 FROM runtime_compact_turn_summaries WHERE turn_id = ?
  `).get(turnId);
}

function loadCandidates(database, transactionTime) {
  return database.prepare(`
    SELECT record_kind, record_id, turn_id, retention_class, anchor_at,
      expires_at, disposal_kind
    FROM runtime_retention_entries
    WHERE disposed_at IS NULL AND expires_at <= ?
    ORDER BY expires_at ASC,
      CASE record_kind
        WHEN 'normalized_event' THEN 10
        WHEN 'provider_raw_event' THEN 15
        WHEN 'intermediate_projection' THEN 20
        WHEN 'terminal_projection' THEN 30
        WHEN 'provider_attempt_detail' THEN 40
        WHEN 'terminal_outbox' THEN 50
        WHEN 'terminal_turn_queue' THEN 60
        WHEN 'terminal_background_work' THEN 70
        WHEN 'terminal_workspace_lease' THEN 80
        WHEN 'operations_idempotency_conflict' THEN 85
        WHEN 'interaction_audit' THEN 90
        WHEN 'permission_action_decision' THEN 90
        WHEN 'permission_audit' THEN 90
        WHEN 'operations_audit' THEN 90
        ELSE 100
      END,
      record_kind ASC, record_id ASC
  `).all(transactionTime).filter(({ turn_id: turnId }) => !turnIsProtected(database, turnId));
}

function readCanonicalContent(database, candidate) {
  let row;
  if (candidate.record_kind === 'operations_idempotency_conflict') {
    const [callerNamespace, controlId, requestHash] = JSON.parse(candidate.record_id);
    row = database.prepare(`
      SELECT *
      FROM runtime_operations_idempotency_conflicts
      WHERE caller_namespace = ? AND control_id = ? AND request_hash = ?
    `).get(callerNamespace, controlId, requestHash);
  } else {
    const source = ROW_SOURCES[candidate.record_kind];
    if (!source) throw new Error(`Unsupported retention record kind ${candidate.record_kind}.`);
    const [table, key] = source;
    row = database.prepare(`SELECT * FROM ${table} WHERE ${key} = ?`).get(candidate.record_id);
  }
  return row === undefined ? null : canonicalJson({
    digest_schema_version: 1,
    record_kind: candidate.record_kind,
    row,
  });
}

function disposeRecord(database, candidate) {
  if (candidate.record_kind === 'normalized_event' && candidate.disposal_kind === 'delete') {
    return database.prepare(`
      DELETE FROM runtime_normalized_events WHERE event_id = ?
    `).run(candidate.record_id).changes;
  }
  if (candidate.record_kind === 'provider_raw_event' && candidate.disposal_kind === 'delete') {
    return database.prepare(`
      DELETE FROM runtime_provider_event_diagnostics WHERE diagnostic_id = ?
    `).run(candidate.record_id).changes;
  }
  if (
    ['intermediate_projection', 'terminal_projection'].includes(candidate.record_kind)
    && candidate.disposal_kind === 'delete'
  ) {
    return database.prepare(`
      DELETE FROM runtime_projection_snapshots WHERE projection_id = ?
    `).run(candidate.record_id).changes;
  }
  if (
    candidate.record_kind === 'provider_attempt_detail'
    && candidate.disposal_kind === 'redact'
  ) {
    return database.prepare(`
      UPDATE runtime_provider_attempts
      SET runtime_instance_id = NULL, runtime_evidence_json = NULL,
        last_provider_event_at = NULL, retry_backoff_ms = NULL,
        next_retry_at = NULL, error_json = NULL
      WHERE attempt_id = ?
    `).run(candidate.record_id).changes;
  }
  if (candidate.record_kind === 'terminal_outbox' && candidate.disposal_kind === 'delete') {
    return database.prepare(`
      DELETE FROM runtime_outbox WHERE outbox_id = ?
    `).run(candidate.record_id).changes;
  }
  if (candidate.record_kind === 'terminal_turn_queue' && candidate.disposal_kind === 'delete') {
    return database.prepare(`
      DELETE FROM runtime_turn_queue WHERE turn_id = ?
    `).run(candidate.record_id).changes;
  }
  if (
    candidate.record_kind === 'terminal_background_work'
    && candidate.disposal_kind === 'delete'
  ) {
    return database.prepare(`
      DELETE FROM runtime_workspace_background_work WHERE background_work_id = ?
    `).run(candidate.record_id).changes;
  }
  if (
    candidate.record_kind === 'terminal_workspace_lease'
    && candidate.disposal_kind === 'delete'
  ) {
    return database.prepare(`
      DELETE FROM runtime_workspace_leases WHERE workspace_lease_id = ?
    `).run(candidate.record_id).changes;
  }
  if (candidate.record_kind === 'permission_audit' && candidate.disposal_kind === 'delete') {
    return database.prepare(`
      DELETE FROM runtime_permission_audit WHERE audit_id = ?
    `).run(candidate.record_id).changes;
  }
  if (candidate.record_kind === 'operations_audit' && candidate.disposal_kind === 'delete') {
    return database.prepare(`
      DELETE FROM runtime_operations_audit WHERE audit_id = ?
    `).run(candidate.record_id).changes;
  }
  if (candidate.record_kind === 'interaction_audit' && candidate.disposal_kind === 'delete') {
    return database.prepare(`
      DELETE FROM runtime_interaction_audit WHERE audit_id = ?
    `).run(candidate.record_id).changes;
  }
  if (
    candidate.record_kind === 'permission_action_decision'
    && candidate.disposal_kind === 'delete'
  ) {
    return database.prepare(`
      DELETE FROM runtime_permission_action_decisions WHERE decision_id = ?
    `).run(candidate.record_id).changes;
  }
  if (
    candidate.record_kind === 'operations_idempotency_conflict'
    && candidate.disposal_kind === 'delete'
  ) {
    const [callerNamespace, controlId, requestHash] = JSON.parse(candidate.record_id);
    return database.prepare(`
      DELETE FROM runtime_operations_idempotency_conflicts
      WHERE caller_namespace = ? AND control_id = ? AND request_hash = ?
    `).run(callerNamespace, controlId, requestHash).changes;
  }
  throw new Error(
    `Unsupported retention disposal ${candidate.record_kind}:${candidate.disposal_kind}.`,
  );
}

export function createRetentionCleanup({
  database,
  now,
  generateId,
  sleep = sleepSynchronously,
}) {
  if (!database || typeof database.prepare !== 'function') {
    throw new TypeError('Retention cleanup requires a Core SQLite database.');
  }
  if (
    typeof now !== 'function'
    || typeof generateId !== 'function'
    || typeof sleep !== 'function'
  ) {
    throw new TypeError('Retention cleanup requires now, generateId, and sleep functions.');
  }

  function run({ dryRun = false, maxLagMs } = {}) {
    validateMaxLag(maxLagMs);
    const { timestamp: transactionTimeMs, iso: transactionTime } = normalizeTimestamp(
      now(),
      'cleanup transaction time',
    );
    if (dryRun) {
      const candidates = loadCandidates(database, transactionTime);
      const oldestExpiryAt = candidates[0]?.expires_at ?? null;
      const observedLagMs = oldestExpiryAt === null
        ? 0
        : Math.max(0, transactionTimeMs - Date.parse(oldestExpiryAt));
      const maxLagExceeded = observedLagMs > maxLagMs;
      return Object.freeze({
        transaction_time: transactionTime,
        candidates,
        candidate_count: candidates.length,
        oldest_expiry_at: oldestExpiryAt,
        observed_lag_ms: observedLagMs,
        max_lag_ms: maxLagMs,
        max_lag_exceeded: maxLagExceeded,
        alert: maxLagExceeded ? Object.freeze({
          code: 'retention_cleanup_max_lag_exceeded',
          retryable: false,
        }) : null,
        dry_run: true,
        disposed_count: 0,
        deleted_by_kind: Object.freeze({}),
      });
    }

    const sweepId = generateId('retention-sweep');
    let busyRetryCount = 0;
    const commit = database.transaction(() => {
      const currentCandidates = loadCandidates(database, transactionTime);
      const oldestExpiryAt = currentCandidates[0]?.expires_at ?? null;
      const observedLagMs = oldestExpiryAt === null
        ? 0
        : Math.max(0, transactionTimeMs - Date.parse(oldestExpiryAt));
      const deletedByKind = {};
      for (const candidate of currentCandidates) {
        const content = readCanonicalContent(database, candidate);
        if (content === null) {
          throw new Error(`Retention source ${candidate.record_kind}:${candidate.record_id} is missing.`);
        }
        const auditId = generateId('retention-deletion-audit');
        const contentSha256 = crypto.createHash('sha256').update(content).digest('hex');
        const changes = disposeRecord(database, candidate);
        if (changes !== 1) {
          throw new Error(`Retention source ${candidate.record_kind}:${candidate.record_id} changed.`);
        }
        database.prepare(`
          INSERT INTO runtime_retention_deletion_audit (
            audit_id, sweep_id, record_kind, record_id, turn_id, retention_class,
            anchor_at, expires_at, disposal_kind, content_digest_version,
            content_sha256, deleted_at
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
        `).run(
          auditId,
          sweepId,
          candidate.record_kind,
          candidate.record_id,
          candidate.turn_id,
          candidate.retention_class,
          candidate.anchor_at,
          candidate.expires_at,
          candidate.disposal_kind,
          contentSha256,
          transactionTime,
        );
        const entry = database.prepare(`
          UPDATE runtime_retention_entries
          SET disposed_at = ?, deletion_audit_id = ?
          WHERE record_kind = ? AND record_id = ? AND disposed_at IS NULL
        `).run(transactionTime, auditId, candidate.record_kind, candidate.record_id);
        if (entry.changes !== 1) {
          throw new Error(`Retention entry ${candidate.record_kind}:${candidate.record_id} changed.`);
        }
        deletedByKind[candidate.record_kind] = (deletedByKind[candidate.record_kind] ?? 0) + 1;
      }
      database.prepare(`
        INSERT INTO runtime_retention_sweeps (
          sweep_id, transaction_time, candidate_count, disposed_count,
          oldest_expiry_at, max_lag_ms, observed_lag_ms, max_lag_exceeded,
          busy_retry_count, committed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `).run(
        sweepId,
        transactionTime,
        currentCandidates.length,
        currentCandidates.length,
        oldestExpiryAt,
        maxLagMs,
        observedLagMs,
        observedLagMs > maxLagMs ? 1 : 0,
        busyRetryCount,
        transactionTime,
      );
      return {
        currentCandidates,
        deletedByKind,
        oldestExpiryAt,
        observedLagMs,
      };
    });
    let committed;
    for (;;) {
      try {
        committed = commit.immediate();
        break;
      } catch (error) {
        if (!isSqliteBusy(error) || busyRetryCount >= BUSY_RETRY_DELAYS_MS.length) throw error;
        const delayMs = BUSY_RETRY_DELAYS_MS[busyRetryCount];
        busyRetryCount += 1;
        sleep(delayMs, busyRetryCount);
      }
    }
    const {
      currentCandidates,
      deletedByKind,
      oldestExpiryAt,
      observedLagMs,
    } = committed;
    const maxLagExceeded = observedLagMs > maxLagMs;
    return Object.freeze({
      transaction_time: transactionTime,
      candidates: currentCandidates,
      candidate_count: currentCandidates.length,
      oldest_expiry_at: oldestExpiryAt,
      observed_lag_ms: observedLagMs,
      max_lag_ms: maxLagMs,
      max_lag_exceeded: maxLagExceeded,
      alert: maxLagExceeded ? Object.freeze({
        code: 'retention_cleanup_max_lag_exceeded',
        retryable: false,
      }) : null,
      dry_run: false,
      sweep_id: sweepId,
      busy_retry_count: busyRetryCount,
      disposed_count: currentCandidates.length,
      deleted_by_kind: Object.freeze(deletedByKind),
    });
  }

  return Object.freeze({ run });
}
