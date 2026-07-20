import crypto from 'node:crypto';
import fs from 'node:fs/promises';
import path from 'node:path';

import { canonicalizeJson, validatePublicFixtureSafety } from '../../contracts/public/index.js';
import { initializeRuntimePersistence } from '../persistence/schema.js';

function requireFunction(name, value) {
  if (typeof value !== 'function') throw new TypeError(`${name} must be a function`);
  return value;
}

function hash(value) {
  return crypto.createHash('sha256').update(canonicalizeJson(value)).digest('hex');
}

async function sha256File(file) {
  return crypto.createHash('sha256').update(await fs.readFile(file)).digest('hex');
}

export function createSqliteSnapshotAdapter({
  database,
  snapshotDirectory,
  openDatabase,
}) {
  if (!database || typeof database.backup !== 'function') {
    throw new TypeError('database must support SQLite online backup');
  }
  if (typeof snapshotDirectory !== 'string' || snapshotDirectory.length === 0) {
    throw new TypeError('snapshotDirectory must be a non-empty string');
  }
  if (!path.isAbsolute(snapshotDirectory)
    || path.parse(snapshotDirectory).root === path.resolve(snapshotDirectory)) {
    throw new TypeError('snapshotDirectory must be an explicit absolute non-root path');
  }
  const snapshotRoot = path.resolve(snapshotDirectory);
  requireFunction('openDatabase', openDatabase);

  async function verifyDescriptor(descriptor) {
    const actualHash = await sha256File(descriptor.database_snapshot_ref);
    if (actualHash !== descriptor.snapshot_sha256) {
      throw new Error('SQLite snapshot hash does not match its durable descriptor.');
    }
    const snapshot = openDatabase(descriptor.database_snapshot_ref, {
      readonly: true,
      fileMustExist: true,
    });
    try {
      const integrity = snapshot.pragma('integrity_check', { simple: true });
      const foreignKeys = snapshot.pragma('foreign_key_check');
      if (integrity !== 'ok' || foreignKeys.length !== 0) {
        throw new Error('SQLite snapshot failed integrity or foreign-key verification.');
      }
    } finally {
      snapshot.close();
    }
    return Object.freeze({
      ...descriptor,
      database_integrity: 'ok',
      foreign_key_violations: 0,
    });
  }

  return Object.freeze({
    async capture({ step_id: stepId, upgrade_id: upgradeId, from_release: fromRelease }) {
      await fs.mkdir(snapshotRoot, { recursive: true });
      const snapshotName = hash({ upgrade_id: upgradeId });
      const snapshotPath = path.join(snapshotRoot, `${snapshotName}.sqlite`);
      if (path.dirname(snapshotPath) !== snapshotRoot) {
        throw new Error('SQLite snapshot path escaped its configured directory.');
      }
      try {
        const existingHash = await sha256File(snapshotPath);
        return verifyDescriptor({
          package_release_ref: fromRelease,
          database_snapshot_ref: snapshotPath,
          snapshot_sha256: existingHash,
          step_id: stepId,
        });
      } catch (error) {
        if (error?.code !== 'ENOENT') throw error;
      }
      const partialPath = path.join(snapshotRoot, `${snapshotName}.sqlite.partial`);
      await fs.rm(partialPath, { force: true });
      await database.backup(partialPath);
      await fs.rename(partialPath, snapshotPath);
      const snapshotHash = await sha256File(snapshotPath);
      return verifyDescriptor({
        package_release_ref: fromRelease,
        database_snapshot_ref: snapshotPath,
        snapshot_sha256: snapshotHash,
        step_id: stepId,
      });
    },
    async verify({ descriptor }) {
      return verifyDescriptor(descriptor);
    },
  });
}

export function createAtomicReleaseAdapter({
  activeReleaseFile,
  releases,
  cleanupPaths = [],
}) {
  if (typeof activeReleaseFile !== 'string' || activeReleaseFile.length === 0) {
    throw new TypeError('activeReleaseFile must be a non-empty string');
  }
  if (!releases || typeof releases !== 'object' || Array.isArray(releases)) {
    throw new TypeError('releases must map release refs to directories');
  }
  if (!Array.isArray(cleanupPaths)) throw new TypeError('cleanupPaths must be an array');
  for (const cleanupPath of cleanupPaths) {
    if (typeof cleanupPath !== 'string' || !path.isAbsolute(cleanupPath)
      || path.parse(cleanupPath).root === cleanupPath) {
      throw new TypeError('cleanupPaths entries must be explicit absolute non-root paths');
    }
  }

  async function switchTo({ step_id: stepId, release_ref: releaseRef }) {
    const releasePath = releases[releaseRef];
    if (typeof releasePath !== 'string' || releasePath.length === 0) {
      throw new Error(`Release ${releaseRef} is not available for atomic activation.`);
    }
    const stat = await fs.stat(releasePath);
    if (!stat.isDirectory()) throw new Error(`Release ${releaseRef} is not a directory.`);
    let existing = null;
    try {
      existing = JSON.parse(await fs.readFile(activeReleaseFile, 'utf8'));
    } catch (error) {
      if (error?.code !== 'ENOENT') throw error;
    }
    if (existing?.release_ref === releaseRef && existing?.release_path === releasePath) {
      return Object.freeze({ ...existing, step_id: stepId });
    }
    await fs.mkdir(path.dirname(activeReleaseFile), { recursive: true });
    const temporaryFile = `${activeReleaseFile}.${hash(stepId).slice(0, 16)}.partial`;
    const value = { release_ref: releaseRef, release_path: releasePath };
    await fs.writeFile(temporaryFile, `${canonicalizeJson(value)}\n`, { mode: 0o600 });
    await fs.rename(temporaryFile, activeReleaseFile);
    return Object.freeze({ ...value, step_id: stepId });
  }

  return Object.freeze({
    activate({ step_id: stepId, to_release: toRelease }) {
      return switchTo({ step_id: stepId, release_ref: toRelease });
    },
    restore({ step_id: stepId, snapshot }) {
      return switchTo({
        step_id: stepId,
        release_ref: snapshot.package_release_ref,
      });
    },
    async cleanup({ step_id: stepId }) {
      const removed = [];
      for (const cleanupPath of cleanupPaths) {
        await fs.rm(cleanupPath, { recursive: true, force: true });
        removed.push(cleanupPath);
      }
      return Object.freeze({ step_id: stepId, removed_paths: Object.freeze(removed) });
    },
  });
}

export function createLegacySourceQueueAdapter({
  sourceQueueFile,
  auditDirectory,
  stopLegacyDispatcher,
  restartLegacyDispatcher,
}) {
  for (const [name, value] of Object.entries({ sourceQueueFile, auditDirectory })) {
    if (typeof value !== 'string' || !path.isAbsolute(value)
      || path.parse(value).root === path.resolve(value)) {
      throw new TypeError(`${name} must be an explicit absolute non-root path`);
    }
  }
  const sourcePath = path.resolve(sourceQueueFile);
  const auditRoot = path.resolve(auditDirectory);
  requireFunction('stopLegacyDispatcher', stopLegacyDispatcher);
  requireFunction('restartLegacyDispatcher', restartLegacyDispatcher);

  function isRollbackSafe(record) {
    if (record?.kind === 'c4') {
      return record.legacy_state === 'pending' && record.route === 'unique'
        && typeof record.legacy_record_id === 'string'
        && record.legacy_record_id.length > 0
        && !/[\u0000-\u001f\u007f]/.test(record.legacy_record_id);
    }
    if (record?.kind !== 'scheduler' || record.legacy_state !== 'pending'
      || record.schedule_type !== 'one-time' || !record.occurrence) return false;
    const scheduledAt = Date.parse(record.scheduled_for);
    const observedAt = Date.parse(record.observed_at);
    return Number.isFinite(scheduledAt) && Number.isFinite(observedAt)
      && Number.isSafeInteger(record.miss_threshold_ms)
      && observedAt - scheduledAt <= record.miss_threshold_ms;
  }

  async function materializeReadOnly(file, document) {
    try {
      const existing = JSON.parse(await fs.readFile(file, 'utf8'));
      if (hash(existing) !== hash(document)) throw new Error('Legacy rollback queue conflicts.');
    } catch (error) {
      if (error?.code !== 'ENOENT') throw error;
      const partial = `${file}.${crypto.randomUUID()}.partial`;
      await fs.writeFile(partial, `${canonicalizeJson(document)}\n`, { mode: 0o600 });
      await fs.rename(partial, file);
    }
    await fs.chmod(file, 0o400);
  }

  async function readAndVerify(file, expectedHash) {
    const document = JSON.parse(await fs.readFile(file, 'utf8'));
    if (hash(document) !== expectedHash) {
      throw new Error('Legacy source queue does not match the migration batch hash.');
    }
    return document;
  }

  return Object.freeze({
    async invalidate({ step_id: stepId, batch_id: batchId, batch_hash: batchHash }) {
      const stopProof = await stopLegacyDispatcher(Object.freeze({
        step_id: stepId, batch_id: batchId, batch_hash: batchHash,
      }));
      if (stopProof?.stopped !== true || typeof stopProof.stopped_at !== 'string') {
        throw new Error('Legacy source invalidation requires dispatcher stop proof.');
      }
      await fs.mkdir(auditRoot, { recursive: true });
      const auditPath = path.join(auditRoot, `${hash({ batch_id: batchId, batch_hash: batchHash })}.json`);
      const rollbackPath = path.join(
        auditRoot, `${hash({ batch_id: batchId, batch_hash: batchHash, kind: 'rollback-safe' })}.json`,
      );
      let sourcePresent = true;
      let sourceDocument = null;
      try {
        sourceDocument = await readAndVerify(sourcePath, batchHash);
      } catch (error) {
        if (error?.code !== 'ENOENT') throw error;
        sourcePresent = false;
      }
      if (sourceDocument === null) sourceDocument = await readAndVerify(auditPath, batchHash);
      const rollbackDocument = {
        batch_id: sourceDocument.batch_id,
        records: sourceDocument.records.filter(isRollbackSafe),
        rollback_reconciled: true,
      };
      await materializeReadOnly(rollbackPath, rollbackDocument);
      if (sourcePresent) {
        await fs.rename(sourcePath, auditPath);
      }
      await readAndVerify(auditPath, batchHash);
      await fs.chmod(auditPath, 0o400);
      return Object.freeze({
        step_id: stepId,
        batch_id: batchId,
        batch_hash: batchHash,
        source_queue_ref: sourcePath,
        audit_queue_ref: auditPath,
        audit_sha256: await sha256File(auditPath),
        rollback_queue_ref: rollbackPath,
        rollback_queue_sha256: await sha256File(rollbackPath),
        rollback_queue_record_count: rollbackDocument.records.length,
        excluded_record_count: sourceDocument.records.length - rollbackDocument.records.length,
        legacy_dispatcher_stopped: true,
        legacy_dispatcher_stopped_at: stopProof.stopped_at,
        source_queue_read_only: true,
      });
    },
    async seal({ audit_queue_ref: auditQueueRef, audit_sha256: auditSha256 }) {
      if (typeof auditQueueRef !== 'string'
        || path.dirname(path.resolve(auditQueueRef)) !== auditRoot) {
        throw new Error('Legacy audit queue seal escaped its configured directory.');
      }
      try {
        if (await sha256File(auditQueueRef) !== auditSha256) {
          throw new Error('Legacy audit queue changed before durable sealing.');
        }
        await fs.chmod(auditQueueRef, 0o600);
        await fs.unlink(auditQueueRef);
      } catch (error) {
        if (error?.code !== 'ENOENT') throw error;
      }
      return Object.freeze({
        audit_queue_ref: auditQueueRef,
        audit_sha256: auditSha256,
        sealed_into_durable_migration: true,
        audit_queue_removed: true,
      });
    },
    async rollback({
      step_id: stepId,
      rollback_queue_ref: rollbackQueueRef,
      rollback_queue_sha256: rollbackQueueSha256,
    }) {
      if (typeof rollbackQueueRef !== 'string'
        || path.dirname(path.resolve(rollbackQueueRef)) !== auditRoot) {
        throw new Error('Legacy rollback queue escaped its configured directory.');
      }
      let restoredDocument;
      try {
        if (await sha256File(sourcePath) !== rollbackQueueSha256) {
          throw new Error('Restored legacy rollback queue conflicts with its durable hash.');
        }
        restoredDocument = JSON.parse(await fs.readFile(sourcePath, 'utf8'));
      } catch (error) {
        if (error?.code !== 'ENOENT') throw error;
        if (await sha256File(rollbackQueueRef) !== rollbackQueueSha256) {
          throw new Error('Legacy rollback queue changed before restoration.');
        }
        await fs.chmod(rollbackQueueRef, 0o600);
        await fs.rename(rollbackQueueRef, sourcePath);
        restoredDocument = JSON.parse(await fs.readFile(sourcePath, 'utf8'));
      }
      await fs.chmod(sourcePath, 0o600);
      const restartProof = await restartLegacyDispatcher(Object.freeze({
        step_id: stepId,
        source_queue_ref: sourcePath,
        rollback_queue_sha256: rollbackQueueSha256,
      }));
      if (restartProof?.restarted !== true || typeof restartProof.restarted_at !== 'string') {
        throw new Error('Legacy rollback requires dispatcher restart proof.');
      }
      return Object.freeze({
        step_id: stepId,
        source_queue_ref: sourcePath,
        rollback_queue_sha256: rollbackQueueSha256,
        restored_record_count: restoredDocument.records.length,
        legacy_dispatcher_restarted: true,
        legacy_dispatcher_restarted_at: restartProof.restarted_at,
      });
    },
    async commit({ rollback_queue_ref: rollbackQueueRef, rollback_queue_sha256: rollbackQueueSha256 }) {
      if (typeof rollbackQueueRef !== 'string'
        || path.dirname(path.resolve(rollbackQueueRef)) !== auditRoot) {
        throw new Error('Legacy commit queue cleanup escaped its configured directory.');
      }
      try {
        if (await sha256File(rollbackQueueRef) !== rollbackQueueSha256) {
          throw new Error('Legacy rollback queue changed before commit cleanup.');
        }
        await fs.chmod(rollbackQueueRef, 0o600);
        await fs.unlink(rollbackQueueRef);
      } catch (error) {
        if (error?.code !== 'ENOENT') throw error;
      }
      return Object.freeze({ rollback_queue_removed: true });
    },
  });
}

export function createRuntimeUpgradeCoordinator({
  database,
  upgradeService,
  snapshotAdapter,
  releaseAdapter,
  legacySourceAdapter = null,
  executorAdapter = null,
  noticeAdapter = null,
  now = () => new Date().toISOString(),
  effectClaimLeaseMs = 30_000,
}) {
  if (!database || typeof database.transaction !== 'function') {
    throw new TypeError('database must be a better-sqlite3 connection');
  }
  for (const [name, value] of Object.entries({ upgradeService, snapshotAdapter, releaseAdapter })) {
    if (!value || typeof value !== 'object') throw new TypeError(`${name} must be an object`);
  }
  if (legacySourceAdapter !== null
    && (!legacySourceAdapter || typeof legacySourceAdapter.invalidate !== 'function'
      || typeof legacySourceAdapter.seal !== 'function')) {
    throw new TypeError('legacySourceAdapter must expose invalidate, seal, rollback, and commit');
  }
  if (legacySourceAdapter !== null
    && (typeof legacySourceAdapter.rollback !== 'function'
      || typeof legacySourceAdapter.commit !== 'function')) {
    throw new TypeError('legacySourceAdapter must expose invalidate, seal, rollback, and commit');
  }
  requireFunction('snapshotAdapter.capture', snapshotAdapter.capture);
  requireFunction('snapshotAdapter.verify', snapshotAdapter.verify);
  requireFunction('releaseAdapter.activate', releaseAdapter.activate);
  requireFunction('releaseAdapter.restore', releaseAdapter.restore);
  requireFunction('releaseAdapter.cleanup', releaseAdapter.cleanup);
  requireFunction('now', now);
  if (!Number.isSafeInteger(effectClaimLeaseMs) || effectClaimLeaseMs <= 0) {
    throw new TypeError('effectClaimLeaseMs must be a positive safe integer');
  }
  initializeRuntimePersistence(database);
  const effectClaimOwner = `upgrade-coordinator-${crypto.randomUUID()}`;

  function loadEffect(upgradeId, stepKey) {
    const row = database.prepare(`
      SELECT step_id, input_hash, state, claim_owner, claim_attempt,
        claim_expires_at, result_json, committed_at, updated_at
      FROM runtime_upgrade_effects WHERE upgrade_id = ? AND step_key = ?
    `).get(upgradeId, stepKey);
    return row === undefined ? null : Object.freeze({
      step_id: row.step_id,
      input_hash: row.input_hash,
      state: row.state,
      claim_owner: row.claim_owner,
      claim_attempt: row.claim_attempt,
      claim_expires_at: row.claim_expires_at,
      result: row.result_json === null ? null : Object.freeze(JSON.parse(row.result_json)),
      committed_at: row.committed_at,
      updated_at: row.updated_at,
    });
  }

  async function performEffect(upgradeId, stepKey, input, invoke) {
    const expectedHash = hash(input);
    const stepId = `${upgradeId}:${stepKey}`;
    const claim = database.transaction(() => {
      const claimedAt = now();
      const expiresAt = new Date(Date.parse(claimedAt) + effectClaimLeaseMs).toISOString();
      const existing = loadEffect(upgradeId, stepKey);
      if (existing?.input_hash !== undefined && existing.input_hash !== expectedHash) {
        throw new Error(`Upgrade effect ${stepId} conflicts with durable input.`);
      }
      if (existing?.state === 'completed') return existing;
      if (existing?.state === 'claimed' && existing.claim_expires_at > claimedAt) {
        throw new Error(`Upgrade effect ${stepId} is already claimed by another coordinator.`);
      }
      if (existing === null) {
        database.prepare(`
          INSERT INTO runtime_upgrade_effects (
            upgrade_id, step_key, step_id, input_hash, state, claim_owner,
            claim_attempt, claim_expires_at, result_json, committed_at, updated_at
          ) VALUES (?, ?, ?, ?, 'claimed', ?, 1, ?, NULL, ?, ?)
        `).run(
          upgradeId, stepKey, stepId, expectedHash, effectClaimOwner,
          expiresAt, claimedAt, claimedAt,
        );
      } else {
        const takeover = database.prepare(`
          UPDATE runtime_upgrade_effects
          SET claim_owner = ?, claim_attempt = claim_attempt + 1,
            claim_expires_at = ?, updated_at = ?
          WHERE upgrade_id = ? AND step_key = ? AND state = 'claimed'
            AND input_hash = ? AND claim_expires_at <= ?
        `).run(
          effectClaimOwner, expiresAt, claimedAt,
          upgradeId, stepKey, expectedHash, claimedAt,
        );
        if (takeover.changes !== 1) {
          throw new Error(`Upgrade effect ${stepId} lost its durable claim CAS.`);
        }
      }
      return loadEffect(upgradeId, stepKey);
    }).immediate();
    if (claim.state === 'completed') return claim;
    let heartbeatFailure = null;
    let heartbeatInFlight = Promise.resolve();
    const renewClaim = () => {
      const renewedAt = now();
      const renewedUntil = new Date(Date.parse(renewedAt) + effectClaimLeaseMs).toISOString();
      const updated = database.prepare(`
        UPDATE runtime_upgrade_effects
        SET claim_expires_at = ?, updated_at = ?
        WHERE upgrade_id = ? AND step_key = ? AND state = 'claimed'
          AND claim_owner = ? AND claim_attempt = ?
      `).run(
        renewedUntil, renewedAt, upgradeId, stepKey, effectClaimOwner, claim.claim_attempt,
      );
      if (updated.changes !== 1) {
        throw new Error(`Upgrade effect ${stepId} lost its live claim heartbeat.`);
      }
    };
    const heartbeat = setInterval(() => {
      heartbeatInFlight = heartbeatInFlight.then(renewClaim).catch((error) => {
        heartbeatFailure = error;
      });
    }, Math.max(1, Math.floor(effectClaimLeaseMs / 3)));
    heartbeat.unref?.();
    let result;
    try {
      result = await invoke(Object.freeze({ ...structuredClone(input), step_id: stepId }));
    } catch (error) {
      clearInterval(heartbeat);
      await heartbeatInFlight;
      database.prepare(`
        UPDATE runtime_upgrade_effects SET claim_expires_at = ?, updated_at = ?
        WHERE upgrade_id = ? AND step_key = ? AND state = 'claimed'
          AND claim_owner = ? AND claim_attempt = ?
      `).run(
        now(), now(), upgradeId, stepKey, effectClaimOwner, claim.claim_attempt,
      );
      throw error;
    }
    clearInterval(heartbeat);
    await heartbeatInFlight;
    if (heartbeatFailure) throw heartbeatFailure;
    if (!result || typeof result !== 'object' || Array.isArray(result)) {
      throw new TypeError(`Upgrade effect ${stepId} must return an object`);
    }
    validatePublicFixtureSafety(result);
    const complete = database.transaction(() => {
      const completedAt = now();
      const updated = database.prepare(`
        UPDATE runtime_upgrade_effects
        SET state = 'completed', claim_expires_at = NULL, result_json = ?, updated_at = ?
        WHERE upgrade_id = ? AND step_key = ? AND state = 'claimed'
          AND input_hash = ? AND claim_owner = ? AND claim_attempt = ?
      `).run(
        canonicalizeJson(result), completedAt, upgradeId, stepKey,
        expectedHash, effectClaimOwner, claim.claim_attempt,
      );
      if (updated.changes !== 1) {
        throw new Error(`Upgrade effect ${stepId} lost its durable completion CAS.`);
      }
      const durable = loadEffect(upgradeId, stepKey);
      if (durable.state !== 'completed' || durable.input_hash !== expectedHash
        || canonicalizeJson(durable.result) !== canonicalizeJson(result)) {
        throw new Error(`Upgrade effect ${stepId} lost its durable idempotency fence.`);
      }
      return durable;
    });
    return complete.immediate();
  }

  async function advance(upgradeId, { legacyBatch = null } = {}) {
    const run = upgradeService.get(upgradeId);
    if (run.state === 'preflight') {
      const input = {
        upgrade_id: upgradeId,
        from_release: run.from_release,
        to_release: run.to_release,
      };
      const effect = loadEffect(upgradeId, 'snapshot-capture');
      if (effect?.state !== 'completed') {
        await performEffect(
          upgradeId,
          'snapshot-capture',
          input,
          (request) => snapshotAdapter.capture(request),
        );
        return Object.freeze({ ...upgradeService.get(upgradeId), completed_step: 'snapshot-capture' });
      }
      const descriptor = {
        package_release_ref: effect.result.package_release_ref,
        database_snapshot_ref: effect.result.database_snapshot_ref,
        snapshot_sha256: effect.result.snapshot_sha256,
      };
      const verified = await snapshotAdapter.verify({
        descriptor,
        step_id: `${upgradeId}:snapshot-record`,
      });
      if (verified.database_integrity !== 'ok'
        || verified.foreign_key_violations !== 0
        || verified.database_snapshot_ref !== descriptor.database_snapshot_ref
        || verified.snapshot_sha256 !== descriptor.snapshot_sha256) {
        throw new Error('Snapshot changed before its durable state transition.');
      }
      return upgradeService.recordSnapshot(upgradeId, descriptor);
    }
    if (run.state === 'snapshotted') return upgradeService.enterMaintenance(upgradeId);
    if (run.state === 'maintenance') return upgradeService.completeDrain(upgradeId);
    if (run.state === 'drained') {
      if (legacyBatch === null) {
        return Object.freeze({ ...run, status: 'waiting_for_legacy_batch' });
      }
      const batchHash = hash(legacyBatch);
      if (loadEffect(upgradeId, 'legacy-source-invalidate')?.state !== 'completed') {
        if (legacySourceAdapter === null) {
          return Object.freeze({ ...run, status: 'waiting_for_legacy_source_invalidation' });
        }
        await performEffect(
          upgradeId,
          'legacy-source-invalidate',
          { upgrade_id: upgradeId, batch_id: legacyBatch.batch_id, batch_hash: batchHash },
          (request) => legacySourceAdapter.invalidate(request),
        );
        return Object.freeze({
          ...upgradeService.get(upgradeId), completed_step: 'legacy-source-invalidate',
        });
      }
      const input = {
        upgrade_id: upgradeId,
        from_release: run.from_release,
        to_release: run.to_release,
        snapshot: run.snapshot,
      };
      if (loadEffect(upgradeId, 'release-activate')?.state !== 'completed') {
        await performEffect(
          upgradeId,
          'release-activate',
          input,
          (request) => releaseAdapter.activate(request),
        );
        return Object.freeze({ ...upgradeService.get(upgradeId), completed_step: 'release-activate' });
      }
      if (!upgradeService.isReleaseFenceActive(upgradeId)) {
        upgradeService.activateReleaseFence(upgradeId);
        return Object.freeze({
          ...upgradeService.get(upgradeId), completed_step: 'release-generation-fence',
        });
      }
      return upgradeService.migrateLegacy(upgradeId, legacyBatch);
    }
    if (run.state === 'migrating') {
      if (legacyBatch === null) {
        return Object.freeze({ ...run, status: 'waiting_for_legacy_batch_retry' });
      }
      return upgradeService.migrateLegacy(upgradeId, legacyBatch);
    }
    if (run.state === 'health_check') {
      const sourceInvalidation = loadEffect(upgradeId, 'legacy-source-invalidate');
      if (sourceInvalidation !== null
        && loadEffect(upgradeId, 'legacy-source-seal')?.state !== 'completed') {
        if (legacySourceAdapter === null) {
          return Object.freeze({ ...run, status: 'waiting_for_legacy_source_seal' });
        }
        await performEffect(
          upgradeId,
          'legacy-source-seal',
          {
            upgrade_id: upgradeId,
            audit_queue_ref: sourceInvalidation.result.audit_queue_ref,
            audit_sha256: sourceInvalidation.result.audit_sha256,
          },
          (request) => legacySourceAdapter.seal(request),
        );
        return Object.freeze({ ...run, completed_step: 'legacy-source-seal' });
      }
      const pendingNotices = database.prepare(`
        SELECT legacy_kind, legacy_record_id
        FROM runtime_legacy_migration_notices
        WHERE upgrade_id = ? AND state = 'pending'
        ORDER BY legacy_kind, legacy_record_id
      `).all(upgradeId);
      if (pendingNotices.length > 0) {
        if (noticeAdapter === null || typeof noticeAdapter.deliver !== 'function') {
          return Object.freeze({ ...run, status: 'waiting_for_notice_delivery' });
        }
        await noticeAdapter.deliver(Object.freeze({
          upgrade_id: upgradeId,
          notice_ids: Object.freeze(pendingNotices.map((notice) => Object.freeze({ ...notice }))),
        }));
        for (const notice of pendingNotices) {
          upgradeService.recordNoticeDelivered(
            upgradeId, notice.legacy_kind, notice.legacy_record_id,
          );
        }
        return Object.freeze({ ...upgradeService.get(upgradeId), completed_step: 'notice-delivery' });
      }
      if (executorAdapter === null || typeof executorAdapter.health !== 'function') {
        return Object.freeze({ ...run, status: 'waiting_for_executor_health' });
      }
      const input = {
        upgrade_id: upgradeId,
        release_ref: run.to_release,
        migration: run.migration,
      };
      const effect = loadEffect(upgradeId, 'executor-health');
      if (effect?.state !== 'completed') {
        await performEffect(
          upgradeId,
          'executor-health',
          input,
          (request) => executorAdapter.health(request),
        );
        return Object.freeze({ ...upgradeService.get(upgradeId), completed_step: 'executor-health' });
      }
      return upgradeService.recordExecutorHealth(upgradeId, effect.result);
    }
    if (run.state === 'ready_to_commit') return upgradeService.commit(upgradeId);
    if (run.state === 'rollback_required') {
      const sourceInvalidation = loadEffect(upgradeId, 'legacy-source-invalidate');
      const durableMigration = database.prepare(`
        SELECT 1 FROM runtime_upgrade_events
        WHERE upgrade_id = ? AND step_key = 'legacy-migration'
      `).get(upgradeId);
      if (sourceInvalidation !== null && durableMigration !== undefined
        && loadEffect(upgradeId, 'legacy-source-seal')?.state !== 'completed') {
        if (legacySourceAdapter === null) {
          return Object.freeze({ ...run, status: 'waiting_for_legacy_source_seal' });
        }
        await performEffect(
          upgradeId,
          'legacy-source-seal',
          {
            upgrade_id: upgradeId,
            audit_queue_ref: sourceInvalidation.result.audit_queue_ref,
            audit_sha256: sourceInvalidation.result.audit_sha256,
          },
          (request) => legacySourceAdapter.seal(request),
        );
        return Object.freeze({ ...run, completed_step: 'legacy-source-seal' });
      }
      const input = {
        upgrade_id: upgradeId,
        from_release: run.from_release,
        to_release: run.to_release,
        snapshot: run.snapshot,
      };
      if (loadEffect(upgradeId, 'rollback-restore')?.state !== 'completed') {
        const verifiedSnapshot = await snapshotAdapter.verify({
          descriptor: run.snapshot,
          step_id: `${upgradeId}:rollback-restore`,
        });
        await performEffect(upgradeId, 'rollback-restore', input, async (request) => ({
          ...(await releaseAdapter.restore(request)),
          database_snapshot_ref: verifiedSnapshot.database_snapshot_ref,
          snapshot_sha256: verifiedSnapshot.snapshot_sha256,
          database_integrity: verifiedSnapshot.database_integrity,
          foreign_key_violations: verifiedSnapshot.foreign_key_violations,
        }));
        return Object.freeze({ ...upgradeService.get(upgradeId), completed_step: 'rollback-restore' });
      }
      const fenceHistory = database.prepare(`
        SELECT restored_at FROM runtime_upgrade_release_fence_history WHERE upgrade_id = ?
      `).get(upgradeId);
      if (fenceHistory && fenceHistory.restored_at === null) {
        upgradeService.restoreReleaseFence(upgradeId);
        return Object.freeze({
          ...upgradeService.get(upgradeId), completed_step: 'release-generation-restore',
        });
      }
      if (sourceInvalidation !== null
        && loadEffect(upgradeId, 'legacy-source-rollback')?.state !== 'completed') {
        if (legacySourceAdapter === null) {
          return Object.freeze({ ...run, status: 'waiting_for_legacy_source_rollback' });
        }
        await performEffect(
          upgradeId,
          'legacy-source-rollback',
          {
            upgrade_id: upgradeId,
            rollback_queue_ref: sourceInvalidation.result.rollback_queue_ref,
            rollback_queue_sha256: sourceInvalidation.result.rollback_queue_sha256,
          },
          (request) => legacySourceAdapter.rollback(request),
        );
        return Object.freeze({ ...run, completed_step: 'legacy-source-rollback' });
      }
      return upgradeService.completeRollback(upgradeId);
    }
    if (run.state === 'committed') {
      const sourceInvalidation = loadEffect(upgradeId, 'legacy-source-invalidate');
      if (sourceInvalidation !== null
        && loadEffect(upgradeId, 'legacy-source-commit')?.state !== 'completed') {
        if (legacySourceAdapter === null) {
          return Object.freeze({ ...run, status: 'waiting_for_legacy_source_commit' });
        }
        await performEffect(
          upgradeId,
          'legacy-source-commit',
          {
            upgrade_id: upgradeId,
            rollback_queue_ref: sourceInvalidation.result.rollback_queue_ref,
            rollback_queue_sha256: sourceInvalidation.result.rollback_queue_sha256,
          },
          (request) => legacySourceAdapter.commit(request),
        );
        return Object.freeze({ ...run, completed_step: 'legacy-source-commit' });
      }
      if (loadEffect(upgradeId, 'postcommit-cleanup')?.state !== 'completed') {
        await performEffect(
          upgradeId,
          'postcommit-cleanup',
          { upgrade_id: upgradeId, release_ref: run.to_release },
          (request) => releaseAdapter.cleanup(request),
        );
        return Object.freeze({ ...run, completed_step: 'postcommit-cleanup' });
      }
    }
    return run;
  }

  return Object.freeze({ advance, loadEffect });
}
