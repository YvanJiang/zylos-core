import fs from 'node:fs';
import path from 'node:path';

import { initializeRuntimePersistence } from '../persistence/schema.js';

export class WorkspaceLeaseError extends Error {
  constructor(code, message) {
    super(message);
    this.name = 'WorkspaceLeaseError';
    this.code = code;
  }
}

function workspaceConflict(code, message) {
  throw new WorkspaceLeaseError(code, message);
}

function parseNow(value) {
  const timestamp = Date.parse(value);
  if (!Number.isFinite(timestamp)) {
    throw new TypeError('now must return an RFC 3339 timestamp');
  }
  return timestamp;
}

export function normalizeWorkspaceRoot(root, { base = process.cwd() } = {}) {
  if (typeof root !== 'string' || root.length === 0) {
    throw new TypeError('workspace root must be a non-empty string');
  }
  if (typeof base !== 'string' || base.length === 0) {
    throw new TypeError('workspace root base must be a non-empty string');
  }
  const resolved = path.resolve(base, root);
  const missingSegments = [];
  let existing = resolved;
  while (!fs.existsSync(existing)) {
    const parent = path.dirname(existing);
    if (parent === existing) break;
    missingSegments.unshift(path.basename(existing));
    existing = parent;
  }
  const canonicalExisting = fs.realpathSync.native(existing);
  return path.resolve(canonicalExisting, ...missingSegments);
}

export function normalizeReadyWorkspaceRoot(root) {
  if (typeof root !== 'string' || !path.isAbsolute(root)) {
    throw new TypeError('ready workspace root must be an absolute path');
  }
  const resolved = path.resolve(root);
  if (resolved === path.parse(resolved).root) {
    throw new TypeError('ready workspace root must not be the filesystem root');
  }
  const stat = fs.lstatSync(resolved);
  if (stat.isSymbolicLink() || !stat.isDirectory()) {
    throw new TypeError('ready workspace root must be a non-symlink directory');
  }
  const canonical = fs.realpathSync.native(resolved);
  if (canonical !== resolved) {
    throw new TypeError('ready workspace root must already be canonical');
  }
  return canonical;
}

export function workspaceRootsOverlap(left, right) {
  const leftRoot = normalizeWorkspaceRoot(left);
  const rightRoot = normalizeWorkspaceRoot(right);
  return leftRoot === rightRoot
    || leftRoot.startsWith(`${rightRoot}${path.sep}`)
    || rightRoot.startsWith(`${leftRoot}${path.sep}`);
}

export function resolveProviderWorkspaceAccess(adapter, context, {
  authoritativeRoot = null,
  bindingKind,
  defaultRoot,
  workspaceGeneration,
  workspaceId,
  workspaceState,
} = {}) {
  if (!adapter || typeof adapter !== 'object') {
    throw new TypeError('adapter must be an object');
  }
  const trustedContext = Object.freeze({
    conversation_id: context?.conversation_id ?? null,
    turn_id: context?.turn_id ?? null,
    lineage_id: context?.lineage_id ?? null,
    provider: context?.provider ?? null,
  });
  const descriptor = typeof adapter.getWorkspaceAccess === 'function'
    ? adapter.getWorkspaceAccess(trustedContext)
    : null;
  if (descriptor !== null && (typeof descriptor !== 'object' || Array.isArray(descriptor))) {
    throw new TypeError('adapter.getWorkspaceAccess must return an object or null');
  }
  const root = typeof authoritativeRoot === 'string' && authoritativeRoot.length > 0
    ? authoritativeRoot
    : (typeof descriptor?.root === 'string' && descriptor.root.length > 0
      ? descriptor.root
      : defaultRoot);
  const enforcedReadOnly = descriptor?.mode === 'read_only'
    && descriptor.read_only_enforced === true
    && descriptor.authority === 'provider_sandbox';
  const access = {
    workspace_root: normalizeWorkspaceRoot(root, { base: defaultRoot }),
    mode: enforcedReadOnly ? 'read_only' : 'writable',
    read_only_enforced: enforcedReadOnly,
  };
  if (
    authoritativeRoot === null
    && bindingKind === undefined
    && workspaceGeneration === undefined
    && workspaceId === undefined
    && workspaceState === undefined
  ) {
    return Object.freeze(access);
  }
  return Object.freeze({
    ...access,
    binding_kind: bindingKind ?? 'legacy_shared',
    workspace_generation: workspaceGeneration ?? 0,
    workspace_id: workspaceId ?? null,
    workspace_state: workspaceState ?? (
      bindingKind === 'legacy_shared' ? 'legacy_shared' : null
    ),
  });
}

function projectLease(row, status = 'acquired') {
  return Object.freeze({
    status,
    workspace_lease_id: row.workspace_lease_id,
    workspace_root: row.workspace_root,
    mode: row.mode,
    holder_service_instance_id: row.holder_service_instance_id,
    holder_conversation_id: row.holder_conversation_id,
    holder_turn_id: row.holder_turn_id,
    lease_epoch: row.lease_epoch,
    lease_expires_at: row.lease_expires_at,
    acquired_at: row.acquired_at,
  });
}

const UNCERTAIN_TURN_STATES = Object.freeze(new Set([
  'starting',
  'running',
  'waiting_user',
  'redirecting',
  'recovering',
  'timed_out',
]));

export function createWorkspaceLeaseCoordinator({
  database,
  serviceInstanceId,
  now = () => new Date().toISOString(),
  generateId,
  leaseDurationMs = 10_000,
}) {
  if (!database || typeof database.transaction !== 'function') {
    throw new TypeError('database must be a better-sqlite3 connection');
  }
  if (typeof serviceInstanceId !== 'string' || serviceInstanceId.length === 0) {
    throw new TypeError('serviceInstanceId must be a non-empty string');
  }
  if (typeof now !== 'function') throw new TypeError('now must be a function');
  if (typeof generateId !== 'function') throw new TypeError('generateId must be a function');
  if (!Number.isFinite(leaseDurationMs) || leaseDurationMs <= 0) {
    throw new TypeError('leaseDurationMs must be a positive finite number');
  }
  initializeRuntimePersistence(database);

  function expireElapsedLeases(expiredAt) {
    const expired = database.prepare(`
      SELECT lease.workspace_lease_id, turn.state AS turn_state,
        EXISTS (
          SELECT 1 FROM runtime_workspace_background_work AS background
          WHERE background.workspace_lease_id = lease.workspace_lease_id
            AND background.state = 'active'
        ) AS has_background_work
      FROM runtime_workspace_leases AS lease
      JOIN runtime_turns AS turn ON turn.turn_id = lease.holder_turn_id
      WHERE lease.state = 'active' AND lease.lease_expires_at <= ?
    `).all(expiredAt);
    for (const row of expired) {
      const workspaceLeaseId = row.workspace_lease_id;
      const uncertain = UNCERTAIN_TURN_STATES.has(row.turn_state)
        || row.has_background_work === 1;
      database.prepare(`
        UPDATE runtime_workspace_background_work
        SET state = 'unknown', ended_at = ?
        WHERE workspace_lease_id = ? AND state = 'active'
      `).run(expiredAt, workspaceLeaseId);
      database.prepare(`
        UPDATE runtime_workspace_leases
        SET state = ?, released_at = ?, updated_at = ?
        WHERE workspace_lease_id = ? AND state = 'active' AND lease_expires_at <= ?
      `).run(
        uncertain ? 'uncertain' : 'expired',
        uncertain ? null : expiredAt,
        expiredAt,
        workspaceLeaseId,
        expiredAt,
      );
    }
    return expired.length;
  }

  function findConflict(canonicalRoot, mode) {
    const active = database.prepare(`
      SELECT * FROM runtime_workspace_leases
      WHERE state IN ('active', 'uncertain')
      ORDER BY acquired_at, workspace_lease_id
    `).all();
    const conflicting = active.find((lease) => (
      workspaceRootsOverlap(lease.workspace_root, canonicalRoot)
      && !(lease.mode === 'read_only' && mode === 'read_only')
    ));
    if (!conflicting) return null;
    return Object.freeze({
      status: 'wait',
      wait_reason: 'workspace_lease',
      workspace_root: canonicalRoot,
      mode,
      conflicting_workspace_root: conflicting.workspace_root,
      holder_conversation_id: conflicting.holder_conversation_id,
      holder_turn_id: conflicting.holder_turn_id,
      holder_background_work_ids: database.prepare(`
        SELECT background_work_id
        FROM runtime_workspace_background_work
        WHERE workspace_lease_id = ? AND state IN ('active', 'unknown')
        ORDER BY background_work_id
      `).all(conflicting.workspace_lease_id).map(({ background_work_id: id }) => id),
      recovery_required: conflicting.state === 'uncertain',
    });
  }

  function loadCurrentFence(fence, checkedAt = now()) {
    if (!fence || typeof fence.workspace_lease_id !== 'string') {
      workspaceConflict('stale_workspace_lease', 'A current workspace lease fence is required.');
    }
    const row = database.prepare(`
      SELECT *
      FROM runtime_workspace_leases
      WHERE workspace_lease_id = ?
    `).get(fence.workspace_lease_id);
    if (
      !row
      || row.state !== 'active'
      || row.holder_service_instance_id !== serviceInstanceId
      || row.workspace_root !== fence.workspace_root
      || row.holder_conversation_id !== fence.holder_conversation_id
      || row.holder_turn_id !== fence.holder_turn_id
      || row.lease_epoch !== fence.lease_epoch
      || row.lease_expires_at <= checkedAt
    ) {
      workspaceConflict(
        'stale_workspace_lease',
        'The workspace lease no longer matches the current holder and fencing epoch.',
      );
    }
    return row;
  }

  function loadHeldOrUncertainFence(fence) {
    if (!fence || typeof fence.workspace_lease_id !== 'string') {
      workspaceConflict('stale_workspace_lease', 'A workspace lease fence is required.');
    }
    const row = database.prepare(`
      SELECT * FROM runtime_workspace_leases
      WHERE workspace_lease_id = ?
    `).get(fence.workspace_lease_id);
    if (
      !row
      || !['active', 'uncertain'].includes(row.state)
      || row.holder_service_instance_id !== serviceInstanceId
      || row.workspace_root !== fence.workspace_root
      || row.holder_conversation_id !== fence.holder_conversation_id
      || row.holder_turn_id !== fence.holder_turn_id
      || row.lease_epoch !== fence.lease_epoch
    ) {
      workspaceConflict(
        'stale_workspace_lease',
        'The workspace isolation fence no longer matches its durable holder and epoch.',
      );
    }
    return row;
  }

  function acquire({
    workspace_root: workspaceRoot,
    mode,
    holder_conversation_id: holderConversationId,
    holder_turn_id: holderTurnId,
  }) {
    if (!['writable', 'read_only'].includes(mode)) {
      throw new TypeError('workspace lease mode must be writable or read_only');
    }
    for (const [name, value] of Object.entries({
      holder_conversation_id: holderConversationId,
      holder_turn_id: holderTurnId,
    })) {
      if (typeof value !== 'string' || value.length === 0) {
        throw new TypeError(`${name} must be a non-empty string`);
      }
    }
    const canonicalRoot = normalizeWorkspaceRoot(workspaceRoot);
    const acquireLease = database.transaction(() => {
      const acquiredAt = now();
      parseNow(acquiredAt);
      expireElapsedLeases(acquiredAt);
      const existing = database.prepare(`
        SELECT * FROM runtime_workspace_leases
        WHERE holder_turn_id = ? AND state IN ('active', 'uncertain')
      `).get(holderTurnId);
      if (existing) {
        if (
          existing.state === 'active'
          && existing.holder_service_instance_id === serviceInstanceId
          && existing.holder_conversation_id === holderConversationId
          && existing.workspace_root === canonicalRoot
          && existing.mode === mode
        ) return projectLease(existing, 'already_acquired');
        workspaceConflict('lease_conflict', 'The turn already holds a different workspace lease.');
      }

      const conflicting = findConflict(canonicalRoot, mode);
      if (conflicting) return conflicting;

      database.prepare(`
        INSERT INTO runtime_workspace_lease_fences (workspace_root, last_epoch, updated_at)
        VALUES (?, 1, ?)
        ON CONFLICT(workspace_root) DO UPDATE SET
          last_epoch = runtime_workspace_lease_fences.last_epoch + 1,
          updated_at = excluded.updated_at
      `).run(canonicalRoot, acquiredAt);
      const leaseEpoch = database.prepare(`
        SELECT last_epoch FROM runtime_workspace_lease_fences WHERE workspace_root = ?
      `).get(canonicalRoot).last_epoch;
      const workspaceLeaseId = generateId('workspace-lease');
      if (typeof workspaceLeaseId !== 'string' || workspaceLeaseId.length === 0) {
        throw new TypeError('generateId must return a non-empty workspace lease ID');
      }
      const leaseExpiresAt = new Date(
        parseNow(acquiredAt) + leaseDurationMs,
      ).toISOString();
      database.prepare(`
        INSERT INTO runtime_workspace_leases (
          workspace_lease_id, workspace_root, mode, holder_service_instance_id,
          holder_conversation_id, holder_turn_id, lease_epoch, lease_expires_at,
          state, acquired_at, updated_at, released_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, ?, NULL)
      `).run(
        workspaceLeaseId,
        canonicalRoot,
        mode,
        serviceInstanceId,
        holderConversationId,
        holderTurnId,
        leaseEpoch,
        leaseExpiresAt,
        acquiredAt,
        acquiredAt,
      );
      return projectLease(database.prepare(`
        SELECT * FROM runtime_workspace_leases WHERE workspace_lease_id = ?
      `).get(workspaceLeaseId));
    });
    return database.inTransaction ? acquireLease() : acquireLease.immediate();
  }

  function assertCurrent(fence) {
    return projectLease(loadCurrentFence(fence), 'current');
  }

  function assertWritable(fence) {
    const row = loadCurrentFence(fence);
    if (row.mode !== 'writable') {
      workspaceConflict('workspace_read_only', 'The provider sandbox enforces read-only access.');
    }
    return projectLease(row, 'current');
  }

  function assertHeldOrUncertain(fence) {
    const row = loadHeldOrUncertainFence(fence);
    return projectLease(row, row.state);
  }

  function isRecoveryRequired(fence) {
    try {
      return loadHeldOrUncertainFence(fence).state === 'uncertain';
    } catch (error) {
      if (error instanceof WorkspaceLeaseError) return false;
      throw error;
    }
  }

  function inspect({ workspace_root: workspaceRoot, mode }) {
    if (!['writable', 'read_only'].includes(mode)) {
      throw new TypeError('workspace lease mode must be writable or read_only');
    }
    const canonicalRoot = normalizeWorkspaceRoot(workspaceRoot);
    const inspectLease = database.transaction(() => {
      const inspectedAt = now();
      parseNow(inspectedAt);
      expireElapsedLeases(inspectedAt);
      return findConflict(canonicalRoot, mode) ?? Object.freeze({
        status: 'available',
        workspace_root: canonicalRoot,
        mode,
      });
    });
    return database.inTransaction ? inspectLease() : inspectLease.immediate();
  }

  function heartbeatOwned() {
    const heartbeat = database.transaction(() => {
      const heartbeatAt = now();
      parseNow(heartbeatAt);
      expireElapsedLeases(heartbeatAt);
      const leaseExpiresAt = new Date(parseNow(heartbeatAt) + leaseDurationMs).toISOString();
      const renewed = database.prepare(`
        UPDATE runtime_workspace_leases
        SET lease_expires_at = ?, updated_at = ?
        WHERE holder_service_instance_id = ? AND state = 'active'
          AND lease_expires_at > ?
      `).run(leaseExpiresAt, heartbeatAt, serviceInstanceId, heartbeatAt);
      const uncertain = database.prepare(`
        SELECT COUNT(*) AS count FROM runtime_workspace_leases
        WHERE holder_service_instance_id = ? AND state = 'uncertain'
      `).get(serviceInstanceId).count;
      return { renewed: renewed.changes, uncertain };
    });
    const result = database.inTransaction ? heartbeat() : heartbeat.immediate();
    if (result.uncertain > 0) {
      workspaceConflict(
        'stale_workspace_lease',
        'A workspace writer expired before its ownership heartbeat.',
      );
    }
    return result.renewed;
  }

  function renew(fence) {
    const renewLease = database.transaction(() => {
      const renewedAt = now();
      const row = loadCurrentFence(fence, renewedAt);
      const leaseExpiresAt = new Date(parseNow(renewedAt) + leaseDurationMs).toISOString();
      const renewed = database.prepare(`
        UPDATE runtime_workspace_leases
        SET lease_expires_at = ?, updated_at = ?
        WHERE workspace_lease_id = ? AND state = 'active'
          AND holder_service_instance_id = ? AND lease_epoch = ?
          AND lease_expires_at > ?
      `).run(
        leaseExpiresAt,
        renewedAt,
        row.workspace_lease_id,
        serviceInstanceId,
        row.lease_epoch,
        renewedAt,
      );
      if (renewed.changes !== 1) {
        workspaceConflict('stale_workspace_lease', 'The workspace lease expired before renewal.');
      }
      return projectLease(database.prepare(`
        SELECT * FROM runtime_workspace_leases WHERE workspace_lease_id = ?
      `).get(row.workspace_lease_id), 'renewed');
    });
    return database.inTransaction ? renewLease() : renewLease.immediate();
  }

  function release(fence) {
    const releaseLease = database.transaction(() => {
      const releasedAt = now();
      const row = loadCurrentFence(fence, releasedAt);
      const background = database.prepare(`
        SELECT 1 FROM runtime_workspace_background_work
        WHERE workspace_lease_id = ? AND state = 'active' LIMIT 1
      `).get(row.workspace_lease_id);
      if (background) {
        workspaceConflict(
          'workspace_background_active',
          'The workspace lease cannot be released while background work is active.',
        );
      }
      const released = database.prepare(`
        UPDATE runtime_workspace_leases
        SET state = 'released', released_at = ?, updated_at = ?
        WHERE workspace_lease_id = ? AND state = 'active'
          AND holder_service_instance_id = ? AND lease_epoch = ?
          AND lease_expires_at > ?
      `).run(
        releasedAt,
        releasedAt,
        row.workspace_lease_id,
        serviceInstanceId,
        row.lease_epoch,
        releasedAt,
      );
      if (released.changes !== 1) {
        workspaceConflict('stale_workspace_lease', 'The workspace lease release lost its fence.');
      }
      return Object.freeze({ ...projectLease(row, 'released'), released_at: releasedAt });
    });
    return database.inTransaction ? releaseLease() : releaseLease.immediate();
  }

  function releaseAfterIsolation(fence, { reason } = {}) {
    if (typeof reason !== 'string' || reason.length === 0) {
      throw new TypeError('an isolation reason is required to release uncertain workspace work');
    }
    const releaseLease = database.transaction(() => {
      const releasedAt = now();
      const row = database.prepare(`
        SELECT * FROM runtime_workspace_leases WHERE workspace_lease_id = ?
      `).get(fence?.workspace_lease_id);
      if (
        !row
        || !['active', 'uncertain'].includes(row.state)
        || row.holder_service_instance_id !== serviceInstanceId
        || row.workspace_root !== fence.workspace_root
        || row.holder_conversation_id !== fence.holder_conversation_id
        || row.holder_turn_id !== fence.holder_turn_id
        || row.lease_epoch !== fence.lease_epoch
      ) {
        workspaceConflict(
          'stale_workspace_lease',
          'The isolated workspace lease no longer matches its durable fence.',
        );
      }
      database.prepare(`
        UPDATE runtime_workspace_background_work
        SET state = 'unknown', ended_at = ?, error_json = COALESCE(error_json, ?)
        WHERE workspace_lease_id = ? AND state = 'active'
      `).run(releasedAt, JSON.stringify({ code: reason }), row.workspace_lease_id);
      const released = database.prepare(`
        UPDATE runtime_workspace_leases
        SET state = 'released', released_at = ?, updated_at = ?
        WHERE workspace_lease_id = ? AND state IN ('active', 'uncertain')
          AND holder_service_instance_id = ? AND lease_epoch = ?
      `).run(
        releasedAt,
        releasedAt,
        row.workspace_lease_id,
        serviceInstanceId,
        row.lease_epoch,
      );
      if (released.changes !== 1) {
        workspaceConflict('stale_workspace_lease', 'The isolated workspace release lost its fence.');
      }
      return Object.freeze({
        ...projectLease(row, 'released_after_isolation'),
        released_at: releasedAt,
        isolation_reason: reason,
      });
    });
    return database.inTransaction ? releaseLease() : releaseLease.immediate();
  }

  function releaseTurnAfterIsolation(holderTurnId, { reason } = {}) {
    if (typeof holderTurnId !== 'string' || holderTurnId.length === 0) {
      throw new TypeError('holderTurnId must be a non-empty string');
    }
    const row = database.prepare(`
      SELECT * FROM runtime_workspace_leases
      WHERE holder_turn_id = ? AND holder_service_instance_id = ?
        AND state IN ('active', 'uncertain')
    `).get(holderTurnId, serviceInstanceId);
    if (!row) return null;
    return releaseAfterIsolation(projectLease(row), { reason });
  }

  function listRecoveryCandidates() {
    const listedAt = now();
    const list = database.transaction(() => {
      expireElapsedLeases(listedAt);
      return database.prepare(`
        SELECT * FROM runtime_workspace_leases
        WHERE state = 'uncertain' AND holder_service_instance_id != ?
        ORDER BY updated_at, workspace_lease_id
      `).all(serviceInstanceId).map((row) => projectLease(row, 'uncertain'));
    });
    return database.inTransaction ? list() : list.immediate();
  }

  function adoptUncertainForRecovery(fence) {
    const adopt = database.transaction(() => {
      const adoptedAt = now();
      parseNow(adoptedAt);
      const row = database.prepare(`
        SELECT * FROM runtime_workspace_leases WHERE workspace_lease_id = ?
      `).get(fence?.workspace_lease_id);
      if (
        !row
        || row.state !== 'uncertain'
        || row.holder_service_instance_id === serviceInstanceId
        || row.holder_service_instance_id !== fence.holder_service_instance_id
        || row.workspace_root !== fence.workspace_root
        || row.holder_conversation_id !== fence.holder_conversation_id
        || row.holder_turn_id !== fence.holder_turn_id
        || row.lease_epoch !== fence.lease_epoch
      ) {
        workspaceConflict(
          'stale_workspace_lease',
          'The orphaned workspace recovery fence is no longer current.',
        );
      }
      database.prepare(`
        INSERT INTO runtime_workspace_lease_fences (workspace_root, last_epoch, updated_at)
        VALUES (?, 1, ?)
        ON CONFLICT(workspace_root) DO UPDATE SET
          last_epoch = runtime_workspace_lease_fences.last_epoch + 1,
          updated_at = excluded.updated_at
      `).run(row.workspace_root, adoptedAt);
      const leaseEpoch = database.prepare(`
        SELECT last_epoch FROM runtime_workspace_lease_fences WHERE workspace_root = ?
      `).get(row.workspace_root).last_epoch;
      const leaseExpiresAt = new Date(parseNow(adoptedAt) + leaseDurationMs).toISOString();
      const adopted = database.prepare(`
        UPDATE runtime_workspace_leases
        SET holder_service_instance_id = ?, lease_epoch = ?, lease_expires_at = ?, updated_at = ?
        WHERE workspace_lease_id = ? AND state = 'uncertain'
          AND holder_service_instance_id = ? AND lease_epoch = ?
      `).run(
        serviceInstanceId,
        leaseEpoch,
        leaseExpiresAt,
        adoptedAt,
        row.workspace_lease_id,
        row.holder_service_instance_id,
        row.lease_epoch,
      );
      if (adopted.changes !== 1) {
        workspaceConflict('stale_workspace_lease', 'The orphaned workspace adoption lost its CAS.');
      }
      return projectLease(database.prepare(`
        SELECT * FROM runtime_workspace_leases WHERE workspace_lease_id = ?
      `).get(row.workspace_lease_id), 'uncertain');
    });
    return database.inTransaction ? adopt() : adopt.immediate();
  }

  function startBackgroundWork(fence, { provider_task_id: providerTaskId }) {
    if (typeof providerTaskId !== 'string' || providerTaskId.length === 0) {
      throw new TypeError('provider_task_id must be a non-empty string');
    }
    const startWork = database.transaction(() => {
      const startedAt = now();
      const lease = loadCurrentFence(fence, startedAt);
      const existing = database.prepare(`
        SELECT * FROM runtime_workspace_background_work
        WHERE workspace_lease_id = ? AND provider_task_id = ?
      `).get(lease.workspace_lease_id, providerTaskId);
      if (existing) {
        return Object.freeze({
          status: existing.state === 'active' ? 'already_started' : 'already_finished',
          background_work_id: existing.background_work_id,
          workspace_lease_id: existing.workspace_lease_id,
          provider_task_id: existing.provider_task_id,
          state: existing.state,
          started_at: existing.started_at,
          ended_at: existing.ended_at,
        });
      }
      const backgroundWorkId = generateId('background-work');
      if (typeof backgroundWorkId !== 'string' || backgroundWorkId.length === 0) {
        throw new TypeError('generateId must return a non-empty background work ID');
      }
      database.prepare(`
        INSERT INTO runtime_workspace_background_work (
          background_work_id, workspace_lease_id, holder_turn_id, provider_task_id,
          state, started_at, ended_at, error_json
        ) VALUES (?, ?, ?, ?, 'active', ?, NULL, NULL)
      `).run(
        backgroundWorkId,
        lease.workspace_lease_id,
        lease.holder_turn_id,
        providerTaskId,
        startedAt,
      );
      return Object.freeze({
        status: 'started',
        background_work_id: backgroundWorkId,
        workspace_lease_id: lease.workspace_lease_id,
        provider_task_id: providerTaskId,
        state: 'active',
        started_at: startedAt,
        ended_at: null,
      });
    });
    return database.inTransaction ? startWork() : startWork.immediate();
  }

  function finishBackgroundWork(fence, {
    provider_task_id: providerTaskId,
    outcome,
    error = null,
  }) {
    if (typeof providerTaskId !== 'string' || providerTaskId.length === 0) {
      throw new TypeError('provider_task_id must be a non-empty string');
    }
    if (!['completed', 'failed', 'unknown'].includes(outcome)) {
      throw new TypeError('background work outcome must be completed, failed, or unknown');
    }
    const finishWork = database.transaction(() => {
      const endedAt = now();
      const lease = loadCurrentFence(fence, endedAt);
      const current = database.prepare(`
        SELECT * FROM runtime_workspace_background_work
        WHERE workspace_lease_id = ? AND provider_task_id = ?
      `).get(lease.workspace_lease_id, providerTaskId);
      if (!current) {
        workspaceConflict(
          'background_work_not_found',
          'The provider background work is not registered under this workspace lease.',
        );
      }
      if (current.state !== 'active') {
        if (current.state === outcome) {
          return Object.freeze({
            status: 'already_finished',
            background_work_id: current.background_work_id,
            workspace_lease_id: current.workspace_lease_id,
            provider_task_id: current.provider_task_id,
            state: current.state,
            started_at: current.started_at,
            ended_at: current.ended_at,
          });
        }
        workspaceConflict('version_conflict', 'The background work already has another outcome.');
      }
      const updated = database.prepare(`
        UPDATE runtime_workspace_background_work
        SET state = ?, ended_at = ?, error_json = ?
        WHERE background_work_id = ? AND state = 'active'
      `).run(
        outcome,
        endedAt,
        error === null ? null : JSON.stringify(error),
        current.background_work_id,
      );
      if (updated.changes !== 1) {
        workspaceConflict('stale_workspace_lease', 'The background work outcome lost its fence.');
      }
      return Object.freeze({
        status: 'finished',
        background_work_id: current.background_work_id,
        workspace_lease_id: current.workspace_lease_id,
        provider_task_id: current.provider_task_id,
        state: outcome,
        started_at: current.started_at,
        ended_at: endedAt,
      });
    });
    return database.inTransaction ? finishWork() : finishWork.immediate();
  }

  function hasBlockingBackgroundWork(conversationId) {
    if (typeof conversationId !== 'string' || conversationId.length === 0) {
      throw new TypeError('conversationId must be a non-empty string');
    }
    return database.prepare(`
      SELECT 1
      FROM runtime_workspace_background_work AS background
      JOIN runtime_workspace_leases AS lease
        ON lease.workspace_lease_id = background.workspace_lease_id
      WHERE lease.holder_conversation_id = ?
        AND lease.state IN ('active', 'uncertain')
        AND background.state IN ('active', 'unknown')
      LIMIT 1
    `).get(conversationId) !== undefined;
  }

  function listActive() {
    const listedAt = now();
    const list = database.transaction(() => {
      expireElapsedLeases(listedAt);
      const workspaceWaiters = database.prepare(`
        SELECT conversation_id, turn_id, wait_detail_json
        FROM runtime_turn_queue
        WHERE status = 'queued' AND wait_reason = 'workspace_lease'
          AND wait_detail_json IS NOT NULL
        ORDER BY conversation_id, turn_id
      `).all().map((waiter) => ({
        ...waiter,
        detail: JSON.parse(waiter.wait_detail_json),
      }));
      return database.prepare(`
        SELECT * FROM runtime_workspace_leases
        WHERE state IN ('active', 'uncertain')
        ORDER BY workspace_root, lease_epoch, workspace_lease_id
      `).all().map((row) => Object.freeze({
        ...projectLease(row, row.state),
        recovery_required: row.state === 'uncertain',
        holder_background_work_ids: database.prepare(`
          SELECT background_work_id
          FROM runtime_workspace_background_work
          WHERE workspace_lease_id = ? AND state IN ('active', 'unknown')
          ORDER BY background_work_id
        `).all(row.workspace_lease_id).map(({ background_work_id: id }) => id),
        waiters: workspaceWaiters.filter(
          ({ detail }) => detail.holder_turn_id === row.holder_turn_id,
        ).map((waiter) => Object.freeze({
          conversation_id: waiter.conversation_id,
          turn_id: waiter.turn_id,
          workspace_root: waiter.detail.workspace_root,
          mode: waiter.detail.mode,
        })),
      }));
    });
    return database.inTransaction ? list() : list.immediate();
  }

  function listObservability() {
    return Object.freeze({
      complete: true,
      items: Object.freeze(listActive().map((lease) => Object.freeze({
        workspace_root: lease.workspace_root,
        mode: lease.mode === 'writable' ? 'write' : 'read',
        holder_conversation_id: lease.holder_conversation_id,
        holder_turn_id: lease.holder_turn_id,
        holder_background_work_id: lease.holder_background_work_ids[0] ?? null,
        expires_at: lease.lease_expires_at,
        epoch: lease.lease_epoch,
        waiter_count: lease.waiters.length,
      }))),
      error: null,
    });
  }

  return Object.freeze({
    acquire,
    adoptUncertainForRecovery,
    assertCurrent,
    assertHeldOrUncertain,
    assertWritable,
    finishBackgroundWork,
    hasBlockingBackgroundWork,
    heartbeatOwned,
    inspect,
    isRecoveryRequired,
    listActive,
    listObservability,
    listRecoveryCandidates,
    release,
    releaseAfterIsolation,
    releaseTurnAfterIsolation,
    renew,
    startBackgroundWork,
  });
}
