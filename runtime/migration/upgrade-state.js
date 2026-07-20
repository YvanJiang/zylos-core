import path from 'node:path';

export const RUNTIME_UPGRADE_OWNERSHIP_MARKER = 'atomic-upgrade-owner.json';
export const RUNTIME_UPGRADE_OWNERSHIP_LOCK = 'upgrade-owner.lock';

export function runtimeUpgradeOwnershipMarkerPath(zylosDir) {
  if (typeof zylosDir !== 'string' || zylosDir.length === 0) {
    throw new TypeError('zylosDir must be a non-empty string');
  }
  return path.join(zylosDir, 'runtime', RUNTIME_UPGRADE_OWNERSHIP_MARKER);
}

export function runtimeUpgradeOwnershipLockPath(zylosDir) {
  if (typeof zylosDir !== 'string' || zylosDir.length === 0) {
    throw new TypeError('zylosDir must be a non-empty string');
  }
  return path.join(zylosDir, 'runtime', RUNTIME_UPGRADE_OWNERSHIP_LOCK);
}

export const BLOCKING_UPGRADE_STATES = Object.freeze([
  'maintenance',
  'drained',
  'migrating',
  'health_check',
  'ready_to_commit',
  'rollback_required',
]);

export const BLOCKING_UPGRADE_STATES_SQL = BLOCKING_UPGRADE_STATES
  .map((state) => `'${state}'`)
  .join(', ');

export function findBlockingRuntimeUpgrade(database, botId) {
  return database.prepare(`
    SELECT upgrade_id, scope_kind, bot_id, state, state_version
    FROM runtime_upgrade_runs
    WHERE state IN (${BLOCKING_UPGRADE_STATES_SQL})
      AND (scope_kind = 'installation' OR (scope_kind = 'bot' AND bot_id = ?))
    ORDER BY created_at ASC
    LIMIT 1
  `).get(botId) ?? null;
}

export function findAnyBlockingRuntimeUpgrade(database) {
  return database.prepare(`
    SELECT upgrade_id, scope_kind, bot_id, state, state_version
    FROM runtime_upgrade_runs
    WHERE state IN (${BLOCKING_UPGRADE_STATES_SQL})
    ORDER BY created_at ASC
    LIMIT 1
  `).get() ?? null;
}
