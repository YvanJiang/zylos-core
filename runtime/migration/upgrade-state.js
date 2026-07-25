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

export function findResumableRuntimeUpgrade(database) {
  const blocking = findAnyBlockingRuntimeUpgrade(database);
  if (blocking !== null) return blocking;
  return database.prepare(`
    SELECT run.upgrade_id, run.scope_kind, run.bot_id, run.state, run.state_version
    FROM runtime_upgrade_runs AS run
    WHERE run.state NOT IN ('committed', 'rolled_back')
       OR (
         run.state = 'committed'
         AND NOT EXISTS (
           SELECT 1 FROM runtime_upgrade_effects AS effect
           WHERE effect.upgrade_id = run.upgrade_id
             AND effect.step_key = 'postcommit-cleanup'
             AND effect.state = 'completed'
         )
       )
    ORDER BY run.created_at ASC
    LIMIT 1
  `).get() ?? null;
}
