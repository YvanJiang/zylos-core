// Isolated, idempotent migration for scheduler rows created before Core-owned
// conversation identities. This module only clears and fences retired state; it
// exports no dispatch, execution, or selection surface.
export function retireLegacySchedulerControls(database, taskColumns) {
  const legacyControls = ['require_idle', 'reply_channel', 'reply_endpoint']
    .filter((name) => taskColumns.has(name));
  if (legacyControls.length === 0) return 0;

  const predicates = [];
  if (taskColumns.has('require_idle')) predicates.push('COALESCE(require_idle, 0) != 0');
  if (taskColumns.has('reply_channel')) predicates.push('reply_channel IS NOT NULL');
  if (taskColumns.has('reply_endpoint')) predicates.push('reply_endpoint IS NOT NULL');
  const assignments = [
    `status = CASE
      WHEN status = 'running' THEN 'running'
      WHEN status IN ('pending', 'paused') THEN 'paused'
      WHEN status = 'completed' AND type IN ('recurring', 'interval') THEN 'paused'
      ELSE status
    END`,
    'requires_reconfiguration = 1',
    "last_error = 'Paused during migration: retired scheduler controls require explicit canonical reconfiguration.'",
  ];
  if (taskColumns.has('require_idle')) assignments.push('require_idle = 0');
  if (taskColumns.has('reply_channel')) assignments.push('reply_channel = NULL');
  if (taskColumns.has('reply_endpoint')) assignments.push('reply_endpoint = NULL');
  return database.prepare(`
    UPDATE tasks SET ${assignments.join(', ')}
    WHERE (${predicates.join(' OR ')})
  `).run().changes;
}
