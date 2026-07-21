import path from 'node:path';

// Canonical, read-only identity facts for the one-time retired supervisor
// boundary. Consumers may inspect these facts but must not use this module to
// dispatch, stop, or otherwise operate a process.
export const RETIRED_PM2_SERVICE_NAMES = Object.freeze([
  'activity-monitor', 'c4-dispatcher', 'scheduler', 'web-console', 'caddy',
]);

export function retiredPm2ServicePaths(zylosDir) {
  if (typeof zylosDir !== 'string' || !path.isAbsolute(zylosDir)) {
    throw new TypeError('zylosDir must be an explicit absolute non-root path');
  }
  const root = path.resolve(zylosDir);
  if (path.parse(root).root === root) {
    throw new TypeError('zylosDir must be an explicit absolute non-root path');
  }
  const skills = path.join(root, '.claude', 'skills');
  return new Map([
    ['activity-monitor', path.join(skills, 'activity-monitor', 'scripts', 'activity-monitor.js')],
    ['c4-dispatcher', path.join(skills, 'comm-bridge', 'scripts', 'c4-dispatcher.js')],
    ['scheduler', path.join(skills, 'scheduler', 'scripts', 'daemon.js')],
    ['web-console', path.join(skills, 'web-console', 'scripts', 'server.js')],
    ['caddy', path.join(root, 'bin', 'caddy')],
  ]);
}
