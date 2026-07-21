import path from 'node:path';

// Canonical, read-only identity facts for the one-time retired supervisor
// boundary. Consumers may inspect these facts but must not use this module to
// dispatch, stop, or otherwise operate a process.
export const RETIRED_PM2_SERVICE_DESCRIPTORS = Object.freeze([
  Object.freeze({
    name: 'activity-monitor',
    path_segments: Object.freeze(['.claude', 'skills', 'activity-monitor', 'scripts', 'activity-monitor.js']),
  }),
  Object.freeze({
    name: 'c4-dispatcher',
    path_segments: Object.freeze(['.claude', 'skills', 'comm-bridge', 'scripts', 'c4-dispatcher.js']),
  }),
  Object.freeze({
    name: 'scheduler',
    path_segments: Object.freeze(['.claude', 'skills', 'scheduler', 'scripts', 'daemon.js']),
  }),
  Object.freeze({
    name: 'web-console',
    path_segments: Object.freeze(['.claude', 'skills', 'web-console', 'scripts', 'server.js']),
  }),
  Object.freeze({ name: 'caddy', path_segments: Object.freeze(['bin', 'caddy']) }),
]);

export const RETIRED_PM2_SERVICE_NAMES = Object.freeze(
  RETIRED_PM2_SERVICE_DESCRIPTORS.map(({ name }) => name),
);

export function retiredPm2ServicePaths(zylosDir) {
  if (typeof zylosDir !== 'string' || !path.isAbsolute(zylosDir)) {
    throw new TypeError('zylosDir must be an explicit absolute non-root path');
  }
  const root = path.resolve(zylosDir);
  if (path.parse(root).root === root) {
    throw new TypeError('zylosDir must be an explicit absolute non-root path');
  }
  return new Map(RETIRED_PM2_SERVICE_DESCRIPTORS.map(({ name, path_segments: segments }) => [
    name,
    path.join(root, ...segments),
  ]));
}
