import path from 'node:path';

import { describe, expect, test } from '@jest/globals';

import {
  RETIRED_PM2_SERVICE_NAMES,
  retiredPm2ServicePaths,
} from '../runtime/retired-pm2-identities.js';

describe('canonical retired PM2 identities', () => {
  test('derives every selected-installation identity from one immutable owner', () => {
    const zylosDir = path.join(path.sep, 'tmp', 'zylos-retired-identities');
    expect(RETIRED_PM2_SERVICE_NAMES).toEqual([
      'activity-monitor', 'c4-dispatcher', 'scheduler', 'web-console', 'caddy',
    ]);
    expect([...retiredPm2ServicePaths(zylosDir)]).toEqual([
      ['activity-monitor', path.join(zylosDir, '.claude', 'skills', 'activity-monitor', 'scripts', 'activity-monitor.js')],
      ['c4-dispatcher', path.join(zylosDir, '.claude', 'skills', 'comm-bridge', 'scripts', 'c4-dispatcher.js')],
      ['scheduler', path.join(zylosDir, '.claude', 'skills', 'scheduler', 'scripts', 'daemon.js')],
      ['web-console', path.join(zylosDir, '.claude', 'skills', 'web-console', 'scripts', 'server.js')],
      ['caddy', path.join(zylosDir, 'bin', 'caddy')],
    ]);
  });
});
