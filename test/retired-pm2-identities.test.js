import path from 'node:path';

import { describe, expect, test } from '@jest/globals';

import {
  RETIRED_PM2_SERVICE_DESCRIPTORS,
  RETIRED_PM2_SERVICE_NAMES,
  retiredPm2ServicePaths,
} from '../runtime/migration/retired-pm2-identities.js';

describe('canonical retired PM2 identities', () => {
  test('derives every selected-installation identity from one immutable owner', () => {
    const zylosDir = path.join(path.sep, 'tmp', 'zylos-retired-identities');
    const paths = retiredPm2ServicePaths(zylosDir);
    expect(Object.isFrozen(RETIRED_PM2_SERVICE_DESCRIPTORS)).toBe(true);
    expect(RETIRED_PM2_SERVICE_NAMES).toEqual(
      RETIRED_PM2_SERVICE_DESCRIPTORS.map(({ name }) => name),
    );
    expect([...paths.keys()]).toEqual(RETIRED_PM2_SERVICE_NAMES);
    for (const descriptor of RETIRED_PM2_SERVICE_DESCRIPTORS) {
      expect(Object.isFrozen(descriptor)).toBe(true);
      expect(Object.isFrozen(descriptor.path_segments)).toBe(true);
      expect(paths.get(descriptor.name)).toBe(path.join(zylosDir, ...descriptor.path_segments));
    }
  });
});
