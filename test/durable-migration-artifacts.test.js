import fs from 'node:fs';
import path from 'node:path';

import { describe, expect, jest, test } from '@jest/globals';

import { atomicJson } from '../runtime/migration/installed-executor-upgrade.js';

describe('durable migration artifacts', () => {
  test('fsyncs file and directory before a plan is treated as reopenable', () => {
    const root = fs.mkdtempSync('/tmp/zylos-durable-upgrade-plan-');
    const file = path.join(root, 'plans', 'upgrade-fixture.json');
    const fsync = jest.spyOn(fs, 'fsyncSync');
    try {
      atomicJson(file, { schema_version: 1, upgrade_id: 'upgrade-fixture' });
      expect(fsync).toHaveBeenCalledTimes(2);
      const descriptor = fs.openSync(file, 'r');
      try {
        expect(JSON.parse(fs.readFileSync(descriptor, 'utf8'))).toEqual({
          schema_version: 1, upgrade_id: 'upgrade-fixture',
        });
      } finally {
        fs.closeSync(descriptor);
      }
      expect(fs.readdirSync(path.dirname(file)).filter((entry) => entry.endsWith('.partial')))
        .toEqual([]);
    } finally {
      fsync.mockRestore();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });
});
