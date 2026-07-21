import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { afterEach, describe, expect, test } from '@jest/globals';

import {
  assertExecutorStartFence,
  issueExecutorStartFence,
} from '../runtime/executor/start-fence.js';

const directories = [];

function fixture() {
  const directory = fs.mkdtempSync(path.join(fs.realpathSync(os.tmpdir()), 'zylos-start-fence-'));
  directories.push(directory);
  return directory;
}

afterEach(() => {
  for (const directory of directories.splice(0)) {
    fs.rmSync(directory, { recursive: true, force: true });
  }
});

describe('executor start fence issuance', () => {
  test('rejects ambiguous fresh-install proof and records only explicit clean issuance', () => {
    const zylosDir = fixture();
    expect(() => issueExecutorStartFence({
      zylosDir,
      proof: { kind: 'fresh_clean', installation_root_absent: false },
    })).toThrow('fresh-clean proof');

    const issued = issueExecutorStartFence({
      zylosDir,
      proof: { kind: 'fresh_clean', installation_root_absent: true },
      now: () => '2026-07-21T00:00:00.000Z',
    });
    expect(assertExecutorStartFence({ zylosDir })).toMatchObject({
      issuance_kind: 'fresh_clean',
      installation_root_absent: true,
    });
    expect(issued.path).toContain('executor-start-fence.json');
  });

  test('requires all committed reconciliation facts before issuing migration authority', () => {
    const zylosDir = fixture();
    expect(() => issueExecutorStartFence({
      zylosDir,
      proof: {
        kind: 'committed_reconciliation', upgrade_id: 'upgrade-fixture',
        legacy_services_quiesced: true, legacy_registrations_absent: true,
        legacy_artifacts_reconciled: false,
      },
    })).toThrow('committed reconciliation proof');
  });
});
