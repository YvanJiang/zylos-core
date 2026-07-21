import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { afterEach, describe, expect, test } from '@jest/globals';

import {
  assertExecutorStartFence,
  issueExecutorStartFence,
} from '../runtime/executor/start-fence.js';
import { freshInstallStartFenceProof } from '../cli/commands/init.js';

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
  test('requires a clean PM2 inventory before issuing fresh-install authority', () => {
    const zylosDir = fixture();
    expect(() => issueExecutorStartFence({
      zylosDir,
      proof: { kind: 'fresh_clean', installation_root_absent: false },
    })).toThrow('fresh-clean proof');

    const proof = freshInstallStartFenceProof({
      installationRootAbsentAtStart: true, installState: 'fresh',
    });
    const expectedActivityMonitor = path.join(
      zylosDir, '.claude', 'skills', 'activity-monitor', 'scripts', 'activity-monitor.js',
    );
    for (const inventory of [
      [{ name: 'activity-monitor', pm2_env: { status: 'stopped', pm_exec_path: expectedActivityMonitor } }],
      [{ name: 'activity-monitor', pm2_env: { status: 'online', pm_exec_path: '/opt/user/activity-monitor.js' } }],
      [{ name: 'activity-monitor', pm2_env: { status: 'online' } }],
    ]) {
      expect(() => issueExecutorStartFence({
        zylosDir,
        proof,
        execFileSyncFn: () => JSON.stringify(inventory),
      })).toThrow(/PM2 (registration|inventory)/);
      expect(fs.existsSync(path.join(zylosDir, 'runtime', 'executor-start-fence.json'))).toBe(false);
    }
    expect(() => issueExecutorStartFence({
      zylosDir,
      proof,
      execFileSyncFn: () => { throw new Error('pm2 unavailable'); },
    })).toThrow('Fresh executor start requires an authoritative PM2 inventory');
    expect(() => issueExecutorStartFence({
      zylosDir,
      proof,
      execFileSyncFn: () => '{not json',
    })).toThrow('Fresh executor start requires an authoritative PM2 inventory');

    const issued = issueExecutorStartFence({
      zylosDir,
      proof,
      execFileSyncFn: () => '[]',
      now: () => '2026-07-21T00:00:00.000Z',
    });
    expect(assertExecutorStartFence({ zylosDir })).toMatchObject({
      issuance_kind: 'fresh_clean',
      installation_root_absent: true,
      legacy_pm2_registrations_absent: true,
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
