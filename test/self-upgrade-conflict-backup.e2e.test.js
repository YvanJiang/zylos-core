import { describe, test, expect, beforeEach, afterEach } from '@jest/globals';
import { spawnSync } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { generateManifest, saveMergeBaseline } from '../cli/lib/manifest.js';

const DRIVER = path.join(import.meta.dirname, 'helpers', 'run-self-upgrade-driver.mjs');

let tmpRoot;
let zylosDir;
let skillsDir;
let packageDir;

function writeFile(dir, relPath, content) {
  const full = path.join(dir, relPath);
  fs.mkdirSync(path.dirname(full), { recursive: true });
  fs.writeFileSync(full, content);
}

function readFile(filePath) {
  return fs.readFileSync(filePath, 'utf8');
}

function prepareThreeWayConflictFixture() {
  const baseline = path.join(tmpRoot, 'baseline');
  const installed = path.join(skillsDir, 'demo-skill');
  const incoming = path.join(packageDir, 'skills', 'demo-skill');

  writeFile(baseline, 'SKILL.md', '# Demo\nvalue=baseline\n');
  writeFile(baseline, 'nested/settings.txt', 'mode=baseline\n');
  writeFile(installed, 'SKILL.md', '# Demo\nvalue=local\n');
  writeFile(installed, 'nested/settings.txt', 'mode=local\n');
  saveMergeBaseline(installed, baseline, generateManifest(baseline));
  writeFile(incoming, 'SKILL.md', '# Demo\nvalue=upstream\n');
  writeFile(incoming, 'nested/settings.txt', 'mode=upstream\n');
}

function runScenario(scenario) {
  const child = spawnSync(process.execPath, [DRIVER, packageDir, scenario], {
    encoding: 'utf8',
    env: { ...process.env, ZYLOS_DIR: zylosDir, NO_COLOR: '1' },
    timeout: 60000,
  });
  expect(child.error).toBeUndefined();
  expect(child.status).toBe(0);
  return JSON.parse(child.stdout);
}

beforeEach(() => {
  tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'zylos-self-upgrade-717-'));
  zylosDir = path.join(tmpRoot, 'zylos');
  skillsDir = path.join(zylosDir, '.claude', 'skills');
  packageDir = path.join(tmpRoot, 'new-package');
  fs.mkdirSync(skillsDir, { recursive: true });
  fs.mkdirSync(packageDir, { recursive: true });
});

afterEach(() => {
  fs.rmSync(tmpRoot, { recursive: true, force: true });
});

describe('self-upgrade durable conflict backups (#717)', () => {
  test.each(['success', 'json', 'later-failure', 'no-conflict'])(
    'legacy launcher scenario %s fails closed without install, stop, or backup mutation',
    (scenario) => {
      if (scenario !== 'no-conflict') prepareThreeWayConflictFixture();
      else writeFile(packageDir, 'skills/new-skill/SKILL.md', '# New\n');

      const { result, launcherOutput, npmCommands, stoppedServices, transactionBackupDir }
        = runScenario(scenario);

      expect(result).toMatchObject({
        success: false,
        failedStep: 0,
        durableRuntimeOwner: true,
        error: 'Legacy self-upgrade is disabled because the durable runtime owns upgrades.',
        steps: [],
        rollback: { performed: false, steps: [] },
      });
      expect(launcherOutput).toEqual([]);
      expect(npmCommands).toEqual([]);
      expect(stoppedServices).toEqual([]);
      expect(fs.existsSync(transactionBackupDir)).toBe(false);
      expect(fs.existsSync(path.join(zylosDir, '.backup'))).toBe(false);
    },
  );
});
