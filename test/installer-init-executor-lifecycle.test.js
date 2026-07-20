import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { describe, expect, test } from '@jest/globals';

import { requireHealthyExecutorStart } from '../cli/commands/init.js';
import { desiredClaudeHooks } from '../cli/lib/sync-settings-hooks.js';
import { cleanupRetiredRuntimeSkillArtifacts } from '../runtime/migration/legacy-lifecycle-artifacts.js';

describe('installer and init executor lifecycle', () => {
  test('init accepts only healthy Core service identity as startup success', () => {
    expect(requireHealthyExecutorStart({
      ok: true,
      serviceInstanceId: 'executor-init-fixture',
    })).toMatchObject({ ok: true, serviceInstanceId: 'executor-init-fixture' });
    expect(() => requireHealthyExecutorStart({
      ok: false,
      error: 'executor_offline',
    })).toThrow('executor_offline');
    expect(() => requireHealthyExecutorStart({ ok: true }))
      .toThrow('authoritative identity');
  });

  test('installer propagates init failure and has no tmux prerequisite', () => {
    const installer = fs.readFileSync(new URL('../scripts/install.sh', import.meta.url), 'utf8');
    expect(installer).toContain('return "$init_exit"');
    expect(installer).not.toMatch(/\btmux\b/i);
  });

  test('fresh executor settings never select the retired activity monitor', () => {
    const hooks = desiredClaudeHooks({
      zylosDir: '/tmp/zylos-executor-hook-fixture',
      existsSync: () => true,
    });
    const template = JSON.parse(fs.readFileSync(
      new URL('../templates/.claude/settings.json', import.meta.url),
      'utf8',
    ));

    expect(JSON.stringify(hooks)).not.toContain('activity-monitor');
    expect(JSON.stringify(template)).not.toContain('activity-monitor');
  });

  test('fresh skill deployment removes the retired dispatcher script from an isolated installation', () => {
    const root = fs.mkdtempSync(path.join(fs.realpathSync('/tmp'), 'zylos-init-retired-script-'));
    const skillsDir = path.join(root, '.claude', 'skills');
    const retired = path.join(skillsDir, 'comm-bridge', 'scripts', 'c4-dispatcher.js');
    fs.mkdirSync(path.dirname(retired), { recursive: true });
    fs.writeFileSync(retired, 'must not ship');
    try {
      expect(cleanupRetiredRuntimeSkillArtifacts({ skillsDir })).toEqual({ removed: [retired] });
      expect(fs.existsSync(retired)).toBe(false);
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  test('init checks legacy ownership using the selected isolated ZYLOS_DIR', () => {
    const initSource = fs.readFileSync(new URL('../cli/commands/init.js', import.meta.url), 'utf8');
    expect(initSource).toContain('assertLegacyServicesInactive({ zylosDir: ZYLOS_DIR })');
  });
});
