import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { execFileSync } from 'node:child_process';

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

  test('the bundled restart skill uses authoritative executor lifecycle control', () => {
    const restartSkill = fs.readFileSync(
      new URL('../skills/restart-claude/SKILL.md', import.meta.url),
      'utf8',
    );
    expect(restartSkill).toContain('zylos restart');
    expect(restartSkill).toContain('zylos status');
    expect(restartSkill).not.toMatch(/tmux|activity-monitor|c4-control/i);
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
    expect(initSource).toContain('reconcileLegacyServicesForExecutorStart({ zylosDir: ZYLOS_DIR })');
  });

  test('the shipped package excludes retired executable runtime implementations', () => {
    const packed = JSON.parse(execFileSync('npm', [
      'pack', '--dry-run', '--json', '--ignore-scripts',
    ], { cwd: path.resolve('.'), encoding: 'utf8', timeout: 30_000 }));
    const files = packed[0].files.map(({ path: file }) => file);
    for (const retired of [
      'skills/activity-monitor/',
      'skills/comm-bridge/scripts/c4-dispatcher.js',
      'skills/comm-bridge/scripts/tmux-input-state.js',
      'cli/lib/runtime/claude.js',
      'cli/lib/runtime/codex.js',
      'cli/lib/runtime/tmux-helpers.js',
    ]) {
      expect(files.some((file) => file === retired || file.startsWith(retired))).toBe(false);
    }
    expect(files.some((file) => file.includes('/node_modules/'))).toBe(false);
    expect(files.some((file) => file.endsWith('.node'))).toBe(false);
    for (const packageManifest of [
      'skills/comm-bridge/package.json', 'skills/comm-bridge/package-lock.json',
      'skills/scheduler/package.json', 'skills/scheduler/package-lock.json',
      'skills/web-console/package.json', 'skills/web-console/package-lock.json',
    ]) expect(files).toContain(packageManifest);
  });

  test('selectable lifecycle skills cannot enqueue retired session commands', () => {
    for (const relative of [
      '../skills/new-session/SKILL.md',
      '../skills/upgrade-claude/SKILL.md',
      '../skills/upgrade-claude/scripts/upgrade.js',
    ]) {
      const source = fs.readFileSync(new URL(relative, import.meta.url), 'utf8');
      expect(source).not.toMatch(/c4-control|activity-monitor|tmux|send-keys|--content['"]?,?\s*['"]\/(?:exit|clear)/i);
    }
  });
});
