import fs from 'node:fs';

import { describe, expect, test } from '@jest/globals';

import { requireHealthyExecutorStart } from '../cli/commands/init.js';
import { desiredClaudeHooks } from '../cli/lib/sync-settings-hooks.js';

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
});
