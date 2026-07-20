import fs from 'node:fs';
import path from 'node:path';
import { execFileSync } from 'node:child_process';

import { describe, expect, test } from '@jest/globals';

const normalRuntimeFiles = [
  'skills/comm-bridge/scripts/c4-receive.js',
  'skills/scheduler/scripts/runtime.js',
  'skills/scheduler/scripts/daemon.js',
  'skills/scheduler/scripts/daemon-tasks.js',
  'skills/web-console/scripts/server.js',
  'skills/web-console/scripts/send.js',
  'skills/health-check/SKILL.md',
  'runtime/observability/executor-snapshot-client.js',
  'runtime/observability/health-projection.js',
  'runtime/scheduler/scheduler-observability.js',
  'templates/claude-system.md',
  'templates/codex-system.md',
  'templates/onboarding.md',
  'docs/hook-activity-tracking.md',
  'README.md',
];

describe('normal product paths have no retired runtime authority', () => {
  test('normal callers contain no terminal or host-file execution authority', () => {
    for (const file of normalRuntimeFiles) {
      const source = fs.readFileSync(path.resolve(file), 'utf8');
      expect(source).not.toMatch(
        /\btmux\b|capture-pane|send-keys|paste-buffer|agent-status\.json|global session|runtime is alive/i,
      );
    }
  });

  test('system instructions never ask a model to select or execute a delivery route', () => {
    for (const file of [
      'templates/claude-system.md', 'templates/codex-system.md', 'templates/onboarding.md',
    ]) {
      const source = fs.readFileSync(path.resolve(file), 'utf8');
      expect(source).not.toMatch(/reply via|c4-send\.js|latest message|parent chat fallback/i);
      expect(source).toMatch(/durable (outbox|delivery)/i);
    }
  });

  test('the exact Node suite cannot execute retired provider-interface tests', () => {
    const runner = fs.readFileSync(path.resolve('scripts/run-node-tests.js'), 'utf8');
    expect(runner).toContain("'skills', 'scheduler', 'scripts', '__tests__'");
    expect(runner).not.toMatch(/activity-monitor|cli', 'lib', 'runtime', '__tests__/);
  });

  test('the package payload excludes every repository-only executable legacy path', () => {
    const packed = JSON.parse(execFileSync('npm', [
      'pack', '--dry-run', '--json', '--ignore-scripts',
    ], { cwd: path.resolve('.'), encoding: 'utf8', timeout: 30_000 }));
    const files = packed[0].files.map(({ path: file }) => file);
    for (const retired of [
      'skills/activity-monitor/',
      'skills/comm-bridge/scripts/c4-dispatcher.js',
      'skills/comm-bridge/scripts/c4-control.js',
      'skills/comm-bridge/scripts/c4-session-init.js',
      'skills/comm-bridge/scripts/tmux-input-state.js',
      'cli/lib/runtime/claude.js',
      'cli/lib/runtime/codex.js',
      'cli/lib/runtime/tmux-helpers.js',
    ]) {
      expect(files.some((file) => file === retired || file.startsWith(retired))).toBe(false);
    }
  });
});
