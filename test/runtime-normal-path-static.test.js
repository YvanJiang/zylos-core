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
  'skills/web-console/scripts/core-outbox-owner.js',
  'skills/web-console/scripts/db.js',
  'skills/web-console/scripts/send.js',
  'skills/web-console/public/app.js',
  'skills/shell/SKILL.md',
  'skills/shell/scripts/send.js',
  'skills/health-check/SKILL.md',
  'skills/check-context/SKILL.md',
  'skills/restart-claude/SKILL.md',
  'skills/zylos-memory/SKILL.md',
  'cli/commands/init.js',
  'cli/commands/add.js',
  'cli/lib/components.js',
  'runtime/observability/executor-snapshot-client.js',
  'runtime/observability/health-projection.js',
  'runtime/scheduler/scheduler-observability.js',
  'templates/claude-system.md',
  'templates/codex-system.md',
  'templates/onboarding.md',
  'docs/hook-activity-tracking.md',
  'README.md',
  'README.zh-CN.md',
];

let cachedPackedFiles = null;
function packedFiles() {
  if (cachedPackedFiles === null) {
    const packed = JSON.parse(execFileSync('npm', [
      'pack', '--dry-run', '--json', '--ignore-scripts',
    ], { cwd: path.resolve('.'), encoding: 'utf8', timeout: 30_000 }));
    cachedPackedFiles = packed[0].files.map(({ path: file }) => file);
  }
  return cachedPackedFiles;
}

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

  test('shipped multilingual docs do not describe global sessions or idle-gated scheduling', () => {
    for (const file of ['README.md', 'README.zh-CN.md']) {
      const source = fs.readFileSync(path.resolve(file), 'utf8');
      expect(source).not.toMatch(
        /unified (?:gateway|conversation|session)|one conversation(?:,|\s+[—-])|idle gating|统一网关|统一会话|一个对话、一份|空闲门控/i,
      );
    }
  });

  test('component installation has no terminal-observation or ownerless C4 fallback', () => {
    const source = fs.readFileSync(path.resolve('cli/lib/components.js'), 'utf8');
    expect(source).not.toMatch(/outputTask|ZYLOS_TASK|COMPONENT_TASK|zylos-cli|reply_channel|Claude session|c4-receive/i);
    expect(source).toMatch(/operator setup/i);
  });

  test('Web Console routes consume only the monotonic channel mailbox projection', () => {
    const source = fs.readFileSync(path.resolve('skills/web-console/scripts/server.js'), 'utf8');
    const appSource = fs.readFileSync(path.resolve('skills/web-console/public/app.js'), 'utf8');
    expect(source).toMatch(/DeliveryMailbox|deliveryMailbox\.list|syncCoreInbound/);
    expect(source).not.toMatch(/getCoreMessages|event\.rowid|outbox_rowid|broadcast\('messages'/);
    expect(appSource).toMatch(/api\/poll\?since_id=/);
    expect(appSource).not.toMatch(/conversations\/recent\?limit=100/);
  });

  test('the exact Node suite cannot execute retired provider-interface tests', () => {
    const runner = fs.readFileSync(path.resolve('scripts/run-node-tests.js'), 'utf8');
    expect(runner).toContain("'skills', 'scheduler', 'scripts', '__tests__'");
    expect(runner).not.toMatch(/activity-monitor|cli', 'lib', 'runtime', '__tests__/);
  });

  test('the package payload excludes every repository-only executable legacy path', () => {
    const files = packedFiles();
    for (const retired of [
      'skills/activity-monitor/',
      'skills/comm-bridge/scripts/c4-dispatcher.js',
      'skills/comm-bridge/scripts/c4-control.js',
      'skills/comm-bridge/scripts/c4-session-init.js',
      'skills/comm-bridge/scripts/c4-db.js',
      'skills/comm-bridge/scripts/c4-fetch.js',
      'skills/comm-bridge/scripts/c4-checkpoint.js',
      'skills/comm-bridge/scripts/tmux-input-state.js',
      'cli/lib/runtime/claude.js',
      'cli/lib/runtime/codex.js',
      'cli/lib/runtime/tmux-helpers.js',
    ]) {
      expect(files.some((file) => file === retired || file.startsWith(retired))).toBe(false);
    }
  });

  test('every reachable packaged caller is free of retired runtime authority', () => {
    const migrationOnly = new Set([
      'runtime/migration/legacy-c4-diagnostic.js',
      'runtime/migration/legacy-lifecycle-artifacts.js',
      'runtime/migration/legacy-provider-quiescence.js',
      'scripts/installed-runtime-inventory.js',
    ]);
    const banned = /\btmux\b|capture-pane|send-keys|paste-buffer|global[ _-]session|terminal injection|agent-status\.json|input health|window health/i;
    const violations = packedFiles()
      .filter((file) => /\.(?:js|cjs|mjs|md|json|ya?ml|sh)$/.test(file))
      .filter((file) => !migrationOnly.has(file))
      .filter((file) => banned.test(fs.readFileSync(path.resolve(file), 'utf8')));
    expect(violations).toEqual([]);
  });
});
