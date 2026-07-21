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

function repositoryFiles() {
  return execFileSync('git', ['ls-files', '-z'], {
    cwd: path.resolve('.'), encoding: 'utf8',
  }).split('\0').filter(Boolean).filter((file) => fs.existsSync(path.resolve(file)));
}

function isScannableText(file) {
  return /\.(?:js|cjs|mjs|md|json|ya?ml|sh|env|sql)$/.test(file)
    || ['Dockerfile', '.npmignore'].includes(path.basename(file))
    || file.startsWith('test/integration/runtime/bin/');
}

const migrationOnlyRepositoryFiles = new Set([
  // Historical record: never imported, executed, packaged as an entrypoint, or dispatched.
  'CHANGELOG.md',
  // Executable negative/migration proofs. These files are tests only; the
  // repository scan still covers every other tracked product, source, doc,
  // fixture, package, and test file.
  'cli/lib/__tests__/codex-hooks.test.js',
  'cli/lib/__tests__/self-upgrade.test.js',
  'cli/lib/__tests__/sync-settings-hooks.test.js',
  'scripts/bootstrap-executor-lifecycle.js',
  'scripts/installed-runtime-inventory.js',
  'test/base-to-executor-bootstrap.test.js',
  'test/exact-base-legacy-source.test.js',
  'test/executor-service-lifecycle.test.js',
  'test/installed-executor-upgrade.test.js',
  'test/installer-init-executor-lifecycle.test.js',
  'test/runtime-atomic-upgrade.test.js',
  'test/runtime-c4-normal-callers.test.js',
  'test/runtime-c4-source-boundary.test.js',
  'test/runtime-normal-path-static.test.js',
  'test/runtime-provider-neutral-health.test.js',
  'test/runtime-upgrade-coordinator.test.js',
  'runtime/migration/installed-executor-upgrade.js',
  'runtime/migration/legacy-lifecycle-artifacts.js',
  'runtime/migration/legacy-provider-quiescence.js',
]);

describe('normal product paths have no retired runtime authority', () => {
  test('the tracked repository contains retired identifiers only in isolated migration code or proofs', () => {
    const banned = /tmux|capture-pane|send-keys|paste-buffer|agent-status\.json|global[ _-]session|terminal injection|activity-monitor|c4-dispatcher|c4-control|c4-session-init/i;
    const violations = repositoryFiles()
      .filter(isScannableText)
      .filter((file) => !migrationOnlyRepositoryFiles.has(file))
      .filter((file) => banned.test(fs.readFileSync(path.resolve(file), 'utf8')));
    expect(violations).toEqual([]);
  });

  test('normal callers contain no terminal or host-file execution authority', () => {
    for (const file of normalRuntimeFiles) {
      const source = fs.readFileSync(path.resolve(file), 'utf8');
      expect(source).not.toMatch(
        /tmux|capture-pane|send-keys|paste-buffer|agent-status\.json|global session|runtime is alive/i,
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

  test('every local import selected by the exact Node suite resolves to a tracked source', () => {
    const roots = [
      'cli/lib/__tests__',
      'skills/scheduler/scripts/__tests__',
    ];
    const tracked = new Set(repositoryFiles());
    const missing = [];
    const testFiles = [];
    for (const root of roots) {
      const pending = [root];
      while (pending.length > 0) {
        const directory = pending.pop();
        for (const entry of fs.readdirSync(path.resolve(directory), { withFileTypes: true })) {
          const file = path.join(directory, entry.name);
          if (entry.isDirectory()) pending.push(file);
          else if (entry.isFile() && entry.name.endsWith('.test.js')) testFiles.push(file);
        }
      }
    }
    for (const file of testFiles) {
      const source = fs.readFileSync(path.resolve(file), 'utf8');
      const imports = source.matchAll(/(?:from\s+|import\s*\()(['"])(\.{1,2}\/[^'"]+)\1/g);
      for (const [, , specifier] of imports) {
        const resolved = path.resolve(path.dirname(file), specifier);
        const relative = path.relative(path.resolve('.'), resolved).split(path.sep).join('/');
        if (!fs.existsSync(resolved) || !tracked.has(relative)) {
          missing.push(`${file} -> ${specifier}`);
        }
      }
    }
    expect(missing).toEqual([]);
  });

  test('retired executable implementations are absent from both repository and package payload', () => {
    const tracked = new Set(repositoryFiles());
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
      'cli/lib/__tests__/claude-auth.test.js',
      'cli/lib/__tests__/codex.test.js',
      'runtime/migration/legacy-c4-runtime-config.js',
      'runtime/migration/legacy-c4-diagnostic.js',
    ]) {
      expect([...tracked].some((file) => file === retired || file.startsWith(retired))).toBe(false);
      expect(files.some((file) => file === retired || file.startsWith(retired))).toBe(false);
    }
  });

  test('every reachable packaged caller is free of retired runtime authority', () => {
    const migrationOnly = new Set([
      'scripts/bootstrap-executor-lifecycle.js',
      'scripts/installed-runtime-inventory.js',
      'runtime/migration/installed-executor-upgrade.js',
      'runtime/migration/legacy-lifecycle-artifacts.js',
      'runtime/migration/legacy-provider-quiescence.js',
    ]);
    const banned = /tmux|capture-pane|send-keys|paste-buffer|global[ _-]session|terminal injection|agent-status\.json|input health|window health/i;
    const violations = packedFiles()
      .filter(isScannableText)
      .filter((file) => !migrationOnly.has(file))
      .filter((file) => banned.test(fs.readFileSync(path.resolve(file), 'utf8')));
    expect(violations).toEqual([]);
  });
});
