import fs from 'node:fs';
import path from 'node:path';
import { describe, expect, test } from '@jest/globals';

import {
  FIVE_REPO_RUNTIME_MATRIX,
  REQUIRED_SCAN_SURFACES,
  auditRepositorySnapshot,
  buildFiveRepoRetiredRuntimePlan,
  scanRepositoryRetiredRuntime,
  validateRetiredRuntimeAllowlist,
} from '../scripts/lib/five-repo-retired-runtime-gate.js';

const retiredMultiplexer = ['t', 'mux'].join('');

function sourceMap(entries) {
  return new Map(entries.map(([file, source]) => [file, source]));
}

describe('five-repository retired-runtime release gate', () => {
  test('declares all five repositories and every required product surface', () => {
    expect(FIVE_REPO_RUNTIME_MATRIX.map(({ repository }) => repository)).toEqual([
      'zylos-core',
      'zylos-feishu',
      'zylos-lark',
      'zylos-dashboard',
      'luna-pet',
    ]);
    expect(REQUIRED_SCAN_SURFACES).toEqual([
      'runtime_code',
      'services',
      'config',
      'cli',
      'installer_upgrade_doctor_self_heal_uninstall',
      'docs_help_errors',
      'tests_fixtures',
      'observability_consumers',
      'package_install_contents',
    ]);
  });

  test('resolves every consumer repository explicitly and never infers a latest checkout', () => {
    const plan = buildFiveRepoRetiredRuntimePlan({
      coreDirectory: '/work/core',
      workspaceDirectory: '/work',
      environment: {
        ZYLOS_FEISHU_RETIRED_RUNTIME_REPO: '/candidate/feishu',
        ZYLOS_LARK_RETIRED_RUNTIME_REPO: '/candidate/lark',
        ZYLOS_DASHBOARD_RETIRED_RUNTIME_REPO: '/candidate/dashboard',
        ZYLOS_LUNA_RETIRED_RUNTIME_REPO: '/candidate/luna',
      },
      allowlist: [],
    });
    expect(plan.map(({ repository, directory }) => [repository, directory])).toEqual([
      ['zylos-core', '/work/core'],
      ['zylos-feishu', '/candidate/feishu'],
      ['zylos-lark', '/candidate/lark'],
      ['zylos-dashboard', '/candidate/dashboard'],
      ['luna-pet', '/candidate/luna'],
    ]);
  });

  test('fails when a prohibited route exists in tracked source or only in package contents', () => {
    const tracked = auditRepositorySnapshot({
      repository: 'zylos-dashboard',
      trackedFiles: ['src/index.js'],
      packagedFiles: ['src/index.js'],
      sources: sourceMap([['src/index.js', `export const transport = '${retiredMultiplexer}';`]]),
      allowlist: [],
    });
    expect(tracked.passed).toBe(false);
    expect(tracked.violations).toEqual([
      expect.objectContaining({ file: 'src/index.js', scope: 'tracked' }),
      expect.objectContaining({ file: 'src/index.js', scope: 'package' }),
    ]);

    const packageOnly = auditRepositorySnapshot({
      repository: 'zylos-feishu',
      trackedFiles: [],
      packagedFiles: ['generated/install-runtime.js'],
      sources: sourceMap([
        ['generated/install-runtime.js', `export const fallback = '${retiredMultiplexer}';`],
      ]),
      allowlist: [],
    });
    expect(packageOnly.passed).toBe(false);
    expect(packageOnly.violations).toEqual([
      expect.objectContaining({ file: 'generated/install-runtime.js', scope: 'package' }),
    ]);
  });

  test.each([
    ['feature flag', 'export const useLegacy = process.env.USE_LEGACY_EXECUTOR;'],
    ['fallback', 'return primary() || legacyExecutorFallback();'],
    ['dual route', "return mode === 'new' ? appServer() : legacyRuntime();"],
    ['permanent compatibility', "export const compatibility = 'legacy runtime';"],
    ['PID health authority', "return process.pid > 0 ? 'healthy' : 'down';"],
    ['prefixed PID health authority', "return executorPid ? 'healthy' : 'down';"],
    ['snake PID health authority', "return daemon_pid ? 'online' : 'down';"],
    ['process-ID health authority', "return childProcessId ? 'alive' : 'down';"],
    ['prefixed PID-file health authority', "return executorPidFile ? 'healthy' : 'down';"],
    ['snake PID-file health authority', "return daemon_pid_file ? 'online' : 'down';"],
    ['process-ID-file health authority', "return childProcessIdFile ? 'alive' : 'down';"],
    ['bare process-ID health authority', "return processId ? 'healthy' : 'down';"],
    ['bare snake process-ID health authority', "return process_id ? 'online' : 'down';"],
    ['uppercase PID-file health authority', "return PID_FILE ? 'alive' : 'down';"],
    ['lowercase PID-file health authority', "return pidfile ? 'healthy' : 'down';"],
    ['prefixed lowercase PID-file health authority', "return executorPidfile ? 'online' : 'down';"],
    ['global session', "export const authority = 'global session';"],
    ['terminal/window health', "export const authority = 'window health';"],
    ['retired idle flag', "export const option = 'require_idle';"],
  ])('rejects prohibited %s behavior on a normal product path', (_label, source) => {
    const result = auditRepositorySnapshot({
      repository: 'zylos-core',
      trackedFiles: ['runtime/executor/normal.js'],
      packagedFiles: ['runtime/executor/normal.js'],
      sources: sourceMap([['runtime/executor/normal.js', source]]),
      allowlist: [],
    });
    expect(result.passed).toBe(false);
  });

  test.each([
    'Rapid health recovery is supported.',
    'The lipid health panel is available.',
  ])('does not mistake ordinary prose for process-liveness authority: %s', (source) => {
    const result = auditRepositorySnapshot({
      repository: 'zylos-dashboard',
      trackedFiles: ['docs/guide.md'],
      packagedFiles: ['docs/guide.md'],
      sources: sourceMap([['docs/guide.md', source]]),
      allowlist: [],
    });
    expect(result.passed).toBe(true);
  });

  test.each([
    'globalSession',
    'windowHealth',
    'inputState',
    'requireIdle',
    'activityMonitor',
    'amHeartbeat',
    'sendKeys',
    'capturePane',
    'pasteBuffer',
    'terminalInjection',
  ])('rejects JavaScript identifier spelling %s', (identifier) => {
    const result = auditRepositorySnapshot({
      repository: 'zylos-dashboard',
      trackedFiles: ['src/runtime.js'],
      packagedFiles: ['src/runtime.js'],
      sources: sourceMap([['src/runtime.js', `export const ${identifier} = true;`]]),
      allowlist: [],
    });
    expect(result.passed).toBe(false);
  });

  test('allows only exact isolated cleanup or negative-proof entries with stated purpose', () => {
    const allowlist = [{
      repository: 'zylos-core',
      file: 'runtime/migration/retired-artifacts.js',
      rules: ['retired_terminal_multiplexer'],
      kind: 'one_time_cleanup',
      purpose: 'Delete exact retired installation artifacts after the atomic upgrade commits.',
    }];
    expect(validateRetiredRuntimeAllowlist({
      repository: 'zylos-core',
      trackedFiles: ['runtime/migration/retired-artifacts.js'],
      packagedFiles: ['runtime/migration/retired-artifacts.js'],
      sources: sourceMap([
        ['runtime/migration/retired-artifacts.js', `export const retired = '${retiredMultiplexer}';`],
      ]),
      allowlist,
    })).toEqual([]);

    const allowed = auditRepositorySnapshot({
      repository: 'zylos-core',
      trackedFiles: ['runtime/migration/retired-artifacts.js'],
      packagedFiles: ['runtime/migration/retired-artifacts.js'],
      sources: sourceMap([
        ['runtime/migration/retired-artifacts.js', `export const retired = '${retiredMultiplexer}';`],
      ]),
      allowlist,
    });
    expect(allowed.passed).toBe(true);
    expect(allowed.allowlisted).toEqual([
      expect.objectContaining({
        file: 'runtime/migration/retired-artifacts.js',
        purpose: expect.stringContaining('atomic upgrade'),
      }),
    ]);
  });

  test('allows an exact isolated migration identity record but no normal runtime data file', () => {
    const migrationRecord = {
      repository: 'zylos-core',
      file: 'runtime/migration/retired-pm2-identities.js',
      rules: ['retired_activity_authority'],
      kind: 'migration_record',
      purpose: 'Fence exact retired supervisor registrations without dispatching or executing them.',
    };
    expect(validateRetiredRuntimeAllowlist({
      repository: 'zylos-core',
      trackedFiles: [migrationRecord.file],
      packagedFiles: [migrationRecord.file],
      sources: sourceMap([[migrationRecord.file, "export const name = 'activity-monitor';"]]),
      allowlist: [migrationRecord],
    })).toEqual([]);

    expect(validateRetiredRuntimeAllowlist({
      repository: 'zylos-core',
      trackedFiles: ['runtime/retired-identities.js'],
      packagedFiles: ['runtime/retired-identities.js'],
      sources: sourceMap([['runtime/retired-identities.js', "export const name = 'activity-monitor';"]]),
      allowlist: [{ ...migrationRecord, file: 'runtime/retired-identities.js' }],
    })).not.toEqual([]);
  });

  test('rejects an orphan allowlist repository instead of silently dropping it', () => {
    expect(() => buildFiveRepoRetiredRuntimePlan({
      coreDirectory: '/work/core',
      workspaceDirectory: '/work',
      environment: {},
      allowlist: [{
        repository: 'zylos-dashbord',
        file: 'test/proof.js',
        rules: ['retired_terminal_multiplexer'],
        kind: 'negative_proof',
        purpose: 'Typo must fail the release gate explicitly.',
      }],
    })).toThrow(/unknown allowlist repository/);
  });

  test.each([
    ['normal runtime location', {
      repository: 'zylos-core', file: 'runtime/executor/normal.js',
      rules: ['retired_terminal_multiplexer'], kind: 'one_time_cleanup', purpose: 'cleanup',
    }],
    ['missing purpose', {
      repository: 'zylos-core', file: 'runtime/migration/retired-artifacts.js',
      rules: ['retired_terminal_multiplexer'], kind: 'one_time_cleanup', purpose: '',
    }],
    ['stale rule', {
      repository: 'zylos-core', file: 'runtime/migration/retired-artifacts.js',
      rules: ['provider_session_scope'], kind: 'one_time_cleanup', purpose: 'cleanup',
    }],
  ])('rejects a non-minimal allowlist entry: %s', (_label, entry) => {
    const errors = validateRetiredRuntimeAllowlist({
      repository: 'zylos-core',
      trackedFiles: [entry.file],
      packagedFiles: [entry.file],
      sources: sourceMap([[entry.file, `export const retired = '${retiredMultiplexer}';`]]),
      allowlist: [entry],
    });
    expect(errors).not.toEqual([]);
  });

  test.each([
    ['import', "import { retired } from '../migration/retired-artifacts.js';"],
    ['dispatch', "dispatch('../migration/retired-artifacts.js');"],
    ['execute', "spawn('node', ['../migration/retired-artifacts.js']);"],
    ['select', "const selected = '../migration/retired-artifacts.js'; select(selected);"],
    ['template select', 'const selected = `../migration/retired-artifacts.js`; select(selected);'],
    ['synchronous spawn', "spawnSync('node', ['../migration/retired-artifacts.js']);"],
    ['synchronous execute', "execFileSync('node', ['../migration/retired-artifacts.js']);"],
  ])('an actual allowlisted module cannot be used by undeclared normal %s', (_operation, normalSource) => {
      const cleanupFile = 'runtime/migration/retired-artifacts.js';
      const normalFile = 'runtime/executor/dispatch.js';
      const result = auditRepositorySnapshot({
        repository: 'zylos-core',
        trackedFiles: [cleanupFile, normalFile],
        packagedFiles: [cleanupFile, normalFile],
        sources: sourceMap([
          [cleanupFile, `export const retired = '${retiredMultiplexer}';`],
          [normalFile, normalSource],
        ]),
        allowlist: [{
          repository: 'zylos-core',
          file: cleanupFile,
          rules: ['retired_terminal_multiplexer'],
          kind: 'one_time_cleanup',
          purpose: 'Delete exact retired artifacts after the atomic upgrade commits.',
        }],
      });
      expect(result.passed).toBe(false);
      expect(result.violations).toEqual(expect.arrayContaining([
        expect.objectContaining({ file: normalFile }),
      ]));
    });

  test.each([
    ['direct relative', 'node ../runtime/migration/retired-artifacts.js'],
    ['redundant relative', 'node ./../runtime/migration/retired-artifacts.js'],
    ['installed absolute', 'node /opt/zylos/runtime/migration/retired-artifacts.js'],
  ])('an actual allowlisted module cannot be executed by a normal shell script: %s', (_label, command) => {
    const cleanupFile = 'runtime/migration/retired-artifacts.js';
    const normalFile = 'scripts/start.sh';
    const result = auditRepositorySnapshot({
      repository: 'zylos-core',
      trackedFiles: [cleanupFile, normalFile],
      packagedFiles: [cleanupFile, normalFile],
      sources: sourceMap([
        [cleanupFile, `export const retired = '${retiredMultiplexer}';`],
        [normalFile, `#!/bin/sh\n${command}`],
      ]),
      allowlist: [{
        repository: 'zylos-core',
        file: cleanupFile,
        rules: ['retired_terminal_multiplexer'],
        kind: 'one_time_cleanup',
        purpose: 'Delete a retired artifact once.',
        allowed_importers: [],
      }],
    });
    expect(result.passed).toBe(false);
    expect(result.violations).toEqual(expect.arrayContaining([
      expect.objectContaining({ file: normalFile, scope: 'reachability' }),
    ]));
  });

  test.each([
    ['config/runtime.json', '{"executor":"runtime/migration/retired-artifacts.js"}'],
    ['config/runtime.yaml', 'executor: runtime/migration/retired-artifacts.js'],
    ['config/runtime.toml', 'executor = "runtime/migration/retired-artifacts.js"'],
    ['config/runtime.env', 'EXECUTOR=runtime/migration/retired-artifacts.js'],
    ['config/absolute-runtime.json', '{"executor":"/opt/zylos/runtime/migration/retired-artifacts.js"}'],
  ])('an actual allowlisted module cannot be selected by normal config %s', (normalFile, normalSource) => {
    const cleanupFile = 'runtime/migration/retired-artifacts.js';
    const result = auditRepositorySnapshot({
      repository: 'zylos-core',
      trackedFiles: [cleanupFile, normalFile],
      packagedFiles: [cleanupFile, normalFile],
      sources: sourceMap([
        [cleanupFile, `export const retired = '${retiredMultiplexer}';`],
        [normalFile, normalSource],
      ]),
      allowlist: [{
        repository: 'zylos-core',
        file: cleanupFile,
        rules: ['retired_terminal_multiplexer'],
        kind: 'one_time_cleanup',
        purpose: 'Delete a retired artifact once.',
        allowed_importers: [],
      }],
    });
    expect(result.passed).toBe(false);
    expect(result.violations).toEqual(expect.arrayContaining([
      expect.objectContaining({ file: normalFile, scope: 'reachability' }),
    ]));
  });

  test('scanner rejects dirty trees, scans extensionless package text, and rejects symlinks', () => {
    const directory = fs.mkdtempSync(path.join(fs.realpathSync('/tmp'), 'five-repo-gate-'));
    const outside = path.join(path.dirname(directory), `${path.basename(directory)}-outside.js`);
    fs.mkdirSync(path.join(directory, 'bin'));
    fs.mkdirSync(path.join(directory, 'src'));
    fs.writeFileSync(path.join(directory, 'bin', 'runtime'), `exec ${retiredMultiplexer}\n`);
    fs.writeFileSync(outside, 'export const outside = true;\n');
    fs.symlinkSync(outside, path.join(directory, 'src', 'linked.js'));

    const item = { repository: 'zylos-core', directory, allowlist: [] };
    const spawnWithStatus = (statusOutput = '') => (_command, args) => {
      if (args[0] === 'status') return { status: 0, stdout: statusOutput, stderr: '' };
      if (args[0] === 'ls-files') return { status: 0, stdout: 'bin/runtime\0src/linked.js\0', stderr: '' };
      if (args[0] === 'pack') return {
        status: 0,
        stdout: JSON.stringify([{ files: [{ path: 'bin/runtime' }, { path: 'src/linked.js' }] }]),
        stderr: '',
      };
      if (args[0] === 'rev-parse') return { status: 0, stdout: `${'a'.repeat(40)}\n`, stderr: '' };
      throw new Error(`unexpected command: ${args.join(' ')}`);
    };

    try {
      expect(() => scanRepositoryRetiredRuntime(item, { spawn: spawnWithStatus(' M bin/runtime\0') }))
        .toThrow(/not clean/);
      expect(() => scanRepositoryRetiredRuntime(item, { spawn: spawnWithStatus() }))
        .toThrow(/symbolic link/);
      fs.unlinkSync(path.join(directory, 'src', 'linked.js'));
      fs.writeFileSync(path.join(directory, 'src', 'linked.js'), 'export const clean = true;\n');
      expect(scanRepositoryRetiredRuntime(item, { spawn: spawnWithStatus() }).passed).toBe(false);
    } finally {
      fs.rmSync(directory, { recursive: true, force: true });
      fs.rmSync(outside, { force: true });
    }
  });
});
