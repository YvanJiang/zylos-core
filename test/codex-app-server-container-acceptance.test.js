import { describe, expect, test } from '@jest/globals';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import {
  buildCodexAppServerContainerAcceptancePlan,
  redactCodexAppServerContainerEvidence,
} from '../scripts/lib/codex-app-server-container-acceptance.js';
import {
  runCodexAppServerInitializeSmoke,
  runCodexAppServerRealInteractionProbe,
  runCodexAppServerRestartResume,
  selectRestartResumeThreadId,
  writeCodexAppServerRealEvidenceFile,
} from '../scripts/lib/codex-app-server-real-integration.js';

describe('Codex app-server Apple Container acceptance', () => {
  test('builds a credential-free official Codex image whose default is stdio app-server', () => {
    const containerfile = fs.readFileSync(new URL(
      '../Containerfile.codex-app-server-acceptance',
      import.meta.url,
    ), 'utf8');

    expect(containerfile).toMatch(/^FROM node:22-bookworm-slim/m);
    expect(containerfile).toContain('ARG CODEX_VERSION=0.144.5');
    expect(containerfile).toContain('npm install --global "@openai/codex@${CODEX_VERSION}"');
    expect(containerfile).toContain('codex-cli ${CODEX_VERSION}');
    expect(containerfile).toContain('org.opencontainers.image.revision="${CANDIDATE_SHA}"');
    expect(containerfile).toMatch(/apt-get install[\s\S]*?bubblewrap/);
    expect(containerfile).toContain('node scripts/install-skill-deps.js');
    expect(containerfile).toContain('CMD ["codex", "app-server", "--stdio"]');
    expect(containerfile).not.toMatch(/ARG .*?(?:TOKEN|KEY|SECRET)|COPY .*?\.codex/i);
    expect(containerfile).not.toMatch(/codex exec|exec resume|--publish|--publish-socket/i);
  });

  test('pins the official Linux arm64 stdio target and a dedicated persistent state volume', () => {
    const plan = buildCodexAppServerContainerAcceptancePlan({
      candidateSha: '0123456789abcdef0123456789abcdef01234567',
      worktreePath: '/workspace/zylos-core-issue-43',
      imageTag: 'zylos-global43-codex-app-server:0.144.5-candidate',
      volumeName: 'zylos-global43-codex-home-0-144-5',
    });

    expect(plan).toMatchObject({
      evidence_schema_version: 1,
      target: {
        package: '@openai/codex',
        cli_version: '0.144.5',
        platform: 'linux/arm64',
        transport: 'stdio',
        app_server_argv: ['codex', 'app-server', '--stdio'],
      },
      isolation: {
        codex_home: '/acceptance/codex-home',
        state_volume: 'zylos-global43-codex-home-0-144-5',
        host_codex_home_reused: false,
      },
    });
    expect(plan.commands.build).toEqual(expect.arrayContaining([
      'container',
      'build',
      '--platform',
      'linux/arm64',
    ]));
    expect(plan.commands.acceptance).toEqual(expect.arrayContaining([
      'container',
      'run',
      '--interactive',
      '--init',
      '--platform',
      'linux/arm64',
      '--mount',
      'type=volume,source=zylos-global43-codex-home-0-144-5,target=/acceptance/codex-home',
      '--env',
      'ZYLOS_CODEX_APP_SERVER_EVIDENCE_FILE=/acceptance/codex-home/global43/real-evidence.json',
    ]));
    expect(plan.commands.restart_resume).toEqual(expect.arrayContaining([
      'container',
      'run',
      '--interactive',
      '--init',
      '--name',
      'zylos-global43-codex-restart-resume',
      '--mount',
      'type=volume,source=zylos-global43-codex-home-0-144-5,target=/acceptance/codex-home',
      'node',
      'scripts/e2e/codex-app-server-restart-resume.js',
    ]));
    for (const scenario of ['command_approval', 'file_approval', 'request_user_input']) {
      expect(plan.commands.interactions[scenario]).toEqual(expect.arrayContaining([
        'container',
        'run',
        '--interactive',
        '--init',
        '--mount',
        'type=volume,source=zylos-global43-codex-home-0-144-5,target=/acceptance/codex-home',
        '--env',
        `ZYLOS_CODEX_INTERACTION_SCENARIO=${scenario}`,
        'node',
        'scripts/e2e/codex-app-server-interaction-probe.js',
      ]));
    }

    const serialized = JSON.stringify(plan);
    expect(serialized).not.toMatch(/publish|publish-socket|websocket|unix socket/i);
    expect(serialized).not.toMatch(/codex exec|exec resume|per-turn|dual transport/i);
    expect(serialized).not.toContain('/Users/yvan/.codex');
  });

  test('redacts credential-shaped evidence without changing protocol facts', () => {
    const evidence = redactCodexAppServerContainerEvidence({
      protocol: { cli_version: 'codex-cli 0.144.5', transport: 'stdio' },
      credential: 'secret-value',
      token: 'token-value',
      nested: { api_key: 'key-value', status: 'authenticated' },
    });

    expect(evidence).toEqual({
      protocol: { cli_version: 'codex-cli 0.144.5', transport: 'stdio' },
      credential: '[REDACTED]',
      token: '[REDACTED]',
      nested: { api_key: '[REDACTED]', status: 'authenticated' },
    });
  });

  test('runs native initialize in a fresh credential-free CODEX_HOME', async () => {
    const calls = [];
    const evidence = await runCodexAppServerInitializeSmoke({
      codexExecutable: '/official/codex',
      baseEnvironment: {
        PATH: '/official/bin',
        HOME: '/host/home',
        CODEX_HOME: '/host/home/.codex',
        CODEX_API_KEY: 'must-not-cross-the-smoke-seam',
      },
      execFile: async (command, args, options) => {
        calls.push({ command, args, options });
        return { stdout: 'codex-cli 0.144.5\n', stderr: '' };
      },
      initialize: async (options) => {
        calls.push(options);
        return {
          userAgent: 'zylos-global43-native-smoke/0.144.5 (Mac OS; arm64)',
          codexHome: fs.realpathSync(options.environment.CODEX_HOME),
          platformFamily: 'unix',
          platformOs: 'macos',
        };
      },
    });

    expect(evidence).toMatchObject({
      provider_transport: 'official_app_server',
      protocol: {
        cli_version: 'codex-cli 0.144.5',
        transport: 'stdio',
        initialized_notification: true,
      },
      isolation: {
        dedicated_codex_home: true,
        host_codex_home_reused: false,
        credentials_inherited: false,
      },
    });
    for (const call of calls) {
      const environment = call.options?.env ?? call.environment;
      expect(environment.HOME).toBe(environment.CODEX_HOME);
      expect(environment.CODEX_HOME).not.toContain('/host/home');
      expect(environment.CODEX_API_KEY).toBeUndefined();
    }
    expect(calls.find((call) => call.environment)?.clientInfo).toEqual({
      name: 'zylos-global43-native-smoke',
      title: 'Zylos Global43 Native Smoke',
      version: '1.0.0',
    });
  });

  test('persists only known real evidence inside the dedicated CODEX_HOME', () => {
    const codexHome = fs.mkdtempSync(path.join(os.tmpdir(), 'global43-evidence-test-'));
    const outsideDirectory = fs.mkdtempSync(path.join(os.tmpdir(), 'global43-evidence-outside-'));
    const evidenceFile = path.join(codexHome, 'global43', 'real-evidence.json');
    const outsideFile = path.join(outsideDirectory, 'outside.json');
    const evidence = {
      evidence_schema_version: 1,
      provider_transport: 'official_app_server',
      lifecycle: { thread_ids: ['thread-1', 'thread-1', 'thread-1'] },
    };
    try {
      writeCodexAppServerRealEvidenceFile({ evidence, evidenceFile, codexHome });
      expect(JSON.parse(fs.readFileSync(evidenceFile, 'utf8'))).toEqual(evidence);
      expect(() => writeCodexAppServerRealEvidenceFile({
        evidence,
        evidenceFile: path.join(codexHome, '..', 'outside.json'),
        codexHome,
      })).toThrow(/inside CODEX_HOME/);
      fs.writeFileSync(outsideFile, 'outside-must-not-change\n');
      fs.rmSync(evidenceFile);
      fs.symlinkSync(outsideFile, evidenceFile);
      writeCodexAppServerRealEvidenceFile({ evidence, evidenceFile, codexHome });
      expect(fs.readFileSync(outsideFile, 'utf8')).toBe('outside-must-not-change\n');
      expect(JSON.parse(fs.readFileSync(evidenceFile, 'utf8'))).toEqual(evidence);
      expect(fs.lstatSync(evidenceFile).isSymbolicLink()).toBe(false);
    } finally {
      fs.rmSync(codexHome, { recursive: true, force: true });
      fs.rmSync(outsideDirectory, { recursive: true, force: true });
    }
  });

  test('resumes the one consistent durable thread after a fresh container start', async () => {
    const firstEvidence = {
      evidence_schema_version: 1,
      provider_transport: 'official_app_server',
      lifecycle: { thread_ids: ['thread-1', 'thread-1', 'thread-1'] },
    };
    expect(selectRestartResumeThreadId(firstEvidence)).toBe('thread-1');
    expect(() => selectRestartResumeThreadId({
      ...firstEvidence,
      lifecycle: { thread_ids: ['thread-1', 'thread-2', 'thread-1'] },
    })).toThrow(/one durable thread ID/);

    let receivedContext;
    let closed = false;
    const evidence = await runCodexAppServerRestartResume({
      providerNativeId: 'thread-1',
      createAdapter() {
        return {
          async *execute(context) {
            receivedContext = context;
            yield { kind: 'turn_state_changed', phase: 'running' };
            yield { kind: 'text_snapshot', payload: { text: 'ZYLOS-GLOBAL43-CONTAINER-RESTART-OK' } };
          },
          async close() { closed = true; },
        };
      },
    });

    expect(receivedContext.lineage.provider_native_id).toBe('thread-1');
    expect(closed).toBe(true);
    expect(evidence).toMatchObject({
      provider_transport: 'official_app_server',
      container_restart: {
        fresh_container: true,
        thread_id_reused: true,
        subsequent_turn: true,
      },
    });
  });

  test('drives a lease-fenced provider approval through durable answer acknowledgement', async () => {
    let workspaceFenceChecked = false;
    const evidence = await runCodexAppServerRealInteractionProbe({
      scenario: 'command_approval',
      createAdapter({ cwd }) {
        return {
          getWorkspaceAccess() {
            return {
              root: cwd,
              mode: 'writable',
              read_only_enforced: false,
              authority: 'core_workspace_lease',
            };
          },
          async *execute(context, controls) {
            await context.bindProviderNativeId('thread-interaction-1');
            context.reportProviderState({
              state: 'started',
              provider_native_id: 'thread-interaction-1',
            });
            controls.assertWorkspaceWrite();
            workspaceFenceChecked = true;
            yield {
              kind: 'tool_started',
              provider_native_id: 'thread-interaction-1',
              payload: {
                tool_use_id: 'tool-command-1',
                tool_name: 'command',
                summary: 'Command started.',
                side_effect_status: 'unknown',
              },
            };
            yield {
              kind: 'interaction_requested',
              provider_native_id: 'thread-interaction-1',
              payload: {
                provider_interaction_ref: 'provider-command-approval-1',
                tool_use_id: 'tool-command-1',
                kind: 'tool_approval',
                prompt: 'Allow the bounded command?',
                choices: [],
                authorized_subjects: context.interaction.authorized_subjects,
                allowed_sources: context.interaction.allowed_sources,
              },
            };
            yield {
              kind: 'text_snapshot',
              provider_native_id: 'thread-interaction-1',
              payload: { text: 'done', end_offset: 4 },
            };
          },
          async prepareInteractionAnswer(delivery) {
            return {
              async send(startedDelivery) {
                return {
                  status: 'accepted',
                  handoff_id: startedDelivery.handoff.handoff_id,
                  provider_attempt_id: startedDelivery.handoff.provider_attempt_id,
                  handoff_attempt_id: startedDelivery.handoff.handoff_attempt_id,
                  handoff_attempt_no: startedDelivery.handoff.handoff_attempt_no,
                  lease_epoch: startedDelivery.handoff.lease_epoch,
                };
              },
            };
          },
          async close() {},
        };
      },
    });

    expect(workspaceFenceChecked).toBe(true);
    expect(evidence).toMatchObject({
      scenario: 'command_approval',
      triggered: true,
      pre_action_lease_fenced: true,
      provider_neutral_tool_name: 'command',
      durable: {
        interaction_state: 'answered',
        handoff_state: 'accepted',
        answer_count: 1,
        audit_outcome: 'accepted',
        turn_state: 'completed',
      },
    });
  });
});
