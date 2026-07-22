import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

import { describe, expect, test } from '@jest/globals';

import {
  CLAUDE_SDK_ACCEPTANCE_MATRIX,
  CLAUDE_SDK_TARGET,
  buildClaudeSdkEvidence,
  detectClaudeSdkPrerequisites,
  validateClaudeSdkTarget,
} from '../scripts/lib/claude-sdk-real-integration.js';
import {
  removeSdkTemporaryDirectoryAfterShutdown,
  runWithTimeout,
} from '../scripts/integration/claude-agent-sdk/live-runner.js';

describe('Claude Agent SDK real-integration acceptance gate', () => {
  test('pins the approved target and maps every acceptance case to executable evidence', () => {
    expect(CLAUDE_SDK_TARGET).toEqual({
      package: '@anthropic-ai/claude-agent-sdk',
      sdk_version: '0.3.215',
      bundled_cli_version: '2.1.215',
      provider_transport: 'claude_agent_sdk',
    });

    const caseIds = CLAUDE_SDK_ACCEPTANCE_MATRIX.map(({ case_id: caseId }) => caseId);
    expect(new Set(caseIds).size).toBe(caseIds.length);
    expect(caseIds).toEqual(expect.arrayContaining([
      'long_lived_async_multi_turn',
      'durable_session_id',
      'cancel',
      'permission_interaction',
      'idle_eviction_rebuild',
      'service_restart_resume',
      'provider_transient',
      'provider_auth',
      'provider_context',
      'stale_attempt',
      'mapping_recovery',
      'answer_send_before_ack',
      'side_effect_unknown_notify_first',
      'no_blind_replay',
    ]));
    expect(new Set(CLAUDE_SDK_ACCEPTANCE_MATRIX.map(({ lane }) => lane)))
      .toEqual(new Set(['real_provider', 'deterministic']));

    for (const entry of CLAUDE_SDK_ACCEPTANCE_MATRIX) {
      expect(entry.evidence).not.toHaveLength(0);
      for (const reference of entry.evidence) {
        const source = fs.readFileSync(new URL(`../${reference.file}`, import.meta.url), 'utf8');
        expect(source).toContain(reference.test_name);
      }
    }
  });

  test('fails closed on target, bundled CLI, capability, or runtime-control drift', () => {
    const valid = {
      sdkVersion: '0.3.215',
      bundledCliVersion: '2.1.215',
      querySurface: {
        async_iterator: true,
        interrupt: true,
        cancel_async_message: true,
        close: true,
      },
      capabilities: ['interrupt_receipt_v1'],
    };
    expect(validateClaudeSdkTarget(valid)).toEqual({
      status: 'passed',
      ...CLAUDE_SDK_TARGET,
      capabilities: ['interrupt_receipt_v1'],
      query_surface: valid.querySurface,
    });

    for (const invalid of [
      { ...valid, sdkVersion: '0.3.216' },
      { ...valid, bundledCliVersion: '2.1.214' },
      { ...valid, capabilities: [] },
      { ...valid, querySurface: { ...valid.querySurface, cancel_async_message: false } },
    ]) {
      expect(() => validateClaudeSdkTarget(invalid)).toThrow(/Claude Agent SDK target/);
    }
  });

  test('detects credential prerequisites without retaining credential values', () => {
    const prerequisites = detectClaudeSdkPrerequisites({
      environment: {
        ANTHROPIC_API_KEY: 'must-never-appear',
        CLAUDE_CODE_OAUTH_TOKEN: 'also-must-never-appear',
      },
      nativeAuth: { loggedIn: false, authMethod: null },
      networkAvailable: true,
    });
    expect(prerequisites).toEqual({
      available: true,
      credential_source: 'environment',
      credential_names: ['ANTHROPIC_API_KEY', 'CLAUDE_CODE_OAUTH_TOKEN'],
      native_auth_method: null,
      native_auth_original_logged_in: false,
      native_auth_isolated_logged_in: false,
      network_available: true,
      missing: [],
    });
    expect(JSON.stringify(prerequisites)).not.toMatch(/must-never|also-must/);

    expect(detectClaudeSdkPrerequisites({
      environment: {},
      nativeAuth: { loggedIn: true, authMethod: 'claude.ai' },
      networkAvailable: true,
    })).toMatchObject({
      available: true,
      credential_source: 'native_login',
      credential_names: [],
      native_auth_method: 'claude.ai',
    });

    expect(detectClaudeSdkPrerequisites({
      environment: {},
      nativeAuth: {
        loggedIn: false,
        originalLoggedIn: true,
        authMethod: null,
      },
      networkAvailable: true,
    })).toMatchObject({
      available: false,
      credential_source: null,
      native_auth_original_logged_in: true,
      native_auth_isolated_logged_in: false,
      missing: ['credential'],
    });
  });

  test('keeps partial evidence non-release-ready until every real-provider case passes', () => {
    const deterministic = { status: 'passed', passed: 8, failed: 0 };
    const nativeTarget = { status: 'passed', ...CLAUDE_SDK_TARGET };
    const partial = buildClaudeSdkEvidence({
      deterministic,
      nativeTarget,
      realProvider: {
        status: 'skipped',
        reason: 'credential_required',
        cases: [],
      },
    });
    expect(partial).toMatchObject({
      schema_version: 1,
      status: 'partial',
      release_ready: false,
      lanes: {
        deterministic: { status: 'passed' },
        native_target: { status: 'passed' },
        real_provider: { status: 'skipped', reason: 'credential_required' },
      },
    });
    expect(partial.unrun_cases).toEqual(expect.arrayContaining([
      'long_lived_async_multi_turn',
      'cancel',
      'permission_interaction',
      'idle_eviction_rebuild',
      'service_restart_resume',
    ]));
    expect(partial.acceptance_matrix).toEqual(CLAUDE_SDK_ACCEPTANCE_MATRIX);

    const realCases = CLAUDE_SDK_ACCEPTANCE_MATRIX
      .filter(({ lane }) => lane === 'real_provider')
      .map(({ case_id: caseId }) => ({ case_id: caseId, status: 'passed' }));
    expect(buildClaudeSdkEvidence({
      deterministic,
      nativeTarget,
      realProvider: { status: 'passed', cases: realCases },
    })).toMatchObject({ status: 'passed', release_ready: true, unrun_cases: [] });
  });

  test('treats timeout cleanup as a barrier before returning control', async () => {
    const abortController = new AbortController();
    const events = [];
    const operation = async () => {
      await new Promise((resolve) => {
        abortController.signal.addEventListener('abort', resolve, { once: true });
      });
      events.push('operation_settled');
    };
    await expect(runWithTimeout(operation, {
      abortController,
      forceClose: async () => events.push('forced_close'),
      timeoutMs: 1,
    })).rejects.toMatchObject({ code: 'live_acceptance_timeout' });
    expect(events).toEqual(expect.arrayContaining(['forced_close', 'operation_settled']));

    const unconfirmedAbort = new AbortController();
    const unconfirmedOperation = async () => {
      await new Promise((resolve) => {
        unconfirmedAbort.signal.addEventListener('abort', resolve, { once: true });
      });
    };
    await expect(runWithTimeout(unconfirmedOperation, {
      abortController: unconfirmedAbort,
      forceClose: async () => {
        throw new Error('unconfirmed');
      },
      timeoutMs: 1,
    })).rejects.toMatchObject({ code: 'live_cleanup_unconfirmed' });

    const rejectedCleanupAbort = new AbortController();
    const rejectedCleanupOperation = async () => {
      await new Promise((resolve) => {
        rejectedCleanupAbort.signal.addEventListener('abort', resolve, { once: true });
      });
      const error = new Error('scenario cleanup rejected');
      error.code = 'live_cleanup_unconfirmed';
      throw error;
    };
    await expect(runWithTimeout(rejectedCleanupOperation, {
      abortController: rejectedCleanupAbort,
      forceClose: async () => {},
      timeoutMs: 1,
    })).rejects.toMatchObject({ code: 'live_cleanup_unconfirmed' });
  });

  test('removes SDK temporary config recreated after shutdown begins', async () => {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'zylos-global42-cleanup-test-'));
    try {
      let waitCalls = 0;
      await removeSdkTemporaryDirectoryAfterShutdown(directory, {
        settleMs: 1,
        wait: async () => {
          waitCalls += 1;
          fs.mkdirSync(directory, { recursive: true });
          fs.writeFileSync(path.join(directory, '.late-sdk-state'), 'late');
        },
      });
      expect(waitCalls).toBe(1);
      expect(fs.existsSync(directory)).toBe(false);
    } finally {
      fs.rmSync(directory, { recursive: true, force: true });
    }
  });

  test('exposes one fail-closed command and an operator record for the real target', () => {
    const packageDocument = JSON.parse(fs.readFileSync(
      new URL('../package.json', import.meta.url),
      'utf8',
    ));
    expect(packageDocument.scripts['test:integration:claude-sdk']).toBe(
      'node scripts/install-skill-deps.js && node scripts/verify-claude-sdk-real-integration.js --require-live',
    );

    const verifier = fs.readFileSync(
      new URL('../scripts/verify-claude-sdk-real-integration.js', import.meta.url),
      'utf8',
    );
    expect(verifier).toContain('ZYLOS_CLAUDE_SDK_ACCEPTANCE_EVIDENCE=');
    expect(verifier).toContain('--require-live');
    expect(verifier).toContain('runLiveClaudeSdkAcceptance');
    expect(verifier).toContain("child.kill('SIGTERM')");
    expect(verifier).toContain("child.kill('SIGKILL')");
    expect(verifier).toContain('PROCESS_HARD_TIMEOUT_MS');

    const liveRunner = fs.readFileSync(
      new URL('../scripts/integration/claude-agent-sdk/live-runner.js', import.meta.url),
      'utf8',
    );
    expect(liveRunner).toContain("stopped.provider_stop_status === 'confirmed'");
    expect(liveRunner).toContain('valid_receipt: Array.isArray(receipt?.still_queued)');
    expect(liveRunner).toContain('CLAUDE_CONFIG_DIR: configDirectory');
    expect(liveRunner).toContain('forceClose: observed.closeAll');
    expect(liveRunner).toContain("return error?.code !== 'live_cleanup_unconfirmed'");
    expect(liveRunner).toContain('await closeScenario(service, observed)');
    expect(liveRunner.match(
      /for \(const caseId of passedCaseIds\) markPassed\(caseId\);/g,
    )).toHaveLength(3);
    expect(liveRunner).not.toMatch(/markPassed\('/);

    const installer = fs.readFileSync(
      new URL('../scripts/install-skill-deps.js', import.meta.url),
      'utf8',
    );
    expect(installer).toContain("installArguments.push('--package-lock=false')");

    const operatorRecord = fs.readFileSync(
      new URL('../docs/claude-agent-sdk-real-integration.md', import.meta.url),
      'utf8',
    );
    expect(operatorRecord).toContain('@anthropic-ai/claude-agent-sdk` `0.3.215`');
    expect(operatorRecord).toContain('Claude Code `2.1.215`');
    expect(operatorRecord).toContain('release_ready=false');
    expect(operatorRecord).toContain('never converts an unavailable real-provider lane into a pass');
  });
});
