import { describe, expect, test } from '@jest/globals';

import { runCodexAppServerRealIntegration } from '../scripts/lib/codex-app-server-real-integration.js';

const realTest = process.env.ZYLOS_RUN_CODEX_APP_SERVER_REAL_INTEGRATION === '1'
  ? test
  : test.skip;

describe('Codex app-server real integration', () => {
  realTest('validates the official target app-server through durable Core seams', async () => {
    const evidence = await runCodexAppServerRealIntegration();

    expect(evidence).toMatchObject({
      evidence_schema_version: 1,
      provider_transport: 'official_app_server',
      protocol: {
        cli_version: 'codex-cli 0.144.5',
        initialize_user_agent: expect.stringContaining('/0.144.5 '),
        experimental_api: true,
      },
      lifecycle: {
        initialize_connect: true,
        new_thread: true,
        subsequent_turn: true,
        early_durable_thread_binding: true,
        restart_thread_resume: true,
        provider_neutral_translation: true,
      },
      failures: {
        auth: 'provider_auth_failed',
        context: 'provider_context_invalid',
        transient: 'delivery_transient',
        side_effect_unknown: 'side_effect_unknown',
      },
      interrupts: {
        stop: 'interrupt_requested',
        timeout: 'provider_stopped',
        steer: 'interrupt_requested',
      },
    });
    expect(evidence.lifecycle.thread_ids).toHaveLength(3);
    expect(new Set(evidence.lifecycle.thread_ids).size).toBe(1);
    expect(evidence.lifecycle.normalized_event_kinds).toEqual(expect.arrayContaining([
      'text_delta',
      'text_snapshot',
      'turn_state_changed',
    ]));
    expect(evidence.unrun_cases).toEqual(expect.arrayContaining([
      expect.objectContaining({ case: 'command_approval' }),
      expect.objectContaining({ case: 'file_approval' }),
      expect.objectContaining({ case: 'request_user_input' }),
      expect.objectContaining({ case: 'mcp_elicitation' }),
    ]));

    process.stdout.write(
      `ZYLOS_CODEX_APP_SERVER_REAL_EVIDENCE=${JSON.stringify(evidence)}\n`,
    );
  }, 180_000);
});
