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
        early_binding_witness: 'sqlite_before_provider_started_insert_trigger',
      },
      prerequisites: {
        credential_status: 'authenticated',
      },
      failures: {
        auth: {
          provider_code: 'provider_auth_failed',
          core_status: 'failed',
          durable_error_event: true,
        },
        context: {
          provider_code: 'provider_context_invalid',
          core_status: 'failed',
          durable_error_event: true,
        },
        transient: {
          provider_code: 'delivery_transient',
          core_status: 'retry_scheduled',
          durable_error_event: true,
        },
        side_effect_unknown: {
          provider_code: 'side_effect_unknown',
          core_status: 'recovering',
          durable_error_event: true,
        },
      },
      interrupts: {
        stop: {
          request_status: 'interrupt_requested',
          provider_status: 'interrupted',
          terminal_status: 'interrupted',
        },
        timeout: {
          request_status: 'provider_stopped',
          provider_status: 'interrupted',
          terminal_status: 'interrupted',
        },
        steer: {
          request_status: 'interrupt_requested',
          provider_status: 'interrupted',
          terminal_status: 'interrupted',
        },
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
    expect(JSON.stringify(evidence)).not.toContain('Logged in using');

    process.stdout.write(
      `ZYLOS_CODEX_APP_SERVER_REAL_EVIDENCE=${JSON.stringify(evidence)}\n`,
    );
  }, 180_000);
});
