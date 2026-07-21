import { describe, expect, test } from '@jest/globals';

import {
  buildExecutorDoctorReport,
  collectExecutorDoctorReport,
} from '../cli/commands/doctor.js';

describe('executor lifecycle doctor', () => {
  test('uses canonical Core health and provider prerequisites', () => {
    expect(buildExecutorDoctorReport({
      initialized: true,
      pm2Installed: true,
      provider: 'codex',
      providerInstalled: true,
      providerAuthStatus: 'success',
      coreHealth: {
        ok: true,
        provider: 'codex',
        health: 'healthy',
        serviceInstanceId: 'executor-doctor',
        snapshot: {
          contract: 'zylos.observability-snapshot',
          service: {
            host_id: 'host-doctor',
            service_instance_id: 'executor-doctor',
            started_at: '2026-07-20T12:00:00.000Z',
          },
        },
      },
    })).toMatchObject({
      passed: true,
      service: {
        health: 'healthy',
        service_instance_id: 'executor-doctor',
        host_id: 'host-doctor',
      },
      provider: { name: 'codex', transport: 'official_app_server', ready: true },
      issues: [],
    });
  });

  test('reports a failed result when Core is offline without inventing process health', () => {
    expect(buildExecutorDoctorReport({
      initialized: true,
      pm2Installed: true,
      provider: 'claude',
      providerInstalled: true,
      providerAuthStatus: 'success',
      coreHealth: { ok: false, error: 'ENOENT' },
    })).toMatchObject({
      passed: false,
      service: { health: 'offline', error: 'ENOENT' },
      issues: [expect.objectContaining({ id: 'executor_offline' })],
    });
  });

  test('reads authoritative Core health even when local init artifacts are absent', async () => {
    const healthCalls = [];
    const report = await collectExecutorDoctorReport({
      zylosDir: '/tmp/zylos-doctor-missing-init',
      configDir: '/tmp/zylos-doctor-missing-init/.zylos',
      readConfig: () => ({ runtime: 'codex' }),
      commandExistsFn: () => true,
      codexAuthenticated: () => true,
      getCoreHealth: async (request) => {
        healthCalls.push(request);
        return {
          ok: true, provider: 'codex', health: 'healthy',
          serviceInstanceId: 'executor-without-config',
          snapshot: {
            contract: 'zylos.observability-snapshot',
            service: {
              host_id: 'host-without-config', service_instance_id: 'executor-without-config',
              started_at: '2026-07-21T00:00:00.000Z',
            },
          },
        };
      },
    });
    expect(healthCalls).toEqual([{ zylosDir: '/tmp/zylos-doctor-missing-init' }]);
    expect(report).toMatchObject({
      initialized: false,
      service: { health: 'healthy', service_instance_id: 'executor-without-config' },
      issues: [expect.objectContaining({ id: 'not_initialized' })],
    });
  });

  test('uses Core provider identity from a valid degraded snapshot and reports configuration drift', () => {
    expect(buildExecutorDoctorReport({
      initialized: true,
      pm2Installed: true,
      configuredProvider: 'claude',
      provider: 'claude',
      providerInstalled: true,
      providerAuthStatus: 'success',
      coreHealth: {
        ok: false,
        error: 'executor_degraded',
        provider: 'codex',
        health: 'degraded',
        serviceInstanceId: 'executor-doctor-provider',
        snapshot: {
          contract: 'zylos.observability-snapshot',
          service: {
            host_id: 'host-doctor-provider',
            service_instance_id: 'executor-doctor-provider',
            started_at: '2026-07-20T12:00:00.000Z',
          },
        },
      },
    })).toMatchObject({
      passed: false,
      provider: { name: 'codex', transport: 'official_app_server' },
      service: { health: 'degraded', service_instance_id: 'executor-doctor-provider' },
      issues: [
        expect.objectContaining({ id: 'provider_identity_mismatch' }),
        expect.objectContaining({ id: 'executor_unhealthy' }),
      ],
    });
  });
});
