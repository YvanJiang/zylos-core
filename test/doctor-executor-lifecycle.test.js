import { describe, expect, test } from '@jest/globals';

import { buildExecutorDoctorReport } from '../cli/commands/doctor.js';

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
        health: 'healthy',
        serviceInstanceId: 'executor-doctor',
        snapshot: {
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
});
