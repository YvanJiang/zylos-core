import assert from 'node:assert/strict';
import fs from 'node:fs';
import { describe, test } from '@jest/globals';

import {
  projectScheduledTurn,
  readExecutorObservability,
} from '../runtime/scheduler/scheduler-observability.js';

const fixtures = JSON.parse(fs.readFileSync(
  new URL('../contracts/public/fixtures/observability-v1.json', import.meta.url),
  'utf8',
));

function snapshotWithTurn(state, overrides = {}) {
  const snapshot = structuredClone(fixtures.cases.complete);
  snapshot.service.maintenance = overrides.maintenance ?? false;
  snapshot.service.draining = overrides.draining ?? false;
  snapshot.turns.items[0] = {
    ...snapshot.turns.items[0],
    state,
    phase: state,
    queue_position: state === 'queued' ? 1 : null,
    side_effect_status: overrides.side_effect_status ?? 'none',
    error: overrides.error ?? null,
  };
  return snapshot;
}

describe('scheduler consumes authoritative Core observability', () => {
  test('keeps queued maintenance and recovering occurrences durable without abandonment', () => {
    assert.deepEqual(projectScheduledTurn(
      snapshotWithTurn('queued', { maintenance: true }),
      'turn-A',
    ), {
      disposition: 'pending',
      core_state: 'queued',
      wait_reason: 'maintenance',
      terminal_error: null,
    });
    assert.deepEqual(projectScheduledTurn(
      snapshotWithTurn('recovering', {
        side_effect_status: 'unknown',
        error: fixtures.cases.complete.turns.items[0].error,
      }),
      'turn-A',
    ), {
      disposition: 'pending',
      core_state: 'recovering',
      wait_reason: 'recovery',
      terminal_error: null,
    });
  });

  test('terminalizes local history only from a canonical Core terminal turn', () => {
    assert.deepEqual(projectScheduledTurn(snapshotWithTurn('completed'), 'turn-A'), {
      disposition: 'succeeded',
      core_state: 'completed',
      wait_reason: null,
      terminal_error: null,
    });
    const failure = fixtures.cases.complete.turns.items[0].error;
    assert.deepEqual(projectScheduledTurn(snapshotWithTurn('failed', { error: failure }), 'turn-A'), {
      disposition: 'failed',
      core_state: 'failed',
      wait_reason: null,
      terminal_error: failure.user_message,
    });
  });

  test('fails closed when the full replacement snapshot cannot prove the turn', () => {
    const snapshot = snapshotWithTurn('running');
    snapshot.turns.items = [];
    assert.deepEqual(projectScheduledTurn(snapshot, 'turn-A'), {
      disposition: 'unavailable',
      core_state: null,
      wait_reason: 'turn_not_visible',
      terminal_error: null,
    });
  });

  test('reads and validates the provider-neutral health response from the executor socket', async () => {
    const snapshot = snapshotWithTurn('running');
    const observed = await readExecutorObservability({
      zylosDir: '/tmp/zylos-scheduler-observability-fixture',
      requestFn: async (socketPath, request) => {
        assert.equal(socketPath, '/tmp/zylos-scheduler-observability-fixture/runtime/executor-service.sock');
        assert.deepEqual(request, { action: 'health' });
        return { ok: true, result: { snapshot } };
      },
    });
    assert.equal(JSON.stringify(observed), JSON.stringify(snapshot));
  });
});
