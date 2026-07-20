import assert from 'node:assert/strict';
import fs from 'node:fs';
import { describe, test } from '@jest/globals';

import { projectRuntimeHealth } from '../runtime/observability/health-projection.js';

const fixtures = JSON.parse(fs.readFileSync(
  new URL('../contracts/public/fixtures/observability-v1.json', import.meta.url),
  'utf8',
));

describe('provider-neutral runtime health', () => {
  test('projects service, turn, queue, and outbox facts without terminal-window authority', () => {
    const projection = projectRuntimeHealth(fixtures.cases.complete);
    assert.equal(projection.state, 'healthy');
    assert.deepEqual(projection.service, {
      health: 'healthy',
      maintenance: false,
      draining: false,
      reconciling: false,
      service_instance_id: 'core-service-A',
    });
    assert.equal(projection.executors.items[0].queue_length, 2);
    assert.equal(projection.turns.items[0].state, 'recovering');
    assert.equal(projection.outbox.retry_count, 1);
    assert.doesNotMatch(JSON.stringify(projection), /tmux|window|pane|prompt|overlay|input_state/i);
  });

  test('web console reads only the canonical executor snapshot client', () => {
    const source = fs.readFileSync(
      new URL('../skills/web-console/scripts/server.js', import.meta.url),
      'utf8',
    );
    assert.match(source, /readExecutorObservability/);
    assert.doesNotMatch(source, /agent-status|activity-monitor|STATUS_FILE|tmux|capture-pane|send-keys/i);
  });

  test('health-check and scheduler skill guidance use Core observability only', () => {
    const health = fs.readFileSync(
      new URL('../skills/health-check/SKILL.md', import.meta.url), 'utf8',
    );
    const scheduler = fs.readFileSync(
      new URL('../skills/scheduler/SKILL.md', import.meta.url), 'utf8',
    );
    assert.match(health, /zylos doctor --check --json/);
    assert.doesNotMatch(health, /pm2|activity.monitor|c4-send|tmux|window|pane|input state/i);
    assert.match(scheduler, /observability snapshot/i);
    assert.doesNotMatch(scheduler, /agent-status|activity.monitor|runtime is alive|done <task/i);
  });
});
