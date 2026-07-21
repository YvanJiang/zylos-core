import { validateObservabilitySnapshot } from '../../contracts/public/index.js';
export { readExecutorObservability } from '../observability/executor-snapshot-client.js';

const TERMINAL_FAILURE_STATES = new Set([
  'failed',
  'timed_out',
  'cancelled',
  'stopped',
  'interrupted',
]);

export function projectScheduledTurn(snapshot, turnId) {
  if (typeof turnId !== 'string' || turnId.length === 0) {
    throw new TypeError('turnId must be a non-empty string');
  }
  const observed = validateObservabilitySnapshot(snapshot).forwarded;
  if (!observed.turns.complete) {
    return {
      disposition: 'unavailable',
      core_state: null,
      wait_reason: 'turn_visibility_degraded',
      terminal_error: null,
    };
  }
  const turn = observed.turns.items.find((item) => item.turn_id === turnId);
  if (turn === undefined) {
    return {
      disposition: 'unavailable',
      core_state: null,
      wait_reason: 'turn_not_visible',
      terminal_error: null,
    };
  }
  if (turn.state === 'completed') {
    return {
      disposition: 'succeeded',
      core_state: turn.state,
      wait_reason: null,
      terminal_error: null,
    };
  }
  if (TERMINAL_FAILURE_STATES.has(turn.state)) {
    return {
      disposition: 'failed',
      core_state: turn.state,
      wait_reason: null,
      terminal_error: turn.error?.user_message ?? `Scheduled turn ${turn.state}.`,
    };
  }
  let waitReason = null;
  if (turn.state === 'queued') {
    waitReason = observed.service.maintenance || observed.service.draining
      ? 'maintenance'
      : 'queue';
  } else if (turn.state === 'recovering') {
    waitReason = 'recovery';
  } else if (turn.state === 'waiting_user') {
    waitReason = 'waiting_user';
  }
  return {
    disposition: 'pending',
    core_state: turn.state,
    wait_reason: waitReason,
    terminal_error: null,
  };
}
