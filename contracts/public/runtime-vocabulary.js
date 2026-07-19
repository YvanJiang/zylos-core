export const CANONICAL_TURN_STATES = Object.freeze([
  'received',
  'queued',
  'starting',
  'running',
  'waiting_user',
  'redirecting',
  'recovering',
  'completed',
  'stopped',
  'cancelled',
  'interrupted',
  'failed',
  'timed_out',
]);

export const TURN_PHASES = Object.freeze([
  'received',
  'queued',
  'starting',
  'running',
  'waiting_user',
  'redirecting',
  'recovering',
  'retrying',
  'completed',
  'stopped',
  'cancelled',
  'interrupted',
  'failed',
  'timed_out',
]);

export const PROVIDERS = Object.freeze(['claude', 'codex']);
export const RUNTIME_HEALTH_STATES = Object.freeze([
  'healthy',
  'degraded',
  'offline',
  'unknown',
]);

export const INTERACTION_STATES = Object.freeze([
  'pending',
  'answer_committed',
  'answer_delivering',
  'delivery_unknown',
  'answered',
  'rejected',
  'expired',
  'cancelled',
]);

export const HANDOFF_STATES = Object.freeze([
  'not_started',
  'pending',
  'delivering',
  'retry_wait',
  'accepted',
  'delivery_unknown',
  'rejected',
  'cancelled',
]);
