import {
  NORMALIZED_EVENT_PHASES,
  TURN_STATES,
} from './normalized-event.js';

export const CANONICAL_TURN_STATES = TURN_STATES;
export const TURN_PHASES = NORMALIZED_EVENT_PHASES;

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
