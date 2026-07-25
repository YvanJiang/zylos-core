import { INTERACTION_REQUEST_SCHEMA_V1 } from './interaction.js';
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

export const INTERACTION_STATES = INTERACTION_REQUEST_SCHEMA_V1.states;
export const HANDOFF_STATES = INTERACTION_REQUEST_SCHEMA_V1.handoffStates;
