import { readFileSync } from 'node:fs';

import { describe, expect, test } from '@jest/globals';

import {
  admitNormalizedEvent,
  ContractKernelError,
  createNormalizedEventStreamState,
  INTERACTION_EVENT_KINDS,
  NORMALIZED_EVENT_KINDS,
  RECOVERY_EVENT_KINDS,
  RETRY_EVENT_KINDS,
  validateNormalizedEvent,
  validatePublicFixtureSafety,
} from '../contracts/public/index.js';

const fixture = JSON.parse(readFileSync(
  new URL('../contracts/public/fixtures/normalized-event-v1.json', import.meta.url),
  'utf8',
));

function materializeEvent(eventPatch) {
  return {
    ...fixture.base_document,
    ...eventPatch,
    payload: eventPatch.payload,
    error: eventPatch.error ?? null,
  };
}

function runStream(streamCase) {
  let state = createNormalizedEventStreamState();
  for (const eventPatch of streamCase.events) {
    state = admitNormalizedEvent(state, materializeEvent(eventPatch), {
      unknownProgressKinds: streamCase.unknown_progress_kinds ?? [],
    });
  }
  return state;
}

describe('zylos.normalized-event v1.0 documents', () => {
  test('publishes all interaction, retry, and recovery event kinds with valid payload fixtures', () => {
    expect(fixture.fixture_version).toBe('1.0');
    expect(fixture.kind_families.interaction).toEqual(INTERACTION_EVENT_KINDS);
    expect(fixture.kind_families.retry).toEqual(RETRY_EVENT_KINDS);
    expect(fixture.kind_families.recovery).toEqual(RECOVERY_EVENT_KINDS);
    expect(new Set(NORMALIZED_EVENT_KINDS).size).toBe(NORMALIZED_EVENT_KINDS.length);

    for (const kindCase of fixture.kind_cases) {
      const event = materializeEvent({
        event_id: `event-kind-${kindCase.kind}`,
        event_sequence: kindCase.event_sequence,
        turn_version: kindCase.turn_version,
        kind: kindCase.kind,
        phase: kindCase.phase,
        ...kindCase.document_fields,
        payload: kindCase.payload,
        error: kindCase.error ?? null,
      });
      expect(validateNormalizedEvent(event).forwarded).toEqual(event);
    }
    expect(validatePublicFixtureSafety(fixture)).toBe(true);
  });
});

describe('zylos.normalized-event v1.0 stream admission', () => {
  test('admits continuous versions and current attempt/lease fences through terminal state', () => {
    const continuous = fixture.valid_streams.find(({ name }) => name === 'continuous_terminal');
    const retryRecovery = fixture.valid_streams.find(
      ({ name }) => name === 'retry_recovery_advances_attempt_and_lease',
    );
    const opaqueProgress = fixture.valid_streams.find(
      ({ name }) => name === 'negotiated_unknown_progress_kind',
    );
    const attemptlessLease = fixture.valid_streams.find(
      ({ name }) => name === 'lease_fenced_before_provider_attempt',
    );
    const waitingInteraction = fixture.valid_streams.find(
      ({ name }) => name === 'fenced_interaction_after_waiting_state',
    );

    expect(runStream(continuous)).toMatchObject({
      turn_id: 'turn-stream-A',
      last_event_sequence: 5,
      last_turn_version: 5,
      current_state: 'completed',
      terminal: true,
    });
    expect(runStream(retryRecovery)).toMatchObject({
      turn_id: 'turn-stream-B',
      last_event_sequence: 12,
      current_attempt_id: 'attempt-B-2',
      current_attempt_no: 2,
      lease_epoch: 12,
      current_state: 'failed',
      terminal: true,
    });
    expect(runStream(opaqueProgress)).toMatchObject({
      turn_id: 'turn-stream-C',
      last_event_sequence: 5,
      current_state: 'running',
      terminal: false,
    });
    expect(runStream(attemptlessLease)).toMatchObject({
      turn_id: 'turn-stream-D',
      last_event_sequence: 2,
      current_attempt_id: null,
      lease_epoch: 9,
      terminal: false,
    });
    expect(runStream(waitingInteraction)).toMatchObject({
      turn_id: 'turn-stream-E',
      last_event_sequence: 6,
      current_state: 'waiting_user',
      current_attempt_id: 'attempt-E-1',
      lease_epoch: 30,
      terminal: false,
    });
  });

  test('rejects gaps, stale fences, unsafe unknown kinds, version regressions, and terminal late events', () => {
    for (const streamCase of fixture.invalid_streams) {
      try {
        runStream(streamCase);
        throw new Error(`expected ${streamCase.name} to be rejected`);
      } catch (error) {
        expect(error).toBeInstanceOf(ContractKernelError);
        expect(error.contractError.code).toBe(streamCase.error_code);
      }
    }
  });
});
