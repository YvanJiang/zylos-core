import { readFileSync } from 'node:fs';

import { describe, expect, test } from '@jest/globals';

import {
  ContractKernelError,
  validateInboundEnvelope,
  validateInboundResult,
  validatePublicFixtureSafety,
} from '../contracts/public/index.js';

const inboundEnvelopeFixture = JSON.parse(readFileSync(
  new URL('../contracts/public/fixtures/inbound-envelope-v1.json', import.meta.url),
  'utf8',
));

const inboundResultFixture = JSON.parse(readFileSync(
  new URL('../contracts/public/fixtures/inbound-result-v1.json', import.meta.url),
  'utf8',
));

describe('zylos.inbound-envelope v1.0', () => {
  test('publishes the complete cross-repository identity and source fixture matrix', () => {
    expect(inboundEnvelopeFixture.fixture_version).toBe('1.0');
    expect(inboundEnvelopeFixture.valid.map(({ name }) => name)).toEqual([
      'authenticated_dm_with_attachment',
      'group_main_conversation',
      'native_thread_or_topic',
      'reply_preserves_group_conversation',
      'scheduler_synthetic_conversation',
      'scheduler_bound_group_conversation',
      'legacy_compatibility_fields',
    ]);

    for (const fixture of inboundEnvelopeFixture.valid) {
      const result = validateInboundEnvelope(fixture.document);
      expect(result.forwarded).toEqual(fixture.document);
    }
    expect(validatePublicFixtureSafety(inboundEnvelopeFixture)).toBe(true);
  });

  test('rejects invalid conditional identity, source, roles, and idempotency fields', () => {
    for (const fixture of inboundEnvelopeFixture.invalid) {
      try {
        validateInboundEnvelope(fixture.document, {
          occurredAt: '2026-07-19T05:30:00Z',
        });
        throw new Error(`expected ${fixture.name} to be rejected`);
      } catch (error) {
        expect(error).toBeInstanceOf(ContractKernelError);
        expect(error.contractError.code).toBe(fixture.error_code);
      }
    }
  });
});

describe('zylos.inbound-result v1.0', () => {
  test('enforces the authoritative required/null result matrix and original-result replay', () => {
    expect(inboundResultFixture.fixture_version).toBe('1.0');
    expect(inboundResultFixture.valid.map(({ name }) => name)).toEqual([
      'normal_bound',
      'control',
      'pending_recovery',
      'queue_full_failed',
      'rejection',
      'persisted_non_queue_rejection',
      'deduplicated_original_result_replay',
    ]);

    for (const fixture of inboundResultFixture.valid) {
      expect(validateInboundResult(fixture.document).forwarded).toEqual(fixture.document);
    }

    const original = inboundResultFixture.valid.find(({ name }) => name === 'normal_bound').document;
    const replay = inboundResultFixture.valid.find(
      ({ name }) => name === 'deduplicated_original_result_replay',
    ).document;
    const { trace_id: originalTrace, deduplicated: originalDedupe, ...originalResult } = original;
    const { trace_id: replayTrace, deduplicated: replayDedupe, ...replayedResult } = replay;
    expect(originalTrace).not.toBe(replayTrace);
    expect(originalDedupe).toBe(false);
    expect(replayDedupe).toBe(true);
    expect(replayedResult).toEqual(originalResult);
    expect(validatePublicFixtureSafety(inboundResultFixture)).toBe(true);
  });

  test('rejects impossible status, lineage, control, commit, and error combinations', () => {
    for (const fixture of inboundResultFixture.invalid) {
      try {
        validateInboundResult(fixture.document, { occurredAt: '2026-07-19T05:31:00Z' });
        throw new Error(`expected ${fixture.name} to be rejected`);
      } catch (error) {
        expect(error).toBeInstanceOf(ContractKernelError);
        expect(error.contractError.code).toBe(fixture.error_code);
      }
    }
  });
});
