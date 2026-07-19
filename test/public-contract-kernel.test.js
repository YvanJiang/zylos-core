import { readFileSync } from 'node:fs';

import { describe, expect, test } from '@jest/globals';

import {
  buildIdempotencyKeyInput,
  canonicalizeJson,
  ContractKernelError,
  createIdempotencyKey,
  createLegacyC4IdempotencyKey,
  createPayloadHash,
  PUBLIC_CONTRACTS,
  PUBLIC_ERROR_CODES,
  projectPayloadForHash,
  resolveIdempotencyReplay,
  validateContractDocument,
  validateContractError,
  validateContractHeader,
  validateOpaqueId,
  validatePublicFixtureSafety,
  validatePublicNumber,
  validateRfc3339Timestamp,
  verifyIdempotencyKey,
} from '../contracts/public/index.js';

describe('RFC 8785 JSON canonicalization', () => {
  test('matches the RFC 8785 primitive serialization and recursive property ordering sample', () => {
    const input = {
      numbers: [333333333.33333329, 1e30, 4.50, 2e-3, 1e-27],
      string: "€$\u000f\nA'B\"\\\\\"/",
      literals: [null, true, false],
    };

    expect(canonicalizeJson(input)).toBe(
      '{"literals":[null,true,false],"numbers":[333333333.3333333,1e+30,4.5,0.002,1e-27],"string":"€$\\u000f\\nA\'B\\"\\\\\\\\\\"/"}',
    );
  });

  test('uses locale-independent UTF-16 key ordering and rejects invalid JCS data', () => {
    const sorted = canonicalizeJson({
      '€': 'Euro Sign',
      '\r': 'Carriage Return',
      'דּ': 'Hebrew Letter Dalet With Dagesh',
      '1': 'One',
      '😀': 'Emoji: Grinning Face',
      '\u0080': 'Control',
      'ö': 'Latin Small Letter O With Diaeresis',
    });

    const orderedKeys = ['\r', '1', '\u0080', 'ö', '€', '😀', 'דּ'];
    const positions = orderedKeys.map((key) => sorted.indexOf(`${JSON.stringify(key)}:`));
    expect(positions.every((position) => position >= 0)).toBe(true);
    expect(positions).toEqual([...positions].sort((left, right) => left - right));
    expect(() => canonicalizeJson({ bad: '\ud800' })).toThrow(ContractKernelError);
    expect(() => canonicalizeJson({ bad: Number.NaN })).toThrow(ContractKernelError);
  });
});

describe('standard idempotency keys', () => {
  test('derives the inbound scope key from the RFC 8785 canonical tuple', () => {
    expect(createIdempotencyKey('inbound', {
      region: 'cn',
      tenant_id: 'tenant-A',
      channel: 'feishu',
      bot_id: 'bot-A',
      inbound_event_id: 'evt-FS-001',
    })).toBe(
      'zid:v1:inbound:96579c1d8766cecd47048ea4932ddc4c0527741d2192cea4c38cbd83bd66fa85',
    );
  });

  test('derives every remaining standard scope and preserves the legacy C4 exception exactly', () => {
    const vectors = [
      ['scheduler', {
        region: 'cn',
        tenant_id: 'tenant-A',
        bot_id: 'bot-A',
        schedule_id: 'schedule-daily',
        occurrence_id: 'occurrence-2026-07-19',
      }, 'zid:v1:scheduler:689969cbec0b058e06d6b1d02523f7289f681018c87bd5ad37c30d1cf1b1e17e'],
      ['interaction', {
        interaction_id: 'interaction-A',
        source_event_or_action_id: 'action-A',
      }, 'zid:v1:interaction:5b22b318e0011f774e272d311c77fa26381d5eb165a618c029e59bcf7b93e224'],
      ['control', {
        caller_namespace: 'dashboard.prod',
        control_id: 'control-A',
      }, 'zid:v1:control:2ab777073e87e0fe4cc7fcfba0547a7ea290efe42b912a77325f99e7e6c138d1'],
      ['delivery', {
        channel: 'feishu',
        target: {
          region: 'cn',
          tenant_id: 'tenant-A',
          bot_id: 'bot-A',
          chat_type: 'group',
          chat_id: 'chat-A',
          native_thread_or_topic_id: null,
        },
        delivery_id: 'delivery-A',
      }, 'zid:v1:delivery:c1ac917e70503d51737bd5066881aeacdcf31d80e01d838108d5ccd1a8893c76'],
    ];

    for (const [scope, fields, expected] of vectors) {
      expect(createIdempotencyKey(scope, fields)).toBe(expected);
    }

    expect(createLegacyC4IdempotencyKey(' record-42 ')).toBe('legacy-c4: record-42 ');
    expect(() => createLegacyC4IdempotencyKey('')).toThrow(ContractKernelError);
    expect(() => createLegacyC4IdempotencyKey('record\n42')).toThrow(ContractKernelError);
  });

  test('recomputes and verifies a producer key with mismatch semantics', () => {
    const fields = {
      interaction_id: 'interaction-A',
      source_event_or_action_id: 'action-A',
    };
    const expected = createIdempotencyKey('interaction', fields);

    expect(verifyIdempotencyKey('interaction', fields, expected)).toBe(true);
    try {
      verifyIdempotencyKey('interaction', fields, `${expected.slice(0, -1)}0`, {
        occurredAt: '2026-07-19T04:00:02Z',
      });
      throw new Error('expected key mismatch');
    } catch (error) {
      expect(error).toBeInstanceOf(ContractKernelError);
      expect(error.contractError.code).toBe('idempotency_key_mismatch');
    }
  });
});

describe('idempotency payload projection', () => {
  test('hashes only stable known business fields and strips transient or optional extension data', () => {
    const payload = {
      contract: 'zylos.inbound-envelope',
      contract_version: '1.0',
      inbound_event_id: 'evt-FS-001',
      idempotency_key: 'ignored-on-purpose',
      trace_id: 'trace-retry-002',
      occurred_at: '2026-07-19T04:00:00Z',
      received_at: '2026-07-19T04:00:01Z',
      region: 'cn',
      tenant_id: 'tenant-A',
      channel: 'feishu',
      bot_id: 'bot-A',
      message_id: 'msg-A',
      content: { kind: 'text', text: 'Hello, Zylos.', attachments: [] },
      source: { kind: 'platform_original', source_ref: 'diagnostic:source-1' },
      headers: { 'x-request-id': 'transport-only' },
      future_optional: { display_hint: 'compact' },
    };
    const knownFields = [
      'contract',
      'contract_version',
      'inbound_event_id',
      'idempotency_key',
      'trace_id',
      'occurred_at',
      'received_at',
      'region',
      'tenant_id',
      'channel',
      'bot_id',
      'message_id',
      'content',
      'source',
      'headers',
    ];

    const projection = projectPayloadForHash(payload, { scope: 'inbound', knownFields });

    expect(projection).toEqual({
      contract: 'zylos.inbound-envelope',
      contract_version: '1.0',
      inbound_event_id: 'evt-FS-001',
      occurred_at: '2026-07-19T04:00:00Z',
      region: 'cn',
      tenant_id: 'tenant-A',
      channel: 'feishu',
      bot_id: 'bot-A',
      message_id: 'msg-A',
      content: { kind: 'text', text: 'Hello, Zylos.', attachments: [] },
      source: { kind: 'platform_original' },
    });
    expect(createPayloadHash(payload, { scope: 'inbound', knownFields })).toBe(
      '65d4f680026f87ba69c2b0e83c2ec707ecdd7f2e129e2e22b069c1aff255f212',
    );
  });

  test('deduplicates the same key and payload while rejecting a changed payload', () => {
    const keyFields = {
      interaction_id: 'interaction-A',
      source_event_or_action_id: 'action-A',
    };
    const payload = {
      contract: 'zylos.interaction-answer',
      contract_version: '1.0',
      trace_id: 'trace-A',
      interaction_id: 'interaction-A',
      source_event_or_action_id: 'action-A',
      value: { kind: 'decision', decision: 'approve' },
    };
    const hashOptions = {
      scope: 'interaction',
      knownFields: Object.keys(payload),
    };
    const existing = {
      idempotency_key: createIdempotencyKey('interaction', keyFields),
      payload_hash: createPayloadHash(payload, hashOptions),
    };
    const transientReplay = {
      idempotency_key: existing.idempotency_key,
      payload_hash: createPayloadHash({ ...payload, trace_id: 'trace-B' }, hashOptions),
    };

    expect(transientReplay.payload_hash).toBe(existing.payload_hash);
    expect(resolveIdempotencyReplay(existing, transientReplay)).toEqual({
      status: 'duplicate',
      error: null,
    });

    const changedPayloadHash = createPayloadHash({
      ...payload,
      value: { kind: 'decision', decision: 'reject' },
    }, hashOptions);
    expect(changedPayloadHash).not.toBe(existing.payload_hash);
    const conflict = resolveIdempotencyReplay(existing, {
      idempotency_key: existing.idempotency_key,
      payload_hash: changedPayloadHash,
    }, { occurredAt: '2026-07-19T04:00:03Z' });
    expect(conflict.status).toBe('conflict');
    expect(conflict.error).toEqual({
      code: 'idempotency_conflict',
      category: 'conflict',
      retryable: false,
      side_effect_status: 'none',
      user_message: 'The idempotency key was already used with a different payload.',
      occurred_at: '2026-07-19T04:00:03Z',
    });
  });

  test('enforces the project numeric subset before hashing', () => {
    expect(() => createPayloadHash({ count: -0 }, {
      scope: 'control',
      knownFields: ['count'],
    })).toThrow(ContractKernelError);
    expect(() => createPayloadHash({ ratio: 1.25 }, {
      scope: 'control',
      knownFields: ['ratio'],
    })).toThrow(ContractKernelError);
    expect(createPayloadHash({ ratio: 1.25 }, {
      scope: 'control',
      knownFields: ['ratio'],
      decimalPaths: ['ratio'],
    })).toMatch(/^[0-9a-f]{64}$/);
  });
});

describe('shared v1 idempotency golden vectors', () => {
  test('recomputes key and payload hash bytes for all five scopes and legacy C4', () => {
    const fixture = JSON.parse(readFileSync(
      new URL('../contracts/public/fixtures/idempotency-v1.json', import.meta.url),
      'utf8',
    ));

    expect(fixture.fixture_version).toBe('1.0');
    expect(fixture.vectors.map((vector) => vector.scope)).toEqual([
      'inbound',
      'scheduler',
      'interaction',
      'control',
      'delivery',
      'legacy-c4',
    ]);

    for (const vector of fixture.vectors) {
      if (vector.scope === 'legacy-c4') {
        expect(vector.key_input).toBeNull();
        expect(vector.key_input_jcs).toBeNull();
        expect(createLegacyC4IdempotencyKey(vector.key_fields.legacy_record_id))
          .toBe(vector.idempotency_key);
      } else {
        const keyInput = buildIdempotencyKeyInput(vector.scope, vector.key_fields);
        expect(keyInput).toEqual(vector.key_input);
        expect(canonicalizeJson(keyInput)).toBe(vector.key_input_jcs);
        expect(createIdempotencyKey(vector.scope, vector.key_fields))
          .toBe(vector.idempotency_key);
      }

      const projection = projectPayloadForHash(vector.payload, {
        scope: vector.scope,
        knownFields: vector.known_payload_fields,
        decimalPaths: vector.decimal_paths,
      });
      expect(projection).toEqual(vector.payload_projection);
      expect(canonicalizeJson(projection)).toBe(vector.payload_projection_jcs);
      expect(createPayloadHash(vector.payload, {
        scope: vector.scope,
        knownFields: vector.known_payload_fields,
        decimalPaths: vector.decimal_paths,
      })).toBe(vector.payload_hash);
    }
  });

  test('contains no secret-shaped fields or provider/channel private payload objects', () => {
    const fixture = JSON.parse(readFileSync(
      new URL('../contracts/public/fixtures/idempotency-v1.json', import.meta.url),
      'utf8',
    ));

    expect(validatePublicFixtureSafety(fixture)).toBe(true);
    expect(() => validatePublicFixtureSafety({ provider_payload: { event: 'private' } }))
      .toThrow(ContractKernelError);
    expect(() => validatePublicFixtureSafety({ api_secret: 'redacted' }))
      .toThrow(ContractKernelError);
  });
});

describe('public contract version negotiation', () => {
  test('accepts additive same-major versions for a known contract', () => {
    const result = validateContractHeader({
      contract: 'zylos.inbound-envelope',
      contract_version: '1.7',
    });

    expect(PUBLIC_CONTRACTS).toContain('zylos.inbound-envelope');
    expect(result).toEqual({
      contract: 'zylos.inbound-envelope',
      contractVersion: '1.7',
      major: 1,
      minor: 7,
    });
  });

  test('rejects an unknown major with the unified public error shape', () => {
    expect.assertions(2);

    try {
      validateContractHeader({
        contract: 'zylos.inbound-envelope',
        contract_version: '2.0',
      }, { occurredAt: '2026-07-19T04:00:00Z' });
    } catch (error) {
      expect(error).toBeInstanceOf(ContractKernelError);
      expect(error.contractError).toEqual({
        code: 'unsupported_contract_version',
        category: 'validation',
        retryable: false,
        side_effect_status: 'none',
        user_message: 'Contract zylos.inbound-envelope major version 2 is not supported.',
        occurred_at: '2026-07-19T04:00:00Z',
      });
    }
  });

  test('partitions unknown same-major optional fields for safe ignore or forwarding', () => {
    const payload = {
      contract: 'zylos.normalized-event',
      contract_version: '1.3',
      event_id: 'event-A',
      event_sequence: 1,
      occurred_at: '2026-07-19T04:00:00Z',
      future_optional: { display_hint: 'compact' },
    };

    const result = validateContractDocument(payload, {
      contract: 'zylos.normalized-event',
      fields: {
        event_id: { required: true, kind: 'opaque_id' },
        event_sequence: { required: true, kind: 'number' },
        occurred_at: { required: true, kind: 'rfc3339' },
      },
    });

    expect(result.known).toEqual({
      contract: 'zylos.normalized-event',
      contract_version: '1.3',
      event_id: 'event-A',
      event_sequence: 1,
      occurred_at: '2026-07-19T04:00:00Z',
    });
    expect(result.extensions).toEqual({
      future_optional: { display_hint: 'compact' },
    });
    expect(result.forwarded).toEqual(payload);
  });
});

describe('unified contract errors', () => {
  const validError = {
    code: 'queue_full',
    category: 'capacity',
    retryable: true,
    side_effect_status: 'none',
    user_message: 'The conversation queue is full.',
    detail_ref: 'diagnostic:error-001',
    occurred_at: '2026-07-19T04:00:00Z',
  };

  test('validates the complete shape and rejects unknown security-critical enums', () => {
    expect(validateContractError(validError)).toEqual(validError);

    for (const malformed of [
      { ...validError, category: 'mystery' },
      { ...validError, side_effect_status: 'probably' },
    ]) {
      try {
        validateContractError(malformed, { occurredAt: '2026-07-19T04:00:01Z' });
        throw new Error('expected validation to reject a critical enum');
      } catch (error) {
        expect(error).toBeInstanceOf(ContractKernelError);
        expect(error.contractError.code).toBe('unsupported_capability');
      }
    }
  });

  test('publishes the stable cross-repository v1 error code vocabulary', () => {
    expect(PUBLIC_ERROR_CODES).toEqual(expect.arrayContaining([
      'unsupported_contract_version',
      'unsupported_capability',
      'validation_error',
      'idempotency_key_mismatch',
      'idempotency_conflict',
      'version_conflict',
      'side_effect_unknown',
    ]));
    expect(new Set(PUBLIC_ERROR_CODES).size).toBe(PUBLIC_ERROR_CODES.length);
  });
});

describe('public scalar validation', () => {
  test('keeps opaque IDs case-sensitive and rejects empty or control-character values', () => {
    expect(validateOpaqueId('turn_id', 'Turn-A')).toBe('Turn-A');
    expect(validateOpaqueId('turn_id', 'turn-a')).toBe('turn-a');
    expect(() => validateOpaqueId('turn_id', '')).toThrow(ContractKernelError);
    expect(() => validateOpaqueId('turn_id', 'turn\u0000a')).toThrow(ContractKernelError);
    expect(() => validateOpaqueId('turn_id', 'turn-\ud800')).toThrow(ContractKernelError);
  });

  test('accepts zoned RFC 3339 timestamps and rejects invalid calendar values', () => {
    expect(validateRfc3339Timestamp('occurred_at', '2026-07-19T12:00:00.123456+08:00'))
      .toBe('2026-07-19T12:00:00.123456+08:00');
    expect(validateRfc3339Timestamp('occurred_at', '2016-12-31T23:59:60Z'))
      .toBe('2016-12-31T23:59:60Z');
    expect(() => validateRfc3339Timestamp('occurred_at', '2026-02-30T12:00:00Z'))
      .toThrow(ContractKernelError);
    expect(() => validateRfc3339Timestamp('occurred_at', '2026-07-19T12:00:00'))
      .toThrow(ContractKernelError);
  });

  test('allows safe integers or explicitly declared finite decimals only', () => {
    expect(validatePublicNumber('event_sequence', 42)).toBe(42);
    expect(validatePublicNumber('ratio', 1.25, { allowDecimal: true })).toBe(1.25);
    expect(() => validatePublicNumber('ratio', 1.25)).toThrow(ContractKernelError);
    expect(() => validatePublicNumber('count', Number.MAX_SAFE_INTEGER + 1, { allowDecimal: true }))
      .toThrow(ContractKernelError);
    expect(() => validatePublicNumber('count', Number.NaN)).toThrow(ContractKernelError);
    expect(() => validatePublicNumber('count', Number.POSITIVE_INFINITY))
      .toThrow(ContractKernelError);
    expect(() => validatePublicNumber('count', -0)).toThrow(ContractKernelError);
  });
});
