import { readFileSync } from 'node:fs';

import { describe, expect, test } from '@jest/globals';

function readSchema(fileName) {
  return JSON.parse(readFileSync(
    new URL(`../contracts/public/schemas/${fileName}`, import.meta.url),
    'utf8',
  ));
}

describe('issue 02 portable v1 JSON Schema artifacts', () => {
  test('publish versioned additive schemas for all three public contracts', () => {
    const schemas = [
      readSchema('inbound-envelope-v1.schema.json'),
      readSchema('inbound-result-v1.schema.json'),
      readSchema('normalized-event-v1.schema.json'),
    ];

    expect(schemas.map((schema) => schema.$id)).toEqual([
      'https://schemas.zylos.ai/public/v1/inbound-envelope.schema.json',
      'https://schemas.zylos.ai/public/v1/inbound-result.schema.json',
      'https://schemas.zylos.ai/public/v1/normalized-event.schema.json',
    ]);
    for (const schema of schemas) {
      expect(schema.$schema).toBe('https://json-schema.org/draft/2020-12/schema');
      expect(schema.type).toBe('object');
      expect(schema.properties.contract_version.pattern).toBe('^1\\.(0|[1-9][0-9]*)$');
      expect(schema.additionalProperties).toBe(true);
    }
  });

  test('encode conditional inbound/result shapes and normalized stream admission metadata', () => {
    const inbound = readSchema('inbound-envelope-v1.schema.json');
    const result = readSchema('inbound-result-v1.schema.json');
    const normalized = readSchema('normalized-event-v1.schema.json');

    expect(inbound.required).toEqual(expect.arrayContaining([
      'native_thread_or_topic_id',
      'actor',
      'content',
      'reply',
      'source',
    ]));
    expect(inbound.allOf).toHaveLength(4);
    expect(result.oneOf).toHaveLength(5);
    expect(normalized.required).toEqual(expect.arrayContaining([
      'event_sequence',
      'turn_version',
      'attempt_id',
      'attempt_no',
      'lease_epoch',
      'kind',
      'phase',
      'error',
    ]));
    expect(normalized['x-zylos-stream-admission']).toEqual({
      event_sequence: 'continuous_from_1',
      turn_version: 'strictly_increasing',
      attempt_fence: ['attempt_id', 'attempt_no', 'lease_epoch'],
      unknown_kind: 'explicit_noncritical_progress_allowlist_only',
      terminal: 'reject_all_late_events',
    });
  });
});
