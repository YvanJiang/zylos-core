# Public Contract Kernel

This directory defines the provider-neutral public contract helpers for the
runtime migration baseline.

## Scope

- Validate `contract` and `contract_version` headers.
- Enforce public ID, time, number, and safety-critical enum rules.
- Produce the unified public `error` shape.
- Canonicalize payloads with RFC 8785 JSON Canonicalization Scheme (JCS).
- Build idempotency keys and payload hashes for the five public scopes plus the
  legacy C4 bridge.

## Versioning

- Known contracts are listed in `constants.js`.
- Unknown major versions are rejected.
- Same-major future optional extensions are accepted and forwarded unchanged.
- Safety-critical enums remain closed even when optional extensions are present.

## Public API

- `validateContractHeaders(headers)`
- `validateContractDocument(document, options)`
- `validateContractError(error)`
- `validateOpaqueId(value, fieldName)`
- `validateRfc3339Timestamp(value, fieldName)`
- `validatePublicNumber(value, fieldName, options)`
- `validateSafetyCriticalEnum(value, allowedValues, fieldName)`
- `createContractError(code, message, options)`
- `canonicalizeJson(value)`
- `canonicalizeJsonToUtf8(value)`
- `createIdempotencyKey(scope, fields)`
- `verifyIdempotencyKey(scope, fields, candidateKey)`
- `createPayloadHash(payload, options)`
- `resolveIdempotencyReplay(previousHash, nextHash)`

## Fixture Goldens

`fixtures/idempotency-v1.json` is the source of truth for:

- inbound
- scheduler
- interaction
- control
- delivery
- legacy-c4

Each vector locks both the canonical key input and the canonical payload
projection, then records the expected SHA-256 digests. Consumers should reuse
the same semantics instead of reinterpreting the rules locally.

## Payload Hash Rules

- Only known payload fields participate in the hash.
- Same-major optional extensions are forwarded by validation, but excluded from
  the compatibility hash until a future contract revision promotes them.
- Transport and retry metadata such as headers, signatures, cookies,
  `trace_id`, `received_at`, and retry timestamps are excluded.
- Numbers used for hashing must already satisfy the public numeric subset.

## Fixture Safety

Fixtures must not contain secrets or raw provider/channel-private objects. Use
normalized public fields only.
