# Zylos public contract kernel

This directory is the versioned, provider-neutral contract kernel shared by Core and its
channel, provider, Dashboard, and Luna consumers. It intentionally contains no provider or
channel runtime objects.

Import the public API from `contracts/public/index.js`; that module is the authoritative export
list and its function declarations are the authoritative signatures.

## Version and scalar validation

- Every public payload has a known `contract` and a `<major>.<minor>` string
  `contract_version`.
- Major `1` is supported. Unknown majors are rejected; unknown optional fields in a known
  same-major payload are split into `extensions` and retained in `forwarded` by
  `validateContractDocument`.
- Use the explicit field rules passed to `validateContractDocument` for opaque IDs,
  RFC 3339 timestamps, safe integers, schema-declared decimals, and safety-critical enums.
- Contract failures use `ContractKernelError.contractError`, whose shape is shared across
  transports. HTTP or RPC status codes do not replace this shape.
- `createContractError` accepts one options object and refuses unknown v1 error codes or an
  otherwise invalid public error shape.

Unknown optional fields are data to preserve, not capabilities to execute. Callers must list
every security, lifecycle, terminal, interaction, and control enum in a `critical_enum` rule.

## Inbound, result, and normalized-event contracts

Issue 02 publishes three portable JSON Schema artifacts under `schemas/` and matching golden
fixtures under `fixtures/`:

| Contract | Authoritative validator | Portable schema | Golden fixture |
|---|---|---|---|
| `zylos.inbound-envelope` | `validateInboundEnvelope` | `inbound-envelope-v1.schema.json` | `inbound-envelope-v1.json` |
| `zylos.inbound-result` | `validateInboundResult` | `inbound-result-v1.schema.json` | `inbound-result-v1.json` |
| `zylos.normalized-event` | `validateNormalizedEvent` | `normalized-event-v1.schema.json` | `normalized-event-v1.json` |

The JSON Schemas describe portable document shape and conditional fields. The JavaScript
validators are authoritative for semantic checks that JSON Schema cannot express safely,
including idempotency-key recomputation, scheduler synthetic identity, result required/null
matrices, event-kind payload rules, and fixture secret safety.

`validateInboundEnvelope` preserves additive same-major top-level extensions while enforcing the
six-part conversation namespace, real thread/topic identity, reply-only mapping fields,
authenticated actor shape, attachments, and the mutually exclusive platform, scheduler, and
legacy sources. A scheduler synthetic conversation uses
`scheduler:<bot_id>:<task_id>` and scheduler-scope idempotency; its required
`bound_conversation` flag does not change that identity. Legacy compatibility alone uses the
exact `legacy-c4:<legacy_record_id>` exception.

`validateInboundResult` enforces the authoritative result matrix for a normal bound turn,
control, pending lineage recovery, persisted queue-full failure, and nullable or persisted
non-queue-full rejection.
`deduplicated=true` does not define a new result shape: the producer must replay the first
business result and commit time, changing only the response trace and deduplication marker.

Use `createNormalizedEventStreamState` plus `admitNormalizedEvent` when consuming a turn stream.
Admission requires continuous `event_sequence`, strictly increasing `turn_version`, current
`attempt_id`/`attempt_no`/`lease_epoch` fencing, and immutable terminal state. A retry may advance
the fence only through `retry_attempt_started` with the next attempt number and a newer lease
epoch. The first admitted event establishes lifecycle state through `turn_state_changed`;
provider output is rejected without a current fenced attempt or before a compatible
`starting`/`running` state. Late events after a terminal transition are rejected with
`turn_terminal`.

Known lifecycle, interaction, retry, recovery, permission, and delivery kinds are fixed exports.
An additive same-major unknown kind is rejected by default. A consumer may pass it through
`unknownProgressKinds` only after capability negotiation and only as opaque, non-terminal,
error-free `starting` or `running` progress. Reserved lifecycle/security prefixes and payload
fields that could change state, permission, interaction, control, terminal, or side-effect
semantics remain rejected.

## Canonicalization and idempotency

`canonicalizeJson` implements the JSON Canonicalization Scheme from
[RFC 8785](https://www.rfc-editor.org/rfc/rfc8785). It returns canonical text;
`canonicalizeJsonBytes` returns the UTF-8 bytes hashed by the idempotency helpers. Public
contract number validation is stricter than JCS itself, so validate/project a contract before
canonicalizing it.

Five standard scopes use `zid:v1:<scope>:<sha256>` keys. Legacy C4 pending migration alone
uses the exact, unhashed `legacy-c4:<legacy_record_id>` form. Consumers must recompute the key
from validated fields with `verifyIdempotencyKey`.

Payload hashes require `knownFields` to list every present field in the current contract and
`extensionFields` to list every present unknown same-major optional field. Unclassified fields
are rejected, which prevents a misspelled or omitted business field from silently disappearing
from the hash. Common transport/diagnostic fields and delivery attempt fields are removed by
the fixed projection rules. Decimal paths must be declared with `decimalPaths`; array items use
the `[]` segment, for example `measurements[].ratio`.

Use `resolveIdempotencyReplay` after persistence lookup:

- the same key and payload hash is `duplicate` and reuses the original result;
- the same key with a different payload hash is `conflict` and must not create side effects.

## Cross-repository golden vectors

`fixtures/idempotency-v1.json` contains raw payloads, explicit optional extensions, key inputs,
canonical JCS strings, keys, payload projections, and payload hashes for inbound, scheduler,
interaction, control, delivery, and legacy C4. Each consuming repository must calculate and
assert these values with its own implementation. Comparing a copied Core result without
recalculation is not a contract test.

The idempotency vectors are hashing-only projections from the issue 01 kernel. Use the issue 02
contract fixtures above—not the intentionally minimal hashing vectors—as document acceptance
fixtures.

Run `validatePublicFixtureSafety` on fixture changes. Fixtures must not contain secrets or raw
provider/channel private objects; only redacted `detail_ref` and `source_ref` references may
point to controlled diagnostics.
