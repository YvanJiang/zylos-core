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

## Interaction answer and durable handoff

`interaction.js` is the authoritative v1 contract package for interaction requests, answers,
answer results, and the Core-owned durable answer handoff record. Import its public schema
descriptors, transition tables, and validators through `contracts/public/index.js`:

- `validateInteractionRequest` enforces provider-turn, security-control, and recovery-control
  parent identity; positive `ordinal`; authorized actor/capability subjects; allowed answer
  sources; runtime fencing; and coherent interaction/handoff projections.
- `validateInteractionRequestSequence` enforces unique ordinals that are contiguous from one
  within each turn or control parent. `validateInteractionAnswerAgainstRequest` binds a
  standalone answer to its request, rejects sources outside `allowed_sources`, restricts
  `magic_command_repeat` to permission confirmations, verifies actor/capability membership and
  the Core-resolved request scope, and accepts only the smallest blocking ordinal. Its
  `requestScope` and `actorCapabilities` options must come from Core's authenticated
  conversation/policy state, not from channel payloads.
- `validateInteractionAnswer` validates the provider-neutral answer value, authenticated actor,
  source context, and the recomputed interaction idempotency key.
- `validateInteractionAnswerResult` keeps `accepted` distinct from provider acknowledgement:
  accepted and duplicate results can only expose `answer_committed` with a pending durable
  handoff. `validateInteractionAnswerResultReplay` proves a duplicate preserved the first
  immutable business result while allowing a new response trace.
- `validateInteractionHandoff` validates the durable handoff record and its send/ack evidence.
  `validateInteractionTransition` and `validateInteractionHandoffTransition` enforce the only
  allowed edges, including pre-send retry/cancel guards, delivery-unknown proof, terminal
  immutability, and late-ack rejection.

The handoff is a Core persistence record, not a new transport payload, so it deliberately is not
added to `PUBLIC_CONTRACTS`. Channels consume interaction request/answer/result documents;
Dashboard observes the redacted handoff projection through its separate observability contract.

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

`fixtures/interaction-handoff-v1.json` publishes ordered provider-turn questions,
security-control and recovery-control requests; every allowed answer source and answer-result
status; executable request/source and smallest-blocking-ordinal adjudication examples; every
durable handoff state; the complete allowed transition tables; and explicit send-before-ack,
delivery-unknown, rejected/cancelled, and late-ack examples. Consumers must treat every omitted
state edge as prohibited.

Run `validatePublicFixtureSafety` on fixture changes. Fixtures must not contain secrets or raw
provider/channel private objects; only redacted `detail_ref` and `source_ref` references may
point to controlled diagnostics.
