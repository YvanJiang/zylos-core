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

## Delivery commands, results, and message mappings

`validateDeliveryCommand` is the authoritative v1 schema for Core outbox commands. It validates
the provider-neutral target and render model, recomputes the delivery idempotency key, and
enforces the operation matrix:

- `create_main` and `send_text` have no platform target or predecessor;
- `update_main` names both the exact platform message and predecessor delivery;
- `send_fallback` creates a new mapping and names the failed or exhausted predecessor;
- main-card operations keep the mapping on the same turn, with either a bound lineage or the
  explicitly pending recovery exception.

`validateDeliveryResult` requires the current fenced command. It rejects any result that does
not echo the logical IDs, attempt number, lease epoch, mapping, operation, and aggregate version.
The result required/null matrix distinguishes `delivered`, `retryable_failure`,
`permanent_failure`, and update-only `obsolete`; ambiguous creates remain retryable with
`side_effect_status=unknown` until reconciled.

`validateDeliveryMapping` distinguishes bound, provisional pending, and non-reply mappings.
Pending mappings add a required `reason`, exactly one of
`mapping_missing`, `mapping_corrupt`, `mapping_unbound`, or `provider_lineage_invalid`, and a
mapping bound from recovery retains that reason for auditability. Normal bound and non-reply
mappings follow the fixed v1 mapping shape and may omit `reason`; an explicit null is also
accepted for same-major forward compatibility.
Only `resolveProvisionalMappingBinding` may project the Core-owned `pending` version 1 mapping to
one bound lineage at version 2. Repeating the same lineage is idempotent; a different lineage,
stale version, or non-Core authority cannot mutate the mapping. Persistence still performs the
specification's atomic compare-and-swap; the pure helper defines the cross-repository outcome.

## Cross-repository golden vectors

`fixtures/idempotency-v1.json` contains raw payloads, explicit optional extensions, key inputs,
canonical JCS strings, keys, payload projections, and payload hashes for inbound, scheduler,
interaction, control, delivery, and legacy C4. Each consuming repository must calculate and
assert these values with its own implementation. Comparing a copied Core result without
recalculation is not a contract test.

`fixtures/delivery-mapping-v1.json` contains valid and rejected create/update/text/fallback
commands, every delivery result status with fencing/error/side-effect combinations, mapping
required/null cases, and the pending-to-bound/same-value/different-value authority matrix.
Consumers must validate the documents against their own adapter implementation and may not
infer an update target from the latest chat message.

Run `validatePublicFixtureSafety` on fixture changes. Fixtures must not contain secrets or raw
provider/channel private objects; only redacted `detail_ref` and `source_ref` references may
point to controlled diagnostics.
