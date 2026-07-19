# Zylos public contract kernel

This directory is the versioned, provider-neutral contract kernel shared by Core and its
channel, provider, Dashboard, and Luna consumers. It intentionally contains no provider or
channel runtime objects.

Import the public API from `contracts/public/index.js`.

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

Payload hashes require an explicit `knownFields` list for the current contract. This removes
unknown optional extensions before hashing. Common transport/diagnostic fields and delivery
attempt fields are removed by the fixed projection rules. Decimal paths must be declared with
`decimalPaths`; array items use the `[]` segment, for example `measurements[].ratio`.

Use `resolveIdempotencyReplay` after persistence lookup:

- the same key and payload hash is `duplicate` and reuses the original result;
- the same key with a different payload hash is `conflict` and must not create side effects.

## Cross-repository golden vectors

`fixtures/idempotency-v1.json` contains raw payloads, key inputs, canonical JCS strings, keys,
payload projections, and payload hashes for inbound, scheduler, interaction, control,
delivery, and legacy C4. Each consuming repository must calculate and assert these values with
its own implementation. Comparing a copied Core result without recalculation is not a contract
test.

Run `validatePublicFixtureSafety` on fixture changes. Fixtures must not contain secrets or raw
provider/channel private objects; only redacted `detail_ref` and `source_ref` references may
point to controlled diagnostics.
