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
- Use the explicit field rules passed to `validateContractDocument` for every top-level scalar:
  opaque IDs, text, booleans, RFC 3339 timestamps, safe integers, schema-declared decimals, and
  safety-critical enums. A domain validator may mark only an already-validated nested object,
  array, or nullable aggregate as `prevalidated`; it must apply these same scalar helpers and
  critical-enum allowlists at every known nested path before partitioning the document.
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

## Cross-repository golden vectors

`fixtures/idempotency-v1.json` contains raw payloads, explicit optional extensions, key inputs,
canonical JCS strings, keys, payload projections, and payload hashes for inbound, scheduler,
interaction, control, delivery, and legacy C4. Each consuming repository must calculate and
assert these values with its own implementation. Comparing a copied Core result without
recalculation is not a contract test.

Run `validatePublicFixtureSafety` on fixture changes. Fixtures must not contain secrets or raw
provider/channel private objects; only redacted `detail_ref` and `source_ref` references may
point to controlled diagnostics.

## Runtime observability, operations control, and Luna projection

The v1 runtime contract package is exported from the same authoritative `index.js` entrypoint:

Each domain validator applies its nested scalar and safety-critical enum rules first, then delegates
the top-level known/extension/forwarded partition to `validateContractDocument`; the runtime
contracts do not maintain a second compatibility path around the public kernel.

- `validateObservabilitySnapshot` and `OBSERVABILITY_SNAPSHOT_V1_SCHEMA` define Core's full
  replacement snapshot. Every collection carries `complete` and `error`; a partial collection
  can never masquerade as an empty successful collection. `resolveObservabilitySnapshotUpdate`
  applies the `(core_service_instance_id, snapshot_version)` replacement rules. PID, PGID, and
  process start time are present only inside a required `runtime_identity` object with
  `diagnostic_only=true`.
- `validateControlRequest`, `validateControlResult`, and the two control schema descriptors
  define the registered caller namespace, transport-injected actor/auth context, versioned
  capability grants and scopes, discriminated action targets, mutation CAS, and asynchronous
  `control_result_version`. Validation does not authorize a request: Core must still resolve the
  target's authoritative namespace and re-check the current policy, grant, revocation, expiry,
  capability, and scope in the control transaction. Because `zylos.control-result` does not carry
  a duplicate top-level action field, consumers pass the correlated request action as
  `validateControlResult(value, { action })` when target/result shape alone is ambiguous, and pass
  the same context to `resolveControlResultUpdate`; the returned `metadata.action` is diagnostic
  validation metadata and is never forwarded as contract data.
- `validateDashboardRuntimeProjection` and `DASHBOARD_RUNTIME_PROJECTION_V1_SCHEMA` define the
  only Dashboard-to-Luna runtime seam. `resolveDashboardRuntimeProjectionUpdate` handles full
  first payloads, Dashboard instance replacement, sequence duplicates/obsolescence/gaps, and
  full resynchronization. Projection capabilities must state `control=false` and
  `core_direct_access=false`; Luna is a read-only Dashboard consumer.

The golden fixtures are `fixtures/observability-v1.json`, `fixtures/control-v1.json`, and
`fixtures/dashboard-runtime-projection-v1.json`. They cover degraded visibility, answer handoff
delivery uncertainty, service-instance replacement, all seven control targets, policy and CAS
metadata, result-version progress, projection gaps/restarts, unsupported major/state behavior,
and the no-control/no-direct-Core Luna boundary. Consumers must validate these payloads with
their own implementation rather than copying Core's validation result.
