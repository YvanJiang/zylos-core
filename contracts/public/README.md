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
legacy sources. A system scheduler task uses the synthetic identity
`scheduler:<bot_id>:<task_id>` and scheduler-scope idempotency. A scheduler occurrence with
`bound_conversation: true` instead carries the complete existing channel identity and enters that
conversation's normal FIFO. Legacy compatibility alone uses the exact
`legacy-c4:<legacy_record_id>` exception.

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
  immutability, and late-ack rejection. An authorized terminal disposition after
  `delivery_unknown` remains `cancelled` but must preserve its claim/send evidence, unknown
  side-effect error, and null provider acknowledgement; it cannot be rewritten as an unsent
  cancellation.

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

## Delivery commands, results, and message mappings

`validateDeliveryCommand` is the authoritative v1 schema for Core outbox commands. The portable
shape is published as `schemas/delivery-command-v1.schema.json`; the JavaScript validator remains
authoritative for idempotency recomputation, target capability checks, and the operation matrix.
The public version/target vocabulary is exported as `DELIVERY_COMMAND_CURRENT_VERSION`,
`DELIVERY_COMMAND_VERSIONS`, `DELIVERY_TARGET_FIELDS_V1_0`, and
`DELIVERY_TARGET_FIELDS_V1_1`.

Version 1.0 remains compatible for non-thread delivery. Version 1.1 adds required nullable target
fields `native_thread_root_message_id` and `native_thread_reply_target_message_id`. A 1.1 native
thread requires non-null conversation, root-message, and reply-target anchors; a 1.1 DM, group, or
synthetic target carries both new fields explicitly as null. A 1.0 native-thread `create_main`,
`send_text`, or `send_fallback`, and a 1.1 target with missing or inconsistent anchors, fails closed
with `unsupported_capability`.

`native_thread_or_topic_id` is conversation identity only. Renderers must use exactly
`native_thread_reply_target_message_id` for a platform reply API and use the root message ID to
constrain/audit its native-thread scope. The reply target must differ from the conversation thread
ID. Renderers may not substitute the thread ID, query a latest message, or fall back to the parent
chat. Core derives both delivery anchors during authenticated inbound acceptance, persists them in
the initial lane/outbox transaction, and reuses the durable lane target for update, text
acknowledgement, and final fallback. Any target identity change is a `version_conflict`; delivery
results cannot rewrite the lane target. Runtime schema initialization rekeys a pre-1.1 native-thread
lane once so its v1.0 conversation identity is fenced before an exact `update_main` can resume.

The validator also validates the provider-neutral render model, recomputes the delivery
idempotency key, and enforces the operation matrix:

Immediately before projection or an irreversible send, a delivery owner must transactionally
renew the exact current claim using its owner, epoch, full command snapshot, and snapshot hash.
An expired non-idempotent pre-send claim remains fenced from automatic replay and is published as
provider-neutral outbox `delivery_unknown`, which degrades Core health pending reconciliation.
Only an owner whose durable sink proves exact same-`delivery_id` idempotency may reclaim that
expired claim; replay must return the original effect or fail on conflicting content.
When opening an older database, Core quarantines every in-flight delivery that lacks an immutable
snapshot matching its exact attempt, owner, full command, and hash. The upgrade never creates a
replacement snapshot from that mutable legacy row and never automatically replays its possible
external effect. A current expired claim is reclaimable only from its verified original snapshot;
the next attempt is derived from that snapshot rather than from the mutable outbox projection.

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

The idempotency vectors are hashing-only projections from the issue 01 kernel. Use the issue 02
contract fixtures above—not the intentionally minimal hashing vectors—as document acceptance
fixtures.

`fixtures/interaction-handoff-v1.json` publishes ordered provider-turn questions,
security-control and recovery-control requests; every allowed answer source and answer-result
status; executable request/source and smallest-blocking-ordinal adjudication examples; every
durable handoff state; the complete allowed transition tables; and explicit send-before-ack,
post-send retry prohibition, delivery-unknown, rejected/cancelled, and late-ack examples.
Consumers must treat every omitted state edge as prohibited.

`fixtures/delivery-mapping-v1.json` contains valid and rejected create/update/text/fallback
commands, every delivery result status with fencing/error/side-effect combinations, mapping
required/null cases, and the pending-to-bound/same-value/different-value authority matrix.
Consumers must validate the documents against their own adapter implementation and may not
infer an update target from the latest chat message.

`fixtures/delivery-native-thread-v1.1.json` adds the v1.0 non-thread compatibility case, valid
v1.1 native-thread commands for every delivery operation, and fail-closed vectors for missing,
cross-scope, and legacy native-thread targets. Consumers must recompute the delivery key and must
not treat the conversation thread ID as a platform reply message ID.

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
  `diagnostic_only=true`. The full snapshot, including audit summaries, errors, and extensions,
  is rejected if it contains credential-shaped values, secret fields, or channel/provider-private
  payloads.
  Core publishes this contract from the real runtime store through
  `createRuntimeSnapshotPublisher` (`runtime/observability/snapshot-publisher.js`) and the executor
  service's `publishObservabilitySnapshot()` surface. Durable aggregates are batch-projected from
  one optimistic WAL transaction that allocates the next version only after collection, without
  holding a writer reservation during the reads. If another connection commits first, SQLite
  rejects the stale read-to-write upgrade instead of publishing older state at a newer version.
  Reopening the same service instance continues its persisted sequence. A failed collection is
  replaced by `complete=false` plus the same top-level degraded error, never by an apparently
  successful empty collection.
- `validateControlRequest`, `validateControlResult`, and the two control schema descriptors
  define the registered caller namespace, transport-injected actor/auth context, versioned
  capability grants and scopes, discriminated action targets, mutation CAS, and asynchronous
  `control_result_version`. Request validation requires an action-specific capability grant whose
  declared tenant/bot/aggregate scope covers the trusted auth context and target. This structural
  check does not authorize a request: Core must still resolve the target's authoritative namespace
  and re-check the current policy, grant, revocation, expiry, capability, and scope in the control
  transaction. Every public request and result, including `reason`, errors, and additive
  extensions, is rejected when it contains credential-shaped values, secret fields, or
  provider/channel-private payloads.
  A higher result version may advance only from `accepted` to a terminal status; terminal results
  are immutable. Accepted-to-terminal updates preserve the action-specific intent and audit
  identities and cannot regress the accepted target version. Because `zylos.control-result` does
  not carry
  a duplicate top-level action field, consumers pass the correlated request action as
  `validateControlResult(value, { action })` when target/result shape alone is ambiguous, and pass
  the same context to `resolveControlResultUpdate`; the returned `metadata.action` is diagnostic
  validation metadata and is never forwarded as contract data.
- `validateDashboardRuntimeProjection` and `DASHBOARD_RUNTIME_PROJECTION_V1_SCHEMA` define the
  only Dashboard-to-Luna runtime seam. `resolveDashboardRuntimeProjectionUpdate` handles full
  first payloads, Dashboard instance replacement, sequence duplicates/obsolescence/gaps, and
  full resynchronization. Projection capabilities must state `control=false` and
  `core_direct_access=false`; `supported_fields` is restricted to the schema's declared
  presentation allowlist and cannot advertise control or Core endpoints. Additive capability
  metadata must remain presentation-only, and the full projection is subject to the same public
  secret/private-payload scan. Luna is a read-only Dashboard consumer.

The golden fixtures are `fixtures/observability-v1.json`, `fixtures/control-v1.json`, and
`fixtures/dashboard-runtime-projection-v1.json`. They cover degraded visibility, answer handoff
delivery uncertainty, service-instance replacement, all seven control targets, policy and CAS
metadata, result-version progress, projection gaps/restarts, unsupported major/state behavior,
and the no-control/no-direct-Core Luna boundary. Consumers must validate these payloads with
their own implementation rather than copying Core's validation result.

## Five-repository compatibility release gate

Core owns the single producer/consumer release gate for the five migration repositories. Run it
from this repository before publishing any public contract change:

```bash
npm run test:contracts:five-repo
```

The gate runs the Core contract suites and the focused contract suites in `zylos-feishu`,
`zylos-lark`, `zylos-dashboard`, and `luna-pet` against this checkout's authoritative
`contracts/public` directory. It fails closed unless every repository exits successfully. The
matrix covers ingress, normalized events, interactions, delivery, observability/control, and the
Dashboard-to-Luna projection, including required/null/optional fields, major rejection, additive
same-major compatibility, critical enum rejection, the public error shape, version conflicts, and
independent computation from raw JCS/idempotency fixtures.

By default the four consumers are resolved from the migration workspace's named integration
worktrees. CI or another release checkout can set `ZYLOS_RUNTIME_MIGRATION_WORKSPACE` and override
individual repositories with `ZYLOS_FEISHU_CONTRACT_REPO`, `ZYLOS_LARK_CONTRACT_REPO`,
`ZYLOS_DASHBOARD_CONTRACT_REPO`, and `ZYLOS_LUNA_CONTRACT_REPO`. The gate passes the Core contract
directory through `ZYLOS_CORE_PUBLIC_CONTRACTS_DIR`; consumer tests must recompute values with their
own implementation and must not accept precomputed Core validation results as evidence.

A consumer exit code alone is not compatibility evidence. Each consumer suite must emit one line
containing `ZYLOS_CONTRACT_COMPATIBILITY_EVIDENCE=` followed by JSON containing schema version 1,
its repository name, the exact Core fixture-set SHA-256 computed from the provided directory, the assertions and
flows it exercised, and positive independent computation counts for raw JCS bytes, idempotency
keys, and payload hashes. Missing, stale, malformed, or incomplete evidence makes the whole gate
fail even when every selected test process exits zero. This prevents skipped fixtures, a different
Core checkout, or tests that merely compare Core-precomputed strings from producing a false-green
release decision.
