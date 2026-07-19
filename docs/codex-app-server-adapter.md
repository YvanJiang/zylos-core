# Codex app-server adapter evidence

This document records the provider-specific boundary implemented for runtime migration issue 10.
It does not change the provider-neutral Core contracts or the migration consensus.

## Transport boundary

Each executor-service adapter instance supervises one `codex app-server --stdio` child and
multiplexes logical conversation executors over its newline-delimited bidirectional protocol.
The child, connection, in-memory thread, request IDs, and active turn handles are not durable
authority. Core's persisted lineage, turn, attempt, lease, interaction, and handoff records remain
authoritative.

The normal Codex path contains no per-turn CLI child, `exec --json`, `exec resume`, transport
fallback, feature flag, or dual-mode route.

## Fixed-version protocol evidence

The implementation was audited against:

- local `codex-cli 0.144.5`;
- locally generated experimental TypeScript and JSON Schema from
  `codex app-server generate-ts --experimental` and
  `codex app-server generate-json-schema --experimental`;
- the official Codex app-server README and protocol source at commit
  `0fb559f0f6e231a88ac02ea002d3ecd248e2b515`.

The stdio connection performs `initialize`, waits for its response, and then sends `initialized`.
It enables `experimentalApi`, disables request attestation, and advertises
`mcpServerOpenaiFormElicitation=false` because Core has no provider-neutral representation for
that private form. New lineages use `thread/start`; the returned thread ID is durably bound before
`turn/start`. Persisted lineages use `thread/resume` once per new connection before starting a
turn.

## Provider-neutral normalization

Private app-server method and item names remain inside the adapter:

| App-server input | Provider-neutral result |
|---|---|
| fenced `turn/started` | provider-neutral started signal; Core atomically authors `starting -> running` with `provider_started` |
| `item/agentMessage/delta` and completed agent message | `text_delta` and `text_snapshot` |
| command, file, MCP, dynamic, collaboration, web, and image tool lifecycle | `tool_started`, `tool_progress`, `tool_finished` |
| completed turn | adapter iterator completion; Core authors the canonical completed state |
| failed/interrupted turn, error notification, or lost connection | typed provider failure; Core authors the canonical failure or recovery state |
| command/file approval | durable `tool_approval` interaction |
| permissions approval | durable `permission_approval` interaction |
| single-question `requestUserInput` | durable `question` or fixed `choice` interaction with answer constraints preserved |
| single-field required MCP typed string/enum form | durable `question` or `choice` interaction; accepted content is reconstructed as the schema-keyed object |

Answers are accepted only through Core's durable interaction-answer and handoff records. The
adapter verifies the current connection, provider request, thread, turn, Core turn, attempt, lease,
and handoff claim before writing a response. A handoff is acknowledged only after its answer was
written and the matching `serverRequest/resolved` notification arrives. Server request IDs are
never reusable within one connection, including after acknowledgement.

Multi-question, secret, provider-auto-resolving, and fixed-choice-plus-Other `requestUserInput`
requests,
multi-field/non-string/optional/formatted
MCP typed forms, `openai/form`, URL elicitation, unknown server requests, duplicate request IDs,
unsupported item types, and stale or mismatched traffic fail closed. Formatted MCP strings are
rejected because the provider-neutral answer contract cannot preserve or validate the fixed-version
`email|uri|date|date-time` constraint. URL elicitation is rejected because its URL can contain
credentials and the public interaction contract has no safe reference field.
Connection loss before an answer cancels still-pending interactions and moves the turn to
`recovering`. Loss after a response may have been sent is recorded as `delivery_unknown`. Neither
case is automatically replayed. A provider-failure latch rejects an interaction descriptor that
was already removed from the connection but had not yet crossed Core's durable interaction
transaction. Fenced provider error or terminal notifications also drive that durable failure path
when Core is suspended in `waiting_user`; a completed turn with an outstanding server request is
treated as an invalid terminal rather than stranding its interaction. The supervised child's stderr
is drained without persistence and stdio errors fail the fenced connection rather than escaping as
unhandled stream errors. Fatal run-scoped protocol/capability failures retire the shared connection;
a replacement connection is not started until the prior child emits `close`.

## Control and reconnect

Stop, timeout, and steer share the provider-neutral adapter `interrupt` seam and use
`turn/interrupt` for the exact current thread/turn/attempt/lease fence. Timeout retains the writer
lease until the matching provider terminal notification confirms that the turn stopped. A fenced
terminal tombstone covers the race in which that notification wins immediately before the timeout
interrupt lookup. Confirmation is bounded to five seconds; missing or uncertain confirmation
leaves the lease held, persists a `side_effect_unknown` provider-stop incident, and enqueues a
high-priority manual-recovery notice. Turn interrupts do not kill the shared app-server process and
do not discard the persisted lineage. A protocol or stdio failure terminates the lost shared
connection under supervision. Uncertain running work enters `recovering` with its writer lease held,
rather than `failed` with an immediately reusable lease. After the retired child is confirmed closed,
a later safe turn creates a new connection and reloads its persisted thread before use; active work
is never replayed automatically.

## Verification boundary

Deterministic tests inject the child process and stdio streams and cover handshake ordering,
single-process multiplexing, thread binding/resume/reconnect, provider-start and text/tool
normalization, every supported bidirectional interaction family, provider acknowledgement and
request-ID tombstones, bounded and confirmed timeout interruption, terminal race tombstones,
transport loss, and stale request/answer/output fences. The
current app-server protocol returns all questions in
one JSON-RPC response, while Core requires each question to have a unique interaction and forbids
answering the next blocking ordinal before provider acknowledgement of the previous one. Until the
interaction authority defines a batch handoff that preserves both rules, multi-question requests
are rejected instead of fabricating an acknowledgement or collapsing distinct questions.

Real local verification requires an authenticated Codex
installation and exercises only safe read-only prompts and explicit negative/interrupt protocol
paths; approval, user-input, and MCP elicitation require a controlled provider/tool fixture before
they can be asserted end to end without creating external side effects.
