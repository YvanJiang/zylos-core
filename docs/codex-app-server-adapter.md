# Codex app-server adapter evidence

This document records the provider-specific boundary implemented for runtime migration issues 10
and 14.
It does not change the provider-neutral Core contracts or the migration consensus.

## Transport boundary

Each executor-service adapter instance supervises one `codex app-server --stdio` process group and
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
| command and file lifecycle | `tool_started`, `tool_progress`, `tool_finished`; item IDs, progress methods, and fixed-version statuses are fenced by type |
| MCP, dynamic, collaboration, web, image, or provider-hook lifecycle | capability failure; these paths are disabled because their fixed execution surface lacks the required synchronous Core fence |
| completed turn | adapter iterator completion; Core authors the canonical completed state |
| failed/interrupted turn, error notification, or lost connection | typed provider failure; Core authors the canonical failure or recovery state |
| fenced token-usage and moderation telemetry | intentionally omitted because the public normalized-event contract has no usage/score event and private provider scores must not escape the adapter |
| command/file approval | one-shot `accept` only after the current durable permission and workspace fences; otherwise a durable `tool_approval` interaction, with the same workspace fence repeated before an approved handoff is sent |
| permissions approval | empty turn-scoped denial followed by capability failure; the adapter never creates a turn/session filesystem or network grant |
| single-question `requestUserInput` | durable `question` or fixed `choice` interaction with answer constraints preserved |
| MCP elicitation or tool execution | disabled and declined because the fixed protocol has no Core-controlled synchronous gate before an MCP tool's external side effects |

User-supplied answers are accepted only through Core's durable interaction-answer and handoff
records. The
adapter verifies the current connection, provider request, thread, turn, Core turn, attempt, lease,
and handoff claim before writing a response. Preparation is side-effect free. Core persists the
exact handoff send-start fence before invoking the prepared one-shot send. A handoff is acknowledged
only after its answer was written and the matching `serverRequest/resolved` notification arrives.
Server request IDs are never reusable within one connection, including after acknowledgement.
The fixed app-server protocol exposes no read-only, idempotent lookup that can prove acceptance for
one prior handoff attempt, so its recovery query truthfully returns `unknown`; Core does not infer
acceptance from connection or in-memory request state and does not resend the answer.

## Workspace access and write fencing

`sandbox: "workspace-write"` is a trusted adapter-construction declaration of the logical access
that Core must serialize; it is not forwarded as the provider sandbox. Core therefore acquires a
writable workspace lease at the normalized configured cwd, while every new, resumed, and started
Codex turn is forced to `read-only` plus `on-request`, with the user reviewer. The turn override is
repeated even after a persisted thread is resumed. `danger-full-access` is rejected at construction,
as is any writable configuration whose approval policy is not `on-request`. A request cannot
self-report read-only access.

Writable execution fails closed unless Core supplies `assertWorkspaceWrite`. Before a one-shot
command/file approval, the adapter passes the exact connection, conversation, Core turn, lineage,
executor instance, provider attempt, durable workspace lease, provider thread/turn/item/approval,
environment, cwd, and bounded write paths back to Core. Core reopens those facts from SQLite and
requires the current executor owner, unexpired executor and workspace leases, matching holder and
epochs, the durably bound provider thread, a null environment, the canonical workspace cwd, and
write paths contained by the workspace root. Only then may the official client return
`{"decision":"accept"}` for that request. It never returns `acceptForSession`, an exec-policy or
network amendment, a session scope, or a filesystem/network permission profile. It declines every
command request with non-null `additionalPermissions` before authorization. A stale or
mismatched approval receives `decline` before the shared connection is retired into the existing
uncertain-recovery boundary.

The run is registered with the shared-connection failure latch before thread load or durable
binding. If the failing fence came from Core persistence, the adapter preserves that failure marker
instead of converting the current run to a generic provider error; the executor service then marks
the expired workspace uncertain, persists the recovery notification, and waits for its delivery
before isolation and ownership release. Other runs affected by retiring the shared connection still
receive the normal provider-loss recovery signal.

App-server provides a blocking pre-action boundary for built-in command and file-change approvals.
The read-only OS sandbox is the enforcement layer that prevents the built-in shell and apply-patch
paths from writing before that response. The adapter also locks each new/resumed thread with empty
MCP and dynamic-tool configuration and disables apps/connectors, plugins, hooks, code mode, browser,
computer use, image generation, web search, collaboration/subagents, JS REPL, tool search, and the
permission-request tools. Both legacy and v2/fanout/collaboration-mode multi-agent feature keys are
disabled so an inherited local configuration cannot reopen a background execution surface. An
unexpected hook, MCP/dynamic/collaboration/web/image item, dynamic tool
server request, permission-profile request, or MCP elicitation is refused and retires the
connection; its item-start notification is only contradiction evidence, never claimed as the
pre-action fence.

The remaining app-server RPCs with independent side effects (`thread/shellCommand`, `command/exec`,
`process/spawn`, `fs/writeFile`, configuration/plugin mutation, direct MCP calls, and their control
methods) are client-initiated APIs. They are unreachable because Zylos exposes no raw RPC surface
and its private sender has an explicit call-site allowlist limited to initialize, thread
start/resume, turn start/interrupt, and the exact server-request responses above.

Multi-question, secret, provider-auto-resolving, and fixed-choice-plus-Other `requestUserInput`
requests, MCP execution and elicitation, unknown server requests, duplicate request IDs,
unsupported item or notification types, incomplete fixed-version request shapes, duplicate or
unfinished tool lifecycles, cross-tool progress, unrenderable approval details, and stale or
mismatched traffic fail closed.
Connection loss before an answer cancels still-pending interactions; a committed handoff whose send
has not started is atomically cancelled with its interaction, audit, projection, and outbox state.
Both paths move the turn to `recovering`. Loss after a response may have been sent is recorded as
`delivery_unknown`. Neither case is automatically replayed. The provider-failure latch covers both
durable-interaction persistence and the window after `turn/start` is written but before its response
arrives, so an uncertain writer lease is retained. Fenced provider error or terminal notifications
also drive that durable failure path when Core is suspended in `waiting_user`; a completed turn with
an outstanding server request or unfinished tool is treated as invalid/uncertain rather than
success. The supervised child's stderr
is drained without persistence and stdio errors fail the fenced connection rather than escaping as
unhandled stream errors. Fatal run-scoped protocol/capability failures retire the shared connection.
On POSIX, the app-server leader is launched in a detached process group; loss of protocol control
signals that exact group with `SIGTERM`, escalates to `SIGKILL`, and waits for both leader `close`
and process-group disappearance. The termination, escalation, and group-observation timers remain
referenced so service-process exit cannot bypass that isolation barrier. A replacement connection
is not started while any member of the prior group can still be observed. JSON-RPC request IDs are
keyed with their protocol type intact, so numeric `1` and string `"1"` cannot share a fence. Each
connection's late-traffic fence collections have a fixed bound; reaching it retires the connection
before any tombstone can be evicted and uncertain active work enters recovery.

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
rather than `failed` with an immediately reusable lease. Process exit proves isolation only after
the supervised group is gone; an iterator return or app-server leader exit alone cannot release
Codex authority. After the retired process group is confirmed gone, a later safe turn creates a new
connection and reloads its persisted thread before use; active work is never replayed automatically.

## Verification boundary

Deterministic tests inject the child process and stdio streams and cover handshake ordering,
single-process multiplexing, thread binding/resume/reconnect, provider-start and text/tool
normalization, every supported bidirectional interaction family, provider acknowledgement and
request-ID tombstones, bounded and confirmed timeout interruption, terminal race tombstones,
transport loss, stale request/answer/output fences, process-group escalation, and the rule that
neither a surviving group nor an iterator return can release recovering authority. The
current app-server protocol returns all questions in
one JSON-RPC response, while Core requires each question to have a unique interaction and forbids
answering the next blocking ordinal before provider acknowledgement of the previous one. Until the
interaction authority defines a batch handoff that preserves both rules, multi-question requests
are rejected instead of fabricating an acknowledgement or collapsing distinct questions.

Real local verification requires an authenticated Codex
installation and exercises only safe read-only prompts and explicit negative/interrupt protocol
paths; approval, user-input, and MCP elicitation require a controlled provider/tool fixture before
they can be asserted end to end without creating external side effects. The fixed target,
prerequisites, machine-readable runner, completed matrix, unrun cases, and residual risks are
recorded in [`codex-app-server-real-integration.md`](./codex-app-server-real-integration.md).
