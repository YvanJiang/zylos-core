# Detached background dispatch

Authenticated `platform_original` ingress no longer makes the user-input
conversation wait for provider execution. `acceptNormalInbound` atomically:

1. commits a terminal dispatch turn in the stable channel conversation;
2. creates a durable `runtime_background_tasks` record;
3. creates a fresh execution conversation, lineage, and queued execution turn;
4. stages the acknowledgement and later result on the original delivery target.

The inbound result remains `zylos.inbound-result@1.0` and adds:

- `dispatch_status: "background_dispatched"`;
- `background_task_id`;
- `background_execution_turn_id`.

These fields are replayed unchanged by inbound idempotency. The execution
conversation is internal; channel identity and delivery targeting remain bound
to the origin conversation.

## Lifecycle and control

`runtime_background_tasks` durably links the dispatch and execution turns. Its
state follows the execution turn, while `side_effect_status` follows the current
provider attempt. The executor service exposes:

- `getBackgroundTask(backgroundTaskId)`;
- `stopBackgroundTask({ background_task_id, stop_id, ... })`.

The service socket exposes the equivalent `get_background_task` and
`stop_background_task` actions. A stop resolves the task identity to the exact
execution conversation and then uses the existing fenced stop linearization.

## Scheduling and safety

The production service host fills up to 20 concurrent run slots instead of
awaiting one long turn before polling again. Each detached task has a distinct
provider conversation, so one provider stream cannot hold the ingress
conversation's executor lease.

Durable queueing is retained behind the dispatch boundary. It is required for
crash recovery, workspace contention, maintenance, and capacity control; it is
not exposed as a foreground wait. Conflicting writable tasks still wait on the
workspace lease. Read-only tasks may run concurrently only when the provider
sandbox proves read-only enforcement. Unknown side effects enter the existing
recovery-notification barrier and are never replayed automatically.

Each internal detached execution conversation also atomically receives a
`requested` Core workspace binding. Filesystem provisioning happens only after
that admission transaction and must reach `ready` before executor claim.
Requested/provisioning bindings wait, while quarantined/failed/retired bindings
fail closed. The origin/channel conversation and the explicit queued
compatibility seam do not receive an independent binding. See
[Detached conversation workspaces](conversation-workspaces.md).

Codex provider-native collaboration remains disabled. Claude/Codex prompts also
must not spawn a provider-native child merely to detach work, because such a
child would bypass Core task identity, control, and recovery ownership.

## Compatibility seam

`acceptQueuedInbound` preserves the prior stable-conversation FIFO contract for
scheduler, one-time migration, focused provider probes, and tests that
intentionally exercise that legacy seam. The C4 channel bridge and new standard
channel ingress call `acceptNormalInbound`.
