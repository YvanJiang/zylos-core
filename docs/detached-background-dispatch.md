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

## Feishu input groups

Authenticated Feishu `platform_original` user input is collected by Core with
a trailing-edge 10-second quiet window. The grouping key is the Core origin
conversation, authenticated actor, and routing intent. Different actors, native
threads/topics, and explicit reply or lineage intents never share a group. A
supplement without an explicit reply may join only when that actor has exactly
one compatible collecting group in the origin conversation.

Each source message keeps an independent inbound event, idempotency/audit
record, and terminal dispatch turn. The group owns one background task and one
execution turn. Durable group members preserve message boundaries, message ID,
actor, occurrence time, content, and reply metadata. Core materializes provider
input in deterministic occurrence/ordinal order only when claim seals the
group; append and claim/seal are serialized by one SQLite transaction.

The default Core-owned policy is enabled only for `channel: "feishu"`:

- quiet window: 10 seconds;
- maximum open window: 120 seconds;
- maximum members: 20.

The queue row's `available_at` is the durable eligibility barrier. A restart
therefore keeps an overdue group claimable exactly once, while duplicate inbound
delivery replays the stored result without adding a member or moving the
deadline. Tests can inject a narrower policy through the normal acceptance seam.
Permission, interaction, `/stop`, and `/steer` control input bypass collection.

The first acknowledgement card says exactly `已收到`. A supplement updates that
same V2 main card to `已收到 N 条补充`, where `N` is the member count minus one.
Core preserves the original explicit delivery target; neither Core nor a
channel renderer may infer a latest message.

Grouped inbound results add all five optional fields together:

- `input_group_id`;
- `input_group_action` (`opened` or `appended`);
- `input_group_member_count`;
- `input_group_supplement_count`;
- `input_group_collect_until`.

Idempotent replay preserves these values unchanged, apart from the existing
`deduplicated` and trace semantics. Background-task, queue-status, and
observability projections expose `input_settling`, counts, and the collection
deadline while the group remains open.

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

Codex provider-native collaboration remains disabled. Claude/Codex prompts also
must not spawn a provider-native child merely to detach work, because such a
child would bypass Core task identity, control, and recovery ownership.

## Compatibility seam

`acceptQueuedInbound` preserves the prior stable-conversation FIFO contract for
scheduler, one-time migration, focused provider probes, and tests that
intentionally exercise that legacy seam. The C4 channel bridge and new standard
channel ingress call `acceptNormalInbound`.
