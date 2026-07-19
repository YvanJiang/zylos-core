# Codex interaction acceptance audit

## Status

Runtime migration issue 10 is **blocked on requirements review**. The current Codex
`exec --json` / `exec resume --json` transport does not expose a bidirectional interaction
protocol, and the adapter does not produce a provider-neutral `interaction_requested` event.
Its fail-closed behavior is a safety boundary, not completion of the interaction acceptance
criterion.

This audit does not change the migration consensus or the GitHub issue acceptance criteria. It
records the conflict against local `codex-cli 0.144.5`, OpenAI Codex source commit
[`0fb559f0f6e231a88ac02ea002d3ecd248e2b515`](https://github.com/openai/codex/tree/0fb559f0f6e231a88ac02ea002d3ecd248e2b515),
and Core after integration commit
`f14aa699a85e034d934c817c0776ae6bff6352a5`.

## Acceptance under audit

Issue 10 requires Codex JSONL to be translated into provider-neutral text, tool, interaction,
state, and error events. Text, tool, state, and error paths are implemented. Interaction is not.

### Adapter input and output paths

The dispatch entry is `acceptJsonlEvent` in
[`runtime/providers/codex-exec-adapter.js`](../runtime/providers/codex-exec-adapter.js):

| JSONL input | Current adapter result | Provider-neutral output path |
|---|---|---|
| `thread.started` | Atomically binds the first provider thread ID | lineage state, not an output event |
| `turn.started` | Advances the private protocol normalizer | executor service authors the canonical `running` state |
| completed `agent_message` item | Builds a descriptor | `text_snapshot` |
| started/updated/completed supported tool item | Builds a descriptor | `tool_started`, `tool_progress`, or `tool_finished` |
| `turn.completed` | Marks the private protocol complete | executor service authors the canonical `completed` state |
| `turn.failed` or top-level `error` | Throws a typed provider failure | executor service authors canonical `failed` state plus public error |
| `interaction.requested` | Throws `provider_interaction_unavailable` | **none**; terminal public error is `unsupported_capability` |
| any other record | Fails closed as an unsupported/invalid protocol record | none |

There is no function that converts an approval, user-input request, or MCP elicitation into an
interaction descriptor. In particular, the `interaction.requested` branch calls `rejectProtocol`
instead of returning `{ kind: 'interaction_requested', ... }`.

### Test reachability

[`test/codex-exec-adapter.test.js`](../test/codex-exec-adapter.test.js) supplies a synthetic
`interaction.requested` record only to assert rejection with `unsupported_capability`. No Codex
adapter test expects an `interaction_requested` descriptor or normalized event. The synthetic
record is also not a published `codex exec --json` event in the fixed CLI version, so this test
proves the safety boundary but not an interaction mapping.

Core does publish provider-neutral interaction contracts independently:

- [`contracts/public/normalized-event.js`](../contracts/public/normalized-event.js) defines
  `interaction_requested`, its fenced provider attribution, and the payload
  `interaction_id`, `ordinal`, `interaction_version`, and `handoff_version`.
- [`contracts/public/fixtures/normalized-event-v1.json`](../contracts/public/fixtures/normalized-event-v1.json)
  demonstrates the valid lifecycle: `running -> waiting_user`, followed by a fenced
  `interaction_requested` event in `waiting_user`.
- [`contracts/public/interaction.js`](../contracts/public/interaction.js) and
  [`contracts/public/fixtures/interaction-handoff-v1.json`](../contracts/public/fixtures/interaction-handoff-v1.json)
  define the durable request, answer, authorization, ordering, runtime fence, and answer-handoff
  shapes.
- Contract and outbox tests validate those supplied fixtures or directly constructed normalized
  events. They do not exercise Codex JSONL as the producer.

The runtime path cannot currently bridge that gap. In
[`runtime/persistence/executor-store.js`](../runtime/persistence/executor-store.js), canonical
executor transitions allow `running` only to `completed` or `failed`, while
`appendAdapterEvent` requires the turn to be `running` and overwrites the descriptor phase with
`running`. Consequently, neither the required `waiting_user` transition nor its subsequent
provider interaction event is reachable through this adapter/service path.

## Fixed-version platform evidence

The local executable reports `codex-cli 0.144.5`. Its help exposes JSONL for `exec` and `resume`
but no interaction request/response channel.

OpenAI's [non-interactive mode documentation](https://developers.openai.com/codex/noninteractive)
lists JSONL top-level events as `thread.started`, `turn.started`, `turn.completed`, `turn.failed`,
`item.*`, and `error`. It lists agent message, reasoning, command, file change, MCP tool call, web
search, and plan item types; it does not list approval, user-input, or elicitation records.

The fixed source confirms that boundary:

- [`exec_events.rs`](https://github.com/openai/codex/blob/0fb559f0f6e231a88ac02ea002d3ecd248e2b515/codex-rs/exec/src/exec_events.rs)
  contains the complete serialized `ThreadEvent` and `ThreadItemDetails` enums. Neither enum has an
  interaction, approval, user-input, or elicitation variant.
- [`event_processor_with_jsonl_output.rs`](https://github.com/openai/codex/blob/0fb559f0f6e231a88ac02ea002d3ecd248e2b515/codex-rs/exec/src/event_processor_with_jsonl_output.rs)
  accepts `ServerNotification`, maps supported items and lifecycle notifications, and leaves other
  notifications on its catch-all running path. It does not receive or serialize bidirectional
  server requests.
- The same source revision models command/file approvals, tool user input, and MCP elicitation as
  bidirectional app-server requests in
  [`ServerRequest.ts`](https://github.com/openai/codex/blob/0fb559f0f6e231a88ac02ea002d3ecd248e2b515/codex-rs/app-server-protocol/schema/typescript/ServerRequest.ts).
  That is evidence that these semantics belong to a different transport; this issue does not
  implement or test that transport.

Therefore the acceptance path is not merely difficult to trigger in real E2E. It does not exist in
the selected transport or in the adapter's deterministic seam.

## Affected semantics

The blocker covers all provider-originated interaction semantics:

- command and file-change approval;
- permission approval;
- provider questions and choices requiring user input;
- MCP elicitation;
- the canonical `waiting_user` transition and durable interaction request;
- returning an authorized answer to the same provider request and resuming execution.

This is an implementation and platform-contract gap, not only a remaining Global 43 real
integration check.

## Requirements-review decisions

1. **Preserve the approved exec/resume baseline and keep issue 10 blocked** until Codex publishes a
   supported bidirectional interaction shape for `exec --json`. This is the recommended current
   decision because it changes neither the baseline nor the acceptance semantics.
2. In a future requirements review, approve a different official bidirectional Codex transport and
   redesign the adapter boundary around its request/response protocol. This requires an explicit
   baseline change and is not attempted here.
3. Explicitly narrow issue 10 acceptance and defer or remove Codex interaction equivalence. This
   changes the agreed product semantics and is not recommended.

Until requirements review chooses and authorizes one of these paths, fail-closed behavior must
remain, but issue 10 must not be reported as completed.
