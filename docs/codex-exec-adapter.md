# Codex exec/resume adapter evidence

This document records the provider-specific compatibility boundary for runtime migration issue 10.
It does not change the provider-neutral Core contracts or the migration consensus.

## Verified CLI surface

Verified locally on 2026-07-19 with `codex-cli 0.144.5` and an authenticated ChatGPT login:

- A new lineage accepts `codex exec --json <prompt>`.
- A persisted lineage accepts `codex exec resume --json <thread_id> <prompt>`.
- `thread.started` is the first JSONL record and carries the stable `thread_id` used by resume.
- A safe read-only live run and resume both exited with status 0 and returned the same thread ID.
- Prompts are placed after the CLI `--` delimiter so option-shaped user text cannot alter policy.

The adapter keeps these arguments and JSONL names private. Core passes only a generic persisted
`provider_native_id`, an atomic binding callback, the public input, and the current attempt fence.
The child receives only an explicit environment-variable allowlist; the executor service's complete
ambient environment is never forwarded.

## Interaction incompatibility

Structured interaction normalization is paused for this transport. The current `codex exec --json`
schema exposes thread and turn lifecycle records plus message, reasoning, command, file-change, MCP,
collaboration, web-search, plan, and error items. It does not expose approval requests, user-input
requests, or MCP elicitation as JSONL items. Current Codex source handles server requests inside the
exec process instead of forwarding them through the JSONL event processor.

An `item.completed` error can be a non-fatal diagnostic (this occurred during the local live probe),
so it remains private when the turn subsequently completes. A top-level `error`, `turn.failed`, or
unsuccessful process exit becomes Core's fenced `failed` state with the public error shape.

The adapter therefore does not manufacture `interaction_requested`. If a future CLI starts emitting
an unrecognized interaction record, this version fails closed with `unsupported_capability`. Support
must return to requirements review once Codex publishes a transport shape that can preserve the Core
interaction contract, ordering, and handoff semantics.

References:

- [Official Codex non-interactive JSONL documentation](https://developers.openai.com/codex/noninteractive)
- [Official Codex exec JSONL event definitions at `0fb559f0`](https://github.com/openai/codex/blob/0fb559f0f6e231a88ac02ea002d3ecd248e2b515/codex-rs/exec/src/exec_events.rs)
- [Official Codex JSONL event processor at `0fb559f0`](https://github.com/openai/codex/blob/0fb559f0f6e231a88ac02ea002d3ecd248e2b515/codex-rs/exec/src/event_processor_with_jsonl_output.rs)
