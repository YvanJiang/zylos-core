# Fenced interaction-answer handoff recovery

Core is the durable authority for answer handoff. The executor claims a committed answer under all
four fences: the interaction runtime fence, provider attempt, turn lease epoch, and newly generated
handoff attempt. Provider preparation is side-effect free. Core persists
`last_send_started_at` under that exact fence before it invokes the prepared one-shot send.

The state paths are deliberately asymmetric:

- A preparation failure may enter `retry_wait` only when its typed error proves
  `side_effect_status=none` and is retryable. A retry receives a new handoff attempt ID and number.
- A non-retryable or side-effect-unproven pre-send failure is cancelled without Core send
  evidence, preserves the adapter's honest side-effect status, and moves the turn to `recovering`
  with a durable user-visible recovery event.
- Any failure after send start becomes `delivery_unknown` with the original send evidence,
  unknown side effects, audit, projection, and outbox notification. Core never automatically
  resends that answer.
- Service startup performs only the issue-17-specific reconciliation: a provider handoff with a
  persisted send barrier and an expired exact writer lease becomes `delivery_unknown`. It does not
  claim or replay provider work; broader lease renewal and health reconciliation remain separate.

Neither query reconciliation nor an authorized disposition can begin merely because the notice is
queued. Core requires a delivered outbox result for a materialized projection whose event sequence
includes the exact `interaction_answer_delivery_unknown` event.

Recovery completion is provider-neutral. `reconcileInteractionHandoff` accepts only a read-only,
idempotent proof for the same handoff ID, provider attempt, lease epoch, handoff attempt ID, and
handoff attempt number. Claude Agent SDK and Codex app-server currently expose no such lookup, so
their adapters return an audited `unknown` result and leave the turn recovering.

Otherwise, `resolveInteractionHandoff` requires an injected trusted authorizer for the exact
conversation, turn, handoff, action, and replacement descriptor. It may terminate the uncertain
handoff or, after the prior writer lease is released or expires under the exact old fence,
atomically establish a Core-owned `recovery_control` interaction. That interaction has no provider
runtime fence, leaves the original turn `recovering`, and its answer is acknowledged by Core without
calling either provider adapter. The replacement descriptor and resulting request are retained with
the prior request, handoff, and authorization decision in the audit entry. A late worker
acknowledgement cannot advance either terminalized handoff and is itself recorded as an ignored
diagnostic.

Provider-native request IDs, connection handles, and acknowledgement mechanics remain private to
the Claude and Codex adapters. The executor/store recovery methods expose only Core interaction,
turn, lease, handoff, proof, and authorization facts.
