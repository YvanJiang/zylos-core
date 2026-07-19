# Fenced interaction-answer handoff recovery

Core is the durable authority for answer handoff. The executor claims a committed answer under all
four fences: the interaction runtime fence, provider attempt, turn lease epoch, and newly generated
handoff attempt. Provider preparation is side-effect free. Core persists
`last_send_started_at` under that exact fence before it invokes the prepared one-shot send.

The state paths are deliberately asymmetric:

- A preparation failure may enter `retry_wait` only when its typed error proves
  `side_effect_status=none` and is retryable. A retry receives a new handoff attempt ID and number.
- A non-retryable pre-send failure is cancelled without send evidence and moves the turn to
  `recovering` with a durable user-visible recovery event.
- Any failure after send start becomes `delivery_unknown` with the original send evidence,
  unknown side effects, audit, projection, and outbox notification. Core never automatically
  resends that answer.

Recovery completion is provider-neutral. `reconcileInteractionHandoff` accepts only a read-only,
idempotent proof for the same handoff ID, provider attempt, lease epoch, handoff attempt ID, and
handoff attempt number. Claude Agent SDK and Codex app-server currently expose no such lookup, so
their adapters return an audited `unknown` result and leave the turn recovering.

Otherwise, `resolveInteractionHandoff` requires an injected trusted authorizer for the exact
conversation, turn, handoff, action, and replacement scope. It may terminate the uncertain handoff
or supersede it with an already-durable newer pending interaction under the same runtime fence.
Supersession never fabricates a provider request and cannot select an older, answered, foreign, or
stale-fence interaction. Both dispositions preserve the prior request and handoff records plus the
authorization decision in the audit entry. A late worker acknowledgement cannot advance either
terminalized handoff.

Provider-native request IDs, connection handles, and acknowledgement mechanics remain private to
the Claude and Codex adapters. The executor/store recovery methods expose only Core interaction,
turn, lease, handoff, proof, and authorization facts.
