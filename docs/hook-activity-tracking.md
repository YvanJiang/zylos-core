# Provider Events and Core Observability

Runtime health is a Core fact, not a terminal-interface inference. Provider
adapters persist normalized turn and tool events while the executor service
owns lifecycle, queue, lease, interaction, recovery, and outbox state.

## Authoritative snapshot

`runtime/observability/snapshot-publisher.js` publishes
`zylos.observability-snapshot@1.0`. Each snapshot identifies one executor
service generation and carries completeness markers for:

- service health, maintenance, draining, and reconciliation;
- per-conversation executor identity, queue length, and wait reason;
- durable turn and interaction state;
- workspace leases;
- delivery retry and dead-letter counts;
- audit summaries.

Consumers must validate the public contract and replace their view by snapshot
version. A degraded or incomplete section is not permission to reconstruct the
missing fact from a provider process, user interface, filesystem mtime, or
model-visible text.

## Event inputs

Claude Agent SDK callbacks and Codex app-server notifications are provider
inputs. Adapters normalize them before durable persistence. Hook events may add
diagnostic context where the provider officially supplies them, but they never
become liveness, idle, completion, process ownership, or delivery authority.

## Consumers

The CLI doctor, service status, scheduler reconciliation, Dashboard, Luna, web
console, and health skills consume the same provider-neutral snapshot. They may
render it differently, but cannot add a second runtime state store.

Scheduler occurrences remain queued during maintenance and remain pending when
turn visibility is degraded. Only a canonical Core terminal turn updates local
scheduler history. Channel delivery state similarly comes from the durable
outbox and delivery results.
