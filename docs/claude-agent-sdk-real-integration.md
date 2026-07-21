# Claude Agent SDK real-integration acceptance

Global42 qualifies the official `@anthropic-ai/claude-agent-sdk` `0.3.215`
package and its bundled Claude Code `2.1.215` executable. This is the only
Claude target accepted by this record. The normal runtime remains the long-lived
SDK `query()` transport; this harness does not add a CLI fallback, per-turn
subprocess, cross-conversation provider runtime, terminal window, PID authority,
or retired-runtime path.

The upstream target is the
[TypeScript SDK v0.3.215 release](https://github.com/anthropics/claude-agent-sdk-typescript/releases/tag/v0.3.215).
Its packaged `package.json`, platform executable, runtime query controls, and
declared `interrupt_receipt_v1` capability are probed before live execution.
An SDK, bundled-CLI, capability, or control-surface drift fails closed and needs
a new requirements review.

## Command and evidence

Run from the `zylos-core` repository root:

```sh
npm run test:integration:claude-sdk
```

The command emits one machine-readable
`ZYLOS_CLAUDE_SDK_ACCEPTANCE_EVIDENCE=<json>` record. The package command uses
`--require-live`; unavailable credentials or network leave
`release_ready=false` and return a non-zero exit. The verifier never converts an unavailable real-provider lane into a pass. It does not print credential values.

The live lane requires one of:

- `ANTHROPIC_API_KEY`, `ANTHROPIC_AUTH_TOKEN`, or `CLAUDE_CODE_OAUTH_TOKEN`; or
- a valid native login that the pinned executable can still read when
  `CLAUDE_CONFIG_DIR` is redirected to the isolated acceptance directory.

The prerequisite evidence reports original-config and isolated-config native
login status separately. If the original login is visible but the isolated one
is not, the run stays unavailable; provide one of the environment credentials
instead. The harness never copies credential files into its temporary state.

It also requires outbound access to `api.anthropic.com` (or the configured
`ANTHROPIC_BASE_URL` origin), Node.js 20.20 or newer, the installed platform
optional package, and the repository skill dependencies installed by the npm
command. `ZYLOS_CLAUDE_SDK_LIVE_MODEL` can select the model;
`ZYLOS_CLAUDE_SDK_LIVE_MAX_BUDGET_USD` defaults to `0.50`. Each live scenario has
a 180-second execution bound and uses an isolated temporary workspace, Claude
config/transcript directory, and SQLite database. On timeout it aborts, forces
every observed SDK query closed, and waits up to 15 seconds each for close and
scenario cleanup before any later scenario can begin.

## Evidence lanes

Real-provider evidence uses the official SDK through the production Core seam:
authenticated inbound acceptance, durable executor service, Claude conversation
adapter, SQLite state, and outbox. It proves:

- one resident query receives asynchronous turns and binds its first native
  session ID durably;
- idle eviction rebuilds from that ID, and a closed/reopened service and database
  resume the same lineage;
- an SDK MCP tool permission becomes a durable provider-neutral interaction,
  resumes only after the durable answer handoff, and executes exactly once;
- stop reaches the real SDK interrupt path while the permission is pending, and
  the cancelled interaction cannot execute its tool.

Deterministic fault evidence remains separate because provider auth, context,
transient classification, stale-attempt fencing, mapping recovery, and deliberate
commit/ack failure points cannot be safely or repeatably forced in a hosted model.
The focused files prove new-attempt retry bounds, auth/context non-retry, stale
callback suppression, recovery notice delivery before provider work, answer
send-before-ack fencing, `delivery_unknown`, and no blind replay of an unknown
side effect. The acceptance matrix and exact test names live in
`scripts/lib/claude-sdk-real-integration.js`.

## SDK-specific residual risk

The exact 0.3.215 runtime object exposes `cancelAsyncMessage`, but the public
`Query` TypeScript surface does not declare it. Core uses that control only for a
UUID-stamped queued message and probes it at acceptance time. Any SDK upgrade or
removal is therefore a requirements-review blocker, not a fallback trigger.

Hosted-model behavior can still vary by model, account policy, rate limit, and
service state. A credential-free run is useful deterministic and native-target
evidence only; all six real-provider cases remain explicitly unrun until the
machine-readable record reports `release_ready=true`.
