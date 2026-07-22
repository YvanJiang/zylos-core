# Cross-system fault-injection acceptance

The Global47 gate runs deterministic fault injection through the public Core and channel seams,
then runs the five-repository public-contract gate against exact integration baselines. It never
uses PM2, a real provider process, a real tenant, or a real channel target. Every database,
workspace, process shim, and socket used by the selected tests is disposable.

```bash
npm run test:acceptance:cross-system-fault-injection
```

The command must run from a clean Core candidate descended directly from
`b19b7e9cbb30ca9da25bcb049c8b7f0b6fa5a907`. It resolves the four consumers from the named
integration worktrees and requires their exact SHAs:

| Repository | Required SHA |
|---|---|
| Core | merge-base `b19b7e9cbb30ca9da25bcb049c8b7f0b6fa5a907` |
| Dashboard | `6142782de860990d30376942d32d5fcfa385bf7a` |
| Feishu | `e46b75d32057f79514c1a5c0fdea927b0e258263` |
| Lark | `92b86f3c5ba1e2ea6f6edb36e6ed0f6cc9a8c99a` |
| Luna | `59ce7e9b43539c63423c1f09c43eeaac06e5c6f5` |

The consensus identity is fixed at
`57a67b0a359172924bb923aa5422398efb28e473988546e859b5e08521eac800`.

Use `ZYLOS_FEISHU_FAULT_REPO`, `ZYLOS_LARK_FAULT_REPO`,
`ZYLOS_DASHBOARD_FAULT_REPO`, and `ZYLOS_LUNA_FAULT_REPO` only to point at other clean worktrees
with those exact SHAs. A different consumer SHA fails closed.

## Injected matrix

The runner selects public-seam tests and channel renderers for these 25 cases:

- executor, channel, and service restart;
- stale provider, delivery, control, and lease results;
- orphan runtime and answer send-before-ack;
- SQLite writer busy, pinned-reader WAL checkpoint busy, and scheduler busy;
- same-root multi-conversation writer contention and durable background work;
- provider network, rate-limit, authentication, and context faults;
- Feishu and Lark transient, permanent, and unknown delivery;
- expired outbox replay.

Each declared case has five required observations: classification, user notification,
recovery/fallback, audit/metrics, and backlog drain. A selected child test contributes only its
exit code and SHA-256 hashes of stdout/stderr to the final record. Channel probes additionally
emit a bounded, identifier-free observation containing the result class, side-effect class,
platform-call count, and proofs that neither latest-message lookup nor parent-chat fallback was
used. Raw child logs are not copied into evidence.

The SQLite probes use the production scheduler admission and runtime schema. The writer-busy case
holds `BEGIN IMMEDIATE`, proves the failed transaction left zero inbound, turn, scheduler-audit,
and outbox rows, releases the lock, then proves one accepted occurrence and one idempotent replay.
The checkpoint case pins a WAL reader, observes `busy=1`, releases it, and proves a truncating
checkpoint drains to `busy=0, log=0, checkpointed=0` without losing the queued turn or receipt.

## Machine evidence

Success emits exactly one line beginning with:

```text
ZYLOS_GLOBAL47_FAULT_INJECTION_EVIDENCE=
```

The JSON record contains repository heads/merge-bases/clean state, all 25 case records, hashed
probe outputs, safe channel observations, exact tool versions, and five consumer-owned contract
records. Contract records are accepted only when each consumer emits the current Core fixture
SHA and positive independent raw-JCS, idempotency-key, and payload-hash counts.

Any failed probe, dirty or stale repository, missing case/dimension, incomplete contract record,
or credential/identity-shaped evidence field or secret-shaped evidence value fails the command.
The selected public-seam probes cover unknown-side-effect replay, duplicate execution, and
latest/parent reply fallback. The repository's separate release gate covers retired runtime
reachability and Codex exec/resume or dual transport; run it before packaging:

```bash
npm run test:release:retired-runtime
```

## Deterministic versus real evidence

The committed runner produces deterministic/native evidence only. It does not claim a real
provider or real-platform PASS. The evidence has separate Claude, Codex, Feishu, and Lark lanes;
without a Global47-specific controlled fault fixture and disposable authorized targets, those
lanes remain `unavailable` and `release_ready` remains `false` even when the deterministic matrix
passes.

Earlier Global43, Global44, or Global45 waivers and success records are not imported. A future
live run must use isolated provider configuration, dedicated disposable tenant/bot/operator
surfaces, bounded cleanup, and Global47-specific evidence. It must not write credentials, tenant
or bot identities, chat/thread/message IDs, operator identities, or content into logs or evidence.
An unknown side effect remains a recovery barrier: deliver the notice first, require read-only
proof or an explicitly authorized disposition, and never resend automatically.
