# c4-send.js — Retired Fail-Closed Interface

All invocations fail closed. Normal outbound messages are durable Core outbox
operations consumed by a channel delivery owner. Internal lifecycle notes are
written to explicit durable lifecycle or memory state, not a hidden global C4
record.
