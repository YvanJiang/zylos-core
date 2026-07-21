#!/usr/bin/env node
// Fail-closed tombstone for older installations. Normal shell delivery is
// claimed from the durable Core outbox by the in-process shell channel owner.
console.error('Direct shell sends are disabled; use the Core outbox delivery owner.');
process.exitCode = 2;
