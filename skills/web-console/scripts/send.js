#!/usr/bin/env node

// Retained only as a fail-closed tombstone for older channel installations.
// Normal delivery is a fenced Core outbox operation consumed by the channel
// owner, never an endpoint/message CLI call.
console.error('Direct web-console sends are disabled; deliver a claimed Core outbox command.');
process.exitCode = 2;
