#!/usr/bin/env node

// Fail-closed tombstone for older callers. Every normal reply is a fenced
// Core outbox operation; internal lifecycle notes belong in explicit durable
// memory or lifecycle state, never a hidden global C4 conversation.
console.error('[C4] Direct and record-only sends are retired; use the durable Core outbox delivery owner.');
process.exitCode = 2;
