import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { test } from '@jest/globals';

test('shipped C4 caller sources select Core ingress and durable outbox ownership', () => {
  const receive = fs.readFileSync(path.resolve('skills/comm-bridge/scripts/c4-receive.js'), 'utf8');
  const send = fs.readFileSync(path.resolve('skills/comm-bridge/scripts/c4-send.js'), 'utf8');
  assert.match(receive, /acceptCompatibilityInbound/);
  assert.doesNotMatch(receive, /agent-status|activity-monitor|am\.sock|reply via|c4-dispatcher|send-keys|paste-buffer/i);
  assert.match(send, /durable Core outbox/);
  assert.doesNotMatch(send, /spawn|scripts\/send\.js|insertConversation\('out',\s*channel/i);
});
