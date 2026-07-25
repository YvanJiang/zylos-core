#!/usr/bin/env node

import { runCodexAppServerRealInteractionProbe } from '../lib/codex-app-server-real-integration.js';

try {
  const evidence = await runCodexAppServerRealInteractionProbe({
    scenario: process.argv[2] ?? process.env.ZYLOS_CODEX_INTERACTION_SCENARIO,
  });
  process.stdout.write(`ZYLOS_CODEX_APP_SERVER_INTERACTION=${JSON.stringify(evidence)}\n`);
} catch (error) {
  process.stderr.write(`Codex app-server interaction probe failed: ${error.message}\n`);
  process.exitCode = 1;
}
