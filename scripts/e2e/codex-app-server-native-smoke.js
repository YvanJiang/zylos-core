#!/usr/bin/env node

import { runCodexAppServerInitializeSmoke } from '../lib/codex-app-server-real-integration.js';

try {
  const evidence = await runCodexAppServerInitializeSmoke();
  process.stdout.write(`ZYLOS_CODEX_APP_SERVER_NATIVE_SMOKE=${JSON.stringify(evidence)}\n`);
} catch (error) {
  process.stderr.write(`Codex app-server native smoke failed: ${error.message}\n`);
  process.exitCode = 1;
}
