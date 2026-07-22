#!/usr/bin/env node

import {
  runCodexAppServerRealIntegration,
  writeCodexAppServerRealEvidenceFile,
} from '../lib/codex-app-server-real-integration.js';

try {
  const evidence = await runCodexAppServerRealIntegration();
  if (process.env.ZYLOS_CODEX_APP_SERVER_EVIDENCE_FILE) {
    writeCodexAppServerRealEvidenceFile({
      evidence,
      evidenceFile: process.env.ZYLOS_CODEX_APP_SERVER_EVIDENCE_FILE,
      codexHome: process.env.CODEX_HOME,
    });
  }
  process.stdout.write(`ZYLOS_CODEX_APP_SERVER_REAL_EVIDENCE=${JSON.stringify(evidence)}\n`);
} catch (error) {
  process.stderr.write(`Codex app-server real integration failed: ${error.message}\n`);
  process.exitCode = 1;
}
