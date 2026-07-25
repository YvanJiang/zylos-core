#!/usr/bin/env node

import fs from 'node:fs';

import {
  runCodexAppServerRestartResume,
  selectRestartResumeThreadId,
} from '../lib/codex-app-server-real-integration.js';

try {
  const evidenceFile = process.env.ZYLOS_CODEX_APP_SERVER_EVIDENCE_FILE;
  if (typeof evidenceFile !== 'string' || evidenceFile.length === 0) {
    throw new Error('ZYLOS_CODEX_APP_SERVER_EVIDENCE_FILE is required.');
  }
  const firstEvidence = JSON.parse(fs.readFileSync(evidenceFile, 'utf8'));
  const providerNativeId = selectRestartResumeThreadId(firstEvidence);
  const evidence = await runCodexAppServerRestartResume({ providerNativeId });
  process.stdout.write(`ZYLOS_CODEX_APP_SERVER_RESTART_RESUME=${JSON.stringify(evidence)}\n`);
} catch (error) {
  process.stderr.write(`Codex app-server restart resume failed: ${error.message}\n`);
  process.exitCode = 1;
}
