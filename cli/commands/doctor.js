/**
 * Diagnose and reconcile the installed executor service.
 *
 * Core observability is the only runtime-health authority. Process-manager and
 * provider checks are prerequisites, never substitutes for Core health.
 */

import fs from 'node:fs';
import path from 'node:path';

import { ZYLOS_DIR, CONFIG_DIR, getZylosConfig } from '../lib/config.js';
import { commandExists } from '../lib/shell-utils.js';
import { isClaudeAuthenticated, isCodexAuthenticated } from '../lib/runtime-setup.js';
import {
  getExecutorServiceHealth,
  selfHealExecutorService,
} from '../lib/executor-service-lifecycle.js';
import { bold, dim, green, red, yellow, heading } from '../lib/colors.js';

function providerTransport(provider) {
  return provider === 'codex' ? 'official_app_server' : 'claude_agent_sdk';
}

export function buildExecutorDoctorReport({
  initialized,
  pm2Installed,
  provider,
  providerInstalled,
  providerAuthStatus,
  coreHealth,
}) {
  const issues = [];
  if (!initialized) {
    issues.push({ id: 'not_initialized', label: 'Zylos is not initialized', hint: 'Run: zylos init' });
  }
  if (!pm2Installed) {
    issues.push({ id: 'pm2_missing', label: 'PM2 is not installed', hint: 'Run: zylos init' });
  }
  if (!providerInstalled) {
    issues.push({
      id: 'provider_missing',
      label: `${provider} provider prerequisite is missing`,
      hint: 'Run: zylos init',
    });
  }
  if (providerInstalled && providerAuthStatus !== 'success') {
    issues.push({
      id: 'provider_auth',
      label: `${provider} authentication is ${providerAuthStatus}`,
      hint: 'Run: zylos init',
    });
  }
  if (!coreHealth.ok) {
    issues.push({
      id: 'executor_offline',
      label: 'Core executor service is unavailable',
      hint: 'Run: zylos doctor to reconcile or zylos start',
    });
  }

  const service = coreHealth.ok ? {
    health: coreHealth.health,
    service_instance_id: coreHealth.serviceInstanceId,
    host_id: coreHealth.snapshot.service.host_id,
    started_at: coreHealth.snapshot.service.started_at,
    error: null,
  } : {
    health: coreHealth.health ?? 'offline',
    service_instance_id: null,
    host_id: null,
    started_at: null,
    error: coreHealth.error,
  };
  return Object.freeze({
    passed: issues.length === 0,
    initialized,
    supervisor: { name: 'pm2', ready: pm2Installed },
    provider: {
      name: provider,
      transport: providerTransport(provider),
      installed: providerInstalled,
      auth_status: providerAuthStatus,
      ready: providerInstalled && providerAuthStatus === 'success',
    },
    service,
    issues: Object.freeze(issues),
  });
}

async function collectReport() {
  const provider = getZylosConfig().runtime === 'codex' ? 'codex' : 'claude';
  const providerInstalled = commandExists(provider);
  let providerAuthStatus = 'failure';
  if (providerInstalled) {
    try {
      providerAuthStatus = (
        provider === 'codex' ? isCodexAuthenticated() : isClaudeAuthenticated()
      ) ? 'success' : 'failure';
    } catch {
      providerAuthStatus = 'uncertain';
    }
  }
  const initialized = fs.existsSync(CONFIG_DIR)
    && fs.existsSync(path.join(ZYLOS_DIR, 'pm2', 'ecosystem.config.cjs'));
  const coreHealth = initialized
    ? await getExecutorServiceHealth({ zylosDir: ZYLOS_DIR })
    : { ok: false, error: 'not_initialized' };
  return buildExecutorDoctorReport({
    initialized,
    pm2Installed: commandExists('pm2'),
    provider,
    providerInstalled,
    providerAuthStatus,
    coreHealth,
  });
}

function display(report) {
  console.log(`\n${heading('Zylos Executor Doctor')}\n`);
  const mark = (ready) => (ready ? green('✓') : red('✗'));
  console.log(`  ${mark(report.initialized)} installation`);
  console.log(`  ${mark(report.supervisor.ready)} PM2 supervisor`);
  console.log(`  ${mark(report.provider.ready)} ${report.provider.name} prerequisite (${report.provider.transport})`);
  console.log(`  ${mark(report.service.health === 'healthy')} Core executor: ${report.service.health}`);
  if (report.service.service_instance_id) {
    console.log(`    ${dim(`identity: ${report.service.service_instance_id}`)}`);
    console.log(`    ${dim(`host: ${report.service.host_id}`)}`);
  }
  if (report.issues.length > 0) {
    console.log(`\n${yellow(bold('Issues'))}`);
    for (const issue of report.issues) {
      console.log(`  ${red('•')} ${issue.label}`);
      console.log(`    ${dim(issue.hint)}`);
    }
  } else {
    console.log(`\n${green('✓ Everything is working.')}\n`);
  }
}

export async function doctorCommand(args) {
  const jsonMode = args.includes('--json');
  const checkOnly = args.includes('--check');
  let report = await collectReport();

  if (!report.passed && !checkOnly && report.initialized && report.supervisor.ready
    && report.provider.ready && report.service.health === 'offline') {
    const repair = await selfHealExecutorService({ zylosDir: ZYLOS_DIR });
    if (repair.ok) report = await collectReport();
  }

  if (jsonMode) console.log(JSON.stringify(report, null, 2));
  else display(report);
  process.exitCode = report.passed ? 0 : 1;
  return report;
}
