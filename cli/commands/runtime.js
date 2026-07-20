/** Switch the provider used by the long-lived Core executor service. */

import fs from 'node:fs';

import { getZylosConfig, updateZylosConfig, ZYLOS_DIR } from '../lib/config.js';
import { buildInstructionFile, isSplitInstructionsActive } from '../lib/runtime/instruction-builder.js';
import { commandExists } from '../lib/shell-utils.js';
import {
  getExecutorServiceHealth,
  restartExecutorService,
} from '../lib/executor-service-lifecycle.js';
import {
  installClaude,
  installCodex,
  isClaudeAuthenticated,
  isCodexAuthenticated,
  isValidBaseUrl,
  saveApiKey,
  saveApiKeyToEnv,
  saveClaudeBaseUrlToSettingsAndEnv,
  saveSetupToken,
  saveSetupTokenToEnv,
  saveCodexApiKey,
  saveCodexApiKeyToEnv,
  saveCodexBaseUrlToEnv,
  writeCodexConfig,
} from '../lib/runtime-setup.js';

const SUPPORTED_RUNTIMES = Object.freeze(['claude', 'codex']);
const green = (s) => `\x1b[32m${s}\x1b[0m`;
const yellow = (s) => `\x1b[33m${s}\x1b[0m`;
const red = (s) => `\x1b[31m${s}\x1b[0m`;
const bold = (s) => `\x1b[1m${s}\x1b[0m`;
const dim = (s) => `\x1b[2m${s}\x1b[0m`;

export async function runtimeCommand(args, {
  getHealth = getExecutorServiceHealth,
  zylosDir = ZYLOS_DIR,
} = {}) {
  const sub = args[0];
  if (!sub || sub === 'status') {
    const current = await getHealth({ zylosDir });
    if (!current.ok || !SUPPORTED_RUNTIMES.includes(current.provider)) {
      const reason = current.error ?? 'executor_provider_identity_unavailable';
      console.error(`Executor status unavailable: ${reason}`);
      process.exitCode = 1;
      return { ok: false, error: reason };
    }
    console.log(`Active executor provider: ${bold(current.provider)}`);
    console.log(`Core service identity: ${dim(current.serviceInstanceId)}`);
    return {
      ok: true,
      health: current.health,
      provider: current.provider,
      serviceInstanceId: current.serviceInstanceId,
    };
  }
  if (sub === '--help' || sub === '-h' || sub === 'help') return showHelp();
  if (!SUPPORTED_RUNTIMES.includes(sub)) {
    console.error(`Unknown provider: ${sub}`);
    showHelp();
    process.exitCode = 1;
    return { ok: false, error: 'unsupported_provider' };
  }
  return switchRuntime(sub, args.slice(1));
}

export function prepareRuntimeInstruction(target, { zylosDir = ZYLOS_DIR } = {}) {
  const instructionPath = buildInstructionFile(target, { zylosDir });
  return {
    instructionPath,
    pendingMigration: !isSplitInstructionsActive({ zylosDir }) && !fs.existsSync(instructionPath),
  };
}

function applyCredentials(target, creds) {
  if (target === 'claude' && creds.setupToken) {
    if (!saveSetupToken(creds.setupToken)) return false;
    saveSetupTokenToEnv(creds.setupToken);
    return true;
  }
  if (target === 'claude' && creds.apiKey) {
    if (!saveApiKey(creds.apiKey)) return false;
    saveApiKeyToEnv(creds.apiKey);
    return true;
  }
  if (target === 'codex' && creds.apiKey) {
    if (!saveCodexApiKey(creds.apiKey)) return false;
    saveCodexApiKeyToEnv(creds.apiKey);
    return true;
  }
  return false;
}

export function applyBaseUrl(target, baseUrl) {
  if (target === 'claude') return saveClaudeBaseUrlToSettingsAndEnv(baseUrl);
  if (target === 'codex') {
    return saveCodexBaseUrlToEnv(baseUrl)
      && writeCodexConfig(ZYLOS_DIR, { openaiBaseUrl: baseUrl });
  }
  return false;
}

export function parseRuntimeFlags(flags) {
  const apiKeyIdx = flags.indexOf('--save-apikey');
  const setupTokenIdx = flags.indexOf('--save-setup-token');
  const baseUrlIdx = flags.indexOf('--save-base-url');
  return {
    apiKey: apiKeyIdx >= 0 ? flags[apiKeyIdx + 1] : null,
    setupToken: setupTokenIdx >= 0 ? flags[setupTokenIdx + 1] : null,
    baseUrl: baseUrlIdx >= 0 ? flags[baseUrlIdx + 1] : null,
    hasApiKey: apiKeyIdx >= 0,
    hasSetupToken: setupTokenIdx >= 0,
    hasBaseUrl: baseUrlIdx >= 0,
    noValidate: flags.includes('--no-validate'),
  };
}

export function validateRuntimeFlags(target, parsed) {
  if (parsed.hasApiKey && (!parsed.apiKey || parsed.apiKey.startsWith('--'))) {
    return { error: 'Missing value for --save-apikey.', example: `zylos runtime ${target} --save-apikey <key>` };
  }
  if (parsed.hasSetupToken && (!parsed.setupToken || parsed.setupToken.startsWith('--'))) {
    return { error: 'Missing value for --save-setup-token.', example: 'zylos runtime claude --save-setup-token <token>' };
  }
  if (parsed.hasBaseUrl && (!parsed.baseUrl || parsed.baseUrl.startsWith('--'))) {
    return { error: 'Missing value for --save-base-url.', example: `zylos runtime ${target} --save-base-url <url>` };
  }
  if (parsed.baseUrl && !isValidBaseUrl(parsed.baseUrl)) {
    return { error: `Invalid base URL: "${parsed.baseUrl}".`, example: `zylos runtime ${target} --save-base-url https://proxy.example.com` };
  }
  return null;
}

export async function checkRuntimeAuthGate(target, adapter, parsed, {
  log = console.log,
  error = console.error,
  exit = (code) => { process.exitCode = code; },
} = {}) {
  if (parsed.noValidate) {
    log(`Skipping ${bold(target)} authentication check (--no-validate).`);
    return { skipped: true };
  }
  const auth = await adapter.checkAuth();
  if (auth.status === 'success') return { skipped: false, status: 'success' };
  error(red(`${target} authentication check ${auth.status === 'uncertain' ? 'was inconclusive' : 'failed'}.`));
  exit(2);
  return { skipped: false, status: auth.status };
}

function authAdapter(target) {
  return {
    async checkAuth() {
      try {
        const authenticated = target === 'codex'
          ? isCodexAuthenticated()
          : isClaudeAuthenticated();
        return { status: authenticated ? 'success' : 'failure' };
      } catch (error) {
        return { status: 'uncertain', reason: error.message };
      }
    },
  };
}

async function switchRuntime(target, flags) {
  const current = getZylosConfig().runtime ?? 'claude';
  const parsed = parseRuntimeFlags(flags);
  const validation = validateRuntimeFlags(target, parsed);
  if (validation) {
    console.error(red(validation.error));
    console.error(dim(`Example: ${validation.example}`));
    process.exitCode = 1;
    return { ok: false, error: 'invalid_flags' };
  }
  if (current === target && !parsed.apiKey && !parsed.setupToken && !parsed.baseUrl) {
    console.log(`Already using ${bold(target)}.`);
    return { ok: true, unchanged: true, provider: target };
  }

  if (!commandExists(target)) {
    const installed = target === 'codex' ? installCodex() : installClaude();
    if (!installed || !commandExists(target)) {
      console.error(red(`Failed to install ${target} provider prerequisite.`));
      process.exitCode = 1;
      return { ok: false, error: 'provider_install_failed' };
    }
  }
  if ((parsed.apiKey || parsed.setupToken) && !applyCredentials(target, parsed)) {
    console.error(red('Failed to save provider credentials.'));
    process.exitCode = 1;
    return { ok: false, error: 'credential_save_failed' };
  }
  if (parsed.baseUrl && !applyBaseUrl(target, parsed.baseUrl)) {
    console.error(red('Failed to save provider base URL.'));
    process.exitCode = 1;
    return { ok: false, error: 'base_url_save_failed' };
  }
  if (target === 'codex') writeCodexConfig(ZYLOS_DIR);

  const gate = await checkRuntimeAuthGate(target, authAdapter(target), parsed);
  if (!gate.skipped && gate.status !== 'success') return { ok: false, error: `auth_${gate.status}` };

  const instruction = prepareRuntimeInstruction(target);
  if (instruction.pendingMigration) {
    console.error(yellow('Provider switch requires the split-instruction migration first.'));
    process.exitCode = 1;
    return { ok: false, error: 'instruction_migration_required' };
  }

  updateZylosConfig({ runtime: target });
  const restarted = await restartExecutorService({ zylosDir: ZYLOS_DIR });
  if (!restarted.ok) {
    updateZylosConfig({ runtime: current });
    console.error(red(`Executor restart failed; provider configuration restored to ${current}.`));
    process.exitCode = 1;
    return { ok: false, error: restarted.error, rolledBackProvider: current };
  }

  console.log(green(`Executor provider switched to ${bold(target)}.`));
  console.log(dim(`Core identity: ${restarted.serviceInstanceId}`));
  return { ok: true, provider: target, ...restarted };
}

export function showHelp() {
  console.log(`
zylos runtime — select the executor provider

Usage:
  zylos runtime <claude|codex>
  zylos runtime status
  zylos runtime <name> --save-apikey <key>
  zylos runtime <name> --save-base-url <url>
  zylos runtime claude --save-setup-token <token>
  zylos runtime <name> --no-validate

Codex uses the official app-server transport exclusively. Provider switches
take effect only after Core reports a new healthy executor service identity.
`);
}
