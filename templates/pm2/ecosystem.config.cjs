const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { execFileSync } = require('node:child_process');

const ZYLOS_DIR = process.env.ZYLOS_DIR || path.join(os.homedir(), 'zylos');

function resolvePackageRoot() {
  if (process.env.ZYLOS_PACKAGE_ROOT) return path.resolve(process.env.ZYLOS_PACKAGE_ROOT);
  try {
    const executable = execFileSync('sh', ['-lc', 'command -v zylos'], {
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'ignore'],
    }).trim();
    if (!executable) throw new Error('zylos executable not found');
    return path.dirname(path.dirname(fs.realpathSync(executable)));
  } catch {
    throw new Error('Cannot resolve the installed zylos package for executor service startup.');
  }
}

const PACKAGE_ROOT = resolvePackageRoot();
const EXECUTOR_ENTRY = path.join(PACKAGE_ROOT, 'runtime', 'executor', 'launcher.js');
if (!fs.existsSync(EXECUTOR_ENTRY)) {
  throw new Error(`Executor service entrypoint is missing: ${EXECUTOR_ENTRY}`);
}

module.exports = {
  apps: [{
    name: 'zylos-executor',
    script: EXECUTOR_ENTRY,
    interpreter: 'none',
    cwd: ZYLOS_DIR,
    env: {
      NODE_ENV: 'production',
      ZYLOS_DIR,
      ZYLOS_EXECUTOR_IGNORE_ACTIVE_RELEASE: process.env.ZYLOS_EXECUTOR_IGNORE_ACTIVE_RELEASE || '',
      OPENAI_API_KEY: process.env.OPENAI_API_KEY || '',
      CODEX_API_KEY: process.env.CODEX_API_KEY || '',
      OPENAI_BASE_URL: process.env.OPENAI_BASE_URL || '',
      CODEX_PROVIDER_BASE_URL: process.env.CODEX_PROVIDER_BASE_URL || '',
      CODEX_NETWORK_ACCESS: process.env.CODEX_NETWORK_ACCESS || '',
      CODEX_APPROVAL_POLICY: process.env.CODEX_APPROVAL_POLICY || '',
      CODEX_SANDBOX_MODE: process.env.CODEX_SANDBOX_MODE || '',
      CODEX_PROVIDER_TURN_TIMEOUT_MS: process.env.CODEX_PROVIDER_TURN_TIMEOUT_MS || '',
      ZYLOS_PROVIDER_TURN_TIMEOUT_MS: process.env.ZYLOS_PROVIDER_TURN_TIMEOUT_MS || '',
    },
    autorestart: true,
    max_restarts: 10,
    min_uptime: '10s',
    kill_timeout: 30_000,
    error_file: path.join(ZYLOS_DIR, 'logs', 'executor-error.log'),
    out_file: path.join(ZYLOS_DIR, 'logs', 'executor-out.log'),
    log_date_format: 'YYYY-MM-DD HH:mm:ss',
  }],
};
