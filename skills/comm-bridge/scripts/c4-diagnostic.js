/**
 * C4 Diagnostic Logging Utilities
 * Shared diagnostic writer for repository-only migration hooks and record tools.
 *
 * Log files are stored under the provider-neutral runtime diagnostics path.
 */

import fs from 'fs';
import path from 'path';
import os from 'os';

const ZYLOS_DIR = process.env.ZYLOS_DIR || path.join(os.homedir(), 'zylos');
const DIAG_DIR = path.join(ZYLOS_DIR, 'runtime', 'diagnostics');

const MAX_LOG_SIZE = 100 * 1024; // 100KB — rotate when exceeded
const KEEP_RATIO = 0.5;          // Keep last 50% of lines after rotation

/**
 * Ensure diagnostic directory exists.
 */
function ensureDir() {
  if (!fs.existsSync(DIAG_DIR)) {
    fs.mkdirSync(DIAG_DIR, { recursive: true });
  }
}

/**
 * Rotate a log file if it exceeds MAX_LOG_SIZE.
 * Keeps the last KEEP_RATIO of lines.
 */
function rotateIfNeeded(filePath) {
  try {
    if (!fs.existsSync(filePath)) return;
    const stats = fs.statSync(filePath);
    if (stats.size < MAX_LOG_SIZE) return;
    const content = fs.readFileSync(filePath, 'utf8');
    const lines = content.split('\n');
    const keepFrom = Math.floor(lines.length * (1 - KEEP_RATIO));
    fs.writeFileSync(filePath, lines.slice(keepFrom).join('\n'));
  } catch {
    // Best effort — don't break caller on rotation failure
  }
}

/**
 * Log hook execution timing.
 * @param {string} hookName - e.g. 'session-start-prompt', 'c4-session-init'
 * @param {number} durationMs - execution time in milliseconds
 */
export function logHookTiming(hookName, durationMs) {
  try {
    ensureDir();
    const filePath = path.join(DIAG_DIR, 'hook-timing.log');
    const ts = new Date().toISOString().replace('T', ' ').substring(0, 19);
    fs.appendFileSync(filePath, `[${ts}] hook=${hookName} duration=${durationMs}ms\n`);
    rotateIfNeeded(filePath);
  } catch {
    // Best effort
  }
}

/**
 * Log C4 delivery failure.
 * @param {string} itemType - 'control' or 'conversation'
 * @param {number|string} itemId - message/control ID
 * @param {string} reason - failure reason
 * @param {object} [extra] - optional extra context
 */
export function logDeliveryFailure(itemType, itemId, reason, extra = {}) {
  try {
    ensureDir();
    const filePath = path.join(DIAG_DIR, 'delivery-failures.log');
    const ts = new Date().toISOString().replace('T', ' ').substring(0, 19);
    const extraStr = Object.keys(extra).length > 0
      ? ' ' + Object.entries(extra).map(([k, v]) => `${k}=${v}`).join(' ')
      : '';
    fs.appendFileSync(filePath, `[${ts}] type=${itemType} id=${itemId} reason=${reason}${extraStr}\n`);
    rotateIfNeeded(filePath);
  } catch {
    // Best effort
  }
}
