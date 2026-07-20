/** Isolated characterization constants for the retired dispatcher only. */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const zylosDir = process.env.ZYLOS_DIR || path.join(os.homedir(), 'zylos');
let config = {};
try {
  config = JSON.parse(fs.readFileSync(path.join(zylosDir, '.zylos', 'config.json'), 'utf8'));
} catch {
  // Historical characterization defaults only; normal runtime never imports this module.
}

export const ACTIVE_RUNTIME = config.runtime === 'codex' ? 'codex' : 'claude';
export const TMUX_SESSION = ACTIVE_RUNTIME === 'codex' ? 'codex-main' : 'claude-main';
export const TMUX_MISSING_WARN_THRESHOLD = 30;
