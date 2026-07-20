/** Isolated one-time characterization logging for the retired dispatcher. */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const MAX_CAPTURE_LENGTH = 8192;

export function saveTmuxCapture(capture, context) {
  try {
    const directory = path.join(process.env.ZYLOS_DIR || path.join(os.homedir(), 'zylos'), 'activity-monitor');
    fs.mkdirSync(directory, { recursive: true });
    const truncated = capture.length > MAX_CAPTURE_LENGTH
      ? capture.slice(-MAX_CAPTURE_LENGTH)
      : capture;
    fs.appendFileSync(
      path.join(directory, 'tmux-captures.log'),
      `\n[${new Date().toISOString()}] context=${context}\n${truncated}\n`,
    );
  } catch {
    // Best effort in isolated characterization only.
  }
}
