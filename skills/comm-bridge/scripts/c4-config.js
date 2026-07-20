import path from 'path';
import os from 'os';

export const POLL_INTERVAL_BASE = 1000;
export const POLL_INTERVAL_MAX = 3000;

export const DELIVERY_DELAY_BASE = 200;
export const DELIVERY_DELAY_PER_KB = 100;
export const DELIVERY_DELAY_MAX = 1000;

export const MAX_RETRIES = 2;
export const RETRY_BASE_MS = 500;
export const CONTROL_MAX_RETRIES = 3;
export const CONTROL_RETENTION_DAYS = 7;
export const CONTROL_CLEANUP_INTERVAL_MS = 24 * 60 * 60 * 1000;

export const ENTER_VERIFY_MAX_RETRIES = 3;
export const ENTER_VERIFY_WAIT_MS = 500;

// For legacy require_idle / external block_queue_until_idle messages:
// minimum sustained idle seconds before delivery.
export const REQUIRE_IDLE_MIN_SECONDS = 3;
// For legacy require_idle / external block_queue_until_idle messages:
// allow execution time before dispatching the next message.
export const REQUIRE_IDLE_POST_SEND_HOLD_MS = 5000;
export const REQUIRE_IDLE_EXECUTION_MAX_WAIT_MS = 120000;
export const REQUIRE_IDLE_EXECUTION_POLL_MS = 1000;

export const FILE_SIZE_THRESHOLD = 2048; // bytes
export const CONTENT_PREVIEW_CHARS = 100;

const ZYLOS_DIR = process.env.ZYLOS_DIR || path.join(os.homedir(), 'zylos');

export const DATA_DIR = path.join(ZYLOS_DIR, 'comm-bridge');
export const DB_PATH = path.join(DATA_DIR, 'c4.db');
export const ATTACHMENTS_DIR = path.join(DATA_DIR, 'attachments');
export const SKILLS_DIR = path.join(ZYLOS_DIR, '.claude', 'skills');

// Historical record-query thresholds. These are not runtime health facts.
export const CHECKPOINT_THRESHOLD = 15;
export const SESSION_INIT_RECENT_COUNT = 6;  // max conversations returned by session-init when above threshold
