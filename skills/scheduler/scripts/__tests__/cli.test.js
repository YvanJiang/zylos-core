import assert from 'node:assert/strict';
import { execFileSync, spawnSync } from 'node:child_process';
import { describe, it } from 'node:test';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import Database from 'better-sqlite3';
import { reconcileRunningTasks } from '../daemon-tasks.js';

const observabilityFixtures = JSON.parse(fs.readFileSync(
  new URL('../../../../contracts/public/fixtures/observability-v1.json', import.meta.url),
  'utf8',
));

const CLI_PATH = fileURLToPath(new URL('../cli.js', import.meta.url));

function cli(args, env = {}) {
  return execFileSync('node', [CLI_PATH, ...args], {
    env: { ...process.env, ...env },
    stdio: 'pipe',
    encoding: 'utf8'
  });
}

/** Run CLI and return { stdout, stderr, status } without throwing */
function cliRaw(args, env = {}) {
  const result = spawnSync('node', [CLI_PATH, ...args], {
    env: { ...process.env, ...env },
    encoding: 'utf8'
  });
  return { stdout: result.stdout, stderr: result.stderr, status: result.status };
}

function withTmpDir(fn) {
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'scheduler-cli-'));
  const dbPath = path.join(tmpDir, 'scheduler', 'scheduler.db');
  const env = { ZYLOS_DIR: tmpDir, TZ: 'UTC' };
  try {
    return fn({ tmpDir, dbPath, env });
  } finally {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  }
}

describe('cli add', () => {
  it('creates a cron task with correct timezone column', () => {
    withTmpDir(({ dbPath, env }) => {
      cli(['add', 'test cron task', '--cron', '0 9 * * *'], env);
      const db = new Database(dbPath);
      try {
        const task = db.prepare('SELECT * FROM tasks LIMIT 1').get();
        assert.equal(task.type, 'recurring');
        assert.equal(task.timezone, 'UTC');
        assert.equal(task.cron_expression, '0 9 * * *');
        assert.equal(task.status, 'pending');
        assert.equal(task.priority, 3);
      } finally {
        db.close();
      }
    });
  });

  it('captures the canonical Core scope once when the task is created', () => {
    withTmpDir(({ dbPath, env }) => {
      cli(['add', 'scoped task', '--cron', '0 9 * * *'], {
        ...env,
        ZYLOS_REGION: 'region-task',
        ZYLOS_TENANT_ID: 'tenant-task',
        ZYLOS_BOT_ID: 'bot-task',
      });
      const db = new Database(dbPath);
      try {
        assert.deepEqual(db.prepare(`
          SELECT scope_region, scope_tenant_id, scope_bot_id FROM tasks LIMIT 1
        `).get(), {
          scope_region: 'region-task', scope_tenant_id: 'tenant-task', scope_bot_id: 'bot-task',
        });
      } finally {
        db.close();
      }
    });
  });

  it('creates a one-time task with --in', () => {
    withTmpDir(({ dbPath, env }) => {
      cli(['add', 'remind me', '--in', '30 minutes'], env);
      const db = new Database(dbPath);
      try {
        const task = db.prepare('SELECT * FROM tasks LIMIT 1').get();
        assert.equal(task.type, 'one-time');
        assert.ok(task.next_run_at > Math.floor(Date.now() / 1000));
      } finally {
        db.close();
      }
    });
  });

  it('creates a one-time task with --at', () => {
    withTmpDir(({ dbPath, env }) => {
      cli(['add', 'send report', '--at', 'tomorrow 9am'], env);
      const db = new Database(dbPath);
      try {
        const task = db.prepare('SELECT * FROM tasks LIMIT 1').get();
        assert.equal(task.type, 'one-time');
      } finally {
        db.close();
      }
    });
  });

  it('creates an interval task with --every', () => {
    withTmpDir(({ dbPath, env }) => {
      cli(['add', 'check updates', '--every', '2 hours'], env);
      const db = new Database(dbPath);
      try {
        const task = db.prepare('SELECT * FROM tasks LIMIT 1').get();
        assert.equal(task.type, 'interval');
        assert.ok(task.interval_seconds >= 7190 && task.interval_seconds <= 7210);
      } finally {
        db.close();
      }
    });
  });

  it('sets priority correctly', () => {
    withTmpDir(({ dbPath, env }) => {
      cli(['add', 'urgent task', '--cron', '0 9 * * *', '--priority', '1'], env);
      const db = new Database(dbPath);
      try {
        const task = db.prepare('SELECT priority FROM tasks LIMIT 1').get();
        assert.equal(task.priority, 1);
      } finally {
        db.close();
      }
    });
  });

  for (const args of [
    ['--block-queue-until-idle'],
    ['--require-idle'],
    ['--reply-channel', 'telegram', '--reply-endpoint', '12345'],
  ]) {
    it(`rejects retired add controls: ${args.join(' ')}`, () => {
      withTmpDir(({ dbPath, env }) => {
        const result = cliRaw(['add', 'retired control', '--cron', '0 2 * * *', ...args], env);
        assert.notEqual(result.status, 0);
        assert.match(result.stderr, /retired.*bound-conversation-json/i);
        const db = new Database(dbPath);
        try {
          assert.equal(db.prepare('SELECT COUNT(*) AS count FROM tasks').get().count, 0);
        } finally {
          db.close();
        }
      });
    });
  }

  it('requires a complete canonical bound conversation identity', () => {
    withTmpDir(({ dbPath, env }) => {
      const incomplete = JSON.stringify({ channel: 'lark', chat_id: 'chat-only' });
      const result = cliRaw([
        'add', 'incomplete binding', '--cron', '0 2 * * *',
        '--bound-conversation-json', incomplete,
      ], env);
      assert.notEqual(result.status, 0);
      assert.match(result.stderr, /bound_conversation\.chat_type is required/);
      const db = new Database(dbPath);
      try {
        assert.equal(db.prepare('SELECT COUNT(*) AS count FROM tasks').get().count, 0);
      } finally {
        db.close();
      }
    });
  });

  for (const [label, identity] of [
    ['unknown chat type', {
      channel: 'lark', chat_type: 'broadcast', chat_id: 'chat-bound',
      native_thread_or_topic_id: null, message_id: 'message-bound', root_message_id: null,
    }],
    ['scheduler synthetic identity', {
      channel: 'scheduler', chat_type: 'synthetic', chat_id: 'scheduler:task',
      native_thread_or_topic_id: null, message_id: 'scheduler:message', root_message_id: null,
    }],
    ['native thread on a dm', {
      channel: 'lark', chat_type: 'dm', chat_id: 'chat-bound',
      native_thread_or_topic_id: 'thread-bound', message_id: 'message-bound', root_message_id: null,
    }],
  ]) {
    it(`rejects ${label} as a bound conversation`, () => {
      withTmpDir(({ dbPath, env }) => {
        const result = cliRaw([
          'add', 'invalid binding', '--cron', '0 2 * * *',
          '--bound-conversation-json', JSON.stringify(identity),
        ], env);
        assert.notEqual(result.status, 0);
        const db = new Database(dbPath);
        try {
          assert.equal(db.prepare('SELECT COUNT(*) AS count FROM tasks').get().count, 0);
        } finally {
          db.close();
        }
      });
    });
  }

  it('persists the complete canonical native-thread identity unchanged', () => {
    withTmpDir(({ dbPath, env }) => {
      const identity = {
        channel: 'lark', chat_type: 'thread', chat_id: 'chat-bound',
        native_thread_or_topic_id: 'thread-bound',
        message_id: 'reply-target-bound', root_message_id: 'root-bound',
      };
      cli([
        'add', 'thread report', '--cron', '0 2 * * *',
        '--bound-conversation-json', JSON.stringify(identity),
      ], env);
      const db = new Database(dbPath);
      try {
        const task = db.prepare('SELECT bound_conversation_json FROM tasks').get();
        assert.deepEqual(JSON.parse(task.bound_conversation_json), identity);
      } finally {
        db.close();
      }
    });
  });

  it('sets custom miss_threshold', () => {
    withTmpDir(({ dbPath, env }) => {
      cli(['add', 'backup', '--cron', '0 2 * * *', '--miss-threshold', '86400'], env);
      const db = new Database(dbPath);
      try {
        const task = db.prepare('SELECT miss_threshold FROM tasks LIMIT 1').get();
        assert.equal(task.miss_threshold, 86400);
      } finally {
        db.close();
      }
    });
  });

  it('sets custom name', () => {
    withTmpDir(({ dbPath, env }) => {
      cli(['add', 'the actual prompt', '--cron', '0 9 * * *', '--name', 'my-task'], env);
      const db = new Database(dbPath);
      try {
        const task = db.prepare('SELECT name, prompt FROM tasks LIMIT 1').get();
        assert.equal(task.name, 'my-task');
        assert.equal(task.prompt, 'the actual prompt');
      } finally {
        db.close();
      }
    });
  });

  it('reports error without timing option', () => {
    withTmpDir(({ env }) => {
      const { stderr } = cliRaw(['add', 'no timing'], env);
      assert.ok(stderr.includes('Error') || stderr.includes('Must specify'));
    });
  });

  it('reports error without prompt', () => {
    withTmpDir(({ env }) => {
      const { stderr } = cliRaw(['add', '--cron', '0 9 * * *'], env);
      assert.ok(stderr.includes('Error') || stderr.includes('Prompt'));
    });
  });
});

describe('cli list', () => {
  it('shows empty list message', () => {
    withTmpDir(({ env }) => {
      const output = cli(['list'], env);
      assert.ok(output.includes('No tasks'));
    });
  });

  it('shows tasks with TZ header', () => {
    withTmpDir(({ env }) => {
      cli(['add', 'task one', '--cron', '0 9 * * *'], env);
      const output = cli(['list'], env);
      assert.ok(output.includes('TZ: UTC'));
      assert.ok(output.includes('task one'));
    });
  });
});

describe('cli terminal authority', () => {
  it('fails closed instead of allowing a caller to complete a Core-owned turn', () => {
    withTmpDir(({ dbPath, env }) => {
      cli(['add', 'complete me', '--cron', '0 9 * * *'], env);
      const db = new Database(dbPath);
      try {
        const task = db.prepare('SELECT id FROM tasks LIMIT 1').get();
        const { stderr, stdout } = cliRaw(['done', task.id], env);
        assert.ok(`${stderr}${stdout}`.includes('Unknown command: done'));
        const updated = db.prepare('SELECT status FROM tasks WHERE id = ?').get(task.id);
        assert.equal(updated.status, 'pending');
      } finally {
        db.close();
      }
    });
  });
});

describe('cli pause and resume', () => {
  it('refuses to resume a migrated task until conversation semantics are explicit', () => {
    withTmpDir(({ dbPath, env }) => {
      cli(['add', 'pause me', '--cron', '0 9 * * *'], env);
      const db = new Database(dbPath);
      try {
        const task = db.prepare('SELECT id FROM tasks LIMIT 1').get();
        cli(['pause', task.id], env);
        const paused = db.prepare('SELECT status FROM tasks WHERE id = ?').get(task.id);
        assert.equal(paused.status, 'paused');
        db.prepare(`
          UPDATE tasks SET requires_reconfiguration = 1, last_error = 'migration pause'
          WHERE id = ?
        `).run(task.id);

        const refused = cliRaw(['resume', task.id], env);
        assert.notEqual(refused.status, 0);
        assert.match(refused.stderr, /reconfiguration.*required/i);
        const stillPaused = db.prepare(`
          SELECT status, requires_reconfiguration, last_error FROM tasks WHERE id = ?
        `).get(task.id);
        assert.deepEqual(stillPaused, {
          status: 'paused', requires_reconfiguration: 1, last_error: 'migration pause',
        });

        cli(['update', task.id, '--use-synthetic-conversation'], env);
        cli(['resume', task.id], env);
        assert.deepEqual(db.prepare(`
          SELECT status, requires_reconfiguration, last_error, bound_conversation_json
          FROM tasks WHERE id = ?
        `).get(task.id), {
          status: 'pending', requires_reconfiguration: 0, last_error: null,
          bound_conversation_json: null,
        });
      } finally {
        db.close();
      }
    });
  });

  it('cannot resume a replay-barrier occurrence until its schedule advances', () => {
    withTmpDir(({ dbPath, env }) => {
      cli(['add', 'barrier task', '--cron', '0 9 * * *'], env);
      const db = new Database(dbPath);
      try {
        const task = db.prepare('SELECT id, next_run_at FROM tasks LIMIT 1').get();
        db.prepare(`
          UPDATE tasks SET status = 'paused', requires_reconfiguration = 0,
            requires_occurrence_advance = 1, current_occurrence_id = ?,
            last_error = 'replay barrier' WHERE id = ?
        `).run(`${task.id}:${task.next_run_at}`, task.id);
        const refused = cliRaw(['resume', task.id], env);
        assert.notEqual(refused.status, 0);
        assert.match(refused.stderr, /advance.*schedule.*replay/i);
        cli(['update', task.id, '--in', '30 minutes'], env);
        cli(['resume', task.id], env);
        assert.deepEqual(db.prepare(`
          SELECT status, requires_occurrence_advance FROM tasks WHERE id = ?
        `).get(task.id), { status: 'pending', requires_occurrence_advance: 0 });
      } finally {
        db.close();
      }
    });
  });
});

describe('cli remove', () => {
  it('removes a task', () => {
    withTmpDir(({ dbPath, env }) => {
      cli(['add', 'remove me', '--cron', '0 9 * * *'], env);
      const db = new Database(dbPath);
      try {
        const task = db.prepare('SELECT id FROM tasks LIMIT 1').get();
        cli(['remove', task.id], env);
        const remaining = db.prepare('SELECT COUNT(*) as count FROM tasks').get();
        assert.equal(remaining.count, 0);
      } finally {
        db.close();
      }
    });
  });

  it('reports error for non-existent task', () => {
    withTmpDir(({ env }) => {
      const { stderr } = cliRaw(['remove', 'nonexistent-id'], env);
      assert.ok(stderr.includes('not found'));
    });
  });
});

describe('cli update', () => {
  it('clears the migration fence only after validating a complete bound identity', () => {
    withTmpDir(({ dbPath, env }) => {
      cli(['add', 'migrated task', '--cron', '0 9 * * *'], env);
      const db = new Database(dbPath);
      try {
        const task = db.prepare('SELECT id FROM tasks LIMIT 1').get();
        db.prepare(`
          UPDATE tasks SET status = 'paused', requires_reconfiguration = 1,
            last_error = 'migration pause' WHERE id = ?
        `).run(task.id);
        const identity = {
          channel: 'lark', chat_type: 'thread', chat_id: 'chat-bound',
          native_thread_or_topic_id: 'thread-bound', message_id: 'reply-target-bound',
          root_message_id: 'root-bound',
        };
        cli([
          'update', task.id, '--bound-conversation-json', JSON.stringify(identity),
        ], env);
        assert.deepEqual(db.prepare(`
          SELECT status, requires_reconfiguration, last_error, bound_conversation_json
          FROM tasks WHERE id = ?
        `).get(task.id), {
          status: 'paused', requires_reconfiguration: 0, last_error: null,
          bound_conversation_json: JSON.stringify(identity),
        });
        cli(['resume', task.id], env);
        assert.equal(
          db.prepare('SELECT status FROM tasks WHERE id = ?').get(task.id).status,
          'pending',
        );
      } finally {
        db.close();
      }
    });
  });

  it('rejects conflicting bound and synthetic reconfiguration choices', () => {
    withTmpDir(({ dbPath, env }) => {
      cli(['add', 'ordinary task', '--cron', '0 9 * * *'], env);
      const db = new Database(dbPath);
      try {
        const task = db.prepare('SELECT id, updated_at FROM tasks LIMIT 1').get();
        const identity = {
          channel: 'lark', chat_type: 'dm', chat_id: 'chat-bound',
          native_thread_or_topic_id: null, message_id: 'reply-target-bound',
          root_message_id: null,
        };
        const result = cliRaw([
          'update', task.id,
          '--bound-conversation-json', JSON.stringify(identity),
          '--use-synthetic-conversation',
        ], env);
        assert.notEqual(result.status, 0);
        assert.deepEqual(
          db.prepare('SELECT id, updated_at FROM tasks WHERE id = ?').get(task.id), task,
        );
      } finally {
        db.close();
      }
    });
  });

  it('cannot clear a legacy resume fence while its admitted turn is still running', () => {
    withTmpDir(({ dbPath, env }) => {
      cli(['add', 'running migration', '--cron', '0 9 * * *'], env);
      let db = new Database(dbPath);
      try {
        const task = db.prepare('SELECT id, next_run_at FROM tasks LIMIT 1').get();
        db.prepare(`
          UPDATE tasks SET status = 'running', requires_reconfiguration = 1,
            current_turn_id = 'turn-admitted', last_error = 'migration pause'
          WHERE id = ?
        `).run(task.id);
        const result = cliRaw([
          'update', task.id, '--use-synthetic-conversation',
        ], env);
        assert.notEqual(result.status, 0);
        assert.match(result.stderr, /running.*terminal.*resume/i);
        assert.deepEqual(db.prepare(`
          SELECT status, requires_reconfiguration, last_error
          FROM tasks WHERE id = ?
        `).get(task.id), {
          status: 'running', requires_reconfiguration: 1, last_error: 'migration pause',
        });
        const scheduleResult = cliRaw(['update', task.id, '--in', '30 minutes'], env);
        assert.notEqual(scheduleResult.status, 0);
        assert.match(scheduleResult.stderr, /running.*schedule/i);
        assert.equal(
          db.prepare('SELECT next_run_at FROM tasks WHERE id = ?').get(task.id).next_run_at,
          task.next_run_at,
        );
        const terminal = structuredClone(observabilityFixtures.cases.complete);
        terminal.turns.items[0] = {
          ...terminal.turns.items[0], turn_id: 'turn-admitted',
          state: 'completed', phase: 'completed', error: null,
        };
        assert.deepEqual(reconcileRunningTasks(db, terminal, { now: () => 100 }), {
          pending: 0, terminal: 1, unavailable: 0,
        });
        db.close();
        db = new Database(dbPath);
        assert.deepEqual(db.prepare(`
          SELECT status, requires_reconfiguration, last_core_state
          FROM tasks WHERE id = ?
        `).get(task.id), {
          status: 'paused', requires_reconfiguration: 1, last_core_state: 'completed',
        });
      } finally {
        db.close();
      }
    });
  });

  for (const [label, invalidId] of [
    ['control characters', 'reply\u0007target'],
    ['invalid Unicode', '\ud800'],
  ]) {
    it(`rejects bound identities with ${label} before persistence`, () => {
      withTmpDir(({ dbPath, env }) => {
        const identity = {
          channel: 'lark', chat_type: 'dm', chat_id: 'chat-bound',
          native_thread_or_topic_id: null, message_id: invalidId, root_message_id: null,
        };
        const result = cliRaw([
          'add', 'invalid scalar', '--cron', '0 9 * * *',
          '--bound-conversation-json', JSON.stringify(identity),
        ], env);
        assert.notEqual(result.status, 0);
        const db = new Database(dbPath);
        try {
          assert.equal(db.prepare('SELECT COUNT(*) AS count FROM tasks').get().count, 0);
        } finally {
          db.close();
        }
      });
    });
  }

  it('updates task name', () => {
    withTmpDir(({ dbPath, env }) => {
      cli(['add', 'original', '--cron', '0 9 * * *'], env);
      const db = new Database(dbPath);
      try {
        const task = db.prepare('SELECT id FROM tasks LIMIT 1').get();
        cli(['update', task.id, '--name', 'new-name'], env);
        const updated = db.prepare('SELECT name FROM tasks WHERE id = ?').get(task.id);
        assert.equal(updated.name, 'new-name');
      } finally {
        db.close();
      }
    });
  });

  it('updates priority', () => {
    withTmpDir(({ dbPath, env }) => {
      cli(['add', 'prio task', '--cron', '0 9 * * *'], env);
      const db = new Database(dbPath);
      try {
        const task = db.prepare('SELECT id FROM tasks LIMIT 1').get();
        cli(['update', task.id, '--priority', '1'], env);
        const updated = db.prepare('SELECT priority FROM tasks WHERE id = ?').get(task.id);
        assert.equal(updated.priority, 1);
      } finally {
        db.close();
      }
    });
  });

  it('rejects retired update reply controls without mutating the task', () => {
    withTmpDir(({ dbPath, env }) => {
      cli(['add', 'reply task', '--cron', '0 9 * * *'], env);
      const db = new Database(dbPath);
      try {
        const task = db.prepare('SELECT id, updated_at FROM tasks LIMIT 1').get();
        const result = cliRaw(['update', task.id, '--clear-reply'], env);
        assert.notEqual(result.status, 0);
        assert.match(result.stderr, /retired.*bound-conversation-json/i);
        assert.deepEqual(
          db.prepare('SELECT id, updated_at FROM tasks WHERE id = ?').get(task.id), task,
        );
      } finally {
        db.close();
      }
    });
  });

  it('switches schedule type from cron to interval', () => {
    withTmpDir(({ dbPath, env }) => {
      cli(['add', 'switch type', '--cron', '0 9 * * *'], env);
      const db = new Database(dbPath);
      try {
        const task = db.prepare('SELECT id FROM tasks LIMIT 1').get();
        cli(['update', task.id, '--every', '2 hours'], env);
        const updated = db.prepare('SELECT type, interval_seconds, cron_expression FROM tasks WHERE id = ?').get(task.id);
        assert.equal(updated.type, 'interval');
        assert.ok(updated.interval_seconds >= 7190 && updated.interval_seconds <= 7210);
        assert.equal(updated.cron_expression, null);
      } finally {
        db.close();
      }
    });
  });

  for (const flag of ['--no-block-queue-until-idle', '--no-require-idle']) {
    it(`rejects retired update control ${flag}`, () => {
      withTmpDir(({ dbPath, env }) => {
        cli(['add', 'ordinary task', '--cron', '0 9 * * *'], env);
        const db = new Database(dbPath);
        try {
          const task = db.prepare('SELECT id, updated_at FROM tasks LIMIT 1').get();
          const result = cliRaw(['update', task.id, flag], env);
          assert.notEqual(result.status, 0);
          assert.match(result.stderr, /retired.*bound-conversation-json/i);
          assert.deepEqual(
            db.prepare('SELECT id, updated_at FROM tasks WHERE id = ?').get(task.id), task,
          );
        } finally {
          db.close();
        }
      });
    });
  }

  it('reports error with no update options', () => {
    withTmpDir(({ dbPath, env }) => {
      cli(['add', 'no update', '--cron', '0 9 * * *'], env);
      const db = new Database(dbPath);
      try {
        const task = db.prepare('SELECT id FROM tasks LIMIT 1').get();
        const { stderr } = cliRaw(['update', task.id], env);
        assert.ok(stderr.includes('No updates'));
      } finally {
        db.close();
      }
    });
  });
});

describe('cli history', () => {
  it('shows empty history', () => {
    withTmpDir(({ env }) => {
      const output = cli(['history'], env);
      assert.ok(output.includes('No execution history'));
    });
  });
});

describe('cli next', () => {
  it('shows upcoming tasks', () => {
    withTmpDir(({ env }) => {
      cli(['add', 'upcoming task', '--cron', '0 9 * * *'], env);
      const output = cli(['next'], env);
      assert.ok(output.includes('Upcoming'));
    });
  });

  it('shows empty message when no pending tasks', () => {
    withTmpDir(({ env }) => {
      const output = cli(['next'], env);
      assert.ok(output.includes('No pending'));
    });
  });
});

describe('cli running', () => {
  it('shows safe to compact when no running tasks', () => {
    withTmpDir(({ env }) => {
      const output = cli(['running'], env);
      assert.ok(output.includes('No running') || output.includes('Safe to compact'));
    });
  });
});

describe('cli help', () => {
  it('shows help with --help flag', () => {
    withTmpDir(({ env }) => {
      const output = cli(['--help'], env);
      assert.ok(output.includes('Usage'));
      assert.ok(output.includes('Commands'));
    });
  });

  it('shows help with help command', () => {
    withTmpDir(({ env }) => {
      const output = cli(['help'], env);
      assert.ok(output.includes('Usage'));
    });
  });

  it('shows help and error for unknown command', () => {
    withTmpDir(({ env }) => {
      const { stderr, stdout } = cliRaw(['unknown-command'], env);
      const output = stderr + stdout;
      assert.ok(output.includes('Unknown command') || output.includes('Usage'));
    });
  });
});
