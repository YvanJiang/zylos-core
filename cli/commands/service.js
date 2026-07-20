/**
 * Executor service lifecycle commands.
 *
 * Core's control socket is authoritative for identity, health, and graceful
 * shutdown. PM2 is only the operating-system process supervisor.
 */

import fs from 'node:fs';
import path from 'node:path';
import { spawn } from 'node:child_process';

import { ZYLOS_DIR } from '../lib/config.js';
import {
  EXECUTOR_SERVICE_NAME,
  getExecutorServiceHealth,
  restartExecutorService,
  startExecutorService,
  stopExecutorService,
} from '../lib/executor-service-lifecycle.js';
import { bold, dim, green, red, yellow, success, error, heading } from '../lib/colors.js';

function reportFailure(prefix, result) {
  console.error(error(`${prefix}: ${result.error ?? 'unknown failure'}`));
  process.exitCode = 1;
  return result;
}

export async function showStatus() {
  console.log(`${heading('Zylos Executor Service')}\n${dim('======================')}\n`);
  const result = await getExecutorServiceHealth({ zylosDir: ZYLOS_DIR });
  if (!result.ok) {
    console.log(`${bold('Health:')} ${red(result.health?.toUpperCase() ?? 'OFFLINE')}`);
    console.log(`  ${dim(`Reason: ${result.error}`)}`);
    console.log(`  ${dim('Run: zylos doctor or zylos start')}`);
    process.exitCode = 1;
    return result;
  }
  const service = result.snapshot.service;
  console.log(`${bold('Health:')} ${green(service.health.toUpperCase())}`);
  console.log(`${bold('Service identity:')} ${service.service_instance_id}`);
  console.log(`${bold('Host identity:')} ${service.host_id}`);
  console.log(`${bold('Started:')} ${service.started_at}`);
  console.log(`${bold('Maintenance:')} ${service.maintenance ? yellow('yes') : 'no'}`);
  console.log(`${bold('Draining:')} ${service.draining ? yellow('yes') : 'no'}`);
  return result;
}

export function showLogs(args) {
  const logType = args[0] || 'executor';
  if (logType === 'pm2') {
    const child = spawn('pm2', ['logs', EXECUTOR_SERVICE_NAME, '--lines', '50'], {
      stdio: 'inherit',
    });
    child.on('close', (code) => { process.exitCode = code ?? 1; });
    return;
  }
  if (logType !== 'executor') {
    console.error(error(`Unknown log type: ${logType}`));
    console.log(dim('Available: executor, pm2'));
    process.exitCode = 1;
    return;
  }
  const logFile = path.join(ZYLOS_DIR, 'logs', 'executor-out.log');
  if (!fs.existsSync(logFile)) {
    console.error(error(`Log file not found: ${logFile}`));
    process.exitCode = 1;
    return;
  }
  const child = spawn('tail', ['-f', '-n', '50', logFile], { stdio: 'inherit' });
  child.on('close', (code) => { process.exitCode = code ?? 1; });
}

export async function startServices() {
  console.log(heading('Starting Zylos executor service...'));
  const result = await startExecutorService({ zylosDir: ZYLOS_DIR });
  if (!result.ok) return reportFailure('Executor service did not become healthy', result);
  console.log(success(`Executor service started: ${result.serviceInstanceId}`));
  return result;
}

export async function stopServices() {
  console.log(heading('Stopping Zylos executor service...'));
  const result = await stopExecutorService({ zylosDir: ZYLOS_DIR });
  if (!result.ok) return reportFailure('Executor service did not confirm shutdown', result);
  console.log(success(`Executor service stopped: ${result.serviceInstanceId}`));
  return result;
}

export async function restartServices() {
  console.log(heading('Restarting Zylos executor service...'));
  const result = await restartExecutorService({ zylosDir: ZYLOS_DIR });
  if (!result.ok) return reportFailure('Executor service restart was not confirmed', result);
  console.log(success(
    `Executor service restarted: ${result.previousServiceInstanceId} → ${result.serviceInstanceId}`,
  ));
  return result;
}
