import fs from 'node:fs';
import path from 'node:path';
import { pathToFileURL } from 'node:url';

import {
  createIdempotencyKey,
  validateDeliveryResult,
} from '../../contracts/public/index.js';

export const CHANNEL_FAULT_PROBE_PREFIX = 'ZYLOS_GLOBAL47_CHANNEL_FAULT_PROBE=';

const SUPPORTED = Object.freeze({
  'zylos-feishu': Object.freeze({
    exportName: 'createFeishuDeliveryRenderer',
    region: 'cn',
    channel: 'feishu',
  }),
  'zylos-lark': Object.freeze({
    exportName: 'createLarkDeliveryRenderer',
    region: 'global',
    channel: 'lark',
  }),
});

function fixtureCommand(coreDirectory, name, target) {
  const fixture = JSON.parse(fs.readFileSync(path.join(
    coreDirectory,
    'contracts',
    'public',
    'fixtures',
    'delivery-mapping-v1.json',
  ), 'utf8'));
  const command = structuredClone(
    fixture.command_vectors.find((vector) => vector.name === name)?.document,
  );
  if (!command) throw new TypeError(`missing Core delivery fixture ${name}`);
  command.target.region = target.region;
  command.target.channel = target.channel;
  command.idempotency_key = createIdempotencyKey('delivery', {
    channel: command.target.channel,
    target: command.target,
    delivery_id: command.delivery_id,
  });
  if (command.operation === 'update_main') command.expected_platform_version = null;
  return command;
}

function failureOutcome(scenario) {
  if (scenario === 'transient_delivery') {
    return { success: false, code: 429, retryable: true, sideEffectStatus: 'none' };
  }
  if (scenario === 'permanent_delivery') {
    return { success: false, code: 400, retryable: false, sideEffectStatus: 'none' };
  }
  if (scenario === 'unknown_delivery') return undefined;
  throw new TypeError(`unsupported channel fault scenario ${scenario}`);
}

function assertResult(result, command, expected) {
  validateDeliveryResult(result, { command });
  if (
    result.status !== expected.status
    || result.error?.code !== expected.errorCode
    || result.error?.side_effect_status !== expected.sideEffectStatus
  ) {
    throw new Error('channel renderer returned an unexpected classified result');
  }
}

export async function runChannelDeliveryFaultProbe({
  repository,
  repositoryDirectory,
  coreDirectory,
  scenario,
}) {
  const target = SUPPORTED[repository];
  if (!target) throw new TypeError(`unsupported channel repository ${repository}`);
  for (const [name, value] of Object.entries({ repositoryDirectory, coreDirectory, scenario })) {
    if (typeof value !== 'string' || value.length === 0) {
      throw new TypeError(`${name} must be a non-empty string`);
    }
  }
  const modulePath = path.join(repositoryDirectory, 'src', 'lib', 'delivery-renderer.js');
  if (!fs.existsSync(modulePath)) throw new TypeError('channel delivery renderer is unavailable');
  const channelModule = await import(pathToFileURL(modulePath).href);
  const createRenderer = channelModule[target.exportName];
  if (typeof createRenderer !== 'function') throw new TypeError('channel renderer export is unavailable');

  let platformCalls = 0;
  let latestLookupCalls = 0;
  let parentFallbackCalls = 0;
  let explicitTargetOnly = true;
  const transport = {
    async createCard() {
      platformCalls += 1;
      return failureOutcome(scenario);
    },
    async updateCard(request) {
      platformCalls += 1;
      if (request.messageId !== 'platform-message-A' || Object.hasOwn(request, 'chatId')) {
        explicitTargetOnly = false;
      }
      return { success: true };
    },
    async listMessages() {
      latestLookupCalls += 1;
      throw new Error('latest-message lookup is forbidden');
    },
    async sendToParent() {
      parentFallbackCalls += 1;
      throw new Error('parent-chat fallback is forbidden');
    },
  };
  const renderer = createRenderer({
    transport,
    now: () => '2026-07-22T12:10:00.000Z',
  });

  let result;
  if (scenario === 'restart_exact_target') {
    const command = fixtureCommand(coreDirectory, 'update_main_bound', target);
    result = await renderer.deliver(command);
    if (result.status !== 'delivered' || !explicitTargetOnly) {
      throw new Error('channel restart did not preserve the explicit Core target');
    }
  } else {
    const command = fixtureCommand(coreDirectory, 'create_main_bound', target);
    result = await renderer.deliver(command);
    const expected = scenario === 'permanent_delivery'
      ? { status: 'permanent_failure', errorCode: 'delivery_permanent', sideEffectStatus: 'none' }
      : {
        status: 'retryable_failure',
        errorCode: 'delivery_transient',
        sideEffectStatus: scenario === 'unknown_delivery' ? 'unknown' : 'none',
      };
    assertResult(result, command, expected);
  }

  if (platformCalls !== 1 || latestLookupCalls !== 0 || parentFallbackCalls !== 0) {
    throw new Error('channel fault probe observed an unexpected platform side effect');
  }
  return Object.freeze({
    schema_version: 1,
    repository,
    scenario,
    result_status: result.status,
    error_code: result.error?.code ?? null,
    side_effect_status: result.error?.side_effect_status ?? 'none',
    platform_calls: platformCalls,
    explicit_target_only: explicitTargetOnly,
    latest_lookup_calls: latestLookupCalls,
    parent_fallback_calls: parentFallbackCalls,
  });
}
