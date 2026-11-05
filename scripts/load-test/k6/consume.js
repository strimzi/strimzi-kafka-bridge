/*
 * Consume load: one consumer per virtual user, polling GET /consumers/{group}/instances/{name}/records.
 *
 * Measures the consumer path of the bridge (polling, conversion, offset handling).
 * Generate messages into the topic with produce.js (or kafka-console-producer) first.
 *
 * Environment variables:
 *   BASE_URL   bridge base URL (default http://localhost:8080)
 *   TOPIC      topic to subscribe (default load-test)
 *   GROUP      consumer group (default load-consumers)
 *   DURATION   scenario duration (default 60s)
 *   RATE       poll requests per second (default 50)
 *   VUS        virtual users, each with its own consumer (default 10)
 *   TIMEOUT_MS long-poll timeout per request (default 1000)
 *   MAX_BYTES  fetch limit per poll (default 1048576)
 *
 * Example:
 *   k6 run -e VUS=10 -e RATE=100 scripts/load-test/k6/consume.js
 */

import http from 'k6/http';
import { check } from 'k6';
import { Trend, Rate } from 'k6/metrics';
import exec from 'k6/execution';
import { tlsClientOptions } from './lib/client.js';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
const TOPIC = __ENV.TOPIC || 'load-test';
const GROUP = __ENV.GROUP || 'load-consumers';
const TIMEOUT_MS = parseInt(__ENV.TIMEOUT_MS || '1000');
const MAX_BYTES = parseInt(__ENV.MAX_BYTES || '1048576');

const pollLatency = new Trend('poll_request_duration', true);
const pollErrors = new Rate('poll_errors');

export const options = Object.assign({
  scenarios: {
    consume: {
      executor: 'constant-arrival-rate',
      rate: parseInt(__ENV.RATE || '50'),
      timeUnit: '1s',
      duration: __ENV.DURATION || '60s',
      preAllocatedVUs: parseInt(__ENV.VUS || '10'),
      maxVUs: parseInt(__ENV.VUS || '10'),
    },
  },
}, tlsClientOptions());

// one consumer per virtual user, created on the first iteration
const consumers = {};

function consumerName() {
  return `load-consumer-${exec.vu.idInTest}`;
}

function ensureConsumer() {
  const vu = exec.vu.idInTest;
  if (consumers[vu]) {
    return;
  }
  const created = http.post(
    `${BASE_URL}/consumers/${GROUP}`,
    JSON.stringify({
      name: consumerName(),
      format: 'json',
      'auto.offset.reset': 'earliest',
      'enable.auto.commit': false,
    }),
    { headers: { 'Content-Type': 'application/vnd.kafka.v2+json' } });
  check(created, { 'consumer created': (r) => r.status === 200 || r.status === 409 });
  const subscribed = http.post(
    `${BASE_URL}/consumers/${GROUP}/instances/${consumerName()}/subscription`,
    JSON.stringify({ topics: [TOPIC] }),
    { headers: { 'Content-Type': 'application/vnd.kafka.v2+json' } });
  check(subscribed, { 'subscribed': (r) => r.status === 204 });
  consumers[vu] = true;
}

export default function () {
  ensureConsumer();
  const response = http.get(
    `${BASE_URL}/consumers/${GROUP}/instances/${consumerName()}/records` +
    `?timeout=${TIMEOUT_MS}&max_bytes=${MAX_BYTES}`,
    {
      // the Accept header must match the consumer format (json)
      headers: { 'Accept': 'application/vnd.kafka.json.v2+json' },
    });
  pollLatency.add(response.timings.duration);
  // 200 with records, or 404 with the "no subscription yet / rebalancing" body on the first polls
  pollErrors.add(!check(response, {
    'status 200 or 404': (r) => r.status === 200 || r.status === 404,
  }));
}

export function teardown() {
  // best-effort cleanup of the consumers of all VUs
  for (let vu = 1; vu <= parseInt(__ENV.VUS || '10'); vu++) {
    http.del(`${BASE_URL}/consumers/${GROUP}/instances/load-consumer-${vu}`);
  }
}
