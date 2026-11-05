/*
 * Produce load: POST /topics/{topic} with a constant arrival rate.
 *
 * Measures the HTTP + JSON conversion + Kafka producer path of the bridge.
 *
 * Environment variables:
 *   BASE_URL             bridge base URL (default http://localhost:8080)
 *   TOPIC                target topic (default load-test)
 *   DURATION             scenario duration (default 60s)
 *   RATE                 produce requests per second (default 500)
 *   RECORDS_PER_REQUEST  records per request body (default 1)
 *   VALUE_SIZE           record value size in bytes (default 1024)
 *   VUS / MAX_VUS        pre-allocated / maximum virtual users (default 50 / 200)
 *
 * Example:
 *   k6 run -e RATE=1000 -e RECORDS_PER_REQUEST=10 scripts/load-test/k6/produce.js
 */

import http from 'k6/http';
import { check } from 'k6';
import { Trend, Rate } from 'k6/metrics';
import { tlsClientOptions } from './lib/client.js';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
const TOPIC = __ENV.TOPIC || 'load-test';
const RECORDS_PER_REQUEST = parseInt(__ENV.RECORDS_PER_REQUEST || '1');
const VALUE_SIZE = parseInt(__ENV.VALUE_SIZE || '1024');

const produceLatency = new Trend('produce_request_duration', true);
const produceErrors = new Rate('produce_errors');

const value = 'x'.repeat(VALUE_SIZE);

export const options = Object.assign({
  discardResponseBodies: true,
  scenarios: {
    produce: {
      executor: 'constant-arrival-rate',
      rate: parseInt(__ENV.RATE || '500'),
      timeUnit: '1s',
      duration: __ENV.DURATION || '60s',
      preAllocatedVUs: parseInt(__ENV.VUS || '50'),
      maxVUs: parseInt(__ENV.MAX_VUS || '200'),
    },
  },
}, tlsClientOptions());

export default function () {
  const records = [];
  for (let i = 0; i < RECORDS_PER_REQUEST; i++) {
    records.push({ value: value });
  }
  const response = http.post(
    `${BASE_URL}/topics/${TOPIC}`,
    JSON.stringify({ records: records }),
    {
      headers: { 'Content-Type': 'application/vnd.kafka.json.v2+json' },
      // the bridge keeps keep-alive connections: reuse them (default behavior)
    });
  produceLatency.add(response.timings.duration);
  produceErrors.add(!check(response, {
    'status 200': (r) => r.status === 200,
  }));
}
