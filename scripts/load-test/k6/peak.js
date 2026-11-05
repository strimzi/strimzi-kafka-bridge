/*
 * Peak load: stepped produce load profile - baseline -> linear ramp to the peak ->
 * peak hold -> drop back to the baseline (recovery) - in a single k6 run.
 *
 * Finds where the bridge saturates (latency ramp and error onset during the ramp,
 * rejected tasks at the executor queue) and verifies that it recovers cleanly once
 * the offered load returns to the baseline (errors back to 0, latency back to the
 * baseline level).
 *
 * Each phase is its own scenario with its own metrics, so the end-of-run summary
 * shows the per-phase latency directly:
 *
 *   peak_baseline_duration / peak_baseline_errors   the baseline before the ramp
 *   peak_ramp_duration / peak_ramp_errors           the linear ramp to the peak
 *   peak_hold_duration / peak_hold_errors           the hold at the peak rate
 *   peak_recovery_duration / peak_recovery_errors   the drop back to the baseline
 *
 * The saturation signals to watch during/after the run (on the bridge /metrics):
 *   executor_rejected_tasks_total > 0        executor queue full, requests rejected
 *   executor_queue_remaining_tasks at/near 0  requests waiting for executor threads
 *   http.server requests + latency           Vert.x-side pressure
 *
 * Environment variables:
 *   BASE_URL              bridge base URL (default http://localhost:8080)
 *   TOPIC                 target topic (default load-test)
 *   BASE_RATE             produce requests per second before and after the peak (default 100)
 *   PEAK_RATE             produce requests per second at the peak (default 1000)
 *   BASELINE_TIME         duration of the initial baseline (default 30s)
 *   RAMP_TIME             duration of the linear ramp from BASE_RATE to PEAK_RATE (default 60s)
 *   PEAK_TIME             duration of the hold at PEAK_RATE (default 30s)
 *   RECOVERY_TIME         duration of the recovery at BASE_RATE after the peak (default 30s)
 *   RECORDS_PER_REQUEST   records per produce request body (default 1)
 *   VALUE_SIZE            record value size in bytes (default 1024)
 *   VUS / MAX_VUS         pre-allocated / maximum virtual users per scenario (default 100 / 500)
 *   AUTH_SCHEME           none (default) | bearer | basic | dpop - authentication of the
 *                         produce requests (requires the OAuth/Basic bridge configuration)
 *   USERS                 number of distinct simulated users (see auth-passthrough.js)
 *   TOKEN_ROTATION_MARGIN seconds subtracted from the token lifetime when reusing it
 *                         (default 5; equal to the token lifetime for a fresh token per request)
 *
 * Example (100/s baseline, ramp to 1000/s, hold 30s, recover):
 *   k6 run -e BASE_RATE=100 -e PEAK_RATE=1000 scripts/load-test/k6/peak.js
 */

import http from 'k6/http';
import { check } from 'k6';
import { Trend, Rate, Counter } from 'k6/metrics';
import exec from 'k6/execution';
import encoding from 'k6/encoding';
import { dpopProof } from './lib/dpop.js';
import { tlsClientOptions } from './lib/client.js';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
const AUTH_SCHEME = __ENV.AUTH_SCHEME || 'none';
const TOPIC = __ENV.TOPIC || 'load-test';
const RECORDS_PER_REQUEST = parseInt(__ENV.RECORDS_PER_REQUEST || '1');
const VALUE_SIZE = parseInt(__ENV.VALUE_SIZE || '1024');

const baseRate = parseInt(__ENV.BASE_RATE || '100');
const peakRate = parseInt(__ENV.PEAK_RATE || '1000');
const baselineTime = parseInt(__ENV.BASELINE_TIME || '30');
const rampTime = parseInt(__ENV.RAMP_TIME || '60');
const peakTime = parseInt(__ENV.PEAK_TIME || '30');
const recoveryTime = parseInt(__ENV.RECOVERY_TIME || '30');

// authentication (same contract as auth-passthrough.js)
const USERS = parseInt(__ENV.USERS || '1');
const KEYCLOAK_TOKEN_URL = __ENV.KEYCLOAK_TOKEN_URL
  || 'http://localhost:8180/realms/loadtest/protocol/openid-connect/token';
const CLIENT_ID = __ENV.CLIENT_ID || 'kafka-broker';
const CLIENT_SECRET = __ENV.CLIENT_SECRET || 'kafka-broker-secret';
const BASIC_USERNAME = __ENV.BASIC_USERNAME || 'kafka-broker';
const BASIC_PASSWORD = __ENV.BASIC_PASSWORD || 'kafka-broker-secret';
const TOKEN_ROTATION_MARGIN = parseInt(__ENV.TOKEN_ROTATION_MARGIN || '5');

// one Trend + Rate per phase: the end-of-run summary shows the per-phase latency
// and error rate directly (the ramp phase is where the saturation point shows)
const phaseDurations = {
  baseline: new Trend('peak_baseline_duration', true),
  ramp: new Trend('peak_ramp_duration', true),
  hold: new Trend('peak_hold_duration', true),
  recovery: new Trend('peak_recovery_duration', true),
};
const phaseErrors = {
  baseline: new Rate('peak_baseline_errors'),
  ramp: new Rate('peak_ramp_errors'),
  hold: new Rate('peak_hold_errors'),
  recovery: new Rate('peak_recovery_errors'),
};
const tokenFetches = new Counter('peak_token_fetches');

const value = 'x'.repeat(VALUE_SIZE);

function scenario(name, executor, startTime) {
  return Object.assign({
    [name]: Object.assign({
      executor: executor,
      timeUnit: '1s',
      startTime: `${startTime}s`,
      exec: `phase_${name}`,
      preAllocatedVUs: parseInt(__ENV.VUS || '100'),
      maxVUs: parseInt(__ENV.MAX_VUS || '500'),
    }, executor === 'ramping-arrival-rate'
      ? { startRate: baseRate, stages: [{ duration: `${rampTime}s`, target: peakRate }] }
      : { rate: name === 'hold' ? peakRate : baseRate, duration: `${{ baseline: baselineTime, hold: peakTime, recovery: recoveryTime }[name]}s` }),
  });
}

export const options = Object.assign({
  // response bodies cannot be discarded: the token endpoint response is parsed
  discardResponseBodies: false,
  scenarios: Object.assign(
    scenario('baseline', 'constant-arrival-rate', 0),
    scenario('ramp', 'ramping-arrival-rate', baselineTime),
    scenario('hold', 'constant-arrival-rate', baselineTime + rampTime),
    scenario('recovery', 'constant-arrival-rate', baselineTime + rampTime + peakTime),
  ),
}, tlsClientOptions());

// per-VU credentials: the user index derives from the VU id (VUs of the same
// user share the credentials but keep their own token cache). The safe modulo
// handles the teardown VU (id 0).
function userIndex() {
  return (((exec.vu.idInTest - 1) % USERS) + USERS) % USERS;
}

function clientId() {
  return USERS <= 1 ? CLIENT_ID : `loadtest-user-${userIndex() + 1}`;
}

function clientSecret() {
  return USERS <= 1 ? CLIENT_SECRET : 'loadtest-user-secret';
}

function basicCredentials() {
  if (USERS <= 1) {
    return encoding.b64encode(`${BASIC_USERNAME}:${BASIC_PASSWORD}`);
  }
  return encoding.b64encode(`loadtest-user-${userIndex() + 1}:loadtest-password`);
}

// token cache, with a controlled rotation margin (each VU fetches and rotates its own token)
let token = null;
let tokenValidUntil = 0;

function getToken() {
  const now = Date.now();
  if (token !== null && now < tokenValidUntil) {
    return token;
  }
  const response = http.post(
    KEYCLOAK_TOKEN_URL,
    `grant_type=client_credentials&client_id=${clientId()}&client_secret=${clientSecret()}`,
    { headers: { 'Content-Type': 'application/x-www-form-urlencoded' } });
  check(response, { 'token fetched': (r) => r.status === 200 });
  tokenFetches.add(1);
  token = response.json('access_token');
  const expiresIn = response.json('expires_in') || 60;
  tokenValidUntil = now + Math.max(0, expiresIn - TOKEN_ROTATION_MARGIN) * 1000;
  return token;
}

function phase(name) {
  const records = [];
  for (let i = 0; i < RECORDS_PER_REQUEST; i++) {
    records.push({ value: value });
  }
  const requestUrl = `${BASE_URL}/topics/${TOPIC}`;
  const headers = { 'Content-Type': 'application/vnd.kafka.json.v2+json' };
  if (AUTH_SCHEME === 'basic') {
    headers['Authorization'] = `Basic ${basicCredentials()}`;
  } else if (AUTH_SCHEME === 'dpop') {
    // a sender-constrained proof per request (the bridge rejects replayed proofs)
    headers['Authorization'] = `DPoP ${getToken()}`;
    headers['DPoP'] = dpopProof('POST', requestUrl);
  } else if (AUTH_SCHEME === 'bearer') {
    headers['Authorization'] = `Bearer ${getToken()}`;
  }
  const response = http.post(
    requestUrl,
    JSON.stringify({ records: records }),
    { tags: { phase: name }, headers: headers });
  phaseDurations[name].add(response.timings.duration);
  phaseErrors[name].add(!check(response, {
    'status 200': (r) => r.status === 200,
  }));
}

export function phase_baseline() { phase('baseline'); }
export function phase_ramp() { phase('ramp'); }
export function phase_hold() { phase('hold'); }
export function phase_recovery() { phase('recovery'); }
