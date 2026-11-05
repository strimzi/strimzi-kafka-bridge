/*
 * OAuth passthrough load: produce with OIDC access tokens fetched from Keycloak
 * (client credentials grant), against a bridge configured with one of the
 * scripts/load-test/config/bridge-oauth-*.properties profiles.
 *
 * Measures the token validation path of the bridge (introspection or JWKS, with
 * the validation caches) under a controlled token rotation rate. With the default
 * rotation margin the token is reused for its whole lifetime (cache-friendly);
 * with TOKEN_ROTATION_MARGIN >= the token lifetime every request presents a fresh
 * token (cache-hostile: every token is validated from scratch - this is the
 * scenario that also bounds the memory growth of the per-token caches).
 *
 * Environment variables:
 *   BASE_URL               bridge base URL (default http://localhost:8080)
 *   AUTH_SCHEME            authentication scheme: bearer (default), basic (the
 *                          credentials are passed through to the broker as
 *                          SASL/PLAIN, against the bridge-basic.properties
 *                          configuration) or dpop (Bearer token plus a fresh
 *                          DPoP proof per request, signed with the key of
 *                          certs/generate-certs.sh, against the
 *                          bridge-oauth-dpop.properties configuration)
 *   USERS                  number of distinct simulated users (default 1): each
 *                          virtual user authenticates with its own credentials
 *                          from the pool of the environment - for bearer/dpop the
 *                          Keycloak clients loadtest-user-1..10 (realm-loadtest.json),
 *                          for basic the SASL/PLAIN users loadtest-user-1..10
 *                          (kafka/server.properties); a value above the pool size
 *                          (10) wraps around. Each VU keeps its own token cache,
 *                          so USERS also multiplies the distinct live tokens
 *   DPOP_KEY_FILE          DPoP private key JSON (default certs/generated/dpop-keys.json:
 *                          an array with one key per user of the pool)
 *   TLS_CLIENT_CERT / TLS_CLIENT_KEY  PEM client certificate and key for mTLS
 *                          (against the bridge-mtls.properties configuration)
 *   KEYCLOAK_TOKEN_URL     token endpoint (default http://localhost:8180/realms/loadtest/...)
 *   CLIENT_ID / CLIENT_SECRET  Keycloak client credentials (default kafka-broker / kafka-broker-secret)
 *   BASIC_USERNAME / BASIC_PASSWORD  credentials for AUTH_SCHEME=basic
 *                          (default kafka-broker / kafka-broker-secret, the static
 *                          users of the SASL/PLAIN listener)
 *   TOPIC                  target topic (default load-test)
 *   DURATION               scenario duration (default 60s)
 *   RATE                   produce requests per second (default 200)
 *   RECORDS_PER_REQUEST    records per request body (default 1)
 *   VALUE_SIZE             record value size in bytes (default 1024)
 *   TOKEN_ROTATION_MARGIN  seconds subtracted from the token lifetime when reusing it:
 *                          equal to the token lifetime (60 in the imported realm) for
 *                          a fresh token on every request (default 5)
 *   VUS / MAX_VUS          pre-allocated / maximum virtual users (default 50 / 200)
 *
 * Examples:
 *   k6 run -e TOKEN_ROTATION_MARGIN=60 -e RATE=100 scripts/load-test/k6/auth-passthrough.js
 *   k6 run -e AUTH_SCHEME=basic -e RATE=100 scripts/load-test/k6/auth-passthrough.js
 *   k6 run -e AUTH_SCHEME=dpop -e RATE=50 scripts/load-test/k6/auth-passthrough.js
 */

import http from 'k6/http';
import { check } from 'k6';
import encoding from 'k6/encoding';
import exec from 'k6/execution';
import { Trend, Rate, Counter } from 'k6/metrics';
import { dpopProof } from './lib/dpop.js';
import { tlsClientOptions } from './lib/client.js';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
const AUTH_SCHEME = __ENV.AUTH_SCHEME || 'bearer';
// the pool size of the environment (see the USERS knob in the header)
const USERS = parseInt(__ENV.USERS || '1');
const KEYCLOAK_TOKEN_URL = __ENV.KEYCLOAK_TOKEN_URL
  || 'http://localhost:8180/realms/loadtest/protocol/openid-connect/token';
const CLIENT_ID = __ENV.CLIENT_ID || 'kafka-broker';
const CLIENT_SECRET = __ENV.CLIENT_SECRET || 'kafka-broker-secret';
const BASIC_USERNAME = __ENV.BASIC_USERNAME || 'kafka-broker';
const BASIC_PASSWORD = __ENV.BASIC_PASSWORD || 'kafka-broker-secret';
const TOPIC = __ENV.TOPIC || 'load-test';
const RECORDS_PER_REQUEST = parseInt(__ENV.RECORDS_PER_REQUEST || '1');
const VALUE_SIZE = parseInt(__ENV.VALUE_SIZE || '1024');
const TOKEN_ROTATION_MARGIN = parseInt(__ENV.TOKEN_ROTATION_MARGIN || '5');

const produceLatency = new Trend('auth_produce_request_duration', true);
const produceErrors = new Rate('auth_produce_errors');
const tokenFetches = new Counter('auth_token_fetches');

const value = 'x'.repeat(VALUE_SIZE);

export const options = Object.assign({
  discardResponseBodies: false,
  scenarios: {
    auth_produce: {
      executor: 'constant-arrival-rate',
      rate: parseInt(__ENV.RATE || '200'),
      timeUnit: '1s',
      duration: __ENV.DURATION || '60s',
      preAllocatedVUs: parseInt(__ENV.VUS || '50'),
      maxVUs: parseInt(__ENV.MAX_VUS || '200'),
    },
  },
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

export default function () {
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
  } else {
    headers['Authorization'] = `Bearer ${getToken()}`;
  }
  const response = http.post(
    requestUrl,
    JSON.stringify({ records: records }),
    { headers: headers });
  produceLatency.add(response.timings.duration);
  produceErrors.add(!check(response, {
    'status 200': (r) => r.status === 200,
  }));
}
