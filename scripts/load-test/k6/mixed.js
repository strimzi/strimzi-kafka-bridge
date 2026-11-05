/*
 * Mixed traffic: produce, consume and (optionally) authenticated produce in a single
 * k6 run, each with its own constant arrival rate - a weighted traffic mix, so that
 * the bridge is measured under concurrent competing workloads instead of a single
 * operation.
 *
 * Works against the plain bridge configuration (auth scenario off by default) and
 * against the OAuth configuration (set AUTH_RATE > 0).
 *
 * Environment variables:
 *   BASE_URL              bridge base URL (default http://localhost:8080)
 *   TOPIC                 target topic (default load-test)
 *   DURATION              duration of all the scenarios (default 60s)
 *   PRODUCE_RATE          produce requests per second (default 300; 0 disables)
 *   CONSUME_RATE          poll requests per second (default 50; 0 disables)
 *   AUTH_RATE             authenticated produce requests per second (default 0 = disabled;
 *                         requires the OAuth bridge configuration and the OIDC provider)
 *   RECORDS_PER_REQUEST   records per produce request body (default 1)
 *   VALUE_SIZE            record value size in bytes (default 1024)
 *   CONSUME_VUS           virtual users of the consume scenario, one consumer each (default 10)
 *   TIMEOUT_MS            long-poll timeout per poll request (default 1000)
 *   MAX_BYTES             fetch limit per poll (default 1048576)
 *   COMMIT_OFFSETS        1 (default) commits the polled offsets after each poll that
 *                         returned records (the steady-state consumer loop with
 *                         enable.auto.commit=false; the commit latency is reported as
 *                         mix_commit_duration); 0 disables the commit requests
 *   METADATA_RATE         metadata requests per second (default 5; 0 disables): each
 *                         iteration lists the topics (GET /topics) and, when listed,
 *                         fetches the target topic (GET /topics/{topic})
 *   KEYCLOAK_TOKEN_URL    token endpoint for the auth scenario
 *   CLIENT_ID / CLIENT_SECRET Keycloak client credentials (default kafka-broker / kafka-broker-secret)
 *   USERS                 number of distinct simulated users (default 1): each virtual
 *                         user authenticates with its own credentials from the pool of
 *                         the environment - for bearer/dpop the Keycloak clients
 *                         loadtest-user-1..10, for basic the SASL/PLAIN users
 *                         loadtest-user-1..10; a value above 10 wraps around
 *   TOKEN_ROTATION_MARGIN seconds subtracted from the token lifetime when reusing it
 *                         (default 5; equal to the token lifetime for a fresh token per request)
 *
 * Example (60/10/30 produce/consume/auth mix):
 *   k6 run -e AUTH_RATE=30 -e PRODUCE_RATE=60 -e CONSUME_RATE=10 scripts/load-test/k6/mixed.js
 */

import http from 'k6/http';
import { check } from 'k6';
import { Trend, Rate, Counter } from 'k6/metrics';
import exec from 'k6/execution';
import encoding from 'k6/encoding';
import { dpopProof } from './lib/dpop.js';
import { tlsClientOptions } from './lib/client.js';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
const AUTH_SCHEME = __ENV.AUTH_SCHEME || 'bearer';
const TOPIC = __ENV.TOPIC || 'load-test';
const RECORDS_PER_REQUEST = parseInt(__ENV.RECORDS_PER_REQUEST || '1');
const VALUE_SIZE = parseInt(__ENV.VALUE_SIZE || '1024');
const CONSUME_VUS = parseInt(__ENV.CONSUME_VUS || '10');
const TIMEOUT_MS = parseInt(__ENV.TIMEOUT_MS || '1000');
const MAX_BYTES = parseInt(__ENV.MAX_BYTES || '1048576');
const KEYCLOAK_TOKEN_URL = __ENV.KEYCLOAK_TOKEN_URL
  || 'http://localhost:8180/realms/loadtest/protocol/openid-connect/token';
const CLIENT_ID = __ENV.CLIENT_ID || 'kafka-broker';
const CLIENT_SECRET = __ENV.CLIENT_SECRET || 'kafka-broker-secret';
const BASIC_USERNAME = __ENV.BASIC_USERNAME || 'kafka-broker';
const BASIC_PASSWORD = __ENV.BASIC_PASSWORD || 'kafka-broker-secret';
// number of distinct simulated users (see the header): each VU authenticates with
// its own credentials from the environment pool (Keycloak clients or SASL/PLAIN
// users loadtest-user-1..10, wrapping around)
const USERS = parseInt(__ENV.USERS || '1');
const TOKEN_ROTATION_MARGIN = parseInt(__ENV.TOKEN_ROTATION_MARGIN || '5');

const produceRate = parseInt(__ENV.PRODUCE_RATE || '300');
const consumeRate = parseInt(__ENV.CONSUME_RATE || '50');
const authRate = parseInt(__ENV.AUTH_RATE || '0');
const metadataRate = parseInt(__ENV.METADATA_RATE || '5');
const commitEnabled = (__ENV.COMMIT_OFFSETS || '1') !== '0';
const duration = __ENV.DURATION || '60s';

const produceLatency = new Trend('mix_produce_duration', true);
const pollLatency = new Trend('mix_poll_duration', true);
const authProduceLatency = new Trend('mix_auth_produce_duration', true);
const commitLatency = new Trend('mix_commit_duration', true);
const listTopicsLatency = new Trend('mix_list_topics_duration', true);
const getTopicLatency = new Trend('mix_get_topic_duration', true);
const mixErrors = new Rate('mix_errors');
const tokenFetches = new Counter('mix_auth_token_fetches');

const value = 'x'.repeat(VALUE_SIZE);

// when the authenticated scenario is enabled, the bridge requires authentication on
// EVERY request (OAuth passthrough or Basic passthrough): all the operations of the
// mix are then authenticated
const allAuthenticated = authRate > 0;

// per-VU credentials: the user index derives from the VU id (VUs of the same
// user share the credentials but keep their own token cache). The safe modulo
// handles the teardown VU (id 0).
function userIndex() {
  return (((exec.vu.idInTest - 1) % USERS) + USERS) % USERS;
}

function basicCredentials() {
    if (USERS <= 1) {
        return encoding.b64encode(`${BASIC_USERNAME}:${BASIC_PASSWORD}`);
    }
    return encoding.b64encode(`loadtest-user-${userIndex() + 1}:loadtest-password`);
}

function clientId() {
    return USERS <= 1 ? CLIENT_ID : `loadtest-user-${userIndex() + 1}`;
}

function authorization(method, url) {
    if (AUTH_SCHEME === 'basic') {
        return `Basic ${basicCredentials()}`;
    }
    if (AUTH_SCHEME === 'dpop') {
        // a sender-constrained proof per request (the bridge rejects replayed proofs);
        // the proof header is set by the caller through dpopProofHeader()
        return `DPoP ${getToken()}`;
    }
    const bearerToken = getToken();
    return bearerToken !== null ? `Bearer ${bearerToken}` : null;
}

function headers(contentType, method, url) {
    const base = contentType ? { 'Content-Type': contentType } : {};
    if (allAuthenticated) {
        if (AUTH_SCHEME === 'dpop') {
            base['DPoP'] = dpopProof(method, url);
        }
        const authValue = authorization(method, url);
        if (authValue !== null) {
            base['Authorization'] = authValue;
        }
    }
    return base;
}

function acceptHeaders(accept, method, url) {
    const base = { 'Accept': accept };
    if (allAuthenticated) {
        if (AUTH_SCHEME === 'dpop') {
            base['DPoP'] = dpopProof(method, url);
        }
        const authValue = authorization(method, url);
        if (authValue !== null) {
            base['Authorization'] = authValue;
        }
    }
    return base;
}

// the scenarios are built at init time, so that a rate of 0 removes the scenario
const scenarios = {};
if (produceRate > 0) {
    scenarios.produce = {
        executor: 'constant-arrival-rate',
        rate: produceRate,
        timeUnit: '1s',
        duration: duration,
        preAllocatedVUs: 50,
        maxVUs: 200,
        exec: 'produceOp',
    };
}
if (consumeRate > 0) {
    scenarios.consume = {
        executor: 'constant-arrival-rate',
        rate: consumeRate,
        timeUnit: '1s',
        duration: duration,
        preAllocatedVUs: CONSUME_VUS,
        maxVUs: CONSUME_VUS,
        exec: 'consumeOp',
    };
}
if (authRate > 0) {
    scenarios.auth = {
        executor: 'constant-arrival-rate',
        rate: authRate,
        timeUnit: '1s',
        duration: duration,
        preAllocatedVUs: 50,
        maxVUs: 200,
        exec: 'authProduceOp',
    };
}
if (metadataRate > 0) {
    scenarios.metadata = {
        executor: 'constant-arrival-rate',
        rate: metadataRate,
        timeUnit: '1s',
        duration: duration,
        preAllocatedVUs: 5,
        maxVUs: 20,
        exec: 'metadataOp',
    };
}

export const options = Object.assign({
    discardResponseBodies: false,
    scenarios: scenarios,
}, tlsClientOptions());

export function produceOp() {
    const records = [];
    for (let i = 0; i < RECORDS_PER_REQUEST; i++) {
        records.push({ value: value });
    }
    const requestUrl = `${BASE_URL}/topics/${TOPIC}`;
    const response = http.post(
        requestUrl,
        JSON.stringify({ records: records }),
        { headers: headers('application/vnd.kafka.json.v2+json', 'POST', requestUrl) });
    produceLatency.add(response.timings.duration);
    mixErrors.add(!check(response, { 'produce 200': (r) => r.status === 200 }));
}

// one consumer per virtual user of the consume scenario, created on the first iteration
const consumers = {};

function consumerName() {
    // the VU id is unique in the whole test run (across the scenarios), so that each
    // consume-scenario VU polls its own consumer instance (concurrent polls on the
    // same consumer instance are rejected by the bridge)
    return `mix-consumer-${exec.vu.idInTest}`;
}

function ensureConsumer() {
    const vu = exec.vu.idInTest;
    if (consumers[vu]) {
        return;
    }
    const createUrl = `${BASE_URL}/consumers/${__ENV.GROUP || 'mix-consumers'}`;
    const created = http.post(
        createUrl,
        JSON.stringify({
            name: consumerName(),
            format: 'json',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': false,
        }),
        { headers: headers('application/vnd.kafka.v2+json', 'POST', createUrl) });
    check(created, { 'consumer created': (r) => r.status === 200 || r.status === 409 });
    const subscribeUrl = `${BASE_URL}/consumers/${__ENV.GROUP || 'mix-consumers'}/instances/${consumerName()}/subscription`;
    const subscribed = http.post(
        subscribeUrl,
        JSON.stringify({ topics: [TOPIC] }),
        { headers: headers('application/vnd.kafka.v2+json', 'POST', subscribeUrl) });
    check(subscribed, { 'subscribed': (r) => r.status === 204 });
    consumers[vu] = true;
}

export function consumeOp() {
    ensureConsumer();
    const pollUrl = `${BASE_URL}/consumers/${__ENV.GROUP || 'mix-consumers'}/instances/${consumerName()}/records`;
    const response = http.get(
        pollUrl + `?timeout=${TIMEOUT_MS}&max_bytes=${MAX_BYTES}`,
        {
            // the Accept header must match the consumer format (json); the Content-Type
            // is not needed on a GET; the DPoP proof htu claim excludes the query
            headers: acceptHeaders('application/vnd.kafka.json.v2+json', 'GET', pollUrl),
        });
    pollLatency.add(response.timings.duration);
    // 200 with records, or 404 while the subscription is not yet effective (rebalancing)
    mixErrors.add(!check(response, {
        'poll 200 or 404': (r) => r.status === 200 || r.status === 404,
    }));

    // the steady-state consumer loop with manual offsets: commit the polled offsets
    // (the next offset to read per topic-partition) after each poll that returned
    // records - same VU, hence the same identity that owns the consumer
    if (commitEnabled && response.status === 200) {
        const records = response.json();
        if (Array.isArray(records) && records.length > 0) {
            // records arrive in offset order per partition: keep the last one
            const offsets = {};
            for (const record of records) {
                offsets[`${record.topic}/${record.partition}`] = {
                    topic: record.topic,
                    partition: record.partition,
                    offset: record.offset + 1,
                };
            }
            const commitUrl = `${BASE_URL}/consumers/${__ENV.GROUP || 'mix-consumers'}/instances/${consumerName()}/offsets`;
            const commitResponse = http.post(
                commitUrl,
                JSON.stringify({ offsets: Object.values(offsets) }),
                { headers: headers('application/vnd.kafka.v2+json', 'POST', commitUrl) });
            commitLatency.add(commitResponse.timings.duration);
            // 204 committed, or 409 while the group rebalances (expected Kafka behavior,
            // not an error signal; the next poll re-establishes the assignment)
            mixErrors.add(!check(commitResponse, {
                'commit 204 or 409': (r) => r.status === 204 || r.status === 409,
            }));
        }
    }
}

// the metadata leg: list the topics and, when the target topic is listed, fetch it
export function metadataOp() {
    const listUrl = `${BASE_URL}/topics`;
    const listResponse = http.get(
        listUrl,
        { headers: acceptHeaders('application/vnd.kafka.v2+json', 'GET', listUrl) });
    listTopicsLatency.add(listResponse.timings.duration);
    mixErrors.add(!check(listResponse, {
        'list topics 200': (r) => r.status === 200,
    }));

    const topics = listResponse.json();
    if (Array.isArray(topics) && topics.includes(TOPIC)) {
        const getUrl = `${BASE_URL}/topics/${TOPIC}`;
        const getResponse = http.get(
            getUrl,
            { headers: acceptHeaders('application/vnd.kafka.v2+json', 'GET', getUrl) });
        getTopicLatency.add(getResponse.timings.duration);
        mixErrors.add(!check(getResponse, {
            'get topic 200': (r) => r.status === 200,
        }));
    }
}

// token cache with a controlled rotation margin (per virtual user of the auth scenario)
let token = null;
let tokenValidUntil = 0;

function getToken() {
    const now = Date.now();
    if (token !== null && now < tokenValidUntil) {
        return token;
    }
    const response = http.post(
        KEYCLOAK_TOKEN_URL,
        `grant_type=client_credentials&client_id=${clientId()}&client_secret=${USERS <= 1 ? CLIENT_SECRET : 'loadtest-user-secret'}`,
        { headers: { 'Content-Type': 'application/x-www-form-urlencoded' } });
    if (!check(response, { 'token fetched': (r) => r.status === 200 })) {
        // keep the previous token (it may still be accepted) and retry on a next request
        return token;
    }
    tokenFetches.add(1);
    token = response.json('access_token');
    const expiresIn = response.json('expires_in') || 60;
    tokenValidUntil = now + Math.max(0, expiresIn - TOKEN_ROTATION_MARGIN) * 1000;
    return token;
}

export function authProduceOp() {
    const records = [];
    for (let i = 0; i < RECORDS_PER_REQUEST; i++) {
        records.push({ value: value });
    }
    const requestUrl = `${BASE_URL}/topics/${TOPIC}`;
    const response = http.post(
        requestUrl,
        JSON.stringify({ records: records }),
        { headers: headers('application/vnd.kafka.json.v2+json', 'POST', requestUrl) });
    authProduceLatency.add(response.timings.duration);
    mixErrors.add(!check(response, { 'auth produce 200': (r) => r.status === 200 }));
}

export function teardown() {
    // best-effort cleanup of the consumers created by the run: the VU ids of the
    // consume scenario are not enumerable upfront (they are assigned globally across
    // the scenarios), so a bounded range of ids is swept (missed consumers are
    // tolerated - a second sweep on the next run or a bridge restart cleans them up)
    for (let vu = 1; vu <= 300; vu++) {
        const deleteUrl = `${BASE_URL}/consumers/${__ENV.GROUP || 'mix-consumers'}/instances/mix-consumer-${vu}`;
        http.del(deleteUrl, null,
            { headers: headers(null, 'DELETE', deleteUrl) });
    }
}
