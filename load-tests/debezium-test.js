import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend } from 'k6/metrics';

const setupDuration = new Trend('setup_duration_ms', true);

// Load test for the /debezium endpoint: Postgres -> Debezium (Kafka Connect) -> Kafka topic -> consumer.
// Unlike the other endpoints, this one needs external setup: a Debezium connector has to be
// registered against Kafka Connect before the run and removed after, otherwise the WAL slot it
// opens on Postgres just sits there accumulating WAL forever.
//
// Usage:
//   k6 run -e VUS=250 -e ITERATIONS=100000 -e OUT=debezium.json load-tests/debezium-test.js
//
// Env vars:
//   BASE_URL     default http://localhost:5280      (the Web app)
//   CONNECT_URL  default http://localhost:8083       (Kafka Connect / Debezium REST API)
//   PG_PASSWORD  default postgres_demo_password      (must match AppHost's "postgres-password" parameter)
//   VUS          default 250
//   ITERATIONS   default 100000
//   OUT          path to write the JSON summary (default summary.json)

const BASE_URL = __ENV.BASE_URL || 'http://localhost:5280';
const CONNECT_URL = __ENV.CONNECT_URL || 'http://localhost:8083';
const PG_PASSWORD = __ENV.PG_PASSWORD || 'postgres_demo_password';
const VUS = parseInt(__ENV.VUS || '250', 10);
const ITERATIONS = parseInt(__ENV.ITERATIONS || '100000', 10);
const WARMUP = parseInt(__ENV.WARMUP || '200', 10);
const OUT = __ENV.OUT || 'summary.json';

const CONNECTOR_NAME = 'messages-connector';

export const options = {
    scenarios: {
        queue: {
            executor: 'shared-iterations',
            vus: VUS,
            iterations: ITERATIONS,
            maxDuration: '10m',
        },
    },
    thresholds: {
        http_req_failed: ['rate<0.01'],
    },
    summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(95)', 'p(99)'],
    setupTimeout: '180s',
};

const connectorConfig = {
    name: CONNECTOR_NAME,
    config: {
        'connector.class': 'io.debezium.connector.postgresql.PostgresConnector',
        'database.hostname': 'postgres', // resolvable inside the Aspire container network
        'database.port': '5432',
        'database.user': 'postgres',
        'database.password': PG_PASSWORD,
        'database.dbname': 'database',
        'topic.prefix': 'cdc',
        'table.include.list': 'public.messages',
        'plugin.name': 'pgoutput',
        'slot.name': 'debezium_slot',
        'publication.name': 'debezium_publication',
        'publication.autocreate.mode': 'filtered',
        // Drop the replication slot when the connector is deleted, otherwise it keeps
        // pinning WAL on the Postgres server forever after the test finishes.
        'slot.drop.on.stop': 'true',
        // Skip the initial table snapshot - only stream changes made during the test.
        'snapshot.mode': 'no_data',
        'poll.interval.ms': '100',
        'tombstones.on.delete': 'false',
        'key.converter': 'org.apache.kafka.connect.json.JsonConverter',
        'key.converter.schemas.enable': 'false',
        'value.converter': 'org.apache.kafka.connect.json.JsonConverter',
        'value.converter.schemas.enable': 'false',
    },
};

export function setup() {
    const t0 = Date.now();
    // Clean up a stale connector from a previous crashed run, if any.
    const existing = http.get(`${CONNECT_URL}/connectors/${CONNECTOR_NAME}/status`);
    if (existing.status === 200) {
        http.del(`${CONNECT_URL}/connectors/${CONNECTOR_NAME}`);
    }

    const created = http.post(`${CONNECT_URL}/connectors`, JSON.stringify(connectorConfig), {
        headers: { 'Content-Type': 'application/json' },
    });
    if (created.status !== 201 && created.status !== 409) {
        throw new Error(`Failed to create Debezium connector: ${created.status} ${created.body}`);
    }

    // Wait until the connector and its task report RUNNING before hammering the endpoint.
    let running = false;
    for (let i = 0; i < 60; i++) {
        const res = http.get(`${CONNECT_URL}/connectors/${CONNECTOR_NAME}/status`);
        if (res.status === 200) {
            const body = JSON.parse(res.body);
            const connectorState = body.connector && body.connector.state;
            const taskState = body.tasks && body.tasks[0] && body.tasks[0].state;
            if (connectorState === 'RUNNING' && taskState === 'RUNNING') {
                running = true;
                break;
            }
            if (taskState === 'FAILED') {
                throw new Error(`Debezium connector task failed: ${JSON.stringify(body)}`);
            }
        }
        sleep(1);
    }
    if (!running) {
        throw new Error('Timed out waiting for Debezium connector to become RUNNING');
    }

    // The destination Kafka topic doesn't exist yet - its auto-creation on the first produce
    // takes ~60s (metadata retry backoff on a brand-new topic), longer than any single request
    // should reasonably block for. Retry in short attempts instead of one long blocking call.
    let firstOk = false;
    for (let i = 0; i < 12 && !firstOk; i++) {
        const first = http.post(`${BASE_URL}/debezium`, payload, { headers: params.headers, timeout: '15s' });
        if (first.status === 201) {
            firstOk = true;
        } else {
            sleep(5);
        }
    }
    if (!firstOk) {
        throw new Error('Debezium warm-up: topic never became ready within the retry budget');
    }

    // Further warm-up: connection pool ramp-up, JIT tiering, Postgres buffer cache.
    const batchSize = Math.min(VUS, 25);
    let sent = 0;
    while (sent < WARMUP) {
        const n = Math.min(batchSize, WARMUP - sent);
        const requests = Array.from({ length: n }, () => ['POST', `${BASE_URL}/debezium`, payload, { headers: params.headers, timeout: '30s' }]);
        http.batch(requests);
        sent += n;
    }

    setupDuration.add(Date.now() - t0);
}

export function teardown() {
    http.del(`${CONNECT_URL}/connectors/${CONNECTOR_NAME}`);
}

const payload = JSON.stringify({ content: 'Hello, world!' });
const params = { headers: { 'Content-Type': 'application/json' } };

export default function () {
    const res = http.post(`${BASE_URL}/debezium`, payload, params);
    check(res, {
        'status is 201': (r) => r.status === 201,
    });
}

export function handleSummary(data) {
    const out = {};
    out[OUT] = JSON.stringify(data, null, 2);
    out['stdout'] = textSummary(data);
    return out;
}

function textSummary(data) {
    const m = data.metrics;
    const fmt = (v) => (v === undefined ? 'n/a' : v.toFixed(2));
    const setupMs = m.setup_duration_ms ? m.setup_duration_ms.values.avg : 0;
    const scenarioS = (data.state.testRunDurationMs - setupMs) / 1000;
    const throughput = ITERATIONS / scenarioS;
    return `
Endpoint:        /debezium
VUs:             ${VUS}
Iterations:      ${ITERATIONS}
Warmup:          ${fmt(setupMs / 1000)} s (connector startup + topic creation + ${WARMUP} discarded requests)
Scenario time:   ${fmt(scenarioS)} s
Iterations/sec:  ${fmt(throughput)}
Req avg:         ${fmt(m.http_req_duration ? m.http_req_duration.values.avg : undefined)} ms
Req p95:         ${fmt(m.http_req_duration ? m.http_req_duration.values['p(95)'] : undefined)} ms
Req p99:         ${fmt(m.http_req_duration ? m.http_req_duration.values['p(99)'] : undefined)} ms
Failed requests: ${fmt(m.http_req_failed ? m.http_req_failed.values.rate * 100 : undefined)} %
`;
}
