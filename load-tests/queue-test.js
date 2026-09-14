import http from 'k6/http';
import { check } from 'k6';
import { Trend } from 'k6/metrics';

const setupDuration = new Trend('setup_duration_ms', true);

// Usage:
//   k6 run -e ENDPOINT=/naive -e OUT=naive.json load-tests/queue-test.js
//
// Env vars:
//   BASE_URL   default http://localhost:5280
//   ENDPOINT   /direct | /naive | /outbox | /replication   (default /naive)
//   VUS        default 250
//   ITERATIONS default 100000
//   WARMUP     number of discarded requests before the timed run (default 1000)
//   OUT        path to write the JSON summary (default summary.json)
//
// setup() fires WARMUP requests (in batches sized to VUS) before the timed scenario
// starts, so the timed numbers aren't paying for connection-pool ramp-up, cold Postgres
// buffers/OS page cache, or .NET JIT tiering - all of which land almost entirely on
// whichever test happens to run first against a freshly started stack.

const BASE_URL = __ENV.BASE_URL || 'http://localhost:5280';
const ENDPOINT = __ENV.ENDPOINT || '/naive';
const VUS = parseInt(__ENV.VUS || '250', 10);
const ITERATIONS = parseInt(__ENV.ITERATIONS || '100000', 10);
const WARMUP = parseInt(__ENV.WARMUP || '1000', 10);
const OUT = __ENV.OUT || 'summary.json';

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

const payload = JSON.stringify({ content: 'Hello, world!' });
const params = { headers: { 'Content-Type': 'application/json' } };

export function setup() {
    const t0 = Date.now();
    const batchSize = Math.min(VUS, 50);
    let sent = 0;
    while (sent < WARMUP) {
        const n = Math.min(batchSize, WARMUP - sent);
        const requests = Array.from({ length: n }, () => ['POST', `${BASE_URL}${ENDPOINT}`, payload, params]);
        http.batch(requests);
        sent += n;
    }
    setupDuration.add(Date.now() - t0);
}

export default function () {
    const res = http.post(`${BASE_URL}${ENDPOINT}`, payload, params);
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
    // data.state.testRunDurationMs spans setup()+scenario+teardown(); subtract the measured
    // setup() time to get the actual timed-scenario duration, since setup() (warm-up) can take
    // a non-trivial amount of wall time and must not be counted against throughput.
    const setupMs = m.setup_duration_ms ? m.setup_duration_ms.values.avg : 0;
    const scenarioS = (data.state.testRunDurationMs - setupMs) / 1000;
    const throughput = ITERATIONS / scenarioS;
    return `
Endpoint:        ${ENDPOINT}
VUs:             ${VUS}
Iterations:      ${ITERATIONS}
Warmup:          ${WARMUP} requests, ${fmt(setupMs / 1000)} s
Scenario time:   ${fmt(scenarioS)} s
Iterations/sec:  ${fmt(throughput)}
Req avg:         ${fmt(m.http_req_duration ? m.http_req_duration.values.avg : undefined)} ms
Req p95:         ${fmt(m.http_req_duration ? m.http_req_duration.values['p(95)'] : undefined)} ms
Req p99:         ${fmt(m.http_req_duration ? m.http_req_duration.values['p(99)'] : undefined)} ms
Failed requests: ${fmt(m.http_req_failed ? m.http_req_failed.values.rate * 100 : undefined)} %
`;
}
