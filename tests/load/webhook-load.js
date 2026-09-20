import http from 'k6/http';
import { check } from 'k6';
import exec from 'k6/execution';
import { Counter, Rate } from 'k6/metrics';

const baseUrl = (__ENV.BASE_URL || 'http://localhost:8080').replace(/\/+$/, '');
const profile = (__ENV.PROFILE || 'smoke').toLowerCase();
const runId = __ENV.TEST_RUN_ID || `k6-${Date.now()}`;
const requestTimeout = __ENV.REQUEST_TIMEOUT || '30s';
const readinessStatuses = http.expectedStatuses(200);
const acceptedStatuses = http.expectedStatuses(202);

const acceptedRate = new Rate('webhook_accepted_rate');
const responseCount = new Counter('webhook_responses_total');

function positiveInteger(name, fallback) {
    const value = Number.parseInt(__ENV[name] || `${fallback}`, 10);
    if (!Number.isInteger(value) || value <= 0) {
        throw new Error(`${name} must be a positive integer`);
    }
    return value;
}

const profiles = {
    smoke: {
        executor: 'constant-arrival-rate',
        rate: 5,
        timeUnit: '1s',
        duration: '1m',
        preAllocatedVUs: 10,
        maxVUs: 20,
        gracefulStop: '30s',
    },
    load: {
        executor: 'constant-arrival-rate',
        rate: 25,
        timeUnit: '1s',
        duration: '15m',
        preAllocatedVUs: 50,
        maxVUs: 150,
        gracefulStop: '1m',
    },
    stress: {
        executor: 'ramping-arrival-rate',
        startRate: 25,
        timeUnit: '1s',
        preAllocatedVUs: 150,
        maxVUs: 1000,
        stages: [
            { target: 50, duration: '1m' },
            { target: 100, duration: '2m' },
            { target: 175, duration: '3m' },
            { target: 250, duration: '2m' },
            { target: 25, duration: '1m' },
        ],
        gracefulStop: '2m',
    },
    spike: {
        executor: 'ramping-arrival-rate',
        startRate: 25,
        timeUnit: '1s',
        preAllocatedVUs: 200,
        maxVUs: 1200,
        stages: [
            { target: 25, duration: '30s' },
            { target: 500, duration: '10s' },
            { target: 500, duration: '30s' },
            { target: 25, duration: '1m' },
        ],
        gracefulStop: '2m',
    },
    soak: {
        executor: 'constant-arrival-rate',
        rate: 25,
        timeUnit: '1s',
        duration: '1h',
        preAllocatedVUs: 50,
        maxVUs: 200,
        gracefulStop: '2m',
    },
    custom: {
        executor: 'constant-arrival-rate',
        rate: positiveInteger('RATE', 25),
        timeUnit: '1s',
        duration: __ENV.DURATION || '5m',
        preAllocatedVUs: positiveInteger('PRE_ALLOCATED_VUS', 50),
        maxVUs: positiveInteger('MAX_VUS', 500),
        gracefulStop: __ENV.GRACEFUL_STOP || '1m',
    },
};

if (!profiles[profile]) {
    throw new Error(`Unknown PROFILE '${profile}'. Use smoke, load, stress, spike, soak, or custom.`);
}

export const options = {
    scenarios: {
        webhook_ingestion: profiles[profile],
    },
    thresholds: {
        webhook_accepted_rate: [
            { threshold: `rate>=${__ENV.MIN_ACCEPTED_RATE || '0.99'}`, abortOnFail: false },
        ],
        'http_req_failed{endpoint:events}': [
            { threshold: `rate<=${__ENV.MAX_FAILED_RATE || '0.01'}`, abortOnFail: false },
        ],
        'http_req_duration{endpoint:events}': [
            { threshold: `p(95)<${__ENV.P95_MS || '1000'}`, abortOnFail: false },
            { threshold: `p(99)<${__ENV.P99_MS || '2000'}`, abortOnFail: false },
        ],
        dropped_iterations: [
            { threshold: 'count==0', abortOnFail: false },
        ],
    },
    summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)', 'p(99)', 'count'],
    tags: {
        profile,
        test_run_id: runId,
    },
};

export function setup() {
    const response = http.get(`${baseUrl}/ready`, {
        timeout: requestTimeout,
        tags: { endpoint: 'readiness' },
        responseCallback: readinessStatuses,
    });

    if (response.status !== 200) {
        throw new Error(`Target is not ready: GET ${baseUrl}/ready returned HTTP ${response.status}`);
    }

    console.log(`run_id=${runId} profile=${profile} base_url=${baseUrl}`);
}

export default function () {
    const sequence = exec.scenario.iterationInTest;
    const eventId = `${runId}-${exec.scenario.name}-${sequence}`;
    const payload = JSON.stringify({
        id: eventId,
        type: 'k6.load.test',
        payload: {
            runId,
            profile,
            sequence,
        },
    });

    const response = http.post(`${baseUrl}/events`, payload, {
        headers: {
            'Content-Type': 'application/json',
            'X-Request-ID': `${runId}-request-${sequence}`,
        },
        timeout: requestTimeout,
        tags: { endpoint: 'events' },
        responseCallback: acceptedStatuses,
    });

    let acceptedBody = false;
    if (response.status === 202) {
        try {
            acceptedBody = response.json('accepted') === true;
        } catch (_) {
            acceptedBody = false;
        }
    }

    const accepted = response.status === 202 && acceptedBody;
    acceptedRate.add(accepted);
    responseCount.add(1, { status: `${response.status}` });

    check(response, {
        'event accepted with HTTP 202': () => response.status === 202,
        'response confirms acceptance': () => acceptedBody,
    });
}
