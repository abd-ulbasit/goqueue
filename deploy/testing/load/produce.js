// =============================================================================
// GOQUEUE LOAD TEST - K6 CONFIGURATION
// =============================================================================
//
// PURPOSE: Performance testing for GoQueue message broker
//
// TARGETS:
//   - Throughput: 100,000+ messages/second
//   - Latency: p99 < 10ms
//   - Error rate: < 0.01%
//
// USAGE:
//   k6 run produce.js
//   k6 run --vus 100 --duration 5m produce.js
//
// =============================================================================

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';
import { randomString } from 'https://jslib.k6.io/k6-utils/1.2.0/index.js';

// Custom metrics
const messagesProduced = new Counter('messages_produced');
const messagesFailed = new Counter('messages_failed');
const produceLatency = new Trend('produce_latency', true);
const errorRate = new Rate('error_rate');

// Configuration
const BASE_URL = __ENV.GOQUEUE_URL || 'http://localhost:8080';
const TOPIC = __ENV.TOPIC || 'load-test';

// Test scenarios
export const options = {
    scenarios: {
        // Ramp-up test
        ramp_up: {
            executor: 'ramping-vus',
            startVUs: 1,
            stages: [
                { duration: '30s', target: 10 },
                { duration: '1m', target: 50 },
                { duration: '2m', target: 100 },
                { duration: '1m', target: 50 },
                { duration: '30s', target: 0 },
            ],
            gracefulRampDown: '10s',
            exec: 'produceMessages',
        },
    },
    thresholds: {
        // p99 latency should be under 10ms
        'produce_latency': ['p(99)<10'],
        // Error rate should be under 0.01%
        'error_rate': ['rate<0.0001'],
        // At least 95% of requests should succeed
        'http_req_failed': ['rate<0.05'],
    },
};

// Generate a test message
function generateMessage(size = 'small') {
    const sizes = {
        small: 100,      // 100 bytes
        medium: 1024,    // 1 KB
        large: 10240,    // 10 KB
    };

    const payloadSize = sizes[size] || 100;

    return {
        id: randomString(16),
        timestamp: Date.now(),
        payload: randomString(payloadSize),
        metadata: {
            source: 'k6-load-test',
            vu: __VU,
            iteration: __ITER,
        },
    };
}

// Produce a single message
export function produceMessages() {
    const message = generateMessage('small');
    const url = `${BASE_URL}/api/v1/topics/${TOPIC}/messages`;

    const params = {
        headers: {
            'Content-Type': 'application/json',
        },
        timeout: '5s',
    };

    const startTime = Date.now();
    const response = http.post(url, JSON.stringify(message), params);
    const latency = Date.now() - startTime;

    produceLatency.add(latency);

    const success = check(response, {
        'status is 200 or 201': (r) => r.status === 200 || r.status === 201,
        'latency under 10ms': () => latency < 10,
    });

    if (success) {
        messagesProduced.add(1);
        errorRate.add(0);
    } else {
        messagesFailed.add(1);
        errorRate.add(1);
        console.log(`Failed to produce message: ${response.status} - ${response.body}`);
    }

    // Minimal sleep to allow for high throughput
    sleep(0.001);
}

// Setup - create topic if needed
export function setup() {
    console.log(`Setting up load test for topic: ${TOPIC}`);
    console.log(`Target URL: ${BASE_URL}`);

    // Try to create the topic
    const createTopicUrl = `${BASE_URL}/api/v1/topics`;
    const topicConfig = {
        name: TOPIC,
        partitions: 8,
        replicationFactor: 3,
        config: {
            'retention.ms': 86400000,  // 24 hours
        },
    };

    const response = http.post(createTopicUrl, JSON.stringify(topicConfig), {
        headers: { 'Content-Type': 'application/json' },
    });

    if (response.status === 200 || response.status === 201 || response.status === 409) {
        console.log('Topic ready');
    } else {
        console.log(`Warning: Could not create topic: ${response.status}`);
    }

    return { topic: TOPIC };
}

// Teardown - print summary
export function teardown(data) {
    console.log(`Load test complete for topic: ${data.topic}`);
}

// Default function
export default function () {
    produceMessages();
}
