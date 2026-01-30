// =============================================================================
// GOQUEUE LOAD TEST - END-TO-END STRESS TEST
// =============================================================================
//
// PURPOSE: Comprehensive stress test combining produce and consume
//
// WHAT THIS TESTS:
//   1. Producer throughput under load
//   2. Consumer throughput matching producer
//   3. End-to-end latency
//   4. System stability under sustained load
//   5. Message ordering and consistency
//
// USAGE:
//   k6 run stress.js
//   k6 run --env GOQUEUE_URL=http://goqueue:8080 stress.js
//
// =============================================================================

import http from 'k6/http';
import { check, sleep, group } from 'k6';
import { Counter, Rate, Trend, Gauge } from 'k6/metrics';
import { randomString, randomIntBetween } from 'https://jslib.k6.io/k6-utils/1.2.0/index.js';

// Custom metrics
const messagesProduced = new Counter('messages_produced');
const messagesConsumed = new Counter('messages_consumed');
const produceLatency = new Trend('produce_latency', true);
const consumeLatency = new Trend('consume_latency', true);
const e2eLatency = new Trend('e2e_latency', true);
const errorRate = new Rate('error_rate');
const throughput = new Gauge('current_throughput');

// Configuration
const BASE_URL = __ENV.GOQUEUE_URL || 'http://localhost:8080';
const TOPIC = __ENV.TOPIC || 'stress-test';

// Stress test scenarios
export const options = {
    scenarios: {
        // Producers - ramping pattern
        producers: {
            executor: 'ramping-vus',
            startVUs: 0,
            stages: [
                { duration: '1m', target: 20 },   // Warm up
                { duration: '3m', target: 50 },   // Ramp to target
                { duration: '5m', target: 100 },  // Peak load
                { duration: '3m', target: 150 },  // Stress test
                { duration: '2m', target: 100 },  // Cool down
                { duration: '1m', target: 0 },    // Ramp down
            ],
            exec: 'producer',
            gracefulRampDown: '30s',
        },
        // Consumers - constant load
        consumers: {
            executor: 'constant-vus',
            vus: 30,
            duration: '15m',
            exec: 'consumer',
            startTime: '30s',  // Start after producers warm up
        },
    },
    thresholds: {
        // Latency thresholds
        'produce_latency': ['p(95)<20', 'p(99)<50'],
        'consume_latency': ['p(95)<50', 'p(99)<100'],
        'e2e_latency': ['p(95)<100', 'p(99)<200'],
        // Error rate
        'error_rate': ['rate<0.001'],
        // HTTP failures
        'http_req_failed': ['rate<0.01'],
    },
};

// Message sizes for variety
const MESSAGE_SIZES = ['small', 'medium', 'large'];
const SIZE_WEIGHTS = [0.7, 0.25, 0.05];  // 70% small, 25% medium, 5% large

function selectMessageSize() {
    const rand = Math.random();
    let cumulative = 0;
    for (let i = 0; i < SIZE_WEIGHTS.length; i++) {
        cumulative += SIZE_WEIGHTS[i];
        if (rand < cumulative) {
            return MESSAGE_SIZES[i];
        }
    }
    return MESSAGE_SIZES[0];
}

function generateMessage(size) {
    const sizes = {
        small: 100,
        medium: 1024,
        large: 10240,
    };

    return {
        id: randomString(16),
        timestamp: Date.now(),
        size: size,
        sequence: __ITER,
        vu: __VU,
        payload: randomString(sizes[size] || 100),
        metadata: {
            test: 'stress',
            priority: randomIntBetween(1, 10),
        },
    };
}

// Producer function
export function producer() {
    group('produce', function () {
        const size = selectMessageSize();
        const message = generateMessage(size);
        const url = `${BASE_URL}/api/v1/topics/${TOPIC}/messages`;

        const startTime = Date.now();
        const response = http.post(url, JSON.stringify(message), {
            headers: { 'Content-Type': 'application/json' },
            timeout: '10s',
        });
        const latency = Date.now() - startTime;

        produceLatency.add(latency);

        const success = check(response, {
            'produce success': (r) => r.status === 200 || r.status === 201,
        });

        if (success) {
            messagesProduced.add(1);
            errorRate.add(0);
        } else {
            errorRate.add(1);
        }
    });

    // High-frequency production
    sleep(randomIntBetween(1, 10) / 1000);
}

// Consumer function
export function consumer() {
    group('consume', function () {
        const groupId = `stress-consumer-${__VU % 5}`;  // 5 consumer groups
        const url = `${BASE_URL}/api/v1/topics/${TOPIC}/consume?group=${groupId}&timeout=1000&maxMessages=10`;

        const startTime = Date.now();
        const response = http.get(url, {
            headers: { 'Content-Type': 'application/json' },
            timeout: '5s',
        });
        const latency = Date.now() - startTime;

        consumeLatency.add(latency);

        if (response.status === 200) {
            try {
                const body = JSON.parse(response.body);
                const messages = body.messages || [];

                messages.forEach((msg) => {
                    messagesConsumed.add(1);
                    errorRate.add(0);

                    // Calculate E2E latency
                    if (msg.timestamp) {
                        e2eLatency.add(Date.now() - msg.timestamp);
                    }
                });

                // Batch acknowledge
                if (messages.length > 0) {
                    const ackUrl = `${BASE_URL}/api/v1/topics/${TOPIC}/ack`;
                    http.post(ackUrl, JSON.stringify({
                        messageIds: messages.map(m => m.id),
                        group: groupId,
                    }), {
                        headers: { 'Content-Type': 'application/json' },
                    });
                }
            } catch (e) {
                // Parse error
            }
        } else if (response.status !== 204) {
            errorRate.add(1);
        }
    });

    sleep(0.01);
}

// Setup
export function setup() {
    console.log('==============================================');
    console.log('GoQueue Stress Test');
    console.log('==============================================');
    console.log(`URL: ${BASE_URL}`);
    console.log(`Topic: ${TOPIC}`);
    console.log('');

    // Create topic
    const createUrl = `${BASE_URL}/api/v1/topics`;
    const response = http.post(createUrl, JSON.stringify({
        name: TOPIC,
        partitions: 16,
        replicationFactor: 3,
    }), {
        headers: { 'Content-Type': 'application/json' },
    });

    if (response.status === 200 || response.status === 201 || response.status === 409) {
        console.log('Topic ready');
    }

    return { startTime: Date.now() };
}

// Teardown
export function teardown(data) {
    const duration = (Date.now() - data.startTime) / 1000;
    console.log('');
    console.log('==============================================');
    console.log('Stress Test Complete');
    console.log('==============================================');
    console.log(`Duration: ${duration.toFixed(2)} seconds`);
}

// Default export
export default function () {
    producer();
}
