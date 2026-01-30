// =============================================================================
// GOQUEUE LOAD TEST - CONSUME MESSAGES
// =============================================================================
//
// PURPOSE: Test consumer performance and latency
//
// TARGETS:
//   - Consumer throughput matching producer
//   - End-to-end latency < 50ms
//   - No message loss
//
// USAGE:
//   k6 run consume.js
//
// =============================================================================

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

// Custom metrics
const messagesConsumed = new Counter('messages_consumed');
const consumeFailed = new Counter('consume_failed');
const consumeLatency = new Trend('consume_latency', true);
const e2eLatency = new Trend('e2e_latency', true);
const errorRate = new Rate('error_rate');

// Configuration
const BASE_URL = __ENV.GOQUEUE_URL || 'http://localhost:8080';
const TOPIC = __ENV.TOPIC || 'load-test';
const GROUP = __ENV.GROUP || 'k6-consumer-group';

export const options = {
    scenarios: {
        // Constant consumer load
        consume: {
            executor: 'constant-vus',
            vus: 10,
            duration: '5m',
            exec: 'consumeMessages',
        },
    },
    thresholds: {
        'consume_latency': ['p(99)<50'],
        'error_rate': ['rate<0.001'],
    },
};

// Consume messages
export function consumeMessages() {
    const url = `${BASE_URL}/api/v1/topics/${TOPIC}/consume?group=${GROUP}&timeout=1000`;

    const params = {
        headers: {
            'Content-Type': 'application/json',
        },
        timeout: '5s',
    };

    const startTime = Date.now();
    const response = http.get(url, params);
    const latency = Date.now() - startTime;

    consumeLatency.add(latency);

    const success = check(response, {
        'status is 200': (r) => r.status === 200,
        'has messages': (r) => {
            if (r.status === 200) {
                try {
                    const body = JSON.parse(r.body);
                    return body.messages && body.messages.length > 0;
                } catch {
                    return false;
                }
            }
            return false;
        },
    });

    if (success && response.status === 200) {
        try {
            const body = JSON.parse(response.body);
            const messages = body.messages || [];

            messages.forEach((msg) => {
                messagesConsumed.add(1);
                errorRate.add(0);

                // Calculate end-to-end latency if timestamp is in message
                if (msg.timestamp) {
                    const msgE2ELatency = Date.now() - msg.timestamp;
                    e2eLatency.add(msgE2ELatency);
                }

                // Acknowledge the message
                acknowledgeMessage(msg.id);
            });
        } catch (e) {
            console.log(`Failed to parse response: ${e}`);
        }
    } else if (response.status === 204) {
        // No messages available - this is OK
        sleep(0.1);
    } else {
        consumeFailed.add(1);
        errorRate.add(1);
    }

    sleep(0.01);
}

// Acknowledge a message
function acknowledgeMessage(messageId) {
    const url = `${BASE_URL}/api/v1/topics/${TOPIC}/ack`;
    const payload = {
        messageId: messageId,
        group: GROUP,
    };

    http.post(url, JSON.stringify(payload), {
        headers: { 'Content-Type': 'application/json' },
    });
}

// Setup
export function setup() {
    console.log(`Setting up consumer test for topic: ${TOPIC}`);
    console.log(`Consumer group: ${GROUP}`);
    return { topic: TOPIC, group: GROUP };
}

// Teardown
export function teardown(data) {
    console.log(`Consumer test complete for topic: ${data.topic}`);
}

export default function () {
    consumeMessages();
}
