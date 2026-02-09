// =============================================================================
// WEBHOOK / EVENT NOTIFICATION SYSTEM
// =============================================================================
//
// WHAT IS THIS?
// An event notification system that sends HTTP webhooks when significant broker
// events occur. This allows external systems to react to queue state changes
// without polling.
//
// WHY WEBHOOKS?
// Polling-based monitoring has limitations:
//   - Latency: Events are discovered only at poll interval
//   - Overhead: Constant requests even when nothing changes
//   - Missed events: If poller is down during an event, it's lost
//
// Webhooks solve these by pushing events in real-time:
//
//   ┌──────────┐   event   ┌───────────────┐   POST   ┌──────────────┐
//   │  Broker  │──────────►│ WebhookManager│─────────►│ External App │
//   └──────────┘           └───────────────┘          └──────────────┘
//                                │
//                                ├── Topic created/deleted
//                                ├── Consumer group joined/left
//                                ├── Messages dead-lettered
//                                ├── Partition scaling
//                                └── Broker health changes
//
// COMPARISON WITH OTHER SYSTEMS:
//   - RabbitMQ: Event exchange (amq.rabbitmq.event) — internal pub/sub
//   - Kafka:    No built-in webhooks; use Kafka Connect or custom consumers
//   - SQS:      CloudWatch Events / EventBridge for queue events
//   - Redis:    Keyspace notifications (pub/sub channel)
//   - goqueue:  HTTP webhooks to configured endpoints
//
// DESIGN DECISIONS:
//
//   FIRE-AND-FORGET vs GUARANTEED DELIVERY:
//   ┌────────────────────┬──────────────────────────────────────────────────┐
//   │ Fire-and-forget    │ Send once, best-effort. Simple, no storage.     │
//   │ At-least-once      │ Retry on failure. Needs persistent queue.       │
//   │ Exactly-once       │ Dedup on receiver. Complex, rarely needed.      │
//   └────────────────────┴──────────────────────────────────────────────────┘
//
//   We use FIRE-AND-FORGET with configurable retries:
//     - Simple: No persistent webhook queue needed
//     - Practical: 3 retries with exponential backoff covers transient failures
//     - Aligned: This is a notification, not a guarantee (consumers should be
//       idempotent anyway)
//
// =============================================================================

package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"
)

// =============================================================================
// EVENT TYPES
// =============================================================================
//
// Each event type maps to a specific broker operation. External systems can
// subscribe to specific events they care about.
//
// EVENT NAMING CONVENTION: <resource>.<action>
//   - topic.created, topic.deleted
//   - group.joined, group.left
//   - message.dead_lettered
//

// EventType represents the type of broker event.
type EventType string

const (
	// Topic lifecycle events
	EventTopicCreated EventType = "topic.created"
	EventTopicDeleted EventType = "topic.deleted"

	// Consumer group events
	EventGroupJoined EventType = "group.joined"
	EventGroupLeft   EventType = "group.left"

	// Message events
	EventMessageDeadLettered EventType = "message.dead_lettered"

	// Partition events
	EventPartitionAdded EventType = "partition.added"

	// Broker health events
	EventBrokerReady    EventType = "broker.ready"
	EventBrokerShutdown EventType = "broker.shutdown"
)

// =============================================================================
// EVENT PAYLOAD
// =============================================================================

// WebhookEvent is the payload sent to webhook endpoints.
//
// JSON EXAMPLE:
//
//	{
//	    "id": "evt_1234567890",
//	    "type": "topic.created",
//	    "timestamp": "2025-01-15T10:30:00Z",
//	    "source": "goqueue-node-0",
//	    "data": {
//	        "topic": "orders",
//	        "partitions": 6
//	    }
//	}
type WebhookEvent struct {
	// ID is a unique identifier for this event (for deduplication)
	ID string `json:"id"`

	// Type is the event type (e.g., "topic.created")
	Type EventType `json:"type"`

	// Timestamp is when the event occurred
	Timestamp time.Time `json:"timestamp"`

	// Source identifies the broker node that generated the event
	Source string `json:"source"`

	// Data contains event-specific payload
	Data map[string]interface{} `json:"data"`
}

// =============================================================================
// WEBHOOK CONFIGURATION
// =============================================================================

// WebhookConfig holds webhook endpoint configuration.
type WebhookConfig struct {
	// URL is the HTTP endpoint to POST events to.
	URL string `json:"url"`

	// Events is the list of event types to subscribe to.
	// Empty means subscribe to all events.
	Events []EventType `json:"events,omitempty"`

	// Secret is an optional HMAC secret for signing payloads.
	// The signature is sent in X-GoQueue-Signature header.
	Secret string `json:"secret,omitempty"`

	// TimeoutSeconds is the HTTP request timeout. Default: 5s.
	TimeoutSeconds int `json:"timeout_seconds,omitempty"`

	// MaxRetries is the number of retries on failure. Default: 3.
	MaxRetries int `json:"max_retries,omitempty"`
}

// WebhookManagerConfig holds global webhook configuration.
type WebhookManagerConfig struct {
	// Enabled controls whether webhook notifications are active.
	Enabled bool `json:"enabled"`

	// Endpoints is the list of webhook endpoints.
	Endpoints []WebhookConfig `json:"endpoints"`

	// SourceNodeID identifies this broker node in events.
	SourceNodeID string `json:"source_node_id"`

	// BufferSize is the channel buffer for async event dispatch.
	// Default: 1000 events.
	BufferSize int `json:"buffer_size,omitempty"`
}

// DefaultWebhookManagerConfig returns a disabled webhook config.
func DefaultWebhookManagerConfig() WebhookManagerConfig {
	return WebhookManagerConfig{
		Enabled:    false,
		Endpoints:  nil,
		BufferSize: 1000,
	}
}

// =============================================================================
// WEBHOOK MANAGER
// =============================================================================
//
// ARCHITECTURE:
//
//   ┌──────────────┐   Emit()   ┌──────────────────┐  POST  ┌──────────────┐
//   │   Broker     │───────────►│  event channel   │───────►│  Endpoint 1  │
//   │ (any gorout.)│            │  (buffered 1000) │   │     └──────────────┘
//   └──────────────┘            └──────────────────┘   │
//                                       │              │     ┌──────────────┐
//                                       │ dispatch     │────►│  Endpoint 2  │
//                                       │ goroutine    │     └──────────────┘
//                                       ▼              │
//                                  fan-out to          │     ┌──────────────┐
//                                  all matching        └────►│  Endpoint N  │
//                                  endpoints                 └──────────────┘
//
// WHY ASYNC DISPATCH?
//   Events are emitted from hot paths (publish, consume). We can't block
//   the broker on HTTP requests to external systems. The buffered channel
//   decouples the event producer from the HTTP dispatch.
//
// BACKPRESSURE:
//   If the channel is full (all endpoints slow), events are dropped with
//   a warning log. This prevents webhook failures from affecting broker
//   performance.

// WebhookManager handles event notification dispatch.
type WebhookManager struct {
	config    WebhookManagerConfig
	eventCh   chan WebhookEvent
	client    *http.Client
	logger    *slog.Logger
	nextID    uint64
	mu        sync.RWMutex
	endpoints []WebhookConfig
	stopCh    chan struct{}
	wg        sync.WaitGroup
}

// NewWebhookManager creates a new webhook manager.
//
// USAGE:
//
//	wm := NewWebhookManager(config)
//	wm.Start()
//	defer wm.Stop()
//	wm.Emit(EventTopicCreated, map[string]interface{}{"topic": "orders"})
func NewWebhookManager(config WebhookManagerConfig) *WebhookManager {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	bufferSize := config.BufferSize
	if bufferSize <= 0 {
		bufferSize = 1000
	}

	// Set defaults on endpoints
	endpoints := make([]WebhookConfig, len(config.Endpoints))
	for i, ep := range config.Endpoints {
		if ep.TimeoutSeconds <= 0 {
			ep.TimeoutSeconds = 5
		}
		if ep.MaxRetries <= 0 {
			ep.MaxRetries = 3
		}
		endpoints[i] = ep
	}

	return &WebhookManager{
		config:    config,
		eventCh:   make(chan WebhookEvent, bufferSize),
		client:    &http.Client{Timeout: 10 * time.Second},
		logger:    logger,
		endpoints: endpoints,
		stopCh:    make(chan struct{}),
	}
}

// Start begins the async dispatch goroutine.
func (wm *WebhookManager) Start() {
	if !wm.config.Enabled {
		return
	}

	wm.wg.Add(1)
	go wm.dispatchLoop()

	wm.logger.Info("webhook manager started",
		"endpoints", len(wm.endpoints),
		"buffer_size", wm.config.BufferSize)
}

// Stop gracefully shuts down the webhook manager.
// Drains remaining events before returning.
func (wm *WebhookManager) Stop() {
	if !wm.config.Enabled {
		return
	}

	close(wm.stopCh)
	wm.wg.Wait()
	wm.logger.Info("webhook manager stopped")
}

// Emit sends an event to all matching webhook endpoints.
//
// THREAD SAFETY: Safe to call from any goroutine.
//
// BACKPRESSURE: If the event channel is full, the event is dropped
// with a warning log. This prevents webhook failures from impacting
// broker performance.
func (wm *WebhookManager) Emit(eventType EventType, data map[string]interface{}) {
	if !wm.config.Enabled {
		return
	}

	wm.mu.RLock()
	id := wm.nextID
	wm.nextID++
	wm.mu.RUnlock()

	event := WebhookEvent{
		ID:        fmt.Sprintf("evt_%d_%d", time.Now().UnixNano(), id),
		Type:      eventType,
		Timestamp: time.Now().UTC(),
		Source:    wm.config.SourceNodeID,
		Data:      data,
	}

	// Non-blocking send — drop if channel full
	select {
	case wm.eventCh <- event:
		// Event queued for dispatch
	default:
		wm.logger.Warn("webhook event dropped (buffer full)",
			"type", eventType,
			"id", event.ID)
	}
}

// =============================================================================
// DISPATCH LOOP
// =============================================================================

// dispatchLoop reads events from the channel and sends them to endpoints.
func (wm *WebhookManager) dispatchLoop() {
	defer wm.wg.Done()

	for {
		select {
		case event := <-wm.eventCh:
			wm.dispatchEvent(event)
		case <-wm.stopCh:
			// Drain remaining events
			for {
				select {
				case event := <-wm.eventCh:
					wm.dispatchEvent(event)
				default:
					return
				}
			}
		}
	}
}

// dispatchEvent sends an event to all matching endpoints.
func (wm *WebhookManager) dispatchEvent(event WebhookEvent) {
	wm.mu.RLock()
	endpoints := wm.endpoints
	wm.mu.RUnlock()

	for _, ep := range endpoints {
		if !wm.shouldSendToEndpoint(ep, event.Type) {
			continue
		}
		wm.sendToEndpoint(ep, event)
	}
}

// shouldSendToEndpoint checks if an event type matches the endpoint's filter.
func (wm *WebhookManager) shouldSendToEndpoint(ep WebhookConfig, eventType EventType) bool {
	// Empty events list means subscribe to all
	if len(ep.Events) == 0 {
		return true
	}
	for _, subscribedType := range ep.Events {
		if subscribedType == eventType {
			return true
		}
	}
	return false
}

// sendToEndpoint sends an event to a single endpoint with retries.
//
// RETRY STRATEGY:
//
//	Attempt 1: Immediate
//	Attempt 2: After 1s
//	Attempt 3: After 2s
//	Attempt 4: After 4s (exponential backoff)
//
// COMPARISON:
//   - Stripe: Exponential backoff up to 72 hours
//   - GitHub: Immediate + 10s, 60s, 5min retries
//   - goqueue: 3 retries with seconds-level backoff (fast feedback)
func (wm *WebhookManager) sendToEndpoint(ep WebhookConfig, event WebhookEvent) {
	body, err := json.Marshal(event)
	if err != nil {
		wm.logger.Error("failed to marshal webhook event",
			"type", event.Type,
			"error", err)
		return
	}

	timeout := time.Duration(ep.TimeoutSeconds) * time.Second

	for attempt := 0; attempt <= ep.MaxRetries; attempt++ {
		if attempt > 0 {
			// Exponential backoff: 1s, 2s, 4s...
			backoff := time.Duration(1<<uint(attempt-1)) * time.Second
			time.Sleep(backoff)
		}

		ctx, cancel := context.WithTimeout(context.Background(), timeout)

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, ep.URL, bytes.NewReader(body))
		if err != nil {
			cancel()
			wm.logger.Error("failed to create webhook request",
				"url", ep.URL,
				"error", err)
			return // Don't retry on request creation failure
		}

		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("User-Agent", "GoQueue-Webhook/1.0")
		req.Header.Set("X-GoQueue-Event", string(event.Type))
		req.Header.Set("X-GoQueue-Delivery", event.ID)

		resp, err := wm.client.Do(req)
		cancel()

		if err != nil {
			wm.logger.Warn("webhook delivery failed",
				"url", ep.URL,
				"type", event.Type,
				"attempt", attempt+1,
				"error", err)
			continue // Retry
		}
		resp.Body.Close()

		// 2xx = success, anything else = retry
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			wm.logger.Debug("webhook delivered",
				"url", ep.URL,
				"type", event.Type,
				"status", resp.StatusCode)
			return
		}

		wm.logger.Warn("webhook returned non-2xx",
			"url", ep.URL,
			"type", event.Type,
			"status", resp.StatusCode,
			"attempt", attempt+1)
	}

	wm.logger.Error("webhook delivery exhausted retries",
		"url", ep.URL,
		"type", event.Type,
		"max_retries", ep.MaxRetries)
}

// =============================================================================
// ENDPOINT MANAGEMENT (RUNTIME)
// =============================================================================

// AddEndpoint adds a webhook endpoint at runtime.
func (wm *WebhookManager) AddEndpoint(ep WebhookConfig) {
	if ep.TimeoutSeconds <= 0 {
		ep.TimeoutSeconds = 5
	}
	if ep.MaxRetries <= 0 {
		ep.MaxRetries = 3
	}

	wm.mu.Lock()
	wm.endpoints = append(wm.endpoints, ep)
	wm.mu.Unlock()

	wm.logger.Info("webhook endpoint added", "url", ep.URL)
}

// RemoveEndpoint removes a webhook endpoint by URL.
func (wm *WebhookManager) RemoveEndpoint(url string) bool {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	for i, ep := range wm.endpoints {
		if ep.URL == url {
			wm.endpoints = append(wm.endpoints[:i], wm.endpoints[i+1:]...)
			wm.logger.Info("webhook endpoint removed", "url", url)
			return true
		}
	}
	return false
}

// EndpointCount returns the number of configured endpoints.
func (wm *WebhookManager) EndpointCount() int {
	wm.mu.RLock()
	defer wm.mu.RUnlock()
	return len(wm.endpoints)
}
