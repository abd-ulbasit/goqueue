// Package goqueue provides a Go client for the GoQueue HTTP/REST API.
//
// GoQueue is a high-performance distributed message queue that combines
// the best features of Kafka (log-based storage, partitions), SQS
// (visibility timeouts, DLQ), and RabbitMQ (priority queues).
//
// # Quick Start
//
//	client := goqueue.NewClient("http://localhost:8080")
//	defer client.Close()
//
//	// Create a topic
//	_, err := client.Topics.Create(ctx, &goqueue.CreateTopicRequest{
//		Name:          "orders",
//		NumPartitions: 3,
//	})
//
//	// Publish a message
//	_, err := client.Messages.Publish(ctx, "orders", []*goqueue.PublishMessage{
//		{Value: `{"orderId": "12345"}`},
//	})
//
//	// Consume with consumer groups
//	join, _ := client.Groups.Join(ctx, "order-processors", &goqueue.JoinGroupRequest{
//		ClientID: "worker-1",
//		Topics:   []string{"orders"},
//	})
//
//	resp, _ := client.Groups.Poll(ctx, "order-processors", join.MemberID, nil)
//	for _, msg := range resp.Messages {
//		// Process message
//		client.Messages.Ack(ctx, msg.ReceiptHandle, nil)
//	}
//
// # Architecture
//
// The client is organized into service-specific APIs:
//
//	Client
//	├── Health    - Health checks and probes
//	├── Topics    - Topic management
//	├── Messages  - Publish, consume, ack/nack
//	├── Delayed   - Delayed message operations
//	├── Groups    - Consumer group operations
//	├── Priority  - Priority queue statistics
//	├── Schemas   - Schema registry
//	├── Transactions - Exactly-once delivery
//	├── Tracing   - Message tracing
//	└── Admin     - Administrative operations
//
// # Comparison with Other Go Clients
//
//   - confluent-kafka-go: Wraps librdkafka C library, cgo dependency
//   - sarama: Pure Go Kafka client, complex callbacks
//   - aws-sdk-go-v2/sqs: Verbose SDK patterns
//   - goqueue: Simple HTTP client, no dependencies, works anywhere
//
// # Error Handling
//
// All errors are returned as *Error which includes the HTTP status code
// and response body:
//
//	_, err := client.Topics.Get(ctx, "non-existent")
//	if err != nil {
//		var qErr *goqueue.Error
//		if errors.As(err, &qErr) {
//			fmt.Printf("Status: %d\n", qErr.Status)
//		}
//	}
package goqueue

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// =============================================================================
// ERROR TYPE
// =============================================================================

// Error represents an error from the GoQueue API.
//
// It includes the HTTP status code and the original response body
// for detailed error handling.
type Error struct {
	// Message is a human-readable error message.
	Message string

	// Status is the HTTP status code.
	Status int

	// Body is the raw response body if available.
	Body []byte
}

// Error implements the error interface.
func (e *Error) Error() string {
	return fmt.Sprintf("goqueue: %s (status=%d)", e.Message, e.Status)
}

// =============================================================================
// CLIENT CONFIGURATION
// =============================================================================

// ClientOption configures a Client.
type ClientOption func(*clientConfig)

type clientConfig struct {
	timeout      time.Duration
	headers      map[string]string
	maxRetries   int
	initialDelay time.Duration
	maxDelay     time.Duration
	httpClient   *http.Client
}

// WithTimeout sets the request timeout.
// Default: 30 seconds.
func WithTimeout(timeout time.Duration) ClientOption {
	return func(c *clientConfig) {
		c.timeout = timeout
	}
}

// WithHeaders adds custom headers to all requests.
func WithHeaders(headers map[string]string) ClientOption {
	return func(c *clientConfig) {
		c.headers = headers
	}
}

// WithMaxRetries sets the maximum number of retry attempts.
// Default: 3.
func WithMaxRetries(n int) ClientOption {
	return func(c *clientConfig) {
		c.maxRetries = n
	}
}

// WithRetryDelays sets the initial and maximum retry delays.
// Default: 100ms initial, 5s max.
func WithRetryDelays(initial, max time.Duration) ClientOption {
	return func(c *clientConfig) {
		c.initialDelay = initial
		c.maxDelay = max
	}
}

// WithHTTPClient sets a custom HTTP client.
// Useful for custom TLS configuration or proxies.
func WithHTTPClient(client *http.Client) ClientOption {
	return func(c *clientConfig) {
		c.httpClient = client
	}
}

// =============================================================================
// MAIN CLIENT
// =============================================================================

// Client is the main GoQueue client.
//
// It provides access to all GoQueue features through service-specific APIs.
// Create a client with NewClient and close it when done.
//
// Example:
//
//	client := goqueue.NewClient("http://localhost:8080")
//	defer client.Close()
//
//	// Use the client
//	health, _ := client.Health.Check(ctx)
//	fmt.Println(health.Status)
type Client struct {
	// Health provides health check operations.
	Health *HealthService

	// Topics provides topic management operations.
	Topics *TopicsService

	// Messages provides message publish/consume operations.
	Messages *MessagesService

	// Delayed provides delayed message operations.
	Delayed *DelayedService

	// Groups provides consumer group operations.
	Groups *GroupsService

	// Priority provides priority queue statistics.
	Priority *PriorityService

	// Schemas provides schema registry operations.
	Schemas *SchemasService

	// Transactions provides transaction operations.
	Transactions *TransactionsService

	// Tracing provides message tracing operations.
	Tracing *TracingService

	// Admin provides administrative operations.
	Admin *AdminService

	http *httpClient
}

// NewClient creates a new GoQueue client.
//
// The baseURL should be the base URL of the GoQueue server,
// e.g., "http://localhost:8080" or "https://queue.example.com".
//
// Example:
//
//	// Basic usage
//	client := goqueue.NewClient("http://localhost:8080")
//
//	// With options
//	client := goqueue.NewClient("http://localhost:8080",
//		goqueue.WithTimeout(60*time.Second),
//		goqueue.WithHeaders(map[string]string{
//			"X-Tenant-ID": "my-tenant",
//		}),
//		goqueue.WithMaxRetries(5),
//	)
func NewClient(baseURL string, opts ...ClientOption) *Client {
	cfg := &clientConfig{
		timeout:      30 * time.Second,
		headers:      make(map[string]string),
		maxRetries:   3,
		initialDelay: 100 * time.Millisecond,
		maxDelay:     5 * time.Second,
	}

	for _, opt := range opts {
		opt(cfg)
	}

	if cfg.httpClient == nil {
		cfg.httpClient = &http.Client{
			Timeout: cfg.timeout,
		}
	}

	h := &httpClient{
		baseURL:      strings.TrimRight(baseURL, "/"),
		httpClient:   cfg.httpClient,
		headers:      cfg.headers,
		maxRetries:   cfg.maxRetries,
		initialDelay: cfg.initialDelay,
		maxDelay:     cfg.maxDelay,
	}

	return &Client{
		Health:       &HealthService{http: h},
		Topics:       &TopicsService{http: h},
		Messages:     &MessagesService{http: h},
		Delayed:      &DelayedService{http: h},
		Groups:       &GroupsService{http: h},
		Priority:     &PriorityService{http: h},
		Schemas:      &SchemasService{http: h},
		Transactions: &TransactionsService{http: h},
		Tracing:      &TracingService{http: h},
		Admin:        &AdminService{http: h},
		http:         h,
	}
}

// Close releases any resources held by the client.
func (c *Client) Close() error {
	// Currently no cleanup needed, but provided for future compatibility
	return nil
}

// =============================================================================
// HTTP CLIENT (INTERNAL)
// =============================================================================

type httpClient struct {
	baseURL      string
	httpClient   *http.Client
	headers      map[string]string
	maxRetries   int
	initialDelay time.Duration
	maxDelay     time.Duration
}

func (h *httpClient) request(ctx context.Context, method, path string, body, result any) error {
	return h.requestWithParams(ctx, method, path, nil, body, result)
}

func (h *httpClient) requestWithParams(ctx context.Context, method, path string, params url.Values, body, result any) error {
	var lastErr error

	for attempt := 0; attempt <= h.maxRetries; attempt++ {
		err := h.doRequest(ctx, method, path, params, body, result)
		if err == nil {
			return nil
		}

		// Don't retry client errors (4xx)
		var qErr *Error
		if ok := errorAs(err, &qErr); ok && qErr.Status >= 400 && qErr.Status < 500 {
			return err
		}

		lastErr = err

		// Wait before retrying
		if attempt < h.maxRetries {
			delay := h.initialDelay * time.Duration(1<<attempt)
			if delay > h.maxDelay {
				delay = h.maxDelay
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
		}
	}

	return lastErr
}

func (h *httpClient) doRequest(ctx context.Context, method, path string, params url.Values, body, result any) error {
	// Build URL
	reqURL := h.baseURL + path
	if len(params) > 0 {
		reqURL += "?" + params.Encode()
	}

	// Build request body
	var bodyReader io.Reader
	if body != nil {
		bodyBytes, err := json.Marshal(body)
		if err != nil {
			return &Error{Message: fmt.Sprintf("failed to encode body: %v", err), Status: 0}
		}
		bodyReader = bytes.NewReader(bodyBytes)
	}

	// Create request
	req, err := http.NewRequestWithContext(ctx, method, reqURL, bodyReader)
	if err != nil {
		return &Error{Message: fmt.Sprintf("failed to create request: %v", err), Status: 0}
	}

	// Set headers
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	for k, v := range h.headers {
		req.Header.Set(k, v)
	}

	// Execute request
	resp, err := h.httpClient.Do(req)
	if err != nil {
		return &Error{Message: fmt.Sprintf("request failed: %v", err), Status: 0}
	}
	defer resp.Body.Close()

	// Read body
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return &Error{Message: fmt.Sprintf("failed to read response: %v", err), Status: resp.StatusCode}
	}

	// Handle errors
	if resp.StatusCode >= 400 {
		var errResp struct {
			Error string `json:"error"`
		}
		if json.Unmarshal(respBody, &errResp) == nil && errResp.Error != "" {
			return &Error{Message: errResp.Error, Status: resp.StatusCode, Body: respBody}
		}
		return &Error{Message: fmt.Sprintf("request failed: %s", resp.Status), Status: resp.StatusCode, Body: respBody}
	}

	// Parse result
	if result != nil {
		// Handle plain text response (like /metrics)
		if strPtr, ok := result.(*string); ok {
			*strPtr = string(respBody)
			return nil
		}

		if err := json.Unmarshal(respBody, result); err != nil {
			return &Error{Message: fmt.Sprintf("failed to decode response: %v", err), Status: resp.StatusCode}
		}
	}

	return nil
}

// errorAs is a simplified errors.As for our Error type
func errorAs(err error, target **Error) bool {
	if e, ok := err.(*Error); ok {
		*target = e
		return true
	}
	return false
}

// =============================================================================
// TYPES
// =============================================================================

// --- Health Types ---

// HealthResponse is returned by the health check endpoint.
type HealthResponse struct {
	Status    string `json:"status"`
	Timestamp string `json:"timestamp,omitempty"`
}

// LivenessResponse is returned by the liveness probe endpoint.
type LivenessResponse struct {
	Status string `json:"status"`
}

// ReadinessResponse is returned by the readiness probe endpoint.
type ReadinessResponse struct {
	Status string                    `json:"status"`
	Checks map[string]ReadinessCheck `json:"checks,omitempty"`
}

// ReadinessCheck represents a single readiness check result.
type ReadinessCheck struct {
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

// VersionResponse is returned by the version endpoint.
type VersionResponse struct {
	Version   string `json:"version"`
	GitCommit string `json:"git_commit,omitempty"`
	BuildTime string `json:"build_time,omitempty"`
	GoVersion string `json:"go_version,omitempty"`
}

// StatsResponse is returned by the stats endpoint.
type StatsResponse struct {
	NodeID         string   `json:"node_id"`
	Uptime         string   `json:"uptime"`
	Topics         int      `json:"topics"`
	TotalSizeBytes int64    `json:"total_size_bytes"`
	TopicList      []string `json:"topic_list,omitempty"`
}

// --- Topic Types ---

// CreateTopicRequest contains the parameters for creating a topic.
type CreateTopicRequest struct {
	// Name is the topic name. Required.
	Name string `json:"name"`

	// NumPartitions is the number of partitions. Default: 1.
	NumPartitions int `json:"num_partitions,omitempty"`

	// RetentionHours is how long to retain messages. Default: server config.
	RetentionHours int `json:"retention_hours,omitempty"`
}

// CreateTopicResponse is returned when a topic is created.
type CreateTopicResponse struct {
	Name       string `json:"name"`
	Partitions int    `json:"partitions"`
	Created    bool   `json:"created"`
}

// TopicListResponse is returned when listing topics.
type TopicListResponse struct {
	Topics []string `json:"topics"`
}

// TopicDetails contains detailed information about a topic.
type TopicDetails struct {
	Name          string          `json:"name"`
	NumPartitions int             `json:"num_partitions"`
	TotalMessages int64           `json:"total_messages"`
	TotalSizeKB   float64         `json:"total_size_kb"`
	Partitions    []PartitionInfo `json:"partitions"`
}

// PartitionInfo contains information about a single partition.
type PartitionInfo struct {
	ID           int     `json:"id"`
	StartOffset  int64   `json:"start_offset"`
	EndOffset    int64   `json:"end_offset"`
	MessageCount int64   `json:"message_count"`
	SizeKB       float64 `json:"size_kb,omitempty"`
}

// --- Message Types ---

// PublishMessage represents a message to be published.
type PublishMessage struct {
	// Key is the partition key (optional). Messages with the same key
	// go to the same partition, preserving order.
	Key string `json:"key,omitempty"`

	// Value is the message payload. Required.
	Value string `json:"value"`

	// Headers are optional message headers.
	Headers map[string]string `json:"headers,omitempty"`

	// Priority is the message priority: "low", "normal", "high", "critical".
	Priority string `json:"priority,omitempty"`

	// Delay is the delivery delay (e.g., "30s", "1h", "24h").
	Delay string `json:"delay,omitempty"`
}

// PublishRequest is the request body for publishing messages.
type PublishRequest struct {
	Messages []*PublishMessage `json:"messages"`
}

// PublishResponse is returned when publishing messages.
type PublishResponse struct {
	Results []*PublishResult `json:"results"`
}

// PublishResult is the result for a single published message.
type PublishResult struct {
	Partition int    `json:"partition,omitempty"`
	Offset    int64  `json:"offset,omitempty"`
	Error     string `json:"error,omitempty"`
}

// ConsumeResponse is returned when consuming messages.
type ConsumeResponse struct {
	Messages   []*Message `json:"messages"`
	NextOffset int64      `json:"next_offset"`
}

// Message represents a consumed message.
type Message struct {
	Topic         string            `json:"topic"`
	Partition     int               `json:"partition"`
	Offset        int64             `json:"offset"`
	Key           string            `json:"key,omitempty"`
	Value         string            `json:"value"`
	Timestamp     string            `json:"timestamp"`
	Headers       map[string]string `json:"headers,omitempty"`
	ReceiptHandle string            `json:"receipt_handle,omitempty"`
	Priority      string            `json:"priority,omitempty"`
}

// AckRequest is the request for acknowledging a message.
type AckRequest struct {
	ReceiptHandle string `json:"receipt_handle"`
	ConsumerID    string `json:"consumer_id,omitempty"`
}

// NackRequest is the request for negative acknowledging a message.
type NackRequest struct {
	ReceiptHandle string `json:"receipt_handle"`
	Delay         string `json:"delay,omitempty"`
}

// RejectRequest is the request for rejecting a message to DLQ.
type RejectRequest struct {
	ReceiptHandle string `json:"receipt_handle"`
	Reason        string `json:"reason,omitempty"`
}

// ExtendVisibilityRequest is the request for extending visibility timeout.
type ExtendVisibilityRequest struct {
	ReceiptHandle string `json:"receipt_handle"`
	Timeout       string `json:"timeout"`
}

// ReliabilityStats contains ACK/NACK/DLQ statistics.
type ReliabilityStats struct {
	AcksTotal    int64 `json:"acks_total"`
	NacksTotal   int64 `json:"nacks_total"`
	RejectsTotal int64 `json:"rejects_total"`
	DLQMessages  int64 `json:"dlq_messages"`
}

// --- Delayed Types ---

// DelayedMessagesResponse is returned when listing delayed messages.
type DelayedMessagesResponse struct {
	Messages []*DelayedMessage `json:"messages"`
	Count    int               `json:"count"`
}

// DelayedMessage represents a delayed message.
type DelayedMessage struct {
	Topic      string `json:"topic"`
	Partition  int    `json:"partition"`
	Offset     int64  `json:"offset"`
	DeliverAt  string `json:"deliver_at"`
	Value      string `json:"value,omitempty"`
	InsertedAt string `json:"inserted_at,omitempty"`
}

// DelayStats contains delay queue statistics.
type DelayStats struct {
	PendingCount   int64 `json:"pending_count"`
	DeliveredCount int64 `json:"delivered_count"`
	CancelledCount int64 `json:"cancelled_count"`
}

// --- Consumer Group Types ---

// GroupListResponse is returned when listing consumer groups.
type GroupListResponse struct {
	Groups []string `json:"groups"`
}

// GroupDetails contains details about a consumer group.
type GroupDetails struct {
	GroupID    string         `json:"group_id"`
	State      string         `json:"state"`
	Generation int            `json:"generation"`
	Members    []*GroupMember `json:"members"`
}

// GroupMember represents a member of a consumer group.
type GroupMember struct {
	MemberID   string   `json:"member_id"`
	ClientID   string   `json:"client_id"`
	Partitions []string `json:"partitions"`
}

// JoinGroupRequest contains the parameters for joining a consumer group.
type JoinGroupRequest struct {
	// ClientID is a unique identifier for this client.
	ClientID string `json:"client_id"`

	// Topics is the list of topics to subscribe to.
	Topics []string `json:"topics"`

	// SessionTimeout is the session timeout (e.g., "30s").
	SessionTimeout string `json:"session_timeout,omitempty"`
}

// JoinGroupResponse is returned when joining a consumer group.
type JoinGroupResponse struct {
	MemberID           string   `json:"member_id"`
	Generation         int      `json:"generation"`
	Leader             string   `json:"leader,omitempty"`
	AssignedPartitions []string `json:"assigned_partitions"`
}

// HeartbeatRequest contains the parameters for sending a heartbeat.
type HeartbeatRequest struct {
	MemberID   string `json:"member_id"`
	Generation int    `json:"generation"`
}

// HeartbeatResponse is returned when sending a heartbeat.
type HeartbeatResponse struct {
	RebalanceRequired bool `json:"rebalance_required"`
}

// LeaveGroupRequest contains the parameters for leaving a consumer group.
type LeaveGroupRequest struct {
	MemberID string `json:"member_id"`
}

// PollOptions contains optional parameters for polling.
type PollOptions struct {
	MaxMessages int
	Timeout     string
}

// PollResponse is returned when polling for messages.
type PollResponse struct {
	Messages []*Message `json:"messages"`
}

// --- Offset Types ---

// OffsetsResponse is returned when getting committed offsets.
type OffsetsResponse struct {
	Offsets map[string]map[int]int64 `json:"offsets"`
}

// CommitOffsetsRequest contains the parameters for committing offsets.
type CommitOffsetsRequest struct {
	MemberID string          `json:"member_id"`
	Offsets  []*OffsetCommit `json:"offsets"`
}

// OffsetCommit represents an offset to commit.
type OffsetCommit struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
}

// --- Priority Types ---

// PriorityStats contains priority queue statistics.
type PriorityStats struct {
	MessagesByPriority    map[string]int64  `json:"messages_by_priority"`
	AvgWaitTimeByPriority map[string]string `json:"avg_wait_time_by_priority"`
}

// --- Schema Types ---

// RegisterSchemaRequest contains the parameters for registering a schema.
type RegisterSchemaRequest struct {
	Schema     string `json:"schema"`
	SchemaType string `json:"schemaType,omitempty"`
}

// RegisterSchemaResponse is returned when registering a schema.
type RegisterSchemaResponse struct {
	ID int `json:"id"`
}

// SchemaVersion contains details about a schema version.
type SchemaVersion struct {
	Subject    string `json:"subject"`
	Version    int    `json:"version"`
	ID         int    `json:"id"`
	Schema     string `json:"schema"`
	SchemaType string `json:"schemaType,omitempty"`
}

// Schema contains a schema definition.
type Schema struct {
	Schema     string `json:"schema"`
	SchemaType string `json:"schemaType,omitempty"`
}

// CompatibilityConfig contains compatibility configuration.
type CompatibilityConfig struct {
	CompatibilityLevel string `json:"compatibilityLevel"`
}

// SchemaStats contains schema registry statistics.
type SchemaStats struct {
	Subjects int `json:"subjects"`
	Schemas  int `json:"schemas"`
	Versions int `json:"versions"`
}

// --- Transaction Types ---

// InitProducerRequest contains the parameters for initializing a producer.
type InitProducerRequest struct {
	TransactionalID string `json:"transactional_id,omitempty"`
}

// InitProducerResponse is returned when initializing a producer.
type InitProducerResponse struct {
	ProducerID int64 `json:"producer_id"`
	Epoch      int   `json:"epoch"`
}

// ProducerHeartbeatRequest contains the parameters for a producer heartbeat.
type ProducerHeartbeatRequest struct {
	Epoch int `json:"epoch"`
}

// BeginTransactionRequest contains the parameters for beginning a transaction.
type BeginTransactionRequest struct {
	ProducerID      int64  `json:"producer_id"`
	Epoch           int    `json:"epoch"`
	TransactionalID string `json:"transactional_id,omitempty"`
}

// BeginTransactionResponse is returned when beginning a transaction.
type BeginTransactionResponse struct {
	TransactionID string `json:"transaction_id"`
}

// TransactionalPublishRequest contains the parameters for transactional publish.
type TransactionalPublishRequest struct {
	ProducerID int64             `json:"producer_id"`
	Epoch      int               `json:"epoch"`
	Topic      string            `json:"topic"`
	Partition  *int              `json:"partition,omitempty"`
	Key        string            `json:"key,omitempty"`
	Value      string            `json:"value"`
	Headers    map[string]string `json:"headers,omitempty"`
	Sequence   int               `json:"sequence"`
}

// TransactionalPublishResponse is returned when publishing transactionally.
type TransactionalPublishResponse struct {
	Partition int   `json:"partition"`
	Offset    int64 `json:"offset"`
}

// CommitTransactionRequest contains the parameters for committing a transaction.
type CommitTransactionRequest struct {
	ProducerID      int64  `json:"producer_id"`
	Epoch           int    `json:"epoch"`
	TransactionalID string `json:"transactional_id,omitempty"`
}

// AbortTransactionRequest contains the parameters for aborting a transaction.
type AbortTransactionRequest struct {
	ProducerID      int64  `json:"producer_id"`
	Epoch           int    `json:"epoch"`
	TransactionalID string `json:"transactional_id,omitempty"`
}

// TransactionListResponse is returned when listing transactions.
type TransactionListResponse struct {
	Transactions []*TransactionInfo `json:"transactions"`
}

// TransactionInfo contains information about a transaction.
type TransactionInfo struct {
	TransactionID   string `json:"transaction_id"`
	ProducerID      int64  `json:"producer_id"`
	State           string `json:"state"`
	StartTime       string `json:"start_time"`
	TransactionalID string `json:"transactional_id,omitempty"`
}

// TransactionStats contains transaction statistics.
type TransactionStats struct {
	ActiveTransactions int64 `json:"active_transactions"`
	CommittedTotal     int64 `json:"committed_total"`
	AbortedTotal       int64 `json:"aborted_total"`
}

// --- Tracing Types ---

// TraceListResponse is returned when listing traces.
type TraceListResponse struct {
	Traces []*Trace `json:"traces"`
}

// Trace contains information about a message trace.
type Trace struct {
	TraceID   string        `json:"trace_id"`
	Topic     string        `json:"topic"`
	Partition int           `json:"partition"`
	Offset    int64         `json:"offset"`
	Status    string        `json:"status"`
	StartTime string        `json:"start_time"`
	EndTime   string        `json:"end_time,omitempty"`
	Events    []*TraceEvent `json:"events,omitempty"`
}

// TraceEvent represents an event in a message trace.
type TraceEvent struct {
	EventType string `json:"event_type"`
	Timestamp string `json:"timestamp"`
	Details   string `json:"details,omitempty"`
}

// TraceSearchParams contains parameters for searching traces.
type TraceSearchParams struct {
	Topic     string
	Partition *int
	Status    string
	Start     string
	End       string
	Limit     int
}

// TracerStats contains tracer statistics.
type TracerStats struct {
	TracesTotal     int64 `json:"traces_total"`
	TracesCompleted int64 `json:"traces_completed"`
	TracesError     int64 `json:"traces_error"`
}

// --- Admin Types ---

// AddPartitionsRequest contains the parameters for adding partitions.
type AddPartitionsRequest struct {
	Count int `json:"count"`
}

// AddPartitionsResponse is returned when adding partitions.
type AddPartitionsResponse struct {
	Success         bool `json:"success"`
	OldCount        int  `json:"old_count"`
	NewCount        int  `json:"new_count"`
	PartitionsAdded int  `json:"partitions_added"`
}

// TenantListResponse is returned when listing tenants.
type TenantListResponse struct {
	Tenants []*Tenant `json:"tenants"`
}

// CreateTenantRequest contains the parameters for creating a tenant.
type CreateTenantRequest struct {
	Name   string        `json:"name"`
	Quotas *TenantQuotas `json:"quotas,omitempty"`
}

// Tenant contains information about a tenant.
type Tenant struct {
	ID        string        `json:"id"`
	Name      string        `json:"name"`
	Quotas    *TenantQuotas `json:"quotas,omitempty"`
	CreatedAt string        `json:"created_at,omitempty"`
}

// TenantQuotas contains tenant resource quotas.
type TenantQuotas struct {
	MaxTopics             int `json:"max_topics,omitempty"`
	MaxPartitionsPerTopic int `json:"max_partitions_per_topic,omitempty"`
	MaxMessagesPerSecond  int `json:"max_messages_per_second,omitempty"`
}

// =============================================================================
// SERVICE IMPLEMENTATIONS
// =============================================================================

// HealthService provides health check operations.
type HealthService struct {
	http *httpClient
}

// Check performs a basic health check.
//
// Returns the overall health status of the server.
func (s *HealthService) Check(ctx context.Context) (*HealthResponse, error) {
	var resp HealthResponse
	if err := s.http.request(ctx, "GET", "/health", nil, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// Liveness performs a Kubernetes liveness probe.
//
// Use for livenessProbe in Kubernetes deployments.
// Failing causes the container to be killed and restarted.
func (s *HealthService) Liveness(ctx context.Context) (*LivenessResponse, error) {
	var resp LivenessResponse
	if err := s.http.request(ctx, "GET", "/healthz", nil, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// Readiness performs a Kubernetes readiness probe.
//
// Use for readinessProbe in Kubernetes deployments.
// Failing removes the pod from service endpoints.
func (s *HealthService) Readiness(ctx context.Context, verbose bool) (*ReadinessResponse, error) {
	params := url.Values{}
	if verbose {
		params.Set("verbose", "true")
	}
	var resp ReadinessResponse
	if err := s.http.requestWithParams(ctx, "GET", "/readyz", params, nil, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// Startup performs a Kubernetes startup probe.
//
// Use for startupProbe in Kubernetes deployments.
func (s *HealthService) Startup(ctx context.Context) (*LivenessResponse, error) {
	var resp LivenessResponse
	if err := s.http.request(ctx, "GET", "/livez", nil, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// Version returns version information about the server.
func (s *HealthService) Version(ctx context.Context) (*VersionResponse, error) {
	var resp VersionResponse
	if err := s.http.request(ctx, "GET", "/version", nil, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// Stats returns operational statistics about the broker.
func (s *HealthService) Stats(ctx context.Context) (*StatsResponse, error) {
	var resp StatsResponse
	if err := s.http.request(ctx, "GET", "/stats", nil, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// Metrics returns Prometheus metrics in text format.
func (s *HealthService) Metrics(ctx context.Context) (string, error) {
	var resp string
	if err := s.http.request(ctx, "GET", "/metrics", nil, &resp); err != nil {
		return "", err
	}
	return resp, nil
}

// TopicsService provides topic management operations.
type TopicsService struct {
	http *httpClient
}

// List returns all topic names.
func (s *TopicsService) List(ctx context.Context) (*TopicListResponse, error) {
	var resp TopicListResponse
	if err := s.http.request(ctx, "GET", "/topics", nil, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// Create creates a new topic.
//
// Topics are the primary unit of organization in GoQueue.
// Messages are published to topics and consumed from topics.
func (s *TopicsService) Create(ctx context.Context, req *CreateTopicRequest) (*CreateTopicResponse, error) {
	var resp CreateTopicResponse
	if err := s.http.request(ctx, "POST", "/topics", req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// Get returns detailed information about a topic.
func (s *TopicsService) Get(ctx context.Context, name string) (*TopicDetails, error) {
	var resp TopicDetails
	if err := s.http.request(ctx, "GET", "/topics/"+url.PathEscape(name), nil, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// Delete permanently deletes a topic and all its data.
func (s *TopicsService) Delete(ctx context.Context, name string) error {
	return s.http.request(ctx, "DELETE", "/topics/"+url.PathEscape(name), nil, nil)
}

// MessagesService provides message publish and consume operations.
type MessagesService struct {
	http *httpClient
}

// Publish publishes messages to a topic.
//
// Messages can include keys for ordering, priorities, and delays.
func (s *MessagesService) Publish(ctx context.Context, topic string, messages []*PublishMessage) (*PublishResponse, error) {
	var resp PublishResponse
	req := &PublishRequest{Messages: messages}
	path := "/topics/" + url.PathEscape(topic) + "/messages"
	if err := s.http.request(ctx, "POST", path, req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// Consume consumes messages from a partition (simple consumer).
//
// For production use, prefer consumer groups with Client.Groups.Poll().
func (s *MessagesService) Consume(ctx context.Context, topic string, partition int, offset int64, limit int) (*ConsumeResponse, error) {
	params := url.Values{}
	params.Set("offset", strconv.FormatInt(offset, 10))
	if limit > 0 {
		params.Set("limit", strconv.Itoa(limit))
	}
	var resp ConsumeResponse
	path := fmt.Sprintf("/topics/%s/partitions/%d/messages", url.PathEscape(topic), partition)
	if err := s.http.requestWithParams(ctx, "GET", path, params, nil, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// Ack acknowledges a message.
//
// Tells the broker the message was successfully processed.
func (s *MessagesService) Ack(ctx context.Context, receiptHandle string, consumerID *string) error {
	req := &AckRequest{ReceiptHandle: receiptHandle}
	if consumerID != nil {
		req.ConsumerID = *consumerID
	}
	return s.http.request(ctx, "POST", "/messages/ack", req, nil)
}

// Nack negative acknowledges a message.
//
// The message will be redelivered after the optional delay.
func (s *MessagesService) Nack(ctx context.Context, receiptHandle string, delay *string) error {
	req := &NackRequest{ReceiptHandle: receiptHandle}
	if delay != nil {
		req.Delay = *delay
	}
	return s.http.request(ctx, "POST", "/messages/nack", req, nil)
}

// Reject rejects a message, sending it to the DLQ.
//
// Use for messages that can never be processed.
func (s *MessagesService) Reject(ctx context.Context, receiptHandle string, reason *string) error {
	req := &RejectRequest{ReceiptHandle: receiptHandle}
	if reason != nil {
		req.Reason = *reason
	}
	return s.http.request(ctx, "POST", "/messages/reject", req, nil)
}

// ExtendVisibility extends the visibility timeout for a message.
//
// Use when processing takes longer than expected.
func (s *MessagesService) ExtendVisibility(ctx context.Context, receiptHandle, timeout string) error {
	req := &ExtendVisibilityRequest{ReceiptHandle: receiptHandle, Timeout: timeout}
	return s.http.request(ctx, "POST", "/messages/visibility", req, nil)
}

// ReliabilityStats returns ACK/NACK/DLQ statistics.
func (s *MessagesService) ReliabilityStats(ctx context.Context) (*ReliabilityStats, error) {
	var resp ReliabilityStats
	if err := s.http.request(ctx, "GET", "/reliability/stats", nil, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// DelayedService provides delayed message operations.
type DelayedService struct {
	http *httpClient
}

// List returns delayed messages for a topic.
func (s *DelayedService) List(ctx context.Context, topic string, limit int) (*DelayedMessagesResponse, error) {
	params := url.Values{}
	if limit > 0 {
		params.Set("limit", strconv.Itoa(limit))
	}
	var resp DelayedMessagesResponse
	path := "/topics/" + url.PathEscape(topic) + "/delayed"
	if err := s.http.requestWithParams(ctx, "GET", path, params, nil, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// Get returns a specific delayed message.
func (s *DelayedService) Get(ctx context.Context, topic string, offset int64) (*DelayedMessage, error) {
	var resp DelayedMessage
	path := fmt.Sprintf("/topics/%s/delayed/%d", url.PathEscape(topic), offset)
	if err := s.http.request(ctx, "GET", path, nil, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// Cancel cancels a delayed message.
func (s *DelayedService) Cancel(ctx context.Context, topic string, partition int, offset int64) error {
	path := fmt.Sprintf("/topics/%s/delayed/%d/%d", url.PathEscape(topic), partition, offset)
	return s.http.request(ctx, "DELETE", path, nil, nil)
}

// Stats returns delay queue statistics.
func (s *DelayedService) Stats(ctx context.Context) (*DelayStats, error) {
	var resp DelayStats
	if err := s.http.request(ctx, "GET", "/delay/stats", nil, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// GroupsService provides consumer group operations.
type GroupsService struct {
	http *httpClient
}

// List returns all consumer group IDs.
func (s *GroupsService) List(ctx context.Context) (*GroupListResponse, error) {
	var resp GroupListResponse
	if err := s.http.request(ctx, "GET", "/groups", nil, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// Get returns details about a consumer group.
func (s *GroupsService) Get(ctx context.Context, groupID string) (*GroupDetails, error) {
	var resp GroupDetails
	path := "/groups/" + url.PathEscape(groupID)
	if err := s.http.request(ctx, "GET", path, nil, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// Delete deletes a consumer group and its offset data.
func (s *GroupsService) Delete(ctx context.Context, groupID string) error {
	path := "/groups/" + url.PathEscape(groupID)
	return s.http.request(ctx, "DELETE", path, nil, nil)
}

// Join joins a consumer group.
//
// The broker will assign partitions based on the number of members.
func (s *GroupsService) Join(ctx context.Context, groupID string, req *JoinGroupRequest) (*JoinGroupResponse, error) {
	var resp JoinGroupResponse
	path := "/groups/" + url.PathEscape(groupID) + "/join"
	if err := s.http.request(ctx, "POST", path, req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// Heartbeat sends a heartbeat to keep the session alive.
func (s *GroupsService) Heartbeat(ctx context.Context, groupID string, req *HeartbeatRequest) (*HeartbeatResponse, error) {
	var resp HeartbeatResponse
	path := "/groups/" + url.PathEscape(groupID) + "/heartbeat"
	if err := s.http.request(ctx, "POST", path, req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// Leave leaves a consumer group.
func (s *GroupsService) Leave(ctx context.Context, groupID string, req *LeaveGroupRequest) error {
	path := "/groups/" + url.PathEscape(groupID) + "/leave"
	return s.http.request(ctx, "POST", path, req, nil)
}

// Poll polls for messages from assigned partitions.
//
// Long-polls for messages. Returns immediately if messages are available,
// or waits up to the timeout.
func (s *GroupsService) Poll(ctx context.Context, groupID, memberID string, opts *PollOptions) (*PollResponse, error) {
	params := url.Values{}
	params.Set("member_id", memberID)
	if opts != nil {
		if opts.MaxMessages > 0 {
			params.Set("max_messages", strconv.Itoa(opts.MaxMessages))
		}
		if opts.Timeout != "" {
			params.Set("timeout", opts.Timeout)
		}
	}
	var resp PollResponse
	path := "/groups/" + url.PathEscape(groupID) + "/poll"
	if err := s.http.requestWithParams(ctx, "GET", path, params, nil, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// GetOffsets returns committed offsets for a consumer group.
func (s *GroupsService) GetOffsets(ctx context.Context, groupID string, topic *string) (*OffsetsResponse, error) {
	params := url.Values{}
	if topic != nil {
		params.Set("topic", *topic)
	}
	var resp OffsetsResponse
	path := "/groups/" + url.PathEscape(groupID) + "/offsets"
	if err := s.http.requestWithParams(ctx, "GET", path, params, nil, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// CommitOffsets commits offsets for a consumer group.
func (s *GroupsService) CommitOffsets(ctx context.Context, groupID string, req *CommitOffsetsRequest) error {
	path := "/groups/" + url.PathEscape(groupID) + "/offsets"
	return s.http.request(ctx, "POST", path, req, nil)
}

// PriorityService provides priority queue statistics.
type PriorityService struct {
	http *httpClient
}

// Stats returns priority queue statistics.
func (s *PriorityService) Stats(ctx context.Context) (*PriorityStats, error) {
	var resp PriorityStats
	if err := s.http.request(ctx, "GET", "/priority/stats", nil, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// SchemasService provides schema registry operations.
type SchemasService struct {
	http *httpClient
}

// ListSubjects returns all schema subjects.
func (s *SchemasService) ListSubjects(ctx context.Context) ([]string, error) {
	var resp []string
	if err := s.http.request(ctx, "GET", "/schemas/subjects", nil, &resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// ListVersions returns versions for a subject.
func (s *SchemasService) ListVersions(ctx context.Context, subject string) ([]int, error) {
	var resp []int
	path := "/schemas/subjects/" + url.PathEscape(subject) + "/versions"
	if err := s.http.request(ctx, "GET", path, nil, &resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// Register registers a new schema version.
func (s *SchemasService) Register(ctx context.Context, subject string, req *RegisterSchemaRequest) (*RegisterSchemaResponse, error) {
	var resp RegisterSchemaResponse
	path := "/schemas/subjects/" + url.PathEscape(subject) + "/versions"
	if err := s.http.request(ctx, "POST", path, req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// GetVersion returns a specific schema version.
// Version can be a number or "latest".
func (s *SchemasService) GetVersion(ctx context.Context, subject string, version string) (*SchemaVersion, error) {
	var resp SchemaVersion
	path := fmt.Sprintf("/schemas/subjects/%s/versions/%s", url.PathEscape(subject), version)
	if err := s.http.request(ctx, "GET", path, nil, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// DeleteVersion deletes a schema version.
func (s *SchemasService) DeleteVersion(ctx context.Context, subject string, version int) error {
	path := fmt.Sprintf("/schemas/subjects/%s/versions/%d", url.PathEscape(subject), version)
	return s.http.request(ctx, "DELETE", path, nil, nil)
}

// GetByID returns a schema by global ID.
func (s *SchemasService) GetByID(ctx context.Context, id int) (*Schema, error) {
	var resp Schema
	path := fmt.Sprintf("/schemas/ids/%d", id)
	if err := s.http.request(ctx, "GET", path, nil, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// GetConfig returns global compatibility configuration.
func (s *SchemasService) GetConfig(ctx context.Context) (*CompatibilityConfig, error) {
	var resp CompatibilityConfig
	if err := s.http.request(ctx, "GET", "/schemas/config", nil, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// SetConfig sets global compatibility configuration.
func (s *SchemasService) SetConfig(ctx context.Context, config *CompatibilityConfig) (*CompatibilityConfig, error) {
	var resp CompatibilityConfig
	if err := s.http.request(ctx, "PUT", "/schemas/config", config, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// Stats returns schema registry statistics.
func (s *SchemasService) Stats(ctx context.Context) (*SchemaStats, error) {
	var resp SchemaStats
	if err := s.http.request(ctx, "GET", "/schemas/stats", nil, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// TransactionsService provides transaction operations.
type TransactionsService struct {
	http *httpClient
}

// InitProducer initializes an idempotent/transactional producer.
func (s *TransactionsService) InitProducer(ctx context.Context, req *InitProducerRequest) (*InitProducerResponse, error) {
	var resp InitProducerResponse
	if req == nil {
		req = &InitProducerRequest{}
	}
	if err := s.http.request(ctx, "POST", "/producers/init", req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// ProducerHeartbeat sends a producer heartbeat.
func (s *TransactionsService) ProducerHeartbeat(ctx context.Context, producerID int64, req *ProducerHeartbeatRequest) error {
	path := fmt.Sprintf("/producers/%d/heartbeat", producerID)
	return s.http.request(ctx, "POST", path, req, nil)
}

// List returns active transactions.
func (s *TransactionsService) List(ctx context.Context) (*TransactionListResponse, error) {
	var resp TransactionListResponse
	if err := s.http.request(ctx, "GET", "/transactions", nil, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// Begin begins a new transaction.
func (s *TransactionsService) Begin(ctx context.Context, req *BeginTransactionRequest) (*BeginTransactionResponse, error) {
	var resp BeginTransactionResponse
	if err := s.http.request(ctx, "POST", "/transactions/begin", req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// Publish publishes a message within a transaction.
func (s *TransactionsService) Publish(ctx context.Context, req *TransactionalPublishRequest) (*TransactionalPublishResponse, error) {
	var resp TransactionalPublishResponse
	if err := s.http.request(ctx, "POST", "/transactions/publish", req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// Commit commits a transaction.
func (s *TransactionsService) Commit(ctx context.Context, req *CommitTransactionRequest) error {
	return s.http.request(ctx, "POST", "/transactions/commit", req, nil)
}

// Abort aborts a transaction.
func (s *TransactionsService) Abort(ctx context.Context, req *AbortTransactionRequest) error {
	return s.http.request(ctx, "POST", "/transactions/abort", req, nil)
}

// Stats returns transaction statistics.
func (s *TransactionsService) Stats(ctx context.Context) (*TransactionStats, error) {
	var resp TransactionStats
	if err := s.http.request(ctx, "GET", "/transactions/stats", nil, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// TracingService provides message tracing operations.
type TracingService struct {
	http *httpClient
}

// List returns recent traces.
func (s *TracingService) List(ctx context.Context, limit int) (*TraceListResponse, error) {
	params := url.Values{}
	if limit > 0 {
		params.Set("limit", strconv.Itoa(limit))
	}
	var resp TraceListResponse
	if err := s.http.requestWithParams(ctx, "GET", "/traces", params, nil, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// Search searches traces by criteria.
func (s *TracingService) Search(ctx context.Context, params *TraceSearchParams) (*TraceListResponse, error) {
	qp := url.Values{}
	if params != nil {
		if params.Topic != "" {
			qp.Set("topic", params.Topic)
		}
		if params.Partition != nil {
			qp.Set("partition", strconv.Itoa(*params.Partition))
		}
		if params.Status != "" {
			qp.Set("status", params.Status)
		}
		if params.Start != "" {
			qp.Set("start", params.Start)
		}
		if params.End != "" {
			qp.Set("end", params.End)
		}
		if params.Limit > 0 {
			qp.Set("limit", strconv.Itoa(params.Limit))
		}
	}
	var resp TraceListResponse
	if err := s.http.requestWithParams(ctx, "GET", "/traces/search", qp, nil, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// Get returns a specific trace.
func (s *TracingService) Get(ctx context.Context, traceID string) (*Trace, error) {
	var resp Trace
	path := "/traces/" + url.PathEscape(traceID)
	if err := s.http.request(ctx, "GET", path, nil, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// Stats returns tracer statistics.
func (s *TracingService) Stats(ctx context.Context) (*TracerStats, error) {
	var resp TracerStats
	if err := s.http.request(ctx, "GET", "/traces/stats", nil, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// AdminService provides administrative operations.
type AdminService struct {
	http *httpClient
}

// AddPartitions adds partitions to a topic.
func (s *AdminService) AddPartitions(ctx context.Context, topic string, req *AddPartitionsRequest) (*AddPartitionsResponse, error) {
	var resp AddPartitionsResponse
	path := "/admin/topics/" + url.PathEscape(topic) + "/partitions"
	if err := s.http.request(ctx, "POST", path, req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// ListTenants returns all tenants.
func (s *AdminService) ListTenants(ctx context.Context) (*TenantListResponse, error) {
	var resp TenantListResponse
	if err := s.http.request(ctx, "GET", "/admin/tenants", nil, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// CreateTenant creates a new tenant.
func (s *AdminService) CreateTenant(ctx context.Context, req *CreateTenantRequest) (*Tenant, error) {
	var resp Tenant
	if err := s.http.request(ctx, "POST", "/admin/tenants", req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}
