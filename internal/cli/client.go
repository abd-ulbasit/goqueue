// =============================================================================
// CLI HTTP CLIENT - ADMIN INTERFACE TO GOQUEUE CLUSTER
// =============================================================================
//
// WHAT IS THIS?
// This is a lightweight HTTP client for administrative CLI operations.

//
// HTTP ENDPOINTS USED:
//
//   Topics:
//     POST   /topics              Create topic
//     GET    /topics              List topics
//     GET    /topics/{name}       Describe topic
//     DELETE /topics/{name}       Delete topic
//
//   Messages:
//     POST   /topics/{name}/messages                  Publish
//     GET    /topics/{name}/partitions/{id}/messages  Consume
//
//   Consumer Groups:
//     GET    /groups              List groups
//     GET    /groups/{id}         Describe group
//     DELETE /groups/{id}         Delete group
//     GET    /groups/{id}/offsets Get offsets
//     POST   /groups/{id}/offsets Reset offsets (custom endpoint)
//
//   Traces:
//     GET    /traces              List traces
//     GET    /traces/{id}         Get trace
//     GET    /traces/search       Search traces
//     GET    /traces/stats        Trace stats
//
//   Cluster:
//     GET    /health              Health check
//     GET    /stats               Broker stats
//     GET    /cluster/state       Cluster state (if clustered)
//     GET    /cluster/health      Cluster health
//
// =============================================================================

package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// =============================================================================
// CLIENT CONFIGURATION
// =============================================================================

// ClientConfig holds configuration for the CLI HTTP client.
type ClientConfig struct {
	// ServerURL is the base URL of the goqueue server (e.g., "http://localhost:8080")
	ServerURL string

	// Timeout is the HTTP request timeout
	Timeout time.Duration

	// APIKey for authentication (future use)
	APIKey string
}

// DefaultClientConfig returns sensible defaults.
func DefaultClientConfig() ClientConfig {
	return ClientConfig{
		ServerURL: "http://localhost:8080",
		Timeout:   30 * time.Second,
	}
}

// =============================================================================
// CLIENT
// =============================================================================

// Client is the HTTP client for CLI operations.
type Client struct {
	config     ClientConfig
	httpClient *http.Client
}

// NewClient creates a new CLI HTTP client.
func NewClient(config ClientConfig) *Client {
	return &Client{
		config: config,
		httpClient: &http.Client{
			Timeout: config.Timeout,
		},
	}
}

// =============================================================================
// HTTP HELPERS
// =============================================================================

// DoRequest is the exported wrapper for making HTTP requests.
// Used by the admin CLI for tenant management operations.
func (c *Client) DoRequest(ctx context.Context, method, path string, body interface{}, result interface{}) error {
	return c.doRequest(ctx, method, path, body, result)
}

// doRequest executes an HTTP request and decodes the JSON response.
func (c *Client) doRequest(ctx context.Context, method, path string, body interface{}, result interface{}) error {
	// Build URL
	u, err := url.JoinPath(c.config.ServerURL, path)
	if err != nil {
		return fmt.Errorf("invalid path: %w", err)
	}

	// Encode body if provided
	var bodyReader io.Reader
	if body != nil {
		bodyBytes, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("failed to encode request body: %w", err)
		}
		bodyReader = bytes.NewReader(bodyBytes)
	}

	// Create request
	req, err := http.NewRequestWithContext(ctx, method, u, bodyReader)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	// Set headers
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")

	// Add API key if configured
	if c.config.APIKey != "" {
		req.Header.Set("X-API-Key", c.config.APIKey)
	}

	// Execute request
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	// Read response body
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response: %w", err)
	}

	// Check for errors
	if resp.StatusCode >= 400 {
		var errResp ErrorResponse
		if json.Unmarshal(respBody, &errResp) == nil && errResp.Error != "" {
			return &APIError{
				StatusCode: resp.StatusCode,
				Message:    errResp.Error,
			}
		}
		return &APIError{
			StatusCode: resp.StatusCode,
			Message:    string(respBody),
		}
	}

	// Decode response if result provided
	if result != nil && len(respBody) > 0 {
		if err := json.Unmarshal(respBody, result); err != nil {
			return fmt.Errorf("failed to decode response: %w", err)
		}
	}

	return nil
}

// doRequestWithQuery executes an HTTP request with query parameters.
func (c *Client) doRequestWithQuery(ctx context.Context, method, path string, query url.Values, result interface{}) error {
	// Build URL with query
	u, err := url.JoinPath(c.config.ServerURL, path)
	if err != nil {
		return fmt.Errorf("invalid path: %w", err)
	}
	if len(query) > 0 {
		u = u + "?" + query.Encode()
	}

	// Create request
	req, err := http.NewRequestWithContext(ctx, method, u, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	// Set headers
	req.Header.Set("Accept", "application/json")
	if c.config.APIKey != "" {
		req.Header.Set("X-API-Key", c.config.APIKey)
	}

	// Execute request
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	// Read response body
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response: %w", err)
	}

	// Check for errors
	if resp.StatusCode >= 400 {
		var errResp ErrorResponse
		if json.Unmarshal(respBody, &errResp) == nil && errResp.Error != "" {
			return &APIError{
				StatusCode: resp.StatusCode,
				Message:    errResp.Error,
			}
		}
		return &APIError{
			StatusCode: resp.StatusCode,
			Message:    string(respBody),
		}
	}

	// Decode response if result provided
	if result != nil && len(respBody) > 0 {
		if err := json.Unmarshal(respBody, result); err != nil {
			return fmt.Errorf("failed to decode response: %w", err)
		}
	}

	return nil
}

// =============================================================================
// ERROR TYPES
// =============================================================================

// APIError represents an error from the API.
type APIError struct {
	StatusCode int
	Message    string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("API error (status %d): %s", e.StatusCode, e.Message)
}

// ErrorResponse is the error response format from the API.
type ErrorResponse struct {
	Error string `json:"error"`
}

// =============================================================================
// TOPIC OPERATIONS
// =============================================================================

// CreateTopicRequest is the request to create a topic.
type CreateTopicRequest struct {
	Name           string `json:"name"`
	NumPartitions  int    `json:"num_partitions,omitempty"`
	RetentionHours int    `json:"retention_hours,omitempty"`
}

// CreateTopicResponse is the response from creating a topic.
type CreateTopicResponse struct {
	Name       string `json:"name"`
	Partitions int    `json:"partitions"`
	Created    bool   `json:"created"`
}

// CreateTopic creates a new topic.
func (c *Client) CreateTopic(ctx context.Context, name string, partitions, retentionHours int) (*CreateTopicResponse, error) {
	req := CreateTopicRequest{
		Name:           name,
		NumPartitions:  partitions,
		RetentionHours: retentionHours,
	}
	var resp CreateTopicResponse
	if err := c.doRequest(ctx, http.MethodPost, "/topics", req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// ListTopicsResponse is the response from listing topics.
type ListTopicsResponse struct {
	Topics []string `json:"topics"`
}

// ListTopics returns all topics.
func (c *Client) ListTopics(ctx context.Context) (*ListTopicsResponse, error) {
	var resp ListTopicsResponse
	if err := c.doRequest(ctx, http.MethodGet, "/topics", nil, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// TopicInfo contains detailed topic information.
type TopicInfo struct {
	Name             string                      `json:"name"`
	Partitions       int                         `json:"partitions"`
	TotalMessages    int64                       `json:"total_messages"`
	TotalSizeBytes   int64                       `json:"total_size_bytes"`
	PartitionOffsets map[string]map[string]int64 `json:"partition_offsets"`
}

// DescribeTopic returns detailed information about a topic.
func (c *Client) DescribeTopic(ctx context.Context, name string) (*TopicInfo, error) {
	var resp TopicInfo
	if err := c.doRequest(ctx, http.MethodGet, "/topics/"+name, nil, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// DeleteTopicResponse is the response from deleting a topic.
type DeleteTopicResponse struct {
	Deleted bool   `json:"deleted"`
	Name    string `json:"name"`
}

// DeleteTopic deletes a topic.
func (c *Client) DeleteTopic(ctx context.Context, name string) (*DeleteTopicResponse, error) {
	var resp DeleteTopicResponse
	if err := c.doRequest(ctx, http.MethodDelete, "/topics/"+name, nil, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// =============================================================================
// MESSAGE OPERATIONS
// =============================================================================

// PublishMessage represents a message to publish.
type PublishMessage struct {
	Key       string `json:"key,omitempty"`
	Value     string `json:"value"`
	Partition *int   `json:"partition,omitempty"`
	Delay     string `json:"delay,omitempty"`
	DeliverAt string `json:"deliverAt,omitempty"`
	Priority  string `json:"priority,omitempty"`
}

// PublishRequest is the request to publish messages.
type PublishRequest struct {
	Messages []PublishMessage `json:"messages"`
}

// PublishResult is the result of publishing a message.
type PublishResult struct {
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
	Priority  string `json:"priority,omitempty"`
	Delayed   bool   `json:"delayed,omitempty"`
	DeliverAt string `json:"deliverAt,omitempty"`
	Error     string `json:"error,omitempty"`
}

// PublishResponse is the response from publishing messages.
type PublishResponse struct {
	Results []PublishResult `json:"results"`
}

// Publish publishes messages to a topic.
func (c *Client) Publish(ctx context.Context, topic string, messages []PublishMessage) (*PublishResponse, error) {
	req := PublishRequest{Messages: messages}
	var resp PublishResponse
	if err := c.doRequest(ctx, http.MethodPost, "/topics/"+topic+"/messages", req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// ConsumeMessage represents a consumed message.
type ConsumeMessage struct {
	Offset    int64  `json:"offset"`
	Timestamp string `json:"timestamp"`
	Key       string `json:"key,omitempty"`
	Value     string `json:"value"`
	Priority  string `json:"priority,omitempty"`
}

// ConsumeResponse is the response from consuming messages.
type ConsumeResponse struct {
	Messages   []ConsumeMessage `json:"messages"`
	NextOffset int64            `json:"next_offset"`
}

// Consume fetches messages from a topic partition.
func (c *Client) Consume(ctx context.Context, topic string, partition int, offset int64, limit int) (*ConsumeResponse, error) {
	query := url.Values{}
	query.Set("offset", strconv.FormatInt(offset, 10))
	query.Set("limit", strconv.Itoa(limit))

	path := fmt.Sprintf("/topics/%s/partitions/%d/messages", topic, partition)
	var resp ConsumeResponse
	if err := c.doRequestWithQuery(ctx, http.MethodGet, path, query, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// =============================================================================
// CONSUMER GROUP OPERATIONS
// =============================================================================

// GroupInfo contains consumer group information.
type GroupInfo struct {
	ID         string       `json:"id"`
	State      string       `json:"state"`
	Generation int          `json:"generation"`
	Topics     []string     `json:"topics"`
	Members    []MemberInfo `json:"members"`
}

// MemberInfo contains consumer group member information.
type MemberInfo struct {
	ID                 string           `json:"id"`
	ClientID           string           `json:"client_id"`
	LastHeartbeat      string           `json:"last_heartbeat"`
	JoinedAt           string           `json:"joined_at"`
	AssignedPartitions []TopicPartition `json:"assigned_partitions,omitempty"`
}

// TopicPartition represents a topic-partition pair.
type TopicPartition struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
}

// ListGroupsResponse is the response from listing groups.
type ListGroupsResponse struct {
	Groups []string `json:"groups"`
}

// ListGroups returns all consumer groups.
func (c *Client) ListGroups(ctx context.Context) (*ListGroupsResponse, error) {
	var resp ListGroupsResponse
	if err := c.doRequest(ctx, http.MethodGet, "/groups", nil, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// DescribeGroup returns detailed information about a consumer group.
func (c *Client) DescribeGroup(ctx context.Context, groupID string) (*GroupInfo, error) {
	var resp GroupInfo
	if err := c.doRequest(ctx, http.MethodGet, "/groups/"+groupID, nil, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// DeleteGroupResponse is the response from deleting a group.
type DeleteGroupResponse struct {
	Deleted bool   `json:"deleted"`
	GroupID string `json:"group_id"`
}

// DeleteGroup deletes a consumer group.
func (c *Client) DeleteGroup(ctx context.Context, groupID string) (*DeleteGroupResponse, error) {
	var resp DeleteGroupResponse
	if err := c.doRequest(ctx, http.MethodDelete, "/groups/"+groupID, nil, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// OffsetInfo contains offset information for a partition.
type OffsetInfo struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
	Lag       int64  `json:"lag,omitempty"`
}

// GetOffsetsResponse is the response from getting offsets.
type GetOffsetsResponse struct {
	GroupID string       `json:"group_id"`
	Offsets []OffsetInfo `json:"offsets"`
}

// GetOffsets returns committed offsets for a consumer group.
func (c *Client) GetOffsets(ctx context.Context, groupID string) (*GetOffsetsResponse, error) {
	var resp GetOffsetsResponse
	if err := c.doRequest(ctx, http.MethodGet, "/groups/"+groupID+"/offsets", nil, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// ResetOffsetsRequest is the request to reset offsets.
type ResetOffsetsRequest struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition,omitempty"` // -1 for all partitions
	Strategy  string `json:"strategy"`            // "earliest", "latest", "timestamp"
	Timestamp string `json:"timestamp,omitempty"` // RFC3339, required if strategy=timestamp
}

// ResetOffsetsResponse is the response from resetting offsets.
type ResetOffsetsResponse struct {
	GroupID    string       `json:"group_id"`
	ResetCount int          `json:"reset_count"`
	NewOffsets []OffsetInfo `json:"new_offsets"`
}

// ResetOffsets resets offsets for a consumer group.
// NOTE: This requires a custom endpoint - we'll add it to the API.
func (c *Client) ResetOffsets(ctx context.Context, groupID string, req ResetOffsetsRequest) (*ResetOffsetsResponse, error) {
	var resp ResetOffsetsResponse
	if err := c.doRequest(ctx, http.MethodPost, "/groups/"+groupID+"/offsets/reset", req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// =============================================================================
// TRACE OPERATIONS
// =============================================================================

// TraceInfo contains trace information.
type TraceInfo struct {
	TraceID   string       `json:"trace_id"`
	StartTime string       `json:"start_time"`
	EndTime   string       `json:"end_time,omitempty"`
	Duration  string       `json:"duration,omitempty"`
	Topic     string       `json:"topic"`
	Partition int          `json:"partition"`
	Offset    int64        `json:"offset"`
	Status    string       `json:"status"`
	Events    []TraceEvent `json:"events,omitempty"`
}

// TraceEvent represents a single event in a trace.
type TraceEvent struct {
	Type      string `json:"type"`
	Timestamp string `json:"timestamp"`
	Details   string `json:"details,omitempty"`
}

// ListTracesResponse is the response from listing traces.
type ListTracesResponse struct {
	Traces []TraceInfo `json:"traces"`
}

// ListTraces returns recent traces.
func (c *Client) ListTraces(ctx context.Context, limit int) (*ListTracesResponse, error) {
	query := url.Values{}
	if limit > 0 {
		query.Set("limit", strconv.Itoa(limit))
	}
	var resp ListTracesResponse
	if err := c.doRequestWithQuery(ctx, http.MethodGet, "/traces", query, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// GetTrace returns a specific trace by ID.
func (c *Client) GetTrace(ctx context.Context, traceID string) (*TraceInfo, error) {
	var resp TraceInfo
	if err := c.doRequest(ctx, http.MethodGet, "/traces/"+traceID, nil, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// SearchTracesRequest contains search parameters.
type SearchTracesRequest struct {
	Topic         string
	Partition     int
	ConsumerGroup string
	StartTime     string
	EndTime       string
	Status        string
	Limit         int
}

// SearchTraces searches for traces matching criteria.
func (c *Client) SearchTraces(ctx context.Context, req SearchTracesRequest) (*ListTracesResponse, error) {
	query := url.Values{}
	if req.Topic != "" {
		query.Set("topic", req.Topic)
	}
	if req.Partition >= 0 {
		query.Set("partition", strconv.Itoa(req.Partition))
	}
	if req.ConsumerGroup != "" {
		query.Set("consumer_group", req.ConsumerGroup)
	}
	if req.StartTime != "" {
		query.Set("start", req.StartTime)
	}
	if req.EndTime != "" {
		query.Set("end", req.EndTime)
	}
	if req.Status != "" {
		query.Set("status", req.Status)
	}
	if req.Limit > 0 {
		query.Set("limit", strconv.Itoa(req.Limit))
	}

	var resp ListTracesResponse
	if err := c.doRequestWithQuery(ctx, http.MethodGet, "/traces/search", query, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// TraceStats contains trace statistics.
type TraceStats struct {
	TotalTraces     int64            `json:"total_traces"`
	ActiveTraces    int64            `json:"active_traces"`
	CompletedTraces int64            `json:"completed_traces"`
	ErrorTraces     int64            `json:"error_traces"`
	ByTopic         map[string]int64 `json:"by_topic"`
}

// GetTraceStats returns trace statistics.
func (c *Client) GetTraceStats(ctx context.Context) (*TraceStats, error) {
	var resp TraceStats
	if err := c.doRequest(ctx, http.MethodGet, "/traces/stats", nil, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// =============================================================================
// CLUSTER OPERATIONS
// =============================================================================

// HealthResponse is the response from health check.
type HealthResponse struct {
	Status    string `json:"status"`
	Timestamp string `json:"timestamp"`
}

// Health checks the server health.
func (c *Client) Health(ctx context.Context) (*HealthResponse, error) {
	var resp HealthResponse
	if err := c.doRequest(ctx, http.MethodGet, "/health", nil, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// BrokerStats contains broker statistics.
type BrokerStats struct {
	NodeID         string                 `json:"node_id"`
	Uptime         string                 `json:"uptime"`
	Topics         int                    `json:"topics"`
	TotalSizeBytes int64                  `json:"total_size_bytes"`
	TopicStats     map[string]interface{} `json:"topic_stats"`
}

// GetStats returns broker statistics.
func (c *Client) GetStats(ctx context.Context) (*BrokerStats, error) {
	var resp BrokerStats
	if err := c.doRequest(ctx, http.MethodGet, "/stats", nil, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// ClusterState contains cluster state information.
type ClusterState struct {
	Version         int64      `json:"version"`
	ControllerID    string     `json:"controller_id"`
	ControllerEpoch int64      `json:"controller_epoch"`
	Nodes           []NodeInfo `json:"nodes"`
}

// NodeInfo contains information about a cluster node.
type NodeInfo struct {
	ID            string `json:"id"`
	ClientAddress string `json:"client_address"`
	PeerAddress   string `json:"peer_address"`
	Status        string `json:"status"`
	IsController  bool   `json:"is_controller"`
}

// GetClusterState returns the cluster state.
func (c *Client) GetClusterState(ctx context.Context) (*ClusterState, error) {
	var resp ClusterState
	if err := c.doRequest(ctx, http.MethodGet, "/cluster/state", nil, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// ClusterHealth contains cluster health information.
type ClusterHealth struct {
	Status    string       `json:"status"`
	NodeCount int          `json:"node_count"`
	Healthy   int          `json:"healthy"`
	Unhealthy int          `json:"unhealthy"`
	Nodes     []NodeHealth `json:"nodes"`
}

// NodeHealth contains health info for a node.
type NodeHealth struct {
	ID       string `json:"id"`
	Status   string `json:"status"`
	LastSeen string `json:"last_seen"`
}

// GetClusterHealth returns the cluster health.
func (c *Client) GetClusterHealth(ctx context.Context) (*ClusterHealth, error) {
	var resp ClusterHealth
	if err := c.doRequest(ctx, http.MethodGet, "/cluster/health", nil, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// =============================================================================
// VERSION INFORMATION
// =============================================================================

// VersionInfo contains version information.
type VersionInfo struct {
	ClientVersion string `json:"client_version"`
	ServerVersion string `json:"server_version,omitempty"`
}

// GetVersion returns version information.
func (c *Client) GetVersion(ctx context.Context) (*VersionInfo, error) {
	// Client version is known
	info := &VersionInfo{
		ClientVersion: Version,
	}

	// Try to get server version from health/stats
	stats, err := c.GetStats(ctx)
	if err == nil && stats.NodeID != "" {
		// Server is reachable
		info.ServerVersion = "v0.2.0" // We could add version endpoint
	}

	return info, nil
}

// Version is the CLI version.
const Version = "v0.2.0"
