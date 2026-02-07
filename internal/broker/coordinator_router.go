// =============================================================================
// COORDINATOR ROUTER - ROUTES REQUESTS TO CORRECT COORDINATOR
// =============================================================================
//
// WHAT: Routes client requests to the correct coordinator based on group hash.
//
// WHY ROUTING?
// In a cluster, consumer groups are distributed across nodes. Each group maps
// to a specific partition of __consumer_offsets, and the leader of that partition
// is the coordinator for groups in that partition.
//
// When a client sends a request (JoinGroup, CommitOffset, etc.), we need to:
//   1. Hash the group ID to find the target partition
//   2. Find the leader of that partition
//   3. Route the request to that node (or handle locally if we're the leader)
//
// ┌──────────────────────────────────────────────────────────────────────────┐
// │                       COORDINATOR ROUTER                                 │
// │                                                                          │
// │   Client Request (groupID="my-group")                                    │
// │          │                                                               │
// │          ▼                                                               │
// │   ┌─────────────────────────────────────────┐                            │
// │   │ hash("my-group") % 50 = partition 17    │                            │
// │   └─────────────────────────────────────────┘                            │
// │          │                                                               │
// │          ▼                                                               │
// │   ┌─────────────────────────────────────────┐                            │
// │   │ Who is leader of partition 17?          │                            │
// │   │                                         │                            │
// │   │ Option A: This node → Handle locally    │                            │
// │   │ Option B: Other node → Forward request  │                            │
// │   └─────────────────────────────────────────┘                            │
// │          │                      │                                        │
// │          ▼                      ▼                                        │
// │   ┌─────────────┐      ┌─────────────────────┐                           │
// │   │ Local       │      │ Forward to leader   │                           │
// │   │ Coordinator │      │ via HTTP            │                           │
// │   └─────────────┘      └─────────────────────┘                           │
// │                                                                          │
// └──────────────────────────────────────────────────────────────────────────┘
//
// SINGLE-NODE MODE:
// When running without a cluster, all groups are handled locally.
// The router simply passes requests to the local coordinator.
//
// =============================================================================

package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"
)

// =============================================================================
// ERRORS
// =============================================================================

var (
	// ErrCoordinatorUnavailable means the coordinator is not reachable.
	ErrCoordinatorUnavailable = errors.New("coordinator unavailable")

	// ErrForwardingFailed means request forwarding failed.
	ErrForwardingFailed = errors.New("request forwarding failed")
)

// =============================================================================
// COORDINATOR INFO
// =============================================================================

// CoordinatorInfo contains information about a group's coordinator.
type CoordinatorInfo struct {
	// NodeID is the coordinator node's ID.
	NodeID string

	// Host is the coordinator's host.
	Host string

	// Port is the coordinator's port.
	Port int

	// IsLocal indicates this node is the coordinator.
	IsLocal bool
}

// =============================================================================
// COORDINATOR ROUTER
// =============================================================================

// CoordinatorRouter routes requests to the correct coordinator.
type CoordinatorRouter struct {
	// localCoordinator handles requests for local groups.
	localCoordinator GroupCoordinatorI

	// internalTopicManager manages __consumer_offsets.
	internalTopicManager *InternalTopicManager

	// clusterEnabled indicates cluster mode is active.
	clusterEnabled bool

	// nodeID is this node's identifier.
	nodeID string

	// partitionLeaders maps partition → leader node info.
	// Only populated in cluster mode.
	partitionLeaders map[int]*CoordinatorInfo

	// httpClient for forwarding requests.
	httpClient *http.Client

	// mu protects partitionLeaders.
	mu sync.RWMutex

	// logger for operations.
	logger *slog.Logger
}

// CoordinatorRouterConfig holds router configuration.
type CoordinatorRouterConfig struct {
	// LocalCoordinator is the coordinator for local groups.
	LocalCoordinator GroupCoordinatorI

	// InternalTopicManager manages __consumer_offsets.
	InternalTopicManager *InternalTopicManager

	// ClusterEnabled indicates cluster mode.
	ClusterEnabled bool

	// NodeID is this node's identifier.
	NodeID string

	// ForwardTimeoutMs is timeout for forwarding requests.
	// Default: 30000 (30 seconds)
	ForwardTimeoutMs int
}

// NewCoordinatorRouter creates a new coordinator router.
func NewCoordinatorRouter(config CoordinatorRouterConfig) *CoordinatorRouter {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})).With("component", "coordinator-router")

	timeout := 30 * time.Second
	if config.ForwardTimeoutMs > 0 {
		timeout = time.Duration(config.ForwardTimeoutMs) * time.Millisecond
	}

	return &CoordinatorRouter{
		localCoordinator:     config.LocalCoordinator,
		internalTopicManager: config.InternalTopicManager,
		clusterEnabled:       config.ClusterEnabled,
		nodeID:               config.NodeID,
		partitionLeaders:     make(map[int]*CoordinatorInfo),
		httpClient: &http.Client{
			Timeout: timeout,
		},
		logger: logger,
	}
}

// =============================================================================
// COORDINATOR DISCOVERY
// =============================================================================

// FindCoordinator returns the coordinator info for a group.
func (r *CoordinatorRouter) FindCoordinator(groupID string) (*CoordinatorInfo, error) {
	partition := r.internalTopicManager.GetPartitionForGroup(groupID)

	// In single-node mode, we're always the coordinator
	if !r.clusterEnabled {
		return &CoordinatorInfo{
			NodeID:  r.nodeID,
			IsLocal: true,
		}, nil
	}

	// Check if we're the leader for this partition
	if r.internalTopicManager.IsCoordinatorFor(groupID) {
		return &CoordinatorInfo{
			NodeID:  r.nodeID,
			IsLocal: true,
		}, nil
	}

	// Look up leader from cached partition leaders
	r.mu.RLock()
	leader, exists := r.partitionLeaders[partition]
	r.mu.RUnlock()

	if !exists {
		return nil, ErrCoordinatorNotFound
	}

	return leader, nil
}

// UpdatePartitionLeader updates the leader info for a partition.
// Called when partition leadership changes.
func (r *CoordinatorRouter) UpdatePartitionLeader(partition int, info *CoordinatorInfo) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.partitionLeaders[partition] = info

	r.logger.Info("updated partition leader",
		"partition", partition,
		"node", info.NodeID,
		"is_local", info.IsLocal)
}

// =============================================================================
// ROUTING METHODS
// =============================================================================
//
// Each method checks if the request should be handled locally or forwarded.
// In single-node mode, all requests are handled locally.
//
// =============================================================================

// JoinGroup routes a join request to the correct coordinator.
func (r *CoordinatorRouter) JoinGroup(ctx context.Context, req *JoinGroupRequest) (*JoinGroupResult, error) {
	coordinator, err := r.FindCoordinator(req.GroupID)
	if err != nil {
		return nil, err
	}

	if coordinator.IsLocal {
		return r.localCoordinator.JoinGroup(req)
	}

	// Forward to remote coordinator
	return r.forwardJoinGroup(ctx, coordinator, req)
}

// LeaveGroup routes a leave request.
func (r *CoordinatorRouter) LeaveGroup(ctx context.Context, req *LeaveGroupRequest) error {
	coordinator, err := r.FindCoordinator(req.GroupID)
	if err != nil {
		return err
	}

	if coordinator.IsLocal {
		return r.localCoordinator.LeaveGroup(req)
	}

	return r.forwardLeaveGroup(ctx, coordinator, req)
}

// Heartbeat routes a heartbeat request.
func (r *CoordinatorRouter) Heartbeat(ctx context.Context, req *HeartbeatRequest) (*HeartbeatResult, error) {
	coordinator, err := r.FindCoordinator(req.GroupID)
	if err != nil {
		return nil, err
	}

	if coordinator.IsLocal {
		return r.localCoordinator.Heartbeat(req)
	}

	return r.forwardHeartbeat(ctx, coordinator, req)
}

// SyncGroup routes a sync request.
func (r *CoordinatorRouter) SyncGroup(ctx context.Context, req *SyncGroupRequest) (*SyncGroupResult, error) {
	coordinator, err := r.FindCoordinator(req.GroupID)
	if err != nil {
		return nil, err
	}

	if coordinator.IsLocal {
		return r.localCoordinator.SyncGroup(req)
	}

	return r.forwardSyncGroup(ctx, coordinator, req)
}

// CommitOffset routes an offset commit.
func (r *CoordinatorRouter) CommitOffset(ctx context.Context, req *OffsetCommitRequest) error {
	coordinator, err := r.FindCoordinator(req.GroupID)
	if err != nil {
		return err
	}

	if coordinator.IsLocal {
		return r.localCoordinator.CommitOffset(req)
	}

	return r.forwardCommitOffset(ctx, coordinator, req)
}

// FetchOffset routes an offset fetch.
func (r *CoordinatorRouter) FetchOffset(ctx context.Context, req *OffsetFetchRequest) (*OffsetFetchResult, error) {
	coordinator, err := r.FindCoordinator(req.GroupID)
	if err != nil {
		return nil, err
	}

	if coordinator.IsLocal {
		return r.localCoordinator.FetchOffset(req)
	}

	return r.forwardFetchOffset(ctx, coordinator, req)
}

// DescribeGroup routes a describe request.
func (r *CoordinatorRouter) DescribeGroup(ctx context.Context, groupID string) (*GroupDescription, error) {
	coordinator, err := r.FindCoordinator(groupID)
	if err != nil {
		return nil, err
	}

	if coordinator.IsLocal {
		return r.localCoordinator.DescribeGroup(groupID)
	}

	return r.forwardDescribeGroup(ctx, coordinator, groupID)
}

// ListGroups returns all groups (local only in single-node, aggregated in cluster).
func (r *CoordinatorRouter) ListGroups(ctx context.Context) ([]string, error) {
	// In single-node mode, just return local groups
	if !r.clusterEnabled {
		return r.localCoordinator.ListGroups()
	}

	// In cluster mode, we'd need to query all nodes
	// For now, just return local groups
	return r.localCoordinator.ListGroups()
}

// =============================================================================
// REQUEST FORWARDING
// =============================================================================

func (r *CoordinatorRouter) forwardJoinGroup(ctx context.Context, coord *CoordinatorInfo, req *JoinGroupRequest) (*JoinGroupResult, error) {
	url := fmt.Sprintf("http://%s:%d/internal/coordinator/join", coord.Host, coord.Port)

	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := r.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrForwardingFailed, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("%w: status=%d body=%s", ErrForwardingFailed, resp.StatusCode, respBody)
	}

	var result JoinGroupResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &result, nil
}

func (r *CoordinatorRouter) forwardLeaveGroup(ctx context.Context, coord *CoordinatorInfo, req *LeaveGroupRequest) error {
	url := fmt.Sprintf("http://%s:%d/internal/coordinator/leave", coord.Host, coord.Port)

	body, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := r.httpClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrForwardingFailed, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("%w: status=%d body=%s", ErrForwardingFailed, resp.StatusCode, respBody)
	}

	return nil
}

func (r *CoordinatorRouter) forwardHeartbeat(ctx context.Context, coord *CoordinatorInfo, req *HeartbeatRequest) (*HeartbeatResult, error) {
	url := fmt.Sprintf("http://%s:%d/internal/coordinator/heartbeat", coord.Host, coord.Port)

	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := r.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrForwardingFailed, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("%w: status=%d body=%s", ErrForwardingFailed, resp.StatusCode, respBody)
	}

	var result HeartbeatResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &result, nil
}

func (r *CoordinatorRouter) forwardSyncGroup(ctx context.Context, coord *CoordinatorInfo, req *SyncGroupRequest) (*SyncGroupResult, error) {
	url := fmt.Sprintf("http://%s:%d/internal/coordinator/sync", coord.Host, coord.Port)

	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := r.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrForwardingFailed, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("%w: status=%d body=%s", ErrForwardingFailed, resp.StatusCode, respBody)
	}

	var result SyncGroupResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &result, nil
}

func (r *CoordinatorRouter) forwardCommitOffset(ctx context.Context, coord *CoordinatorInfo, req *OffsetCommitRequest) error {
	url := fmt.Sprintf("http://%s:%d/internal/coordinator/commit", coord.Host, coord.Port)

	body, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := r.httpClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrForwardingFailed, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("%w: status=%d body=%s", ErrForwardingFailed, resp.StatusCode, respBody)
	}

	return nil
}

func (r *CoordinatorRouter) forwardFetchOffset(ctx context.Context, coord *CoordinatorInfo, req *OffsetFetchRequest) (*OffsetFetchResult, error) {
	url := fmt.Sprintf("http://%s:%d/internal/coordinator/fetch-offset?group=%s&topic=%s&partition=%d",
		coord.Host, coord.Port, req.GroupID, req.Topic, req.Partition)

	httpReq, err := http.NewRequestWithContext(ctx, "GET", url, http.NoBody)
	if err != nil {
		return nil, err
	}

	resp, err := r.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrForwardingFailed, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrOffsetNotFound
	}
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("%w: status=%d body=%s", ErrForwardingFailed, resp.StatusCode, respBody)
	}

	var result OffsetFetchResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &result, nil
}

func (r *CoordinatorRouter) forwardDescribeGroup(ctx context.Context, coord *CoordinatorInfo, groupID string) (*GroupDescription, error) {
	url := fmt.Sprintf("http://%s:%d/internal/coordinator/describe?group=%s", coord.Host, coord.Port, groupID)

	httpReq, err := http.NewRequestWithContext(ctx, "GET", url, http.NoBody)
	if err != nil {
		return nil, err
	}

	resp, err := r.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrForwardingFailed, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrGroupNotFound
	}
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("%w: status=%d body=%s", ErrForwardingFailed, resp.StatusCode, respBody)
	}

	var result GroupDescription
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &result, nil
}

// =============================================================================
// TOPIC REGISTRATION (pass-through to local coordinator)
// =============================================================================

// RegisterTopic registers a topic (local only).
func (r *CoordinatorRouter) RegisterTopic(topicName string, partitionCount int) {
	r.localCoordinator.RegisterTopic(topicName, partitionCount)
}

// UnregisterTopic unregisters a topic (local only).
func (r *CoordinatorRouter) UnregisterTopic(topicName string) {
	r.localCoordinator.UnregisterTopic(topicName)
}

// =============================================================================
// STATS
// =============================================================================

// RouterStats contains router statistics.
type RouterStats struct {
	ClusterEnabled  bool
	NodeID          string
	LocalPartitions int
	CachedLeaders   int
	ForwardedCount  int64
	LocalCount      int64
}

func (r *CoordinatorRouter) Stats() RouterStats {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return RouterStats{
		ClusterEnabled:  r.clusterEnabled,
		NodeID:          r.nodeID,
		LocalPartitions: len(r.internalTopicManager.GetLocalPartitions()),
		CachedLeaders:   len(r.partitionLeaders),
	}
}
