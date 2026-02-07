// =============================================================================
// REPLICATION CLIENT - HTTP CLIENT FOR FOLLOWERS
// =============================================================================
//
// WHAT: HTTP client that followers use to communicate with leaders and controller.
//
// METHODS:
//   - Fetch()           - Get messages from leader (used by FollowerFetcher)
//   - RequestSnapshot() - Request snapshot creation
//   - DownloadSnapshot()- Download snapshot file
//   - ReportISRUpdate() - Report ISR changes to controller
//   - RequestLeaderElection() - Request leader election
//
// USAGE:
//   The FollowerFetcher uses this client in its background loop:
//   ┌─────────────────────────────────────────────────────────────────────────┐
//   │                                                                         │
//   │   FollowerFetcher loop:                                                 │
//   │                                                                         │
//   │   for {                                                                 │
//   │       resp, err := client.Fetch(ctx, topic, partition, offset)          │
//   │       if err != nil {                                                   │
//   │           // Handle error (backoff, retry)                              │
//   │           continue                                                      │
//   │       }                                                                 │
//   │                                                                         │
//   │       if resp.ErrorCode == FetchErrOffsetOutOfRange {                   │
//   │           // Far behind - need snapshot                                 │
//   │           snapshot, _ := client.RequestSnapshot(ctx, topic, partition)  │
//   │           client.DownloadSnapshot(ctx, snapshot.DownloadURL)            │
//   │           // Apply snapshot, update offset                              │
//   │           continue                                                      │
//   │       }                                                                 │
//   │                                                                         │
//   │       // Apply messages to local log                                    │
//   │       applyMessages(resp.Messages)                                      │
//   │       offset = resp.LogEndOffset                                        │
//   │   }                                                                     │
//   │                                                                         │
//   └─────────────────────────────────────────────────────────────────────────┘
//
// =============================================================================

package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

// =============================================================================
// REPLICATION CLIENT
// =============================================================================

// ReplicationClient makes replication requests to other nodes.
type ReplicationClient struct {
	// httpClient for HTTP requests.
	httpClient *http.Client

	// localNodeID is this node's ID.
	localNodeID NodeID

	// membership for looking up node addresses.
	membership *Membership

	// metadataStore for partition info.
	metadataStore *MetadataStore

	// config for replication settings.
	config ReplicationConfig

	// logger for operations.
	logger *slog.Logger
}

// NewReplicationClient creates a new replication client.
func NewReplicationClient(
	localNodeID NodeID,
	membership *Membership,
	metadataStore *MetadataStore,
	config ReplicationConfig,
	logger *slog.Logger,
) *ReplicationClient {
	return &ReplicationClient{
		httpClient: &http.Client{
			Timeout: time.Duration(config.FetchTimeoutMs) * time.Millisecond,
		},
		localNodeID:   localNodeID,
		membership:    membership,
		metadataStore: metadataStore,
		config:        config,
		logger:        logger.With("component", "replication-client"),
	}
}

// =============================================================================
// FETCH - PULL MESSAGES FROM LEADER
// =============================================================================
//
// This is the core replication method. Followers call this periodically
// to get new messages from the leader.
//
// BEHAVIOR:
//   - Sends FetchRequest with fromOffset
//   - Leader returns messages, highWatermark, and any errors
//   - Handles various error codes appropriately
//
// ERROR HANDLING:
//   - FetchErrNotLeader: Refresh metadata, retry with new leader
//   - FetchErrOffsetOutOfRange: Need snapshot catch-up
//   - FetchErrFencedLeader: Refresh metadata (stale epoch)
//   - Network errors: Backoff and retry
//
// =============================================================================

// Fetch fetches messages from the leader for a partition.
func (rc *ReplicationClient) Fetch(ctx context.Context, topic string, partition int, fromOffset, leaderEpoch int64) (*FetchResponse, error) {
	// Get leader address.
	leaderAddr, err := rc.getLeaderAddress(topic, partition)
	if err != nil {
		return nil, fmt.Errorf("get leader address: %w", err)
	}

	req := &FetchRequest{
		Topic:       topic,
		Partition:   partition,
		FromOffset:  fromOffset,
		MaxBytes:    rc.config.FetchMaxBytes,
		FollowerID:  rc.localNodeID,
		LeaderEpoch: leaderEpoch,
	}

	var resp FetchResponse
	if err := rc.doPost(ctx, leaderAddr, "/cluster/fetch", req, &resp); err != nil {
		return nil, fmt.Errorf("fetch request: %w", err)
	}

	return &resp, nil
}

// =============================================================================
// SNAPSHOT OPERATIONS
// =============================================================================

// RequestSnapshot requests the leader to create a snapshot.
func (rc *ReplicationClient) RequestSnapshot(ctx context.Context, topic string, partition int) (*SnapshotResponse, error) {
	leaderAddr, err := rc.getLeaderAddress(topic, partition)
	if err != nil {
		return nil, fmt.Errorf("get leader address: %w", err)
	}

	req := &SnapshotRequest{
		Topic:       topic,
		Partition:   partition,
		RequestedBy: rc.localNodeID,
	}

	var resp SnapshotResponse
	if err := rc.doPost(ctx, leaderAddr, "/cluster/snapshot/create", req, &resp); err != nil {
		return nil, fmt.Errorf("snapshot request: %w", err)
	}

	return &resp, nil
}

// DownloadSnapshot downloads a snapshot file from the leader.
// Returns the path to the downloaded snapshot.
func (rc *ReplicationClient) DownloadSnapshot(ctx context.Context, topic string, partition int, downloadURL, destDir string) (string, error) {
	leaderAddr, err := rc.getLeaderAddress(topic, partition)
	if err != nil {
		return "", fmt.Errorf("get leader address: %w", err)
	}

	url := fmt.Sprintf("http://%s%s", leaderAddr, downloadURL)

	rc.logger.Info("downloading snapshot",
		"topic", topic,
		"partition", partition,
		"url", url)

	// Create request.
	httpReq, err := http.NewRequestWithContext(ctx, "GET", url, http.NoBody)
	if err != nil {
		return "", fmt.Errorf("create request: %w", err)
	}

	// Execute request.
	resp, err := rc.httpClient.Do(httpReq)
	if err != nil {
		return "", fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("download failed (%d): %s", resp.StatusCode, string(body))
	}

	// Create destination directory.
	if err := os.MkdirAll(destDir, 0o755); err != nil {
		return "", fmt.Errorf("create dest dir: %w", err)
	}

	// Create destination file.
	destPath := filepath.Join(destDir, fmt.Sprintf("snapshot-%s-%d.tar.gz", topic, partition))
	destFile, err := os.Create(destPath)
	if err != nil {
		return "", fmt.Errorf("create dest file: %w", err)
	}
	defer destFile.Close()

	// Copy response body to file.
	written, err := io.Copy(destFile, resp.Body)
	if err != nil {
		os.Remove(destPath)
		return "", fmt.Errorf("copy snapshot: %w", err)
	}

	rc.logger.Info("snapshot downloaded",
		"topic", topic,
		"partition", partition,
		"size", written,
		"path", destPath)

	return destPath, nil
}

// =============================================================================
// ISR UPDATES
// =============================================================================

// ReportISRShrink reports that a follower has fallen out of ISR.
// Called by leaders when a follower falls behind.
func (rc *ReplicationClient) ReportISRShrink(ctx context.Context, topic string, partition int, nodeID NodeID, reason string) error {
	controllerAddr, err := rc.getControllerAddress()
	if err != nil {
		return fmt.Errorf("get controller address: %w", err)
	}

	req := &ISRUpdate{
		Topic:     topic,
		Partition: partition,
		NodeID:    nodeID,
		Type:      ISRShrink,
		Reason:    reason,
		Timestamp: time.Now(),
	}

	var resp map[string]bool
	if err := rc.doPost(ctx, controllerAddr, "/cluster/isr", req, &resp); err != nil {
		return fmt.Errorf("report ISR shrink: %w", err)
	}

	return nil
}

// ReportISRExpand requests to rejoin ISR after catching up.
// Called by followers when they've caught up with the leader.
func (rc *ReplicationClient) ReportISRExpand(ctx context.Context, topic string, partition int, offset int64) error {
	controllerAddr, err := rc.getControllerAddress()
	if err != nil {
		return fmt.Errorf("get controller address: %w", err)
	}

	req := &ISRUpdate{
		Topic:     topic,
		Partition: partition,
		NodeID:    rc.localNodeID,
		Type:      ISRExpand,
		Offset:    offset,
		Reason:    "caught up with leader",
		Timestamp: time.Now(),
	}

	var resp map[string]bool
	if err := rc.doPost(ctx, controllerAddr, "/cluster/isr", req, &resp); err != nil {
		return fmt.Errorf("report ISR expand: %w", err)
	}

	return nil
}

// =============================================================================
// LEADER ELECTION
// =============================================================================

// RequestLeaderElection requests a leader election for a partition.
// Called when a follower detects the leader is unavailable.
func (rc *ReplicationClient) RequestLeaderElection(ctx context.Context, topic string, partition int, reason string, allowUnclean bool) (*LeaderElectionResponse, error) {
	controllerAddr, err := rc.getControllerAddress()
	if err != nil {
		return nil, fmt.Errorf("get controller address: %w", err)
	}

	req := &LeaderElectionRequest{
		Topic:        topic,
		Partition:    partition,
		Reason:       reason,
		AllowUnclean: allowUnclean,
	}

	var resp LeaderElectionResponse
	if err := rc.doPost(ctx, controllerAddr, "/cluster/leader-election", req, &resp); err != nil {
		return nil, fmt.Errorf("leader election request: %w", err)
	}

	return &resp, nil
}

// =============================================================================
// PARTITION INFO
// =============================================================================

// GetPartitionInfo gets replication info for a partition.
func (rc *ReplicationClient) GetPartitionInfo(ctx context.Context, nodeID NodeID, topic string, partition int) (*PartitionInfoResponse, error) {
	nodeInfo := rc.membership.GetNode(nodeID)
	if nodeInfo == nil {
		return nil, fmt.Errorf("node %s not found", nodeID)
	}

	url := fmt.Sprintf("http://%s/cluster/partition/%s/%d/info", nodeInfo.ClusterAddress.String(), topic, partition)

	httpReq, err := http.NewRequestWithContext(ctx, "GET", url, http.NoBody)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	resp, err := rc.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("request failed (%d): %s", resp.StatusCode, string(body))
	}

	var info PartitionInfoResponse
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	return &info, nil
}

// =============================================================================
// HELPER METHODS
// =============================================================================

func (rc *ReplicationClient) getLeaderAddress(topic string, partition int) (string, error) {
	assignment := rc.metadataStore.GetAssignment(topic, partition)
	if assignment == nil {
		return "", fmt.Errorf("partition %s-%d not found", topic, partition)
	}

	if assignment.Leader.IsEmpty() {
		return "", fmt.Errorf("no leader for partition %s-%d", topic, partition)
	}

	leaderInfo := rc.membership.GetNode(assignment.Leader)
	if leaderInfo == nil {
		return "", fmt.Errorf("leader node %s not found in membership", assignment.Leader)
	}

	return leaderInfo.ClusterAddress.String(), nil
}

func (rc *ReplicationClient) getControllerAddress() (string, error) {
	controllerID := rc.membership.ControllerID()
	if controllerID == "" {
		return "", fmt.Errorf("no controller elected")
	}

	controllerInfo := rc.membership.GetNode(controllerID)
	if controllerInfo == nil {
		return "", fmt.Errorf("controller node %s not found in membership", controllerID)
	}

	return controllerInfo.ClusterAddress.String(), nil
}

func (rc *ReplicationClient) doPost(ctx context.Context, addr, path string, reqBody, respBody interface{}) error {
	// Marshal request.
	body, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}

	// Build URL.
	url := fmt.Sprintf("http://%s%s", addr, path)

	// Create request.
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	// Execute.
	resp, err := rc.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	// Check status.
	if resp.StatusCode != http.StatusOK {
		respBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("request failed (%d): %s", resp.StatusCode, string(respBytes))
	}

	// Decode response.
	if err := json.NewDecoder(resp.Body).Decode(respBody); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}

	return nil
}
