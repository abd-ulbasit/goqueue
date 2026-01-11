// =============================================================================
// CLUSTER SERVER - INTER-NODE HTTP API
// =============================================================================
//
// WHAT: HTTP endpoints for node-to-node communication.
//
// ENDPOINTS:
//   POST /cluster/heartbeat  - Periodic health check
//   POST /cluster/join       - Node joining cluster
//   POST /cluster/leave      - Node leaving cluster
//   GET  /cluster/state      - Get cluster state (for sync)
//   POST /cluster/vote       - Controller election vote request
//   POST /cluster/metadata   - Sync metadata from controller
//
// WHY HTTP/JSON INSTEAD OF gRPC?
//   - Simpler to implement and debug (can curl endpoints)
//   - Consistent with existing broker API
//   - Good enough for our scale (< 10 nodes)
//   - gRPC planned for M14 (high-performance inter-node)
//
// COMPARISON:
//   - Kafka: Custom binary protocol over TCP
//   - Cassandra: Custom gossip protocol
//   - etcd: gRPC for client, Raft for consensus
//   - Consul: RPC over TCP, HTTP for agents
//   - goqueue: HTTP/JSON (simple first, optimize later)
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
// CLUSTER SERVER
// =============================================================================

// ClusterServer handles inter-node HTTP communication.
type ClusterServer struct {
	// node is this node's identity
	node *Node

	// membership manages cluster membership
	membership *Membership

	// failureDetector monitors node health
	failureDetector *FailureDetector

	// elector handles controller election
	elector *ControllerElector

	// metadataStore stores cluster metadata
	metadataStore *MetadataStore

	// logger for server operations
	logger *slog.Logger
}

// NewClusterServer creates a new cluster server.
func NewClusterServer(
	node *Node,
	membership *Membership,
	failureDetector *FailureDetector,
	elector *ControllerElector,
	metadataStore *MetadataStore,
	logger *slog.Logger,
) *ClusterServer {
	return &ClusterServer{
		node:            node,
		membership:      membership,
		failureDetector: failureDetector,
		elector:         elector,
		metadataStore:   metadataStore,
		logger:          logger.With("component", "cluster-server"),
	}
}

// =============================================================================
// HTTP HANDLERS
// =============================================================================

// RegisterRoutes registers cluster endpoints on an HTTP mux.
// Call this with your existing chi router or http.ServeMux.
func (cs *ClusterServer) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /cluster/heartbeat", cs.handleHeartbeat)
	mux.HandleFunc("POST /cluster/join", cs.handleJoin)
	mux.HandleFunc("POST /cluster/leave", cs.handleLeave)
	mux.HandleFunc("GET /cluster/state", cs.handleGetState)
	mux.HandleFunc("POST /cluster/vote", cs.handleVote)
	mux.HandleFunc("POST /cluster/metadata", cs.handleMetadata)
	mux.HandleFunc("GET /cluster/health", cs.handleHealth)
}

// =============================================================================
// HEARTBEAT HANDLER
// =============================================================================
//
// FLOW:
//   1. Node A sends heartbeat to Node B
//   2. Node B records heartbeat timestamp
//   3. Node B responds with its cluster state
//   4. Node A merges state (learns about new nodes)
//
// WHY PIGGYBACK STATE ON HEARTBEAT?
//   - Efficient: no separate state sync endpoint needed
//   - Eventually consistent: state spreads during heartbeats
//   - Kafka uses similar pattern for ISR updates
//
// =============================================================================

func (cs *ClusterServer) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	var req HeartbeatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		cs.jsonError(w, "invalid request body", http.StatusBadRequest)
		return
	}

	cs.logger.Debug("received heartbeat",
		"from", req.NodeID,
		"epoch", req.Epoch)

	// Record heartbeat (updates failure detector)
	cs.failureDetector.RecordHeartbeat(req.NodeID)

	// If sender thinks someone else is controller, check it
	if req.ControllerID != "" && req.ControllerID != cs.membership.ControllerID() {
		// Handle potential controller change
		cs.elector.AcknowledgeController(req.ControllerID, req.Epoch)
	}

	// Build response with our view of cluster
	state := cs.membership.State()
	resp := HeartbeatResponse{
		NodeID:          cs.node.ID(),
		Version:         state.Version,
		ControllerID:    state.ControllerID,
		ControllerEpoch: state.ControllerEpoch,
		Nodes:           state.Nodes,
	}

	cs.jsonResponse(w, resp)
}

// =============================================================================
// JOIN HANDLER
// =============================================================================
//
// FLOW:
//   1. New node sends JoinRequest to any existing node
//   2. If receiver is controller: process join
//   3. If receiver is NOT controller: forward to controller (or reject)
//
// WHY JOIN GOES TO CONTROLLER?
//   - Single writer pattern - only controller modifies membership
//   - Avoids conflicts if multiple nodes try to add same node
//   - Controller broadcasts membership change to all nodes
//
// =============================================================================

func (cs *ClusterServer) handleJoin(w http.ResponseWriter, r *http.Request) {
	var req JoinRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		cs.jsonError(w, "invalid request body", http.StatusBadRequest)
		return
	}

	cs.logger.Info("received join request",
		"from", req.NodeID,
		"addr", req.NodeInfo.ClientAddress)

	// Only controller can process joins
	if !cs.elector.IsController() {
		// Redirect to controller
		controllerID := cs.membership.ControllerID()
		if controllerID == "" {
			cs.jsonError(w, "no controller elected yet", http.StatusServiceUnavailable)
			return
		}

		// Return controller address for redirect
		controllerInfo := cs.membership.GetNode(controllerID)
		if controllerInfo == nil {
			cs.jsonError(w, "controller not found", http.StatusServiceUnavailable)
			return
		}

		resp := JoinResponse{
			Success:          false,
			Error:            "not controller, redirect to controller",
			ControllerID:     controllerID,
			ControllerAddr:   controllerInfo.ClusterAddress.String(),
			RedirectRequired: true,
		}
		cs.jsonResponse(w, resp)
		return
	}

	// Process join as controller
	nodeInfo := req.NodeInfo
	if err := cs.membership.AddNode(&nodeInfo); err != nil {
		cs.logger.Error("failed to add node",
			"node", req.NodeID,
			"error", err)
		cs.jsonError(w, fmt.Sprintf("failed to add node: %v", err), http.StatusInternalServerError)
		return
	}

	// Return success with current cluster state
	state := cs.membership.State()
	resp := JoinResponse{
		Success:          true,
		ClusterState:     state,
		ControllerID:     state.ControllerID,
		ControllerAddr:   "",
		RedirectRequired: false,
	}

	cs.logger.Info("node joined cluster",
		"node", req.NodeID,
		"cluster_size", len(state.Nodes))

	cs.jsonResponse(w, resp)
}

// =============================================================================
// LEAVE HANDLER
// =============================================================================
//
// GRACEFUL vs UNGRACEFUL LEAVE:
//   - Graceful: Node calls /leave, controller marks it as "leaving", then removes
//   - Ungraceful: Node crashes, failure detector marks as "dead" after timeout
//
// WHY GRACEFUL LEAVE MATTERS:
//   - No false alarms (node isn't dead, just leaving)
//   - Can drain in-flight requests
//   - Can trigger partition reassignment proactively
//
// =============================================================================

func (cs *ClusterServer) handleLeave(w http.ResponseWriter, r *http.Request) {
	var req LeaveRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		cs.jsonError(w, "invalid request body", http.StatusBadRequest)
		return
	}

	cs.logger.Info("received leave request",
		"from", req.NodeID)

	// Only controller can process leaves
	if !cs.elector.IsController() {
		controllerID := cs.membership.ControllerID()
		cs.jsonError(w, fmt.Sprintf("not controller, redirect to %s", controllerID), http.StatusServiceUnavailable)
		return
	}

	// Mark as leaving
	if err := cs.membership.UpdateNodeStatus(req.NodeID, NodeStatusLeaving); err != nil {
		cs.logger.Warn("failed to update node status",
			"node", req.NodeID,
			"error", err)
	}

	// Remove from membership
	if err := cs.membership.RemoveNode(req.NodeID, true); err != nil {
		cs.logger.Error("failed to remove node",
			"node", req.NodeID,
			"error", err)
		cs.jsonError(w, fmt.Sprintf("failed to remove node: %v", err), http.StatusInternalServerError)
		return
	}

	resp := LeaveResponse{
		Success: true,
	}

	cs.logger.Info("node left cluster",
		"node", req.NodeID)

	cs.jsonResponse(w, resp)
}

// =============================================================================
// STATE SYNC HANDLER
// =============================================================================
//
// WHEN IS THIS CALLED?
//   - Node restarts and needs current cluster state
//   - Node detects its state is stale (version < controller's version)
//   - Manual state refresh
//
// =============================================================================

func (cs *ClusterServer) handleGetState(w http.ResponseWriter, r *http.Request) {
	state := cs.membership.State()
	meta := cs.metadataStore.Meta()

	resp := StateSyncResponse{
		ClusterState:    state,
		ClusterMeta:     meta,
		ControllerID:    state.ControllerID,
		ControllerEpoch: state.ControllerEpoch,
	}

	cs.jsonResponse(w, resp)
}

// =============================================================================
// VOTE HANDLER
// =============================================================================
//
// FLOW:
//   1. Candidate sends VoteRequest to all nodes
//   2. Node grants vote if:
//      - Haven't voted in this epoch yet
//      - Candidate's epoch >= our last seen epoch
//   3. Candidate with majority becomes controller
//
// =============================================================================

func (cs *ClusterServer) handleVote(w http.ResponseWriter, r *http.Request) {
	var req ControllerVoteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		cs.jsonError(w, "invalid request body", http.StatusBadRequest)
		return
	}

	cs.logger.Debug("received vote request",
		"from", req.CandidateID,
		"epoch", req.Epoch)

	// Process vote request
	resp := cs.elector.HandleVoteRequest(&req)

	cs.logger.Debug("vote response",
		"to", req.CandidateID,
		"granted", resp.VoteGranted,
		"epoch", req.Epoch)

	cs.jsonResponse(w, resp)
}

// =============================================================================
// METADATA SYNC HANDLER
// =============================================================================
//
// WHY SEPARATE FROM CLUSTER STATE?
//   - Cluster state: nodes, controller (small, changes rarely)
//   - Metadata: topics, partitions (larger, changes more often)
//   - Separate sync allows efficient delta updates (future)
//
// =============================================================================

func (cs *ClusterServer) handleMetadata(w http.ResponseWriter, r *http.Request) {
	// This endpoint receives metadata from controller
	// Followers call this to sync metadata

	var req struct {
		Metadata *ClusterMeta `json:"metadata"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		cs.jsonError(w, "invalid request body", http.StatusBadRequest)
		return
	}

	cs.logger.Debug("received metadata sync",
		"version", req.Metadata.Version)

	// Apply received metadata
	if err := cs.metadataStore.ApplyMeta(req.Metadata); err != nil {
		cs.jsonError(w, fmt.Sprintf("failed to apply metadata: %v", err), http.StatusInternalServerError)
		return
	}

	cs.jsonResponse(w, map[string]bool{"success": true})
}

// =============================================================================
// HEALTH HANDLER
// =============================================================================

func (cs *ClusterServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	state := cs.membership.State()
	stats := cs.failureDetector.Stats()

	resp := struct {
		NodeID          NodeID               `json:"node_id"`
		Status          NodeStatus           `json:"status"`
		Role            NodeRole             `json:"role"`
		ControllerID    NodeID               `json:"controller_id"`
		ControllerEpoch int64                `json:"controller_epoch"`
		ClusterSize     int                  `json:"cluster_size"`
		HealthyNodes    int                  `json:"healthy_nodes"`
		Stats           FailureDetectorStats `json:"failure_detector_stats"`
	}{
		NodeID:          cs.node.ID(),
		Status:          cs.node.Status(),
		Role:            cs.node.Role(),
		ControllerID:    state.ControllerID,
		ControllerEpoch: state.ControllerEpoch,
		ClusterSize:     len(state.Nodes),
		HealthyNodes:    stats.AliveCount,
		Stats:           stats,
	}

	cs.jsonResponse(w, resp)
}

// =============================================================================
// HELPER METHODS
// =============================================================================

func (cs *ClusterServer) jsonResponse(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

func (cs *ClusterServer) jsonError(w http.ResponseWriter, message string, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": message})
}

// =============================================================================
// CLUSTER CLIENT - OUTGOING REQUESTS
// =============================================================================

// ClusterClient makes requests to other cluster nodes.
type ClusterClient struct {
	// httpClient is the HTTP client for requests
	httpClient *http.Client

	// node is our local node
	node *Node

	// membership for looking up node addresses
	membership *Membership

	// logger for client operations
	logger *slog.Logger
}

// NewClusterClient creates a new cluster client.
func NewClusterClient(node *Node, membership *Membership, logger *slog.Logger) *ClusterClient {
	return &ClusterClient{
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
		},
		node:       node,
		membership: membership,
		logger:     logger.With("component", "cluster-client"),
	}
}

// =============================================================================
// CLIENT METHODS
// =============================================================================

// SendHeartbeat sends a heartbeat to another node.
func (cc *ClusterClient) SendHeartbeat(ctx context.Context, targetID NodeID) (*HeartbeatResponse, error) {
	targetInfo := cc.membership.GetNode(targetID)
	if targetInfo == nil {
		return nil, fmt.Errorf("node %s not found", targetID)
	}

	state := cc.membership.State()
	req := HeartbeatRequest{
		NodeID:       cc.node.ID(),
		Timestamp:    time.Now(),
		ControllerID: state.ControllerID,
		Epoch:        state.ControllerEpoch,
	}

	var resp HeartbeatResponse
	err := cc.doRequest(ctx, targetInfo.ClusterAddress.String(), "/cluster/heartbeat", req, &resp)
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

// RequestJoin requests to join the cluster through a peer.
func (cc *ClusterClient) RequestJoin(ctx context.Context, peerAddr string) (*JoinResponse, error) {
	req := JoinRequest{
		NodeID:   cc.node.ID(),
		NodeInfo: *cc.node.Info(),
	}

	var resp JoinResponse
	err := cc.doRequest(ctx, peerAddr, "/cluster/join", req, &resp)
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

// RequestLeave requests to leave the cluster.
func (cc *ClusterClient) RequestLeave(ctx context.Context) (*LeaveResponse, error) {
	controllerID := cc.membership.ControllerID()
	if controllerID == "" {
		return nil, fmt.Errorf("no controller")
	}

	controllerInfo := cc.membership.GetNode(controllerID)
	if controllerInfo == nil {
		return nil, fmt.Errorf("controller not found")
	}

	req := LeaveRequest{
		NodeID: cc.node.ID(),
	}

	var resp LeaveResponse
	err := cc.doRequest(ctx, controllerInfo.ClusterAddress.String(), "/cluster/leave", req, &resp)
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

// RequestVote requests a vote from another node.
func (cc *ClusterClient) RequestVote(ctx context.Context, targetID NodeID, epoch int64) (*ControllerVoteResponse, error) {
	targetInfo := cc.membership.GetNode(targetID)
	if targetInfo == nil {
		return nil, fmt.Errorf("node %s not found", targetID)
	}

	req := ControllerVoteRequest{
		CandidateID: cc.node.ID(),
		Epoch:       epoch,
	}

	var resp ControllerVoteResponse
	err := cc.doRequest(ctx, targetInfo.ClusterAddress.String(), "/cluster/vote", req, &resp)
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

// FetchState fetches cluster state from another node.
func (cc *ClusterClient) FetchState(ctx context.Context, targetID NodeID) (*StateSyncResponse, error) {
	targetInfo := cc.membership.GetNode(targetID)
	if targetInfo == nil {
		return nil, fmt.Errorf("node %s not found", targetID)
	}

	// GET request doesn't need body
	url := fmt.Sprintf("http://%s/cluster/state", targetInfo.ClusterAddress.String())
	httpReq, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	httpResp, err := cc.httpClient.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(httpResp.Body)
		return nil, fmt.Errorf("request failed: %s", string(body))
	}

	var resp StateSyncResponse
	if err := json.NewDecoder(httpResp.Body).Decode(&resp); err != nil {
		return nil, err
	}

	return &resp, nil
}

// =============================================================================
// REPLICATION CLIENT METHODS
// =============================================================================
//
// These methods support log replication between leader and follower partitions.
// Followers use Fetch to pull messages from leaders.
//
// PULL-BASED REPLICATION MODEL:
//   Leader                     Follower
//   ┌──────────┐              ┌──────────┐
//   │          │◄─────────────│ Fetch()  │  (every 500ms)
//   │  Log     │              │          │
//   │          │─────────────►│  Apply   │
//   └──────────┘ FetchResponse└──────────┘
//
// WHY PULL:
//   - Follower controls pace (backpressure)
//   - Leader doesn't need to track each follower's position
//   - Simpler failure handling
//

// Fetch requests log entries from a partition leader.
// Used by followers to replicate leader's log.
func (cc *ClusterClient) Fetch(ctx context.Context, leaderAddr string, req *FetchRequest) (*FetchResponse, error) {
	var resp FetchResponse
	err := cc.doRequest(ctx, leaderAddr, "/cluster/fetch", req, &resp)
	if err != nil {
		return nil, fmt.Errorf("fetch from leader: %w", err)
	}
	return &resp, nil
}

// =============================================================================
// SNAPSHOT METHODS
// =============================================================================
//
// WHY SNAPSHOTS FOR FOLLOWERS:
//   When a follower is very far behind (e.g., after crash recovery or new replica),
//   fetching messages one-by-one is too slow. Snapshots provide fast catch-up:
//
//   FLOW:
//   ┌────────────────────────────────────────────────────────────────────────┐
//   │                                                                        │
//   │   1. Follower's fetch offset is out of range (log truncated)           │
//   │      - Leader returns FetchErrorOffsetOutOfRange                       │
//   │                                                                        │
//   │   2. Follower requests snapshot from leader                            │
//   │      - RequestSnapshot() → POST /cluster/snapshot/create               │
//   │                                                                        │
//   │   3. Leader creates snapshot (tar.gz of log files + metadata)          │
//   │      - Returns download URL and snapshot info                          │
//   │                                                                        │
//   │   4. Follower downloads snapshot                                       │
//   │      - DownloadSnapshot() → GET /cluster/snapshot/{topic}/{part}/{off} │
//   │                                                                        │
//   │   5. Follower applies snapshot to local storage                        │
//   │      - Replaces local log with snapshot contents                       │
//   │      - Updates fetch offset to snapshot's last offset + 1              │
//   │                                                                        │
//   │   6. Follower resumes normal fetching                                  │
//   │                                                                        │
//   └────────────────────────────────────────────────────────────────────────┘
//
// COMPARISON:
//   - Kafka: Log truncation and follower catch-up via fetch
//   - Raft: Snapshot + InstallSnapshot RPC
//   - etcd: Snapshot transfer for lagging followers
//   - goqueue: Similar to etcd, tar.gz snapshot over HTTP
//
// =============================================================================

// RequestSnapshot asks the leader to create a snapshot for catch-up.
// Returns snapshot metadata including download URL.
func (cc *ClusterClient) RequestSnapshot(ctx context.Context, leaderAddr string, topic string, partition int, requestedBy NodeID) (*SnapshotResponse, error) {
	req := SnapshotRequest{
		Topic:       topic,
		Partition:   partition,
		RequestedBy: requestedBy,
	}

	var resp SnapshotResponse
	err := cc.doRequest(ctx, leaderAddr, "/cluster/snapshot/create", req, &resp)
	if err != nil {
		return nil, fmt.Errorf("request snapshot: %w", err)
	}

	if resp.ErrorCode != SnapshotErrorNone {
		return nil, fmt.Errorf("snapshot error (%d): %s", resp.ErrorCode, resp.ErrorMessage)
	}

	return &resp, nil
}

// DownloadSnapshot downloads a snapshot file from the leader.
// The snapshot is saved to the specified destination directory.
// Returns the path to the downloaded snapshot file.
func (cc *ClusterClient) DownloadSnapshot(ctx context.Context, leaderAddr string, downloadURL string, destDir string) (string, error) {
	// Be defensive: callers may pass an empty destination directory.
	// In that case we download into the current working directory.
	if destDir == "" {
		destDir = "."
	}

	// Build full URL.
	url := fmt.Sprintf("http://%s%s", leaderAddr, downloadURL)

	// Create request.
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return "", fmt.Errorf("create request: %w", err)
	}

	// Execute request.
	resp, err := cc.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("download snapshot: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("download failed: status %d", resp.StatusCode)
	}

	// Ensure destination directory exists.
	if err := os.MkdirAll(destDir, 0755); err != nil {
		return "", fmt.Errorf("create dest dir: %w", err)
	}

	// Create destination file.
	// Extract filename from URL or use a default.
	destPath := filepath.Join(destDir, "snapshot.tar.gz")
	file, err := os.Create(destPath)
	if err != nil {
		return "", fmt.Errorf("create snapshot file: %w", err)
	}
	defer file.Close()

	// Copy response body to file.
	_, err = io.Copy(file, resp.Body)
	if err != nil {
		os.Remove(destPath)
		return "", fmt.Errorf("write snapshot file: %w", err)
	}

	return destPath, nil
}

// PushMetadata pushes metadata to a follower node.
// Used by controller to sync metadata.
func (cc *ClusterClient) PushMetadata(ctx context.Context, targetID NodeID, meta *ClusterMeta) error {
	targetInfo := cc.membership.GetNode(targetID)
	if targetInfo == nil {
		return fmt.Errorf("node %s not found", targetID)
	}

	req := struct {
		Metadata *ClusterMeta `json:"metadata"`
	}{
		Metadata: meta,
	}

	var resp map[string]bool
	return cc.doRequest(ctx, targetInfo.ClusterAddress.String(), "/cluster/metadata", req, &resp)
}

// doRequest performs an HTTP request to a cluster node.
func (cc *ClusterClient) doRequest(ctx context.Context, addr string, path string, reqBody interface{}, respBody interface{}) error {
	// Marshal request body
	body, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}

	// Build URL
	url := fmt.Sprintf("http://%s%s", addr, path)

	// Create request
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	// Execute request
	resp, err := cc.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	// Check status
	if resp.StatusCode != http.StatusOK {
		respBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("request failed (%d): %s", resp.StatusCode, string(respBytes))
	}

	// Decode response
	if err := json.NewDecoder(resp.Body).Decode(respBody); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}

	return nil
}

// =============================================================================
// GOSSIP-STYLE MEMBERSHIP PROPAGATION
// =============================================================================
//
// WHY MERGE HEARTBEAT RESPONSES?
//
// PROBLEM WITHOUT GOSSIP:
//   - Node A knows about [A, B, C]
//   - Node D joins via Node B only
//   - Node A never learns about D until D heartbeats to A directly
//   - In a large cluster, this creates "pockets" of inconsistent membership
//
// SOLUTION - GOSSIP-LIKE PROPAGATION:
//   - When A heartbeats to B, B responds with its full membership view
//   - A merges B's view into its own
//   - Eventually, all nodes converge to same membership
//
// FLOW:
//   ┌──────────────────────────────────────────────────────────────────────┐
//   │  Gossip Propagation                                                  │
//   │                                                                      │
//   │   Node A heartbeats → Node B                                         │
//   │   Node B responds with {A, B, C, D}                                  │
//   │   Node A merges → now knows {A, B, C, D}                             │
//   │                                                                      │
//   │   Node A heartbeats → Node C                                         │
//   │   Node A tells C about D (in its heartbeat state)                    │
//   │   Node C responds with {A, B, C}                                     │
//   │   Node C learns about D from A's heartbeat                           │
//   │                                                                      │
//   │   After O(log N) rounds, all nodes have consistent view              │
//   └──────────────────────────────────────────────────────────────────────┘
//
// COMPARISON:
//   - Cassandra: Full gossip protocol (SWIM-based)
//   - Kafka: Controller broadcasts state, followers don't gossip
//   - Consul: SWIM + Serf for membership
//   - goqueue: Simple piggyback gossip on heartbeats
//
// =============================================================================

// BroadcastHeartbeats sends heartbeats to all known nodes.
// Called periodically by the heartbeat sender.
// Implements gossip-style membership propagation by merging responses.
func (cc *ClusterClient) BroadcastHeartbeats(ctx context.Context) {
	nodes := cc.membership.AliveNodes()
	myID := cc.node.ID()

	for _, node := range nodes {
		if node.ID == myID {
			continue // Don't heartbeat ourselves
		}

		go func(nodeID NodeID) {
			resp, err := cc.SendHeartbeat(ctx, nodeID)
			if err != nil {
				cc.logger.Debug("heartbeat failed",
					"to", nodeID,
					"error", err)
				return
			}

			// Gossip: merge the response's membership view into ours.
			// This propagates membership changes transitively through the cluster.
			// If the remote node knows about nodes we don't, we learn about them.
			if resp.Nodes != nil && len(resp.Nodes) > 0 {
				clusterState := &ClusterState{
					Version:         resp.Version,
					ControllerID:    resp.ControllerID,
					ControllerEpoch: resp.ControllerEpoch,
					Nodes:           resp.Nodes,
				}

				if err := cc.membership.MergeState(clusterState); err != nil {
					cc.logger.Debug("failed to merge heartbeat state",
						"from", nodeID,
						"error", err)
				}
			}
		}(node.ID)
	}
}
