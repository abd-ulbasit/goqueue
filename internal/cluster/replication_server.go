// =============================================================================
// REPLICATION SERVER - HTTP ENDPOINTS FOR LOG REPLICATION
// =============================================================================
//
// WHAT: HTTP endpoints that followers use to fetch logs from leaders.
//
// NEW ENDPOINTS (M11):
//   POST /cluster/fetch            - Followers fetch logs from leader
//   POST /cluster/snapshot/create  - Request snapshot for fast catch-up
//   GET  /cluster/snapshot/{topic}/{partition}/{offset}  - Download snapshot
//   POST /cluster/isr              - Report ISR updates to controller
//   POST /cluster/leader-election  - Request leader election
//   GET  /cluster/partition/{topic}/{partition}/info - Get partition replication info
//
// REPLICATION FLOW (Pull Model):
//   ┌─────────────────────────────────────────────────────────────────────────┐
//   │                                                                         │
//   │   1. Follower sends FetchRequest to Leader                              │
//   │      - Includes: topic, partition, fromOffset, followerID               │
//   │                                                                         │
//   │   2. Leader validates request                                           │
//   │      - Check epoch (reject stale leaders)                               │
//   │      - Check offset range                                               │
//   │                                                                         │
//   │   3. Leader reads messages from log                                     │
//   │      - Up to maxBytes                                                   │
//   │      - Returns high watermark                                           │
//   │                                                                         │
//   │   4. Leader records follower progress                                   │
//   │      - Updates ISR manager                                              │
//   │                                                                         │
//   │   5. Follower applies messages to local log                             │
//   │      - Updates local offset                                             │
//   │                                                                         │
//   └─────────────────────────────────────────────────────────────────────────┘
//
// COMPARISON:
//   - Kafka: Custom binary protocol over TCP
//   - RabbitMQ: Erlang distribution protocol
//   - etcd: Raft AppendEntries RPC
//   - goqueue: HTTP REST (simple, debuggable)
//
// =============================================================================

package cluster

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
)

// =============================================================================
// REPLICATION SERVER
// =============================================================================

// LogReaderFunc is a callback to read messages from storage.
// This allows the cluster package to read from broker storage without direct dependency.
//
// WHY A CALLBACK?
//
//	The cluster package contains replication logic but shouldn't depend on broker.
//	The broker sets this callback during initialization to provide storage access.
//
// PARAMETERS:
//   - topic: Topic name
//   - partition: Partition number
//   - fromOffset: Starting offset (inclusive)
//   - maxBytes: Maximum bytes to read (approximate, may return slightly more)
//
// RETURNS:
//   - messages: Slice of replicated messages
//   - logEndOffset: Current end offset of the log (LEO)
//   - error: If read fails (offset out of range, partition not found, etc.)
type LogReaderFunc func(topic string, partition int, fromOffset int64, maxBytes int64) ([]ReplicatedMessage, int64, error)

// LogDirFunc is a callback to get the log directory for a partition.
type LogDirFunc func(topic string, partition int) string

// ReplicationServer handles replication HTTP endpoints.
// This is embedded in or used alongside ClusterServer.
type ReplicationServer struct {
	// replicaManager manages local replicas.
	replicaManager *ReplicaManager

	// snapshotManager handles snapshots.
	snapshotManager *SnapshotManager

	// partitionElector handles leader elections.
	partitionElector *PartitionLeaderElector

	// metadataStore for partition info.
	metadataStore *MetadataStore

	// logReader reads messages from storage.
	// Set by broker during initialization.
	logReader LogReaderFunc

	// logDirFn returns the log directory for a partition.
	// Set by broker during initialization.
	logDirFn LogDirFunc

	// config for replication settings.
	config ReplicationConfig

	// logger for operations.
	logger *slog.Logger
}

// NewReplicationServer creates a new replication server.
func NewReplicationServer(
	replicaManager *ReplicaManager,
	snapshotManager *SnapshotManager,
	partitionElector *PartitionLeaderElector,
	metadataStore *MetadataStore,
	config ReplicationConfig,
	logger *slog.Logger,
) *ReplicationServer {
	return &ReplicationServer{
		replicaManager:   replicaManager,
		snapshotManager:  snapshotManager,
		partitionElector: partitionElector,
		metadataStore:    metadataStore,
		config:           config,
		logger:           logger.With("component", "replication-server"),
	}
}

// SetLogReader sets the callback for reading messages from storage.
// Must be called before the server starts handling requests.
func (rs *ReplicationServer) SetLogReader(reader LogReaderFunc) {
	rs.logReader = reader
}

// SetLogDirFn sets the callback for getting log directories.
// Must be called before the server starts handling requests.
func (rs *ReplicationServer) SetLogDirFn(fn LogDirFunc) {
	rs.logDirFn = fn
}

// RegisterRoutes registers replication endpoints.
func (rs *ReplicationServer) RegisterRoutes(mux *http.ServeMux) {
	// Fetch endpoint - followers pull from leaders.
	mux.HandleFunc("POST /cluster/fetch", rs.handleFetch)

	// Snapshot endpoints - for fast catch-up.
	mux.HandleFunc("POST /cluster/snapshot/create", rs.handleCreateSnapshot)
	mux.HandleFunc("GET /cluster/snapshot/{topic}/{partition}/{offset}", rs.handleGetSnapshot)

	// ISR update endpoint - replicas report to controller.
	mux.HandleFunc("POST /cluster/isr", rs.handleISRUpdate)

	// Leader election endpoint - request election.
	mux.HandleFunc("POST /cluster/leader-election", rs.handleLeaderElection)

	// Partition info endpoint.
	mux.HandleFunc("GET /cluster/partition/{topic}/{partition}/info", rs.handlePartitionInfo)
}

// =============================================================================
// FETCH HANDLER
// =============================================================================
//
// WHAT: Followers call this to get messages from the leader.
//
// FLOW:
//   ┌──────────────────────────────────────────────────────────────────────┐
//   │                                                                      │
//   │   Follower                         Leader                            │
//   │   ────────                         ──────                            │
//   │                                                                      │
//   │   FetchRequest ───────────────────►                                  │
//   │   {topic, partition,               Validate:                         │
//   │    fromOffset, epoch,              - Am I leader? (check role)       │
//   │    followerID, maxBytes}           - Valid epoch? (reject stale)     │
//   │                                    - Offset in range?                │
//   │                                                                      │
//   │                                    Read messages from log            │
//   │                                                                      │
//   │                                    Record follower progress          │
//   │                                    (for ISR tracking)                │
//   │                                                                      │
//   │   ◄─────────────────────────────── FetchResponse                     │
//   │                                    {messages, highWatermark,         │
//   │                                     leaderEpoch, errorCode}          │
//   │                                                                      │
//   └──────────────────────────────────────────────────────────────────────┘
//
// ERROR CODES:
//   - FetchErrNone: Success
//   - FetchErrNotLeader: Not the leader, include actual leader
//   - FetchErrOffsetOutOfRange: Requested offset too old or too new
//   - FetchErrUnknownPartition: Partition doesn't exist
//   - FetchErrFencedLeader: Stale leader epoch
//
// =============================================================================

func (rs *ReplicationServer) handleFetch(w http.ResponseWriter, r *http.Request) {
	var req FetchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		rs.jsonError(w, "invalid request body", http.StatusBadRequest)
		return
	}

	rs.logger.Debug("fetch request received",
		"topic", req.Topic,
		"partition", req.Partition,
		"from_offset", req.FromOffset,
		"follower", req.FollowerID,
		"epoch", req.LeaderEpoch)

	// Get local replica.
	replica := rs.replicaManager.GetLocalReplica(req.Topic, req.Partition)
	if replica == nil {
		rs.jsonResponse(w, &FetchResponse{
			Topic:        req.Topic,
			Partition:    req.Partition,
			ErrorCode:    FetchErrorUnknownPartition,
			ErrorMessage: "partition not found on this node",
		})
		return
	}

	replica.mu.RLock()
	role := replica.State.Role
	currentEpoch := replica.State.LeaderEpoch
	replica.mu.RUnlock()

	// Validate we are the leader.
	if role != ReplicaRoleLeader {
		rs.jsonResponse(w, &FetchResponse{
			Topic:        req.Topic,
			Partition:    req.Partition,
			ErrorCode:    FetchErrorNotLeader,
			ErrorMessage: "not the leader for this partition",
		})
		return
	}

	// Validate epoch (fencing).
	// If follower has stale epoch, tell them to refresh metadata.
	if req.LeaderEpoch > 0 && req.LeaderEpoch < currentEpoch {
		rs.jsonResponse(w, &FetchResponse{
			Topic:        req.Topic,
			Partition:    req.Partition,
			ErrorCode:    FetchErrorEpochFenced,
			ErrorMessage: fmt.Sprintf("stale epoch %d, current is %d", req.LeaderEpoch, currentEpoch),
			LeaderEpoch:  currentEpoch,
		})
		return
	}

	// Get messages from log.
	// In real implementation, this would read from the partition's storage log.
	// For now, we interface with ReplicaManager.
	maxBytes := req.MaxBytes
	if maxBytes == 0 {
		maxBytes = 1024 * 1024 // 1MB default
	}

	// Read messages from local storage.
	messages, logEndOffset, err := rs.readMessagesFromLog(req.Topic, req.Partition, req.FromOffset, int64(maxBytes))
	if err != nil {
		if strings.Contains(err.Error(), "offset out of range") {
			rs.jsonResponse(w, &FetchResponse{
				Topic:        req.Topic,
				Partition:    req.Partition,
				ErrorCode:    FetchErrorOffsetOutOfRange,
				ErrorMessage: err.Error(),
				LeaderEpoch:  currentEpoch,
				LogEndOffset: logEndOffset,
			})
			return
		}

		rs.logger.Error("failed to read messages",
			"topic", req.Topic,
			"partition", req.Partition,
			"error", err)
		rs.jsonError(w, "internal error reading messages", http.StatusInternalServerError)
		return
	}

	// Record follower progress for ISR tracking.
	// Pass the leader's LEO so ISRManager can calculate lag.
	if replica.isrManager != nil && req.FollowerID != "" {
		fetchOffset := req.FromOffset + int64(len(messages))
		replica.isrManager.RecordFetch(req.FollowerID, fetchOffset, logEndOffset)
	}

	// Get high watermark.
	replica.mu.RLock()
	highWatermark := replica.State.HighWatermark
	replica.mu.RUnlock()

	rs.logger.Debug("fetch response",
		"topic", req.Topic,
		"partition", req.Partition,
		"messages", len(messages),
		"high_watermark", highWatermark)

	rs.jsonResponse(w, &FetchResponse{
		Topic:         req.Topic,
		Partition:     req.Partition,
		Messages:      messages,
		HighWatermark: highWatermark,
		LeaderEpoch:   currentEpoch,
		LogEndOffset:  logEndOffset,
		ErrorCode:     FetchErrorNone,
	})
}

// readMessagesFromLog reads messages from storage.
//
// HOW IT WORKS:
//  1. Delegates to the logReader callback set by the broker
//  2. If no callback set, returns empty (graceful degradation)
//  3. The broker's callback reads from actual storage.Log
//
// WHY CALLBACK PATTERN:
//   - Cluster package shouldn't import broker package (circular dep)
//   - Broker sets the callback during ReplicationServer initialization
//   - Clean separation: cluster handles protocol, broker handles storage
//
// ERROR HANDLING:
//   - "offset out of range" triggers snapshot-based catch-up
//   - Other errors are logged and propagated
func (rs *ReplicationServer) readMessagesFromLog(topic string, partition int, fromOffset int64, maxBytes int64) ([]ReplicatedMessage, int64, error) {
	// If no log reader configured, return empty.
	// This allows the server to start without storage (useful for testing).
	if rs.logReader == nil {
		rs.logger.Warn("log reader not configured, returning empty",
			"topic", topic,
			"partition", partition)
		return []ReplicatedMessage{}, fromOffset, nil
	}

	// Delegate to the broker's log reader.
	return rs.logReader(topic, partition, fromOffset, maxBytes)
}

// getLeaderHint returns the current leader for a partition.
func (rs *ReplicationServer) getLeaderHint(topic string, partition int) NodeID {
	assignment := rs.metadataStore.GetAssignment(topic, partition)
	if assignment == nil {
		return ""
	}
	return assignment.Leader
}

// =============================================================================
// SNAPSHOT HANDLERS
// =============================================================================
//
// WHY SNAPSHOTS?
//   When a follower is very far behind (e.g., new node or recovered after outage),
//   fetching messages one by one is too slow. Instead:
//   1. Leader creates a snapshot of current state
//   2. Follower downloads snapshot
//   3. Follower applies snapshot and continues from there
//
// FLOW:
//   ┌──────────────────────────────────────────────────────────────────────┐
//   │                                                                      │
//   │   Follower far behind:                                               │
//   │                                                                      │
//   │   1. Follower tries to fetch from offset 0                           │
//   │      Leader responds: "offset out of range, min is 50000"            │
//   │                                                                      │
//   │   2. Follower requests snapshot creation                             │
//   │      POST /cluster/snapshot/create                                   │
//   │                                                                      │
//   │   3. Leader creates snapshot at current offset                       │
//   │      (tar + gzip the log segment files)                              │
//   │                                                                      │
//   │   4. Follower downloads snapshot                                     │
//   │      GET /cluster/snapshot/{topic}/{partition}/{offset}              │
//   │                                                                      │
//   │   5. Follower applies snapshot, resumes fetching                     │
//   │                                                                      │
//   └──────────────────────────────────────────────────────────────────────┘
//
// =============================================================================

func (rs *ReplicationServer) handleCreateSnapshot(w http.ResponseWriter, r *http.Request) {
	var req SnapshotRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		rs.jsonError(w, "invalid request body", http.StatusBadRequest)
		return
	}

	rs.logger.Info("snapshot creation requested",
		"topic", req.Topic,
		"partition", req.Partition,
		"by", req.RequestedBy)

	// Verify we're the leader.
	replica := rs.replicaManager.GetLocalReplica(req.Topic, req.Partition)
	if replica == nil {
		rs.jsonResponse(w, &SnapshotResponse{
			ErrorCode:    SnapshotErrorNotAvailable,
			ErrorMessage: "partition not found on this node",
		})
		return
	}

	replica.mu.RLock()
	role := replica.State.Role
	logEndOffset := replica.State.LogEndOffset
	epoch := replica.State.LeaderEpoch
	replica.mu.RUnlock()

	if role != ReplicaRoleLeader {
		rs.jsonResponse(w, &SnapshotResponse{
			ErrorCode:    SnapshotErrorNotLeader,
			ErrorMessage: "not the leader for this partition",
		})
		return
	}

	// Create snapshot.
	// The snapshot includes all data up to logEndOffset-1.
	logDir := rs.getLogDirectory(req.Topic, req.Partition)
	snap, err := rs.snapshotManager.CreateSnapshot(req.Topic, req.Partition, logDir, logEndOffset-1, epoch)
	if err != nil {
		rs.logger.Error("failed to create snapshot",
			"topic", req.Topic,
			"partition", req.Partition,
			"error", err)
		rs.jsonResponse(w, &SnapshotResponse{
			ErrorCode:    SnapshotErrorInternal,
			ErrorMessage: fmt.Sprintf("failed to create snapshot: %v", err),
		})
		return
	}

	rs.jsonResponse(w, &SnapshotResponse{
		ErrorCode:   SnapshotErrorNone,
		Snapshot:    snap,
		DownloadURL: fmt.Sprintf("/cluster/snapshot/%s/%d/%d", req.Topic, req.Partition, snap.LastIncludedOffset),
	})
}

// getLogDirectory returns the log directory for a partition.
//
// Uses the callback set by the broker to get the actual storage path.
// Falls back to a standard layout if callback not set.
func (rs *ReplicationServer) getLogDirectory(topic string, partition int) string {
	if rs.logDirFn != nil {
		return rs.logDirFn(topic, partition)
	}
	// Fallback to standard layout.
	return fmt.Sprintf("/data/logs/%s/%d", topic, partition)
}

func (rs *ReplicationServer) handleGetSnapshot(w http.ResponseWriter, r *http.Request) {
	// Parse path parameters.
	topic := r.PathValue("topic")
	partitionStr := r.PathValue("partition")
	offsetStr := r.PathValue("offset")

	partition, err := strconv.Atoi(partitionStr)
	if err != nil {
		rs.jsonError(w, "invalid partition number", http.StatusBadRequest)
		return
	}

	offset, err := strconv.ParseInt(offsetStr, 10, 64)
	if err != nil {
		rs.jsonError(w, "invalid offset", http.StatusBadRequest)
		return
	}

	rs.logger.Debug("snapshot download requested",
		"topic", topic,
		"partition", partition,
		"offset", offset)

	// Get the snapshot for this partition.
	snap := rs.snapshotManager.GetSnapshot(topic, partition)
	if snap == nil {
		rs.jsonError(w, "snapshot not found", http.StatusNotFound)
		return
	}

	// Verify the requested offset matches.
	if snap.LastIncludedOffset != offset {
		rs.jsonError(w, "snapshot offset mismatch", http.StatusNotFound)
		return
	}

	// Stream the snapshot file.
	http.ServeFile(w, r, snap.FilePath)
}

// =============================================================================
// ISR UPDATE HANDLER
// =============================================================================
//
// WHY:
//   Replicas report their sync state to the controller.
//   Controller maintains authoritative ISR list.
//
// WHO CALLS THIS:
//   - Leaders report ISR shrinks (follower fell behind)
//   - Followers request to rejoin ISR (caught up)
//
// =============================================================================

func (rs *ReplicationServer) handleISRUpdate(w http.ResponseWriter, r *http.Request) {
	var req ISRUpdate
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		rs.jsonError(w, "invalid request body", http.StatusBadRequest)
		return
	}

	rs.logger.Info("ISR update received",
		"topic", req.Topic,
		"partition", req.Partition,
		"type", req.Type,
		"node", req.NodeID)

	// Update ISR in metadata store.
	switch req.Type {
	case ISRShrink:
		// Leader reports a follower fell out of ISR.
		if err := rs.removeFromISR(req.Topic, req.Partition, req.NodeID); err != nil {
			rs.jsonError(w, fmt.Sprintf("failed to shrink ISR: %v", err), http.StatusInternalServerError)
			return
		}

	case ISRExpand:
		// Follower requests to rejoin ISR.
		if err := rs.addToISR(req.Topic, req.Partition, req.NodeID, req.Offset); err != nil {
			rs.jsonError(w, fmt.Sprintf("failed to expand ISR: %v", err), http.StatusInternalServerError)
			return
		}

	default:
		rs.jsonError(w, "unknown ISR update type", http.StatusBadRequest)
		return
	}

	rs.jsonResponse(w, map[string]bool{"success": true})
}

func (rs *ReplicationServer) removeFromISR(topic string, partition int, nodeID NodeID) error {
	assignment := rs.metadataStore.GetAssignment(topic, partition)
	if assignment == nil {
		return fmt.Errorf("partition not found")
	}

	// Build new ISR without the node.
	newISR := make([]NodeID, 0, len(assignment.ISR))
	for _, id := range assignment.ISR {
		if id != nodeID {
			newISR = append(newISR, id)
		}
	}

	return rs.metadataStore.UpdateISR(topic, partition, newISR)
}

func (rs *ReplicationServer) addToISR(topic string, partition int, nodeID NodeID, offset int64) error {
	assignment := rs.metadataStore.GetAssignment(topic, partition)
	if assignment == nil {
		return fmt.Errorf("partition not found")
	}

	// Check if already in ISR.
	for _, id := range assignment.ISR {
		if id == nodeID {
			return nil // Already in ISR
		}
	}

	// Verify node is in replicas.
	inReplicas := false
	for _, id := range assignment.Replicas {
		if id == nodeID {
			inReplicas = true
			break
		}
	}
	if !inReplicas {
		return fmt.Errorf("node %s is not a replica for this partition", nodeID)
	}

	// TODO: Verify offset is caught up with leader.
	// For now, trust the request.

	newISR := append(assignment.ISR, nodeID)
	return rs.metadataStore.UpdateISR(topic, partition, newISR)
}

// =============================================================================
// LEADER ELECTION HANDLER
// =============================================================================

func (rs *ReplicationServer) handleLeaderElection(w http.ResponseWriter, r *http.Request) {
	var req LeaderElectionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		rs.jsonError(w, "invalid request body", http.StatusBadRequest)
		return
	}

	rs.logger.Info("leader election requested",
		"topic", req.Topic,
		"partition", req.Partition,
		"reason", req.Reason)

	resp, err := rs.partitionElector.ElectLeader(&req)
	if err != nil {
		rs.jsonError(w, fmt.Sprintf("election failed: %v", err), http.StatusInternalServerError)
		return
	}

	rs.jsonResponse(w, resp)
}

// =============================================================================
// PARTITION INFO HANDLER
// =============================================================================

func (rs *ReplicationServer) handlePartitionInfo(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	partitionStr := r.PathValue("partition")

	partition, err := strconv.Atoi(partitionStr)
	if err != nil {
		rs.jsonError(w, "invalid partition number", http.StatusBadRequest)
		return
	}

	assignment := rs.metadataStore.GetAssignment(topic, partition)
	if assignment == nil {
		rs.jsonError(w, "partition not found", http.StatusNotFound)
		return
	}

	// Get local replica info if we have it.
	var localInfo *PartitionReplicaInfo
	replica := rs.replicaManager.GetLocalReplica(topic, partition)
	if replica != nil {
		replica.mu.RLock()
		localInfo = &PartitionReplicaInfo{
			Role:          replica.State.Role,
			LogEndOffset:  replica.State.LogEndOffset,
			HighWatermark: replica.State.HighWatermark,
			Epoch:         replica.State.LeaderEpoch,
		}
		replica.mu.RUnlock()
	}

	resp := &PartitionInfoResponse{
		Topic:        topic,
		Partition:    partition,
		Leader:       assignment.Leader,
		Replicas:     assignment.Replicas,
		ISR:          assignment.ISR,
		Version:      assignment.Version,
		LocalReplica: localInfo,
	}

	rs.jsonResponse(w, resp)
}

// =============================================================================
// HELPER TYPES
// =============================================================================

// PartitionReplicaInfo contains local replica state.
type PartitionReplicaInfo struct {
	Role          ReplicaRole `json:"role"`
	LogEndOffset  int64       `json:"log_end_offset"`
	HighWatermark int64       `json:"high_watermark"`
	Epoch         int64       `json:"epoch"`
}

// PartitionInfoResponse contains partition replication info.
type PartitionInfoResponse struct {
	Topic        string                `json:"topic"`
	Partition    int                   `json:"partition"`
	Leader       NodeID                `json:"leader"`
	Replicas     []NodeID              `json:"replicas"`
	ISR          []NodeID              `json:"isr"`
	Version      int64                 `json:"version"`
	LocalReplica *PartitionReplicaInfo `json:"local_replica,omitempty"`
}

// =============================================================================
// HELPER METHODS
// =============================================================================

func (rs *ReplicationServer) jsonResponse(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

func (rs *ReplicationServer) jsonError(w http.ResponseWriter, message string, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": message})
}
