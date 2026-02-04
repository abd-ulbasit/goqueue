// =============================================================================
// FAILURE DETECTOR - HEARTBEAT-BASED NODE HEALTH MONITORING
// =============================================================================
//
// WHAT: Detects when cluster nodes become unresponsive.
//
// HOW IT WORKS:
//   1. Each node sends heartbeats to all other nodes periodically
//   2. If we don't receive a heartbeat from a node for SuspectTimeout → SUSPECT
//   3. If no heartbeat for DeadTimeout → DEAD
//   4. If heartbeat received from SUSPECT/DEAD node → RECOVERED
//
// THE FUNDAMENTAL PROBLEM:
//   We can NEVER know for sure if a node is dead or just slow/unreachable.
//   This is the core challenge of distributed systems.
//
//   "A process P considers another process Q dead if P's view of Q is that
//    Q has not sent any message within a specified timeout."
//
// TRADE-OFF (configured via timeouts):
//   - SHORT timeout: Fast failure detection, but more false positives
//     (network hiccup looks like node death)
//   - LONG timeout: Fewer false positives, but slow to detect real failures
//
// COMPARISON:
//   - Kafka: Uses session.timeout.ms (default 45s) for consumers
//   - Cassandra: Phi Accrual Failure Detector (probabilistic)
//   - etcd: Raft heartbeat (150ms) with election timeout (1000-2000ms)
//   - goqueue: Simple timeout-based (3s heartbeat, 9s dead)
//
// =============================================================================

package cluster

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// =============================================================================
// FAILURE DETECTOR
// =============================================================================

// FailureDetector monitors node health using heartbeats.
type FailureDetector struct {
	// mu protects mutable state
	mu sync.RWMutex

	// membership is the cluster membership manager
	membership *Membership

	// config holds timing configuration
	config *ClusterConfig

	// lastHeartbeats tracks when we last heard from each node
	lastHeartbeats map[NodeID]time.Time

	// ctx for lifecycle management
	ctx    context.Context
	cancel context.CancelFunc

	// wg tracks background goroutines
	wg sync.WaitGroup

	// logger for failure detection events
	logger *slog.Logger
}

// NewFailureDetector creates a new failure detector.
func NewFailureDetector(membership *Membership, config *ClusterConfig) *FailureDetector {
	ctx, cancel := context.WithCancel(context.Background())

	return &FailureDetector{
		membership:     membership,
		config:         config,
		lastHeartbeats: make(map[NodeID]time.Time),
		ctx:            ctx,
		cancel:         cancel,
		logger:         slog.Default().With("component", "failure_detector"),
	}
}

// =============================================================================
// LIFECYCLE
// =============================================================================

// Start begins the failure detection loop.
//
// DETECTION LOOP:
//
//	Every HeartbeatInterval, check all nodes:
//	  - If lastHeartbeat > SuspectTimeout → mark SUSPECT
//	  - If lastHeartbeat > DeadTimeout → mark DEAD
//
// ┌─────────────────────────────────────────────────────────────────┐
// │                    FAILURE DETECTION TIMELINE                   │
// │                                                                 │
// │  Time: ─────────────────────────────────────────────────────►   │
// │                                                                 │
// │  Heartbeat ──●─────●─────●─────●─────────────────────────────   │
// │  Received                       ▲                               │
// │                                 │ Last heartbeat                │
// │                                 │                               │
// │  ─────────────────────────────────────────────────────────────  │
// │           │         │                │                          │
// │           ▼         ▼                ▼                          │
// │        ALIVE    SUSPECT (6s)      DEAD (9s)                     │
// │                                                                 │
// │  Timeouts configured:                                           │
// │    SuspectTimeout = 6s (2 missed heartbeats)                    │
// │    DeadTimeout = 9s (3 missed heartbeats)                       │
// └─────────────────────────────────────────────────────────────────┘
func (fd *FailureDetector) Start() {
	// =========================================================================
	// SEED INITIAL HEARTBEATS FOR ALL EXISTING NODES
	// =========================================================================
	//
	// WHY: When failure detector starts, there may already be nodes in the
	// cluster (learned from bootstrap). We seed initial heartbeats for them
	// so they can be properly detected if they fail before sending a heartbeat.
	//
	// ALSO: Register a listener to seed heartbeats for newly discovered nodes.
	//
	fd.seedExistingNodes()
	fd.membership.AddListener(fd.handleMembershipEvent)

	fd.wg.Add(1)
	go fd.detectionLoop()

	fd.logger.Info("failure detector started",
		"heartbeat_interval", fd.config.HeartbeatInterval,
		"suspect_timeout", fd.config.SuspectTimeout,
		"dead_timeout", fd.config.DeadTimeout,
	)
}

// seedExistingNodes records initial heartbeats for all nodes already in membership.
func (fd *FailureDetector) seedExistingNodes() {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	for _, node := range fd.membership.GetOtherNodes() {
		if _, ok := fd.lastHeartbeats[node.ID]; !ok {
			fd.lastHeartbeats[node.ID] = time.Now()
		}
	}
}

// handleMembershipEvent seeds heartbeats for newly joined nodes.
func (fd *FailureDetector) handleMembershipEvent(event MembershipEvent) {
	if event.Type == EventNodeJoined {
		fd.mu.Lock()
		if _, ok := fd.lastHeartbeats[event.NodeID]; !ok {
			fd.lastHeartbeats[event.NodeID] = time.Now()
		}
		fd.mu.Unlock()
	}
}

// Stop gracefully shuts down the failure detector.
func (fd *FailureDetector) Stop() {
	fd.cancel()
	fd.wg.Wait()
	fd.logger.Info("failure detector stopped")
}

// =============================================================================
// HEARTBEAT HANDLING
// =============================================================================

// RecordHeartbeat records a heartbeat from a node.
// Called when we receive a heartbeat message.
func (fd *FailureDetector) RecordHeartbeat(nodeID NodeID) {
	fd.mu.Lock()
	fd.lastHeartbeats[nodeID] = time.Now()
	fd.mu.Unlock()

	// Update membership (this may trigger recovery if node was suspect/dead)
	if err := fd.membership.UpdateHeartbeat(nodeID); err != nil {
		fd.logger.Warn("failed to update heartbeat in membership",
			"node_id", nodeID,
			"error", err,
		)
	}
}

// LastHeartbeat returns when we last heard from a node.
func (fd *FailureDetector) LastHeartbeat(nodeID NodeID) (time.Time, bool) {
	fd.mu.RLock()
	defer fd.mu.RUnlock()

	t, ok := fd.lastHeartbeats[nodeID]
	return t, ok
}

// =============================================================================
// DETECTION LOOP
// =============================================================================

// detectionLoop runs periodically to check node health.
func (fd *FailureDetector) detectionLoop() {
	defer fd.wg.Done()

	ticker := time.NewTicker(fd.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-fd.ctx.Done():
			return
		case <-ticker.C:
			fd.checkNodes()
		}
	}
}

// checkNodes evaluates the health of all nodes.
func (fd *FailureDetector) checkNodes() {
	nodes := fd.membership.GetOtherNodes()
	now := time.Now()

	fd.mu.RLock()
	defer fd.mu.RUnlock()

	for _, node := range nodes {
		lastHB, ok := fd.lastHeartbeats[node.ID]
		if !ok {
			// Never heard from this node
			// Could be new node that hasn't sent heartbeat yet
			continue
		}

		elapsed := now.Sub(lastHB)
		currentStatus := node.Status

		// Determine new status based on elapsed time
		var newStatus NodeStatus
		switch {
		case elapsed >= fd.config.DeadTimeout:
			newStatus = NodeStatusDead
		case elapsed >= fd.config.SuspectTimeout:
			newStatus = NodeStatusSuspect
		default:
			newStatus = NodeStatusAlive
		}

		// Update if status changed
		if newStatus != currentStatus && currentStatus != NodeStatusLeaving {
			fd.handleStatusChange(node.ID, currentStatus, newStatus, elapsed)
		}
	}
}

// handleStatusChange processes a node status transition.
func (fd *FailureDetector) handleStatusChange(nodeID NodeID, from, to NodeStatus, elapsed time.Duration) {
	// Log the transition
	switch to {
	case NodeStatusSuspect:
		fd.logger.Warn("node is suspect (missed heartbeats)",
			"node_id", nodeID,
			"elapsed", elapsed,
			"threshold", fd.config.SuspectTimeout,
		)
	case NodeStatusDead:
		fd.logger.Error("node declared dead",
			"node_id", nodeID,
			"elapsed", elapsed,
			"threshold", fd.config.DeadTimeout,
		)
	case NodeStatusAlive:
		fd.logger.Info("node recovered",
			"node_id", nodeID,
			"previous_status", from,
		)
	}

	// Update membership
	// Note: We need to release read lock before calling UpdateNodeStatus
	// which takes write lock. This is handled by the caller.
	// TODO: why are we doing this in a goroutine?
	go func() {
		if err := fd.membership.UpdateNodeStatus(nodeID, to); err != nil {
			fd.logger.Warn("failed to update node status",
				"node_id", nodeID,
				"status", to,
				"error", err,
			)
		}
	}()
}

// =============================================================================
// HEALTH QUERIES
// =============================================================================

// IsAlive returns true if a node is considered alive.
func (fd *FailureDetector) IsAlive(nodeID NodeID) bool {
	node := fd.membership.GetNode(nodeID)
	if node == nil {
		return false
	}
	return node.Status == NodeStatusAlive
}

// IsSuspect returns true if a node is in suspect state.
func (fd *FailureDetector) IsSuspect(nodeID NodeID) bool {
	node := fd.membership.GetNode(nodeID)
	if node == nil {
		return false
	}
	return node.Status == NodeStatusSuspect
}

// IsDead returns true if a node is declared dead.
func (fd *FailureDetector) IsDead(nodeID NodeID) bool {
	node := fd.membership.GetNode(nodeID)
	if node == nil {
		return false
	}
	return node.Status == NodeStatusDead
}

// =============================================================================
// STATISTICS
// =============================================================================

// Stats contains failure detector statistics.
type FailureDetectorStats struct {
	// AliveCount is the number of alive nodes.
	AliveCount int `json:"alive_count"`

	// SuspectCount is the number of suspect nodes.
	SuspectCount int `json:"suspect_count"`

	// DeadCount is the number of dead nodes.
	DeadCount int `json:"dead_count"`

	// TotalNodes is the total number of tracked nodes.
	TotalNodes int `json:"total_nodes"`

	// HeartbeatInterval is the configured interval.
	HeartbeatInterval time.Duration `json:"heartbeat_interval"`

	// SuspectTimeout is the configured suspect threshold.
	SuspectTimeout time.Duration `json:"suspect_timeout"`

	// DeadTimeout is the configured dead threshold.
	DeadTimeout time.Duration `json:"dead_timeout"`
}

// Stats returns current failure detector statistics.
func (fd *FailureDetector) Stats() FailureDetectorStats {
	nodes := fd.membership.AllNodes()

	stats := FailureDetectorStats{
		TotalNodes:        len(nodes),
		HeartbeatInterval: fd.config.HeartbeatInterval,
		SuspectTimeout:    fd.config.SuspectTimeout,
		DeadTimeout:       fd.config.DeadTimeout,
	}

	for _, node := range nodes {
		switch node.Status {
		case NodeStatusAlive:
			stats.AliveCount++
		case NodeStatusSuspect:
			stats.SuspectCount++
		case NodeStatusDead:
			stats.DeadCount++
		}
	}

	return stats
}

// =============================================================================
// DEBUGGING
// =============================================================================

// NodeHealthReport contains detailed health info for a node.
type NodeHealthReport struct {
	NodeID         NodeID        `json:"node_id"`
	Status         NodeStatus    `json:"status"`
	LastHeartbeat  time.Time     `json:"last_heartbeat"`
	SinceHeartbeat time.Duration `json:"since_heartbeat"`
	TimeToSuspect  time.Duration `json:"time_to_suspect"`
	TimeToDead     time.Duration `json:"time_to_dead"`
}

// GetNodeHealth returns detailed health info for a node.
func (fd *FailureDetector) GetNodeHealth(nodeID NodeID) (*NodeHealthReport, error) {
	node := fd.membership.GetNode(nodeID)
	if node == nil {
		return nil, fmt.Errorf("node %s not found", nodeID)
	}

	fd.mu.RLock()
	lastHB, ok := fd.lastHeartbeats[nodeID]
	fd.mu.RUnlock()

	if !ok {
		lastHB = time.Time{}
	}

	now := time.Now()
	sinceHB := now.Sub(lastHB)

	return &NodeHealthReport{
		NodeID:         nodeID,
		Status:         node.Status,
		LastHeartbeat:  lastHB,
		SinceHeartbeat: sinceHB,
		TimeToSuspect:  fd.config.SuspectTimeout - sinceHB,
		TimeToDead:     fd.config.DeadTimeout - sinceHB,
	}, nil
}

// String returns a string representation.
func (fd *FailureDetector) String() string {
	stats := fd.Stats()
	return fmt.Sprintf("FailureDetector{alive=%d, suspect=%d, dead=%d}",
		stats.AliveCount, stats.SuspectCount, stats.DeadCount)
}
