// =============================================================================
// CLUSTER COORDINATOR - BOOTSTRAP & LIFECYCLE ORCHESTRATION
// =============================================================================
//
// WHAT: The coordinator is the "main function" for cluster operations.
// It orchestrates startup, shutdown, and ongoing cluster activities.
//
// RESPONSIBILITIES:
//   1. Bootstrap: Start components in correct order
//   2. Discovery: Find and connect to existing cluster
//   3. Election: Trigger controller election when needed
//   4. Heartbeating: Send periodic heartbeats
//   5. Shutdown: Graceful leave and cleanup
//
// BOOTSTRAP SEQUENCE:
//   ┌─────────────────────────────────────────────────────────────────┐
//   │ 1. Load persisted state (if any)                                │
//   │ 2. Register self in membership                                  │
//   │ 3. Try to contact configured peers                              │
//   │ 4. If peers found → sync state, join cluster                    │
//   │ 5. If no peers → wait for quorum, start election                │
//   │ 6. Start failure detector                                       │
//   │ 7. Start heartbeat sender                                       │
//   │ 8. Ready to serve                                               │
//   └─────────────────────────────────────────────────────────────────┘
//
// COMPARISON:
//   - Kafka: Complex bootstrap with ZK/KRaft coordination
//   - Cassandra: Gossip-based (seed nodes, ring formation)
//   - etcd: Raft bootstrap (initial cluster config)
//   - goqueue: Static peers, simple election (M10)
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
// COORDINATOR
// =============================================================================

// Coordinator orchestrates cluster lifecycle.
type Coordinator struct {
	// config is the cluster configuration
	config *ClusterConfig

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

	// client makes requests to other nodes
	client *ClusterClient

	// server handles incoming cluster requests
	server *ClusterServer

	// logger for coordinator operations
	logger *slog.Logger

	// mu protects state
	mu sync.RWMutex

	// running indicates if coordinator is active
	running bool

	// ctx is the coordinator's context
	ctx context.Context

	// cancel cancels the coordinator's context
	cancel context.CancelFunc

	// wg waits for background goroutines
	wg sync.WaitGroup

	// readyCh is closed when coordinator is ready
	readyCh chan struct{}

	// eventListeners receive coordinator events
	eventListeners []func(CoordinatorEvent)
}

// CoordinatorEvent represents coordinator lifecycle events.
type CoordinatorEvent struct {
	Type      CoordinatorEventType
	Timestamp time.Time
	Details   string
}

// CoordinatorEventType enumerates coordinator events.
type CoordinatorEventType string

const (
	EventBootstrapStarted  CoordinatorEventType = "bootstrap_started"
	EventBootstrapComplete CoordinatorEventType = "bootstrap_complete"
	EventBootstrapFailed   CoordinatorEventType = "bootstrap_failed"
	EventJoinedCluster     CoordinatorEventType = "joined_cluster"
	EventLeftCluster       CoordinatorEventType = "left_cluster"
	EventBecameController  CoordinatorEventType = "became_controller"
	EventLostController    CoordinatorEventType = "lost_controller"
	EventQuorumLost        CoordinatorEventType = "quorum_lost"
	EventQuorumRestored    CoordinatorEventType = "quorum_restored"
)

// NewCoordinator creates a new cluster coordinator.
func NewCoordinator(config *ClusterConfig, dataDir string, logger *slog.Logger) (*Coordinator, error) {
	// Apply defaults
	config = config.WithDefaults()

	// Validate config
	if err := validateConfig(config); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	// Create node
	node, err := NewNode(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create node: %w", err)
	}

	// Create membership manager
	membership := NewMembership(node, config, dataDir)

	// Create failure detector
	failureDetector := NewFailureDetector(membership, config)

	// Create controller elector
	elector := NewControllerElector(node, membership, config)

	// Create metadata store
	metadataStore := NewMetadataStore(dataDir)

	// Create client
	client := NewClusterClient(node, membership, logger)

	// Create server
	server := NewClusterServer(node, membership, failureDetector, elector, metadataStore, logger)

	// Wire elector's request sender
	elector.SetRequestVoteFunc(func(nodeID NodeID, req *ControllerVoteRequest) (*ControllerVoteResponse, error) {
		return client.RequestVote(context.Background(), nodeID, req.Epoch)
	})

	c := &Coordinator{
		config:          config,
		node:            node,
		membership:      membership,
		failureDetector: failureDetector,
		elector:         elector,
		metadataStore:   metadataStore,
		client:          client,
		server:          server,
		logger:          logger.With("component", "coordinator"),
		readyCh:         make(chan struct{}),
	}

	// Register membership event listener
	membership.AddListener(c.handleMembershipEvent)

	return c, nil
}

// validateConfig validates cluster configuration.
func validateConfig(config *ClusterConfig) error {
	if config.NodeID == "" {
		// Will use hostname, but verify we can get it
		// Already handled in NewNode
	}

	if len(config.Peers) == 0 && config.QuorumSize > 1 {
		return fmt.Errorf("quorum size %d requires peers to be configured", config.QuorumSize)
	}

	if config.QuorumSize < 1 {
		return fmt.Errorf("quorum size must be at least 1")
	}

	return nil
}

// =============================================================================
// ACCESSORS
// =============================================================================

// Node returns the local node.
func (c *Coordinator) Node() *Node {
	return c.node
}

// Membership returns the membership manager.
func (c *Coordinator) Membership() *Membership {
	return c.membership
}

// MetadataStore returns the metadata store.
func (c *Coordinator) MetadataStore() *MetadataStore {
	return c.metadataStore
}

// Server returns the cluster server.
func (c *Coordinator) Server() *ClusterServer {
	return c.server
}

// IsController returns true if this node is the controller.
func (c *Coordinator) IsController() bool {
	return c.elector.IsController()
}

// Ready returns a channel that's closed when coordinator is ready.
func (c *Coordinator) Ready() <-chan struct{} {
	return c.readyCh
}

// =============================================================================
// LIFECYCLE
// =============================================================================

// Start begins cluster operations.
func (c *Coordinator) Start(ctx context.Context) error {
	c.mu.Lock()
	if c.running {
		c.mu.Unlock()
		return fmt.Errorf("coordinator already running")
	}
	c.running = true
	c.ctx, c.cancel = context.WithCancel(ctx)
	c.mu.Unlock()

	c.logger.Info("starting cluster coordinator",
		"node_id", c.node.ID(),
		"peers", len(c.config.Peers))

	c.emitEvent(EventBootstrapStarted, "")

	// Bootstrap sequence
	if err := c.bootstrap(); err != nil {
		c.emitEvent(EventBootstrapFailed, err.Error())
		return fmt.Errorf("bootstrap failed: %w", err)
	}

	// Start background tasks
	c.startBackgroundTasks()

	// Mark ready
	close(c.readyCh)
	c.emitEvent(EventBootstrapComplete, "")

	c.logger.Info("cluster coordinator ready",
		"node_id", c.node.ID(),
		"is_controller", c.elector.IsController())

	return nil
}

// Stop gracefully shuts down the coordinator.
func (c *Coordinator) Stop(ctx context.Context) error {
	c.mu.Lock()
	if !c.running {
		c.mu.Unlock()
		return nil
	}
	c.running = false
	c.mu.Unlock()

	c.logger.Info("stopping cluster coordinator")

	// Request graceful leave
	if err := c.leaveCluster(ctx); err != nil {
		c.logger.Warn("failed to leave cluster gracefully",
			"error", err)
	}

	// Cancel context to stop background tasks
	c.cancel()

	// Stop components
	c.failureDetector.Stop()
	c.elector.Stop()

	// Wait for background tasks
	c.wg.Wait()

	c.emitEvent(EventLeftCluster, "")
	c.logger.Info("cluster coordinator stopped")

	return nil
}

// =============================================================================
// BOOTSTRAP
// =============================================================================
//
// BOOTSTRAP FLOW:
//
//   ┌───────────────────┐
//   │ Load Local State  │
//   └─────────┬─────────┘
//             │
//             ▼
//   ┌───────────────────┐
//   │ Register Self     │
//   └─────────┬─────────┘
//             │
//             ▼
//   ┌───────────────────┐     Yes    ┌───────────────────┐
//   │ Peers Configured? │───────────►│ Try Join Cluster  │
//   └─────────┬─────────┘            └─────────┬─────────┘
//             │ No                             │
//             ▼                                │ Success
//   ┌───────────────────┐                      │
//   │ Wait for Quorum   │◄─────────────────────┘
//   └─────────┬─────────┘                      │ Failed
//             │                                │
//             ▼                                ▼
//   ┌───────────────────┐            ┌───────────────────┐
//   │ Start Election    │            │ Become Standalone │
//   └───────────────────┘            └───────────────────┘
//
// =============================================================================

func (c *Coordinator) bootstrap() error {
	// Step 1: Load persisted state
	if err := c.loadState(); err != nil {
		c.logger.Warn("failed to load persisted state, starting fresh",
			"error", err)
	}

	// Step 2: Register self in membership
	if err := c.membership.RegisterSelf(); err != nil {
		return fmt.Errorf("failed to register self: %w", err)
	}

	// Step 3: Try to join existing cluster via peers
	if len(c.config.Peers) > 0 {
		if err := c.joinViaDiscovery(); err != nil {
			c.logger.Warn("failed to join existing cluster, will wait for quorum",
				"error", err)
		}
	}

	// Step 4: Wait for quorum (if needed)
	if c.membership.AliveCount() < c.config.QuorumSize {
		c.logger.Info("waiting for quorum",
			"current", c.membership.AliveCount(),
			"needed", c.config.QuorumSize)

		if err := c.waitForQuorum(); err != nil {
			// If we can't get quorum and we're the only node, proceed anyway
			if c.config.QuorumSize == 1 {
				c.logger.Warn("quorum timeout, proceeding as single node")
			} else {
				return fmt.Errorf("failed to reach quorum: %w", err)
			}
		}
	}

	// Step 5: Start election if no controller
	if c.membership.ControllerID() == "" {
		c.logger.Info("no controller, starting election")
		c.elector.Start()
	} else {
		c.logger.Info("controller exists",
			"controller", c.membership.ControllerID())
		c.elector.Start()
	}

	return nil
}

// loadState loads persisted state from disk.
func (c *Coordinator) loadState() error {
	// Load cluster state
	if err := c.membership.LoadState(); err != nil {
		return fmt.Errorf("failed to load membership state: %w", err)
	}

	// Load metadata
	if err := c.metadataStore.Load(); err != nil {
		return fmt.Errorf("failed to load metadata: %w", err)
	}

	return nil
}

// joinViaDiscovery tries to join the cluster through configured peers.
func (c *Coordinator) joinViaDiscovery() error {
	ctx, cancel := context.WithTimeout(c.ctx, 10*time.Second)
	defer cancel()

	c.logger.Info("attempting to join cluster via peers",
		"peers", c.config.Peers)

	for _, peer := range c.config.Peers {
		// Skip self
		if peer == c.node.Info().ClusterAddress.String() {
			continue
		}

		c.logger.Debug("trying peer",
			"peer", peer)

		resp, err := c.client.RequestJoin(ctx, peer)
		if err != nil {
			c.logger.Debug("peer unavailable",
				"peer", peer,
				"error", err)
			continue
		}

		// Handle redirect
		if resp.RedirectRequired {
			c.logger.Debug("redirected to controller",
				"controller", resp.ControllerAddr)

			resp, err = c.client.RequestJoin(ctx, resp.ControllerAddr)
			if err != nil {
				c.logger.Debug("controller unavailable",
					"controller", resp.ControllerAddr,
					"error", err)
				continue
			}
		}

		if !resp.Success {
			c.logger.Debug("join rejected",
				"peer", peer,
				"error", resp.Error)
			continue
		}

		// Success! Apply cluster state
		if resp.ClusterState != nil {
			if err := c.membership.ApplyState(resp.ClusterState); err != nil {
				c.logger.Warn("failed to apply cluster state",
					"error", err)
			}
		}

		c.logger.Info("joined cluster",
			"via", peer,
			"controller", resp.ControllerID,
			"cluster_size", len(resp.ClusterState.Nodes))

		c.emitEvent(EventJoinedCluster, fmt.Sprintf("via %s", peer))
		return nil
	}

	return fmt.Errorf("no peers available")
}

// waitForQuorum waits until we have enough nodes for quorum.
func (c *Coordinator) waitForQuorum() error {
	ctx, cancel := context.WithTimeout(c.ctx, c.config.BootstrapTimeout)
	defer cancel()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			count := c.membership.AliveCount()
			if count >= c.config.QuorumSize {
				c.logger.Info("quorum reached",
					"count", count,
					"needed", c.config.QuorumSize)
				return nil
			}
			c.logger.Debug("waiting for quorum",
				"current", count,
				"needed", c.config.QuorumSize)
		}
	}
}

// leaveCluster gracefully leaves the cluster.
func (c *Coordinator) leaveCluster(ctx context.Context) error {
	// Update our status to leaving
	c.node.SetStatus(NodeStatusLeaving)

	// If we're controller, should trigger re-election
	// (In M11, we'll do proper handoff)
	if c.elector.IsController() {
		c.logger.Info("stepping down as controller before leaving")
		c.elector.Stop()
	}

	// Notify cluster
	_, err := c.client.RequestLeave(ctx)
	return err
}

// =============================================================================
// BACKGROUND TASKS
// =============================================================================

func (c *Coordinator) startBackgroundTasks() {
	// Start failure detector
	c.failureDetector.Start()

	// Start heartbeat sender
	c.wg.Add(1)
	go c.heartbeatLoop()

	// Start state sync loop (controller pushes state to followers)
	c.wg.Add(1)
	go c.stateSyncLoop()
}

// heartbeatLoop periodically sends heartbeats to all nodes.
func (c *Coordinator) heartbeatLoop() {
	defer c.wg.Done()

	ticker := time.NewTicker(c.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.client.BroadcastHeartbeats(c.ctx)
		}
	}
}

// stateSyncLoop handles state synchronization.
// Controller: pushes metadata to followers
// Follower: (future) could pull from controller periodically
func (c *Coordinator) stateSyncLoop() {
	defer c.wg.Done()

	// Sync less frequently than heartbeats
	ticker := time.NewTicker(c.config.HeartbeatInterval * 3)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			if c.elector.IsController() {
				c.syncMetadataToFollowers()
			}
		}
	}
}

// syncMetadataToFollowers pushes current metadata to all followers.
func (c *Coordinator) syncMetadataToFollowers() {
	meta := c.metadataStore.Meta()
	nodes := c.membership.AliveNodes()
	myID := c.node.ID()

	for _, node := range nodes {
		if node.ID == myID {
			continue
		}

		go func(nodeID NodeID) {
			ctx, cancel := context.WithTimeout(c.ctx, 5*time.Second)
			defer cancel()

			if err := c.client.PushMetadata(ctx, nodeID, meta); err != nil {
				c.logger.Debug("failed to sync metadata",
					"to", nodeID,
					"error", err)
			}
		}(node.ID)
	}
}

// =============================================================================
// EVENT HANDLING
// =============================================================================

// handleMembershipEvent handles membership change events.
func (c *Coordinator) handleMembershipEvent(event MembershipEvent) {
	c.logger.Debug("membership event",
		"type", event.Type,
		"node", event.NodeID)

	switch event.Type {
	case EventNodeDied:
		// If controller died, trigger election
		if event.NodeID == c.membership.ControllerID() {
			c.logger.Info("controller died, triggering election")
			c.elector.TriggerElection()
		}

	case EventControllerChanged:
		if event.NodeID == c.node.ID() {
			c.emitEvent(EventBecameController, "")
		} else {
			// We lost controller role
			if c.elector.IsController() == false && c.node.Role() == NodeRoleController {
				c.emitEvent(EventLostController, "")
			}
		}

	case EventNodeJoined:
		// New node joined - if we're controller, sync metadata
		if c.elector.IsController() {
			go func() {
				ctx, cancel := context.WithTimeout(c.ctx, 5*time.Second)
				defer cancel()
				meta := c.metadataStore.Meta()
				if err := c.client.PushMetadata(ctx, event.NodeID, meta); err != nil {
					c.logger.Warn("failed to sync metadata to new node",
						"node", event.NodeID,
						"error", err)
				}
			}()
		}
	}

	// Check quorum
	c.checkQuorum()
}

// checkQuorum checks if we still have quorum.
func (c *Coordinator) checkQuorum() {
	aliveCount := c.membership.AliveCount()
	hasQuorum := aliveCount >= c.config.QuorumSize

	// NOTE: Could track quorum state changes with a field here
	// For now, just emit event when quorum is lost

	if !hasQuorum {
		c.logger.Warn("quorum lost",
			"alive", aliveCount,
			"needed", c.config.QuorumSize)
		c.emitEvent(EventQuorumLost, fmt.Sprintf("alive=%d, needed=%d", aliveCount, c.config.QuorumSize))
	}
}

// AddEventListener adds a listener for coordinator events.
func (c *Coordinator) AddEventListener(listener func(CoordinatorEvent)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.eventListeners = append(c.eventListeners, listener)
}

// emitEvent fires a coordinator event.
func (c *Coordinator) emitEvent(eventType CoordinatorEventType, details string) {
	event := CoordinatorEvent{
		Type:      eventType,
		Timestamp: time.Now(),
		Details:   details,
	}

	c.mu.RLock()
	listeners := c.eventListeners
	c.mu.RUnlock()

	for _, listener := range listeners {
		go listener(event)
	}
}

// =============================================================================
// STATISTICS
// =============================================================================

// CoordinatorStats contains coordinator statistics.
type CoordinatorStats struct {
	NodeID          NodeID               `json:"node_id"`
	Status          NodeStatus           `json:"status"`
	Role            NodeRole             `json:"role"`
	IsController    bool                 `json:"is_controller"`
	ControllerID    NodeID               `json:"controller_id"`
	ControllerEpoch int64                `json:"controller_epoch"`
	ClusterSize     int                  `json:"cluster_size"`
	AliveNodes      int                  `json:"alive_nodes"`
	HasQuorum       bool                 `json:"has_quorum"`
	QuorumSize      int                  `json:"quorum_size"`
	MetadataVersion int64                `json:"metadata_version"`
	Uptime          time.Duration        `json:"uptime"`
	FailureDetector FailureDetectorStats `json:"failure_detector"`
}

// Stats returns coordinator statistics.
func (c *Coordinator) Stats() CoordinatorStats {
	state := c.membership.State()
	fdStats := c.failureDetector.Stats()

	return CoordinatorStats{
		NodeID:          c.node.ID(),
		Status:          c.node.Status(),
		Role:            c.node.Role(),
		IsController:    c.elector.IsController(),
		ControllerID:    state.ControllerID,
		ControllerEpoch: state.ControllerEpoch,
		ClusterSize:     len(state.Nodes),
		AliveNodes:      fdStats.AliveCount,
		HasQuorum:       fdStats.AliveCount >= c.config.QuorumSize,
		QuorumSize:      c.config.QuorumSize,
		MetadataVersion: c.metadataStore.Version(),
		FailureDetector: fdStats,
	}
}
