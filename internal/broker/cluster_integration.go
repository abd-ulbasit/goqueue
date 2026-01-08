// =============================================================================
// CLUSTER INTEGRATION - BROKER CLUSTER MODE SUPPORT
// =============================================================================
//
// WHAT: Integrates the cluster package with the broker.
//
// WHY WRAPPER?
//   - Decouples broker from cluster internals
//   - Provides broker-specific helpers (IsLeaderFor, RouteRequest)
//   - Enables single-node mode (wrapper is nil)
//
// CLUSTER MODE FEATURES:
//   - Partition leadership: Which node handles which partition
//   - Request routing: Forward requests to correct node
//   - Metadata sync: Keep topic configs in sync
//   - Controller ops: Create topics, reassign partitions (controller only)
//
// =============================================================================

package broker

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"goqueue/internal/cluster"
)

// =============================================================================
// CLUSTER COORDINATOR WRAPPER
// =============================================================================

// clusterCoordinator wraps the cluster.Coordinator for broker integration.
// Provides broker-specific operations on top of raw cluster functionality.
type clusterCoordinator struct {
	// coordinator is the underlying cluster coordinator
	coordinator *cluster.Coordinator

	// broker is a reference back to the broker
	broker *Broker

	// logger for cluster operations
	logger *slog.Logger
}

// newClusterCoordinator creates a cluster coordinator for the broker.
func newClusterCoordinator(broker *Broker, config *ClusterModeConfig, logger *slog.Logger) (*clusterCoordinator, error) {
	if config == nil {
		return nil, fmt.Errorf("cluster config is required")
	}

	// Build cluster config from broker config
	clusterConfig := &cluster.ClusterConfig{
		NodeID:             broker.config.NodeID,
		ClientAddress:      config.ClientAddress,
		ClusterAddress:     config.ClusterAddress,
		AdvertiseAddress:   config.AdvertiseAddress,
		Peers:              config.Peers,
		QuorumSize:         config.QuorumSize,
		HeartbeatInterval:  3 * time.Second, // Kafka default
		SuspectTimeout:     6 * time.Second, // 2 heartbeats
		DeadTimeout:        9 * time.Second, // 3 heartbeats
		BootstrapTimeout:   60 * time.Second,
		LeaseTimeout:       15 * time.Second,
		LeaseRenewInterval: 5 * time.Second,
		Version:            "0.10.0",
	}

	// Create underlying coordinator
	coordinator, err := cluster.NewCoordinator(clusterConfig, broker.config.DataDir, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create cluster coordinator: %w", err)
	}

	cc := &clusterCoordinator{
		coordinator: coordinator,
		broker:      broker,
		logger:      logger.With("component", "cluster-broker"),
	}

	// Register for metadata changes
	coordinator.MetadataStore().AddListener(cc.handleMetadataChange)

	// Register for coordinator events
	coordinator.AddEventListener(cc.handleCoordinatorEvent)

	return cc, nil
}

// =============================================================================
// LIFECYCLE
// =============================================================================

// Start begins cluster operations.
func (cc *clusterCoordinator) Start(ctx context.Context) error {
	cc.logger.Info("starting cluster mode")

	if err := cc.coordinator.Start(ctx); err != nil {
		return fmt.Errorf("failed to start coordinator: %w", err)
	}

	// Wait for ready
	select {
	case <-cc.coordinator.Ready():
		cc.logger.Info("cluster coordinator ready",
			"node_id", cc.coordinator.Node().ID(),
			"is_controller", cc.coordinator.IsController())
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(30 * time.Second):
		return fmt.Errorf("cluster coordinator not ready after 30s")
	}

	return nil
}

// Stop gracefully shuts down cluster operations.
func (cc *clusterCoordinator) Stop(ctx context.Context) error {
	cc.logger.Info("stopping cluster mode")
	return cc.coordinator.Stop(ctx)
}

// =============================================================================
// CLUSTER STATE ACCESS
// =============================================================================

// NodeID returns this node's ID.
func (cc *clusterCoordinator) NodeID() cluster.NodeID {
	return cc.coordinator.Node().ID()
}

// IsController returns true if this node is the cluster controller.
func (cc *clusterCoordinator) IsController() bool {
	return cc.coordinator.IsController()
}

// ControllerID returns the current controller's ID.
func (cc *clusterCoordinator) ControllerID() cluster.NodeID {
	return cc.coordinator.Membership().ControllerID()
}

// ClusterSize returns the number of nodes in the cluster.
func (cc *clusterCoordinator) ClusterSize() int {
	return cc.coordinator.Membership().NodeCount()
}

// =============================================================================
// PARTITION LEADERSHIP
// =============================================================================

// IsLeaderFor returns true if this node is the leader for the given partition.
// In single-node mode (cc == nil), always returns true.
func (cc *clusterCoordinator) IsLeaderFor(topic string, partition int) bool {
	assign := cc.coordinator.MetadataStore().GetAssignment(topic, partition)
	if assign == nil {
		// No assignment = we're the leader (single-node or new topic)
		return true
	}
	return assign.Leader == cc.coordinator.Node().ID()
}

// GetLeader returns the node ID of the partition leader.
func (cc *clusterCoordinator) GetLeader(topic string, partition int) cluster.NodeID {
	assign := cc.coordinator.MetadataStore().GetAssignment(topic, partition)
	if assign == nil {
		return cc.coordinator.Node().ID() // Self is leader if no assignment
	}
	return assign.Leader
}

// GetReplicas returns the replica node IDs for a partition.
func (cc *clusterCoordinator) GetReplicas(topic string, partition int) []cluster.NodeID {
	assign := cc.coordinator.MetadataStore().GetAssignment(topic, partition)
	if assign == nil {
		return []cluster.NodeID{cc.coordinator.Node().ID()} // Self is only replica
	}
	return assign.Replicas
}

// =============================================================================
// TOPIC METADATA (CONTROLLER ONLY)
// =============================================================================

// CreateTopicMeta registers a topic in cluster metadata.
// Only the controller should call this.
func (cc *clusterCoordinator) CreateTopicMeta(name string, partitions int, replicationFactor int) error {
	if !cc.IsController() {
		return fmt.Errorf("not controller, cannot create topic metadata")
	}

	config := cluster.DefaultTopicConfig()
	if err := cc.coordinator.MetadataStore().CreateTopic(name, partitions, replicationFactor, config); err != nil {
		return err
	}

	// Create partition assignments
	// For M10, use simple round-robin assignment
	nodes := cc.coordinator.Membership().AliveNodes()
	if len(nodes) == 0 {
		return fmt.Errorf("no alive nodes for partition assignment")
	}

	for p := 0; p < partitions; p++ {
		// Simple round-robin: partition p goes to node p % len(nodes)
		leaderIdx := p % len(nodes)
		leader := nodes[leaderIdx].ID

		// Build replica list (up to replicationFactor)
		replicas := make([]cluster.NodeID, 0, replicationFactor)
		for i := 0; i < replicationFactor && i < len(nodes); i++ {
			replicaIdx := (leaderIdx + i) % len(nodes)
			replicas = append(replicas, nodes[replicaIdx].ID)
		}

		assign := &cluster.PartitionAssignment{
			Topic:     name,
			Partition: p,
			Leader:    leader,
			Replicas:  replicas,
			ISR:       replicas, // Initially all replicas are in-sync
			Version:   1,
		}

		if err := cc.coordinator.MetadataStore().SetAssignment(assign); err != nil {
			cc.logger.Error("failed to set partition assignment",
				"topic", name,
				"partition", p,
				"error", err)
		}
	}

	return nil
}

// DeleteTopicMeta removes a topic from cluster metadata.
// Only the controller should call this.
func (cc *clusterCoordinator) DeleteTopicMeta(name string) error {
	if !cc.IsController() {
		return fmt.Errorf("not controller, cannot delete topic metadata")
	}
	return cc.coordinator.MetadataStore().DeleteTopic(name)
}

// =============================================================================
// HTTP HANDLERS
// =============================================================================

// RegisterRoutes registers cluster HTTP endpoints.
// Should be called after creating the HTTP server.
func (cc *clusterCoordinator) RegisterRoutes(mux *http.ServeMux) {
	cc.coordinator.Server().RegisterRoutes(mux)
}

// =============================================================================
// EVENT HANDLERS
// =============================================================================

// handleMetadataChange is called when cluster metadata changes.
func (cc *clusterCoordinator) handleMetadataChange(meta *cluster.ClusterMeta) {
	cc.logger.Debug("metadata changed",
		"version", meta.Version,
		"topics", len(meta.Topics))

	// In future: sync topic configs with local broker
	// For M10: metadata is informational only
}

// handleCoordinatorEvent is called for coordinator lifecycle events.
func (cc *clusterCoordinator) handleCoordinatorEvent(event cluster.CoordinatorEvent) {
	cc.logger.Info("coordinator event",
		"type", event.Type,
		"details", event.Details)

	switch event.Type {
	case cluster.EventBecameController:
		cc.logger.Info("this node became cluster controller")
		// Could trigger partition rebalancing here

	case cluster.EventLostController:
		cc.logger.Info("this node lost controller role")

	case cluster.EventQuorumLost:
		cc.logger.Warn("cluster lost quorum - some operations may fail")
	}
}

// =============================================================================
// STATISTICS
// =============================================================================

// Stats returns cluster statistics.
func (cc *clusterCoordinator) Stats() cluster.CoordinatorStats {
	return cc.coordinator.Stats()
}
