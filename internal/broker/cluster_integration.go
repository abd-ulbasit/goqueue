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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
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

// GetLeaderClientAddress returns the client-facing address of the partition leader.
//
// WHY CLIENT ADDRESS (not cluster address)?
//
//	The client address is where producers/consumers connect. When we forward
//	a publish request, we're acting as a proxy for the producer, so we need
//	to hit the same HTTP API port they would use.
//
// RETURNS:
//   - Empty string if leader not found or is self
//   - Format: "host:port" (e.g., "goqueue-0.goqueue.svc:8080")
func (cc *clusterCoordinator) GetLeaderClientAddress(topic string, partition int) string {
	leaderID := cc.GetLeader(topic, partition)
	if leaderID == cc.coordinator.Node().ID() {
		return "" // We are the leader
	}

	nodeInfo := cc.coordinator.Membership().GetNode(leaderID)
	if nodeInfo == nil {
		return ""
	}
	return nodeInfo.ClientAddress.String()
}

// GetReplicas returns the replica node IDs for a partition.
func (cc *clusterCoordinator) GetReplicas(topic string, partition int) []cluster.NodeID {
	assign := cc.coordinator.MetadataStore().GetAssignment(topic, partition)
	if assign == nil {
		return []cluster.NodeID{cc.coordinator.Node().ID()} // Self is only replica
	}
	return assign.Replicas
}

// GetPartitionInfo returns detailed partition assignment info for API responses.
func (cc *clusterCoordinator) GetPartitionInfo(topic string, partition int) *PartitionInfo {
	assign := cc.coordinator.MetadataStore().GetAssignment(topic, partition)
	if assign == nil {
		// No assignment - single node mode
		return &PartitionInfo{
			Topic:     topic,
			Partition: partition,
			Leader:    string(cc.coordinator.Node().ID()),
			Replicas:  []string{string(cc.coordinator.Node().ID())},
			ISR:       []string{string(cc.coordinator.Node().ID())},
			Version:   0,
		}
	}

	replicas := make([]string, len(assign.Replicas))
	for i, r := range assign.Replicas {
		replicas[i] = string(r)
	}

	isr := make([]string, len(assign.ISR))
	for i, r := range assign.ISR {
		isr[i] = string(r)
	}

	return &PartitionInfo{
		Topic:     assign.Topic,
		Partition: assign.Partition,
		Leader:    string(assign.Leader),
		Replicas:  replicas,
		ISR:       isr,
		Version:   assign.Version,
	}
}

// GetTopicPartitions returns partition info for all partitions of a topic.
func (cc *clusterCoordinator) GetTopicPartitions(topic string) []*PartitionInfo {
	assignments := cc.coordinator.MetadataStore().GetAssignmentsForTopic(topic)
	if len(assignments) == 0 {
		// Check if topic exists in broker
		return nil
	}

	infos := make([]*PartitionInfo, 0, len(assignments))
	for _, assign := range assignments {
		replicas := make([]string, len(assign.Replicas))
		for i, r := range assign.Replicas {
			replicas[i] = string(r)
		}

		isr := make([]string, len(assign.ISR))
		for i, r := range assign.ISR {
			isr[i] = string(r)
		}

		infos = append(infos, &PartitionInfo{
			Topic:     assign.Topic,
			Partition: assign.Partition,
			Leader:    string(assign.Leader),
			Replicas:  replicas,
			ISR:       isr,
			Version:   assign.Version,
		})
	}

	return infos
}

// PartitionInfo represents partition assignment info for API responses.
type PartitionInfo struct {
	Topic     string   `json:"topic"`
	Partition int      `json:"partition"`
	Leader    string   `json:"leader"`
	Replicas  []string `json:"replicas"`
	ISR       []string `json:"isr"`
	Version   int64    `json:"version"`
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

	// =========================================================================
	// IMMEDIATE METADATA SYNC
	// =========================================================================
	//
	// WHY: Background sync runs every 9s. Clients expect topic to be usable
	// immediately after creation. Push metadata NOW so all nodes know.
	//
	// FLOW:
	//   1. Topic created on controller
	//   2. Assignments stored locally
	//   3. SyncMetadataNow() pushes to all followers
	//   4. Followers update their local metadata store
	//   5. Any node can now handle requests for this topic
	//
	// =========================================================================
	cc.logger.Info("syncing new topic metadata to followers",
		"topic", name,
		"partitions", partitions,
		"replication_factor", replicationFactor)
	cc.coordinator.SyncMetadataNow()

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
//
// WHY THIS MATTERS:
//
//	When the controller creates a topic, it syncs metadata to followers.
//	Followers must create the topic locally so they can:
//	1. Accept replication requests from leaders
//	2. Serve as leader if elected for a partition
//	3. Respond to fetch requests from other followers
//
// FLOW:
//
//	┌────────────────┐    sync     ┌────────────────┐
//	│   Controller   │────────────►│    Follower    │
//	│ CreateTopic()  │             │handleMetadata()│
//	└────────────────┘             └───────┬────────┘
//	                                       │
//	                                       ▼
//	                            ┌──────────────────┐
//	                            │ ensureLocalTopic │
//	                            │ for each topic   │
//	                            └──────────────────┘
//
// WHAT GETS SYNCED:
//   - Topic name and partition count
//   - Partition assignments (leader, replicas, ISR)
//   - Topic configuration (retention, etc.) - future
func (cc *clusterCoordinator) handleMetadataChange(meta *cluster.ClusterMeta) {
	cc.logger.Debug("metadata changed",
		"version", meta.Version,
		"topics", len(meta.Topics))

	// Ensure all topics in cluster metadata exist locally.
	// This is critical for replication - followers must have topics
	// to accept fetch requests and store replicated messages.
	for topicName, topicMeta := range meta.Topics {
		if err := cc.ensureLocalTopic(topicName, topicMeta.PartitionCount); err != nil {
			cc.logger.Error("failed to create local topic from metadata",
				"topic", topicName,
				"partitions", topicMeta.PartitionCount,
				"error", err)
		}
	}
}

// ensureLocalTopic creates a topic locally if it doesn't exist.
//
// WHY NOT JUST CALL CreateTopic?
//
//	CreateTopic would trigger another CreateTopicMeta call, creating
//	a feedback loop. We need to create the topic locally WITHOUT
//	registering it with cluster metadata again.
//
// IDEMPOTENT:
//
//	Safe to call multiple times - skips if topic already exists.
func (cc *clusterCoordinator) ensureLocalTopic(name string, partitions int) error {
	// Check if topic already exists (fast path)
	if cc.broker.TopicExists(name) {
		return nil
	}

	cc.logger.Info("creating local topic from cluster metadata",
		"topic", name,
		"partitions", partitions)

	// Create topic locally without triggering cluster metadata registration.
	// This is a direct local creation for replication purposes.
	return cc.broker.CreateTopicLocal(TopicConfig{
		Name:           name,
		NumPartitions:  partitions,
		RetentionHours: 168, // 7 days default
	})
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
// REQUEST FORWARDING
// =============================================================================
//
// WHY FORWARD REQUESTS?
//   In a partitioned queue system, each partition has exactly ONE leader.
//   Writes must go to the leader to maintain ordering and consistency.
//
// FLOW:
//   ┌──────────┐  publish   ┌───────────────┐  not leader  ┌───────────────┐
//   │ Producer │───────────►│ goqueue-1     │─────────────►│ goqueue-0     │
//   └──────────┘            │ (any node)    │              │ (leader)      │
//        ▲                  └───────────────┘              └───────┬───────┘
//        │                                                         │
//        │◄────────────────────────────────────────────────────────┘
//                            forwarded response
//
// COMPARISON:
//   - Kafka: Clients get metadata and connect directly to leaders
//   - RabbitMQ: Queue mirrors have one master, requests forwarded
//   - Redis Cluster: MOVED error tells client where to go
//   - goqueue: Transparent forwarding (client doesn't need to know)
//
// TRANSPARENCY:
//   The producer doesn't know their request was forwarded. This simplifies
//   client implementation but adds one network hop for non-leader requests.
//
// =============================================================================

// ForwardPublishRequest is the request format for forwarded publishes.
type ForwardPublishRequest struct {
	Key       []byte `json:"key,omitempty"`
	Value     []byte `json:"value"`
	Partition int    `json:"partition"`
}

// ForwardPublishResponse is the response from a forwarded publish.
type ForwardPublishResponse struct {
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
	Error     string `json:"error,omitempty"`
}

// ForwardPublish forwards a publish request to the partition leader.
//
// WHY THIS EXISTS:
//
//	When a producer publishes to a partition we don't lead, we forward
//	the request to the actual leader rather than failing.
//
// PARAMETERS:
//   - ctx: Context for timeout/cancellation
//   - leaderAddr: The leader's client address (host:port)
//   - topic: Topic name
//   - partition: Target partition number
//   - key: Message routing key
//   - value: Message payload
//
// RETURNS:
//   - offset: The offset assigned by the leader
//   - error: If forwarding fails or leader returns error
func (cc *clusterCoordinator) ForwardPublish(ctx context.Context, leaderAddr string, topic string, partition int, key, value []byte) (int64, error) {
	// Build request
	reqBody := ForwardPublishRequest{
		Key:       key,
		Value:     value,
		Partition: partition,
	}

	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return 0, fmt.Errorf("marshal request: %w", err)
	}

	// Build URL: POST /topics/{topic}/messages/forward
	url := fmt.Sprintf("http://%s/topics/%s/messages/forward", leaderAddr, topic)

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(bodyBytes))
	if err != nil {
		return 0, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	// Execute request
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("forward request: %w", err)
	}
	defer resp.Body.Close()

	// Read response
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("leader returned %d: %s", resp.StatusCode, string(respBody))
	}

	// Parse response
	var result ForwardPublishResponse
	if err := json.Unmarshal(respBody, &result); err != nil {
		return 0, fmt.Errorf("parse response: %w", err)
	}

	if result.Error != "" {
		return 0, fmt.Errorf("leader error: %s", result.Error)
	}

	cc.logger.Debug("forwarded publish to leader",
		"topic", topic,
		"partition", partition,
		"leader", leaderAddr,
		"offset", result.Offset)

	return result.Offset, nil
}

// =============================================================================
// STATISTICS
// =============================================================================

// Stats returns cluster statistics.
func (cc *clusterCoordinator) Stats() cluster.CoordinatorStats {
	return cc.coordinator.Stats()
}
