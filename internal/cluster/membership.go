// =============================================================================
// MEMBERSHIP - CLUSTER MEMBERSHIP MANAGEMENT
// =============================================================================
//
// WHAT: Manages the list of nodes in the cluster.
// Handles join, leave, and tracking node health states.
//
// RESPONSIBILITIES:
//   - Track all known nodes and their states
//   - Handle node registration (join requests)
//   - Handle node departure (leave requests)
//   - Update node status based on heartbeat information
//   - Maintain cluster state version for synchronization
//
// THREAD SAFETY:
//   All methods are thread-safe. The membership list can be read/written
//   concurrently by heartbeat receivers, failure detector, and API handlers.
//
// COMPARISON:
//   - Kafka: Uses ZooKeeper/KRaft for membership
//   - Cassandra: Gossiper + FailureDetector
//   - etcd: Raft-based membership
//   - goqueue: Simple in-memory with file persistence
//
// =============================================================================

package cluster

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// =============================================================================
// MEMBERSHIP EVENTS
// =============================================================================
//
// WHY EVENTS?
//   Other components need to react to membership changes:
//   - Controller: Re-assign partitions when node dies
//   - Replication: Update follower list when nodes change
//   - Load balancer: Update routing tables
//
// =============================================================================

// MembershipEventType identifies the type of membership change.
type MembershipEventType string

const (
	// EventNodeJoined fires when a new node joins the cluster.
	EventNodeJoined MembershipEventType = "node_joined"

	// EventNodeLeft fires when a node gracefully leaves.
	EventNodeLeft MembershipEventType = "node_left"

	// EventNodeDied fires when a node is declared dead.
	EventNodeDied MembershipEventType = "node_died"

	// EventNodeSuspect fires when a node becomes suspect.
	EventNodeSuspect MembershipEventType = "node_suspect"

	// EventNodeRecovered fires when a suspect/dead node comes back.
	EventNodeRecovered MembershipEventType = "node_recovered"

	// EventControllerChanged fires when controller changes.
	EventControllerChanged MembershipEventType = "controller_changed"

	// EventStateUpdated fires when cluster state is updated.
	EventStateUpdated MembershipEventType = "state_updated"
)

// MembershipEvent represents a change in cluster membership.
type MembershipEvent struct {
	// Type is the event type.
	Type MembershipEventType

	// NodeID is the affected node (if applicable).
	NodeID NodeID

	// NodeInfo is the node's info (if applicable).
	NodeInfo *NodeInfo

	// OldStatus is the previous status (for status changes).
	OldStatus NodeStatus

	// NewStatus is the new status (for status changes).
	NewStatus NodeStatus

	// Timestamp is when the event occurred.
	Timestamp time.Time
}

// MembershipListener is called when membership events occur.
type MembershipListener func(event MembershipEvent)

// =============================================================================
// MEMBERSHIP MANAGER
// =============================================================================

// Membership manages the cluster membership list.
type Membership struct {
	// mu protects all mutable state
	mu sync.RWMutex

	// localNode is this node
	localNode *Node

	// state is the current cluster state
	state *ClusterState

	// listeners receive membership events
	listeners []MembershipListener

	// dataDir is where state is persisted
	dataDir string

	// config is the cluster configuration
	// TODO: is this config needed here? if not then remove it
	config *ClusterConfig
}

// NewMembership creates a new membership manager.
func NewMembership(localNode *Node, config *ClusterConfig, dataDir string) *Membership {
	return &Membership{
		localNode: localNode,
		state:     NewClusterState(),
		listeners: make([]MembershipListener, 0),
		dataDir:   dataDir,
		config:    config,
	}
}

// =============================================================================
// CLUSTER STATE ACCESS
// =============================================================================

// State returns a snapshot of the current cluster state.
func (m *Membership) State() *ClusterState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.state.Clone()
}

// Version returns the current state version.
func (m *Membership) Version() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.state.Version
}

// ControllerID returns the current controller's ID.
func (m *Membership) ControllerID() NodeID {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.state.ControllerID
}

// ControllerEpoch returns the current controller epoch.
func (m *Membership) ControllerEpoch() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.state.ControllerEpoch
}

// IsController returns true if the local node is the controller.
func (m *Membership) IsController() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.state.ControllerID == m.localNode.ID()
}

// GetNode returns info for a specific node.
func (m *Membership) GetNode(id NodeID) *NodeInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.state.GetNode(id)
}

// AllNodes returns info for all nodes.
func (m *Membership) AllNodes() []*NodeInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	nodes := make([]*NodeInfo, 0, len(m.state.Nodes))
	for _, node := range m.state.Nodes {
		nodes = append(nodes, node.Clone())
	}
	return nodes
}

// AliveNodes returns all nodes with ALIVE status.
func (m *Membership) AliveNodes() []*NodeInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.state.AliveNodes()
}

// NodeCount returns the total number of nodes.
func (m *Membership) NodeCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.state.NodeCount()
}

// AliveCount returns the number of ALIVE nodes.
func (m *Membership) AliveCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.state.AliveNodeCount()
}

// HasQuorum returns true if we have a majority of nodes alive.
func (m *Membership) HasQuorum() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.state.HasQuorum()
}

// =============================================================================
// NODE REGISTRATION
// =============================================================================

// RegisterSelf adds the local node to the cluster.
// Called during bootstrap.
func (m *Membership) RegisterSelf() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	localInfo := m.localNode.Info()
	localInfo.Status = NodeStatusAlive
	localInfo.JoinedAt = time.Now()
	localInfo.LastHeartbeat = time.Now()

	m.state.Nodes[m.localNode.ID()] = localInfo
	m.state.Version++
	m.state.UpdatedAt = time.Now()

	// Update local node state
	m.localNode.SetStatus(NodeStatusAlive)
	m.localNode.SetJoinedAt(localInfo.JoinedAt)

	// Emit event
	m.emitEventLocked(MembershipEvent{
		Type:      EventNodeJoined,
		NodeID:    m.localNode.ID(),
		NodeInfo:  localInfo.Clone(),
		NewStatus: NodeStatusAlive,
		Timestamp: time.Now(),
	})

	return m.persistStateLocked()
}

// AddNode adds a new node to the cluster.
// Called when we receive a join request.
func (m *Membership) AddNode(info *NodeInfo) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if node already exists
	existing := m.state.Nodes[info.ID]
	if existing != nil {
		// Update existing node
		existing.Status = NodeStatusAlive
		existing.LastHeartbeat = time.Now()
		existing.ClientAddress = info.ClientAddress
		existing.ClusterAddress = info.ClusterAddress
		existing.Version = info.Version
		existing.Rack = info.Rack
		existing.Tags = info.Tags

		m.state.Version++
		m.state.UpdatedAt = time.Now()

		m.emitEventLocked(MembershipEvent{
			Type:      EventNodeRecovered,
			NodeID:    info.ID,
			NodeInfo:  existing.Clone(),
			OldStatus: existing.Status,
			NewStatus: NodeStatusAlive,
			Timestamp: time.Now(),
		})

		return m.persistStateLocked()
	}

	// Add new node
	newNode := info.Clone()
	newNode.Status = NodeStatusAlive
	newNode.JoinedAt = time.Now()
	newNode.LastHeartbeat = time.Now()

	m.state.Nodes[info.ID] = newNode
	m.state.Version++
	m.state.UpdatedAt = time.Now()

	m.emitEventLocked(MembershipEvent{
		Type:      EventNodeJoined,
		NodeID:    info.ID,
		NodeInfo:  newNode.Clone(),
		NewStatus: NodeStatusAlive,
		Timestamp: time.Now(),
	})

	return m.persistStateLocked()
}

// RemoveNode removes a node from the cluster.
// Called when a node leaves or is permanently removed.
func (m *Membership) RemoveNode(id NodeID, graceful bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	node := m.state.Nodes[id]
	if node == nil {
		return fmt.Errorf("node %s not found", id)
	}

	delete(m.state.Nodes, id)
	m.state.Version++
	m.state.UpdatedAt = time.Now()

	eventType := EventNodeDied
	if graceful {
		eventType = EventNodeLeft
	}

	m.emitEventLocked(MembershipEvent{
		Type:      eventType,
		NodeID:    id,
		NodeInfo:  node,
		OldStatus: node.Status,
		Timestamp: time.Now(),
	})

	return m.persistStateLocked()
}

// =============================================================================
// NODE STATUS UPDATES
// =============================================================================

// UpdateNodeStatus updates a node's health status.
// Called by the failure detector.
func (m *Membership) UpdateNodeStatus(id NodeID, status NodeStatus) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	node := m.state.Nodes[id]
	if node == nil {
		return fmt.Errorf("node %s not found", id)
	}

	oldStatus := node.Status
	if oldStatus == status {
		return nil // No change
	}

	node.Status = status
	m.state.Version++
	m.state.UpdatedAt = time.Now()

	// Determine event type based on transition
	var eventType MembershipEventType
	switch status {
	case NodeStatusSuspect:
		eventType = EventNodeSuspect
	case NodeStatusDead:
		eventType = EventNodeDied
	case NodeStatusAlive:
		eventType = EventNodeRecovered
	case NodeStatusLeaving:
		eventType = EventNodeLeft
	default:
		eventType = EventStateUpdated
	}

	m.emitEventLocked(MembershipEvent{
		Type:      eventType,
		NodeID:    id,
		NodeInfo:  node.Clone(),
		OldStatus: oldStatus,
		NewStatus: status,
		Timestamp: time.Now(),
	})

	return m.persistStateLocked()
}

// UpdateHeartbeat updates the last heartbeat time for a node.
// Called when we receive a heartbeat.
func (m *Membership) UpdateHeartbeat(id NodeID) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	node := m.state.Nodes[id]
	if node == nil {
		return fmt.Errorf("node %s not found", id)
	}

	node.LastHeartbeat = time.Now()

	// If node was suspect or dead, mark it alive
	if node.Status == NodeStatusSuspect || node.Status == NodeStatusDead {
		oldStatus := node.Status
		node.Status = NodeStatusAlive
		m.state.Version++
		m.state.UpdatedAt = time.Now()

		m.emitEventLocked(MembershipEvent{
			Type:      EventNodeRecovered,
			NodeID:    id,
			NodeInfo:  node.Clone(),
			OldStatus: oldStatus,
			NewStatus: NodeStatusAlive,
			Timestamp: time.Now(),
		})

		return m.persistStateLocked()
	}

	return nil
}

// =============================================================================
// CONTROLLER MANAGEMENT
// =============================================================================

// SetController sets the current controller.
// Called when controller election completes.
func (m *Membership) SetController(id NodeID, epoch int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Verify node exists
	node := m.state.Nodes[id]
	if node == nil && !id.IsEmpty() {
		return fmt.Errorf("node %s not found", id)
	}

	oldController := m.state.ControllerID
	m.state.ControllerID = id
	m.state.ControllerEpoch = epoch
	m.state.Version++
	m.state.UpdatedAt = time.Now()

	// Update roles
	for nodeID, nodeInfo := range m.state.Nodes {
		if nodeID == id {
			nodeInfo.Role = NodeRoleController
		} else {
			nodeInfo.Role = NodeRoleFollower
		}
	}

	// Update local node role
	if id == m.localNode.ID() {
		m.localNode.SetRole(NodeRoleController)
	} else {
		m.localNode.SetRole(NodeRoleFollower)
	}

	m.emitEventLocked(MembershipEvent{
		Type:      EventControllerChanged,
		NodeID:    id,
		NodeInfo:  node,
		Timestamp: time.Now(),
	})

	_ = oldController // May use for logging

	return m.persistStateLocked()
}

// =============================================================================
// STATE SYNCHRONIZATION
// =============================================================================

// ApplyState replaces local state with received state.
// Called when we sync from another node.
func (m *Membership) ApplyState(state *ClusterState) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Only apply if version is newer
	if state.Version <= m.state.Version {
		return nil
	}

	m.state = state.Clone()

	// Update local node role based on new state
	if m.state.ControllerID == m.localNode.ID() {
		m.localNode.SetRole(NodeRoleController)
	} else {
		m.localNode.SetRole(NodeRoleFollower)
	}

	m.emitEventLocked(MembershipEvent{
		Type:      EventStateUpdated,
		Timestamp: time.Now(),
	})

	return m.persistStateLocked()
}

// MergeState merges received state with local state.
// Used for incremental updates.
func (m *Membership) MergeState(state *ClusterState) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	changed := false

	// Merge nodes
	for id, node := range state.Nodes {
		existing := m.state.Nodes[id]
		if existing == nil {
			// New node
			m.state.Nodes[id] = node.Clone()
			changed = true
		} else if node.LastHeartbeat.After(existing.LastHeartbeat) {
			// Newer info
			m.state.Nodes[id] = node.Clone()
			changed = true
		}
	}

	// Update controller if epoch is higher
	if state.ControllerEpoch > m.state.ControllerEpoch {
		m.state.ControllerID = state.ControllerID
		m.state.ControllerEpoch = state.ControllerEpoch
		changed = true
	}

	if changed {
		m.state.Version++
		m.state.UpdatedAt = time.Now()
		return m.persistStateLocked()
	}

	return nil
}

// =============================================================================
// EVENT LISTENERS
// =============================================================================

// AddListener registers a callback for membership events.
func (m *Membership) AddListener(listener MembershipListener) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.listeners = append(m.listeners, listener)
}

// emitEventLocked fires an event to all listeners.
// Must be called with mu held.
func (m *Membership) emitEventLocked(event MembershipEvent) {
	for _, listener := range m.listeners {
		// Call listener in goroutine to prevent blocking
		go listener(event)
	}
}

// =============================================================================
// PERSISTENCE
// =============================================================================
//
// WHY PERSIST?
//   - On restart, we need to know who was in the cluster
//   - Avoids bootstrap confusion (is this a new cluster or recovery?)
//   - Preserves controller epoch to prevent zombie controllers
//
// =============================================================================

// stateFilePath returns the path to the cluster state file.
func (m *Membership) stateFilePath() string {
	return filepath.Join(m.dataDir, "cluster", "state.json")
}

// persistStateLocked saves state to disk.
// Must be called with mu held.
func (m *Membership) persistStateLocked() error {
	if m.dataDir == "" {
		return nil // Persistence disabled
	}

	// Ensure directory exists
	dir := filepath.Dir(m.stateFilePath())
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create cluster state directory: %w", err)
	}

	// Marshal state
	data, err := json.MarshalIndent(m.state, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal cluster state: %w", err)
	}

	// Write atomically (write to temp, then rename)
	tempPath := m.stateFilePath() + ".tmp"
	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write cluster state: %w", err)
	}

	if err := os.Rename(tempPath, m.stateFilePath()); err != nil {
		return fmt.Errorf("failed to rename cluster state file: %w", err)
	}

	return nil
}

// LoadState loads persisted state from disk.
func (m *Membership) LoadState() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.dataDir == "" {
		return nil // Persistence disabled
	}

	data, err := os.ReadFile(m.stateFilePath())
	if os.IsNotExist(err) {
		return nil // No state yet
	}
	if err != nil {
		return fmt.Errorf("failed to read cluster state: %w", err)
	}

	var state ClusterState
	if err := json.Unmarshal(data, &state); err != nil {
		return fmt.Errorf("failed to unmarshal cluster state: %w", err)
	}

	m.state = &state
	return nil
}

// =============================================================================
// UTILITY METHODS
// =============================================================================

// LocalNode returns the local node.
func (m *Membership) LocalNode() *Node {
	return m.localNode
}

// GetOtherNodes returns all nodes except the local one.
func (m *Membership) GetOtherNodes() []*NodeInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	localID := m.localNode.ID()
	nodes := make([]*NodeInfo, 0, len(m.state.Nodes)-1)
	for id, node := range m.state.Nodes {
		if id != localID {
			nodes = append(nodes, node.Clone())
		}
	}
	return nodes
}

// GetAliveOtherNodes returns alive nodes except the local one.
func (m *Membership) GetAliveOtherNodes() []*NodeInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	localID := m.localNode.ID()
	nodes := make([]*NodeInfo, 0)
	for id, node := range m.state.Nodes {
		if id != localID && node.Status == NodeStatusAlive {
			nodes = append(nodes, node.Clone())
		}
	}
	return nodes
}

// String returns a string representation of membership.
func (m *Membership) String() string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return fmt.Sprintf("Membership{nodes=%d, controller=%s, version=%d}",
		len(m.state.Nodes), m.state.ControllerID, m.state.Version)
}
