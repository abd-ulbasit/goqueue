// =============================================================================
// CLUSTER TYPES - FOUNDATIONAL DATA STRUCTURES
// =============================================================================
//
// WHY: These types define the shared vocabulary for all cluster components.
// Every node needs to agree on what a "node", "member", or "cluster state" means.
//
// DESIGN PHILOSOPHY:
//   - Types are minimal and focused
//   - JSON tags for wire format (inter-node communication)
//   - Immutable where possible (use methods to create new states)
//
// =============================================================================

package cluster

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

// =============================================================================
// NODE IDENTITY
// =============================================================================
//
// WHAT: Uniquely identifies a node in the cluster.
//
// WHY NOT JUST USE IP:PORT?
//   - IP addresses can change (DHCP, Kubernetes pod restarts)
//   - Multiple nodes could run on same IP (different ports)
//   - Human-readable IDs help debugging ("node-1" vs "10.0.0.5:9000")
//
// COMPARISON:
//   - Kafka: Uses broker.id (integer, configured)
//   - Cassandra: Uses host_id (UUID, auto-generated)
//   - etcd: Uses member ID (uint64)
//   - goqueue: Uses string ID (flexible, human-readable)
//
// =============================================================================

// NodeID uniquely identifies a node in the cluster.
// Must be unique across all nodes and stable across restarts.
type NodeID string

// String implements fmt.Stringer for NodeID.
func (n NodeID) String() string {
	return string(n)
}

// IsEmpty returns true if the NodeID is not set.
func (n NodeID) IsEmpty() bool {
	return n == ""
}

// =============================================================================
// NODE STATUS
// =============================================================================
//
// STATE MACHINE:
//
//                    ┌───────────┐
//                    │  UNKNOWN  │  (initial state)
//                    └─────┬─────┘
//                          │ first heartbeat received
//                          ▼
//                    ┌───────────┐
//            ┌──────►│   ALIVE   │◄────────────────┐
//            │       └─────┬─────┘                 │
//            │             │ heartbeat timeout     │
//            │             ▼                       │
//            │       ┌───────────┐                 │
//            │       │  SUSPECT  │  (grace period) │
//            │       └─────┬─────┘                 │
//            │             │                       │
//            │     ┌───────┴───────┐               │
//            │     │               │               │
//            │ more timeouts  heartbeat received ─►│
//            │     ▼                               │
//            │  ┌───────────┐                      │
//            │  │   DEAD    │                      │
//            │  └─────┬─────┘  (resurrection)      │
//            │        │                            │
//            │        │ removed from cluster       │
//            │        ▼                            │
//            │ ┌───────────┐                       │
//            └─┤  LEAVING  │───────────────────────┘
//              └───────────┘   (rejoins)
//
// WHY SUSPECT STATE?
//   Network glitches are common. SUSPECT gives the node a chance to recover
//   before we declare it DEAD and trigger expensive failover operations.
//
// =============================================================================

// NodeStatus represents the health status of a node.
type NodeStatus string

const (
	// NodeStatusUnknown is the initial state before any heartbeat.
	NodeStatusUnknown NodeStatus = "unknown"

	// NodeStatusAlive means the node is healthy and responsive.
	NodeStatusAlive NodeStatus = "alive"

	// NodeStatusSuspect means the node missed some heartbeats but isn't dead yet.
	// This is a grace period to handle temporary network issues.
	NodeStatusSuspect NodeStatus = "suspect"

	// NodeStatusDead means the node has been unresponsive for too long.
	// Partition failover should be triggered.
	NodeStatusDead NodeStatus = "dead"

	// NodeStatusLeaving means the node is gracefully shutting down.
	// It has announced its departure and is draining work.
	NodeStatusLeaving NodeStatus = "leaving"
)

// IsHealthy returns true if the node is alive or leaving (still functional).
func (s NodeStatus) IsHealthy() bool {
	return s == NodeStatusAlive || s == NodeStatusLeaving
}

// =============================================================================
// NODE ROLE
// =============================================================================
//
// WHAT: Special responsibilities a node may have.
//
// CONTROLLER ROLE:
//   - Only ONE node is controller at a time
//   - Controller manages cluster metadata (topic configs, partition assignments)
//   - If controller dies, another node must be elected
//   - Similar to Kafka's controller broker
//
// FOLLOWER ROLE:
//   - Regular nodes that follow the controller
//   - Can still serve read/write requests for partitions they own
//
// =============================================================================

// NodeRole represents the special role a node plays in the cluster.
type NodeRole string

const (
	// NodeRoleFollower is the default role. Follows controller decisions.
	NodeRoleFollower NodeRole = "follower"

	// NodeRoleController is the elected controller managing cluster state.
	// Only ONE node should have this role at any time.
	NodeRoleController NodeRole = "controller"
)

// =============================================================================
// NODE ADDRESS
// =============================================================================
//
// WHY SEPARATE ADDRESS TYPE?
//   - Nodes have multiple addresses for different purposes:
//     - Client address: Where producers/consumers connect (external)
//     - Internal address: Where other nodes connect (cluster gossip, replication)
//   - Validation logic in one place
//
// =============================================================================

// NodeAddress represents a network endpoint for a node.
type NodeAddress struct {
	// Host is the hostname or IP address.
	Host string `json:"host"`

	// Port is the port number.
	Port int `json:"port"`
}

// String returns the address in "host:port" format.
func (a NodeAddress) String() string {
	return fmt.Sprintf("%s:%d", a.Host, a.Port)
}

// IsValid checks if the address has required fields.
func (a NodeAddress) IsValid() bool {
	return a.Host != "" && a.Port > 0 && a.Port <= 65535
}

// ParseNodeAddress parses a "host:port" string into NodeAddress.
func ParseNodeAddress(addr string) (NodeAddress, error) {
	if addr == "" {
		return NodeAddress{}, fmt.Errorf("invalid address format: empty string (expected host:port)")
	}

	// Find the last colon (to support IPv6 addresses in future)
	lastColon := -1
	for i := len(addr) - 1; i >= 0; i-- {
		if addr[i] == ':' {
			lastColon = i
			break
		}
	}

	if lastColon == -1 {
		return NodeAddress{}, fmt.Errorf("invalid address format: %s (expected host:port)", addr)
	}

	host := addr[:lastColon]
	portStr := addr[lastColon+1:]

	port, err := strconv.Atoi(portStr)
	if err != nil {
		return NodeAddress{}, fmt.Errorf("invalid port in address: %s", addr)
	}

	return NodeAddress{Host: host, Port: port}, nil
}

// =============================================================================
// NODE INFO
// =============================================================================
//
// WHAT: Complete information about a node in the cluster.
// This is the "business card" that nodes exchange with each other.
//
// COMPARISON:
//   - Kafka: BrokerRegistrationRequest contains similar info
//   - Cassandra: EndpointState in gossip protocol
//
// =============================================================================

// NodeInfo contains all metadata about a cluster node.
type NodeInfo struct {
	// ID uniquely identifies this node.
	ID NodeID `json:"id"`

	// ClientAddress is where producers/consumers connect.
	// This is the external-facing address.
	ClientAddress NodeAddress `json:"client_address"`

	// ClusterAddress is where other cluster nodes connect.
	// Used for heartbeats, replication, and controller communication.
	ClusterAddress NodeAddress `json:"cluster_address"`

	// Status is the current health status.
	Status NodeStatus `json:"status"`

	// Role is the special role (controller or follower).
	Role NodeRole `json:"role"`

	// JoinedAt is when this node joined the cluster.
	JoinedAt time.Time `json:"joined_at"`

	// LastHeartbeat is when we last heard from this node.
	LastHeartbeat time.Time `json:"last_heartbeat"`

	// Version is the goqueue version running on this node.
	// Useful for rolling upgrades and compatibility checks.
	Version string `json:"version"`

	// Rack is the failure domain (datacenter, rack, availability zone).
	// Used for replica placement to ensure fault tolerance.
	Rack string `json:"rack,omitempty"`

	// Tags are arbitrary key-value pairs for node metadata.
	// Examples: {"environment": "production", "tier": "hot"}
	Tags map[string]string `json:"tags,omitempty"`
}

// Clone creates a deep copy of NodeInfo.
func (n *NodeInfo) Clone() *NodeInfo {
	if n == nil {
		return nil
	}

	clone := *n
	if n.Tags != nil {
		clone.Tags = make(map[string]string, len(n.Tags))
		for k, v := range n.Tags {
			clone.Tags[k] = v
		}
	}
	return &clone
}

// IsController returns true if this node is the cluster controller.
func (n *NodeInfo) IsController() bool {
	return n.Role == NodeRoleController
}

// =============================================================================
// CLUSTER STATE
// =============================================================================
//
// WHAT: The complete state of the cluster at a point in time.
// This is what gets replicated to all nodes.
//
// VERSIONING: ClusterVersion is a monotonically increasing counter.
// When the controller makes changes, it bumps the version.
// Followers compare versions to know if they need updates.
//
// =============================================================================

// ClusterState represents the complete cluster membership state.
type ClusterState struct {
	// Version is incremented on every state change.
	// Used for optimistic concurrency and cache invalidation.
	Version int64 `json:"version"`

	// ControllerID is the NodeID of the current controller.
	// Empty if no controller is elected.
	ControllerID NodeID `json:"controller_id"`

	// ControllerEpoch increments each time a new controller is elected.
	// Used to fence zombie controllers.
	ControllerEpoch int64 `json:"controller_epoch"`

	// Nodes is the current membership list.
	Nodes map[NodeID]*NodeInfo `json:"nodes"`

	// UpdatedAt is when this state was last modified.
	UpdatedAt time.Time `json:"updated_at"`
}

// NewClusterState creates an empty cluster state.
func NewClusterState() *ClusterState {
	return &ClusterState{
		Version:   0,
		Nodes:     make(map[NodeID]*NodeInfo),
		UpdatedAt: time.Now(),
	}
}

// Clone creates a deep copy of ClusterState.
func (s *ClusterState) Clone() *ClusterState {
	if s == nil {
		return nil
	}

	clone := &ClusterState{
		Version:         s.Version,
		ControllerID:    s.ControllerID,
		ControllerEpoch: s.ControllerEpoch,
		Nodes:           make(map[NodeID]*NodeInfo, len(s.Nodes)),
		UpdatedAt:       s.UpdatedAt,
	}

	for id, node := range s.Nodes {
		clone.Nodes[id] = node.Clone()
	}

	return clone
}

// GetNode returns the NodeInfo for a given ID, or nil if not found.
func (s *ClusterState) GetNode(id NodeID) *NodeInfo {
	if s == nil || s.Nodes == nil {
		return nil
	}
	return s.Nodes[id]
}

// AliveNodes returns all nodes with status ALIVE.
func (s *ClusterState) AliveNodes() []*NodeInfo {
	var alive []*NodeInfo
	for _, node := range s.Nodes {
		if node.Status == NodeStatusAlive {
			alive = append(alive, node)
		}
	}
	return alive
}

// HealthyNodes returns all nodes with healthy status (ALIVE or LEAVING).
func (s *ClusterState) HealthyNodes() []*NodeInfo {
	var healthy []*NodeInfo
	for _, node := range s.Nodes {
		if node.Status.IsHealthy() {
			healthy = append(healthy, node)
		}
	}
	return healthy
}

// NodeCount returns the total number of nodes.
func (s *ClusterState) NodeCount() int {
	return len(s.Nodes)
}

// AliveNodeCount returns the number of ALIVE nodes.
func (s *ClusterState) AliveNodeCount() int {
	count := 0
	for _, node := range s.Nodes {
		if node.Status == NodeStatusAlive {
			count++
		}
	}
	return count
}

// HasQuorum returns true if the cluster has a majority of nodes alive.
// For N nodes, quorum = N/2 + 1.
//
// WHY QUORUM MATTERS:
//   - Prevents split-brain (two halves of cluster both thinking they're valid)
//   - Ensures any decision has majority agreement
//   - If we lose quorum, we stop accepting writes (safety > availability)
func (s *ClusterState) HasQuorum() bool {
	total := s.NodeCount()
	if total == 0 {
		return false
	}
	alive := s.AliveNodeCount()
	quorum := (total / 2) + 1
	return alive >= quorum
}

// GetController returns the current controller's NodeInfo, or nil.
func (s *ClusterState) GetController() *NodeInfo {
	if s.ControllerID.IsEmpty() {
		return nil
	}
	return s.GetNode(s.ControllerID)
}

// =============================================================================
// CLUSTER CONFIGURATION
// =============================================================================
//
// WHAT: Configuration options for cluster behavior.
//
// TUNING GUIDELINES:
//   - HeartbeatInterval: Smaller = faster detection, more traffic
//   - SuspectTimeout: Smaller = faster failover, more false positives
//   - DeadTimeout: How long to wait before declaring node dead
//
// =============================================================================

// ClusterConfig contains configuration for cluster behavior.
type ClusterConfig struct {
	// NodeID is this node's unique identifier.
	// If empty, will use hostname.
	NodeID string `json:"node_id" yaml:"node_id"`

	// ClientAddress is where clients connect (e.g., "0.0.0.0:8080").
	ClientAddress string `json:"client_address" yaml:"client_address"`

	// ClusterAddress is where other nodes connect for cluster operations (e.g., "0.0.0.0:9000").
	ClusterAddress string `json:"cluster_address" yaml:"cluster_address"`

	// AdvertiseAddress is the address to advertise to other nodes.
	// Use this when running behind NAT or in containers.
	// If empty, uses ClusterAddress.
	AdvertiseAddress string `json:"advertise_address" yaml:"advertise_address"`

	// Peers is the list of other nodes to connect to on startup.
	// is this going to be thier cluster address or advertise address?
	// Format: ["host1:port1", "host2:port2"]
	Peers []string `json:"peers" yaml:"peers"`

	// QuorumSize is the minimum number of nodes required for cluster operations.
	// Default: 1 (single-node mode)
	QuorumSize int `json:"quorum_size" yaml:"quorum_size"`

	// HeartbeatInterval is how often to send heartbeats to peers.
	// Default: 3s (Kafka default)
	HeartbeatInterval time.Duration `json:"heartbeat_interval" yaml:"heartbeat_interval"`

	// SuspectTimeout is how long after missed heartbeat before marking SUSPECT.
	// Default: 6s (2 missed heartbeats)
	SuspectTimeout time.Duration `json:"suspect_timeout" yaml:"suspect_timeout"`

	// DeadTimeout is how long after missed heartbeat before marking DEAD.
	// Default: 9s (3 missed heartbeats)
	DeadTimeout time.Duration `json:"dead_timeout" yaml:"dead_timeout"`

	// BootstrapExpectNodes is how many nodes to wait for before forming cluster.
	// If 0, will bootstrap immediately (single-node mode).
	// For multi-node, set to expected cluster size for safe bootstrap.
	BootstrapExpectNodes int `json:"bootstrap_expect_nodes" yaml:"bootstrap_expect_nodes"`

	// BootstrapTimeout is how long to wait for expected nodes during bootstrap.
	// Default: 60s
	BootstrapTimeout time.Duration `json:"bootstrap_timeout" yaml:"bootstrap_timeout"`

	// LeaseTimeout is how long a controller lease is valid.
	// Controller must renew before this expires.
	// Default: 15s
	LeaseTimeout time.Duration `json:"lease_timeout" yaml:"lease_timeout"`

	// LeaseRenewInterval is how often controller renews its lease.
	// Should be less than LeaseTimeout/3 for safety margin.
	// Default: 5s
	LeaseRenewInterval time.Duration `json:"lease_renew_interval" yaml:"lease_renew_interval"`

	// DataDir is where cluster state is persisted.
	// Default: uses broker's data directory
	DataDir string `json:"data_dir" yaml:"data_dir"`

	// Version is the goqueue version (for compatibility checks).
	Version string `json:"version" yaml:"version"`

	// Rack is the failure domain for this node.
	Rack string `json:"rack" yaml:"rack"`

	// Tags are arbitrary metadata for this node.
	Tags map[string]string `json:"tags" yaml:"tags"`
}

// DefaultClusterConfig returns configuration with sensible defaults.
func DefaultClusterConfig() *ClusterConfig {
	return &ClusterConfig{
		QuorumSize:           1, // Single-node mode by default
		HeartbeatInterval:    3 * time.Second,
		SuspectTimeout:       6 * time.Second,
		DeadTimeout:          9 * time.Second,
		BootstrapExpectNodes: 0, // Single-node mode by default
		BootstrapTimeout:     60 * time.Second,
		LeaseTimeout:         15 * time.Second,
		LeaseRenewInterval:   5 * time.Second,
		Version:              "0.10.0",
	}
}

// WithDefaults returns a new config with defaults applied for any unset values.
func (c *ClusterConfig) WithDefaults() *ClusterConfig {
	defaults := DefaultClusterConfig()
	result := *c // Copy

	if result.QuorumSize <= 0 {
		result.QuorumSize = defaults.QuorumSize
	}
	if result.HeartbeatInterval <= 0 {
		result.HeartbeatInterval = defaults.HeartbeatInterval
	}
	if result.SuspectTimeout <= 0 {
		result.SuspectTimeout = defaults.SuspectTimeout
	}
	if result.DeadTimeout <= 0 {
		result.DeadTimeout = defaults.DeadTimeout
	}
	if result.BootstrapTimeout <= 0 {
		result.BootstrapTimeout = defaults.BootstrapTimeout
	}
	if result.LeaseTimeout <= 0 {
		result.LeaseTimeout = defaults.LeaseTimeout
	}
	if result.LeaseRenewInterval <= 0 {
		result.LeaseRenewInterval = defaults.LeaseRenewInterval
	}
	if result.Version == "" {
		result.Version = defaults.Version
	}

	return &result
}

// Validate checks if the configuration is valid.
func (c *ClusterConfig) Validate() error {
	if c.HeartbeatInterval <= 0 {
		return fmt.Errorf("heartbeat_interval must be positive")
	}
	if c.SuspectTimeout <= c.HeartbeatInterval {
		return fmt.Errorf("suspect_timeout must be greater than heartbeat_interval")
	}
	if c.DeadTimeout <= c.SuspectTimeout {
		return fmt.Errorf("dead_timeout must be greater than suspect_timeout")
	}
	if c.LeaseTimeout <= 0 {
		return fmt.Errorf("lease_timeout must be positive")
	}
	if c.LeaseRenewInterval >= c.LeaseTimeout {
		return fmt.Errorf("lease_renew_interval must be less than lease_timeout")
	}
	return nil
}

// =============================================================================
// WIRE PROTOCOL MESSAGES
// =============================================================================
//
// These are the messages exchanged between nodes over HTTP/JSON.
// Each message type corresponds to a specific cluster operation.
//
// =============================================================================

// HeartbeatRequest is sent by nodes to signal liveness.
type HeartbeatRequest struct {
	// NodeID is the sender's ID.
	NodeID NodeID `json:"node_id"`

	// Timestamp is when the heartbeat was sent.
	Timestamp time.Time `json:"timestamp"`

	// ControllerID is who the sender thinks is controller.
	ControllerID NodeID `json:"controller_id"`

	// Epoch is the controller epoch the sender knows about.
	Epoch int64 `json:"epoch"`

	// ClusterVersion is the sender's view of cluster state version.
	// If behind, receiver should send state update.
	ClusterVersion int64 `json:"cluster_version"`
}

// HeartbeatResponse is the response to a heartbeat.
type HeartbeatResponse struct {
	// NodeID is the responder's ID.
	NodeID NodeID `json:"node_id"`

	// Version is the responder's cluster state version.
	Version int64 `json:"version"`

	// ControllerID is the current controller.
	ControllerID NodeID `json:"controller_id"`

	// ControllerEpoch is the current controller's epoch.
	ControllerEpoch int64 `json:"controller_epoch"`

	// Nodes is the current membership list (piggybacked).
	Nodes map[NodeID]*NodeInfo `json:"nodes,omitempty"`

	// Error message if heartbeat was rejected.
	Error string `json:"error,omitempty"`
}

// JoinRequest is sent when a node wants to join the cluster.
type JoinRequest struct {
	// NodeID is the joining node's ID.
	NodeID NodeID `json:"node_id"`

	// NodeInfo is the joining node's information.
	NodeInfo NodeInfo `json:"node_info"`
}

// JoinResponse is the response to a join request.
type JoinResponse struct {
	// Success indicates if join was accepted.
	Success bool `json:"success"`

	// ClusterState is the current cluster state (if accepted).
	ClusterState *ClusterState `json:"cluster_state,omitempty"`

	// ControllerID is who the controller is.
	ControllerID NodeID `json:"controller_id"`

	// ControllerAddr is the controller's cluster address.
	ControllerAddr string `json:"controller_addr,omitempty"`

	// RedirectRequired is true if the request should go to controller.
	RedirectRequired bool `json:"redirect_required"`

	// Error message if success is false.
	Error string `json:"error,omitempty"`
}

// LeaveRequest is sent when a node wants to leave gracefully.
type LeaveRequest struct {
	// NodeID is the leaving node's ID.
	NodeID NodeID `json:"node_id"`

	// Graceful indicates if this is a planned departure.
	// If true, other nodes can wait for work to drain.
	Graceful bool `json:"graceful"`
}

// LeaveResponse is the response to a leave request.
type LeaveResponse struct {
	// Success indicates if leave was accepted.
	Success bool `json:"success"`

	// Error message if success is false.
	Error string `json:"error,omitempty"`
}

// StateSyncRequest asks for the current cluster state.
type StateSyncRequest struct {
	// NodeID is the requesting node.
	NodeID NodeID `json:"node_id"`

	// CurrentVersion is the requester's current state version.
	CurrentVersion int64 `json:"current_version"`
}

// StateSyncResponse contains the cluster state.
type StateSyncResponse struct {
	// ClusterState is the current state.
	ClusterState *ClusterState `json:"cluster_state"`

	// ClusterMeta is the cluster metadata (topics, assignments).
	ClusterMeta *ClusterMeta `json:"cluster_meta,omitempty"`

	// ControllerID is the current controller.
	ControllerID NodeID `json:"controller_id"`

	// ControllerEpoch is the controller's epoch.
	ControllerEpoch int64 `json:"controller_epoch"`
}

// ControllerVoteRequest is used during controller election.
type ControllerVoteRequest struct {
	// CandidateID is the node requesting to become controller.
	CandidateID NodeID `json:"candidate_id"`

	// Epoch is the epoch this candidate proposes.
	Epoch int64 `json:"epoch"`

	// LeaseExpiry is when this lease would expire.
	LeaseExpiry time.Time `json:"lease_expiry"`
}

// ControllerVoteResponse is the response to a vote request.
type ControllerVoteResponse struct {
	// VoterID is the responder's node ID.
	VoterID NodeID `json:"voter_id"`

	// VoteGranted is true if the vote is granted.
	VoteGranted bool `json:"vote_granted"`

	// Epoch is the epoch of the vote.
	Epoch int64 `json:"epoch"`

	// CurrentControllerID is who we think is controller (if vote denied).
	CurrentControllerID NodeID `json:"current_controller_id,omitempty"`

	// Error message if vote denied.
	Error string `json:"error,omitempty"`
}

// =============================================================================
// JSON SERIALIZATION HELPERS
// =============================================================================

// MarshalJSON implements json.Marshaler for ClusterState with sorting.
func (s *ClusterState) MarshalJSON() ([]byte, error) {
	type Alias ClusterState
	return json.Marshal((*Alias)(s))
}

// MarshalJSON implements json.Marshaler for NodeInfo.
func (n *NodeInfo) MarshalJSON() ([]byte, error) {
	type Alias NodeInfo
	return json.Marshal((*Alias)(n))
}
