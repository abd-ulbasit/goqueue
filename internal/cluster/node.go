// =============================================================================
// NODE - LOCAL NODE IDENTITY AND STATE
// =============================================================================
//
// WHAT: Represents THIS node in the cluster.
// Each goqueue process has exactly one Node instance representing itself.
//
// RESPONSIBILITIES:
//   - Maintain local node identity (ID, addresses)
//   - Track own state (role, status)
//   - Provide self-description for cluster membership
//
// COMPARISON:
//   - Kafka: KafkaBroker class holds broker identity
//   - Cassandra: StorageService manages local node state
//
// =============================================================================

package cluster

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// =============================================================================
// NODE
// =============================================================================

// Node represents the local node in the cluster.
// Thread-safe for concurrent access.
type Node struct {
	// mu protects all mutable fields
	mu sync.RWMutex

	// id is this node's unique identifier
	id NodeID

	// clientAddr is where clients connect
	clientAddr NodeAddress

	// clusterAddr is where other nodes connect
	clusterAddr NodeAddress

	// advertiseAddr is the address advertised to other nodes
	// (may differ from clusterAddr if behind NAT)
	advertiseAddr NodeAddress

	// status is our current health status (as we see it)
	status NodeStatus

	// role is our role in the cluster (controller or follower)
	role NodeRole

	// joinedAt is when we joined the cluster
	joinedAt time.Time

	// version is the goqueue version
	version string

	// rack is our failure domain
	rack string

	// tags are arbitrary metadata
	tags map[string]string

	// config is our cluster configuration
	config *ClusterConfig
}

// =============================================================================
// NODE CREATION
// =============================================================================

// NewNode creates a new Node with the given configuration.
//
// The node starts in UNKNOWN status and FOLLOWER role.
// Call Start() to begin participating in the cluster.
func NewNode(config *ClusterConfig) (*Node, error) {
	if config == nil {
		config = DefaultClusterConfig()
	}

	// Validate config
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid cluster config: %w", err)
	}

	// Determine node ID
	nodeID := config.NodeID
	if nodeID == "" {
		// Fall back to hostname
		hostname, err := os.Hostname()
		if err != nil {
			return nil, fmt.Errorf("failed to get hostname for node ID: %w", err)
		}
		nodeID = hostname
	}

	// Parse client address
	clientAddr, err := parseAddressWithDefaults(config.ClientAddress, "0.0.0.0", 8080)
	if err != nil {
		return nil, fmt.Errorf("invalid client address: %w", err)
	}

	// Parse cluster address (inter-node communication)
	clusterAddr, err := parseAddressWithDefaults(config.ClusterAddress, "0.0.0.0", 9000)
	if err != nil {
		return nil, fmt.Errorf("invalid cluster address: %w", err)
	}

	// Parse advertise address (defaults to cluster)
	advertiseAddr := clusterAddr
	if config.AdvertiseAddress != "" {
		advertiseAddr, err = parseAddressWithDefaults(config.AdvertiseAddress, clusterAddr.Host, clusterAddr.Port)
		if err != nil {
			return nil, fmt.Errorf("invalid advertise address: %w", err)
		}
	}

	// Copy tags
	var tags map[string]string
	if config.Tags != nil {
		tags = make(map[string]string, len(config.Tags))
		for k, v := range config.Tags {
			tags[k] = v
		}
	}

	return &Node{
		id:            NodeID(nodeID),
		clientAddr:    clientAddr,
		clusterAddr:   clusterAddr,
		advertiseAddr: advertiseAddr,
		status:        NodeStatusUnknown,
		role:          NodeRoleFollower,
		version:       config.Version,
		rack:          config.Rack,
		tags:          tags,
		config:        config,
	}, nil
}

// =============================================================================
// NODE IDENTITY
// =============================================================================

// ID returns the node's unique identifier.
func (n *Node) ID() NodeID {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.id
}

// ClientAddress returns where clients should connect.
func (n *Node) ClientAddress() NodeAddress {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.clientAddr
}

// ClusterAddress returns where other nodes should connect.
func (n *Node) ClusterAddress() NodeAddress {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.clusterAddr
}

// AdvertiseAddress returns the address to advertise to other nodes.
func (n *Node) AdvertiseAddress() NodeAddress {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.advertiseAddr
}

// Version returns the goqueue version.
func (n *Node) Version() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.version
}

// Rack returns the failure domain.
func (n *Node) Rack() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.rack
}

// Tags returns a copy of the node's tags.
func (n *Node) Tags() map[string]string {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if n.tags == nil {
		return nil
	}

	tags := make(map[string]string, len(n.tags))
	for k, v := range n.tags {
		tags[k] = v
	}
	return tags
}

// Config returns the cluster configuration.
func (n *Node) Config() *ClusterConfig {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.config
}

// =============================================================================
// NODE STATE
// =============================================================================

// Status returns the node's current health status.
func (n *Node) Status() NodeStatus {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.status
}

// SetStatus updates the node's health status.
func (n *Node) SetStatus(status NodeStatus) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.status = status
}

// Role returns the node's current role.
func (n *Node) Role() NodeRole {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.role
}

// SetRole updates the node's role.
func (n *Node) SetRole(role NodeRole) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.role = role
}

// IsController returns true if this node is the cluster controller.
func (n *Node) IsController() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.role == NodeRoleController
}

// JoinedAt returns when this node joined the cluster.
func (n *Node) JoinedAt() time.Time {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.joinedAt
}

// SetJoinedAt updates when this node joined the cluster.
func (n *Node) SetJoinedAt(t time.Time) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.joinedAt = t
}

// =============================================================================
// NODE INFO EXPORT
// =============================================================================

// Info returns a NodeInfo snapshot of this node's current state.
// This is what we send to other nodes.
func (n *Node) Info() *NodeInfo {
	n.mu.RLock()
	defer n.mu.RUnlock()

	var tags map[string]string
	if n.tags != nil {
		tags = make(map[string]string, len(n.tags))
		for k, v := range n.tags {
			tags[k] = v
		}
	}

	return &NodeInfo{
		ID:             n.id,
		ClientAddress:  n.clientAddr,
		ClusterAddress: n.advertiseAddr, // Use advertise address for other nodes
		Status:         n.status,
		Role:           n.role,
		JoinedAt:       n.joinedAt,
		LastHeartbeat:  time.Now(), // Current time as we're alive
		Version:        n.version,
		Rack:           n.rack,
		Tags:           tags,
	}
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

// parseAddressWithDefaults parses an address string with fallback defaults.
func parseAddressWithDefaults(addr string, defaultHost string, defaultPort int) (NodeAddress, error) {
	if addr == "" {
		return NodeAddress{Host: defaultHost, Port: defaultPort}, nil
	}

	// Handle formats:
	//   - ":8080"         (default host)
	//   - "host:8080"     (explicit host + port)
	//   - "host"          (default port)
	//
	// NOTE:
	//   We avoid fmt.Sscanf here because Go does not support C-style scansets
	//   like %[^:], which would silently mis-parse "host:port".
	if strings.HasPrefix(addr, ":") {
		port, err := strconv.Atoi(strings.TrimPrefix(addr, ":"))
		if err != nil {
			return NodeAddress{}, fmt.Errorf("invalid port in address: %s", addr)
		}
		return NodeAddress{Host: defaultHost, Port: port}, nil
	}

	// Split on the last colon (matches ParseNodeAddress behavior and is IPv6-friendly-ish).
	if idx := strings.LastIndex(addr, ":"); idx != -1 {
		host := addr[:idx]
		portStr := addr[idx+1:]
		if host == "" {
			host = defaultHost
		}
		port, err := strconv.Atoi(portStr)
		if err != nil {
			return NodeAddress{}, fmt.Errorf("invalid port in address: %s", addr)
		}
		return NodeAddress{Host: host, Port: port}, nil
	}

	// Host only.
	return NodeAddress{Host: addr, Port: defaultPort}, nil
}

// =============================================================================
// STRING REPRESENTATION
// =============================================================================

// String returns a human-readable representation of the node.
func (n *Node) String() string {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return fmt.Sprintf("Node{id=%s, status=%s, role=%s, addr=%s}",
		n.id, n.status, n.role, n.advertiseAddr)
}
