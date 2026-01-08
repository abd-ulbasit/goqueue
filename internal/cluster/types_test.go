package cluster

import (
	"testing"
	"time"
)

// =============================================================================
// NODE ADDRESS TESTS
// =============================================================================

func TestNodeAddress_String(t *testing.T) {
	tests := []struct {
		name string
		addr NodeAddress
		want string
	}{
		{
			name: "standard address",
			addr: NodeAddress{Host: "localhost", Port: 8080},
			want: "localhost:8080",
		},
		{
			name: "ip address",
			addr: NodeAddress{Host: "192.168.1.1", Port: 9090},
			want: "192.168.1.1:9090",
		},
		{
			name: "empty host",
			addr: NodeAddress{Host: "", Port: 8080},
			want: ":8080",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.addr.String(); got != tt.want {
				t.Errorf("NodeAddress.String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseNodeAddress(t *testing.T) {
	tests := []struct {
		name    string
		addr    string
		want    NodeAddress
		wantErr bool
	}{
		{
			name:    "valid address",
			addr:    "localhost:8080",
			want:    NodeAddress{Host: "localhost", Port: 8080},
			wantErr: false,
		},
		{
			name:    "ip address",
			addr:    "192.168.1.1:9090",
			want:    NodeAddress{Host: "192.168.1.1", Port: 9090},
			wantErr: false,
		},
		{
			name:    "missing port",
			addr:    "localhost",
			want:    NodeAddress{},
			wantErr: true,
		},
		{
			name:    "invalid port",
			addr:    "localhost:notanumber",
			want:    NodeAddress{},
			wantErr: true,
		},
		{
			name:    "empty string",
			addr:    "",
			want:    NodeAddress{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseNodeAddress(tt.addr)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseNodeAddress() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("ParseNodeAddress() = %v, want %v", got, tt.want)
			}
		})
	}
}

// =============================================================================
// CLUSTER STATE TESTS
// =============================================================================

func TestClusterState_NodeCount(t *testing.T) {
	state := &ClusterState{
		Version: 1,
		Nodes:   make(map[NodeID]*NodeInfo),
	}

	// Empty state
	if got := state.NodeCount(); got != 0 {
		t.Errorf("NodeCount() = %v, want 0", got)
	}

	// Add nodes
	state.Nodes["node1"] = &NodeInfo{ID: "node1", Status: NodeStatusAlive}
	state.Nodes["node2"] = &NodeInfo{ID: "node2", Status: NodeStatusDead}
	state.Nodes["node3"] = &NodeInfo{ID: "node3", Status: NodeStatusSuspect}

	if got := state.NodeCount(); got != 3 {
		t.Errorf("NodeCount() = %v, want 3", got)
	}
}

func TestClusterState_AliveNodeCount(t *testing.T) {
	state := &ClusterState{
		Version: 1,
		Nodes:   make(map[NodeID]*NodeInfo),
	}

	// Add nodes with various statuses
	state.Nodes["node1"] = &NodeInfo{ID: "node1", Status: NodeStatusAlive}
	state.Nodes["node2"] = &NodeInfo{ID: "node2", Status: NodeStatusDead}
	state.Nodes["node3"] = &NodeInfo{ID: "node3", Status: NodeStatusSuspect}
	state.Nodes["node4"] = &NodeInfo{ID: "node4", Status: NodeStatusAlive}

	if got := state.AliveNodeCount(); got != 2 {
		t.Errorf("AliveNodeCount() = %v, want 2", got)
	}
}

func TestClusterState_HasQuorum(t *testing.T) {
	tests := []struct {
		name  string
		nodes map[NodeID]*NodeInfo
		want  bool
	}{
		{
			name:  "empty cluster no quorum",
			nodes: map[NodeID]*NodeInfo{},
			want:  false,
		},
		{
			name: "single node has quorum",
			nodes: map[NodeID]*NodeInfo{
				"node1": {ID: "node1", Status: NodeStatusAlive},
			},
			want: true, // 1 node, quorum = 1
		},
		{
			name: "two of three alive has quorum",
			nodes: map[NodeID]*NodeInfo{
				"node1": {ID: "node1", Status: NodeStatusAlive},
				"node2": {ID: "node2", Status: NodeStatusAlive},
				"node3": {ID: "node3", Status: NodeStatusDead},
			},
			want: true, // 3 nodes, quorum = 2, alive = 2
		},
		{
			name: "one of three alive no quorum",
			nodes: map[NodeID]*NodeInfo{
				"node1": {ID: "node1", Status: NodeStatusAlive},
				"node2": {ID: "node2", Status: NodeStatusDead},
				"node3": {ID: "node3", Status: NodeStatusDead},
			},
			want: false, // 3 nodes, quorum = 2, alive = 1
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := &ClusterState{
				Version: 1,
				Nodes:   tt.nodes,
			}
			if got := state.HasQuorum(); got != tt.want {
				t.Errorf("HasQuorum() = %v, want %v", got, tt.want)
			}
		})
	}
}

// =============================================================================
// CLUSTER CONFIG TESTS
// =============================================================================

func TestClusterConfig_WithDefaults(t *testing.T) {
	// Empty config
	emptyConfig := &ClusterConfig{}
	result := emptyConfig.WithDefaults()

	if result.HeartbeatInterval == 0 {
		t.Error("HeartbeatInterval not set by defaults")
	}
	if result.SuspectTimeout == 0 {
		t.Error("SuspectTimeout not set by defaults")
	}
	if result.DeadTimeout == 0 {
		t.Error("DeadTimeout not set by defaults")
	}
	if result.QuorumSize == 0 {
		t.Error("QuorumSize not set by defaults")
	}

	// Test existing values are preserved
	customConfig := &ClusterConfig{
		HeartbeatInterval: 10 * time.Second,
		QuorumSize:        5,
	}
	result = customConfig.WithDefaults()

	if result.HeartbeatInterval != 10*time.Second {
		t.Errorf("HeartbeatInterval = %v, want 10s (should preserve custom)", result.HeartbeatInterval)
	}
	if result.QuorumSize != 5 {
		t.Errorf("QuorumSize = %v, want 5 (should preserve custom)", result.QuorumSize)
	}
}

// =============================================================================
// NODE STATUS TESTS
// =============================================================================

func TestNodeStatus_Constants(t *testing.T) {
	tests := []struct {
		status NodeStatus
		want   string
	}{
		{NodeStatusUnknown, "unknown"},
		{NodeStatusAlive, "alive"},
		{NodeStatusSuspect, "suspect"},
		{NodeStatusDead, "dead"},
		{NodeStatusLeaving, "leaving"},
	}

	for _, tt := range tests {
		t.Run(string(tt.status), func(t *testing.T) {
			if got := string(tt.status); got != tt.want {
				t.Errorf("NodeStatus = %v, want %v", got, tt.want)
			}
		})
	}
}
