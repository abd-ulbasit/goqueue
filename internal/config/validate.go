package config

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
)

// =============================================================================
// CONFIG VALIDATION MODULE
// =============================================================================
//
// WHY VALIDATE CONFIG AT STARTUP?
//
//   Bad config is the #1 cause of production outages. Catching it at startup
//   (fail-fast) is MUCH better than discovering it at 3 AM.
//
//   FAIL-FAST: Bad config -> immediate, clear error -> fix before traffic hits
//   FAIL-LAZY: Bad config -> broker starts -> first publish fails -> pages on-call
//
//   PATTERN: ACCUMULATE ERRORS
//   We collect ALL validation errors and return them together so the operator
//   can fix everything in one pass instead of playing whack-a-mole.
//
//   COMPARISON:
//     - Kafka: Validates config at startup, logs errors and exits
//     - RabbitMQ: Validates config at startup, fails with clear message
//     - NATS: Validates config, returns structured errors
//     - goqueue: Returns all errors at once (not one-by-one)
//
// =============================================================================

// ValidationError holds one or more configuration validation failures.
//
// WHY A CUSTOM ERROR TYPE?
//   - Collects multiple errors (operator fixes all at once)
//   - Formats nicely for logging and display
//   - Can be type-asserted to check if error is validation-related
type ValidationError struct {
	Errors []string
}

// Error implements the error interface.
// Formats all validation errors as a numbered list for readability.
func (e *ValidationError) Error() string {
	if len(e.Errors) == 1 {
		return fmt.Sprintf("configuration validation failed: %s", e.Errors[0])
	}

	var b strings.Builder
	b.WriteString("configuration validation failed:\n")
	for i, err := range e.Errors {
		fmt.Fprintf(&b, "  %d. %s\n", i+1, err)
	}
	return b.String()
}

// =============================================================================
// BROKER CONFIG VALIDATION
// =============================================================================

// BrokerConfigValidator validates broker configuration.
type BrokerConfigValidator struct{}

// BrokerConfig mirrors the broker.BrokerConfig to avoid import cycles.
//
// WHY NOT IMPORT broker.BrokerConfig?
//
//	Go prohibits circular imports. The broker package will call this validator,
//	so this package cannot import broker. We define a minimal mirror struct.
type BrokerConfig struct {
	DataDir        string
	NodeID         string
	ClusterEnabled bool
	ClusterConfig  *ClusterConfig
}

// ClusterConfig mirrors the cluster configuration subset.
type ClusterConfig struct {
	ClientAddress    string
	ClusterAddress   string
	AdvertiseAddress string
	Peers            []string
	QuorumSize       int
}

// Validate checks the broker configuration for common mistakes.
// Returns nil if valid, or a *ValidationError with all problems found.
func (v *BrokerConfigValidator) Validate(cfg BrokerConfig) error {
	var errs []string

	// DataDir: where all WAL segments, indexes, and metadata live
	if cfg.DataDir == "" {
		errs = append(errs, "data_dir: must not be empty")
	} else {
		errs = append(errs, validateDataDir(cfg.DataDir)...)
	}

	// NodeID: unique broker identity in the cluster
	if cfg.NodeID == "" {
		errs = append(errs, "node_id: must not be empty")
	} else if strings.ContainsAny(cfg.NodeID, " \t\n\r") {
		errs = append(errs, "node_id: must not contain whitespace")
	}

	// Cluster config: required when cluster mode is enabled
	if cfg.ClusterEnabled {
		if cfg.ClusterConfig == nil {
			errs = append(errs, "cluster: cluster_config is required when cluster mode is enabled")
		} else {
			errs = append(errs, validateClusterConfig(cfg.ClusterConfig)...)
		}
	}

	if len(errs) > 0 {
		return &ValidationError{Errors: errs}
	}
	return nil
}

// validateDataDir checks that the data directory is usable.
func validateDataDir(dir string) []string {
	var errs []string

	absDir, err := filepath.Abs(dir)
	if err != nil {
		errs = append(errs, fmt.Sprintf("data_dir: cannot resolve path %q: %v", dir, err))
		return errs
	}

	info, err := os.Stat(absDir)
	if err == nil {
		if !info.IsDir() {
			errs = append(errs, fmt.Sprintf("data_dir: %q exists but is not a directory", absDir))
		}
		return errs
	}

	if !os.IsNotExist(err) {
		errs = append(errs, fmt.Sprintf("data_dir: cannot access %q: %v", absDir, err))
		return errs
	}

	// Directory doesn't exist -- check if parent is accessible
	parent := filepath.Dir(absDir)
	if _, err := os.Stat(parent); err != nil {
		errs = append(errs, fmt.Sprintf("data_dir: %q does not exist and parent %q is not accessible: %v", absDir, parent, err))
	}

	return errs
}

// validateClusterConfig checks cluster-specific configuration.
func validateClusterConfig(cfg *ClusterConfig) []string {
	var errs []string

	// Peers: need at least one to cluster with
	if len(cfg.Peers) == 0 {
		errs = append(errs, "cluster.peers: at least one peer is required in cluster mode")
	} else {
		for i, peer := range cfg.Peers {
			if err := validateAddress(peer); err != nil {
				errs = append(errs, fmt.Sprintf("cluster.peers[%d]: invalid address %q: %v", i, peer, err))
			}
		}
	}

	// QuorumSize: must be > 0 and <= total cluster size
	totalNodes := len(cfg.Peers) + 1
	if cfg.QuorumSize <= 0 {
		errs = append(errs, fmt.Sprintf("cluster.quorum_size: must be > 0, got %d", cfg.QuorumSize))
	} else if cfg.QuorumSize > totalNodes {
		errs = append(errs, fmt.Sprintf("cluster.quorum_size: %d exceeds total cluster size %d (peers=%d + self=1)", cfg.QuorumSize, totalNodes, len(cfg.Peers)))
	}

	// ClusterAddress: how other nodes reach us
	if cfg.ClusterAddress == "" {
		errs = append(errs, "cluster.cluster_address: must not be empty")
	} else if err := validateAddress(cfg.ClusterAddress); err != nil {
		errs = append(errs, fmt.Sprintf("cluster.cluster_address: invalid: %v", err))
	}

	// AdvertiseAddress: optional but must be valid if set
	if cfg.AdvertiseAddress != "" {
		if err := validateAddress(cfg.AdvertiseAddress); err != nil {
			errs = append(errs, fmt.Sprintf("cluster.advertise_address: invalid: %v", err))
		}
	}

	return errs
}

// validateAddress checks that a string is a valid host:port or :port address.
func validateAddress(addr string) error {
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("must be host:port format: %w", err)
	}
	if port == "" {
		return fmt.Errorf("port must not be empty")
	}
	return nil
}
