package config

import (
	"os"
	"path/filepath"
	"testing"
)

// =============================================================================
// CONFIG VALIDATION TESTS
// =============================================================================
//
// These tests verify that configuration validation catches common mistakes
// BEFORE the broker starts. This is the "fail-fast" principle:
//   - Bad config → clear error at startup → fix before traffic hits
//
// TEST STRATEGY: Table-driven tests
//   Each test case specifies:
//   - name: what we're testing
//   - config: the BrokerConfig to validate
//   - wantErr: whether we expect validation to fail
//   - errContains: substring(s) the error message should contain
// =============================================================================

func TestBrokerConfigValidator_Validate(t *testing.T) {
	// Create a temporary directory for valid data dir tests
	tmpDir := t.TempDir()

	tests := []struct {
		name        string
		config      BrokerConfig
		wantErr     bool
		errContains []string
	}{
		{
			name: "valid minimal config",
			config: BrokerConfig{
				DataDir: tmpDir,
				NodeID:  "node-1",
			},
			wantErr: false,
		},
		{
			name: "empty data dir",
			config: BrokerConfig{
				DataDir: "",
				NodeID:  "node-1",
			},
			wantErr:     true,
			errContains: []string{"data_dir: must not be empty"},
		},
		{
			name: "empty node ID",
			config: BrokerConfig{
				DataDir: tmpDir,
				NodeID:  "",
			},
			wantErr:     true,
			errContains: []string{"node_id: must not be empty"},
		},
		{
			name: "node ID with whitespace",
			config: BrokerConfig{
				DataDir: tmpDir,
				NodeID:  "node 1",
			},
			wantErr:     true,
			errContains: []string{"node_id: must not contain whitespace"},
		},
		{
			name: "cluster enabled without config",
			config: BrokerConfig{
				DataDir:        tmpDir,
				NodeID:         "node-1",
				ClusterEnabled: true,
				ClusterConfig:  nil,
			},
			wantErr:     true,
			errContains: []string{"cluster_config is required"},
		},
		{
			name: "valid cluster config",
			config: BrokerConfig{
				DataDir:        tmpDir,
				NodeID:         "node-1",
				ClusterEnabled: true,
				ClusterConfig: &ClusterConfig{
					ClusterAddress:   ":7000",
					AdvertiseAddress: "goqueue-0.goqueue-headless:7000",
					Peers:            []string{"goqueue-1.goqueue-headless:7000", "goqueue-2.goqueue-headless:7000"},
					QuorumSize:       2,
				},
			},
			wantErr: false,
		},
		{
			name: "cluster with no peers",
			config: BrokerConfig{
				DataDir:        tmpDir,
				NodeID:         "node-1",
				ClusterEnabled: true,
				ClusterConfig: &ClusterConfig{
					ClusterAddress: ":7000",
					Peers:          []string{},
					QuorumSize:     1,
				},
			},
			wantErr:     true,
			errContains: []string{"at least one peer is required"},
		},
		{
			name: "cluster with invalid peer address",
			config: BrokerConfig{
				DataDir:        tmpDir,
				NodeID:         "node-1",
				ClusterEnabled: true,
				ClusterConfig: &ClusterConfig{
					ClusterAddress: ":7000",
					Peers:          []string{"invalid-no-port"},
					QuorumSize:     1,
				},
			},
			wantErr:     true,
			errContains: []string{"cluster.peers[0]", "invalid address"},
		},
		{
			name: "cluster quorum exceeds cluster size",
			config: BrokerConfig{
				DataDir:        tmpDir,
				NodeID:         "node-1",
				ClusterEnabled: true,
				ClusterConfig: &ClusterConfig{
					ClusterAddress: ":7000",
					Peers:          []string{"peer:7000"},
					QuorumSize:     5,
				},
			},
			wantErr:     true,
			errContains: []string{"exceeds total cluster size"},
		},
		{
			name: "cluster quorum zero",
			config: BrokerConfig{
				DataDir:        tmpDir,
				NodeID:         "node-1",
				ClusterEnabled: true,
				ClusterConfig: &ClusterConfig{
					ClusterAddress: ":7000",
					Peers:          []string{"peer:7000"},
					QuorumSize:     0,
				},
			},
			wantErr:     true,
			errContains: []string{"quorum_size: must be > 0"},
		},
		{
			name: "empty cluster address",
			config: BrokerConfig{
				DataDir:        tmpDir,
				NodeID:         "node-1",
				ClusterEnabled: true,
				ClusterConfig: &ClusterConfig{
					ClusterAddress: "",
					Peers:          []string{"peer:7000"},
					QuorumSize:     1,
				},
			},
			wantErr:     true,
			errContains: []string{"cluster_address: must not be empty"},
		},
		{
			name: "multiple errors at once",
			config: BrokerConfig{
				DataDir: "",
				NodeID:  "",
			},
			wantErr:     true,
			errContains: []string{"data_dir", "node_id"},
		},
		{
			name: "data dir that does not exist but parent exists",
			config: BrokerConfig{
				DataDir: filepath.Join(tmpDir, "new-subdir"),
				NodeID:  "node-1",
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &BrokerConfigValidator{}
			err := v.Validate(tt.config)

			if tt.wantErr && err == nil {
				t.Errorf("expected error but got nil")
				return
			}
			if !tt.wantErr && err != nil {
				t.Errorf("expected no error but got: %v", err)
				return
			}

			if err != nil {
				errMsg := err.Error()
				for _, want := range tt.errContains {
					if !containsSubstring(errMsg, want) {
						t.Errorf("error %q should contain %q", errMsg, want)
					}
				}

				// Verify it's a ValidationError
				if _, ok := err.(*ValidationError); !ok {
					t.Errorf("error should be *ValidationError, got %T", err)
				}
			}
		})
	}
}

// TestValidateAddress tests the address validation helper.
func TestValidateAddress(t *testing.T) {
	tests := []struct {
		addr    string
		wantErr bool
	}{
		{":8080", false},
		{"0.0.0.0:8080", false},
		{"localhost:9000", false},
		{"goqueue-0.goqueue-headless.default.svc.cluster.local:7000", false},
		{"[::1]:8080", false},
		{"invalid", true},
		{"no-port:", true},
		{"", true},
	}

	for _, tt := range tests {
		t.Run(tt.addr, func(t *testing.T) {
			err := validateAddress(tt.addr)
			if tt.wantErr && err == nil {
				t.Errorf("expected error for %q but got nil", tt.addr)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("expected no error for %q but got: %v", tt.addr, err)
			}
		})
	}
}

// TestValidateDataDir tests data directory validation edge cases.
func TestValidateDataDir(t *testing.T) {
	// Test: path exists but is a file, not directory
	tmpFile := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(tmpFile, []byte("test"), 0644); err != nil {
		t.Fatal(err)
	}

	errs := validateDataDir(tmpFile)
	if len(errs) == 0 {
		t.Error("expected error when data dir is a file, got none")
	}

	found := false
	for _, e := range errs {
		if containsSubstring(e, "not a directory") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected 'not a directory' error, got: %v", errs)
	}
}

// TestValidationError_SingleError tests error formatting with one error.
func TestValidationError_SingleError(t *testing.T) {
	err := &ValidationError{Errors: []string{"data_dir: must not be empty"}}
	msg := err.Error()
	expected := "configuration validation failed: data_dir: must not be empty"
	if msg != expected {
		t.Errorf("got %q, want %q", msg, expected)
	}
}

// TestValidationError_MultipleErrors tests error formatting with multiple errors.
func TestValidationError_MultipleErrors(t *testing.T) {
	err := &ValidationError{Errors: []string{"error one", "error two"}}
	msg := err.Error()
	if !containsSubstring(msg, "1. error one") {
		t.Errorf("expected numbered format, got: %s", msg)
	}
	if !containsSubstring(msg, "2. error two") {
		t.Errorf("expected numbered format, got: %s", msg)
	}
}

func containsSubstring(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && contains(s, substr))
}

func contains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
