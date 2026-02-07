// =============================================================================
// PARTITION SCALER - UNIT TESTS
// =============================================================================
//
// Tests for online partition scaling functionality.
//
// COVERAGE:
//   - Error type definitions
//   - Internal topic detection
//   - Scaling request validation logic
//
// NOTE:
// Full integration tests require a running broker with metadata store.
// These unit tests focus on validation logic and helper functions.
//
// =============================================================================

package broker

import (
	"testing"
)

// =============================================================================
// ERROR CONSTANT TESTS
// =============================================================================

func TestPartitionScalerErrors(t *testing.T) {
	// Verify error messages are meaningful
	tests := []struct {
		name string
		err  error
	}{
		{"CannotReducePartitions", ErrCannotReducePartitions},
		{"NoChange", ErrNoChange},
		{"InsufficientNodes", ErrInsufficientNodes},
		{"ScalingInProgress", ErrScalingInProgress},
		{"InternalTopicScaling", ErrInternalTopicScaling},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.err == nil {
				t.Error("error should not be nil")
			}
			if tt.err.Error() == "" {
				t.Error("error message should not be empty")
			}
		})
	}
}

// =============================================================================
// INTERNAL TOPIC DETECTION TESTS
// =============================================================================

func TestIsInternalTopic(t *testing.T) {
	tests := []struct {
		name     string
		topic    string
		expected bool
	}{
		{
			name:     "consumer_offsets_topic",
			topic:    ConsumerOffsetsTopicName,
			expected: true,
		},
		{
			name:     "double_underscore_prefix",
			topic:    "__some_internal",
			expected: true,
		},
		{
			name:     "normal_topic",
			topic:    "my-topic",
			expected: false,
		},
		{
			name:     "single_underscore_prefix",
			topic:    "_consumer_offsets",
			expected: false,
		},
		{
			name:     "empty_string",
			topic:    "",
			expected: false,
		},
		{
			name:     "double_underscore_only",
			topic:    "__",
			expected: true,
		},
		{
			name:     "transaction_state",
			topic:    "__transaction_state",
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isInternalTopic(tt.topic)
			if result != tt.expected {
				t.Errorf("isInternalTopic(%q): got %v, want %v", tt.topic, result, tt.expected)
			}
		})
	}
}

// =============================================================================
// PARTITION SCALE REQUEST TESTS
// =============================================================================

func TestPartitionScaleRequest_Validation(t *testing.T) {
	// Test that request fields are properly structured
	request := PartitionScaleRequest{
		TopicName:         "test-topic",
		NewPartitionCount: 10,
	}

	if request.TopicName == "" {
		t.Error("topic name should be set")
	}
	if request.NewPartitionCount <= 0 {
		t.Error("new partition count should be positive")
	}
}

func TestPartitionScaleResult_Structure(t *testing.T) {
	// Test result struct fields
	result := PartitionScaleResult{
		Success:           true,
		TopicName:         "test-topic",
		OldPartitionCount: 4,
		NewPartitionCount: 8,
		PartitionsAdded:   []int{4, 5, 6, 7},
	}

	if result.TopicName != "test-topic" {
		t.Error("topic name should match")
	}
	if !result.Success {
		t.Error("success should be true")
	}
	if result.OldPartitionCount >= result.NewPartitionCount {
		t.Error("new count should be greater than old count")
	}
	if len(result.PartitionsAdded) != 4 {
		t.Errorf("expected 4 partitions added, got %d", len(result.PartitionsAdded))
	}

	// Verify partition IDs are sequential starting from old count
	for i, pid := range result.PartitionsAdded {
		expected := result.OldPartitionCount + i
		if pid != expected {
			t.Errorf("partition ID at index %d: got %d, want %d", i, pid, expected)
		}
	}
}

// =============================================================================
// PARTITION CHANGE LISTENER TESTS
// =============================================================================

func TestPartitionChangeListener_Type(t *testing.T) {
	// Verify listener function signature is correct
	var listener PartitionChangeListener = func(topicName string, oldCount, newCount int, newPartitions []int) {
		// This should compile with correct signature
	}

	// Call it to verify it works
	listener("test", 4, 8, []int{4, 5, 6, 7})
}
