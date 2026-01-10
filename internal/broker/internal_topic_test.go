// =============================================================================
// INTERNAL TOPIC TYPES - UNIT TESTS
// =============================================================================
//
// Tests for the internal topic record types and encoding/decoding.
//
// COVERAGE:
//   - InternalRecord encoding/decoding roundtrip
//   - OffsetCommitValue encoding/decoding
//   - GroupMetadataValue encoding/decoding
//   - GroupToPartition hash distribution
//   - Tombstone records for deletion
//
// =============================================================================

package broker

import (
	"bytes"
	"testing"
	"time"
)

// =============================================================================
// INTERNAL RECORD TESTS
// =============================================================================

func TestInternalRecord_EncodeDecodeRoundtrip(t *testing.T) {
	tests := []struct {
		name   string
		record *InternalRecord
	}{
		{
			name: "offset_commit",
			record: &InternalRecord{
				Type:  RecordTypeOffsetCommit,
				Key:   []byte("offset-key"),
				Value: []byte("offset-value"),
				// Timestamp not encoded in record, it's in the storage.Message wrapper
			},
		},
		{
			name: "group_metadata",
			record: &InternalRecord{
				Type:  RecordTypeGroupMetadata,
				Key:   []byte("group-key"),
				Value: []byte("group-value"),
			},
		},
		{
			name: "tombstone",
			record: &InternalRecord{
				Type:  RecordTypeTombstone,
				Key:   []byte("tombstone-key"),
				Value: nil, // Tombstones have nil value
			},
		},
		{
			name: "empty_value",
			record: &InternalRecord{
				Type:  RecordTypeOffsetCommit,
				Key:   []byte("key"),
				Value: []byte{},
			},
		},
		{
			name: "large_value",
			record: &InternalRecord{
				Type:  RecordTypeGroupMetadata,
				Key:   []byte("large-key"),
				Value: bytes.Repeat([]byte("x"), 10000),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encode
			encoded := tt.record.Encode()
			if len(encoded) == 0 {
				t.Fatal("encoded record is empty")
			}

			// Decode
			decoded, err := DecodeInternalRecord(encoded)
			if err != nil {
				t.Fatalf("failed to decode: %v", err)
			}

			// Verify
			if decoded.Type != tt.record.Type {
				t.Errorf("type mismatch: got %v, want %v", decoded.Type, tt.record.Type)
			}
			if !bytes.Equal(decoded.Key, tt.record.Key) {
				t.Errorf("key mismatch: got %v, want %v", decoded.Key, tt.record.Key)
			}
			if !bytes.Equal(decoded.Value, tt.record.Value) {
				t.Errorf("value mismatch: got %v, want %v", decoded.Value, tt.record.Value)
			}
			// NOTE: Timestamp is NOT encoded in InternalRecord itself.
			// The timestamp is set when the record is wrapped in a storage.Message.
			// This is intentional - the InternalRecord is the payload, and the
			// storage layer adds the timestamp when appending to the log.
		})
	}
}

func TestDecodeInternalRecord_Errors(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{
			name: "nil_data",
			data: nil,
		},
		{
			name: "empty_data",
			data: []byte{},
		},
		{
			name: "too_short",
			data: []byte{0x01, 0x02}, // Less than minimum header
		},
		{
			name: "invalid_magic",
			data: []byte{0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := DecodeInternalRecord(tt.data)
			if err == nil {
				t.Error("expected error, got nil")
			}
		})
	}
}

// =============================================================================
// OFFSET COMMIT VALUE TESTS
// =============================================================================

func TestOffsetCommitValue_EncodeDecodeRoundtrip(t *testing.T) {
	now := time.Now().UnixMilli()
	tests := []struct {
		name  string
		value OffsetCommitValue
	}{
		{
			name: "basic_commit",
			value: OffsetCommitValue{
				Offset:          12345,
				Metadata:        "test-metadata",
				Timestamp:       now,
				ExpireTimestamp: now + 24*60*60*1000, // +24 hours
			},
		},
		{
			name: "zero_offset",
			value: OffsetCommitValue{
				Offset:          0,
				Metadata:        "",
				Timestamp:       now,
				ExpireTimestamp: 0, // Never expires
			},
		},
		{
			name: "large_offset",
			value: OffsetCommitValue{
				Offset:          int64(1) << 62, // Large offset
				Metadata:        "large-offset-metadata",
				Timestamp:       now,
				ExpireTimestamp: now + 365*24*60*60*1000, // +1 year
			},
		},
		{
			name: "unicode_metadata",
			value: OffsetCommitValue{
				Offset:          100,
				Metadata:        "метаданные 数据 🚀",
				Timestamp:       now,
				ExpireTimestamp: now + 60*60*1000,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encode
			encoded := tt.value.Encode()
			if len(encoded) == 0 {
				t.Fatal("encoded value is empty")
			}

			// Decode
			decoded, err := DecodeOffsetCommitValue(encoded)
			if err != nil {
				t.Fatalf("failed to decode: %v", err)
			}

			// Verify
			if decoded.Offset != tt.value.Offset {
				t.Errorf("offset mismatch: got %d, want %d", decoded.Offset, tt.value.Offset)
			}
			if decoded.Metadata != tt.value.Metadata {
				t.Errorf("metadata mismatch: got %q, want %q", decoded.Metadata, tt.value.Metadata)
			}
			if decoded.Timestamp != tt.value.Timestamp {
				t.Errorf("timestamp mismatch: got %d, want %d", decoded.Timestamp, tt.value.Timestamp)
			}
			if decoded.ExpireTimestamp != tt.value.ExpireTimestamp {
				t.Errorf("expire_timestamp mismatch: got %d, want %d", decoded.ExpireTimestamp, tt.value.ExpireTimestamp)
			}
		})
	}
}

// =============================================================================
// GROUP METADATA VALUE TESTS
// =============================================================================

func TestGroupMetadataValue_EncodeDecodeRoundtrip(t *testing.T) {
	tests := []struct {
		name  string
		value GroupMetadataValue
	}{
		{
			name: "empty_group",
			value: GroupMetadataValue{
				State:      GroupStateEmpty,
				Generation: 1,
				Protocol:   "range",
				Leader:     "",
				Members:    nil,
			},
		},
		{
			name: "single_member",
			value: GroupMetadataValue{
				State:      GroupStateStable,
				Generation: 5,
				Protocol:   "roundrobin",
				Leader:     "member-1",
				Members: []InternalMemberMetadata{
					{
						MemberID:       "member-1",
						ClientID:       "client-1",
						ClientHost:     "host-1",
						SessionTimeout: 30000,
						Assignment:     []byte("assignment-data"),
					},
				},
			},
		},
		{
			name: "multiple_members",
			value: GroupMetadataValue{
				State:      GroupStateStable,
				Generation: 10,
				Protocol:   "sticky",
				Leader:     "member-2",
				Members: []InternalMemberMetadata{
					{
						MemberID:       "member-1",
						ClientID:       "client-1",
						ClientHost:     "host-1",
						SessionTimeout: 30000,
						Assignment:     []byte("assignment-1"),
					},
					{
						MemberID:       "member-2",
						ClientID:       "client-2",
						ClientHost:     "host-2",
						SessionTimeout: 45000,
						Assignment:     []byte("assignment-2"),
					},
					{
						MemberID:       "member-3",
						ClientID:       "client-3",
						ClientHost:     "host-3",
						SessionTimeout: 60000,
						Assignment:     []byte("assignment-3"),
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encode
			encoded := tt.value.Encode()
			if len(encoded) == 0 {
				t.Fatal("encoded value is empty")
			}

			// Decode
			decoded, err := DecodeGroupMetadataValue(encoded)
			if err != nil {
				t.Fatalf("failed to decode: %v", err)
			}

			// Verify
			if decoded.State != tt.value.State {
				t.Errorf("state mismatch: got %v, want %v", decoded.State, tt.value.State)
			}
			if decoded.Generation != tt.value.Generation {
				t.Errorf("generation mismatch: got %d, want %d", decoded.Generation, tt.value.Generation)
			}
			if decoded.Protocol != tt.value.Protocol {
				t.Errorf("protocol mismatch: got %q, want %q", decoded.Protocol, tt.value.Protocol)
			}
			if decoded.Leader != tt.value.Leader {
				t.Errorf("leader mismatch: got %q, want %q", decoded.Leader, tt.value.Leader)
			}
			if len(decoded.Members) != len(tt.value.Members) {
				t.Errorf("members count mismatch: got %d, want %d", len(decoded.Members), len(tt.value.Members))
			}

			// Verify each member
			for i, member := range decoded.Members {
				if i >= len(tt.value.Members) {
					break
				}
				expected := tt.value.Members[i]
				if member.MemberID != expected.MemberID {
					t.Errorf("member[%d] ID mismatch: got %q, want %q", i, member.MemberID, expected.MemberID)
				}
				if member.ClientID != expected.ClientID {
					t.Errorf("member[%d] ClientID mismatch: got %q, want %q", i, member.ClientID, expected.ClientID)
				}
			}
		})
	}
}

// =============================================================================
// KEY STRUCTURE TESTS
// =============================================================================

func TestOffsetCommitKey_EncodeDecodeRoundtrip(t *testing.T) {
	tests := []struct {
		name string
		key  OffsetCommitKey
	}{
		{
			name: "simple",
			key:  OffsetCommitKey{Group: "group-1", Topic: "topic-1", Partition: 0},
		},
		{
			name: "multi_partition",
			key:  OffsetCommitKey{Group: "group-1", Topic: "topic-1", Partition: 5},
		},
		{
			name: "special_chars",
			key:  OffsetCommitKey{Group: "group.with.dots", Topic: "topic-with-dashes", Partition: 10},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encode
			encoded := tt.key.Encode()
			if len(encoded) == 0 {
				t.Error("key is empty")
			}

			// Decode
			decoded, err := DecodeOffsetCommitKey(encoded)
			if err != nil {
				t.Fatalf("failed to decode: %v", err)
			}

			// Verify
			if decoded.Group != tt.key.Group {
				t.Errorf("group mismatch: got %q, want %q", decoded.Group, tt.key.Group)
			}
			if decoded.Topic != tt.key.Topic {
				t.Errorf("topic mismatch: got %q, want %q", decoded.Topic, tt.key.Topic)
			}
			if decoded.Partition != tt.key.Partition {
				t.Errorf("partition mismatch: got %d, want %d", decoded.Partition, tt.key.Partition)
			}
		})
	}
}

func TestGroupMetadataKey_EncodeDecodeRoundtrip(t *testing.T) {
	tests := []struct {
		name  string
		group string
	}{
		{"simple", "group-1"},
		{"with_dots", "group.with.dots"},
		{"unicode", "группа"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := GroupMetadataKey{Group: tt.group}

			// Encode
			encoded := key.Encode()
			if len(encoded) == 0 {
				t.Error("key is empty")
			}

			// Decode
			decoded, err := DecodeGroupMetadataKey(encoded)
			if err != nil {
				t.Fatalf("failed to decode: %v", err)
			}

			// Verify
			if decoded.Group != tt.group {
				t.Errorf("group mismatch: got %q, want %q", decoded.Group, tt.group)
			}
		})
	}
}

// =============================================================================
// GROUP TO PARTITION MAPPING TESTS
// =============================================================================

func TestGroupToPartition(t *testing.T) {
	// Test that the same group always maps to the same partition
	t.Run("deterministic", func(t *testing.T) {
		group := "test-group"
		partitions := 50

		p1 := GroupToPartition(group, partitions)
		p2 := GroupToPartition(group, partitions)

		if p1 != p2 {
			t.Errorf("same group mapped to different partitions: %d vs %d", p1, p2)
		}
	})

	// Test partition range
	t.Run("in_range", func(t *testing.T) {
		partitions := 50
		for i := 0; i < 1000; i++ {
			group := "group-" + string(rune('a'+i%26))
			p := GroupToPartition(group, partitions)
			if p < 0 || p >= partitions {
				t.Errorf("partition %d out of range [0, %d)", p, partitions)
			}
		}
	})

	// Test distribution (basic check that it's not all one partition)
	t.Run("distribution", func(t *testing.T) {
		partitions := 50
		counts := make(map[int]int)

		for i := 0; i < 10000; i++ {
			group := "group-" + string(rune(i))
			p := GroupToPartition(group, partitions)
			counts[p]++
		}

		// Should have more than 1 partition used
		if len(counts) < 10 {
			t.Errorf("poor distribution: only %d partitions used", len(counts))
		}
	})
}

// =============================================================================
// RECORD BUILDER TESTS
// =============================================================================

func TestNewOffsetCommitRecord(t *testing.T) {
	record := NewOffsetCommitRecord("group-1", "topic-1", 0, 100, "metadata")

	if record.Type != RecordTypeOffsetCommit {
		t.Errorf("wrong type: got %v, want %v", record.Type, RecordTypeOffsetCommit)
	}
	if len(record.Key) == 0 {
		t.Error("key is empty")
	}
	if len(record.Value) == 0 {
		t.Error("value is empty")
	}
	if record.Timestamp.IsZero() {
		t.Error("timestamp is zero")
	}
}

func TestNewGroupMetadataRecord(t *testing.T) {
	members := []InternalMemberMetadata{
		{MemberID: "m1", ClientID: "c1"},
	}
	record := NewGroupMetadataRecord("group-1", GroupStateStable, "range", "m1", 1, members)

	if record.Type != RecordTypeGroupMetadata {
		t.Errorf("wrong type: got %v, want %v", record.Type, RecordTypeGroupMetadata)
	}
	if len(record.Key) == 0 {
		t.Error("key is empty")
	}
	if len(record.Value) == 0 {
		t.Error("value is empty")
	}
}

func TestNewTombstoneRecord(t *testing.T) {
	originalKey := []byte("some-key")
	record := NewTombstoneRecord(RecordTypeOffsetCommit, originalKey)

	if record.Type != RecordTypeTombstone {
		t.Errorf("wrong type: got %v, want %v", record.Type, RecordTypeTombstone)
	}
	if !bytes.Equal(record.Key, originalKey) {
		t.Errorf("key mismatch: got %v, want %v", record.Key, originalKey)
	}
	if record.Value != nil {
		t.Errorf("tombstone should have nil value, got %v", record.Value)
	}
}
