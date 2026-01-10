// =============================================================================
// INTERNAL TOPIC - SYSTEM TOPICS FOR COORDINATOR STATE REPLICATION
// =============================================================================
//
// WHAT ARE INTERNAL TOPICS?
// Internal topics are system-managed topics used for storing coordinator state.
// They use the same log+replication infrastructure as user topics but:
//   - Hidden from user-facing APIs (ListTopics, etc.)
//   - Have special naming prefix "__" (double underscore)
//   - May have different retention policies (e.g., compaction)
//   - Contain binary-encoded state records instead of user messages
//
// ┌──────────────────────────────────────────────────────────────────────────┐
// │                     INTERNAL TOPICS IN GOQUEUE                           │
// │                                                                          │
// │   __consumer_offsets                                                     │
// │   ├── Partition 0 ──────────► Groups hashing to partition 0              │
// │   ├── Partition 1 ──────────► Groups hashing to partition 1              │
// │   ├── ...                                                                │
// │   └── Partition 49 ─────────► Groups hashing to partition 49             │
// │                                                                          │
// │   Each partition leader is the COORDINATOR for groups in that partition. │
// │   Failover = standard partition leader election → coordinator moves.     │
// │                                                                          │
// └──────────────────────────────────────────────────────────────────────────┘
//
// WHY NOT JUST USE JSON?
// Binary format is chosen for:
//   - Efficiency: Coordinator writes on every offset commit (high volume)
//   - Compactness: Less disk I/O, faster replication
//   - Performance: No JSON parsing overhead
//   - Forward compatibility: Version byte allows evolution
//
// RECORD TYPES:
//   1. OffsetCommitRecord: Consumer offset commits
//   2. GroupMetadataRecord: Group membership/assignment state
//   3. InternalMemberMetadataRecord: Individual member info
//
// COMPARISON WITH KAFKA:
//   - Kafka's __consumer_offsets uses custom binary format
//   - 50 partitions by default (same as us)
//   - Compact cleanup policy (keep latest per key)
//   - Key = group+topic+partition, Value = offset
//
// =============================================================================

package broker

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"time"
)

// =============================================================================
// INTERNAL TOPIC NAMES
// =============================================================================

const (
	// ConsumerOffsetsTopicName is the internal topic for consumer group state.
	// Stores:
	//   - Offset commits (groupID → topic → partition → offset)
	//   - Group metadata (members, assignments, generation)
	ConsumerOffsetsTopicName = "__consumer_offsets"

	// DefaultOffsetsPartitionCount is the default number of partitions.
	// Kafka uses 50, we follow the same convention.
	// This spreads coordinator load across the cluster.
	DefaultOffsetsPartitionCount = 50

	// DefaultOffsetsReplicationFactor ensures coordinator state survives node failures.
	DefaultOffsetsReplicationFactor = 3
)

// =============================================================================
// RECORD TYPES
// =============================================================================
//
// Binary Record Format:
//
// ┌───────────────────────────────────────────────────────────────────────┐
// │                         RECORD HEADER (12 bytes)                      │
// ├─────────┬─────────┬─────────┬─────────────────────────────────────────┤
// │ Version │  Type   │ Flags   │              CRC32                      │
// │  (1B)   │  (1B)   │  (2B)   │              (4B)                       │
// ├─────────┴─────────┴─────────┴─────────────────────────────────────────┤
// │                         Key Length (4B)                               │
// ├───────────────────────────────────────────────────────────────────────┤
// │                         Key (variable)                                │
// ├───────────────────────────────────────────────────────────────────────┤
// │                         Value Length (4B)                             │
// ├───────────────────────────────────────────────────────────────────────┤
// │                         Value (variable)                              │
// └───────────────────────────────────────────────────────────────────────┘
//
// =============================================================================

// RecordType identifies the type of internal record.
type RecordType uint8

const (
	// RecordTypeOffsetCommit is an offset commit record.
	// Key: OffsetCommitKey, Value: OffsetCommitValue
	RecordTypeOffsetCommit RecordType = 1

	// RecordTypeGroupMetadata is a group metadata record.
	// Key: GroupMetadataKey, Value: GroupMetadataValue
	RecordTypeGroupMetadata RecordType = 2

	// RecordTypeInternalMemberMetadata is a member metadata record (rarely used alone).
	// Usually embedded in GroupMetadataValue.
	RecordTypeInternalMemberMetadata RecordType = 3

	// RecordTypeTombstone marks a deleted entry (for compaction).
	// Key: any key type, Value: empty (nil)
	RecordTypeTombstone RecordType = 4
)

// RecordVersion is the current binary format version.
// Increment when making breaking changes to the format.
const RecordVersion uint8 = 1

// =============================================================================
// OFFSET COMMIT KEY
// =============================================================================
//
// Binary Format:
// ┌─────────────────┬─────────────────┬────────────────────┬───────────────┐
// │ GroupLen (2B)   │ Group (var)     │ TopicLen (2B)      │ Topic (var)   │
// ├─────────────────┴─────────────────┴────────────────────┴───────────────┤
// │                              Partition (4B)                            │
// └────────────────────────────────────────────────────────────────────────┘
//
// This key format allows log compaction to keep only the latest offset
// for each group+topic+partition combination.
//
// =============================================================================

// OffsetCommitKey is the key for offset commit records.
type OffsetCommitKey struct {
	Group     string
	Topic     string
	Partition int32
}

// Encode serializes the key to bytes.
func (k *OffsetCommitKey) Encode() []byte {
	// Calculate size
	size := 2 + len(k.Group) + 2 + len(k.Topic) + 4

	buf := make([]byte, size)
	offset := 0

	// Group
	binary.BigEndian.PutUint16(buf[offset:], uint16(len(k.Group)))
	offset += 2
	copy(buf[offset:], k.Group)
	offset += len(k.Group)

	// Topic
	binary.BigEndian.PutUint16(buf[offset:], uint16(len(k.Topic)))
	offset += 2
	copy(buf[offset:], k.Topic)
	offset += len(k.Topic)

	// Partition
	binary.BigEndian.PutUint32(buf[offset:], uint32(k.Partition))

	return buf
}

// DecodeOffsetCommitKey deserializes a key from bytes.
func DecodeOffsetCommitKey(data []byte) (*OffsetCommitKey, error) {
	if len(data) < 8 { // minimum: 2+0+2+0+4
		return nil, errors.New("offset commit key too short")
	}

	offset := 0
	key := &OffsetCommitKey{}

	// Group
	groupLen := binary.BigEndian.Uint16(data[offset:])
	offset += 2
	if offset+int(groupLen) > len(data) {
		return nil, errors.New("invalid group length in offset commit key")
	}
	key.Group = string(data[offset : offset+int(groupLen)])
	offset += int(groupLen)

	// Topic
	if offset+2 > len(data) {
		return nil, errors.New("offset commit key truncated at topic length")
	}
	topicLen := binary.BigEndian.Uint16(data[offset:])
	offset += 2
	if offset+int(topicLen) > len(data) {
		return nil, errors.New("invalid topic length in offset commit key")
	}
	key.Topic = string(data[offset : offset+int(topicLen)])
	offset += int(topicLen)

	// Partition
	if offset+4 > len(data) {
		return nil, errors.New("offset commit key truncated at partition")
	}
	key.Partition = int32(binary.BigEndian.Uint32(data[offset:]))

	return key, nil
}

// =============================================================================
// OFFSET COMMIT VALUE
// =============================================================================
//
// Binary Format:
// ┌─────────────────┬────────────────────┬────────────────────────────────┐
// │   Offset (8B)   │  Timestamp (8B)    │  MetadataLen (2B)              │
// ├─────────────────┴────────────────────┴────────────────────────────────┤
// │                          Metadata (var)                               │
// ├───────────────────────────────────────────────────────────────────────┤
// │                       ExpireTimestamp (8B)                            │
// └───────────────────────────────────────────────────────────────────────┘
//
// =============================================================================

// OffsetCommitValue is the value for offset commit records.
type OffsetCommitValue struct {
	Offset          int64
	Timestamp       int64 // Unix millis when committed
	Metadata        string
	ExpireTimestamp int64 // Unix millis when this commit expires (0 = never)
}

// Encode serializes the value to bytes.
func (v *OffsetCommitValue) Encode() []byte {
	// Calculate size: 8 + 8 + 2 + len(metadata) + 8
	size := 26 + len(v.Metadata)

	buf := make([]byte, size)
	offset := 0

	// Offset
	binary.BigEndian.PutUint64(buf[offset:], uint64(v.Offset))
	offset += 8

	// Timestamp
	binary.BigEndian.PutUint64(buf[offset:], uint64(v.Timestamp))
	offset += 8

	// Metadata
	binary.BigEndian.PutUint16(buf[offset:], uint16(len(v.Metadata)))
	offset += 2
	copy(buf[offset:], v.Metadata)
	offset += len(v.Metadata)

	// ExpireTimestamp
	binary.BigEndian.PutUint64(buf[offset:], uint64(v.ExpireTimestamp))

	return buf
}

// DecodeOffsetCommitValue deserializes a value from bytes.
func DecodeOffsetCommitValue(data []byte) (*OffsetCommitValue, error) {
	if len(data) < 26 { // minimum: 8+8+2+0+8
		return nil, errors.New("offset commit value too short")
	}

	offset := 0
	value := &OffsetCommitValue{}

	// Offset
	value.Offset = int64(binary.BigEndian.Uint64(data[offset:]))
	offset += 8

	// Timestamp
	value.Timestamp = int64(binary.BigEndian.Uint64(data[offset:]))
	offset += 8

	// Metadata
	metadataLen := binary.BigEndian.Uint16(data[offset:])
	offset += 2
	if offset+int(metadataLen) > len(data) {
		return nil, errors.New("invalid metadata length in offset commit value")
	}
	value.Metadata = string(data[offset : offset+int(metadataLen)])
	offset += int(metadataLen)

	// ExpireTimestamp
	if offset+8 > len(data) {
		return nil, errors.New("offset commit value truncated at expire timestamp")
	}
	value.ExpireTimestamp = int64(binary.BigEndian.Uint64(data[offset:]))

	return value, nil
}

// =============================================================================
// GROUP METADATA KEY
// =============================================================================
//
// Binary Format:
// ┌─────────────────┬─────────────────────────────────────────────────────┐
// │ GroupLen (2B)   │              Group (variable)                       │
// └─────────────────┴─────────────────────────────────────────────────────┘
//
// Simple key: just the group name. One record per group.
//
// =============================================================================

// GroupMetadataKey is the key for group metadata records.
type GroupMetadataKey struct {
	Group string
}

// Encode serializes the key to bytes.
func (k *GroupMetadataKey) Encode() []byte {
	buf := make([]byte, 2+len(k.Group))
	binary.BigEndian.PutUint16(buf[0:], uint16(len(k.Group)))
	copy(buf[2:], k.Group)
	return buf
}

// DecodeGroupMetadataKey deserializes a key from bytes.
func DecodeGroupMetadataKey(data []byte) (*GroupMetadataKey, error) {
	if len(data) < 2 {
		return nil, errors.New("group metadata key too short")
	}

	groupLen := binary.BigEndian.Uint16(data[0:])
	if int(groupLen)+2 > len(data) {
		return nil, errors.New("invalid group length in group metadata key")
	}

	return &GroupMetadataKey{
		Group: string(data[2 : 2+groupLen]),
	}, nil
}

// =============================================================================
// GROUP METADATA VALUE
// =============================================================================
//
// Binary Format:
// ┌─────────────────┬─────────────────┬─────────────────┬─────────────────┐
// │ State (1B)      │ ProtocolLen(2B) │ Protocol (var)  │ LeaderLen (2B)  │
// ├─────────────────┴─────────────────┴─────────────────┴─────────────────┤
// │                          Leader (variable)                            │
// ├───────────────────────────────────────────────────────────────────────┤
// │                          Generation (4B)                              │
// ├───────────────────────────────────────────────────────────────────────┤
// │                       MemberCount (4B)                                │
// ├───────────────────────────────────────────────────────────────────────┤
// │                  [InternalMemberMetadata] * MemberCount               │
// └───────────────────────────────────────────────────────────────────────┘
//
// GROUP STATE:
//   We use the existing GroupState from consumer_group.go:
//   - GroupStateEmpty (0)
//   - GroupStateStable (1)
//   - GroupStateRebalancing (2)
//   - GroupStateDead (3)
//
// =============================================================================

// GroupMetadataValue is the value for group metadata records.
type GroupMetadataValue struct {
	State      GroupState // Uses existing GroupState from consumer_group.go
	Protocol   string     // e.g., "range", "roundrobin", "cooperative-sticky"
	Leader     string     // memberID of the leader
	Generation int32
	Members    []InternalMemberMetadata
}

// InternalMemberMetadata represents a single member in the group.
// Note: Named differently from existing InternalMemberMetadata to avoid conflicts.
type InternalMemberMetadata struct {
	MemberID         string
	ClientID         string
	ClientHost       string
	SessionTimeout   int32  // milliseconds
	RebalanceTimeout int32  // milliseconds
	Assignment       []byte // Serialized partition assignment
	Metadata         []byte // Protocol-specific metadata
}

// Encode serializes the value to bytes.
func (v *GroupMetadataValue) Encode() []byte {
	// Calculate size
	size := 1 + 2 + len(v.Protocol) + 2 + len(v.Leader) + 4 + 4

	// Add member sizes
	for _, m := range v.Members {
		size += v.memberSize(&m)
	}

	buf := make([]byte, size)
	offset := 0

	// State
	buf[offset] = byte(v.State)
	offset++

	// Protocol
	binary.BigEndian.PutUint16(buf[offset:], uint16(len(v.Protocol)))
	offset += 2
	copy(buf[offset:], v.Protocol)
	offset += len(v.Protocol)

	// Leader
	binary.BigEndian.PutUint16(buf[offset:], uint16(len(v.Leader)))
	offset += 2
	copy(buf[offset:], v.Leader)
	offset += len(v.Leader)

	// Generation
	binary.BigEndian.PutUint32(buf[offset:], uint32(v.Generation))
	offset += 4

	// Member count
	binary.BigEndian.PutUint32(buf[offset:], uint32(len(v.Members)))
	offset += 4

	// Members
	for _, m := range v.Members {
		offset = v.encodeMember(buf, offset, &m)
	}

	return buf
}

func (v *GroupMetadataValue) memberSize(m *InternalMemberMetadata) int {
	return 2 + len(m.MemberID) + 2 + len(m.ClientID) + 2 + len(m.ClientHost) +
		4 + 4 + 4 + len(m.Assignment) + 4 + len(m.Metadata)
}

func (v *GroupMetadataValue) encodeMember(buf []byte, offset int, m *InternalMemberMetadata) int {
	// MemberID
	binary.BigEndian.PutUint16(buf[offset:], uint16(len(m.MemberID)))
	offset += 2
	copy(buf[offset:], m.MemberID)
	offset += len(m.MemberID)

	// ClientID
	binary.BigEndian.PutUint16(buf[offset:], uint16(len(m.ClientID)))
	offset += 2
	copy(buf[offset:], m.ClientID)
	offset += len(m.ClientID)

	// ClientHost
	binary.BigEndian.PutUint16(buf[offset:], uint16(len(m.ClientHost)))
	offset += 2
	copy(buf[offset:], m.ClientHost)
	offset += len(m.ClientHost)

	// SessionTimeout
	binary.BigEndian.PutUint32(buf[offset:], uint32(m.SessionTimeout))
	offset += 4

	// RebalanceTimeout
	binary.BigEndian.PutUint32(buf[offset:], uint32(m.RebalanceTimeout))
	offset += 4

	// Assignment
	binary.BigEndian.PutUint32(buf[offset:], uint32(len(m.Assignment)))
	offset += 4
	copy(buf[offset:], m.Assignment)
	offset += len(m.Assignment)

	// Metadata
	binary.BigEndian.PutUint32(buf[offset:], uint32(len(m.Metadata)))
	offset += 4
	copy(buf[offset:], m.Metadata)
	offset += len(m.Metadata)

	return offset
}

// DecodeGroupMetadataValue deserializes a value from bytes.
func DecodeGroupMetadataValue(data []byte) (*GroupMetadataValue, error) {
	if len(data) < 11 { // minimum: 1+2+0+2+0+4+4
		return nil, errors.New("group metadata value too short")
	}

	offset := 0
	value := &GroupMetadataValue{}

	// State
	value.State = GroupState(data[offset])
	offset++

	// Protocol
	protocolLen := binary.BigEndian.Uint16(data[offset:])
	offset += 2
	if offset+int(protocolLen) > len(data) {
		return nil, errors.New("invalid protocol length")
	}
	value.Protocol = string(data[offset : offset+int(protocolLen)])
	offset += int(protocolLen)

	// Leader
	if offset+2 > len(data) {
		return nil, errors.New("truncated at leader length")
	}
	leaderLen := binary.BigEndian.Uint16(data[offset:])
	offset += 2
	if offset+int(leaderLen) > len(data) {
		return nil, errors.New("invalid leader length")
	}
	value.Leader = string(data[offset : offset+int(leaderLen)])
	offset += int(leaderLen)

	// Generation
	if offset+4 > len(data) {
		return nil, errors.New("truncated at generation")
	}
	value.Generation = int32(binary.BigEndian.Uint32(data[offset:]))
	offset += 4

	// Member count
	if offset+4 > len(data) {
		return nil, errors.New("truncated at member count")
	}
	memberCount := binary.BigEndian.Uint32(data[offset:])
	offset += 4

	// Members
	value.Members = make([]InternalMemberMetadata, memberCount)
	for i := uint32(0); i < memberCount; i++ {
		member, newOffset, err := decodeMember(data, offset)
		if err != nil {
			return nil, fmt.Errorf("failed to decode member %d: %w", i, err)
		}
		value.Members[i] = *member
		offset = newOffset
	}

	return value, nil
}

func decodeMember(data []byte, offset int) (*InternalMemberMetadata, int, error) {
	member := &InternalMemberMetadata{}

	// MemberID
	if offset+2 > len(data) {
		return nil, 0, errors.New("truncated at member ID length")
	}
	memberIDLen := binary.BigEndian.Uint16(data[offset:])
	offset += 2
	if offset+int(memberIDLen) > len(data) {
		return nil, 0, errors.New("invalid member ID length")
	}
	member.MemberID = string(data[offset : offset+int(memberIDLen)])
	offset += int(memberIDLen)

	// ClientID
	if offset+2 > len(data) {
		return nil, 0, errors.New("truncated at client ID length")
	}
	clientIDLen := binary.BigEndian.Uint16(data[offset:])
	offset += 2
	if offset+int(clientIDLen) > len(data) {
		return nil, 0, errors.New("invalid client ID length")
	}
	member.ClientID = string(data[offset : offset+int(clientIDLen)])
	offset += int(clientIDLen)

	// ClientHost
	if offset+2 > len(data) {
		return nil, 0, errors.New("truncated at client host length")
	}
	clientHostLen := binary.BigEndian.Uint16(data[offset:])
	offset += 2
	if offset+int(clientHostLen) > len(data) {
		return nil, 0, errors.New("invalid client host length")
	}
	member.ClientHost = string(data[offset : offset+int(clientHostLen)])
	offset += int(clientHostLen)

	// SessionTimeout
	if offset+4 > len(data) {
		return nil, 0, errors.New("truncated at session timeout")
	}
	member.SessionTimeout = int32(binary.BigEndian.Uint32(data[offset:]))
	offset += 4

	// RebalanceTimeout
	if offset+4 > len(data) {
		return nil, 0, errors.New("truncated at rebalance timeout")
	}
	member.RebalanceTimeout = int32(binary.BigEndian.Uint32(data[offset:]))
	offset += 4

	// Assignment
	if offset+4 > len(data) {
		return nil, 0, errors.New("truncated at assignment length")
	}
	assignmentLen := binary.BigEndian.Uint32(data[offset:])
	offset += 4
	if offset+int(assignmentLen) > len(data) {
		return nil, 0, errors.New("invalid assignment length")
	}
	member.Assignment = make([]byte, assignmentLen)
	copy(member.Assignment, data[offset:offset+int(assignmentLen)])
	offset += int(assignmentLen)

	// Metadata
	if offset+4 > len(data) {
		return nil, 0, errors.New("truncated at metadata length")
	}
	metadataLen := binary.BigEndian.Uint32(data[offset:])
	offset += 4
	if offset+int(metadataLen) > len(data) {
		return nil, 0, errors.New("invalid metadata length")
	}
	member.Metadata = make([]byte, metadataLen)
	copy(member.Metadata, data[offset:offset+int(metadataLen)])
	offset += int(metadataLen)

	return member, offset, nil
}

// =============================================================================
// INTERNAL RECORD
// =============================================================================
//
// InternalRecord wraps key+value with type and CRC for the internal topic.
//
// Wire Format:
// ┌─────────┬─────────┬─────────┬─────────┬─────────┬─────────┬─────────┐
// │ Version │  Type   │ Flags   │  CRC32  │ KeyLen  │  Key    │ ValLen  │
// │  (1B)   │  (1B)   │  (2B)   │  (4B)   │  (4B)   │  (var)  │  (4B)   │
// ├─────────┴─────────┴─────────┴─────────┴─────────┴─────────┴─────────┤
// │                           Value (var)                               │
// └─────────────────────────────────────────────────────────────────────┘
//
// =============================================================================

// InternalRecord is a record in an internal topic.
type InternalRecord struct {
	Version   uint8
	Type      RecordType
	Flags     uint16
	Key       []byte
	Value     []byte
	Timestamp time.Time
}

// HeaderSize is the fixed header size before key/value.
const InternalRecordHeaderSize = 16 // 1+1+2+4+4+4

// Encode serializes the record to bytes with CRC.
func (r *InternalRecord) Encode() []byte {
	size := InternalRecordHeaderSize + len(r.Key) + len(r.Value)
	buf := make([]byte, size)

	offset := 0

	// Version
	buf[offset] = r.Version
	offset++

	// Type
	buf[offset] = byte(r.Type)
	offset++

	// Flags
	binary.BigEndian.PutUint16(buf[offset:], r.Flags)
	offset += 2

	// CRC placeholder (will fill after key/value)
	crcOffset := offset
	offset += 4

	// Key length
	binary.BigEndian.PutUint32(buf[offset:], uint32(len(r.Key)))
	offset += 4

	// Key
	copy(buf[offset:], r.Key)
	offset += len(r.Key)

	// Value length
	binary.BigEndian.PutUint32(buf[offset:], uint32(len(r.Value)))
	offset += 4

	// Value
	copy(buf[offset:], r.Value)

	// Calculate CRC over everything except CRC field itself
	// CRC covers: version(1) + type(1) + flags(2) + keylen(4) + key + vallen(4) + value
	crcData := make([]byte, 0, size-4)
	crcData = append(crcData, buf[0:crcOffset]...)  // version, type, flags
	crcData = append(crcData, buf[crcOffset+4:]...) // keylen, key, vallen, value
	crc := crc32.ChecksumIEEE(crcData)
	binary.BigEndian.PutUint32(buf[crcOffset:], crc)

	return buf
}

// DecodeInternalRecord deserializes a record from bytes.
func DecodeInternalRecord(data []byte) (*InternalRecord, error) {
	if len(data) < InternalRecordHeaderSize {
		return nil, errors.New("internal record too short")
	}

	offset := 0
	record := &InternalRecord{}

	// Version
	record.Version = data[offset]
	offset++

	// Type
	record.Type = RecordType(data[offset])
	offset++

	// Flags
	record.Flags = binary.BigEndian.Uint16(data[offset:])
	offset += 2

	// CRC
	storedCRC := binary.BigEndian.Uint32(data[offset:])
	crcOffset := offset
	offset += 4

	// Key length
	if offset+4 > len(data) {
		return nil, errors.New("truncated at key length")
	}
	keyLen := binary.BigEndian.Uint32(data[offset:])
	offset += 4

	// Key
	if offset+int(keyLen) > len(data) {
		return nil, errors.New("invalid key length")
	}
	record.Key = make([]byte, keyLen)
	copy(record.Key, data[offset:offset+int(keyLen)])
	offset += int(keyLen)

	// Value length
	if offset+4 > len(data) {
		return nil, errors.New("truncated at value length")
	}
	valLen := binary.BigEndian.Uint32(data[offset:])
	offset += 4

	// Value
	if offset+int(valLen) > len(data) {
		return nil, errors.New("invalid value length")
	}
	record.Value = make([]byte, valLen)
	copy(record.Value, data[offset:offset+int(valLen)])

	// Verify CRC
	crcData := make([]byte, 0, len(data)-4)
	crcData = append(crcData, data[0:crcOffset]...)
	crcData = append(crcData, data[crcOffset+4:]...)
	calculatedCRC := crc32.ChecksumIEEE(crcData)
	if storedCRC != calculatedCRC {
		return nil, fmt.Errorf("CRC mismatch: stored=%d, calculated=%d", storedCRC, calculatedCRC)
	}

	return record, nil
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

// NewOffsetCommitRecord creates an internal record for an offset commit.
func NewOffsetCommitRecord(group, topic string, partition int32, offset int64, metadata string) *InternalRecord {
	key := &OffsetCommitKey{
		Group:     group,
		Topic:     topic,
		Partition: partition,
	}

	value := &OffsetCommitValue{
		Offset:          offset,
		Timestamp:       time.Now().UnixMilli(),
		Metadata:        metadata,
		ExpireTimestamp: 0, // never expire
	}

	return &InternalRecord{
		Version:   RecordVersion,
		Type:      RecordTypeOffsetCommit,
		Flags:     0,
		Key:       key.Encode(),
		Value:     value.Encode(),
		Timestamp: time.Now(),
	}
}

// NewGroupMetadataRecord creates an internal record for group metadata.
func NewGroupMetadataRecord(group string, state GroupState, protocol, leader string, generation int32, members []InternalMemberMetadata) *InternalRecord {
	key := &GroupMetadataKey{
		Group: group,
	}

	value := &GroupMetadataValue{
		State:      state,
		Protocol:   protocol,
		Leader:     leader,
		Generation: generation,
		Members:    members,
	}

	return &InternalRecord{
		Version:   RecordVersion,
		Type:      RecordTypeGroupMetadata,
		Flags:     0,
		Key:       key.Encode(),
		Value:     value.Encode(),
		Timestamp: time.Now(),
	}
}

// NewTombstoneRecord creates a deletion marker for log compaction.
func NewTombstoneRecord(recordType RecordType, key []byte) *InternalRecord {
	return &InternalRecord{
		Version:   RecordVersion,
		Type:      RecordTypeTombstone,
		Flags:     uint16(recordType), // Store original type in flags for debugging
		Key:       key,
		Value:     nil,
		Timestamp: time.Now(),
	}
}

// =============================================================================
// PARTITION ROUTING
// =============================================================================
//
// HASH-BASED GROUP-TO-PARTITION MAPPING
//
// To find which partition (and thus which coordinator) manages a group:
//   partition = murmur3(groupID) % numOffsetsPartitions
//
// This is deterministic: any node can compute the mapping.
// The leader of that partition IS the coordinator for that group.
//
// EXAMPLE (50 partitions):
//   hash("order-processors") % 50 = 17 → Partition 17's leader is coordinator
//   hash("log-consumers") % 50 = 17    → Same coordinator (same partition)
//   hash("analytics-group") % 50 = 42  → Partition 42's leader is coordinator
//
// =============================================================================

// GroupToPartition calculates which partition a group maps to.
// Uses murmur3 hash for consistency with partitioner.
func GroupToPartition(groupID string, numPartitions int) int {
	// Use DefaultPartitioner.Partition for consistency with message routing
	return DefaultPartitioner.Partition([]byte(groupID), nil, numPartitions)
}
