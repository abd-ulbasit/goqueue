package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// =============================================================================
// BACKUP MANAGER (M23)
// =============================================================================
//
// ┌─────────────────────────────────────────────────────────────────────────────┐
// │ KUBERNETES-FIRST BACKUP STRATEGY                                            │
// │                                                                             │
// │ GoQueue backups operate at two levels:                                      │
// │                                                                             │
// │ 1. VOLUME LEVEL (VolumeSnapshots)                                           │
// │    - CSI driver creates point-in-time snapshots of PVC                      │
// │    - Fast: <1 minute for 100GB on EBS                                       │
// │    - Restore: Create PVC from VolumeSnapshot                                │
// │    - Handles: Message logs, indexes, WAL files                              │
// │                                                                             │
// │ 2. APPLICATION LEVEL (This code)                                            │
// │    - Exports metadata to JSON files                                         │
// │    - Topics, consumer groups, offsets, schemas                              │
// │    - Portable: Can restore to different cluster                             │
// │    - Useful for: Migration, cross-region DR                                 │
// │                                                                             │
// │ COMPARISON:                                                                 │
// │   - Kafka: MirrorMaker 2 for replication, no built-in backup                │
// │   - RabbitMQ: definitions.json export, Shovel for migration                 │
// │   - Redis: RDB snapshots + AOF persistence                                  │
// │   - goqueue: VolumeSnapshot + JSON metadata export                          │
// │                                                                             │
// │ BACKUP FLOW:                                                                │
// │   ┌──────────┐   quiesce   ┌───────────┐   snapshot   ┌──────────┐         │
// │   │  Broker  │────────────►│ Read-only │─────────────►│ Complete │         │
// │   └──────────┘             │   mode    │              └────┬─────┘         │
// │                            └───────────┘                   │               │
// │                                 │                          ▼               │
// │                                 │              ┌───────────────────┐       │
// │                             export             │ VolumeSnapshot +  │       │
// │                           metadata             │ metadata.json     │       │
// │                                 │              └───────────────────┘       │
// │                                 ▼                                          │
// │                          ┌───────────┐                                     │
// │                          │ S3/local  │                                     │
// │                          └───────────┘                                     │
// └─────────────────────────────────────────────────────────────────────────────┘
//
// =============================================================================

// BackupManager handles backup and restore operations for the broker.
//
// WHY SEPARATE MANAGER?
// - Backup operations are expensive (disk I/O, potential pauses)
// - Isolation prevents backup bugs from affecting message flow
// - Allows different backup strategies (full, incremental, differential)
//
// THREAD SAFETY:
// - BackupManager is safe to call from multiple goroutines
// - Uses broker's internal locks for consistency
// - Full backups acquire read locks on all topics briefly
type BackupManager struct {
	broker *Broker
	config BackupConfig
	logger *slog.Logger
}

// BackupConfig holds configuration for backup operations.
type BackupConfig struct {
	// BackupDir is where backup files are written
	// Default: {dataDir}/backups
	BackupDir string

	// IncludeTopics enables topic metadata backup
	IncludeTopics bool

	// IncludeOffsets enables consumer group offset backup
	IncludeOffsets bool

	// IncludeSchemas enables schema registry backup
	IncludeSchemas bool

	// IncludeConfig enables broker config backup
	IncludeConfig bool

	// RetentionCount is how many backups to keep
	RetentionCount int

	// CompressBackups enables gzip compression
	CompressBackups bool
}

// DefaultBackupConfig returns sensible backup defaults.
func DefaultBackupConfig() BackupConfig {
	return BackupConfig{
		IncludeTopics:   true,
		IncludeOffsets:  true,
		IncludeSchemas:  true,
		IncludeConfig:   true,
		RetentionCount:  7,
		CompressBackups: true,
	}
}

// NewBackupManager creates a backup manager for the broker.
func NewBackupManager(broker *Broker, config BackupConfig) *BackupManager {
	if config.BackupDir == "" {
		config.BackupDir = filepath.Join(broker.logsDir, "backups")
	}

	return &BackupManager{
		broker: broker,
		config: config,
		logger: slog.Default().With("component", "backup-manager"),
	}
}

// =============================================================================
// BACKUP METADATA STRUCTURES
// =============================================================================
//
// These structures define what gets exported during backup.
// They're designed to be:
//   1. Self-describing (includes version and timestamp)
//   2. Forward-compatible (new fields are optional)
//   3. Human-readable (JSON with meaningful names)
//
// =============================================================================

// BackupMetadata is the root structure for a backup.
type BackupMetadata struct {
	// Version is the backup format version
	Version string `json:"version"`

	// Timestamp is when the backup was created
	Timestamp time.Time `json:"timestamp"`

	// BrokerID identifies which broker created the backup
	BrokerID string `json:"broker_id"`

	// ClusterID identifies the cluster (for cross-cluster restore validation)
	ClusterID string `json:"cluster_id,omitempty"`

	// Topics contains topic metadata and statistics
	Topics []TopicBackup `json:"topics,omitempty"`

	// ConsumerGroups contains consumer group state
	ConsumerGroups []ConsumerGroupBackup `json:"consumer_groups,omitempty"`

	// Schemas contains registered schemas
	Schemas []SchemaBackup `json:"schemas,omitempty"`

	// Config contains broker configuration
	Config *ConfigBackup `json:"config,omitempty"`

	// Stats contains backup statistics
	Stats BackupStats `json:"stats"`
}

// TopicBackup represents a backed-up topic.
type TopicBackup struct {
	Name              string            `json:"name"`
	NumPartitions     int               `json:"num_partitions"`
	ReplicationFactor int               `json:"replication_factor"`
	RetentionHours    int               `json:"retention_hours"`
	CompactionEnabled bool              `json:"compaction_enabled"`
	CreatedAt         time.Time         `json:"created_at"`
	Metadata          map[string]string `json:"metadata,omitempty"`

	// Per-partition info
	Partitions []PartitionBackup `json:"partitions"`
}

// PartitionBackup represents a backed-up partition.
type PartitionBackup struct {
	ID           int   `json:"id"`
	FirstOffset  int64 `json:"first_offset"`
	LastOffset   int64 `json:"last_offset"`
	MessageCount int64 `json:"message_count"`
	SizeBytes    int64 `json:"size_bytes"`
}

// ConsumerGroupBackup represents a backed-up consumer group.
type ConsumerGroupBackup struct {
	GroupID   string                      `json:"group_id"`
	State     string                      `json:"state"`
	Offsets   map[string]PartitionOffsets `json:"offsets"`
	CreatedAt time.Time                   `json:"created_at"`
}

// PartitionOffsets maps partition ID to offset.
type PartitionOffsets map[int]int64

// SchemaBackup represents a backed-up schema.
type SchemaBackup struct {
	Subject       string `json:"subject"`
	Version       int    `json:"version"`
	ID            int64  `json:"id"`
	SchemaType    string `json:"schema_type"`
	Schema        string `json:"schema"`
	Compatibility string `json:"compatibility"`
}

// ConfigBackup represents backed-up configuration.
type ConfigBackup struct {
	BrokerConfig map[string]interface{} `json:"broker_config"`
	TopicConfigs map[string]interface{} `json:"topic_configs"`
}

// BackupStats contains statistics about the backup.
type BackupStats struct {
	TopicCount         int           `json:"topic_count"`
	PartitionCount     int           `json:"partition_count"`
	TotalMessages      int64         `json:"total_messages"`
	TotalBytes         int64         `json:"total_bytes"`
	ConsumerGroupCount int           `json:"consumer_group_count"`
	SchemaCount        int           `json:"schema_count"`
	Duration           time.Duration `json:"duration_ns"`
	DurationHuman      string        `json:"duration_human"`
}

// =============================================================================
// BACKUP OPERATIONS
// =============================================================================

// CreateBackup creates a full backup of the broker state.
//
// WHAT IT BACKS UP:
//   - Topic metadata (names, partitions, retention settings)
//   - Consumer group offsets (where each consumer left off)
//   - Schema registry (all registered schemas)
//   - Broker configuration
//
// WHAT IT DOES NOT BACK UP:
//   - Actual message data (that's handled by VolumeSnapshot)
//   - In-flight messages (they're pending, not committed)
//   - Runtime state (connections, in-progress transactions)
//
// FLOW:
//  1. Create backup directory with timestamp
//  2. Export topics (parallel for performance)
//  3. Export consumer groups
//  4. Export schemas
//  5. Export config
//  6. Write metadata.json
//  7. Cleanup old backups
func (bm *BackupManager) CreateBackup(ctx context.Context) (*BackupMetadata, error) {
	startTime := time.Now()

	// Create timestamped backup directory
	timestamp := time.Now().UTC().Format("2006-01-02-150405")
	backupDir := filepath.Join(bm.config.BackupDir, timestamp)
	if err := os.MkdirAll(backupDir, 0755); err != nil {
		return nil, fmt.Errorf("creating backup directory: %w", err)
	}

	// Initialize backup metadata
	metadata := &BackupMetadata{
		Version:   "1.0",
		Timestamp: time.Now().UTC(),
		BrokerID:  bm.broker.NodeID(),
	}

	// Get cluster ID if available (using node ID as proxy for now)
	if bm.broker.clusterCoordinator != nil {
		metadata.ClusterID = bm.broker.NodeID()
	}

	// Export topics
	if bm.config.IncludeTopics {
		topics, err := bm.exportTopics(ctx)
		if err != nil {
			return nil, fmt.Errorf("exporting topics: %w", err)
		}
		metadata.Topics = topics
		metadata.Stats.TopicCount = len(topics)
		for _, t := range topics {
			metadata.Stats.PartitionCount += len(t.Partitions)
			for _, p := range t.Partitions {
				metadata.Stats.TotalMessages += p.MessageCount
				metadata.Stats.TotalBytes += p.SizeBytes
			}
		}
	}

	// Export consumer groups
	if bm.config.IncludeOffsets {
		groups, err := bm.exportConsumerGroups(ctx)
		if err != nil {
			return nil, fmt.Errorf("exporting consumer groups: %w", err)
		}
		metadata.ConsumerGroups = groups
		metadata.Stats.ConsumerGroupCount = len(groups)
	}

	// Export schemas
	if bm.config.IncludeSchemas && bm.broker.schemaRegistry != nil {
		schemas, err := bm.exportSchemas(ctx)
		if err != nil {
			return nil, fmt.Errorf("exporting schemas: %w", err)
		}
		metadata.Schemas = schemas
		metadata.Stats.SchemaCount = len(schemas)
	}

	// Export config
	if bm.config.IncludeConfig {
		config := bm.exportConfig()
		metadata.Config = config
	}

	// Calculate duration
	metadata.Stats.Duration = time.Since(startTime)
	metadata.Stats.DurationHuman = metadata.Stats.Duration.String()

	// Write metadata.json
	metadataPath := filepath.Join(backupDir, "metadata.json")
	metadataBytes, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshaling metadata: %w", err)
	}
	if err := os.WriteFile(metadataPath, metadataBytes, 0644); err != nil {
		return nil, fmt.Errorf("writing metadata: %w", err)
	}

	// Cleanup old backups
	if err := bm.cleanupOldBackups(); err != nil {
		// Log but don't fail the backup
		bm.logger.Warn("failed to cleanup old backups", "error", err)
	}

	bm.logger.Info("backup created",
		"backup_id", timestamp,
		"topics", metadata.Stats.TopicCount,
		"consumer_groups", metadata.Stats.ConsumerGroupCount,
		"schemas", metadata.Stats.SchemaCount,
		"duration", metadata.Stats.DurationHuman,
	)

	return metadata, nil
}

// exportTopics exports all topic metadata.
func (bm *BackupManager) exportTopics(ctx context.Context) ([]TopicBackup, error) {
	bm.broker.mu.RLock()
	topics := make([]*Topic, 0, len(bm.broker.topics))
	for _, t := range bm.broker.topics {
		topics = append(topics, t)
	}
	bm.broker.mu.RUnlock()

	backups := make([]TopicBackup, 0, len(topics))
	for _, t := range topics {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		backup := TopicBackup{
			Name:              t.Name(),
			NumPartitions:     t.NumPartitions(),
			ReplicationFactor: 1, // Default until cluster replication is fully implemented
			Partitions:        make([]PartitionBackup, 0, t.NumPartitions()),
		}

		// Get partition info
		for i := 0; i < t.NumPartitions(); i++ {
			p, err := t.Partition(i)
			if err != nil || p == nil {
				continue
			}

			pBackup := PartitionBackup{
				ID:           i,
				FirstOffset:  p.EarliestOffset(),
				LastOffset:   p.LatestOffset(),
				MessageCount: p.MessageCount(),
				SizeBytes:    p.Size(),
			}
			backup.Partitions = append(backup.Partitions, pBackup)
		}

		backups = append(backups, backup)
	}

	return backups, nil
}

// exportConsumerGroups exports all consumer group state.
func (bm *BackupManager) exportConsumerGroups(ctx context.Context) ([]ConsumerGroupBackup, error) {
	if bm.broker.groupCoordinator == nil {
		return nil, nil
	}

	groups := bm.broker.groupCoordinator.ListGroups()
	backups := make([]ConsumerGroupBackup, 0, len(groups))

	for _, groupID := range groups {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		info, err := bm.broker.groupCoordinator.GetGroupInfo(groupID)
		if err != nil || info == nil {
			continue
		}

		backup := ConsumerGroupBackup{
			GroupID:   groupID,
			State:     info.State,
			Offsets:   make(map[string]PartitionOffsets),
			CreatedAt: info.CreatedAt,
		}

		// Get committed offsets from offset manager
		offsets := bm.broker.groupCoordinator.offsetManager
		if offsets != nil {
			groupOffsets, err := offsets.GetGroupOffsets(groupID)
			if err == nil && groupOffsets != nil {
				for topic, topicOffsets := range groupOffsets.Topics {
					partOffsets := make(PartitionOffsets)
					for partition, po := range topicOffsets.Partitions {
						partOffsets[partition] = po.Offset
					}
					backup.Offsets[topic] = partOffsets
				}
			}
		}

		backups = append(backups, backup)
	}

	return backups, nil
}

// exportSchemas exports all registered schemas.
func (bm *BackupManager) exportSchemas(ctx context.Context) ([]SchemaBackup, error) {
	if bm.broker.schemaRegistry == nil {
		return nil, nil
	}

	subjects := bm.broker.schemaRegistry.ListSubjects()
	backups := make([]SchemaBackup, 0)

	for _, subject := range subjects {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		versions, err := bm.broker.schemaRegistry.ListVersions(subject)
		if err != nil {
			continue
		}

		for _, version := range versions {
			schema, err := bm.broker.schemaRegistry.GetSchemaBySubjectVersion(subject, version)
			if err != nil {
				continue
			}

			backup := SchemaBackup{
				Subject:       subject,
				Version:       version,
				ID:            schema.ID,
				SchemaType:    schema.SchemaType,
				Schema:        schema.Schema,
				Compatibility: string(bm.broker.schemaRegistry.GetCompatibilityMode(subject)),
			}
			backups = append(backups, backup)
		}
	}

	return backups, nil
}

// exportConfig exports broker configuration.
func (bm *BackupManager) exportConfig() *ConfigBackup {
	config := &ConfigBackup{
		BrokerConfig: map[string]interface{}{
			"node_id":  bm.broker.config.NodeID,
			"data_dir": bm.broker.logsDir,
		},
		TopicConfigs: make(map[string]interface{}),
	}

	// Export per-topic configs
	bm.broker.mu.RLock()
	for name, topic := range bm.broker.topics {
		config.TopicConfigs[name] = map[string]interface{}{
			"num_partitions": topic.NumPartitions(),
		}
	}
	bm.broker.mu.RUnlock()

	return config
}

// cleanupOldBackups removes old backups beyond retention count.
func (bm *BackupManager) cleanupOldBackups() error {
	entries, err := os.ReadDir(bm.config.BackupDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	// Filter to directories only (backup folders)
	var backups []os.DirEntry
	for _, e := range entries {
		if e.IsDir() {
			backups = append(backups, e)
		}
	}

	// Sort by name (timestamp format ensures chronological order)
	sort.Slice(backups, func(i, j int) bool {
		return backups[i].Name() > backups[j].Name()
	})

	// Remove old backups
	for i := bm.config.RetentionCount; i < len(backups); i++ {
		path := filepath.Join(bm.config.BackupDir, backups[i].Name())
		if err := os.RemoveAll(path); err != nil {
			return fmt.Errorf("removing old backup %s: %w", path, err)
		}
		bm.logger.Info("removed old backup", "backup_id", backups[i].Name())
	}

	return nil
}

// =============================================================================
// RESTORE OPERATIONS
// =============================================================================

// RestoreFromBackup restores broker state from a backup.
//
// WHAT IT RESTORES:
//   - Topics (creates if they don't exist)
//   - Consumer group offsets
//   - Schemas
//
// WHAT IT DOES NOT RESTORE:
//   - Actual message data (that comes from VolumeSnapshot)
//   - Cannot restore over existing data (safety check)
//
// FLOW:
//  1. Validate backup metadata
//  2. Check for conflicts (existing topics, etc.)
//  3. Restore topics
//  4. Restore consumer groups
//  5. Restore schemas
func (bm *BackupManager) RestoreFromBackup(ctx context.Context, backupPath string, skipExisting bool) error {
	// Read metadata
	metadataPath := filepath.Join(backupPath, "metadata.json")
	metadataBytes, err := os.ReadFile(metadataPath)
	if err != nil {
		return fmt.Errorf("reading backup metadata: %w", err)
	}

	var metadata BackupMetadata
	if err := json.Unmarshal(metadataBytes, &metadata); err != nil {
		return fmt.Errorf("parsing backup metadata: %w", err)
	}

	// Validate version
	if metadata.Version != "1.0" {
		return fmt.Errorf("unsupported backup version: %s", metadata.Version)
	}

	bm.logger.Info("starting restore",
		"backup_id", filepath.Base(backupPath),
		"topics", len(metadata.Topics),
		"consumer_groups", len(metadata.ConsumerGroups),
		"schemas", len(metadata.Schemas),
	)

	// Restore topics
	for _, topic := range metadata.Topics {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Check if topic exists
		if bm.broker.TopicExists(topic.Name) {
			if skipExisting {
				bm.logger.Info("skipping existing topic", "topic", topic.Name)
				continue
			}
			return fmt.Errorf("topic %s already exists", topic.Name)
		}

		// Create topic using TopicConfig
		topicConfig := TopicConfig{
			Name:          topic.Name,
			NumPartitions: topic.NumPartitions,
		}
		if err := bm.broker.CreateTopic(topicConfig); err != nil {
			return fmt.Errorf("creating topic %s: %w", topic.Name, err)
		}
		bm.logger.Info("restored topic", "topic", topic.Name, "partitions", topic.NumPartitions)
	}

	// Restore consumer group offsets
	if bm.broker.groupCoordinator != nil {
		for _, group := range metadata.ConsumerGroups {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			for topic, partitionOffsets := range group.Offsets {
				for partition, offset := range partitionOffsets {
					// CommitOffset requires memberID - use empty string for restore
					if err := bm.broker.groupCoordinator.CommitOffset(
						group.GroupID,
						topic,
						partition,
						offset,
						"", // memberID empty for restore
					); err != nil {
						bm.logger.Warn("failed to restore offset",
							"group", group.GroupID,
							"topic", topic,
							"partition", partition,
							"error", err,
						)
					}
				}
			}
			bm.logger.Info("restored consumer group offsets", "group", group.GroupID)
		}
	}

	// Restore schemas
	if bm.broker.schemaRegistry != nil {
		for _, schema := range metadata.Schemas {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			// Set compatibility mode first
			if schema.Compatibility != "" {
				mode := ParseCompatibilityMode(schema.Compatibility)
				if err := bm.broker.schemaRegistry.SetCompatibilityMode(schema.Subject, mode); err != nil {
					bm.logger.Warn("failed to set schema compatibility",
						"subject", schema.Subject,
						"error", err,
					)
				}
			}

			// Register schema (RegisterSchema only takes subject and schema string)
			_, err := bm.broker.schemaRegistry.RegisterSchema(schema.Subject, schema.Schema)
			if err != nil {
				// Log but don't fail - schema might already exist
				bm.logger.Warn("failed to restore schema",
					"subject", schema.Subject,
					"error", err,
				)
			} else {
				bm.logger.Info("restored schema", "subject", schema.Subject, "version", schema.Version)
			}
		}
	}

	bm.logger.Info("restore complete",
		"backup_id", filepath.Base(backupPath),
		"topics_restored", len(metadata.Topics),
		"groups_restored", len(metadata.ConsumerGroups),
		"schemas_restored", len(metadata.Schemas),
	)

	return nil
}

// ListBackups returns available backups.
func (bm *BackupManager) ListBackups() ([]BackupMetadata, error) {
	entries, err := os.ReadDir(bm.config.BackupDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var backups []BackupMetadata
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}

		metadataPath := filepath.Join(bm.config.BackupDir, e.Name(), "metadata.json")
		metadataBytes, err := os.ReadFile(metadataPath)
		if err != nil {
			continue
		}

		var metadata BackupMetadata
		if err := json.Unmarshal(metadataBytes, &metadata); err != nil {
			continue
		}

		backups = append(backups, metadata)
	}

	// Sort by timestamp (newest first)
	sort.Slice(backups, func(i, j int) bool {
		return backups[i].Timestamp.After(backups[j].Timestamp)
	})

	return backups, nil
}

// GetBackup returns metadata for a specific backup.
func (bm *BackupManager) GetBackup(backupID string) (*BackupMetadata, error) {
	metadataPath := filepath.Join(bm.config.BackupDir, backupID, "metadata.json")
	metadataBytes, err := os.ReadFile(metadataPath)
	if err != nil {
		return nil, fmt.Errorf("reading backup metadata: %w", err)
	}

	var metadata BackupMetadata
	if err := json.Unmarshal(metadataBytes, &metadata); err != nil {
		return nil, fmt.Errorf("parsing backup metadata: %w", err)
	}

	return &metadata, nil
}

// DeleteBackup removes a backup.
func (bm *BackupManager) DeleteBackup(backupID string) error {
	backupPath := filepath.Join(bm.config.BackupDir, backupID)
	if err := os.RemoveAll(backupPath); err != nil {
		return fmt.Errorf("deleting backup: %w", err)
	}
	bm.logger.Info("deleted backup", "backup_id", backupID)
	return nil
}

// GetBackupPath returns the filesystem path for a backup.
func (bm *BackupManager) GetBackupPath(backupID string) string {
	return filepath.Join(bm.config.BackupDir, backupID)
}

// ExportToWriter writes backup to an io.Writer (for streaming to S3, etc.)
func (bm *BackupManager) ExportToWriter(ctx context.Context, w io.Writer) error {
	metadata, err := bm.CreateBackup(ctx)
	if err != nil {
		return err
	}

	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	return encoder.Encode(metadata)
}
