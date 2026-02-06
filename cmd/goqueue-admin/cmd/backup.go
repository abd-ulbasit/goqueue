// =============================================================================
// BACKUP COMMAND - BACKUP AND RESTORE OPERATIONS
// =============================================================================
//
// SUBCOMMANDS:
//   goqueue-admin backup create    Create a new backup
//   goqueue-admin backup list      List available backups
//   goqueue-admin backup get       Get backup details
//   goqueue-admin backup delete    Delete a backup
//   goqueue-admin backup restore   Restore from a backup
//
// KUBERNETES INTEGRATION:
//   This CLI is designed to work with the backup CronJob for scheduled backups.
//   For VolumeSnapshot-based backups, use kubectl directly:
//     kubectl create -f volumesnapshot.yaml
//
// =============================================================================

package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
)

// =============================================================================
// BACKUP PARENT COMMAND
// =============================================================================

var backupCmd = &cobra.Command{
	Use:   "backup",
	Short: "Backup and restore operations",
	Long: `Manage goqueue backups - create, list, and restore.

GoQueue uses a two-tier backup strategy:
  • VolumeSnapshot (CSI): Point-in-time snapshots of PVC data (message logs)
  • Metadata Backup (this CLI): Topics, consumer groups, schemas

This CLI handles metadata backups. For VolumeSnapshots, use:
  kubectl get volumesnapshots -n goqueue

Examples:
  # Create a full backup
  goqueue-admin backup create

  # Create backup with specific components
  goqueue-admin backup create --topics --offsets

  # List available backups
  goqueue-admin backup list

  # Restore from a backup
  goqueue-admin backup restore 2024-01-15-020000`,
}

// =============================================================================
// CREATE BACKUP
// =============================================================================

var (
	backupTopicsFlag  bool
	backupOffsetsFlag bool
	backupSchemasFlag bool
	backupConfigFlag  bool
	backupUploadFlag  bool
	backupBucketFlag  string
	backupPrefixFlag  string
)

var backupCreateCmd = &cobra.Command{
	Use:   "create",
	Short: "Create a new backup",
	Long: `Create a new backup of broker metadata.

This operation exports:
  • Topic metadata (names, partitions, retention settings)
  • Consumer group offsets
  • Schema registry contents
  • Broker configuration

The backup is stored on the broker's filesystem and can optionally be
uploaded to S3-compatible object storage.

Examples:
  # Create full backup
  goqueue-admin backup create

  # Backup only topics and offsets
  goqueue-admin backup create --topics --offsets

  # Create and upload to S3
  goqueue-admin backup create --upload --bucket my-bucket --prefix goqueue/backups`,
	RunE: runBackupCreate,
}

func init() {
	// Backup create flags
	backupCreateCmd.Flags().BoolVar(&backupTopicsFlag, "topics", true, "Include topics in backup")
	backupCreateCmd.Flags().BoolVar(&backupOffsetsFlag, "offsets", true, "Include consumer group offsets")
	backupCreateCmd.Flags().BoolVar(&backupSchemasFlag, "schemas", true, "Include schema registry")
	backupCreateCmd.Flags().BoolVar(&backupConfigFlag, "config", true, "Include broker configuration")
	backupCreateCmd.Flags().BoolVar(&backupUploadFlag, "upload", false, "Upload backup to object storage")
	backupCreateCmd.Flags().StringVar(&backupBucketFlag, "bucket", "", "S3 bucket name for upload")
	backupCreateCmd.Flags().StringVar(&backupPrefixFlag, "prefix", "backups", "S3 key prefix for upload")

	// Backup restore flags
	backupRestoreCmd.Flags().BoolVar(&restoreSkipExistingFlag, "skip-existing", false, "Skip resources that already exist")

	// Add subcommands
	backupCmd.AddCommand(backupCreateCmd)
	backupCmd.AddCommand(backupListCmd)
	backupCmd.AddCommand(backupGetCmd)
	backupCmd.AddCommand(backupDeleteCmd)
	backupCmd.AddCommand(backupRestoreCmd)
}

// BackupRequest is the request body for creating a backup.
type BackupRequest struct {
	IncludeTopics  bool `json:"include_topics"`
	IncludeOffsets bool `json:"include_offsets"`
	IncludeSchemas bool `json:"include_schemas"`
	IncludeConfig  bool `json:"include_config"`
}

// BackupResponse is the response from backup creation.
type BackupResponse struct {
	BackupID  string      `json:"backup_id"`
	Timestamp time.Time   `json:"timestamp"`
	BrokerID  string      `json:"broker_id"`
	ClusterID string      `json:"cluster_id,omitempty"`
	Stats     BackupStats `json:"stats"`
}

// BackupStats contains statistics about the backup.
type BackupStats struct {
	TopicCount         int    `json:"topic_count"`
	PartitionCount     int    `json:"partition_count"`
	TotalMessages      int64  `json:"total_messages"`
	TotalBytes         int64  `json:"total_bytes"`
	ConsumerGroupCount int    `json:"consumer_group_count"`
	SchemaCount        int    `json:"schema_count"`
	Duration           string `json:"duration_human"`
}

func runBackupCreate(cmd *cobra.Command, args []string) error {
	ctx, cancel := getContext()
	defer cancel()

	req := BackupRequest{
		IncludeTopics:  backupTopicsFlag,
		IncludeOffsets: backupOffsetsFlag,
		IncludeSchemas: backupSchemasFlag,
		IncludeConfig:  backupConfigFlag,
	}

	var resp BackupResponse
	if err := client.DoRequest(ctx, "POST", "/admin/backup", req, &resp); err != nil {
		return handleError(err)
	}

	// Handle output format
	switch outputFlag {
	case "json":
		data, _ := json.MarshalIndent(resp, "", "  ")
		fmt.Println(string(data))
	case "yaml":
		fmt.Printf("backup_id: %s\n", resp.BackupID)
		fmt.Printf("timestamp: %s\n", resp.Timestamp.Format(time.RFC3339))
		fmt.Printf("broker_id: %s\n", resp.BrokerID)
		fmt.Printf("topics: %d\n", resp.Stats.TopicCount)
		fmt.Printf("consumer_groups: %d\n", resp.Stats.ConsumerGroupCount)
		fmt.Printf("schemas: %d\n", resp.Stats.SchemaCount)
		fmt.Printf("duration: %s\n", resp.Stats.Duration)
	default:
		fmt.Printf("✓ Backup created successfully\n\n")
		fmt.Printf("  Backup ID:       %s\n", resp.BackupID)
		fmt.Printf("  Created:         %s\n", resp.Timestamp.Format(time.RFC3339))
		fmt.Printf("  Topics:          %d (%d partitions)\n", resp.Stats.TopicCount, resp.Stats.PartitionCount)
		fmt.Printf("  Consumer Groups: %d\n", resp.Stats.ConsumerGroupCount)
		fmt.Printf("  Schemas:         %d\n", resp.Stats.SchemaCount)
		fmt.Printf("  Duration:        %s\n", resp.Stats.Duration)
	}

	return nil
}

// =============================================================================
// LIST BACKUPS
// =============================================================================

var backupListCmd = &cobra.Command{
	Use:   "list",
	Short: "List available backups",
	Long: `List all available backups in the broker's backup directory.

Examples:
  # List all backups
  goqueue-admin backup list

  # List in JSON format
  goqueue-admin backup list -o json`,
	RunE: runBackupList,
}

// BackupListResponse is the response from listing backups.
type BackupListResponse struct {
	Backups []BackupSummary `json:"backups"`
}

// BackupSummary is a summary of a backup.
type BackupSummary struct {
	BackupID  string    `json:"backup_id"`
	Timestamp time.Time `json:"timestamp"`
	BrokerID  string    `json:"broker_id"`
	Topics    int       `json:"topics"`
	Groups    int       `json:"consumer_groups"`
	Schemas   int       `json:"schemas"`
}

func runBackupList(cmd *cobra.Command, args []string) error {
	ctx, cancel := getContext()
	defer cancel()

	var resp BackupListResponse
	if err := client.DoRequest(ctx, "GET", "/admin/backup", nil, &resp); err != nil {
		return handleError(err)
	}

	if len(resp.Backups) == 0 {
		fmt.Println("No backups found")
		return nil
	}

	// Handle output format
	switch outputFlag {
	case "json":
		data, _ := json.MarshalIndent(resp, "", "  ")
		fmt.Println(string(data))
	case "yaml":
		for _, b := range resp.Backups {
			fmt.Printf("- backup_id: %s\n", b.BackupID)
			fmt.Printf("  timestamp: %s\n", b.Timestamp.Format(time.RFC3339))
			fmt.Printf("  topics: %d\n", b.Topics)
			fmt.Printf("  groups: %d\n", b.Groups)
			fmt.Printf("  schemas: %d\n", b.Schemas)
		}
	default:
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "BACKUP ID\tTIMESTAMP\tTOPICS\tGROUPS\tSCHEMAS")
		for _, b := range resp.Backups {
			fmt.Fprintf(w, "%s\t%s\t%d\t%d\t%d\n",
				b.BackupID,
				b.Timestamp.Format("2006-01-02 15:04:05"),
				b.Topics,
				b.Groups,
				b.Schemas,
			)
		}
		w.Flush()
	}

	return nil
}

// =============================================================================
// GET BACKUP
// =============================================================================

var backupGetCmd = &cobra.Command{
	Use:   "get <backup-id>",
	Short: "Get backup details",
	Long: `Get detailed information about a specific backup.

Examples:
  # Get backup details
  goqueue-admin backup get 2024-01-15-020000`,
	Args: cobra.ExactArgs(1),
	RunE: runBackupGet,
}

// BackupDetail is the detailed response for a backup.
type BackupDetail struct {
	BackupID       string         `json:"backup_id"`
	Version        string         `json:"version"`
	Timestamp      time.Time      `json:"timestamp"`
	BrokerID       string         `json:"broker_id"`
	ClusterID      string         `json:"cluster_id,omitempty"`
	Topics         []TopicBackup  `json:"topics,omitempty"`
	ConsumerGroups []GroupBackup  `json:"consumer_groups,omitempty"`
	Schemas        []SchemaBackup `json:"schemas,omitempty"`
	Stats          BackupStats    `json:"stats"`
}

// TopicBackup represents a backed-up topic.
type TopicBackup struct {
	Name          string `json:"name"`
	NumPartitions int    `json:"num_partitions"`
}

// GroupBackup represents a backed-up consumer group.
type GroupBackup struct {
	GroupID string `json:"group_id"`
	State   string `json:"state"`
}

// SchemaBackup represents a backed-up schema.
type SchemaBackup struct {
	Subject string `json:"subject"`
	Version int    `json:"version"`
}

func runBackupGet(cmd *cobra.Command, args []string) error {
	ctx, cancel := getContext()
	defer cancel()

	backupID := args[0]
	path := fmt.Sprintf("/admin/backup/%s", backupID)

	var resp BackupDetail
	if err := client.DoRequest(ctx, "GET", path, nil, &resp); err != nil {
		return handleError(err)
	}

	// Handle output format
	switch outputFlag {
	case "json":
		data, _ := json.MarshalIndent(resp, "", "  ")
		fmt.Println(string(data))
	case "yaml":
		fmt.Printf("backup_id: %s\n", resp.BackupID)
		fmt.Printf("version: %s\n", resp.Version)
		fmt.Printf("timestamp: %s\n", resp.Timestamp.Format(time.RFC3339))
		fmt.Printf("broker_id: %s\n", resp.BrokerID)
		if len(resp.Topics) > 0 {
			fmt.Println("topics:")
			for _, t := range resp.Topics {
				fmt.Printf("  - name: %s\n", t.Name)
				fmt.Printf("    partitions: %d\n", t.NumPartitions)
			}
		}
	default:
		fmt.Printf("Backup: %s\n", resp.BackupID)
		fmt.Printf("─────────────────────────────────────\n")
		fmt.Printf("Version:          %s\n", resp.Version)
		fmt.Printf("Created:          %s\n", resp.Timestamp.Format(time.RFC3339))
		fmt.Printf("Broker:           %s\n", resp.BrokerID)
		if resp.ClusterID != "" {
			fmt.Printf("Cluster:          %s\n", resp.ClusterID)
		}
		fmt.Println()

		if len(resp.Topics) > 0 {
			fmt.Printf("Topics (%d):\n", len(resp.Topics))
			for _, t := range resp.Topics {
				fmt.Printf("  • %s (%d partitions)\n", t.Name, t.NumPartitions)
			}
			fmt.Println()
		}

		if len(resp.ConsumerGroups) > 0 {
			fmt.Printf("Consumer Groups (%d):\n", len(resp.ConsumerGroups))
			for _, g := range resp.ConsumerGroups {
				fmt.Printf("  • %s [%s]\n", g.GroupID, g.State)
			}
			fmt.Println()
		}

		if len(resp.Schemas) > 0 {
			fmt.Printf("Schemas (%d):\n", len(resp.Schemas))
			for _, s := range resp.Schemas {
				fmt.Printf("  • %s (v%d)\n", s.Subject, s.Version)
			}
		}
	}

	return nil
}

// =============================================================================
// DELETE BACKUP
// =============================================================================

var backupDeleteCmd = &cobra.Command{
	Use:   "delete <backup-id>",
	Short: "Delete a backup",
	Long: `Delete a backup from the broker's backup directory.

Examples:
  # Delete a specific backup
  goqueue-admin backup delete 2024-01-15-020000`,
	Args: cobra.ExactArgs(1),
	RunE: runBackupDelete,
}

func runBackupDelete(cmd *cobra.Command, args []string) error {
	ctx, cancel := getContext()
	defer cancel()

	backupID := args[0]
	path := fmt.Sprintf("/admin/backup/%s", backupID)

	if err := client.DoRequest(ctx, "DELETE", path, nil, nil); err != nil {
		return handleError(err)
	}

	fmt.Printf("✓ Backup %s deleted\n", backupID)
	return nil
}

// =============================================================================
// RESTORE BACKUP
// =============================================================================

var restoreSkipExistingFlag bool

var backupRestoreCmd = &cobra.Command{
	Use:   "restore <backup-id>",
	Short: "Restore from a backup",
	Long: `Restore broker state from a backup.

This restores:
  • Topics (creates if they don't exist)
  • Consumer group offsets
  • Schema registry contents

IMPORTANT:
  • This does NOT restore message data (that's handled by VolumeSnapshot)
  • Cannot restore over existing resources by default (use --skip-existing)
  • For full restore, first restore the VolumeSnapshot, then run this

Examples:
  # Restore from a backup
  goqueue-admin backup restore 2024-01-15-020000

  # Restore, skipping existing resources
  goqueue-admin backup restore 2024-01-15-020000 --skip-existing`,
	Args: cobra.ExactArgs(1),
	RunE: runBackupRestore,
}

// RestoreRequest is the request body for restoring a backup.
type RestoreRequest struct {
	SkipExisting bool `json:"skip_existing"`
}

// RestoreResponse is the response from restore operation.
type RestoreResponse struct {
	TopicsRestored  int `json:"topics_restored"`
	GroupsRestored  int `json:"groups_restored"`
	SchemasRestored int `json:"schemas_restored"`
	Skipped         int `json:"skipped"`
}

func runBackupRestore(cmd *cobra.Command, args []string) error {
	ctx, cancel := getContext()
	defer cancel()

	backupID := args[0]
	path := fmt.Sprintf("/admin/backup/%s/restore", backupID)

	req := RestoreRequest{
		SkipExisting: restoreSkipExistingFlag,
	}

	var resp RestoreResponse
	if err := client.DoRequest(ctx, "POST", path, req, &resp); err != nil {
		return handleError(err)
	}

	// Handle output format
	switch outputFlag {
	case "json":
		data, _ := json.MarshalIndent(resp, "", "  ")
		fmt.Println(string(data))
	case "yaml":
		fmt.Printf("topics_restored: %d\n", resp.TopicsRestored)
		fmt.Printf("groups_restored: %d\n", resp.GroupsRestored)
		fmt.Printf("schemas_restored: %d\n", resp.SchemasRestored)
		fmt.Printf("skipped: %d\n", resp.Skipped)
	default:
		fmt.Printf("✓ Restore completed\n\n")
		fmt.Printf("  Topics restored:         %d\n", resp.TopicsRestored)
		fmt.Printf("  Consumer groups restored: %d\n", resp.GroupsRestored)
		fmt.Printf("  Schemas restored:        %d\n", resp.SchemasRestored)
		if resp.Skipped > 0 {
			fmt.Printf("  Skipped (existing):      %d\n", resp.Skipped)
		}
	}

	return nil
}

func init() {
	// Add backup command to root
	rootCmd.AddCommand(backupCmd)
}
