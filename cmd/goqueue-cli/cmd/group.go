// =============================================================================
// GROUP COMMANDS - MANAGE CONSUMER GROUPS
// =============================================================================
//
// WHAT IS THIS?
// Commands for managing consumer groups in goqueue.
//
// COMMANDS:
//   goqueue group list                 List all consumer groups
//   goqueue group describe <id>        Show group details
//   goqueue group delete <id>          Delete a consumer group
//   goqueue group offsets <id>         Show committed offsets
//   goqueue group reset-offsets <id>   Reset offsets
//
// EXAMPLES:
//   goqueue group list
//   goqueue group describe order-processors
//   goqueue group offsets order-processors
//   goqueue group reset-offsets order-processors --topic orders --to-earliest
//
// =============================================================================

package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	"goqueue/internal/cli"
)

// =============================================================================
// GROUP COMMAND (PARENT)
// =============================================================================

var groupCmd = &cobra.Command{
	Use:   "group",
	Short: "Manage consumer groups",
	Long: `Manage consumer groups in the goqueue cluster.

Consumer groups enable multiple consumers to share the work of processing
messages from a topic. Each partition is assigned to exactly one consumer
within a group.

Examples:
  goqueue group list                            # List all groups
  goqueue group describe order-processors       # Show group details
  goqueue group offsets order-processors        # Show committed offsets
  goqueue group delete old-group                # Delete a group`,
}

func init() {
	groupCmd.AddCommand(groupListCmd)
	groupCmd.AddCommand(groupDescribeCmd)
	groupCmd.AddCommand(groupDeleteCmd)
	groupCmd.AddCommand(groupOffsetsCmd)
	groupCmd.AddCommand(groupResetOffsetsCmd)
}

// =============================================================================
// GROUP LIST
// =============================================================================

var groupListCmd = &cobra.Command{
	Use:   "list",
	Short: "List all consumer groups",
	Long: `List all consumer groups in the goqueue cluster.

Examples:
  goqueue group list              # List groups (table format)
  goqueue group list -o json      # List groups (JSON format)`,
	RunE: runGroupList,
}

func runGroupList(cmd *cobra.Command, args []string) error {
	ctx, cancel := getContext()
	defer cancel()

	resp, err := client.ListGroups(ctx)
	if err != nil {
		return handleError(err)
	}

	return formatter.FormatGroups(resp.Groups)
}

// =============================================================================
// GROUP DESCRIBE
// =============================================================================

var groupDescribeCmd = &cobra.Command{
	Use:   "describe <group-id>",
	Short: "Show consumer group details",
	Long: `Show detailed information about a consumer group.

Arguments:
  group-id    The ID of the consumer group

Output includes:
  - Group state and generation
  - Topics subscribed
  - Members and their partition assignments
  - Last heartbeat times

Examples:
  goqueue group describe order-processors
  goqueue group describe order-processors -o json`,
	Args: cobra.ExactArgs(1),
	RunE: runGroupDescribe,
}

func runGroupDescribe(cmd *cobra.Command, args []string) error {
	ctx, cancel := getContext()
	defer cancel()

	groupID := args[0]

	resp, err := client.DescribeGroup(ctx, groupID)
	if err != nil {
		return handleError(err)
	}

	return formatter.FormatGroupInfo(resp)
}

// =============================================================================
// GROUP DELETE
// =============================================================================

var groupDeleteForce bool

var groupDeleteCmd = &cobra.Command{
	Use:   "delete <group-id>",
	Short: "Delete a consumer group",
	Long: `Delete a consumer group.

WARNING: This will remove all committed offsets for this group.
If the group has active members, the deletion may fail or cause issues.

Arguments:
  group-id    The ID of the consumer group to delete

Flags:
  -f, --force    Skip confirmation prompt

Examples:
  goqueue group delete old-group
  goqueue group delete old-group -f`,
	Args: cobra.ExactArgs(1),
	RunE: runGroupDelete,
}

func init() {
	groupDeleteCmd.Flags().BoolVarP(&groupDeleteForce, "force", "f", false,
		"Skip confirmation prompt")
}

func runGroupDelete(cmd *cobra.Command, args []string) error {
	ctx, cancel := getContext()
	defer cancel()

	groupID := args[0]

	// Confirmation (unless --force)
	if !groupDeleteForce {
		cli.PrintInfo("Are you sure you want to delete consumer group %q?", groupID)
		cli.PrintInfo("This will remove all committed offsets. Type 'yes' to confirm: ")
		var confirm string
		if _, err := fmt.Scanln(&confirm); err != nil || confirm != "yes" {
			cli.PrintInfo("Aborted.")
			return nil
		}
	}

	resp, err := client.DeleteGroup(ctx, groupID)
	if err != nil {
		return handleError(err)
	}

	if outputFlag == "table" {
		cli.PrintSuccess("Consumer group %q deleted", resp.GroupID)
		return nil
	}

	return formatter.Format(resp)
}

// =============================================================================
// GROUP OFFSETS
// =============================================================================

var groupOffsetsCmd = &cobra.Command{
	Use:   "offsets <group-id>",
	Short: "Show committed offsets for a consumer group",
	Long: `Show committed offsets for a consumer group.

Arguments:
  group-id    The ID of the consumer group

Output includes:
  - Topic and partition
  - Current committed offset
  - Consumer lag (if available)

Examples:
  goqueue group offsets order-processors
  goqueue group offsets order-processors -o json`,
	Args: cobra.ExactArgs(1),
	RunE: runGroupOffsets,
}

func runGroupOffsets(cmd *cobra.Command, args []string) error {
	ctx, cancel := getContext()
	defer cancel()

	groupID := args[0]

	resp, err := client.GetOffsets(ctx, groupID)
	if err != nil {
		return handleError(err)
	}

	return formatter.FormatOffsets(resp)
}

// =============================================================================
// GROUP RESET-OFFSETS
// =============================================================================

var (
	resetOffsetsTopic       string
	resetOffsetsPartition   int
	resetOffsetsToEarliest  bool
	resetOffsetsToLatest    bool
	resetOffsetsToTimestamp string
	resetOffsetsDryRun      bool
)

var groupResetOffsetsCmd = &cobra.Command{
	Use:   "reset-offsets <group-id>",
	Short: "Reset offsets for a consumer group",
	Long: `Reset committed offsets for a consumer group.

WARNING: Resetting offsets will cause messages to be reprocessed (if reset to 
earlier) or skipped (if reset to later).

Arguments:
  group-id    The ID of the consumer group

Flags:
  --topic           Topic to reset offsets for (required)
  --partition       Partition to reset (-1 for all)
  --to-earliest     Reset to earliest offset
  --to-latest       Reset to latest offset
  --to-timestamp    Reset to offset at timestamp (RFC3339)
  --dry-run         Show what would be reset without applying

Examples:
  # Reset all partitions of 'orders' topic to earliest
  goqueue group reset-offsets order-processors --topic orders --to-earliest

  # Reset partition 0 to latest
  goqueue group reset-offsets order-processors --topic orders -p 0 --to-latest

  # Reset to a specific timestamp
  goqueue group reset-offsets order-processors --topic orders \
    --to-timestamp 2024-01-15T09:00:00Z`,
	Args: cobra.ExactArgs(1),
	RunE: runGroupResetOffsets,
}

func init() {
	groupResetOffsetsCmd.Flags().StringVar(&resetOffsetsTopic, "topic", "",
		"Topic to reset offsets for (required)")
	groupResetOffsetsCmd.Flags().IntVarP(&resetOffsetsPartition, "partition", "p", -1,
		"Partition to reset (-1 for all)")
	groupResetOffsetsCmd.Flags().BoolVar(&resetOffsetsToEarliest, "to-earliest", false,
		"Reset to earliest offset")
	groupResetOffsetsCmd.Flags().BoolVar(&resetOffsetsToLatest, "to-latest", false,
		"Reset to latest offset")
	groupResetOffsetsCmd.Flags().StringVar(&resetOffsetsToTimestamp, "to-timestamp", "",
		"Reset to offset at timestamp (RFC3339)")
	groupResetOffsetsCmd.Flags().BoolVar(&resetOffsetsDryRun, "dry-run", false,
		"Show what would be reset without applying")

	groupResetOffsetsCmd.MarkFlagRequired("topic")
}

func runGroupResetOffsets(cmd *cobra.Command, args []string) error {
	ctx, cancel := getContext()
	defer cancel()

	groupID := args[0]

	// Validate strategy
	var strategy string
	switch {
	case resetOffsetsToEarliest:
		strategy = "earliest"
	case resetOffsetsToLatest:
		strategy = "latest"
	case resetOffsetsToTimestamp != "":
		strategy = "timestamp"
	default:
		cli.PrintError("must specify one of --to-earliest, --to-latest, or --to-timestamp")
		return cmd.Usage()
	}

	req := cli.ResetOffsetsRequest{
		Topic:     resetOffsetsTopic,
		Partition: resetOffsetsPartition,
		Strategy:  strategy,
		Timestamp: resetOffsetsToTimestamp,
	}

	if resetOffsetsDryRun {
		cli.PrintInfo("[DRY RUN] Would reset offsets for group %q:", groupID)
		cli.PrintInfo("  Topic:     %s", req.Topic)
		cli.PrintInfo("  Partition: %d", req.Partition)
		cli.PrintInfo("  Strategy:  %s", req.Strategy)
		if req.Timestamp != "" {
			cli.PrintInfo("  Timestamp: %s", req.Timestamp)
		}
		return nil
	}

	resp, err := client.ResetOffsets(ctx, groupID, req)
	if err != nil {
		return handleError(err)
	}

	if outputFlag == "table" {
		cli.PrintSuccess("Reset %d offset(s) for group %q", resp.ResetCount, resp.GroupID)
		return nil
	}

	return formatter.Format(resp)
}
