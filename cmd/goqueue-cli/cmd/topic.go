// =============================================================================
// TOPIC COMMANDS - MANAGE TOPICS
// =============================================================================
//
// WHAT IS THIS?
// Commands for managing topics in goqueue.
//
// COMMANDS:
//   goqueue topic list              List all topics
//   goqueue topic create <name>     Create a new topic
//   goqueue topic describe <name>   Show topic details
//   goqueue topic delete <name>     Delete a topic
//
// EXAMPLES:
//   goqueue topic create orders --partitions 6 --retention 168
//   goqueue topic list -o json
//   goqueue topic describe orders
//   goqueue topic delete old-topic
//
// =============================================================================

package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	"goqueue/internal/cli"
)

// =============================================================================
// TOPIC COMMAND (PARENT)
// =============================================================================

var topicCmd = &cobra.Command{
	Use:   "topic",
	Short: "Manage topics",
	Long: `Manage topics in the goqueue cluster.

Topics are the primary unit of organization in goqueue. Each topic can have
multiple partitions for parallel processing.

Examples:
  goqueue topic list                        # List all topics
  goqueue topic create orders -p 6          # Create topic with 6 partitions
  goqueue topic describe orders             # Show topic details
  goqueue topic delete old-topic            # Delete a topic`,
}

func init() {
	topicCmd.AddCommand(topicListCmd)
	topicCmd.AddCommand(topicCreateCmd)
	topicCmd.AddCommand(topicDescribeCmd)
	topicCmd.AddCommand(topicDeleteCmd)
}

// =============================================================================
// TOPIC LIST
// =============================================================================

var topicListCmd = &cobra.Command{
	Use:   "list",
	Short: "List all topics",
	Long: `List all topics in the goqueue cluster.

Examples:
  goqueue topic list              # List topics (table format)
  goqueue topic list -o json      # List topics (JSON format)
  goqueue topic list -o yaml      # List topics (YAML format)`,
	RunE: runTopicList,
}

func runTopicList(cmd *cobra.Command, args []string) error {
	ctx, cancel := getContext()
	defer cancel()

	resp, err := client.ListTopics(ctx)
	if err != nil {
		return handleError(err)
	}

	return formatter.FormatTopics(resp.Topics)
}

// =============================================================================
// TOPIC CREATE
// =============================================================================

var (
	topicCreatePartitions int
	topicCreateRetention  int
)

var topicCreateCmd = &cobra.Command{
	Use:   "create <name>",
	Short: "Create a new topic",
	Long: `Create a new topic with the specified configuration.

Arguments:
  name    The name of the topic to create

Flags:
  -p, --partitions   Number of partitions (default: 3)
  -r, --retention    Retention period in hours (default: 168 = 7 days)

Examples:
  goqueue topic create orders                    # Create with defaults
  goqueue topic create orders -p 6              # Create with 6 partitions
  goqueue topic create orders -p 12 -r 720      # 12 partitions, 30 days retention`,
	Args: cobra.ExactArgs(1),
	RunE: runTopicCreate,
}

func init() {
	topicCreateCmd.Flags().IntVarP(&topicCreatePartitions, "partitions", "p", 3,
		"Number of partitions")
	topicCreateCmd.Flags().IntVarP(&topicCreateRetention, "retention", "r", 168,
		"Retention period in hours")
}

func runTopicCreate(cmd *cobra.Command, args []string) error {
	ctx, cancel := getContext()
	defer cancel()

	name := args[0]

	resp, err := client.CreateTopic(ctx, name, topicCreatePartitions, topicCreateRetention)
	if err != nil {
		return handleError(err)
	}

	if outputFlag == "table" {
		cli.PrintSuccess("Topic %q created with %d partitions", resp.Name, resp.Partitions)
		return nil
	}

	return formatter.Format(resp)
}

// =============================================================================
// TOPIC DESCRIBE
// =============================================================================

var topicDescribeCmd = &cobra.Command{
	Use:   "describe <name>",
	Short: "Show topic details",
	Long: `Show detailed information about a topic.

Arguments:
  name    The name of the topic to describe

Output includes:
  - Topic name and partition count
  - Total messages and size
  - Per-partition offset ranges

Examples:
  goqueue topic describe orders              # Show topic info (table)
  goqueue topic describe orders -o json      # Show as JSON`,
	Args: cobra.ExactArgs(1),
	RunE: runTopicDescribe,
}

func runTopicDescribe(cmd *cobra.Command, args []string) error {
	ctx, cancel := getContext()
	defer cancel()

	name := args[0]

	resp, err := client.DescribeTopic(ctx, name)
	if err != nil {
		return handleError(err)
	}

	return formatter.FormatTopicInfo(resp)
}

// =============================================================================
// TOPIC DELETE
// =============================================================================

var topicDeleteForce bool

var topicDeleteCmd = &cobra.Command{
	Use:   "delete <name>",
	Short: "Delete a topic",
	Long: `Delete a topic and all its data.

WARNING: This operation is irreversible! All messages in the topic
will be permanently deleted.

Arguments:
  name    The name of the topic to delete

Flags:
  -f, --force    Skip confirmation prompt

Examples:
  goqueue topic delete old-topic             # Delete with confirmation
  goqueue topic delete old-topic -f          # Delete without confirmation`,
	Args: cobra.ExactArgs(1),
	RunE: runTopicDelete,
}

func init() {
	topicDeleteCmd.Flags().BoolVarP(&topicDeleteForce, "force", "f", false,
		"Skip confirmation prompt")
}

func runTopicDelete(cmd *cobra.Command, args []string) error {
	ctx, cancel := getContext()
	defer cancel()

	name := args[0]

	// Confirmation (unless --force)
	if !topicDeleteForce {
		cli.PrintInfo("Are you sure you want to delete topic %q? This cannot be undone.", name)
		cli.PrintInfo("Type 'yes' to confirm: ")
		var confirm string
		if _, err := fmt.Scanln(&confirm); err != nil || confirm != "yes" {
			cli.PrintInfo("Aborted.")
			return nil
		}
	}

	resp, err := client.DeleteTopic(ctx, name)
	if err != nil {
		return handleError(err)
	}

	if outputFlag == "table" {
		cli.PrintSuccess("Topic %q deleted", resp.Name)
		return nil
	}

	return formatter.Format(resp)
}
