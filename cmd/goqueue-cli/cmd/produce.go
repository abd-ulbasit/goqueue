// =============================================================================
// PRODUCE COMMAND - PUBLISH MESSAGES
// =============================================================================
//
// WHAT IS THIS?
// Command for publishing messages to topics.
//
// USAGE:
//   goqueue produce <topic> [flags]
//
// FLAGS:
//   -m, --message     Message value (required unless using --file)
//   -k, --key         Message key (for partitioning)
//   -p, --partition   Target partition (override key-based routing)
//   -d, --delay       Delay duration (e.g., "30s", "1h")
//   --deliver-at      Deliver at timestamp (RFC3339)
//   --priority        Priority level (critical, high, normal, low, background)
//   -f, --file        Read messages from file (one per line, JSON format)
//
// EXAMPLES:
//   goqueue produce orders -m '{"id": 123}'
//   goqueue produce orders -m "hello" -k "user-123"
//   goqueue produce orders -m "later" -d 30m
//   goqueue produce orders -f messages.json
//
// =============================================================================

package cmd

import (
	"bufio"
	"encoding/json"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"goqueue/internal/cli"
)

// =============================================================================
// PRODUCE FLAGS
// =============================================================================

var (
	produceMessage   string
	produceKey       string
	producePartition int
	produceDelay     string
	produceDeliverAt string
	producePriority  string
	produceFile      string
)

// =============================================================================
// PRODUCE COMMAND
// =============================================================================

var produceCmd = &cobra.Command{
	Use:   "produce <topic>",
	Short: "Publish messages to a topic",
	Long: `Publish messages to a goqueue topic.

Arguments:
  topic    The name of the topic to publish to

Messages can be provided via:
  - --message flag (single message)
  - --file flag (multiple messages from file)

Examples:
  # Single message
  goqueue produce orders -m '{"order_id": 123}'

  # With key (for partitioning)
  goqueue produce orders -m "data" -k "user-123"

  # With delay
  goqueue produce orders -m "scheduled" -d 30m

  # With priority
  goqueue produce orders -m "urgent" --priority critical

  # From file (one JSON message per line)
  goqueue produce orders -f messages.jsonl`,
	Args: cobra.ExactArgs(1),
	RunE: runProduce,
}

func init() {
	produceCmd.Flags().StringVarP(&produceMessage, "message", "m", "",
		"Message value to publish")
	produceCmd.Flags().StringVarP(&produceKey, "key", "k", "",
		"Message key (determines partition)")
	produceCmd.Flags().IntVarP(&producePartition, "partition", "p", -1,
		"Target partition (overrides key-based routing)")
	produceCmd.Flags().StringVarP(&produceDelay, "delay", "d", "",
		"Delay duration (e.g., 30s, 1h, 24h)")
	produceCmd.Flags().StringVar(&produceDeliverAt, "deliver-at", "",
		"Deliver at timestamp (RFC3339)")
	produceCmd.Flags().StringVar(&producePriority, "priority", "",
		"Priority level (critical, high, normal, low, background)")
	produceCmd.Flags().StringVarP(&produceFile, "file", "f", "",
		"File containing messages (JSON Lines format)")
}

func runProduce(cmd *cobra.Command, args []string) error {
	ctx, cancel := getContext()
	defer cancel()

	topic := args[0]

	// Build messages
	var messages []cli.PublishMessage

	if produceFile != "" {
		// Read from file
		fileMessages, err := readMessagesFromFile(produceFile)
		if err != nil {
			return handleError(err)
		}
		messages = fileMessages
	} else if produceMessage != "" {
		// Single message from flag
		msg := cli.PublishMessage{
			Value:     produceMessage,
			Key:       produceKey,
			Delay:     produceDelay,
			DeliverAt: produceDeliverAt,
			Priority:  producePriority,
		}
		if producePartition >= 0 {
			msg.Partition = &producePartition
		}
		messages = []cli.PublishMessage{msg}
	} else {
		cli.PrintError("either --message or --file is required")
		return cmd.Usage()
	}

	// Publish
	resp, err := client.Publish(ctx, topic, messages)
	if err != nil {
		return handleError(err)
	}

	// Output
	if outputFlag == "table" {
		// Simple output for single message
		if len(resp.Results) == 1 {
			r := resp.Results[0]
			if r.Error != "" {
				cli.PrintError("Failed to publish: %s", r.Error)
			} else if r.Delayed {
				cli.PrintSuccess("Published to %s partition %d offset %d (delivers at %s)",
					topic, r.Partition, r.Offset, r.DeliverAt)
			} else {
				cli.PrintSuccess("Published to %s partition %d offset %d",
					topic, r.Partition, r.Offset)
			}
			return nil
		}
	}

	return formatter.FormatPublishResults(resp)
}

// readMessagesFromFile reads messages from a JSON Lines file.
func readMessagesFromFile(path string) ([]cli.PublishMessage, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var messages []cli.PublishMessage
	scanner := bufio.NewScanner(file)
	lineNum := 0

	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		var msg cli.PublishMessage
		if err := json.Unmarshal([]byte(line), &msg); err != nil {
			// Treat as plain text value
			msg = cli.PublishMessage{Value: line}
		}
		messages = append(messages, msg)
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return messages, nil
}
