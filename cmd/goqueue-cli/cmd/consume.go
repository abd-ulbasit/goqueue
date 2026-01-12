// =============================================================================
// CONSUME COMMAND - READ MESSAGES
// =============================================================================
//
// WHAT IS THIS?
// Command for consuming messages from topics.
//
// USAGE:
//   goqueue consume <topic> [flags]
//
// FLAGS:
//   -p, --partition   Partition to consume from (default: 0)
//   -o, --offset      Starting offset (default: 0)
//   -n, --limit       Maximum messages to fetch (default: 10)
//   -f, --follow      Follow mode (like tail -f) - continuously poll
//   --from-beginning  Start from earliest offset
//   --from-latest     Start from latest offset
//
// EXAMPLES:
//   goqueue consume orders                    # Consume from partition 0
//   goqueue consume orders -p 1 -n 100        # Consume 100 from partition 1
//   goqueue consume orders -f                 # Follow mode (continuous)
//   goqueue consume orders --from-beginning   # Start from earliest
//
// =============================================================================

package cmd

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"goqueue/internal/cli"
)

// =============================================================================
// CONSUME FLAGS
// =============================================================================

var (
	consumePartition     int
	consumeOffset        int64
	consumeLimit         int
	consumeFollow        bool
	consumeFromBeginning bool
	consumeFromLatest    bool
)

// =============================================================================
// CONSUME COMMAND
// =============================================================================

var consumeCmd = &cobra.Command{
	Use:   "consume <topic>",
	Short: "Read messages from a topic",
	Long: `Read messages from a goqueue topic partition.

Arguments:
  topic    The name of the topic to consume from

Examples:
  # Basic consume from partition 0
  goqueue consume orders

  # Consume from partition 1, starting at offset 100
  goqueue consume orders -p 1 -o 100

  # Consume 50 messages
  goqueue consume orders -n 50

  # Follow mode (continuous, like tail -f)
  goqueue consume orders -f

  # Start from the beginning
  goqueue consume orders --from-beginning

  # Output as JSON for scripting
  goqueue consume orders -o json | jq '.messages[].value'`,
	Args: cobra.ExactArgs(1),
	RunE: runConsume,
}

func init() {
	consumeCmd.Flags().IntVarP(&consumePartition, "partition", "p", 0,
		"Partition to consume from")
	consumeCmd.Flags().Int64Var(&consumeOffset, "offset", 0,
		"Starting offset")
	consumeCmd.Flags().IntVarP(&consumeLimit, "limit", "n", 10,
		"Maximum messages to fetch")
	consumeCmd.Flags().BoolVarP(&consumeFollow, "follow", "f", false,
		"Follow mode (continuously poll for new messages)")
	consumeCmd.Flags().BoolVar(&consumeFromBeginning, "from-beginning", false,
		"Start from earliest offset")
	consumeCmd.Flags().BoolVar(&consumeFromLatest, "from-latest", false,
		"Start from latest offset")
}

func runConsume(cmd *cobra.Command, args []string) error {
	topic := args[0]

	// Resolve starting offset
	offset := consumeOffset
	if consumeFromBeginning {
		offset = 0
	} else if consumeFromLatest {
		// Get latest offset from topic info
		ctx, cancel := getContext()
		info, err := client.DescribeTopic(ctx, topic)
		cancel()
		if err != nil {
			return handleError(err)
		}
		partStr := fmt.Sprintf("%d", consumePartition)
		if offsets, ok := info.PartitionOffsets[partStr]; ok {
			offset = offsets["latest"]
		}
	}

	// Follow mode: continuously poll
	if consumeFollow {
		return runConsumeFollow(topic, consumePartition, offset)
	}

	// One-shot consume
	ctx, cancel := getContext()
	defer cancel()

	resp, err := client.Consume(ctx, topic, consumePartition, offset, consumeLimit)
	if err != nil {
		return handleError(err)
	}

	if len(resp.Messages) == 0 {
		if outputFlag == "table" {
			cli.PrintInfo("No messages found at offset %d", offset)
		}
		return nil
	}

	return formatter.FormatMessages(resp)
}

// runConsumeFollow implements follow mode (continuous polling).
func runConsumeFollow(topic string, partition int, startOffset int64) error {
	offset := startOffset
	pollInterval := 500 * time.Millisecond

	cli.PrintInfo("Following %s partition %d from offset %d (Ctrl+C to stop)...",
		topic, partition, offset)
	fmt.Println()

	for {
		ctx, cancel := getContext()
		resp, err := client.Consume(ctx, topic, partition, offset, 100)
		cancel()

		if err != nil {
			// On error, wait and retry
			cli.PrintError("Error: %v (retrying...)", err)
			time.Sleep(pollInterval)
			continue
		}

		// Print new messages
		for _, msg := range resp.Messages {
			if outputFlag == "json" {
				fmt.Printf(`{"offset":%d,"key":%q,"value":%q,"priority":%q}`+"\n",
					msg.Offset, msg.Key, msg.Value, msg.Priority)
			} else {
				// Simple inline format for follow mode
				keyPart := ""
				if msg.Key != "" {
					keyPart = fmt.Sprintf(" [%s]", msg.Key)
				}
				fmt.Printf("%d%s: %s\n", msg.Offset, keyPart, msg.Value)
			}
		}

		// Update offset
		if len(resp.Messages) > 0 {
			offset = resp.NextOffset
		}

		// Wait before next poll
		time.Sleep(pollInterval)
	}
}
