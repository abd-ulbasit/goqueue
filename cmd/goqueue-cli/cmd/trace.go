// =============================================================================
// TRACE COMMANDS - QUERY MESSAGE TRACES
// =============================================================================
//
// WHAT IS THIS?
// Commands for querying message traces (M7 feature).
//
// COMMANDS:
//   goqueue trace list                List recent traces
//   goqueue trace get <trace-id>      Get trace by ID
//   goqueue trace search              Search traces with filters
//   goqueue trace stats               Show trace statistics
//
// EXAMPLES:
//   goqueue trace list -n 50
//   goqueue trace get abc123
//   goqueue trace search --topic orders --status error
//
// =============================================================================

package cmd

import (
	"github.com/spf13/cobra"

	"goqueue/internal/cli"
)

// =============================================================================
// TRACE COMMAND (PARENT)
// =============================================================================

var traceCmd = &cobra.Command{
	Use:   "trace",
	Short: "Query message traces",
	Long: `Query message traces for debugging and observability.

GoQueue tracks the lifecycle of messages as they flow through the system:
  - publish.received    Message received by broker
  - publish.persisted   Message written to log
  - consume.fetched     Message delivered to consumer
  - consume.acked       Message acknowledged
  - consume.nacked      Message negatively acknowledged (retry)
  - consume.rejected    Message rejected (sent to DLQ)

Examples:
  goqueue trace list                            # List recent traces
  goqueue trace get abc123-def456-...           # Get specific trace
  goqueue trace search --topic orders           # Search by topic
  goqueue trace stats                           # Show statistics`,
}

func init() {
	traceCmd.AddCommand(traceListCmd)
	traceCmd.AddCommand(traceGetCmd)
	traceCmd.AddCommand(traceSearchCmd)
	traceCmd.AddCommand(traceStatsCmd)
}

// =============================================================================
// TRACE LIST
// =============================================================================

var traceListLimit int

var traceListCmd = &cobra.Command{
	Use:   "list",
	Short: "List recent traces",
	Long: `List recent message traces.

Flags:
  -n, --limit    Maximum number of traces to return (default: 100)

Examples:
  goqueue trace list              # List recent traces
  goqueue trace list -n 50        # List last 50 traces
  goqueue trace list -o json      # Output as JSON`,
	RunE: runTraceList,
}

func init() {
	traceListCmd.Flags().IntVarP(&traceListLimit, "limit", "n", 100,
		"Maximum number of traces to return")
}

func runTraceList(cmd *cobra.Command, args []string) error {
	ctx, cancel := getContext()
	defer cancel()

	resp, err := client.ListTraces(ctx, traceListLimit)
	if err != nil {
		return handleError(err)
	}

	if len(resp.Traces) == 0 {
		if outputFlag == "table" {
			cli.PrintInfo("No traces found")
			return nil
		}
	}

	return formatter.FormatTraces(resp.Traces)
}

// =============================================================================
// TRACE GET
// =============================================================================

var traceGetCmd = &cobra.Command{
	Use:   "get <trace-id>",
	Short: "Get trace by ID",
	Long: `Get detailed information about a specific trace.

Arguments:
  trace-id    The trace ID (UUID format)

Output includes:
  - Trace metadata (topic, partition, offset)
  - Status and duration
  - Event timeline

Examples:
  goqueue trace get abc123-def456-ghi789
  goqueue trace get abc123-def456-ghi789 -o json`,
	Args: cobra.ExactArgs(1),
	RunE: runTraceGet,
}

func runTraceGet(cmd *cobra.Command, args []string) error {
	traceID := args[0]

	ctx, cancel := getContext()
	defer cancel()

	resp, err := client.GetTrace(ctx, traceID)
	if err != nil {
		return handleError(err)
	}

	return formatter.FormatTraceDetail(resp)
}

// =============================================================================
// TRACE SEARCH
// =============================================================================

var (
	traceSearchTopic     string
	traceSearchPartition int
	traceSearchGroup     string
	traceSearchStart     string
	traceSearchEnd       string
	traceSearchStatus    string
	traceSearchLimit     int
)

var traceSearchCmd = &cobra.Command{
	Use:   "search",
	Short: "Search traces with filters",
	Long: `Search for traces matching specific criteria.

Flags:
  --topic           Filter by topic name
  --partition       Filter by partition number
  --consumer-group  Filter by consumer group
  --start           Start time (RFC3339)
  --end             End time (RFC3339)
  --status          Filter by status (completed, error, pending)
  -n, --limit       Maximum results (default: 100)

Examples:
  # Find all traces for the orders topic
  goqueue trace search --topic orders
  # Find error traces from the last hour
  goqueue trace search --status error --start 2024-01-15T08:00:00Z
  # Find traces for a specific consumer group
  goqueue trace search --consumer-group order-processors`,
	RunE: runTraceSearch,
}

func init() {
	traceSearchCmd.Flags().StringVar(&traceSearchTopic, "topic", "",
		"Filter by topic name")
	traceSearchCmd.Flags().IntVar(&traceSearchPartition, "partition", -1,
		"Filter by partition number (-1 for all)")
	traceSearchCmd.Flags().StringVar(&traceSearchGroup, "consumer-group", "",
		"Filter by consumer group")
	traceSearchCmd.Flags().StringVar(&traceSearchStart, "start", "",
		"Start time (RFC3339)")
	traceSearchCmd.Flags().StringVar(&traceSearchEnd, "end", "",
		"End time (RFC3339)")
	traceSearchCmd.Flags().StringVar(&traceSearchStatus, "status", "",
		"Filter by status (completed, error, pending)")
	traceSearchCmd.Flags().IntVarP(&traceSearchLimit, "limit", "n", 100,
		"Maximum results")
}

func runTraceSearch(cmd *cobra.Command, args []string) error {
	ctx, cancel := getContext()
	defer cancel()

	req := cli.SearchTracesRequest{
		Topic:         traceSearchTopic,
		Partition:     traceSearchPartition,
		ConsumerGroup: traceSearchGroup,
		StartTime:     traceSearchStart,
		EndTime:       traceSearchEnd,
		Status:        traceSearchStatus,
		Limit:         traceSearchLimit,
	}

	resp, err := client.SearchTraces(ctx, req)
	if err != nil {
		return handleError(err)
	}

	if len(resp.Traces) == 0 {
		if outputFlag == "table" {
			cli.PrintInfo("No traces found matching criteria")
			return nil
		}
	}

	return formatter.FormatTraces(resp.Traces)
}

// =============================================================================
// TRACE STATS
// =============================================================================

var traceStatsCmd = &cobra.Command{
	Use:   "stats",
	Short: "Show trace statistics",
	Long: `Show statistics about message tracing.

Output includes:
  - Total traces collected
  - Active vs completed traces
  - Error rate
  - Traces by topic

Examples:
  goqueue trace stats
  goqueue trace stats -o json`,
	RunE: runTraceStats,
}

func runTraceStats(cmd *cobra.Command, args []string) error {
	ctx, cancel := getContext()
	defer cancel()

	stats, err := client.GetTraceStats(ctx)
	if err != nil {
		return handleError(err)
	}

	if outputFlag == "json" {
		return formatter.Format(stats)
	}
	if outputFlag == "yaml" {
		return formatter.Format(stats)
	}

	// Table format
	cli.PrintInfo("Trace Statistics:")
	cli.PrintInfo("  Total Traces:     %d", stats.TotalTraces)
	cli.PrintInfo("  Active:           %d", stats.ActiveTraces)
	cli.PrintInfo("  Completed:        %d", stats.CompletedTraces)
	cli.PrintInfo("  Errors:           %d", stats.ErrorTraces)

	if len(stats.ByTopic) > 0 {
		cli.PrintInfo("\nBy Topic:")
		for topic, count := range stats.ByTopic {
			cli.PrintInfo("  %s: %d", topic, count)
		}
	}

	return nil
}
