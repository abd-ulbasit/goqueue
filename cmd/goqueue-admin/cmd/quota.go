// =============================================================================
// QUOTA COMMAND - QUOTA MANAGEMENT
// =============================================================================
//
// SUBCOMMANDS:
//   goqueue-admin quota get    Get tenant quotas
//   goqueue-admin quota set    Update tenant quotas
//   goqueue-admin quota reset  Reset quotas to defaults
//
// =============================================================================

package cmd

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

// =============================================================================
// QUOTA PARENT COMMAND
// =============================================================================

var quotaCmd = &cobra.Command{
	Use:   "quota",
	Short: "Manage tenant quotas",
	Long: `Manage resource quotas for tenants.

Quotas control resource usage per tenant:
  • Rate limits (publish rate, consume rate)
  • Storage limits (max bytes, max topics)
  • Size limits (max message size)
  • Connection limits (max connections)

Examples:
  # View current quotas
  goqueue-admin quota get acme

  # Update quotas
  goqueue-admin quota set acme --publish-rate 10000 --max-topics 50`,
}

// =============================================================================
// GET QUOTAS
// =============================================================================

var quotaGetCmd = &cobra.Command{
	Use:   "get <tenant-id>",
	Short: "Get tenant quotas",
	Long: `Display current quota configuration for a tenant.

Examples:
  goqueue-admin quota get acme
  goqueue-admin quota get acme -o json`,
	Args: cobra.ExactArgs(1),
	RunE: runQuotaGet,
}

func init() {
	quotaCmd.AddCommand(quotaGetCmd)
}

func runQuotaGet(cmd *cobra.Command, args []string) error {
	tenantID := args[0]
	ctx, cancel := getContext()
	defer cancel()

	var result map[string]interface{}
	err := client.DoRequest(ctx, "GET", "/admin/tenants/"+tenantID+"/quotas", nil, &result)
	if err != nil {
		return handleError(err)
	}

	if outputFlag == "json" {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(result)
	}

	fmt.Printf("Quotas for tenant '%s':\n\n", tenantID)

	fmt.Println("Rate Limits:")
	if v, ok := result["publish_rate_limit"]; ok {
		fmt.Printf("  Publish Rate:       %v msg/sec\n", v)
	}
	if v, ok := result["consume_rate_limit"]; ok {
		fmt.Printf("  Consume Rate:       %v msg/sec\n", v)
	}
	if v, ok := result["publish_bytes_rate_limit"]; ok {
		fmt.Printf("  Publish Bytes Rate: %s/sec\n", formatBytes(int64(v.(float64))))
	}
	if v, ok := result["consume_bytes_rate_limit"]; ok {
		fmt.Printf("  Consume Bytes Rate: %s/sec\n", formatBytes(int64(v.(float64))))
	}

	fmt.Println("\nStorage Limits:")
	if v, ok := result["max_storage_bytes"]; ok {
		fmt.Printf("  Max Storage:        %s\n", formatBytes(int64(v.(float64))))
	}
	if v, ok := result["max_topics"]; ok {
		fmt.Printf("  Max Topics:         %v\n", v)
	}
	if v, ok := result["max_partitions_total"]; ok {
		fmt.Printf("  Max Partitions:     %v\n", v)
	}

	fmt.Println("\nSize Limits:")
	if v, ok := result["max_message_size_bytes"]; ok {
		fmt.Printf("  Max Message Size:   %s\n", formatBytes(int64(v.(float64))))
	}
	if v, ok := result["max_retention_ms"]; ok {
		ms := int64(v.(float64))
		fmt.Printf("  Max Retention:      %v hours\n", ms/3600000)
	}
	if v, ok := result["max_delay_ms"]; ok {
		ms := int64(v.(float64))
		fmt.Printf("  Max Delay:          %v hours\n", ms/3600000)
	}

	fmt.Println("\nConnection Limits:")
	if v, ok := result["max_connections"]; ok {
		fmt.Printf("  Max Connections:    %v\n", v)
	}
	if v, ok := result["max_consumer_groups"]; ok {
		fmt.Printf("  Max Consumer Groups: %v\n", v)
	}
	if v, ok := result["consumers_per_group"]; ok {
		fmt.Printf("  Consumers Per Group: %v\n", v)
	}

	return nil
}

// =============================================================================
// SET QUOTAS
// =============================================================================

var (
	setPublishRate      int64
	setConsumeRate      int64
	setPublishBytesRate string
	setConsumeBytesRate string
	setMaxStorage       string
	setMaxTopics        int64
	setMaxPartitions    int64
	setMaxMsgSize       string
	setMaxRetention     string
	setMaxDelay         string
	setMaxConnections   int64
	setMaxGroups        int64
	setConsumersPerGrp  int64
)

var quotaSetCmd = &cobra.Command{
	Use:   "set <tenant-id>",
	Short: "Update tenant quotas",
	Long: `Update quota configuration for a tenant.

All flags are optional - only specified quotas will be updated.
Use 0 to set unlimited (remove quota).

Examples:
  # Update rate limits
  goqueue-admin quota set acme --publish-rate 10000

  # Update storage limits
  goqueue-admin quota set acme --max-storage 100GB --max-topics 50

  # Update multiple quotas
  goqueue-admin quota set startup \
    --publish-rate 1000 \
    --max-storage 10GB \
    --max-message-size 1MB`,
	Args: cobra.ExactArgs(1),
	RunE: runQuotaSet,
}

func init() {
	quotaCmd.AddCommand(quotaSetCmd)

	quotaSetCmd.Flags().Int64Var(&setPublishRate, "publish-rate", -1, "Max publish rate (msg/sec), 0 for unlimited")
	quotaSetCmd.Flags().Int64Var(&setConsumeRate, "consume-rate", -1, "Max consume rate (msg/sec), 0 for unlimited")
	quotaSetCmd.Flags().StringVar(&setPublishBytesRate, "publish-bytes-rate", "", "Max publish bytes rate (e.g., 10MB)")
	quotaSetCmd.Flags().StringVar(&setConsumeBytesRate, "consume-bytes-rate", "", "Max consume bytes rate (e.g., 50MB)")
	quotaSetCmd.Flags().StringVar(&setMaxStorage, "max-storage", "", "Max storage (e.g., 100GB)")
	quotaSetCmd.Flags().Int64Var(&setMaxTopics, "max-topics", -1, "Max number of topics")
	quotaSetCmd.Flags().Int64Var(&setMaxPartitions, "max-partitions", -1, "Max total partitions")
	quotaSetCmd.Flags().StringVar(&setMaxMsgSize, "max-message-size", "", "Max message size (e.g., 1MB)")
	quotaSetCmd.Flags().StringVar(&setMaxRetention, "max-retention", "", "Max retention period (e.g., 168h)")
	quotaSetCmd.Flags().StringVar(&setMaxDelay, "max-delay", "", "Max delay for delayed messages (e.g., 24h)")
	quotaSetCmd.Flags().Int64Var(&setMaxConnections, "max-connections", -1, "Max concurrent connections")
	quotaSetCmd.Flags().Int64Var(&setMaxGroups, "max-consumer-groups", -1, "Max consumer groups")
	quotaSetCmd.Flags().Int64Var(&setConsumersPerGrp, "consumers-per-group", -1, "Max consumers per group")
}

func runQuotaSet(cmd *cobra.Command, args []string) error {
	tenantID := args[0]
	ctx, cancel := getContext()
	defer cancel()

	quotas := make(map[string]interface{})

	if setPublishRate >= 0 {
		quotas["publish_rate_limit"] = setPublishRate
	}
	if setConsumeRate >= 0 {
		quotas["consume_rate_limit"] = setConsumeRate
	}
	if setPublishBytesRate != "" {
		bytes, err := parseSize(setPublishBytesRate)
		if err != nil {
			return fmt.Errorf("invalid publish-bytes-rate: %w", err)
		}
		quotas["publish_bytes_rate_limit"] = bytes
	}
	if setConsumeBytesRate != "" {
		bytes, err := parseSize(setConsumeBytesRate)
		if err != nil {
			return fmt.Errorf("invalid consume-bytes-rate: %w", err)
		}
		quotas["consume_bytes_rate_limit"] = bytes
	}
	if setMaxStorage != "" {
		bytes, err := parseSize(setMaxStorage)
		if err != nil {
			return fmt.Errorf("invalid max-storage: %w", err)
		}
		quotas["max_storage_bytes"] = bytes
	}
	if setMaxTopics >= 0 {
		quotas["max_topics"] = setMaxTopics
	}
	if setMaxPartitions >= 0 {
		quotas["max_partitions_total"] = setMaxPartitions
	}
	if setMaxMsgSize != "" {
		bytes, err := parseSize(setMaxMsgSize)
		if err != nil {
			return fmt.Errorf("invalid max-message-size: %w", err)
		}
		quotas["max_message_size_bytes"] = bytes
	}
	if setMaxRetention != "" {
		ms, err := parseDurationMs(setMaxRetention)
		if err != nil {
			return fmt.Errorf("invalid max-retention: %w", err)
		}
		quotas["max_retention_ms"] = ms
	}
	if setMaxDelay != "" {
		ms, err := parseDurationMs(setMaxDelay)
		if err != nil {
			return fmt.Errorf("invalid max-delay: %w", err)
		}
		quotas["max_delay_ms"] = ms
	}
	if setMaxConnections >= 0 {
		quotas["max_connections"] = setMaxConnections
	}
	if setMaxGroups >= 0 {
		quotas["max_consumer_groups"] = setMaxGroups
	}
	if setConsumersPerGrp >= 0 {
		quotas["consumers_per_group"] = setConsumersPerGrp
	}

	if len(quotas) == 0 {
		return fmt.Errorf("no quotas specified, use --help to see available options")
	}

	var result map[string]interface{}
	err := client.DoRequest(ctx, "PUT", "/admin/tenants/"+tenantID+"/quotas", quotas, &result)
	if err != nil {
		return handleError(err)
	}

	fmt.Printf("✓ Quotas updated for tenant '%s'\n", tenantID)
	fmt.Printf("  Updated %d quota setting(s)\n", len(quotas))
	return nil
}

// =============================================================================
// HELPERS
// =============================================================================

// parseDurationMs parses a duration string like "24h" to milliseconds.
func parseDurationMs(s string) (int64, error) {
	var value float64
	var unit string

	_, err := fmt.Sscanf(s, "%f%s", &value, &unit)
	if err != nil {
		return 0, fmt.Errorf("invalid duration format: %s", s)
	}

	var multiplier int64
	switch unit {
	case "ms":
		multiplier = 1
	case "s":
		multiplier = 1000
	case "m":
		multiplier = 60 * 1000
	case "h":
		multiplier = 60 * 60 * 1000
	case "d":
		multiplier = 24 * 60 * 60 * 1000
	default:
		return 0, fmt.Errorf("unknown duration unit: %s (use ms, s, m, h, d)", unit)
	}

	return int64(value * float64(multiplier)), nil
}
