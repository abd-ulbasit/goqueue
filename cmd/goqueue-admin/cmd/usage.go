// =============================================================================
// USAGE COMMAND - TENANT USAGE STATISTICS
// =============================================================================
//
// SUBCOMMANDS:
//   goqueue-admin usage show      Show tenant usage
//   goqueue-admin usage stats     Show tenant statistics (quotas vs usage)
//   goqueue-admin usage reset     Reset usage counters
//   goqueue-admin usage topics    List tenant's topics
//   goqueue-admin usage groups    List tenant's consumer groups
//
// =============================================================================

package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/spf13/cobra"
)

// =============================================================================
// USAGE PARENT COMMAND
// =============================================================================

var usageCmd = &cobra.Command{
	Use:   "usage",
	Short: "View tenant usage statistics",
	Long: `View and manage tenant resource usage.

Usage tracking includes:
  • Messages published/consumed
  • Bytes transferred
  • Storage used
  • Topic/partition counts

Examples:
  # View usage
  goqueue-admin usage show acme

  # View statistics (usage vs quotas)
  goqueue-admin usage stats acme`,
}

// =============================================================================
// SHOW USAGE
// =============================================================================

var usageShowCmd = &cobra.Command{
	Use:   "show <tenant-id>",
	Short: "Show tenant usage",
	Long: `Display resource usage for a tenant.

Examples:
  goqueue-admin usage show acme
  goqueue-admin usage show acme -o json`,
	Args: cobra.ExactArgs(1),
	RunE: runUsageShow,
}

func init() {
	usageCmd.AddCommand(usageShowCmd)
}

func runUsageShow(cmd *cobra.Command, args []string) error {
	tenantID := args[0]
	ctx, cancel := getContext()
	defer cancel()

	var result map[string]interface{}
	err := client.DoRequest(ctx, "GET", "/admin/tenants/"+tenantID+"/usage", nil, &result)
	if err != nil {
		return handleError(err)
	}

	if outputFlag == "json" {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(result)
	}

	fmt.Printf("Usage for tenant '%s':\n\n", tenantID)

	fmt.Println("Messages:")
	if v, ok := result["total_messages_published"]; ok {
		fmt.Printf("  Published: %v\n", v)
	}
	if v, ok := result["total_messages_consumed"]; ok {
		fmt.Printf("  Consumed:  %v\n", v)
	}

	fmt.Println("\nBytes Transferred:")
	if v, ok := result["total_bytes_published"]; ok {
		fmt.Printf("  Published: %s\n", formatBytes(int64(v.(float64))))
	}
	if v, ok := result["total_bytes_consumed"]; ok {
		fmt.Printf("  Consumed:  %s\n", formatBytes(int64(v.(float64))))
	}

	fmt.Println("\nResources:")
	if v, ok := result["storage_bytes"]; ok {
		fmt.Printf("  Storage Used:    %s\n", formatBytes(int64(v.(float64))))
	}
	if v, ok := result["topic_count"]; ok {
		fmt.Printf("  Topics:          %v\n", v)
	}
	if v, ok := result["partition_count"]; ok {
		fmt.Printf("  Partitions:      %v\n", v)
	}
	if v, ok := result["connection_count"]; ok {
		fmt.Printf("  Connections:     %v\n", v)
	}

	if v, ok := result["last_updated"]; ok {
		fmt.Printf("\nLast Updated: %v\n", v)
	}

	return nil
}

// =============================================================================
// SHOW STATS
// =============================================================================

var usageStatsCmd = &cobra.Command{
	Use:   "stats <tenant-id>",
	Short: "Show tenant statistics",
	Long: `Display usage statistics compared to quotas.

Shows percentage utilization for each quota type.

Examples:
  goqueue-admin usage stats acme`,
	Args: cobra.ExactArgs(1),
	RunE: runUsageStats,
}

func init() {
	usageCmd.AddCommand(usageStatsCmd)
}

func runUsageStats(cmd *cobra.Command, args []string) error {
	tenantID := args[0]
	ctx, cancel := getContext()
	defer cancel()

	var result map[string]interface{}
	err := client.DoRequest(ctx, "GET", "/admin/tenants/"+tenantID+"/stats", nil, &result)
	if err != nil {
		return handleError(err)
	}

	if outputFlag == "json" {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(result)
	}

	fmt.Printf("Statistics for tenant '%s':\n\n", tenantID)

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "RESOURCE\tUSAGE %\tSTATUS")
	fmt.Fprintln(w, "--------\t-------\t------")

	if v, ok := result["storage_usage_percent"].(float64); ok {
		status := getStatus(v)
		fmt.Fprintf(w, "Storage\t%.1f%%\t%s\n", v, status)
	}
	if v, ok := result["topic_usage_percent"].(float64); ok {
		status := getStatus(v)
		fmt.Fprintf(w, "Topics\t%.1f%%\t%s\n", v, status)
	}
	if v, ok := result["partition_usage_percent"].(float64); ok {
		status := getStatus(v)
		fmt.Fprintf(w, "Partitions\t%.1f%%\t%s\n", v, status)
	}
	if v, ok := result["connection_usage_percent"].(float64); ok {
		status := getStatus(v)
		fmt.Fprintf(w, "Connections\t%.1f%%\t%s\n", v, status)
	}
	w.Flush()

	if v, ok := result["quota_violations_total"].(float64); ok && v > 0 {
		fmt.Printf("\n⚠️  Quota Violations: %v\n", int64(v))
	}

	return nil
}

// getStatus returns a status string based on usage percentage.
func getStatus(percent float64) string {
	switch {
	case percent >= 90:
		return "⚠️  CRITICAL"
	case percent >= 75:
		return "⚠️  WARNING"
	case percent >= 50:
		return "OK"
	default:
		return "LOW"
	}
}

// =============================================================================
// RESET USAGE
// =============================================================================

var usageResetCmd = &cobra.Command{
	Use:   "reset <tenant-id>",
	Short: "Reset usage counters",
	Long: `Reset cumulative usage counters for a tenant.

This resets:
  • Total messages published/consumed
  • Total bytes published/consumed

Does NOT reset:
  • Current storage usage (calculated from actual data)
  • Topic/partition counts

Examples:
  goqueue-admin usage reset acme`,
	Args: cobra.ExactArgs(1),
	RunE: runUsageReset,
}

func init() {
	usageCmd.AddCommand(usageResetCmd)
}

func runUsageReset(cmd *cobra.Command, args []string) error {
	tenantID := args[0]
	ctx, cancel := getContext()
	defer cancel()

	var result map[string]interface{}
	err := client.DoRequest(ctx, "POST", "/admin/tenants/"+tenantID+"/usage/reset", nil, &result)
	if err != nil {
		return handleError(err)
	}

	fmt.Printf("✓ Usage counters reset for tenant '%s'\n", tenantID)
	return nil
}

// =============================================================================
// LIST TOPICS
// =============================================================================

var usageTopicsCmd = &cobra.Command{
	Use:   "topics <tenant-id>",
	Short: "List tenant's topics",
	Long: `List all topics belonging to a tenant.

Examples:
  goqueue-admin usage topics acme`,
	Args: cobra.ExactArgs(1),
	RunE: runUsageTopics,
}

func init() {
	usageCmd.AddCommand(usageTopicsCmd)
}

func runUsageTopics(cmd *cobra.Command, args []string) error {
	tenantID := args[0]
	ctx, cancel := getContext()
	defer cancel()

	var result map[string]interface{}
	err := client.DoRequest(ctx, "GET", "/admin/tenants/"+tenantID+"/topics", nil, &result)
	if err != nil {
		return handleError(err)
	}

	if outputFlag == "json" {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(result)
	}

	topics, ok := result["topics"].([]interface{})
	if !ok || len(topics) == 0 {
		fmt.Printf("No topics found for tenant '%s'\n", tenantID)
		return nil
	}

	fmt.Printf("Topics for tenant '%s':\n\n", tenantID)
	for i, t := range topics {
		fmt.Printf("  %d. %s\n", i+1, t)
	}
	fmt.Printf("\nTotal: %v topics\n", result["count"])
	return nil
}

// =============================================================================
// LIST GROUPS
// =============================================================================

var usageGroupsCmd = &cobra.Command{
	Use:   "groups <tenant-id>",
	Short: "List tenant's consumer groups",
	Long: `List all consumer groups belonging to a tenant.

Examples:
  goqueue-admin usage groups acme`,
	Args: cobra.ExactArgs(1),
	RunE: runUsageGroups,
}

func init() {
	usageCmd.AddCommand(usageGroupsCmd)
}

func runUsageGroups(cmd *cobra.Command, args []string) error {
	tenantID := args[0]
	ctx, cancel := getContext()
	defer cancel()

	var result map[string]interface{}
	err := client.DoRequest(ctx, "GET", "/admin/tenants/"+tenantID+"/groups", nil, &result)
	if err != nil {
		return handleError(err)
	}

	if outputFlag == "json" {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(result)
	}

	groups, ok := result["groups"].([]interface{})
	if !ok || len(groups) == 0 {
		fmt.Printf("No consumer groups found for tenant '%s'\n", tenantID)
		return nil
	}

	fmt.Printf("Consumer groups for tenant '%s':\n\n", tenantID)
	for i, g := range groups {
		fmt.Printf("  %d. %s\n", i+1, g)
	}
	fmt.Printf("\nTotal: %v groups\n", result["count"])
	return nil
}
