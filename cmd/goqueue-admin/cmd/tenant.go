// =============================================================================
// TENANT COMMAND - TENANT LIFECYCLE MANAGEMENT
// =============================================================================
//
// SUBCOMMANDS:
//   goqueue-admin tenant create    Create a new tenant
//   goqueue-admin tenant list      List all tenants
//   goqueue-admin tenant get       Get tenant details
//   goqueue-admin tenant update    Update tenant metadata
//   goqueue-admin tenant delete    Delete a tenant
//   goqueue-admin tenant suspend   Suspend a tenant
//   goqueue-admin tenant activate  Reactivate a tenant
//   goqueue-admin tenant disable   Permanently disable a tenant
//
// =============================================================================

package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/spf13/cobra"
)

// =============================================================================
// TENANT PARENT COMMAND
// =============================================================================

var tenantCmd = &cobra.Command{
	Use:   "tenant",
	Short: "Manage tenants",
	Long: `Manage goqueue tenants - create, list, update, and control tenant lifecycle.

Tenants provide namespace isolation and resource quotas for multi-tenant deployments.
Each tenant has:
  • Isolated topic namespace (topics prefixed with tenant ID)
  • Configurable quotas (rate limits, storage limits)
  • Lifecycle states (active, suspended, disabled)

Examples:
  # Create a tenant
  goqueue-admin tenant create acme --name "Acme Corp"

  # List all tenants
  goqueue-admin tenant list

  # Get tenant details
  goqueue-admin tenant get acme

  # Suspend a tenant
  goqueue-admin tenant suspend acme --reason "Payment overdue"`,
}

// =============================================================================
// CREATE TENANT
// =============================================================================

var (
	createNameFlag     string
	createDescFlag     string
	createPublishRate  int64
	createConsumeRate  int64
	createMaxStorage   string
	createMaxTopics    int64
	createMaxMsgSize   string
	createMetadataFlag []string
)

var tenantCreateCmd = &cobra.Command{
	Use:   "create <tenant-id>",
	Short: "Create a new tenant",
	Long: `Create a new tenant with namespace isolation and resource quotas.

The tenant ID must:
  • Be 3-63 characters long
  • Contain only lowercase letters, numbers, and hyphens
  • Start with a letter
  • Not start with "__" (reserved for system)

Examples:
  # Create with default quotas
  goqueue-admin tenant create acme --name "Acme Corp"

  # Create with custom quotas
  goqueue-admin tenant create startup \
    --name "Startup Inc" \
    --publish-rate 1000 \
    --max-topics 10 \
    --max-storage 1GB

  # Create with metadata
  goqueue-admin tenant create enterprise \
    --name "Enterprise LLC" \
    --meta plan=enterprise \
    --meta region=us-west`,
	Args: cobra.ExactArgs(1),
	RunE: runTenantCreate,
}

func init() {
	tenantCmd.AddCommand(tenantCreateCmd)

	tenantCreateCmd.Flags().StringVar(&createNameFlag, "name", "", "Human-readable tenant name (required)")
	tenantCreateCmd.Flags().StringVar(&createDescFlag, "description", "", "Tenant description")
	tenantCreateCmd.Flags().Int64Var(&createPublishRate, "publish-rate", 0, "Max publish rate (msg/sec)")
	tenantCreateCmd.Flags().Int64Var(&createConsumeRate, "consume-rate", 0, "Max consume rate (msg/sec)")
	tenantCreateCmd.Flags().StringVar(&createMaxStorage, "max-storage", "", "Max storage (e.g., 10GB)")
	tenantCreateCmd.Flags().Int64Var(&createMaxTopics, "max-topics", 0, "Max number of topics")
	tenantCreateCmd.Flags().StringVar(&createMaxMsgSize, "max-message-size", "", "Max message size (e.g., 1MB)")
	tenantCreateCmd.Flags().StringArrayVar(&createMetadataFlag, "meta", nil, "Metadata key=value pairs")

	tenantCreateCmd.MarkFlagRequired("name")
}

func runTenantCreate(cmd *cobra.Command, args []string) error {
	tenantID := args[0]
	ctx, cancel := getContext()
	defer cancel()

	// Build request body
	body := map[string]interface{}{
		"id":   tenantID,
		"name": createNameFlag,
	}

	if createDescFlag != "" {
		body["description"] = createDescFlag
	}

	// Parse metadata
	if len(createMetadataFlag) > 0 {
		metadata := make(map[string]string)
		for _, m := range createMetadataFlag {
			parts := strings.SplitN(m, "=", 2)
			if len(parts) == 2 {
				metadata[parts[0]] = parts[1]
			}
		}
		body["metadata"] = metadata
	}

	// Build quotas if any specified
	quotas := make(map[string]interface{})
	if createPublishRate > 0 {
		quotas["publish_rate_limit"] = createPublishRate
	}
	if createConsumeRate > 0 {
		quotas["consume_rate_limit"] = createConsumeRate
	}
	if createMaxTopics > 0 {
		quotas["max_topics"] = createMaxTopics
	}
	if createMaxStorage != "" {
		bytes, err := parseSize(createMaxStorage)
		if err != nil {
			return fmt.Errorf("invalid max-storage: %w", err)
		}
		quotas["max_storage_bytes"] = bytes
	}
	if createMaxMsgSize != "" {
		bytes, err := parseSize(createMaxMsgSize)
		if err != nil {
			return fmt.Errorf("invalid max-message-size: %w", err)
		}
		quotas["max_message_size_bytes"] = bytes
	}
	if len(quotas) > 0 {
		body["quotas"] = quotas
	}

	// Make request
	var result map[string]interface{}
	err := client.DoRequest(ctx, "POST", "/admin/tenants", body, &result)
	if err != nil {
		return handleError(err)
	}

	// Output result
	if outputFlag == "json" {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(result)
	}

	fmt.Printf("✓ Tenant '%s' created successfully\n", tenantID)
	fmt.Printf("  Name: %s\n", result["name"])
	fmt.Printf("  Status: %s\n", result["status"])
	return nil
}

// =============================================================================
// LIST TENANTS
// =============================================================================

var listStatusFlag string

var tenantListCmd = &cobra.Command{
	Use:   "list",
	Short: "List all tenants",
	Long: `List all tenants with their status and basic info.

Examples:
  # List all tenants
  goqueue-admin tenant list

  # List only active tenants
  goqueue-admin tenant list --status active

  # Output as JSON
  goqueue-admin tenant list -o json`,
	RunE: runTenantList,
}

func init() {
	tenantCmd.AddCommand(tenantListCmd)
	tenantListCmd.Flags().StringVar(&listStatusFlag, "status", "", "Filter by status (active, suspended, disabled)")
}

func runTenantList(cmd *cobra.Command, args []string) error {
	ctx, cancel := getContext()
	defer cancel()

	path := "/admin/tenants"
	if listStatusFlag != "" {
		path += "?status=" + listStatusFlag
	}

	var result map[string]interface{}
	err := client.DoRequest(ctx, "GET", path, nil, &result)
	if err != nil {
		return handleError(err)
	}

	// Output result
	if outputFlag == "json" {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(result)
	}

	tenants, ok := result["tenants"].([]interface{})
	if !ok || len(tenants) == 0 {
		fmt.Println("No tenants found.")
		return nil
	}

	// Table output
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tNAME\tSTATUS\tTOPICS\tSTORAGE")
	fmt.Fprintln(w, "--\t----\t------\t------\t-------")

	for _, t := range tenants {
		tenant := t.(map[string]interface{})
		id := tenant["id"].(string)
		name := tenant["name"].(string)
		status := tenant["status"].(string)

		// Truncate long names
		if len(name) > 20 {
			name = name[:17] + "..."
		}

		fmt.Fprintf(w, "%s\t%s\t%s\t-\t-\n", id, name, status)
	}
	w.Flush()

	fmt.Printf("\nTotal: %v tenants\n", result["count"])
	return nil
}

// =============================================================================
// GET TENANT
// =============================================================================

var tenantGetCmd = &cobra.Command{
	Use:   "get <tenant-id>",
	Short: "Get tenant details",
	Long: `Get detailed information about a specific tenant.

Displays:
  • Basic info (ID, name, status)
  • Quota configuration
  • Metadata

Examples:
  goqueue-admin tenant get acme
  goqueue-admin tenant get acme -o json`,
	Args: cobra.ExactArgs(1),
	RunE: runTenantGet,
}

func init() {
	tenantCmd.AddCommand(tenantGetCmd)
}

func runTenantGet(cmd *cobra.Command, args []string) error {
	tenantID := args[0]
	ctx, cancel := getContext()
	defer cancel()

	var result map[string]interface{}
	err := client.DoRequest(ctx, "GET", "/admin/tenants/"+tenantID, nil, &result)
	if err != nil {
		return handleError(err)
	}

	// Output result
	if outputFlag == "json" {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(result)
	}

	// Formatted output
	fmt.Printf("Tenant: %s\n", result["id"])
	fmt.Printf("Name: %s\n", result["name"])
	fmt.Printf("Status: %s\n", result["status"])
	if desc, ok := result["description"].(string); ok && desc != "" {
		fmt.Printf("Description: %s\n", desc)
	}
	fmt.Printf("Created: %s\n", result["created_at"])
	fmt.Printf("Updated: %s\n", result["updated_at"])

	if result["status"] == "suspended" {
		if reason, ok := result["suspend_reason"].(string); ok {
			fmt.Printf("Suspend Reason: %s\n", reason)
		}
		if at, ok := result["suspended_at"].(string); ok {
			fmt.Printf("Suspended At: %s\n", at)
		}
	}

	// Show quotas
	if quotas, ok := result["quotas"].(map[string]interface{}); ok {
		fmt.Println("\nQuotas:")
		if v, ok := quotas["publish_rate_limit"]; ok {
			fmt.Printf("  Publish Rate: %v msg/sec\n", v)
		}
		if v, ok := quotas["consume_rate_limit"]; ok {
			fmt.Printf("  Consume Rate: %v msg/sec\n", v)
		}
		if v, ok := quotas["max_storage_bytes"]; ok {
			fmt.Printf("  Max Storage: %s\n", formatBytes(int64(v.(float64))))
		}
		if v, ok := quotas["max_topics"]; ok {
			fmt.Printf("  Max Topics: %v\n", v)
		}
		if v, ok := quotas["max_message_size_bytes"]; ok {
			fmt.Printf("  Max Message Size: %s\n", formatBytes(int64(v.(float64))))
		}
	}

	// Show metadata
	if metadata, ok := result["metadata"].(map[string]interface{}); ok && len(metadata) > 0 {
		fmt.Println("\nMetadata:")
		for k, v := range metadata {
			fmt.Printf("  %s: %v\n", k, v)
		}
	}

	return nil
}

// =============================================================================
// UPDATE TENANT
// =============================================================================

var (
	updateNameFlag string
	updateDescFlag string
	updateMetaFlag []string
)

var tenantUpdateCmd = &cobra.Command{
	Use:   "update <tenant-id>",
	Short: "Update tenant metadata",
	Long: `Update tenant name, description, or metadata.

Note: To update quotas, use 'goqueue-admin quota set'.

Examples:
  # Update name
  goqueue-admin tenant update acme --name "Acme Corporation"

  # Update metadata
  goqueue-admin tenant update acme --meta plan=enterprise`,
	Args: cobra.ExactArgs(1),
	RunE: runTenantUpdate,
}

func init() {
	tenantCmd.AddCommand(tenantUpdateCmd)
	tenantUpdateCmd.Flags().StringVar(&updateNameFlag, "name", "", "New tenant name")
	tenantUpdateCmd.Flags().StringVar(&updateDescFlag, "description", "", "New description")
	tenantUpdateCmd.Flags().StringArrayVar(&updateMetaFlag, "meta", nil, "Metadata key=value pairs")
}

func runTenantUpdate(cmd *cobra.Command, args []string) error {
	tenantID := args[0]
	ctx, cancel := getContext()
	defer cancel()

	body := make(map[string]interface{})
	if updateNameFlag != "" {
		body["name"] = updateNameFlag
	}
	if updateDescFlag != "" {
		body["description"] = updateDescFlag
	}
	if len(updateMetaFlag) > 0 {
		metadata := make(map[string]string)
		for _, m := range updateMetaFlag {
			parts := strings.SplitN(m, "=", 2)
			if len(parts) == 2 {
				metadata[parts[0]] = parts[1]
			}
		}
		body["metadata"] = metadata
	}

	if len(body) == 0 {
		return fmt.Errorf("no updates specified, use --name, --description, or --meta")
	}

	var result map[string]interface{}
	err := client.DoRequest(ctx, "PATCH", "/admin/tenants/"+tenantID, body, &result)
	if err != nil {
		return handleError(err)
	}

	fmt.Printf("✓ Tenant '%s' updated successfully\n", tenantID)
	return nil
}

// =============================================================================
// DELETE TENANT
// =============================================================================

var deleteForceFlag bool

var tenantDeleteCmd = &cobra.Command{
	Use:   "delete <tenant-id>",
	Short: "Delete a tenant",
	Long: `Delete a tenant and optionally all its resources.

WARNING: This is destructive and cannot be undone!

By default, deletion fails if the tenant has any topics.
Use --force to delete anyway.

Examples:
  # Delete empty tenant
  goqueue-admin tenant delete old-tenant

  # Force delete with resources
  goqueue-admin tenant delete old-tenant --force`,
	Args: cobra.ExactArgs(1),
	RunE: runTenantDelete,
}

func init() {
	tenantCmd.AddCommand(tenantDeleteCmd)
	tenantDeleteCmd.Flags().BoolVar(&deleteForceFlag, "force", false, "Force delete even if tenant has resources")
}

func runTenantDelete(cmd *cobra.Command, args []string) error {
	tenantID := args[0]
	ctx, cancel := getContext()
	defer cancel()

	path := "/admin/tenants/" + tenantID
	if deleteForceFlag {
		path += "?force=true"
	}

	var result map[string]interface{}
	err := client.DoRequest(ctx, "DELETE", path, nil, &result)
	if err != nil {
		return handleError(err)
	}

	fmt.Printf("✓ Tenant '%s' deleted successfully\n", tenantID)
	return nil
}

// =============================================================================
// SUSPEND TENANT
// =============================================================================

var suspendReasonFlag string

var tenantSuspendCmd = &cobra.Command{
	Use:   "suspend <tenant-id>",
	Short: "Suspend a tenant",
	Long: `Temporarily suspend a tenant.

Suspended tenants:
  • Cannot publish new messages
  • Cannot consume messages
  • Cannot create new topics
  • Data is preserved (not deleted)

Use 'goqueue-admin tenant activate' to reactivate.

Examples:
  goqueue-admin tenant suspend acme --reason "Payment overdue"
  goqueue-admin tenant suspend trial --reason "Trial expired"`,
	Args: cobra.ExactArgs(1),
	RunE: runTenantSuspend,
}

func init() {
	tenantCmd.AddCommand(tenantSuspendCmd)
	tenantSuspendCmd.Flags().StringVar(&suspendReasonFlag, "reason", "", "Reason for suspension (required)")
	tenantSuspendCmd.MarkFlagRequired("reason")
}

func runTenantSuspend(cmd *cobra.Command, args []string) error {
	tenantID := args[0]
	ctx, cancel := getContext()
	defer cancel()

	body := map[string]string{
		"reason": suspendReasonFlag,
	}

	var result map[string]interface{}
	err := client.DoRequest(ctx, "POST", "/admin/tenants/"+tenantID+"/suspend", body, &result)
	if err != nil {
		return handleError(err)
	}

	fmt.Printf("✓ Tenant '%s' suspended\n", tenantID)
	fmt.Printf("  Reason: %s\n", suspendReasonFlag)
	return nil
}

// =============================================================================
// ACTIVATE TENANT
// =============================================================================

var tenantActivateCmd = &cobra.Command{
	Use:   "activate <tenant-id>",
	Short: "Reactivate a suspended tenant",
	Long: `Reactivate a previously suspended tenant.

Examples:
  goqueue-admin tenant activate acme`,
	Args: cobra.ExactArgs(1),
	RunE: runTenantActivate,
}

func init() {
	tenantCmd.AddCommand(tenantActivateCmd)
}

func runTenantActivate(cmd *cobra.Command, args []string) error {
	tenantID := args[0]
	ctx, cancel := getContext()
	defer cancel()

	var result map[string]interface{}
	err := client.DoRequest(ctx, "POST", "/admin/tenants/"+tenantID+"/activate", nil, &result)
	if err != nil {
		return handleError(err)
	}

	fmt.Printf("✓ Tenant '%s' activated\n", tenantID)
	return nil
}

// =============================================================================
// DISABLE TENANT
// =============================================================================

var tenantDisableCmd = &cobra.Command{
	Use:   "disable <tenant-id>",
	Short: "Permanently disable a tenant",
	Long: `Permanently disable a tenant.

Disabled tenants:
  • Cannot perform any operations
  • Are effectively frozen
  • Usually a precursor to deletion

Use with caution - this is typically permanent.

Examples:
  goqueue-admin tenant disable departed-customer`,
	Args: cobra.ExactArgs(1),
	RunE: runTenantDisable,
}

func init() {
	tenantCmd.AddCommand(tenantDisableCmd)
}

func runTenantDisable(cmd *cobra.Command, args []string) error {
	tenantID := args[0]
	ctx, cancel := getContext()
	defer cancel()

	var result map[string]interface{}
	err := client.DoRequest(ctx, "POST", "/admin/tenants/"+tenantID+"/disable", nil, &result)
	if err != nil {
		return handleError(err)
	}

	fmt.Printf("✓ Tenant '%s' disabled\n", tenantID)
	return nil
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

// parseSize parses a human-readable size like "10GB" to bytes.
func parseSize(s string) (int64, error) {
	s = strings.TrimSpace(strings.ToUpper(s))

	var multiplier int64 = 1
	var numStr string

	switch {
	case strings.HasSuffix(s, "TB"):
		multiplier = 1024 * 1024 * 1024 * 1024
		numStr = s[:len(s)-2]
	case strings.HasSuffix(s, "GB"):
		multiplier = 1024 * 1024 * 1024
		numStr = s[:len(s)-2]
	case strings.HasSuffix(s, "MB"):
		multiplier = 1024 * 1024
		numStr = s[:len(s)-2]
	case strings.HasSuffix(s, "KB"):
		multiplier = 1024
		numStr = s[:len(s)-2]
	case strings.HasSuffix(s, "B"):
		numStr = s[:len(s)-1]
	default:
		numStr = s
	}

	var num float64
	_, err := fmt.Sscanf(numStr, "%f", &num)
	if err != nil {
		return 0, fmt.Errorf("invalid size: %s", s)
	}

	return int64(num * float64(multiplier)), nil
}

// formatBytes formats bytes to human-readable form.
func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}
