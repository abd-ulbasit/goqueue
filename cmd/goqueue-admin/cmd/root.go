// =============================================================================
// ROOT COMMAND - ADMIN CLI ENTRY POINT
// =============================================================================
//
// GLOBAL FLAGS:
//   --server, -s    Server URL (default: http://localhost:8080)
//   --output, -o    Output format: table, json, yaml (default: table)
//   --timeout       Request timeout in seconds (default: 30)
//   --api-key       API key for authentication (env: GOQUEUE_ADMIN_API_KEY)
//
// =============================================================================

package cmd

import (
	"context"
	"os"
	"time"

	"github.com/spf13/cobra"

	"goqueue/internal/cli"
)

// =============================================================================
// GLOBAL STATE
// =============================================================================

var (
	// Global flags
	serverFlag  string
	outputFlag  string
	timeoutFlag int
	apiKeyFlag  string

	// Shared instances
	client    *cli.Client
	formatter *cli.Formatter
)

// =============================================================================
// ROOT COMMAND
// =============================================================================

var rootCmd = &cobra.Command{
	Use:   "goqueue-admin",
	Short: "Superadmin CLI for goqueue tenant and quota management",
	Long: `goqueue-admin - Superadmin interface for managing goqueue tenants and quotas.

This CLI provides administrative operations for multi-tenant goqueue deployments:
  • Tenant lifecycle management (create, suspend, disable, delete)
  • Quota configuration (rate limits, storage limits, count limits)
  • Usage monitoring and statistics
  • Tenant resource isolation

IMPORTANT: This CLI should only be used by cluster administrators.
Regular users should use the 'goqueue-cli' for their operations.

Use "goqueue-admin [command] --help" for more information about a command.`,
	PersistentPreRunE: initializeClient,
	SilenceUsage:      true,
	SilenceErrors:     true,
}

// Execute runs the root command.
func Execute() error {
	return rootCmd.Execute()
}

func init() {
	// Global flags available to all commands
	rootCmd.PersistentFlags().StringVarP(&serverFlag, "server", "s", "",
		"Server URL (env: GOQUEUE_ADMIN_SERVER)")
	rootCmd.PersistentFlags().StringVarP(&outputFlag, "output", "o", "table",
		"Output format: table, json, yaml")
	rootCmd.PersistentFlags().IntVar(&timeoutFlag, "timeout", 30,
		"Request timeout in seconds")
	rootCmd.PersistentFlags().StringVar(&apiKeyFlag, "api-key", "",
		"API key for authentication (env: GOQUEUE_ADMIN_API_KEY)")

	// Add subcommands
	rootCmd.AddCommand(tenantCmd)
	rootCmd.AddCommand(quotaCmd)
	rootCmd.AddCommand(usageCmd)
	rootCmd.AddCommand(versionCmd)
}

// initializeClient sets up the HTTP client before command execution.
func initializeClient(cmd *cobra.Command, args []string) error {
	// Skip initialization for version command
	if cmd.Name() == "version" {
		return nil
	}

	// Determine server URL (flag > env > default)
	server := serverFlag
	if server == "" {
		server = os.Getenv("GOQUEUE_ADMIN_SERVER")
	}
	if server == "" {
		server = "http://localhost:8080"
	}

	// Determine API key (flag > env)
	apiKey := apiKeyFlag
	if apiKey == "" {
		apiKey = os.Getenv("GOQUEUE_ADMIN_API_KEY")
	}

	// Create client
	clientConfig := cli.ClientConfig{
		ServerURL: server,
		Timeout:   time.Duration(timeoutFlag) * time.Second,
		APIKey:    apiKey,
	}
	client = cli.NewClient(clientConfig)

	// Create formatter - parse the string flag to OutputFormat
	outputFormat, err := cli.ParseOutputFormat(outputFlag)
	if err != nil {
		return err
	}
	formatter = cli.NewFormatter(outputFormat)

	return nil
}

// getContext returns a context with timeout for API calls.
func getContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), time.Duration(timeoutFlag)*time.Second)
}

// handleError prints an error in a consistent format and returns it.
// This matches the pattern in goqueue-cli for consistent UX across CLIs.
func handleError(err error) error {
	cli.PrintError("%v", err)
	return err
}
