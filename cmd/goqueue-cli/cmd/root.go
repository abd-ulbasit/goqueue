// =============================================================================
// ROOT COMMAND - CLI ENTRY POINT AND GLOBAL FLAGS
// =============================================================================
//
// WHAT IS THIS?
// The root command that initializes the CLI and defines global flags.
// All subcommands inherit these flags and share the client configuration.
//
// GLOBAL FLAGS:
//   --server, -s    Server URL (default: http://localhost:8080)
//   --context, -c   Config context to use
//   --output, -o    Output format: table, json, yaml (default: table)
//   --timeout       Request timeout in seconds (default: 30)
//
// SUBCOMMANDS:
//   topic       Manage topics
//   produce     Publish messages
//   consume     Read messages
//   group       Manage consumer groups
//   trace       Query message traces
//   cluster     Cluster operations
//   config      Manage CLI configuration
//   version     Show version information
//
// =============================================================================

package cmd

import (
	"context"
	"fmt"
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
	contextFlag string
	outputFlag  string
	timeoutFlag int

	// Shared instances
	config    *cli.Config
	client    *cli.Client
	formatter *cli.Formatter
)

// =============================================================================
// ROOT COMMAND
// =============================================================================

var rootCmd = &cobra.Command{
	Use:   "goqueue",
	Short: "Command-line interface for goqueue message broker",
	Long: `goqueue CLI - Manage your goqueue message broker from the command line.

GoQueue is a high-performance message queue built in Go, featuring:
  • Multi-partition topics for horizontal scaling
  • Consumer groups with cooperative rebalancing
  • Priority lanes and delayed messages
  • Message tracing and schema validation
  • Cluster support with leader election

Use "goqueue [command] --help" for more information about a command.`,
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
		"Server URL (env: GOQUEUE_SERVER)")
	rootCmd.PersistentFlags().StringVarP(&contextFlag, "context", "c", "",
		"Config context to use (env: GOQUEUE_CONTEXT)")
	rootCmd.PersistentFlags().StringVarP(&outputFlag, "output", "o", "table",
		"Output format: table, json, yaml")
	rootCmd.PersistentFlags().IntVar(&timeoutFlag, "timeout", 30,
		"Request timeout in seconds")

	// Add subcommands
	rootCmd.AddCommand(topicCmd)
	rootCmd.AddCommand(produceCmd)
	rootCmd.AddCommand(consumeCmd)
	rootCmd.AddCommand(groupCmd)
	rootCmd.AddCommand(traceCmd)
	rootCmd.AddCommand(clusterCmd)
	rootCmd.AddCommand(configCmd)
	rootCmd.AddCommand(versionCmd)
}

// =============================================================================
// CLIENT INITIALIZATION
// =============================================================================

// initializeClient sets up the HTTP client and formatter before each command.
func initializeClient(cmd *cobra.Command, args []string) error {
	// Skip initialization for config commands (they manage config themselves)
	if cmd.Name() == "config" || cmd.Parent() != nil && cmd.Parent().Name() == "config" {
		return nil
	}

	// Skip for version command
	if cmd.Name() == "version" {
		return initializeMinimal()
	}

	// Load configuration
	var err error
	config, err = cli.LoadConfig()
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	// Handle context override
	if contextFlag != "" {
		if err := config.UseContext(contextFlag); err != nil {
			return err
		}
	} else if envCtx := os.Getenv(cli.EnvContext); envCtx != "" {
		if err := config.UseContext(envCtx); err != nil {
			return err
		}
	}

	// Resolve server URL
	serverURL := cli.ResolveServer(serverFlag, config)

	// Create client
	clientConfig := cli.ClientConfig{
		ServerURL: serverURL,
		Timeout:   time.Duration(timeoutFlag) * time.Second,
		APIKey:    cli.ResolveAPIKey("", config),
	}
	client = cli.NewClient(clientConfig)

	// Create formatter
	format, err := cli.ParseOutputFormat(outputFlag)
	if err != nil {
		return err
	}
	formatter = cli.NewFormatter(format)

	return nil
}

// initializeMinimal sets up minimal state for commands that don't need full client.
func initializeMinimal() error {
	// Load config (if available)
	config, _ = cli.LoadConfig()

	// Create formatter
	format, err := cli.ParseOutputFormat(outputFlag)
	if err != nil {
		return err
	}
	formatter = cli.NewFormatter(format)

	return nil
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

// getContext returns a context with timeout.
func getContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), time.Duration(timeoutFlag)*time.Second)
}

// handleError prints an error and returns it.
func handleError(err error) error {
	cli.PrintError("%v", err)
	return err
}
