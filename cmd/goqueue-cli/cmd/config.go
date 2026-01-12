// =============================================================================
// CONFIG COMMANDS - MANAGE CLI CONFIGURATION
// =============================================================================
//
// WHAT IS THIS?
// Commands for managing CLI configuration (contexts, servers).
//
// COMMANDS:
//   goqueue config view              Show current configuration
//   goqueue config get-contexts      List all contexts
//   goqueue config use-context       Switch to a context
//   goqueue config set-context       Create/update a context
//   goqueue config delete-context    Delete a context
//
// EXAMPLES:
//   goqueue config view
//   goqueue config set-context prod --server https://goqueue.prod.example.com
//   goqueue config use-context prod
//
// =============================================================================

package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	"goqueue/internal/cli"
)

// =============================================================================
// CONFIG COMMAND (PARENT)
// =============================================================================

var configCmd = &cobra.Command{
	Use:   "config",
	Short: "Manage CLI configuration",
	Long: `Manage goqueue CLI configuration.

Configuration is stored in ~/.goqueue/config.yaml and supports multiple
contexts for different clusters (similar to kubectl contexts).

Examples:
  goqueue config view                    # Show current config
  goqueue config get-contexts            # List all contexts
  goqueue config use-context production  # Switch to production
  goqueue config set-context staging \
    --server https://staging.example.com # Create staging context`,
}

func init() {
	configCmd.AddCommand(configViewCmd)
	configCmd.AddCommand(configGetContextsCmd)
	configCmd.AddCommand(configUseContextCmd)
	configCmd.AddCommand(configSetContextCmd)
	configCmd.AddCommand(configDeleteContextCmd)
}

// =============================================================================
// CONFIG VIEW
// =============================================================================

var configViewCmd = &cobra.Command{
	Use:   "view",
	Short: "Show current configuration",
	Long: `Show the current CLI configuration.

Output includes:
  - Current context
  - All defined contexts and their settings
  - Config file location

Examples:
  goqueue config view
  goqueue config view -o json`,
	RunE: runConfigView,
}

func runConfigView(cmd *cobra.Command, args []string) error {
	cfg, err := cli.LoadConfig()
	if err != nil {
		return handleError(err)
	}

	// Create formatter
	format, err := cli.ParseOutputFormat(outputFlag)
	if err != nil {
		return err
	}
	f := cli.NewFormatter(format)

	if format == cli.OutputJSON || format == cli.OutputYAML {
		return f.Format(cfg)
	}

	// Table format
	fmt.Printf("Config file: %s\n\n", cli.DefaultConfigPath())
	fmt.Printf("Current context: %s\n\n", cfg.CurrentContext)
	fmt.Println("Contexts:")

	table := f.Table()
	table.SetHeaders("NAME", "SERVER", "CURRENT")
	table.WriteHeaders()

	for name, ctx := range cfg.Contexts {
		current := ""
		if name == cfg.CurrentContext {
			current = "*"
		}
		table.WriteRow(name, ctx.Server, current)
	}
	return table.Flush()
}

// =============================================================================
// CONFIG GET-CONTEXTS
// =============================================================================

var configGetContextsCmd = &cobra.Command{
	Use:   "get-contexts",
	Short: "List all contexts",
	Long: `List all configured contexts.

Examples:
  goqueue config get-contexts
  goqueue config get-contexts -o json`,
	RunE: runConfigGetContexts,
}

func runConfigGetContexts(cmd *cobra.Command, args []string) error {
	cfg, err := cli.LoadConfig()
	if err != nil {
		return handleError(err)
	}

	// Create formatter
	format, err := cli.ParseOutputFormat(outputFlag)
	if err != nil {
		return err
	}
	f := cli.NewFormatter(format)

	if format == cli.OutputJSON || format == cli.OutputYAML {
		return f.Format(cfg.Contexts)
	}

	// Table format
	table := f.Table()
	table.SetHeaders("NAME", "SERVER", "CURRENT")
	table.WriteHeaders()

	for name, ctx := range cfg.Contexts {
		current := ""
		if name == cfg.CurrentContext {
			current = "*"
		}
		table.WriteRow(name, ctx.Server, current)
	}
	return table.Flush()
}

// =============================================================================
// CONFIG USE-CONTEXT
// =============================================================================

var configUseContextCmd = &cobra.Command{
	Use:   "use-context <name>",
	Short: "Switch to a context",
	Long: `Switch to a different context.

Arguments:
  name    The name of the context to use

Examples:
  goqueue config use-context production
  goqueue config use-context staging`,
	Args: cobra.ExactArgs(1),
	RunE: runConfigUseContext,
}

func runConfigUseContext(cmd *cobra.Command, args []string) error {
	name := args[0]

	cfg, err := cli.LoadConfig()
	if err != nil {
		return handleError(err)
	}

	if err := cfg.UseContext(name); err != nil {
		return handleError(err)
	}

	if err := cfg.Save(); err != nil {
		return handleError(err)
	}

	cli.PrintSuccess("Switched to context %q", name)
	return nil
}

// =============================================================================
// CONFIG SET-CONTEXT
// =============================================================================

var (
	setContextServer  string
	setContextAPIKey  string
	setContextTimeout int
)

var configSetContextCmd = &cobra.Command{
	Use:   "set-context <name>",
	Short: "Create or update a context",
	Long: `Create a new context or update an existing one.

Arguments:
  name    The name of the context

Flags:
  --server     Server URL (required for new contexts)
  --api-key    API key for authentication
  --timeout    Request timeout in seconds

Examples:
  # Create a new context
  goqueue config set-context prod --server https://goqueue.prod.example.com

  # Update existing context
  goqueue config set-context prod --api-key "new-key-123"

  # Create with all options
  goqueue config set-context staging \
    --server https://staging.example.com \
    --api-key "staging-key" \
    --timeout 60`,
	Args: cobra.ExactArgs(1),
	RunE: runConfigSetContext,
}

func init() {
	configSetContextCmd.Flags().StringVar(&setContextServer, "server", "",
		"Server URL")
	configSetContextCmd.Flags().StringVar(&setContextAPIKey, "api-key", "",
		"API key for authentication")
	configSetContextCmd.Flags().IntVar(&setContextTimeout, "timeout", 30,
		"Request timeout in seconds")
}

func runConfigSetContext(cmd *cobra.Command, args []string) error {
	name := args[0]

	cfg, err := cli.LoadConfig()
	if err != nil {
		return handleError(err)
	}

	// Get existing context or create new
	ctx, _ := cfg.GetContext(name)
	if ctx == nil {
		// New context requires server
		if setContextServer == "" {
			cli.PrintError("--server is required for new contexts")
			return cmd.Usage()
		}
		ctx = &cli.ContextConfig{
			Timeout: 30,
		}
	}

	// Update fields
	if setContextServer != "" {
		ctx.Server = setContextServer
	}
	if setContextAPIKey != "" {
		ctx.APIKey = setContextAPIKey
	}
	if cmd.Flags().Changed("timeout") {
		ctx.Timeout = setContextTimeout
	}

	cfg.SetContext(name, ctx)

	if err := cfg.Save(); err != nil {
		return handleError(err)
	}

	cli.PrintSuccess("Context %q saved", name)
	return nil
}

// =============================================================================
// CONFIG DELETE-CONTEXT
// =============================================================================

var configDeleteContextCmd = &cobra.Command{
	Use:   "delete-context <name>",
	Short: "Delete a context",
	Long: `Delete a context from the configuration.

Arguments:
  name    The name of the context to delete

Examples:
  goqueue config delete-context old-staging`,
	Args: cobra.ExactArgs(1),
	RunE: runConfigDeleteContext,
}

func runConfigDeleteContext(cmd *cobra.Command, args []string) error {
	name := args[0]

	cfg, err := cli.LoadConfig()
	if err != nil {
		return handleError(err)
	}

	if err := cfg.DeleteContext(name); err != nil {
		return handleError(err)
	}

	if err := cfg.Save(); err != nil {
		return handleError(err)
	}

	cli.PrintSuccess("Context %q deleted", name)
	return nil
}
