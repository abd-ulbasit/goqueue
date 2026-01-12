// =============================================================================
// VERSION COMMAND - SHOW VERSION INFORMATION
// =============================================================================
//
// WHAT IS THIS?
// Command to display CLI and server version information.
//
// USAGE:
//   goqueue version
//
// OUTPUT:
//   Client Version: v0.2.0
//   Server Version: v0.2.0 (if reachable)
//
// =============================================================================

package cmd

import (
	"github.com/spf13/cobra"

	"goqueue/internal/cli"
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Show version information",
	Long: `Show CLI and server version information.

Examples:
  goqueue version
  goqueue version -o json`,
	RunE: runVersion,
}

func runVersion(cmd *cobra.Command, args []string) error {
	info := &cli.VersionInfo{
		ClientVersion: cli.Version,
	}

	// Try to get server version (if client is available)
	if client != nil {
		ctx, cancel := getContext()
		stats, err := client.GetStats(ctx)
		cancel()
		if err == nil && stats != nil {
			info.ServerVersion = "v0.2.0" // Could add version endpoint
		}
	}

	return formatter.FormatVersion(info)
}
