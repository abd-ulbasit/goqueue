// =============================================================================
// VERSION COMMAND - SHOW VERSION INFORMATION
// =============================================================================

package cmd

import (
	"fmt"
	"runtime"

	"github.com/spf13/cobra"
)

// Build information - set via ldflags
var (
	Version   = "dev"
	Commit    = "unknown"
	BuildDate = "unknown"
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Show version information",
	Long:  `Display version, build commit, and build date for the goqueue-admin CLI.`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("goqueue-admin version %s\n", Version)
		fmt.Printf("  Commit:     %s\n", Commit)
		fmt.Printf("  Build Date: %s\n", BuildDate)
		fmt.Printf("  Go Version: %s\n", runtime.Version())
		fmt.Printf("  OS/Arch:    %s/%s\n", runtime.GOOS, runtime.GOARCH)
	},
}
