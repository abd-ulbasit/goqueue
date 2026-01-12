// =============================================================================
// GOQUEUE CLI - MAIN ENTRY POINT
// =============================================================================
//
// WHAT IS THIS?
// The main entry point for the goqueue command-line interface (CLI).
// This tool allows operators to manage goqueue clusters from the terminal.
//
// USAGE:
//   goqueue [command] [subcommand] [flags]
//
// EXAMPLES:
//   goqueue topic list                        # List all topics
//   goqueue topic create orders -p 6          # Create topic with 6 partitions
//   goqueue produce orders -m "hello world"   # Publish a message
//   goqueue consume orders -p 0 --follow      # Consume messages (follow mode)
//   goqueue group list                        # List consumer groups
//   goqueue cluster info                      # Show cluster status
//
// CONFIGURATION:
//   Config file: ~/.goqueue/config.yaml
//   Env vars: GOQUEUE_SERVER, GOQUEUE_CONTEXT, GOQUEUE_API_KEY
//
// =============================================================================

package main

import (
	"os"

	"goqueue/cmd/goqueue-cli/cmd"
)

func main() {
	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}
