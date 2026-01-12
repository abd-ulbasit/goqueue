// =============================================================================
// CLUSTER COMMANDS - CLUSTER OPERATIONS
// =============================================================================
//
// WHAT IS THIS?
// Commands for cluster operations and health monitoring.
//
// COMMANDS:
//   goqueue cluster info       Show cluster overview
//   goqueue cluster health     Check cluster health
//   goqueue cluster nodes      List cluster nodes
//
// EXAMPLES:
//   goqueue cluster info
//   goqueue cluster health
//   goqueue cluster nodes -o json
//
// =============================================================================

package cmd

import (
	"github.com/spf13/cobra"

	"goqueue/internal/cli"
)

// =============================================================================
// CLUSTER COMMAND (PARENT)
// =============================================================================

var clusterCmd = &cobra.Command{
	Use:   "cluster",
	Short: "Cluster operations",
	Long: `Manage and monitor the goqueue cluster.

For single-node deployments, these commands show the local broker status.
For clustered deployments, they show cluster-wide information.

Examples:
  goqueue cluster info         # Show cluster overview
  goqueue cluster health       # Check cluster health
  goqueue cluster nodes        # List all nodes`,
}

func init() {
	clusterCmd.AddCommand(clusterInfoCmd)
	clusterCmd.AddCommand(clusterHealthCmd)
	clusterCmd.AddCommand(clusterNodesCmd)
}

// =============================================================================
// CLUSTER INFO
// =============================================================================

var clusterInfoCmd = &cobra.Command{
	Use:   "info",
	Short: "Show cluster overview",
	Long: `Show cluster overview including broker statistics.

Output includes:
  - Node ID and uptime
  - Number of topics and total size
  - For clustered mode: controller info, node count

Examples:
  goqueue cluster info
  goqueue cluster info -o json`,
	RunE: runClusterInfo,
}

func runClusterInfo(cmd *cobra.Command, args []string) error {
	ctx, cancel := getContext()
	defer cancel()

	// First try cluster state (for clustered deployments)
	clusterState, clusterErr := client.GetClusterState(ctx)

	// Always get broker stats
	stats, err := client.GetStats(ctx)
	if err != nil {
		return handleError(err)
	}

	// If JSON/YAML, combine both
	if outputFlag == "json" || outputFlag == "yaml" {
		combined := map[string]interface{}{
			"broker_stats": stats,
		}
		if clusterErr == nil && clusterState != nil {
			combined["cluster_state"] = clusterState
		}
		return formatter.Format(combined)
	}

	// Table format
	cli.PrintInfo("Broker Information:")
	formatter.FormatBrokerStats(stats)

	// Show cluster info if available
	if clusterErr == nil && clusterState != nil && len(clusterState.Nodes) > 0 {
		cli.PrintInfo("\nCluster Information:")
		formatter.FormatClusterState(clusterState)
	}

	return nil
}

// =============================================================================
// CLUSTER HEALTH
// =============================================================================

var clusterHealthCmd = &cobra.Command{
	Use:   "health",
	Short: "Check cluster health",
	Long: `Check the health status of the cluster.

Returns exit code 0 if healthy, non-zero otherwise.
Useful for health checks in container orchestration.

Examples:
  goqueue cluster health
  goqueue cluster health && echo "Cluster is healthy"`,
	RunE: runClusterHealth,
}

func runClusterHealth(cmd *cobra.Command, args []string) error {
	ctx, cancel := getContext()
	defer cancel()

	// Check basic health
	health, err := client.Health(ctx)
	if err != nil {
		return handleError(err)
	}

	// Try cluster health for more detail
	clusterHealth, _ := client.GetClusterHealth(ctx)

	if outputFlag == "json" || outputFlag == "yaml" {
		combined := map[string]interface{}{
			"status":    health.Status,
			"timestamp": health.Timestamp,
		}
		if clusterHealth != nil {
			combined["cluster"] = clusterHealth
		}
		return formatter.Format(combined)
	}

	// Table format
	formatter.FormatHealth(health)

	if clusterHealth != nil {
		cli.PrintInfo("\nCluster Health:")
		cli.PrintInfo("  Node Count: %d", clusterHealth.NodeCount)
		cli.PrintInfo("  Healthy:    %d", clusterHealth.Healthy)
		cli.PrintInfo("  Unhealthy:  %d", clusterHealth.Unhealthy)
	}

	return nil
}

// =============================================================================
// CLUSTER NODES
// =============================================================================

var clusterNodesCmd = &cobra.Command{
	Use:   "nodes",
	Short: "List cluster nodes",
	Long: `List all nodes in the cluster.

Output includes:
  - Node ID and addresses
  - Health status
  - Controller flag

Examples:
  goqueue cluster nodes
  goqueue cluster nodes -o json`,
	RunE: runClusterNodes,
}

func runClusterNodes(cmd *cobra.Command, args []string) error {
	ctx, cancel := getContext()
	defer cancel()

	state, err := client.GetClusterState(ctx)
	if err != nil {
		// Single node mode - show local info
		stats, statsErr := client.GetStats(ctx)
		if statsErr != nil {
			return handleError(err) // Return original error
		}

		// Create a single-node "cluster"
		state = &cli.ClusterState{
			ControllerID: stats.NodeID,
			Nodes: []cli.NodeInfo{
				{
					ID:            stats.NodeID,
					ClientAddress: serverFlag,
					Status:        "online",
					IsController:  true,
				},
			},
		}
	}

	return formatter.FormatClusterState(state)
}
