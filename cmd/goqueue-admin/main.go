// =============================================================================
// GOQUEUE-ADMIN CLI - SUPERADMIN MANAGEMENT TOOL
// =============================================================================
//
// WHAT IS THIS?
// A command-line interface for superadmin operations on goqueue, specifically
// for managing tenants and quotas. This is separate from the regular goqueue-cli
// which is for tenant users.
//
// WHY A SEPARATE CLI?
//   - Clear separation of privileges (admin vs user operations)
//   - Different default behaviors (admin sees all tenants)
//   - Explicit admin intent (prevents accidental admin actions)
//   - Can have different authentication mechanisms
//
// COMPARISON:
//   - Kafka: kafka-acls.sh, kafka-configs.sh for admin operations
//   - RabbitMQ: rabbitmqadmin for management API
//   - AWS: Separate IAM admin console
//
// COMMANDS:
//   goqueue-admin tenant     Manage tenants
//   goqueue-admin quota      Manage quotas
//   goqueue-admin usage      View usage statistics
//   goqueue-admin version    Show version information
//
// USAGE EXAMPLES:
//   # Create a new tenant
//   goqueue-admin tenant create acme --name "Acme Corp"
//
//   # Update quotas
//   goqueue-admin quota set acme --publish-rate 10000 --max-topics 50
//
//   # View usage
//   goqueue-admin usage show acme
//
//   # Suspend a tenant
//   goqueue-admin tenant suspend acme --reason "Payment overdue"
//
// =============================================================================

package main

import (
	"fmt"
	"os"

	"goqueue/cmd/goqueue-admin/cmd"
)

func main() {
	if err := cmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
