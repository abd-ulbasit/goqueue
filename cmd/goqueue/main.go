// =============================================================================
// GOQUEUE MAIN ENTRY POINT
// =============================================================================
//
// This is the entry point for the goqueue broker. It demonstrates:
//   - Creating a broker with default configuration
//   - Creating topics
//   - Publishing messages
//   - Consuming messages
//   - Viewing statistics
//
// =============================================================================

package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"goqueue/internal/broker"
)

func main() {
	fmt.Println("╔═══════════════════════════════════════════════════════════════╗")
	fmt.Println("║                     GoQueue v0.1.0                            ║")
	fmt.Println("║              Milestone 1: Storage Engine Demo                 ║")
	fmt.Println("╚═══════════════════════════════════════════════════════════════╝")
	fmt.Println()

	// -------------------------------------------------------------------------
	// STEP 1: Create broker with default configuration
	// -------------------------------------------------------------------------
	fmt.Println("📦 Starting broker...")
	config := broker.DefaultBrokerConfig()
	config.DataDir = "./data" // Store data in ./data directory

	b, err := broker.NewBroker(config)
	if err != nil {
		log.Fatalf("Failed to create broker: %v", err)
	}
	defer b.Close()

	fmt.Printf("   ✓ Broker started (NodeID: %s)\n", b.NodeID())
	fmt.Printf("   ✓ Data directory: %s\n\n", b.DataDir())

	// -------------------------------------------------------------------------
	// STEP 2: Create a topic (or use existing)
	// -------------------------------------------------------------------------
	topicName := "demo-orders"
	if !b.TopicExists(topicName) {
		fmt.Printf("📝 Creating topic '%s'...\n", topicName)
		err := b.CreateTopic(broker.TopicConfig{
			Name:          topicName,
			NumPartitions: 1, // Single partition for M1
		})
		if err != nil {
			log.Fatalf("Failed to create topic: %v", err)
		}
		fmt.Printf("   ✓ Topic created\n\n")
	} else {
		fmt.Printf("📂 Topic '%s' already exists\n\n", topicName)
	}

	// -------------------------------------------------------------------------
	// STEP 3: Publish some messages
	// -------------------------------------------------------------------------
	fmt.Println("📤 Publishing messages...")
	messages := []struct {
		Key   string
		Value string
	}{
		{"order-001", `{"id": "001", "product": "Widget", "qty": 5}`},
		{"order-002", `{"id": "002", "product": "Gadget", "qty": 3}`},
		{"order-003", `{"id": "003", "product": "Gizmo", "qty": 10}`},
		{"user-123", `{"user": "123", "action": "purchase"}`},
		{"user-456", `{"user": "456", "action": "view"}`},
	}

	for _, msg := range messages {
		partition, offset, err := b.Publish(
			topicName,
			[]byte(msg.Key),
			[]byte(msg.Value),
		)
		if err != nil {
			log.Printf("   ✗ Failed to publish %s: %v", msg.Key, err)
			continue
		}
		fmt.Printf("   ✓ Published: key=%s → partition=%d, offset=%d\n",
			msg.Key, partition, offset)
	}
	fmt.Println()

	// -------------------------------------------------------------------------
	// STEP 4: Consume messages
	// -------------------------------------------------------------------------
	fmt.Println("📥 Consuming messages from beginning...")
	consumed, err := b.Consume(topicName, 0, 0, 100) // partition 0, from offset 0, max 100
	if err != nil {
		log.Fatalf("Failed to consume: %v", err)
	}

	fmt.Printf("   Retrieved %d messages:\n\n", len(consumed))
	for _, m := range consumed {
		fmt.Printf("   ┌─ Offset: %d\n", m.Offset)
		fmt.Printf("   │  Time:   %s\n", m.Timestamp.Format(time.RFC3339))
		fmt.Printf("   │  Key:    %s\n", string(m.Key))
		fmt.Printf("   └─ Value:  %s\n\n", string(m.Value))
	}

	// -------------------------------------------------------------------------
	// STEP 5: Show statistics
	// -------------------------------------------------------------------------
	fmt.Println("📊 Broker Statistics:")
	stats := b.Stats()
	fmt.Printf("   Node ID:     %s\n", stats.NodeID)
	fmt.Printf("   Uptime:      %s\n", stats.Uptime.Round(time.Millisecond))
	fmt.Printf("   Topics:      %d\n", stats.TopicCount)
	fmt.Printf("   Total Size:  %d bytes\n", stats.TotalSize)
	for name, ts := range stats.TopicStats {
		fmt.Printf("\n   Topic '%s':\n", name)
		fmt.Printf("     Partitions: %d\n", ts.Partitions)
		fmt.Printf("     Messages:   %d\n", ts.TotalMessages)
		fmt.Printf("     Size:       %d bytes\n", ts.TotalSize)
	}

	// -------------------------------------------------------------------------
	// STEP 6: Demonstrate persistence
	// -------------------------------------------------------------------------
	fmt.Println()
	fmt.Println("💾 Persistence Demo:")
	fmt.Println("   Messages are now persisted to disk.")
	fmt.Println("   Restart the program to see messages survive restarts!")

	// -------------------------------------------------------------------------
	// STEP 7: Wait for interrupt (simple server mode)
	// -------------------------------------------------------------------------
	fmt.Println()
	fmt.Println("🚀 Broker running. Press Ctrl+C to stop.")

	// Set up signal handling for graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	fmt.Println("\n\n🛑 Shutting down...")
	fmt.Println("   ✓ Broker shutdown complete")
}
