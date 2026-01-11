// =============================================================================
// GOQUEUE MAIN ENTRY POINT
// =============================================================================
//
// This is the entry point for the goqueue broker. It demonstrates:
//   - Creating a broker with default configuration
//   - Creating multi-partition topics
//   - Using the Producer with client-side batching
//   - Message routing via consistent hashing (Murmur3)
//   - HTTP API server for external access
//   - Consuming messages from partitions
//   - Graceful shutdown
//
// MILESTONE 2 FEATURES DEMONSTRATED:
//   - Multi-partition topics (default 3 partitions)
//   - Producer batching (100 msgs, 5ms linger, 64KB)
//   - Murmur3 hash partitioning for message key routing
//   - HTTP REST API (create topics, publish, consume)
//
// =============================================================================

package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"goqueue/internal/api"
	"goqueue/internal/broker"
	"goqueue/internal/grpc"
)

func main() {
	fmt.Println("╔═══════════════════════════════════════════════════════════════╗")
	fmt.Println("║                     GoQueue v0.2.0                            ║")
	fmt.Println("║          Milestone 2: Topics, Partitions & Producer           ║")
	fmt.Println("╚═══════════════════════════════════════════════════════════════╝")
	fmt.Println()

	// -------------------------------------------------------------------------
	// STEP 1: Create broker with default configuration
	// -------------------------------------------------------------------------
	// ┌─────────────────────────────────────────────────────────────────────────┐
	// │ The Broker is the central component that:                               │
	// │   - Manages all topics and their partitions                             │
	// │   - Handles message persistence via the storage engine (M1)             │
	// │   - Coordinates producers and consumers                                 │
	// │                                                                         │
	// │ COMPARISON:                                                             │
	// │   - Kafka: Broker is a JVM process, typically 3+ in a cluster           │
	// │   - RabbitMQ: Broker = the entire RabbitMQ node                         │
	// │   - goqueue: Single broker for now (clustering in M10-M11)              │
	// └─────────────────────────────────────────────────────────────────────────┘
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
	// STEP 2: Create a multi-partition topic
	// -------------------------------------------------------------------------
	// ┌─────────────────────────────────────────────────────────────────────────┐
	// │ PARTITIONS - The Unit of Parallelism                                    │
	// │                                                                         │
	// │ A topic is split into partitions for:                                   │
	// │   1. PARALLELISM: Multiple consumers can read different partitions      │
	// │   2. ORDERING: Messages with same key go to same partition (ordered)    │
	// │   3. SCALABILITY: Partitions can be spread across nodes (future)        │
	// │                                                                         │
	// │ HOW IT WORKS:                                                           │
	// │   Producer ──► Topic ──┬── Partition 0 ──► Messages: A, D, G            │
	// │                        ├── Partition 1 ──► Messages: B, E, H            │
	// │                        └── Partition 2 ──► Messages: C, F, I            │
	// │                                                                         │
	// │ ROUTING DECISION:                                                       │
	// │   - With key: hash(key) % numPartitions → deterministic partition       │
	// │   - Without key: round-robin across partitions                          │
	// │                                                                         │
	// │ COMPARISON:                                                             │
	// │   - Kafka: Same model, partitions are fundamental unit                  │
	// │   - RabbitMQ: Queues (not partitions), different semantics              │
	// │   - SQS: FIFO queues have MessageGroupId (similar concept)              │
	// └─────────────────────────────────────────────────────────────────────────┘
	topicName := "demo-orders"
	numPartitions := 3

	if !b.TopicExists(topicName) {
		fmt.Printf("📝 Creating topic '%s' with %d partitions...\n", topicName, numPartitions)
		err := b.CreateTopic(broker.TopicConfig{
			Name:          topicName,
			NumPartitions: numPartitions,
		})
		if err != nil {
			log.Fatalf("Failed to create topic: %v", err)
		}
		fmt.Printf("   ✓ Topic created\n\n")
	} else {
		fmt.Printf("📂 Topic '%s' already exists\n\n", topicName)
	}

	// -------------------------------------------------------------------------
	// STEP 3: Create a Producer with batching
	// -------------------------------------------------------------------------
	// ┌─────────────────────────────────────────────────────────────────────────┐
	// │ PRODUCER BATCHING - Trading Latency for Throughput                      │
	// │                                                                         │
	// │ Instead of sending each message immediately:                            │
	// │   1. Messages accumulate in an in-memory buffer                         │
	// │   2. Batch is flushed when ANY trigger fires:                           │
	// │      - BatchSize reached (100 messages)                                 │
	// │      - LingerMs elapsed (5ms since first message)                       │
	// │      - BatchBytes exceeded (64KB total)                                 │
	// │                                                                         │
	// │ FLOW:                                                                   │
	// │   Send() ──► Batch Buffer ──[trigger]──► Flush to Broker               │
	// │                 │                                                       │
	// │                 ├── size >= 100?    ──► flush                           │
	// │                 ├── age >= 5ms?     ──► flush                           │
	// │                 └── bytes >= 64KB?  ──► flush                           │
	// │                                                                         │
	// │ COMPARISON:                                                             │
	// │   - Kafka: Same model (batch.size, linger.ms)                           │
	// │   - RabbitMQ: Publisher confirms, no client batching                    │
	// │   - SQS: SendMessageBatch (max 10 messages)                             │
	// └─────────────────────────────────────────────────────────────────────────┘
	fmt.Println("🚀 Creating Producer with batching enabled...")
	producerConfig := broker.ProducerConfig{
		Topic:      topicName,
		BatchSize:  10,        // Smaller for demo (normally 100)
		LingerMs:   50,        // 50ms - longer for demo visibility
		BatchBytes: 64 * 1024, // 64KB
		AckMode:    broker.AckLeader,
	}

	producer, err := broker.NewProducer(b, producerConfig)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	fmt.Printf("   ✓ Producer started (BatchSize=%d, LingerMs=%dms, AckMode=%s)\n\n",
		producerConfig.BatchSize, producerConfig.LingerMs, producerConfig.AckMode)

	// -------------------------------------------------------------------------
	// STEP 4: Publish messages with keys (demonstrates partitioning)
	// -------------------------------------------------------------------------
	// ┌─────────────────────────────────────────────────────────────────────────┐
	// │ KEY-BASED PARTITIONING - Ordering Guarantee                             │
	// │                                                                         │
	// │ When you provide a message key:                                         │
	// │   partition = murmur3(key) % numPartitions                              │
	// │                                                                         │
	// │ This ensures:                                                           │
	// │   - Same key ALWAYS goes to same partition                              │
	// │   - Messages for same key are ORDERED                                   │
	// │   - Different keys may share partitions (hash collisions)               │
	// │                                                                         │
	// │ EXAMPLE (3 partitions):                                                 │
	// │   "user-100" → murmur3 → partition 0                                    │
	// │   "user-200" → murmur3 → partition 2                                    │
	// │   "user-300" → murmur3 → partition 1                                    │
	// │   "user-100" → murmur3 → partition 0 (SAME!)                            │
	// │                                                                         │
	// │ WHY MURMUR3:                                                            │
	// │   - Fast (non-cryptographic)                                            │
	// │   - Excellent distribution (uniform across partitions)                  │
	// │   - Industry standard (Kafka default)                                   │
	// └─────────────────────────────────────────────────────────────────────────┘
	fmt.Println("📤 Publishing messages with keys (observing partition routing)...")
	messages := []struct {
		Key   string
		Value string
	}{
		// Orders from different users - should go to consistent partitions
		{"user-100", `{"order": "A", "user": "100", "product": "Widget"}`},
		{"user-200", `{"order": "B", "user": "200", "product": "Gadget"}`},
		{"user-300", `{"order": "C", "user": "300", "product": "Gizmo"}`},
		{"user-100", `{"order": "D", "user": "100", "product": "Sprocket"}`}, // Same user as A
		{"user-200", `{"order": "E", "user": "200", "product": "Cog"}`},      // Same user as B
		{"user-100", `{"order": "F", "user": "100", "product": "Bolt"}`},     // Same user as A, D
	}

	// Track which partition each user goes to
	userPartitions := make(map[string]int)

	for _, msg := range messages {
		// Use synchronous send for demo (easier to show partition assignment)
		ctx := context.Background()
		result := producer.SendSync(ctx, broker.ProducerRecord{
			Key:   []byte(msg.Key),
			Value: []byte(msg.Value),
		})
		if result.Error != nil {
			log.Printf("   ✗ Failed to publish: %v", result.Error)
			continue
		}

		partition := result.Partition
		offset := result.Offset

		// Track partition for this user
		if existing, ok := userPartitions[msg.Key]; ok {
			if existing != partition {
				fmt.Printf("   ⚠ PARTITION MISMATCH for %s! (expected %d, got %d)\n",
					msg.Key, existing, partition)
			}
		} else {
			userPartitions[msg.Key] = partition
		}

		fmt.Printf("   ✓ key=%-10s → partition=%d, offset=%d\n", msg.Key, partition, offset)
	}

	fmt.Println("\n   📊 Partition Assignment Summary:")
	for user, part := range userPartitions {
		fmt.Printf("      %s → Partition %d\n", user, part)
	}
	fmt.Println()

	// -------------------------------------------------------------------------
	// STEP 5: Consume messages from each partition
	// -------------------------------------------------------------------------
	fmt.Println("📥 Consuming messages from each partition...")
	for p := 0; p < numPartitions; p++ {
		consumed, err := b.Consume(topicName, p, 0, 100)
		if err != nil {
			log.Printf("   ✗ Failed to consume from partition %d: %v", p, err)
			continue
		}

		fmt.Printf("\n   Partition %d (%d messages):\n", p, len(consumed))
		for _, m := range consumed {
			key := string(m.Key)
			if key == "" {
				key = "(no key)"
			}
			fmt.Printf("      [offset=%d] key=%s\n", m.Offset, key)
		}
	}
	fmt.Println()

	// -------------------------------------------------------------------------
	// STEP 6: Start HTTP API Server
	// -------------------------------------------------------------------------
	// ┌─────────────────────────────────────────────────────────────────────────┐
	// │ HTTP API - External Access to GoQueue                                   │
	// │                                                                         │
	// │ Endpoints:                                                              │
	// │   GET  /health                              - Health check              │
	// │   GET  /stats                               - Broker statistics         │
	// │   POST /topics                              - Create topic              │
	// │   GET  /topics                              - List topics               │
	// │   GET  /topics/{name}                       - Get topic info            │
	// │   DELETE /topics/{name}                     - Delete topic              │
	// │   POST /topics/{name}/messages              - Publish messages          │
	// │   GET  /topics/{name}/partitions/{id}/msgs  - Consume messages          │
	// │                                                                         │
	// │ COMPARISON:                                                             │
	// │   - Kafka: Binary protocol (librdkafka), REST proxy separate            │
	// │   - RabbitMQ: AMQP protocol, HTTP management API                        │
	// │   - SQS: HTTP/REST API                                                  │
	// │   - goqueue: REST-first (gRPC planned for M14)                          │
	// └─────────────────────────────────────────────────────────────────────────┘
	fmt.Println("🌐 Starting HTTP API server...")
	serverConfig := api.DefaultServerConfig()
	serverConfig.Addr = "127.0.0.1:8080"

	server := api.NewServer(b, serverConfig)
	if err := server.Start(); err != nil {
		log.Fatalf("Failed to start HTTP server: %v", err)
	}

	fmt.Printf("   ✓ HTTP API listening on http://%s\n", serverConfig.Addr)
	fmt.Println()

	// -------------------------------------------------------------------------
	// STEP 6b: Start gRPC Server (M15)
	// -------------------------------------------------------------------------
	// ┌─────────────────────────────────────────────────────────────────────────┐
	// │ gRPC API - High-Performance Binary Protocol                             │
	// │                                                                         │
	// │ WHY gRPC alongside HTTP:                                                │
	// │   - HTTP: Easy debugging, curl-friendly, wide compatibility             │
	// │   - gRPC: High performance, streaming, type-safe (for hot path)         │
	// │                                                                         │
	// │ gRPC Services:                                                          │
	// │   PublishService  - Message publishing (unary + streaming)              │
	// │   ConsumeService  - Message consuming (streaming)                       │
	// │   AckService      - Message acknowledgment                              │
	// │   OffsetService   - Consumer offset management                          │
	// │   HealthService   - Health checking (gRPC standard)                     │
	// │                                                                         │
	// │ COMPARISON:                                                             │
	// │   - Kafka: Custom binary protocol over TCP                              │
	// │   - RabbitMQ: AMQP protocol (binary)                                    │
	// │   - NATS: Custom binary protocol                                        │
	// │   - goqueue: gRPC/HTTP2 with Protocol Buffers                           │
	// └─────────────────────────────────────────────────────────────────────────┘
	fmt.Println("🔌 Starting gRPC server...")

	grpcConfig := grpc.DefaultServerConfig()
	grpcConfig.Address = "127.0.0.1:9000"
	grpcConfig.EnableReflection = true // Enable for debugging with grpcurl

	grpcServer := grpc.NewServer(b, grpcConfig)
	if err := grpcServer.Start(); err != nil {
		log.Fatalf("Failed to start gRPC server: %v", err)
	}

	fmt.Printf("   ✓ gRPC API listening on %s\n", grpcConfig.Address)
	fmt.Println()

	fmt.Println("   Try these commands:")
	fmt.Println("   ┌────────────────────────────────────────────────────────────────────────┐")
	fmt.Println("   │ HTTP API (debugging):                                                  │")
	fmt.Println("   │   curl http://localhost:8080/health                                    │")
	fmt.Println("   │   curl http://localhost:8080/stats                                     │")
	fmt.Println("   │   curl http://localhost:8080/topics                                    │")
	fmt.Println("   │   curl -X POST -d '{\"name\":\"test\"}' http://localhost:8080/topics    │")
	fmt.Println("   │                                                                        │")
	fmt.Println("   │ gRPC API (high performance):                                           │")
	fmt.Println("   │   Use the goqueue Go client for gRPC operations                        │")
	fmt.Println("   │   grpcurl -plaintext localhost:9000 list                               │")
	fmt.Println("   │   grpcurl -plaintext localhost:9000 goqueue.v1.HealthService/Check     │")
	fmt.Println("   └────────────────────────────────────────────────────────────────────────┘")
	fmt.Println()

	// -------------------------------------------------------------------------
	// STEP 7: Show statistics
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
	// STEP 8: Wait for interrupt (server mode)
	// -------------------------------------------------------------------------
	fmt.Println()
	fmt.Println("🚀 GoQueue running. Press Ctrl+C to stop.")

	// Set up signal handling for graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	fmt.Println("\n\n🛑 Shutting down...")

	// Graceful shutdown with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Stop gRPC server first (handles in-flight RPCs)
	grpcServer.Stop()
	fmt.Println("   ✓ gRPC server stopped")

	// Stop HTTP server
	if err := server.Stop(ctx); err != nil {
		log.Printf("HTTP server shutdown error: %v", err)
	}
	fmt.Println("   ✓ HTTP server stopped")

	fmt.Println("   ✓ Shutdown complete")
}
