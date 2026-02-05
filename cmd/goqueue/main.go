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
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"goqueue/internal/api"
	"goqueue/internal/broker"
	"goqueue/internal/grpc"
	"goqueue/internal/metrics"
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
	// │   - goqueue: Cluster mode enabled via GOQUEUE_CLUSTER_ENABLED           │
	// └─────────────────────────────────────────────────────────────────────────┘
	fmt.Println("📦 Starting broker...")
	config := broker.DefaultBrokerConfig()

	// Read data directory from environment variable (for Kubernetes deployments)
	// Falls back to ./data for local development
	if dataDir := os.Getenv("GOQUEUE_BROKER_DATADIR"); dataDir != "" {
		config.DataDir = dataDir
	} else {
		config.DataDir = "./data" // Default for local development
	}

	// ┌─────────────────────────────────────────────────────────────────────────┐
	// │ CLUSTER MODE CONFIGURATION                                              │
	// │                                                                         │
	// │ When GOQUEUE_CLUSTER_ENABLED=true, the broker runs in cluster mode:     │
	// │   - Joins other brokers via GOQUEUE_CLUSTER_PEERS                       │
	// │   - Participates in leader election                                     │
	// │   - Replicates data across nodes                                        │
	// │   - Synchronizes metadata (topics, partitions)                          │
	// │                                                                         │
	// │ KUBERNETES DEPLOYMENT:                                                  │
	// │   Each pod gets a stable DNS name via headless service:                 │
	// │     goqueue-0.goqueue-headless.namespace.svc.cluster.local:7000         │
	// │     goqueue-1.goqueue-headless.namespace.svc.cluster.local:7000         │
	// │     goqueue-2.goqueue-headless.namespace.svc.cluster.local:7000         │
	// │                                                                         │
	// │ COMPARISON:                                                             │
	// │   - Kafka: ZooKeeper (old) or KRaft (new) for coordination              │
	// │   - RabbitMQ: Erlang distribution for clustering                        │
	// │   - goqueue: Gossip-based membership + controller election              │
	// └─────────────────────────────────────────────────────────────────────────┘
	if os.Getenv("GOQUEUE_CLUSTER_ENABLED") == "true" {
		fmt.Println("🔗 Cluster mode enabled")
		config.ClusterEnabled = true

		// ClientAdvertiseAddress is used for inter-node forwarding (where producers
		// should connect when forwarded). Falls back to listener address if not set.
		clientAdvertise := os.Getenv("GOQUEUE_CLUSTER_CLIENT_ADVERTISE")
		if clientAdvertise == "" {
			clientAdvertise = normalizeAddr(getEnvOrDefault("GOQUEUE_LISTENERS_HTTP", "8080"))
		}

		config.ClusterConfig = &broker.ClusterModeConfig{
			// normalizeAddr ensures we have a valid :port format
			// Handles both "8080" and ":8080" input formats
			ClientAddress:    clientAdvertise, // Use advertised address for forwarding
			ClusterAddress:   normalizeAddr(getEnvOrDefault("GOQUEUE_LISTENERS_INTERNAL", "7000")),
			AdvertiseAddress: os.Getenv("GOQUEUE_CLUSTER_ADVERTISE"),
			Peers:            splitPeers(os.Getenv("GOQUEUE_CLUSTER_PEERS")),
			QuorumSize:       getEnvIntOrDefault("GOQUEUE_CLUSTER_QUORUM", 2),
		}
		fmt.Printf("   ✓ Peers: %v\n", config.ClusterConfig.Peers)
		fmt.Printf("   ✓ Advertise: %s\n", config.ClusterConfig.AdvertiseAddress)
		fmt.Printf("   ✓ Client Advertise: %s\n", config.ClusterConfig.ClientAddress)
	}

	// Read node ID from environment (for Kubernetes, this is the pod name)
	if nodeID := os.Getenv("GOQUEUE_BROKER_NODEID"); nodeID != "" {
		config.NodeID = nodeID
	}

	b, err := broker.NewBroker(config)
	if err != nil {
		log.Fatalf("Failed to create broker: %v", err)
	}
	defer b.Close()

	fmt.Printf("   ✓ Broker started (NodeID: %s)\n", b.NodeID())
	fmt.Printf("   ✓ Data directory: %s\n\n", b.DataDir())

	// -------------------------------------------------------------------------
	// STEP 1b: Initialize Prometheus Metrics (M17)
	// -------------------------------------------------------------------------
	// ┌─────────────────────────────────────────────────────────────────────────┐
	// │ PROMETHEUS METRICS - Observability for Production                       │
	// │                                                                         │
	// │ WHY METRICS MATTER:                                                     │
	// │   - Monitor message throughput (messages/sec)                           │
	// │   - Track latencies (p50, p95, p99)                                     │
	// │   - Alert on errors and anomalies                                       │
	// │   - Debug performance issues                                            │
	// │   - Capacity planning                                                   │
	// │                                                                         │
	// │ EXPOSED AT: http://localhost:8080/metrics                               │
	// │                                                                         │
	// │ METRICS CATEGORIES:                                                     │
	// │   - Broker: messages published/consumed, latencies, errors              │
	// │   - Storage: bytes written/read, fsync latency                          │
	// │   - Consumer: group members, lag, rebalances                            │
	// │   - Cluster: node health, leader elections, ISR changes                 │
	// │   - Go runtime: goroutines, memory, GC (optional)                       │
	// │                                                                         │
	// │ COMPARISON:                                                             │
	// │   - Kafka: JMX metrics, Confluent metrics reporter                      │
	// │   - RabbitMQ: Prometheus plugin                                         │
	// │   - SQS: CloudWatch metrics (AWS managed)                               │
	// │   - goqueue: Prometheus client_golang                                   │
	// └─────────────────────────────────────────────────────────────────────────┘
	fmt.Println("📊 Initializing Prometheus metrics...")
	metricsConfig := metrics.DefaultConfig()
	metricsConfig.Enabled = true
	metricsConfig.IncludeGoCollector = true      // Include Go runtime metrics
	metricsConfig.IncludeProcessCollector = true // Include process metrics
	metrics.Init(metricsConfig)
	fmt.Println("   ✓ Metrics initialized (endpoint: /metrics)")
	fmt.Println()

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

	// Read HTTP listener address from environment variable (for Kubernetes)
	if httpAddr := os.Getenv("GOQUEUE_LISTENERS_HTTP"); httpAddr != "" {
		serverConfig.Addr = httpAddr
	} else {
		serverConfig.Addr = "127.0.0.1:8080" // Default for local development
	}

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

	// Read gRPC listener address from environment variable (for Kubernetes)
	if grpcAddr := os.Getenv("GOQUEUE_LISTENERS_GRPC"); grpcAddr != "" {
		grpcConfig.Address = grpcAddr
	} else {
		grpcConfig.Address = "127.0.0.1:9000" // Default for local development
	}
	grpcConfig.EnableReflection = true // Enable for debugging with grpcurl

	grpcServer := grpc.NewServer(b, grpcConfig)

	// Start gRPC server in a goroutine (it blocks until Stop() is called)
	go func() {
		if err := grpcServer.Start(); err != nil {
			log.Fatalf("Failed to start gRPC server: %v", err)
		}
	}()

	// Small delay to ensure gRPC server is listening before marking as ready
	time.Sleep(100 * time.Millisecond)

	fmt.Printf("   ✓ gRPC API listening on %s\n", grpcConfig.Address)
	fmt.Println()

	// -------------------------------------------------------------------------
	// STEP 6c: Start Cluster HTTP Server (if cluster mode enabled)
	// -------------------------------------------------------------------------
	// ┌─────────────────────────────────────────────────────────────────────────┐
	// │ CLUSTER INTER-NODE COMMUNICATION                                        │
	// │                                                                         │
	// │ WHY SEPARATE HTTP SERVER:                                               │
	// │   - Cluster traffic is internal (between nodes), not client-facing      │
	// │   - Separate port (7000) allows network policies to isolate traffic     │
	// │   - Uses Go 1.22 http.ServeMux (simpler than chi for internal API)      │
	// │   - Must be running BEFORE coordinator tries to join cluster            │
	// │                                                                         │
	// │ ENDPOINTS (port 7000):                                                  │
	// │   POST /cluster/heartbeat  - Health check between nodes                 │
	// │   POST /cluster/join       - Node requesting to join cluster            │
	// │   POST /cluster/leave      - Node gracefully departing                  │
	// │   GET  /cluster/state      - Get cluster membership state               │
	// │   POST /cluster/vote       - Controller election vote request           │
	// │   POST /cluster/metadata   - Sync topic metadata from controller        │
	// │   GET  /cluster/health     - Cluster health status                      │
	// │                                                                         │
	// │ STARTUP ORDER (CRITICAL):                                               │
	// │   1. Main HTTP server starts (port 8080) ✓                              │
	// │   2. gRPC server starts (port 9000) ✓                                   │
	// │   3. Cluster HTTP server starts (port 7000) <- WE ARE HERE              │
	// │   4. Coordinator bootstrap (joins/forms cluster)                        │
	// │   5. Mark as ready                                                      │
	// │                                                                         │
	// │ This order ensures peers can receive join requests before anyone tries  │
	// │ to join, preventing the deadlock where all pods fail to connect.        │
	// └─────────────────────────────────────────────────────────────────────────┘
	if b.IsClusterEnabled() {
		// Get cluster address from config (default :7000)
		clusterAddr := getEnvOrDefault("GOQUEUE_LISTENERS_INTERNAL", ":7000")
		clusterAddr = normalizeAddr(clusterAddr)

		fmt.Printf("🔗 Starting cluster HTTP server on %s...\n", clusterAddr)

		// Create a separate http.ServeMux for cluster endpoints
		clusterMux := http.NewServeMux()
		b.RegisterClusterRoutes(clusterMux)

		// Create and start the cluster HTTP server
		clusterServer := &http.Server{
			Addr:         clusterAddr,
			Handler:      clusterMux,
			ReadTimeout:  30 * time.Second,
			WriteTimeout: 30 * time.Second,
			IdleTimeout:  60 * time.Second,
		}

		// Start cluster server in a goroutine
		go func() {
			if err := clusterServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Printf("Cluster HTTP server error: %v", err)
			}
		}()

		// Give the listener time to start
		time.Sleep(100 * time.Millisecond)
		fmt.Printf("   ✓ Cluster HTTP server listening on %s\n", clusterAddr)

		// Now that HTTP is listening, start the cluster coordinator
		fmt.Println("   ✓ Starting cluster coordinator...")
		if err := b.StartCluster(); err != nil {
			log.Fatalf("Failed to start cluster: %v", err)
		}
		fmt.Println("   ✓ Cluster coordinator started")
		fmt.Println()
	}

	// -------------------------------------------------------------------------
	// Mark server as ready for Kubernetes readiness probe
	// -------------------------------------------------------------------------
	api.GetHealthState().SetReady(true)
	fmt.Println("   ✓ Server marked as ready")
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

// =============================================================================
// HELPER FUNCTIONS FOR ENVIRONMENT CONFIGURATION
// =============================================================================

// getEnvOrDefault returns the environment variable value or a default.
func getEnvOrDefault(key, defaultValue string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultValue
}

// getEnvIntOrDefault returns the environment variable as int or a default.
func getEnvIntOrDefault(key string, defaultValue int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := parseIntSafe(v); err == nil {
			return i
		}
	}
	return defaultValue
}

// parseIntSafe parses a string to int safely.
func parseIntSafe(s string) (int, error) {
	var i int
	_, err := fmt.Sscanf(s, "%d", &i)
	return i, err
}

// splitPeers splits a comma-separated list of peers into a slice.
// Empty string returns empty slice (single-node mode).
func splitPeers(peers string) []string {
	if peers == "" {
		return nil
	}
	var result []string
	for _, p := range splitString(peers, ",") {
		p = trimSpace(p)
		if p != "" {
			result = append(result, p)
		}
	}
	return result
}

// splitString splits a string by separator (simple implementation).
func splitString(s, sep string) []string {
	if s == "" {
		return nil
	}
	var result []string
	start := 0
	for i := 0; i <= len(s)-len(sep); i++ {
		if s[i:i+len(sep)] == sep {
			result = append(result, s[start:i])
			start = i + len(sep)
			i += len(sep) - 1
		}
	}
	result = append(result, s[start:])
	return result
}

// trimSpace removes leading and trailing whitespace.
func trimSpace(s string) string {
	start := 0
	end := len(s)
	for start < end && (s[start] == ' ' || s[start] == '\t' || s[start] == '\n' || s[start] == '\r') {
		start++
	}
	for end > start && (s[end-1] == ' ' || s[end-1] == '\t' || s[end-1] == '\n' || s[end-1] == '\r') {
		end--
	}
	return s[start:end]
}

// normalizeAddr ensures address is in :port format.
// Handles these input formats:
//   - "8080"         → ":8080"
//   - ":8080"        → ":8080"
//   - "0.0.0.0:8080" → ":8080" (extracts port from full address)
//   - "host:8080"    → ":8080" (extracts port from host:port)
func normalizeAddr(addr string) string {
	addr = trimSpace(addr)
	if addr == "" {
		return ""
	}
	// Check if it contains a colon (could be host:port or just :port)
	if colonIdx := strings.LastIndex(addr, ":"); colonIdx >= 0 {
		// Extract just the port part
		port := addr[colonIdx:]
		return port
	}
	// No colon, assume it's just a port number
	return ":" + addr
}
