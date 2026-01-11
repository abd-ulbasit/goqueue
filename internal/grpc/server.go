// =============================================================================
// gRPC SERVER - HIGH-PERFORMANCE API FOR GOQUEUE
// =============================================================================
//
// WHAT IS THIS?
// This module provides a gRPC server that offers:
//   - Low-latency message publishing (unary and streaming)
//   - Real-time message consumption (server streaming)
//   - Efficient ACK/NACK operations
//   - Offset management
//
// WHY gRPC IN ADDITION TO HTTP?
//
//   ┌─────────────────────────────────────────────────────────────────────────┐
//   │ Use Case                  │ HTTP (chi)        │ gRPC                    │
//   ├───────────────────────────┼───────────────────┼─────────────────────────┤
//   │ Admin operations          │ ✅ Simple         │ ⚠️ Overkill              │
//   │ Browser clients           │ ✅ Native         │ ❌ Needs grpc-web        │
//   │ Debugging/curl            │ ✅ Easy           │ ⚠️ Need grpcurl          │
//   │ High-throughput publish   │ ⚠️ OK             │ ✅ Streaming             │
//   │ Real-time consume         │ ⚠️ Long-polling   │ ✅ Server streaming      │
//   │ Batch ACKs                │ ⚠️ Multiple calls │ ✅ Single stream         │
//   └───────────────────────────┴───────────────────┴─────────────────────────┘
//
// ARCHITECTURE:
//
//   ┌──────────────────────────────────────────────────────────────────────────┐
//   │                              CLIENT                                      │
//   │                                                                          │
//   │   ┌─────────────────────────────────────────────────────────────────┐    │
//   │   │  gRPC Client (generated from proto)                             │    │
//   │   │  - Publish()        → Unary RPC                                 │    │
//   │   │  - PublishStream()  → Bidirectional streaming                   │    │
//   │   │  - Consume()        → Server streaming                          │    │
//   │   │  - Ack()/Nack()     → Unary RPC                                 │    │
//   │   └─────────────────────────────────────────────────────────────────┘    │
//   │                                │                                         │
//   │                         HTTP/2 │ (multiplexed)                           │
//   │                                │                                         │
//   └────────────────────────────────┼─────────────────────────────────────────┘
//                                    │
//   ┌────────────────────────────────┼─────────────────────────────────────────┐
//   │                              SERVER                                      │
//   │                                ▼                                         │
//   │   ┌─────────────────────────────────────────────────────────────────┐    │
//   │   │  gRPC Server (this file)                                        │    │
//   │   │  - Interceptors (logging, recovery, metrics)                    │    │
//   │   │  - Service implementations                                      │    │
//   │   └─────────────────────────────────────────────────────────────────┘    │
//   │                                │                                         │
//   │                                ▼                                         │
//   │   ┌─────────────────────────────────────────────────────────────────┐    │
//   │   │  Broker (internal/broker)                                       │    │
//   │   │  - Topic management                                             │    │
//   │   │  - Message storage                                              │    │
//   │   │  - Consumer groups                                              │    │
//   │   └─────────────────────────────────────────────────────────────────┘    │
//   │                                                                          │
//   └──────────────────────────────────────────────────────────────────────────┘
//
// PORT CONFIGURATION:
//   - HTTP API: :8080 (admin, debugging, simple clients)
//   - gRPC API: :9000 (high-performance clients)
//   - Cluster:  :7000 (inter-node communication)
//
// COMPARISON WITH OTHER SYSTEMS:
//   - Kafka: Custom binary protocol on single port (9092)
//   - Pulsar: Binary protocol + separate HTTP admin
//   - NATS: Custom text/binary protocol
//   - goqueue: gRPC + HTTP (modern, well-tooled)
//
// =============================================================================

package grpc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	"goqueue/internal/broker"
)

// =============================================================================
// SERVER CONFIGURATION
// =============================================================================

// ServerConfig holds gRPC server configuration.
type ServerConfig struct {
	// Address to listen on (e.g., ":9000")
	Address string

	// MaxRecvMsgSize is the max message size in bytes (default: 4MB)
	MaxRecvMsgSize int

	// MaxSendMsgSize is the max message size in bytes (default: 4MB)
	MaxSendMsgSize int

	// MaxConcurrentStreams per connection (default: 100)
	MaxConcurrentStreams uint32

	// Keepalive settings
	KeepaliveTime    time.Duration // How often to ping if no activity
	KeepaliveTimeout time.Duration // How long to wait for ping response

	// EnableReflection enables gRPC reflection for debugging tools
	// Set to true in development, false in production
	EnableReflection bool
}

// DefaultServerConfig returns sensible defaults.
//
// TUNING NOTES:
//   - MaxRecvMsgSize: 4MB handles most messages, increase for large payloads
//   - MaxConcurrentStreams: 100 is conservative, increase for high-fanout
//   - Keepalive: 30s ping, 10s timeout balances detection vs overhead
func DefaultServerConfig() ServerConfig {
	return ServerConfig{
		Address:              ":9000",
		MaxRecvMsgSize:       4 * 1024 * 1024, // 4MB
		MaxSendMsgSize:       4 * 1024 * 1024, // 4MB
		MaxConcurrentStreams: 100,
		KeepaliveTime:        30 * time.Second,
		KeepaliveTimeout:     10 * time.Second,
		EnableReflection:     true, // Enable for development
	}
}

// =============================================================================
// SERVER STRUCT
// =============================================================================

// Server is the gRPC server for goqueue.
type Server struct {
	config     ServerConfig
	broker     *broker.Broker
	grpcServer *grpc.Server
	logger     *slog.Logger

	// mu protects server state
	mu       sync.RWMutex
	running  bool
	listener net.Listener
}

// NewServer creates a new gRPC server.
//
// INITIALIZATION FLOW:
//  1. Create gRPC server with options (interceptors, limits)
//  2. Register all service implementations
//  3. Optionally enable reflection
//  4. Server is ready but not yet listening
func NewServer(b *broker.Broker, config ServerConfig) *Server {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	// ==========================================================================
	// gRPC SERVER OPTIONS
	// ==========================================================================
	//
	// WHY THESE OPTIONS?
	//
	// MaxRecvMsgSize/MaxSendMsgSize:
	//   Default gRPC limit is 4MB. For message queues, we might need larger
	//   messages (e.g., batch publishes). Configurable per-deployment.
	//
	// MaxConcurrentStreams:
	//   Limits how many streams a single client can have. Prevents a single
	//   client from consuming all server resources.
	//
	// Keepalive:
	//   - Time: How often to send PING if connection is idle
	//   - Timeout: How long to wait for PING response
	//   - Detects dead connections, especially behind NAT/firewalls
	//
	// COMPARISON:
	//   - Kafka: Custom keep-alive in protocol
	//   - HTTP/2: Built-in PING frames (gRPC uses these)
	//
	opts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(config.MaxRecvMsgSize),
		grpc.MaxSendMsgSize(config.MaxSendMsgSize),
		grpc.MaxConcurrentStreams(config.MaxConcurrentStreams),

		// Keepalive parameters
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    config.KeepaliveTime,
			Timeout: config.KeepaliveTimeout,
		}),

		// Keepalive enforcement policy
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			PermitWithoutStream: true,
			MinTime:             10 * time.Second,
		}),

		// Chain unary interceptors (middleware for unary RPCs)
		grpc.ChainUnaryInterceptor(
			unaryLoggingInterceptor(logger),
			unaryRecoveryInterceptor(logger),
		),

		// Chain stream interceptors (middleware for streaming RPCs)
		grpc.ChainStreamInterceptor(
			streamLoggingInterceptor(logger),
			streamRecoveryInterceptor(logger),
		),
	}

	grpcServer := grpc.NewServer(opts...)

	s := &Server{
		config:     config,
		broker:     b,
		grpcServer: grpcServer,
		logger:     logger,
	}

	// Register service implementations
	s.registerServices()

	// Enable reflection for debugging tools (grpcurl, grpcui)
	if config.EnableReflection {
		reflection.Register(grpcServer)
	}

	return s
}

// registerServices registers all gRPC service implementations.
func (s *Server) registerServices() {
	RegisterPublishServiceServer(s.grpcServer, NewPublishServiceServer(s.broker, s.logger))
	RegisterConsumeServiceServer(s.grpcServer, NewConsumeServiceServer(s.broker, s.logger))
	RegisterAckServiceServer(s.grpcServer, NewAckServiceServer(s.broker, s.logger))
	RegisterOffsetServiceServer(s.grpcServer, NewOffsetServiceServer(s.broker, s.logger))
	RegisterHealthServiceServer(s.grpcServer, NewHealthServiceServer(s.broker, s.logger))
}

// =============================================================================
// SERVER LIFECYCLE
// =============================================================================

// Start begins listening for gRPC connections.
//
// This method blocks until the server is stopped. Typically called in a goroutine:
//
//	go func() {
//	    if err := server.Start(); err != nil {
//	        log.Fatal(err)
//	    }
//	}()
func (s *Server) Start() error {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return errors.New("server already running")
	}

	listener, err := net.Listen("tcp", s.config.Address)
	if err != nil {
		s.mu.Unlock()
		return fmt.Errorf("failed to listen on %s: %w", s.config.Address, err)
	}

	s.listener = listener
	s.running = true
	s.mu.Unlock()

	s.logger.Info("gRPC server starting",
		"address", s.config.Address,
		"reflection", s.config.EnableReflection,
	)

	// Serve blocks until Stop() is called
	return s.grpcServer.Serve(listener)
}

// Stop gracefully shuts down the server.
//
// GRACEFUL SHUTDOWN FLOW:
//  1. Stop accepting new connections
//  2. Wait for existing RPCs to complete (with timeout)
//  3. Force close remaining connections
//
// WHY GRACEFUL?
//   - Don't lose in-flight publishes
//   - Let consumers finish processing current batch
//   - Allow offset commits to complete
func (s *Server) Stop() {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return
	}
	s.running = false
	s.mu.Unlock()

	s.logger.Info("gRPC server stopping...")

	// GracefulStop waits for existing RPCs to complete
	// Has built-in timeout, falls back to force stop
	s.grpcServer.GracefulStop()

	s.logger.Info("gRPC server stopped")
}

// Address returns the address the server is listening on.
// Useful when using port 0 for dynamic port assignment.
func (s *Server) Address() string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.listener != nil {
		return s.listener.Addr().String()
	}
	return s.config.Address
}

// =============================================================================
// INTERCEPTORS (MIDDLEWARE)
// =============================================================================
//
// WHAT ARE INTERCEPTORS?
// Interceptors in gRPC are like middleware in HTTP frameworks. They wrap
// RPCs to add cross-cutting concerns:
//   - Logging: Log all requests/responses
//   - Recovery: Catch panics, convert to errors
//   - Metrics: Record latency, counts
//   - Auth: Validate credentials
//
// TWO TYPES:
//   - Unary: For request-response RPCs
//   - Stream: For streaming RPCs (client, server, or bidirectional)
//
// EXECUTION ORDER (ChainUnaryInterceptor):
//   Request → Logging → Recovery → Handler
//   Response ← Logging ← Recovery ← Handler
//
// =============================================================================

// unaryLoggingInterceptor logs unary RPC calls.
func unaryLoggingInterceptor(logger *slog.Logger) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		start := time.Now()

		// Call the actual handler
		resp, err := handler(ctx, req)

		// Log after completion
		duration := time.Since(start)
		level := slog.LevelInfo
		if err != nil {
			level = slog.LevelError
		}

		logger.Log(ctx, level, "gRPC unary",
			"method", info.FullMethod,
			"duration_ms", duration.Milliseconds(),
			"error", err,
		)

		return resp, err
	}
}

// unaryRecoveryInterceptor catches panics and converts them to errors.
func unaryRecoveryInterceptor(logger *slog.Logger) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (resp interface{}, err error) {
		// Recover from panic
		defer func() {
			if r := recover(); r != nil {
				logger.Error("gRPC panic recovered",
					"method", info.FullMethod,
					"panic", r,
				)
				err = status.Errorf(codes.Internal, "internal server error")
			}
		}()

		return handler(ctx, req)
	}
}

// streamLoggingInterceptor logs streaming RPC calls.
func streamLoggingInterceptor(logger *slog.Logger) grpc.StreamServerInterceptor {
	return func(
		srv interface{},
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		start := time.Now()

		// Call the actual handler
		err := handler(srv, ss)

		// Log after stream closes
		duration := time.Since(start)
		level := slog.LevelInfo
		if err != nil {
			level = slog.LevelError
		}

		logger.Log(ss.Context(), level, "gRPC stream",
			"method", info.FullMethod,
			"duration_ms", duration.Milliseconds(),
			"client_stream", info.IsClientStream,
			"server_stream", info.IsServerStream,
			"error", err,
		)

		return err
	}
}

// streamRecoveryInterceptor catches panics in streaming RPCs.
func streamRecoveryInterceptor(logger *slog.Logger) grpc.StreamServerInterceptor {
	return func(
		srv interface{},
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) (err error) {
		defer func() {
			if r := recover(); r != nil {
				logger.Error("gRPC stream panic recovered",
					"method", info.FullMethod,
					"panic", r,
				)
				err = status.Errorf(codes.Internal, "internal server error")
			}
		}()

		return handler(srv, ss)
	}
}
