// =============================================================================
// SERVICE REGISTRATION - BRIDGE TO GENERATED CODE
// =============================================================================
//
// WHAT IS THIS?
// This file bridges our service implementations to the generated gRPC code.
// It re-exports the registration functions and types so our server.go doesn't
// need to import the generated package directly.
//
// WHY THIS INDIRECTION?
// 1. Cleaner imports in server.go
// 2. Single place to update if proto package path changes
// 3. Allows us to add custom registration logic later
//
// GENERATED CODE LOCATION:
//   api/proto/gen/go/goqueue.pb.go      - Message types
//   api/proto/gen/go/goqueue_grpc.pb.go - Service interfaces
//
// =============================================================================

package grpc

import (
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "goqueue/api/proto/gen/go"
)

// =============================================================================
// TYPE ALIASES
// =============================================================================
//
// These aliases allow our service implementations to use the generated types
// without needing to import the proto package directly.
// =============================================================================

// Proto message types
type (
	// Publish service
	PublishRequest        = pb.PublishRequest
	PublishResponse       = pb.PublishResponse
	PublishStreamRequest  = pb.PublishStreamRequest
	PublishStreamResponse = pb.PublishStreamResponse
	AckMode               = pb.AckMode

	// Consume service
	ConsumeRequest        = pb.ConsumeRequest
	ConsumeResponse       = pb.ConsumeResponse
	SubscribeRequest      = pb.SubscribeRequest
	Message               = pb.Message
	MessageBatch          = pb.MessageBatch
	Heartbeat             = pb.Heartbeat
	Assignment            = pb.Assignment
	RebalanceNotification = pb.RebalanceNotification
	TopicPartition        = pb.TopicPartition
	ConsumeStartPosition  = pb.ConsumeStartPosition

	// Ack service
	AckRequest               = pb.AckRequest
	AckResponse              = pb.AckResponse
	NackRequest              = pb.NackRequest
	NackResponse             = pb.NackResponse
	RejectRequest            = pb.RejectRequest
	RejectResponse           = pb.RejectResponse
	ExtendVisibilityRequest  = pb.ExtendVisibilityRequest
	ExtendVisibilityResponse = pb.ExtendVisibilityResponse
	BatchAckRequest          = pb.BatchAckRequest
	BatchAckResponse         = pb.BatchAckResponse
	OffsetAck                = pb.OffsetAck
	AckError                 = pb.AckError

	// Offset service
	CommitOffsetsRequest  = pb.CommitOffsetsRequest
	CommitOffsetsResponse = pb.CommitOffsetsResponse
	FetchOffsetsRequest   = pb.FetchOffsetsRequest
	FetchOffsetsResponse  = pb.FetchOffsetsResponse
	ResetOffsetsRequest   = pb.ResetOffsetsRequest
	ResetOffsetsResponse  = pb.ResetOffsetsResponse
	OffsetCommit          = pb.OffsetCommit
	OffsetCommitResult    = pb.OffsetCommitResult
	OffsetFetchResult     = pb.OffsetFetchResult
	OffsetResetResult     = pb.OffsetResetResult
	OffsetResetStrategy   = pb.OffsetResetStrategy

	// Health service
	HealthCheckRequest  = pb.HealthCheckRequest
	HealthCheckResponse = pb.HealthCheckResponse
	ServingStatus       = pb.ServingStatus

	// Transaction service
	InitProducerRequest         = pb.InitProducerRequest
	InitProducerResponse        = pb.InitProducerResponse
	BeginTransactionRequest     = pb.BeginTransactionRequest
	BeginTransactionResponse    = pb.BeginTransactionResponse
	AddPartitionRequest         = pb.AddPartitionRequest
	AddPartitionResponse        = pb.AddPartitionResponse
	CommitTransactionRequest    = pb.CommitTransactionRequest
	CommitTransactionResponse   = pb.CommitTransactionResponse
	AbortTransactionRequest     = pb.AbortTransactionRequest
	AbortTransactionResponse    = pb.AbortTransactionResponse
	ListTransactionsRequest     = pb.ListTransactionsRequest
	ListTransactionsResponse    = pb.ListTransactionsResponse
	DescribeTransactionRequest  = pb.DescribeTransactionRequest
	DescribeTransactionResponse = pb.DescribeTransactionResponse
	TransactionInfo             = pb.TransactionInfo
	TransactionPartition        = pb.TransactionPartition

	// Common types
	Error     = pb.Error
	ErrorCode = pb.ErrorCode

	// Oneof wrapper types for ConsumeResponse
	ConsumeResponse_Messages   = pb.ConsumeResponse_Messages
	ConsumeResponse_Heartbeat  = pb.ConsumeResponse_Heartbeat
	ConsumeResponse_Assignment = pb.ConsumeResponse_Assignment
	ConsumeResponse_Rebalance  = pb.ConsumeResponse_Rebalance
	ConsumeResponse_Error      = pb.ConsumeResponse_Error
)

// Constants
const (
	// Ack modes
	AckModeLeader = pb.AckMode_ACK_MODE_LEADER
	AckModeNone   = pb.AckMode_ACK_MODE_NONE
	AckModeAll    = pb.AckMode_ACK_MODE_ALL

	// Consume start positions
	ConsumeStartEarliest  = pb.ConsumeStartPosition_CONSUME_START_POSITION_EARLIEST
	ConsumeStartLatest    = pb.ConsumeStartPosition_CONSUME_START_POSITION_LATEST
	ConsumeStartOffset    = pb.ConsumeStartPosition_CONSUME_START_POSITION_OFFSET
	ConsumeStartTimestamp = pb.ConsumeStartPosition_CONSUME_START_POSITION_TIMESTAMP

	// Offset reset strategies
	OffsetResetEarliest  = pb.OffsetResetStrategy_OFFSET_RESET_STRATEGY_EARLIEST
	OffsetResetLatest    = pb.OffsetResetStrategy_OFFSET_RESET_STRATEGY_LATEST
	OffsetResetTimestamp = pb.OffsetResetStrategy_OFFSET_RESET_STRATEGY_TIMESTAMP
	OffsetResetOffset    = pb.OffsetResetStrategy_OFFSET_RESET_STRATEGY_OFFSET

	// Serving status
	ServingStatusUnknown        = pb.ServingStatus_SERVING_STATUS_UNKNOWN
	ServingStatusServing        = pb.ServingStatus_SERVING_STATUS_SERVING
	ServingStatusNotServing     = pb.ServingStatus_SERVING_STATUS_NOT_SERVING
	ServingStatusServiceUnknown = pb.ServingStatus_SERVING_STATUS_SERVICE_UNKNOWN

	// Error codes
	ErrorCodeUnspecified          = pb.ErrorCode_ERROR_CODE_UNSPECIFIED
	ErrorCodeTopicNotFound        = pb.ErrorCode_ERROR_CODE_TOPIC_NOT_FOUND
	ErrorCodeTopicAlreadyExists   = pb.ErrorCode_ERROR_CODE_TOPIC_ALREADY_EXISTS
	ErrorCodePartitionNotFound    = pb.ErrorCode_ERROR_CODE_PARTITION_NOT_FOUND
	ErrorCodeGroupNotFound        = pb.ErrorCode_ERROR_CODE_GROUP_NOT_FOUND
	ErrorCodeNotGroupMember       = pb.ErrorCode_ERROR_CODE_NOT_GROUP_MEMBER
	ErrorCodeRebalanceInProgress  = pb.ErrorCode_ERROR_CODE_REBALANCE_IN_PROGRESS
	ErrorCodeMessageTooLarge      = pb.ErrorCode_ERROR_CODE_MESSAGE_TOO_LARGE
	ErrorCodeReceiptHandleInvalid = pb.ErrorCode_ERROR_CODE_RECEIPT_HANDLE_INVALID
	ErrorCodeReceiptHandleExpired = pb.ErrorCode_ERROR_CODE_RECEIPT_HANDLE_EXPIRED

	// Transaction error codes
	ErrorCodeTransactionNotFound = pb.ErrorCode_ERROR_CODE_TRANSACTION_NOT_FOUND
	ErrorCodeTransactionTimeout  = pb.ErrorCode_ERROR_CODE_TRANSACTION_TIMEOUT
	ErrorCodeProducerFenced      = pb.ErrorCode_ERROR_CODE_PRODUCER_FENCED
	ErrorCodeInvalidTxnState     = pb.ErrorCode_ERROR_CODE_INVALID_TXN_STATE

	// Rebalance types
	RebalanceTypeUnspecified = pb.RebalanceType_REBALANCE_TYPE_UNSPECIFIED
	RebalanceTypeRevoke      = pb.RebalanceType_REBALANCE_TYPE_REVOKE
	RebalanceTypeAssign      = pb.RebalanceType_REBALANCE_TYPE_ASSIGN
)

// =============================================================================
// SERVICE INTERFACES
// =============================================================================
//
// These interfaces are defined in the generated code. Our implementations
// must satisfy these interfaces.
// =============================================================================

type (
	PublishServiceServer     = pb.PublishServiceServer
	ConsumeServiceServer     = pb.ConsumeServiceServer
	AckServiceServer         = pb.AckServiceServer
	OffsetServiceServer      = pb.OffsetServiceServer
	HealthServiceServer      = pb.HealthServiceServer
	TransactionServiceServer = pb.TransactionServiceServer
)

// Streaming interfaces
type (
	PublishService_PublishStreamServer = pb.PublishService_PublishStreamServer
	ConsumeService_ConsumeServer       = pb.ConsumeService_ConsumeServer
	ConsumeService_SubscribeServer     = pb.ConsumeService_SubscribeServer
	HealthService_WatchServer          = pb.HealthService_WatchServer
)

// =============================================================================
// REGISTRATION FUNCTIONS
// =============================================================================
//
// These functions register our service implementations with the gRPC server.
// Called during server initialization.
// =============================================================================

// RegisterPublishServiceServer registers the publish service.
func RegisterPublishServiceServer(s *grpc.Server, srv PublishServiceServer) {
	pb.RegisterPublishServiceServer(s, srv)
}

// RegisterConsumeServiceServer registers the consume service.
func RegisterConsumeServiceServer(s *grpc.Server, srv ConsumeServiceServer) {
	pb.RegisterConsumeServiceServer(s, srv)
}

// RegisterAckServiceServer registers the ack service.
func RegisterAckServiceServer(s *grpc.Server, srv AckServiceServer) {
	pb.RegisterAckServiceServer(s, srv)
}

// RegisterOffsetServiceServer registers the offset service.
func RegisterOffsetServiceServer(s *grpc.Server, srv OffsetServiceServer) {
	pb.RegisterOffsetServiceServer(s, srv)
}

// RegisterHealthServiceServer registers the health service.
func RegisterHealthServiceServer(s *grpc.Server, srv HealthServiceServer) {
	pb.RegisterHealthServiceServer(s, srv)
}

// RegisterTransactionServiceServer registers the transaction service.
func RegisterTransactionServiceServer(s *grpc.Server, srv TransactionServiceServer) {
	pb.RegisterTransactionServiceServer(s, srv)
}

// =============================================================================
// UNIMPLEMENTED SERVERS (for embedding)
// =============================================================================
//
// gRPC requires that unimplemented methods return "unimplemented" errors.
// These embedded structs provide that behavior automatically.
// =============================================================================

type (
	UnimplementedPublishServiceServer     = pb.UnimplementedPublishServiceServer
	UnimplementedConsumeServiceServer     = pb.UnimplementedConsumeServiceServer
	UnimplementedAckServiceServer         = pb.UnimplementedAckServiceServer
	UnimplementedOffsetServiceServer      = pb.UnimplementedOffsetServiceServer
	UnimplementedHealthServiceServer      = pb.UnimplementedHealthServiceServer
	UnimplementedTransactionServiceServer = pb.UnimplementedTransactionServiceServer
)

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================
//
// Common helper functions shared across service implementations.
// =============================================================================

// mapBrokerError converts a broker error to a gRPC status error.
// This provides proper gRPC error codes based on the error type.
func mapBrokerError(err error) error {
	if err == nil {
		return nil
	}

	errMsg := err.Error()

	// Map to gRPC status codes
	switch {
	case contains(errMsg, "not found"):
		return status.Error(codes.NotFound, errMsg)
	case contains(errMsg, "already exists"):
		return status.Error(codes.AlreadyExists, errMsg)
	case contains(errMsg, "invalid"):
		return status.Error(codes.InvalidArgument, errMsg)
	case contains(errMsg, "closed"):
		return status.Error(codes.Unavailable, errMsg)
	case contains(errMsg, "timeout"):
		return status.Error(codes.DeadlineExceeded, errMsg)
	default:
		return status.Error(codes.Internal, errMsg)
	}
}

// contains checks if a string contains a substring (case-insensitive).
func contains(s, substr string) bool {
	return strings.Contains(strings.ToLower(s), strings.ToLower(substr))
}
