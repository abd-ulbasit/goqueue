package api

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"

	"goqueue/internal/broker"
)

// =============================================================================
// TRANSACTION HANDLERS (M9)
// =============================================================================
//
// These handlers implement the transactional producer API for exactly-once
// semantics (EOS).
//
// KAFKA COMPARISON:
//   - Kafka clients use initTransactions(), beginTransaction(), send(),
//     commitTransaction(), abortTransaction()
//   - goqueue exposes these as HTTP endpoints for language-agnostic access
//
// THREAD SAFETY:
//   All handler methods are thread-safe. The transaction coordinator handles
//   all synchronization internally.
//

// =============================================================================
// PRODUCER INITIALIZATION
// =============================================================================

// initProducer handles POST /producers/init
//
// Initializes a producer and returns a Producer ID (PID) and epoch.
// If the producer was previously registered (via transactional_id), it will
// get the same PID with an incremented epoch (zombie fencing).
//
// REQUEST BODY:
//
//	{
//	  "transactional_id": "my-producer"  // Required for transactional producers
//	}
//
// RESPONSE:
//
//	{
//	  "producer_id": 123,
//	  "epoch": 1
//	}
//
// ERRORS:
//   - 400 Bad Request: Invalid JSON or missing transactional_id
//   - 500 Internal Server Error: Coordinator error
//
// ZOMBIE FENCING:
//
//	When a producer re-initializes with the same transactional_id:
//	  - Epoch is incremented (e.g., from 1 to 2)
//	  - Any in-progress transaction from the old epoch is aborted
//	  - Requests from the old epoch are rejected
//
//	This prevents "zombie" producers (e.g., a crashed producer that restarted)
//	from interfering with the new producer instance.
func (s *Server) initProducer(w http.ResponseWriter, r *http.Request) {
	var req struct {
		TransactionalID      string `json:"transactional_id"`
		TransactionTimeoutMs int64  `json:"transaction_timeout_ms"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	if req.TransactionalID == "" {
		s.errorResponse(w, http.StatusBadRequest, "transactional_id is required")
		return
	}

	// Default timeout if not specified
	timeoutMs := req.TransactionTimeoutMs
	if timeoutMs == 0 {
		timeoutMs = 60000 // 60 seconds default
	}

	// Get transaction coordinator
	coord := s.broker.GetTransactionCoordinator()
	if coord == nil {
		s.errorResponse(w, http.StatusInternalServerError, "transaction coordinator not available")
		return
	}

	// Initialize producer
	pid, err := coord.InitProducerID(req.TransactionalID, timeoutMs)
	if err != nil {
		s.errorResponse(w, http.StatusInternalServerError, "failed to init producer: "+err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"producer_id": pid.ProducerID,
		"epoch":       pid.Epoch,
	})
}

// producerHeartbeat handles POST /producers/{producerID}/heartbeat
//
// Sends a heartbeat to keep the producer session alive.
// If no heartbeat is received within the session timeout (default 30s),
// any active transaction is aborted and the producer is marked as dead.
//
// URL PARAMETERS:
//   - producerID: The producer ID (from /producers/init)
//
// REQUEST BODY:
//
//	{
//	  "transactional_id": "my-producer"
//	}
//
// RESPONSE:
//
//	{
//	  "status": "ok"
//	}
//
// ERRORS:
//   - 400 Bad Request: Invalid producer ID or missing transactional_id
//   - 404 Not Found: Producer not found
//   - 409 Conflict: Producer has been fenced (newer epoch exists)
func (s *Server) producerHeartbeat(w http.ResponseWriter, r *http.Request) {
	producerIDStr := chi.URLParam(r, "producerID")
	producerID, err := strconv.ParseInt(producerIDStr, 10, 64)
	if err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid producer_id")
		return
	}

	var req struct {
		TransactionalID string `json:"transactional_id"`
		Epoch           int16  `json:"epoch"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	if req.TransactionalID == "" {
		s.errorResponse(w, http.StatusBadRequest, "transactional_id is required")
		return
	}

	coord := s.broker.GetTransactionCoordinator()
	if coord == nil {
		s.errorResponse(w, http.StatusInternalServerError, "transaction coordinator not available")
		return
	}

	pid := broker.ProducerIDAndEpoch{
		ProducerID: producerID,
		Epoch:      req.Epoch,
	}

	if err := coord.Heartbeat(req.TransactionalID, pid); err != nil {
		if strings.Contains(err.Error(), "not found") {
			s.errorResponse(w, http.StatusNotFound, err.Error())
			return
		}
		if strings.Contains(err.Error(), "fenced") || strings.Contains(err.Error(), "epoch") {
			s.errorResponse(w, http.StatusConflict, err.Error())
			return
		}
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"status": "ok",
	})
}

// =============================================================================
// TRANSACTION LIFECYCLE
// =============================================================================

// beginTransaction handles POST /transactions/begin
//
// Begins a new transaction for the producer. A producer can only have one
// active transaction at a time.
//
// REQUEST BODY:
//
//	{
//	  "producer_id": 123,
//	  "epoch": 1,
//	  "transactional_id": "my-producer"
//	}
//
// RESPONSE:
//
//	{
//	  "transaction_id": "txn-123-1-1234567890"
//	}
//
// ERRORS:
//   - 400 Bad Request: Invalid JSON or missing fields
//   - 404 Not Found: Producer not found
//   - 409 Conflict: Producer fenced or transaction already in progress
func (s *Server) beginTransaction(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ProducerID      int64  `json:"producer_id"`
		Epoch           int16  `json:"epoch"`
		TransactionalID string `json:"transactional_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	if req.TransactionalID == "" {
		s.errorResponse(w, http.StatusBadRequest, "transactional_id is required")
		return
	}

	coord := s.broker.GetTransactionCoordinator()
	if coord == nil {
		s.errorResponse(w, http.StatusInternalServerError, "transaction coordinator not available")
		return
	}

	pid := broker.ProducerIDAndEpoch{
		ProducerID: req.ProducerID,
		Epoch:      req.Epoch,
	}

	txnID, err := coord.BeginTransaction(req.TransactionalID, pid)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			s.errorResponse(w, http.StatusNotFound, err.Error())
			return
		}
		if strings.Contains(err.Error(), "fenced") || strings.Contains(err.Error(), "in progress") {
			s.errorResponse(w, http.StatusConflict, err.Error())
			return
		}
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"transaction_id": txnID,
	})
}

// publishTransactional handles POST /transactions/publish
//
// Publishes a message as part of an active transaction. The message is written
// to the partition log but not visible to consumers until the transaction commits.
//
// REQUEST BODY:
//
//	{
//	  "producer_id": 123,
//	  "epoch": 1,
//	  "topic": "orders",
//	  "partition": -1,           // -1 for auto-partition based on key
//	  "key": "order-123",        // Base64-encoded or string
//	  "value": "{...}",          // Message payload
//	  "sequence": 0              // Sequence number for deduplication
//	}
//
// RESPONSE:
//
//	{
//	  "partition": 0,
//	  "offset": 42,
//	  "duplicate": false
//	}
//
// SEQUENCE NUMBERS:
//
//	The sequence number is per-partition and must be monotonically increasing.
//	  - Start from 0 for each partition
//	  - Increment by 1 for each message
//	  - Duplicates (same sequence) return success without re-writing
//
// ERRORS:
//   - 400 Bad Request: Invalid JSON, missing fields, or invalid sequence
//   - 404 Not Found: Topic not found
//   - 409 Conflict: Producer fenced or no active transaction
func (s *Server) publishTransactional(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ProducerID int64  `json:"producer_id"`
		Epoch      int16  `json:"epoch"`
		Topic      string `json:"topic"`
		Partition  int    `json:"partition"`
		Key        string `json:"key"`
		Value      string `json:"value"`
		Sequence   int32  `json:"sequence"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	if req.Topic == "" {
		s.errorResponse(w, http.StatusBadRequest, "topic is required")
		return
	}

	partition, offset, duplicate, err := s.broker.PublishTransactional(
		req.Topic,
		req.Partition,
		[]byte(req.Key),
		[]byte(req.Value),
		req.ProducerID,
		req.Epoch,
		req.Sequence,
	)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			s.errorResponse(w, http.StatusNotFound, err.Error())
			return
		}
		if strings.Contains(err.Error(), "sequence") {
			s.errorResponse(w, http.StatusBadRequest, err.Error())
			return
		}
		if strings.Contains(err.Error(), "fenced") {
			s.errorResponse(w, http.StatusConflict, err.Error())
			return
		}
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"partition": partition,
		"offset":    offset,
		"duplicate": duplicate,
	})
}

// addPartitionToTransaction handles POST /transactions/add-partition
//
// Adds a topic-partition to the transaction scope. This is typically called
// automatically by publishTransactional, but can be called explicitly for
// partitions that will receive messages later.
//
// REQUEST BODY:
//
//	{
//	  "producer_id": 123,
//	  "epoch": 1,
//	  "transactional_id": "my-producer",
//	  "topic": "orders",
//	  "partition": 0
//	}
//
// RESPONSE:
//
//	{
//	  "status": "added"
//	}
func (s *Server) addPartitionToTransaction(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ProducerID      int64  `json:"producer_id"`
		Epoch           int16  `json:"epoch"`
		TransactionalID string `json:"transactional_id"`
		Topic           string `json:"topic"`
		Partition       int    `json:"partition"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	coord := s.broker.GetTransactionCoordinator()
	if coord == nil {
		s.errorResponse(w, http.StatusInternalServerError, "transaction coordinator not available")
		return
	}

	pid := broker.ProducerIDAndEpoch{
		ProducerID: req.ProducerID,
		Epoch:      req.Epoch,
	}

	err := coord.AddPartitionToTransaction(req.TransactionalID, pid, req.Topic, req.Partition)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			s.errorResponse(w, http.StatusNotFound, err.Error())
			return
		}
		if strings.Contains(err.Error(), "fenced") {
			s.errorResponse(w, http.StatusConflict, err.Error())
			return
		}
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"status": "added",
	})
}

// commitTransaction handles POST /transactions/commit
//
// Commits the active transaction, making all messages visible to consumers.
// This is a two-phase commit:
//  1. Write COMMIT control records to all partitions
//  2. Mark transaction as completed
//
// REQUEST BODY:
//
//	{
//	  "producer_id": 123,
//	  "epoch": 1,
//	  "transactional_id": "my-producer"
//	}
//
// RESPONSE:
//
//	{
//	  "status": "committed",
//	  "partitions_committed": 3
//	}
//
// ERRORS:
//   - 400 Bad Request: Invalid JSON or missing fields
//   - 404 Not Found: No active transaction
//   - 409 Conflict: Producer fenced
//   - 500 Internal Server Error: Commit failed
func (s *Server) commitTransaction(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ProducerID      int64  `json:"producer_id"`
		Epoch           int16  `json:"epoch"`
		TransactionalID string `json:"transactional_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	if req.TransactionalID == "" {
		s.errorResponse(w, http.StatusBadRequest, "transactional_id is required")
		return
	}

	coord := s.broker.GetTransactionCoordinator()
	if coord == nil {
		s.errorResponse(w, http.StatusInternalServerError, "transaction coordinator not available")
		return
	}

	pid := broker.ProducerIDAndEpoch{
		ProducerID: req.ProducerID,
		Epoch:      req.Epoch,
	}

	err := coord.CommitTransaction(req.TransactionalID, pid)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			s.errorResponse(w, http.StatusNotFound, err.Error())
			return
		}
		if strings.Contains(err.Error(), "fenced") {
			s.errorResponse(w, http.StatusConflict, err.Error())
			return
		}
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"status": "committed",
	})
}

// abortTransaction handles POST /transactions/abort
//
// Aborts the active transaction, discarding all messages written within it.
// This writes ABORT control records to all partitions.
//
// REQUEST BODY:
//
//	{
//	  "producer_id": 123,
//	  "epoch": 1,
//	  "transactional_id": "my-producer"
//	}
//
// RESPONSE:
//
//	{
//	  "status": "aborted",
//	  "partitions_aborted": 3
//	}
//
// ERRORS:
//   - 400 Bad Request: Invalid JSON or missing fields
//   - 404 Not Found: No active transaction
//   - 409 Conflict: Producer fenced
func (s *Server) abortTransaction(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ProducerID      int64  `json:"producer_id"`
		Epoch           int16  `json:"epoch"`
		TransactionalID string `json:"transactional_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	if req.TransactionalID == "" {
		s.errorResponse(w, http.StatusBadRequest, "transactional_id is required")
		return
	}

	coord := s.broker.GetTransactionCoordinator()
	if coord == nil {
		s.errorResponse(w, http.StatusInternalServerError, "transaction coordinator not available")
		return
	}

	pid := broker.ProducerIDAndEpoch{
		ProducerID: req.ProducerID,
		Epoch:      req.Epoch,
	}

	err := coord.AbortTransaction(req.TransactionalID, pid)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			s.errorResponse(w, http.StatusNotFound, err.Error())
			return
		}
		if strings.Contains(err.Error(), "fenced") {
			s.errorResponse(w, http.StatusConflict, err.Error())
			return
		}
		s.errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"status": "aborted",
	})
}

// =============================================================================
// TRANSACTION MONITORING
// =============================================================================

// listTransactions handles GET /transactions
//
// Lists all active transactions.
//
// QUERY PARAMETERS:
//   - limit: Maximum number of transactions to return (default: 100)
//
// RESPONSE:
//
//	{
//	  "transactions": [
//	    {
//	      "transaction_id": "txn-123-1-1234567890",
//	      "transactional_id": "my-producer",
//	      "producer_id": 123,
//	      "epoch": 1,
//	      "state": "Ongoing",
//	      "start_time": "2024-01-15T10:30:00Z",
//	      "partitions": [
//	        {"topic": "orders", "partition": 0}
//	      ]
//	    }
//	  ],
//	  "count": 1
//	}
func (s *Server) listTransactions(w http.ResponseWriter, r *http.Request) {
	coord := s.broker.GetTransactionCoordinator()
	if coord == nil {
		s.errorResponse(w, http.StatusInternalServerError, "transaction coordinator not available")
		return
	}

	transactions := coord.GetActiveTransactions()

	// Convert to JSON-friendly format
	type txnPartition struct {
		Topic     string `json:"topic"`
		Partition int    `json:"partition"`
	}
	type txnInfo struct {
		TransactionID   string         `json:"transaction_id"`
		TransactionalID string         `json:"transactional_id"`
		ProducerID      int64          `json:"producer_id"`
		Epoch           int16          `json:"epoch"`
		State           string         `json:"state"`
		StartTime       time.Time      `json:"start_time"`
		Partitions      []txnPartition `json:"partitions"`
	}

	txnList := make([]txnInfo, 0, len(transactions))
	for _, txn := range transactions {
		// Convert map[string]map[int]struct{} to []txnPartition
		partitions := make([]txnPartition, 0)
		for topic, partMap := range txn.Partitions {
			for partition := range partMap {
				partitions = append(partitions, txnPartition{
					Topic:     topic,
					Partition: partition,
				})
			}
		}
		txnList = append(txnList, txnInfo{
			TransactionID:   txn.TransactionID,
			TransactionalID: txn.TransactionalID,
			ProducerID:      txn.ProducerID,
			Epoch:           txn.Epoch,
			State:           txn.State.String(),
			StartTime:       txn.StartTime,
			Partitions:      partitions,
		})
	}

	s.writeJSON(w, http.StatusOK, map[string]interface{}{
		"transactions": txnList,
		"count":        len(txnList),
	})
}

// handleTransactionStats handles GET /transactions/stats
//
// Returns transaction coordinator statistics.
//
// RESPONSE:
//
//	{
//	  "total_producers": 10,
//	  "transactional_producers": 5,
//	  "active_transactions": 2,
//	  "total_transactions_committed": 150,
//	  "total_transactions_aborted": 3,
//	  "total_transactions_timed_out": 1
//	}
func (s *Server) handleTransactionStats(w http.ResponseWriter, r *http.Request) {
	coord := s.broker.GetTransactionCoordinator()
	if coord == nil {
		s.errorResponse(w, http.StatusInternalServerError, "transaction coordinator not available")
		return
	}

	stats := coord.Stats()
	s.writeJSON(w, http.StatusOK, stats)
}
