package broker

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"
)

func TestTransactionLog_NeedsSnapshotAndRemoveAll(t *testing.T) {
	// =============================================================================
	// WHY THIS TEST EXISTS
	// =============================================================================
	// The transaction log uses a snapshot + WAL approach.
	//
	// Two easy-to-regress behaviors live in "utility" methods that are still
	// production critical:
	//   1) NeedsSnapshot(): whether we should cut a snapshot based on WAL size.
	//   2) RemoveAll(): a dangerous helper used by tests that MUST refuse to
	//      delete files while the log is still open.
	// =============================================================================

	dir := t.TempDir()

	cfg := DefaultTransactionLogConfig(dir)
	cfg.EnableWAL = true
	cfg.WALSyncIntervalMs = 0 // sync every write so tests can immediately read
	cfg.MaxWALSizeBytes = 1   // tiny threshold: any write should exceed

	l, err := NewTransactionLog(cfg)
	if err != nil {
		t.Fatalf("NewTransactionLog failed: %v", err)
	}

	// Any record should increase WAL size above the tiny threshold.
	if err := l.WriteInitProducer("txn-1", 123, 0, 30000); err != nil {
		t.Fatalf("WriteInitProducer failed: %v", err)
	}

	if !l.NeedsSnapshot() {
		t.Fatalf("NeedsSnapshot = false, want true")
	}

	// RemoveAll must fail while the log is open (to prevent deleting a file
	// that is still being written to).
	if err := l.RemoveAll(); err == nil {
		t.Fatalf("RemoveAll = nil, want error while log is open")
	}

	if err := l.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if err := l.RemoveAll(); err != nil {
		t.Fatalf("RemoveAll failed after Close: %v", err)
	}

	// Best-effort verification: files should be gone.
	if _, err := os.Stat(l.snapshotPath); err == nil {
		t.Fatalf("snapshot file still exists after RemoveAll")
	}
	if _, err := os.Stat(l.walPath); err == nil {
		t.Fatalf("WAL file still exists after RemoveAll")
	}
}

func TestTransactionLog_ParseHelpers(t *testing.T) {
	// These parse helpers are small but they are effectively the "schema" layer
	// for replay, debugging, and future tooling.

	initRaw := json.RawMessage(`{"transactionalId":"t","producerId":1,"epoch":2,"transactionTimeoutMs":30000}`)
	initData, err := ParseInitProducerData(initRaw)
	if err != nil {
		t.Fatalf("ParseInitProducerData failed: %v", err)
	}
	if initData.TransactionalId != "t" || initData.ProducerId != 1 || initData.Epoch != 2 {
		t.Fatalf("ParseInitProducerData parsed unexpected values: %+v", initData)
	}

	beginRaw := json.RawMessage(`{"transactionalId":"t","transactionId":"x","producerId":9,"epoch":1}`)
	beginData, err := ParseBeginTxnData(beginRaw)
	if err != nil {
		t.Fatalf("ParseBeginTxnData failed: %v", err)
	}
	if beginData.TransactionId != "x" || beginData.ProducerId != 9 {
		t.Fatalf("ParseBeginTxnData parsed unexpected values: %+v", beginData)
	}

	seqRaw := json.RawMessage(`{"producerId":7,"topic":"orders","partition":3,"sequence":42,"offset":100}`)
	seqData, err := ParseUpdateSequenceData(seqRaw)
	if err != nil {
		t.Fatalf("ParseUpdateSequenceData failed: %v", err)
	}
	if seqData.Topic != "orders" || seqData.Partition != 3 || seqData.Sequence != 42 || seqData.Offset != 100 {
		t.Fatalf("ParseUpdateSequenceData parsed unexpected values: %+v", seqData)
	}

	// Invalid JSON must return an error.
	if _, err := ParseHeartbeatData(json.RawMessage(`{"transactionalId":`)); err == nil {
		t.Fatalf("ParseHeartbeatData = nil, want error for invalid JSON")
	}
}

func TestWALReader_SkipEmptyLinesAndEOF(t *testing.T) {
	dir := t.TempDir()
	walPath := dir + "/transactions.log"

	rec := WALRecord{
		Type:      WALRecordHeartbeat,
		Timestamp: time.Now(),
		Data:      json.RawMessage(`{"transactionalId":"t","producerId":1,"epoch":0}`),
	}
	b, err := json.Marshal(rec)
	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}

	// Leading + trailing empty lines ensure Next() exercises its recursion.
	content := "\n" + string(b) + "\n\n"
	if err := os.WriteFile(walPath, []byte(content), 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	r, err := NewWALReader(walPath)
	if err != nil {
		t.Fatalf("NewWALReader failed: %v", err)
	}
	defer r.Close()

	got, err := r.Next()
	if err != nil {
		t.Fatalf("Next failed: %v", err)
	}
	if got.Type != WALRecordHeartbeat {
		t.Fatalf("record type = %q, want %q", got.Type, WALRecordHeartbeat)
	}

	_, err = r.Next()
	if !errors.Is(err, io.EOF) {
		t.Fatalf("Next = %v, want io.EOF", err)
	}
}

func TestWALReader_InvalidJSON(t *testing.T) {
	dir := t.TempDir()
	walPath := dir + "/transactions.log"

	if err := os.WriteFile(walPath, []byte("not-json\n"), 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	r, err := NewWALReader(walPath)
	if err != nil {
		t.Fatalf("NewWALReader failed: %v", err)
	}
	defer r.Close()

	_, err = r.Next()
	if err == nil {
		t.Fatalf("Next = nil, want error")
	}
	if !strings.Contains(err.Error(), "failed to parse WAL record") {
		t.Fatalf("error = %v, want contains %q", err, "failed to parse WAL record")
	}
}
