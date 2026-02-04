package storage

import (
	"testing"
)

// BenchmarkAppendSingle benchmarks single message append (old path)
func BenchmarkAppendSingle(b *testing.B) {
	dir := b.TempDir()
	log, err := NewLog(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer log.Close()

	msg := NewMessage([]byte("key"), make([]byte, 1024)) // 1KB message

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := log.Append(msg)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkAppendBatch benchmarks batch append (new path)
func BenchmarkAppendBatch(b *testing.B) {
	dir := b.TempDir()
	log, err := NewLog(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer log.Close()

	// Create batch of 100 messages
	batchSize := 100
	msgs := make([]*Message, batchSize)
	for i := 0; i < batchSize; i++ {
		msgs[i] = NewMessage([]byte("key"), make([]byte, 1024)) // 1KB message
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := log.AppendBatch(msgs)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(batchSize), "msgs/op")
}

// BenchmarkAppendBatch1000 benchmarks larger batch
func BenchmarkAppendBatch1000(b *testing.B) {
	dir := b.TempDir()
	log, err := NewLog(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer log.Close()

	batchSize := 1000
	msgs := make([]*Message, batchSize)
	for i := 0; i < batchSize; i++ {
		msgs[i] = NewMessage([]byte("key"), make([]byte, 100)) // 100B message
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := log.AppendBatch(msgs)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(batchSize), "msgs/op")
}

// BenchmarkAppendFastPath benchmarks the fast append + manual flush
func BenchmarkAppendFastPath(b *testing.B) {
	dir := b.TempDir()
	log, err := NewLog(dir)
	if err != nil {
		b.Fatal(err)
	}
	defer log.Close()

	msg := NewMessage([]byte("key"), make([]byte, 1024))
	batchSize := 100

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Write batch without flushing
		for j := 0; j < batchSize; j++ {
			_, err := log.AppendFast(msg)
			if err != nil {
				b.Fatal(err)
			}
		}
		// Flush once per batch
		if err := log.FlushBuffer(); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(batchSize), "msgs/op")
}
