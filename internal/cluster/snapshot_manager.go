// =============================================================================
// SNAPSHOT MANAGER - FAST FOLLOWER CATCH-UP
// =============================================================================
//
// WHAT: Creates and manages partition snapshots for fast follower catch-up.
//
// WHY SNAPSHOTS?
//   ┌─────────────────────────────────────────────────────────────────────────┐
//   │                    THE SLOW CATCH-UP PROBLEM                            │
//   │                                                                         │
//   │   Scenario: New follower joins, or old follower was down for hours      │
//   │                                                                         │
//   │   Leader LEO: 1,000,000  (1 million messages)                           │
//   │   Follower:   0          (needs everything!)                            │
//   │                                                                         │
//   │   Without snapshot:                                                     │
//   │     - Fetch 1,000,000 messages one batch at a time                      │
//   │     - At 1000 msg/fetch × 500ms = 500 seconds = 8+ minutes!             │
//   │                                                                         │
//   │   With snapshot:                                                        │
//   │     - Download compressed snapshot (e.g., 100MB)                        │
//   │     - Load snapshot → follower at offset 999,000                        │
//   │     - Fetch remaining 1,000 messages = 0.5 seconds                      │
//   │     - Total: ~10 seconds for 100MB download + 0.5s fetch                │
//   │                                                                         │
//   └─────────────────────────────────────────────────────────────────────────┘
//
// SNAPSHOT CREATION (Leader):
//   1. Pause writes momentarily (or use copy-on-write)
//   2. Copy log segments to snapshot directory
//   3. Record snapshot metadata (last included offset, epoch)
//   4. Compress snapshot (gzip/zstd)
//   5. Calculate checksum
//
// SNAPSHOT LOADING (Follower):
//   1. Download snapshot from leader
//   2. Verify checksum
//   3. Decompress to temporary directory
//   4. Replace local log with snapshot
//   5. Set fetch offset to lastIncludedOffset + 1
//
// COMPARISON:
//   - Kafka: Uses log compaction and segment copying
//   - Raft: Explicit install_snapshot RPC
//   - etcd: Snapshot streaming with chunk transfer
//   - goqueue: Compressed tarball + HTTP download
//
// =============================================================================

package cluster

import (
	"archive/tar"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// =============================================================================
// SNAPSHOT MANAGER
// =============================================================================

// SnapshotManager handles snapshot creation and loading.
type SnapshotManager struct {
	// dataDir is the base data directory.
	dataDir string

	// snapshotDir is where snapshots are stored.
	snapshotDir string

	// snapshots maps "topic-partition" to available snapshot.
	snapshots map[string]*Snapshot

	// mu protects snapshots map.
	mu sync.RWMutex

	// logger for operations.
	logger *slog.Logger
}

// NewSnapshotManager creates a new snapshot manager.
func NewSnapshotManager(dataDir string, logger *slog.Logger) *SnapshotManager {
	snapshotDir := filepath.Join(dataDir, "snapshots")

	// Ensure snapshot directory exists.
	if err := os.MkdirAll(snapshotDir, 0755); err != nil {
		logger.Error("failed to create snapshot directory", "error", err)
	}

	sm := &SnapshotManager{
		dataDir:     dataDir,
		snapshotDir: snapshotDir,
		snapshots:   make(map[string]*Snapshot),
		logger:      logger.With("component", "snapshot-manager"),
	}

	// Load existing snapshots.
	sm.loadExistingSnapshots()

	return sm
}

// =============================================================================
// SNAPSHOT CREATION (LEADER)
// =============================================================================

// CreateSnapshot creates a snapshot of a partition.
// Called by leader when a follower is too far behind.
func (sm *SnapshotManager) CreateSnapshot(topic string, partition int, logDir string, lastOffset int64, epoch int64) (*Snapshot, error) {
	key := partitionKey(topic, partition)
	sm.logger.Info("creating snapshot",
		"topic", topic,
		"partition", partition,
		"last_offset", lastOffset)

	startTime := time.Now()

	// Create snapshot file path.
	snapshotName := fmt.Sprintf("%s-%d-%d.tar.gz", topic, partition, lastOffset)
	snapshotPath := filepath.Join(sm.snapshotDir, snapshotName)

	// Create snapshot file.
	snapshotFile, err := os.Create(snapshotPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create snapshot file: %w", err)
	}

	// Create gzip writer.
	gzWriter := gzip.NewWriter(snapshotFile)

	// Create tar writer.
	tarWriter := tar.NewWriter(gzWriter)

	// Walk the log directory and add all files.
	err = filepath.Walk(logDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip directories (tar handles them implicitly).
		if info.IsDir() {
			return nil
		}

		// Create tar header.
		relPath, err := filepath.Rel(logDir, path)
		if err != nil {
			return err
		}

		header := &tar.Header{
			Name:    relPath,
			Size:    info.Size(),
			Mode:    int64(info.Mode()),
			ModTime: info.ModTime(),
		}

		if err := tarWriter.WriteHeader(header); err != nil {
			return fmt.Errorf("failed to write tar header: %w", err)
		}

		// Copy file content.
		file, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("failed to open file: %w", err)
		}
		defer file.Close()

		if _, err := io.Copy(tarWriter, file); err != nil {
			return fmt.Errorf("failed to copy file: %w", err)
		}

		return nil
	})

	if err != nil {
		tarWriter.Close()
		gzWriter.Close()
		snapshotFile.Close()
		os.Remove(snapshotPath)
		return nil, fmt.Errorf("failed to create tar archive: %w", err)
	}

	// Close writers.
	if err := tarWriter.Close(); err != nil {
		gzWriter.Close()
		snapshotFile.Close()
		os.Remove(snapshotPath)
		return nil, fmt.Errorf("failed to close tar writer: %w", err)
	}

	if err := gzWriter.Close(); err != nil {
		snapshotFile.Close()
		os.Remove(snapshotPath)
		return nil, fmt.Errorf("failed to close gzip writer: %w", err)
	}

	if err := snapshotFile.Close(); err != nil {
		os.Remove(snapshotPath)
		return nil, fmt.Errorf("failed to close snapshot file: %w", err)
	}

	// Calculate checksum.
	checksum, err := sm.calculateChecksum(snapshotPath)
	if err != nil {
		os.Remove(snapshotPath)
		return nil, fmt.Errorf("failed to calculate checksum: %w", err)
	}

	// Get file size.
	fileInfo, err := os.Stat(snapshotPath)
	if err != nil {
		os.Remove(snapshotPath)
		return nil, fmt.Errorf("failed to stat snapshot: %w", err)
	}

	// Create snapshot metadata.
	snapshot := &Snapshot{
		Topic:              topic,
		Partition:          partition,
		LastIncludedOffset: lastOffset,
		LastIncludedEpoch:  epoch,
		SizeBytes:          fileInfo.Size(),
		Checksum:           checksum,
		CreatedAt:          time.Now(),
		FilePath:           snapshotPath,
	}

	// Store snapshot.
	sm.mu.Lock()
	sm.snapshots[key] = snapshot
	sm.mu.Unlock()

	sm.logger.Info("snapshot created",
		"topic", topic,
		"partition", partition,
		"size_mb", float64(snapshot.SizeBytes)/(1024*1024),
		"duration_ms", time.Since(startTime).Milliseconds())

	return snapshot, nil
}

// =============================================================================
// SNAPSHOT ACCESS
// =============================================================================

// GetSnapshot returns the latest snapshot for a partition.
func (sm *SnapshotManager) GetSnapshot(topic string, partition int) *Snapshot {
	key := partitionKey(topic, partition)

	sm.mu.RLock()
	defer sm.mu.RUnlock()

	return sm.snapshots[key]
}

// GetSnapshotPath returns the file path for a snapshot.
func (sm *SnapshotManager) GetSnapshotPath(topic string, partition int) string {
	snapshot := sm.GetSnapshot(topic, partition)
	if snapshot == nil {
		return ""
	}
	return snapshot.FilePath
}

// =============================================================================
// SNAPSHOT LOADING (FOLLOWER)
// =============================================================================

// LoadSnapshot loads a snapshot into the local log directory.
// This replaces the existing log with the snapshot contents.
func (sm *SnapshotManager) LoadSnapshot(snapshotPath string, targetDir string, expectedChecksum string) error {
	sm.logger.Info("loading snapshot",
		"path", snapshotPath,
		"target", targetDir)

	startTime := time.Now()

	// Verify checksum.
	actualChecksum, err := sm.calculateChecksum(snapshotPath)
	if err != nil {
		return fmt.Errorf("failed to calculate checksum: %w", err)
	}

	if actualChecksum != expectedChecksum {
		return fmt.Errorf("checksum mismatch: expected %s, got %s", expectedChecksum, actualChecksum)
	}

	// Create temporary directory for extraction.
	tempDir := targetDir + ".snapshot_tmp"
	if err := os.MkdirAll(tempDir, 0755); err != nil {
		return fmt.Errorf("failed to create temp directory: %w", err)
	}

	// Extract snapshot to temp directory.
	if err := sm.extractSnapshot(snapshotPath, tempDir); err != nil {
		os.RemoveAll(tempDir)
		return fmt.Errorf("failed to extract snapshot: %w", err)
	}

	// Backup existing log (if any).
	backupDir := targetDir + ".backup"
	if _, err := os.Stat(targetDir); err == nil {
		if err := os.Rename(targetDir, backupDir); err != nil {
			os.RemoveAll(tempDir)
			return fmt.Errorf("failed to backup existing log: %w", err)
		}
	}

	// Move extracted snapshot to target.
	if err := os.Rename(tempDir, targetDir); err != nil {
		// Restore backup if rename fails.
		if _, err := os.Stat(backupDir); err == nil {
			os.Rename(backupDir, targetDir)
		}
		return fmt.Errorf("failed to move snapshot to target: %w", err)
	}

	// Remove backup.
	os.RemoveAll(backupDir)

	sm.logger.Info("snapshot loaded",
		"target", targetDir,
		"duration_ms", time.Since(startTime).Milliseconds())

	return nil
}

// extractSnapshot extracts a gzipped tar archive.
func (sm *SnapshotManager) extractSnapshot(snapshotPath string, targetDir string) error {
	// Open snapshot file.
	file, err := os.Open(snapshotPath)
	if err != nil {
		return fmt.Errorf("failed to open snapshot: %w", err)
	}
	defer file.Close()

	// Create gzip reader.
	gzReader, err := gzip.NewReader(file)
	if err != nil {
		return fmt.Errorf("failed to create gzip reader: %w", err)
	}
	defer gzReader.Close()

	// Create tar reader.
	tarReader := tar.NewReader(gzReader)

	// Extract files.
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read tar entry: %w", err)
		}

		targetPath := filepath.Join(targetDir, header.Name)

		// Create parent directory.
		if err := os.MkdirAll(filepath.Dir(targetPath), 0755); err != nil {
			return fmt.Errorf("failed to create directory: %w", err)
		}

		// Create file.
		outFile, err := os.Create(targetPath)
		if err != nil {
			return fmt.Errorf("failed to create file: %w", err)
		}

		// Copy content.
		if _, err := io.Copy(outFile, tarReader); err != nil {
			outFile.Close()
			return fmt.Errorf("failed to extract file: %w", err)
		}

		outFile.Close()

		// Set permissions.
		if err := os.Chmod(targetPath, os.FileMode(header.Mode)); err != nil {
			// Non-fatal, log and continue.
			sm.logger.Warn("failed to set file permissions", "path", targetPath, "error", err)
		}
	}

	return nil
}

// =============================================================================
// CHECKSUM
// =============================================================================

// calculateChecksum computes SHA256 checksum of a file.
func (sm *SnapshotManager) calculateChecksum(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}

	return hex.EncodeToString(hash.Sum(nil)), nil
}

// =============================================================================
// CLEANUP
// =============================================================================

// DeleteSnapshot removes a snapshot file.
func (sm *SnapshotManager) DeleteSnapshot(topic string, partition int) error {
	key := partitionKey(topic, partition)

	sm.mu.Lock()
	snapshot, ok := sm.snapshots[key]
	if ok {
		delete(sm.snapshots, key)
	}
	sm.mu.Unlock()

	if !ok {
		return nil
	}

	if err := os.Remove(snapshot.FilePath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete snapshot file: %w", err)
	}

	sm.logger.Info("snapshot deleted",
		"topic", topic,
		"partition", partition)

	return nil
}

// CleanupOldSnapshots removes snapshots older than the given duration.
func (sm *SnapshotManager) CleanupOldSnapshots(maxAge time.Duration) int {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	cutoff := time.Now().Add(-maxAge)
	removed := 0

	for key, snapshot := range sm.snapshots {
		if snapshot.CreatedAt.Before(cutoff) {
			if err := os.Remove(snapshot.FilePath); err != nil && !os.IsNotExist(err) {
				sm.logger.Warn("failed to delete old snapshot",
					"path", snapshot.FilePath,
					"error", err)
				continue
			}
			delete(sm.snapshots, key)
			removed++
			sm.logger.Info("cleaned up old snapshot",
				"topic", snapshot.Topic,
				"partition", snapshot.Partition,
				"age", time.Since(snapshot.CreatedAt))
		}
	}

	return removed
}

// =============================================================================
// INITIALIZATION
// =============================================================================

// loadExistingSnapshots scans for existing snapshot files on startup.
func (sm *SnapshotManager) loadExistingSnapshots() {
	entries, err := os.ReadDir(sm.snapshotDir)
	if err != nil {
		if !os.IsNotExist(err) {
			sm.logger.Warn("failed to read snapshot directory", "error", err)
		}
		return
	}

	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".gz" {
			continue
		}

		// Parse filename: topic-partition-offset.tar.gz
		// For now, just log that we found snapshots.
		sm.logger.Debug("found existing snapshot", "file", entry.Name())
		// TODO: Parse and load metadata if needed.
	}
}

// =============================================================================
// STATISTICS
// =============================================================================

// Stats returns snapshot statistics.
type SnapshotStats struct {
	TotalSnapshots int
	TotalSizeBytes int64
	OldestSnapshot time.Time
	NewestSnapshot time.Time
}

// GetStats returns current snapshot statistics.
func (sm *SnapshotManager) GetStats() SnapshotStats {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	stats := SnapshotStats{
		TotalSnapshots: len(sm.snapshots),
	}

	for _, s := range sm.snapshots {
		stats.TotalSizeBytes += s.SizeBytes

		if stats.OldestSnapshot.IsZero() || s.CreatedAt.Before(stats.OldestSnapshot) {
			stats.OldestSnapshot = s.CreatedAt
		}
		if s.CreatedAt.After(stats.NewestSnapshot) {
			stats.NewestSnapshot = s.CreatedAt
		}
	}

	return stats
}
