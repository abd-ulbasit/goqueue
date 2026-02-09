// =============================================================================
// DISK MONITOR - UNIX PLATFORM IMPLEMENTATION (Linux, macOS, FreeBSD)
// =============================================================================
//
// This file contains the platform-specific refreshStats() implementation
// for Unix-like operating systems. It uses the unix.Statfs syscall to
// query filesystem statistics.
//
// BUILD CONSTRAINT:
//   - Compiles on: linux, darwin, freebsd, openbsd, netbsd
//   - Excluded on: windows (see disk_monitor_windows.go)
//
// WHY SEPARATE FILES?
//   Go uses build tags (//go:build) for platform-specific code instead
//   of #ifdef like C. Each platform gets its own file with the same
//   function signatures. The compiler selects the right file based on
//   GOOS at build time.
//
//   COMPARISON:
//     - C/C++: #ifdef _WIN32 / #ifdef __linux__ (preprocessor)
//     - Rust: #[cfg(target_os = "linux")] (attribute)
//     - Go: //go:build linux (build tags + separate files)
//
//   Go's approach is cleaner: no conditional compilation in the same file,
//   each platform's code is isolated and testable independently.
//
// =============================================================================

//go:build !windows

package broker

import (
	"fmt"
	"time"

	"golang.org/x/sys/unix"
)

// refreshStats queries the filesystem for current disk usage via unix.Statfs.
//
// IMPLEMENTATION:
//
//	Uses unix.Statfs (syscall) to get filesystem stats directly from the
//	kernel. This is a thin wrapper around the statfs(2) system call.
//
// SYSCALL FIELDS:
//   - Blocks: Total data blocks in filesystem
//   - Bfree: Free blocks (for superuser)
//   - Bavail: Free blocks (for unprivileged users) ← we use this
//   - Bsize: Optimal transfer block size
//
// WHY Bavail (not Bfree)?
//
//	Filesystems reserve ~5% for root (ext4's reserved-blocks-percentage).
//	Bavail reflects what's ACTUALLY available to our process.
//	Using Bfree would overestimate available space.
//
// PERFORMANCE:
//   - statfs(2) is very fast (~1μs) since it reads cached superblock info
//   - No disk I/O required; the kernel caches filesystem metadata
//   - Safe to call frequently, but we cache anyway (30s default interval)
func (dm *DiskMonitor) refreshStats() error {
	var stat unix.Statfs_t
	if err := unix.Statfs(dm.config.DataDir, &stat); err != nil {
		return fmt.Errorf("statfs(%s): %w", dm.config.DataDir, err)
	}

	// Calculate disk usage from filesystem block counts
	//
	// MATH:
	//   total = Blocks * Bsize  (total capacity in bytes)
	//   avail = Bavail * Bsize  (available to unprivileged users)
	//   used  = total - avail   (bytes currently in use)
	//
	// NOTE: We cast Bsize to uint64 because on some platforms (macOS)
	// it's int32, which would cause overflow for large filesystems.
	totalBytes := stat.Blocks * uint64(stat.Bsize)
	availBytes := stat.Bavail * uint64(stat.Bsize)
	usedBytes := totalBytes - availBytes

	var usagePercent float64
	if totalBytes > 0 {
		usagePercent = float64(usedBytes) / float64(totalBytes) * 100
	}

	stats := DiskStats{
		TotalBytes:     totalBytes,
		AvailableBytes: availBytes,
		UsedBytes:      usedBytes,
		UsagePercent:   usagePercent,
		LastChecked:    time.Now(),
	}

	dm.statsMu.Lock()
	dm.stats = stats
	dm.statsMu.Unlock()

	// Update the atomic flag for hot-path checks
	wasFull := dm.diskFull.Load()
	isFull := usagePercent >= dm.config.ThresholdPercent

	dm.diskFull.Store(isFull)

	// Log state transitions (only on change to avoid log spam)
	switch {
	case isFull && !wasFull:
		dm.logger.Error("DISK SPACE CRITICAL: writes suspended",
			"usage_percent", fmt.Sprintf("%.1f%%", usagePercent),
			"threshold_percent", dm.config.ThresholdPercent,
			"available_bytes", availBytes,
			"total_bytes", totalBytes,
		)
	case !isFull && wasFull:
		dm.logger.Info("disk space recovered: writes resumed",
			"usage_percent", fmt.Sprintf("%.1f%%", usagePercent),
			"threshold_percent", dm.config.ThresholdPercent,
			"available_bytes", availBytes,
		)
	case usagePercent >= dm.config.ThresholdPercent-5 && !isFull:
		// Warning when within 5% of threshold
		dm.logger.Warn("disk space warning: approaching threshold",
			"usage_percent", fmt.Sprintf("%.1f%%", usagePercent),
			"threshold_percent", dm.config.ThresholdPercent,
			"available_bytes", availBytes,
		)
	}

	return nil
}
