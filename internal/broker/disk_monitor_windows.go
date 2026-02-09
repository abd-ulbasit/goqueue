// =============================================================================
// DISK MONITOR - WINDOWS PLATFORM IMPLEMENTATION
// =============================================================================
//
// This file contains the platform-specific refreshStats() implementation
// for Windows. It uses the GetDiskFreeSpaceEx Win32 API to query
// filesystem statistics.
//
// BUILD CONSTRAINT:
//   - Compiles on: windows only
//   - Unix systems use: disk_monitor_unix.go (unix.Statfs)
//
// WINDOWS API: GetDiskFreeSpaceExW
//   The Windows equivalent of Unix statfs(2). Returns three values:
//     - FreeBytesAvailableToCaller: Available to the calling user (like Bavail)
//     - TotalNumberOfBytes: Total disk capacity (like Blocks * Bsize)
//     - TotalNumberOfFreeBytes: Total free (like Bfree * Bsize)
//
//   We use FreeBytesAvailableToCaller (not TotalNumberOfFreeBytes) for the
//   same reason Unix uses Bavail: it respects disk quotas and per-user limits.
//
// =============================================================================

//go:build windows

package broker

import (
	"fmt"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

// refreshStats queries the filesystem for current disk usage via
// Windows GetDiskFreeSpaceEx API.
//
// COMPARISON TO UNIX:
//   - Unix: statfs(2) → Blocks, Bavail, Bsize fields
//   - Windows: GetDiskFreeSpaceExW → FreeBytesAvailable, TotalBytes, TotalFree
//
// Both give us the same information, just through different syscalls.
// The logic after getting the raw numbers is identical across platforms.
func (dm *DiskMonitor) refreshStats() error {
	// Convert the data directory path to a UTF-16 pointer for the Win32 API
	pathPtr, err := windows.UTF16PtrFromString(dm.config.DataDir)
	if err != nil {
		return fmt.Errorf("invalid path %q: %w", dm.config.DataDir, err)
	}

	var freeBytesAvailable, totalBytes, totalFreeBytes uint64

	// GetDiskFreeSpaceExW is the Unicode version of GetDiskFreeSpaceEx.
	// It returns disk space info for the volume containing the specified path.
	//
	// Parameters:
	//   lpDirectoryName: Path to query (can be any path on the volume)
	//   lpFreeBytesAvailableToCaller: Free bytes respecting user quotas
	//   lpTotalNumberOfBytes: Total volume capacity
	//   lpTotalNumberOfFreeBytes: Total free (ignoring quotas)
	if err := windows.GetDiskFreeSpaceEx(
		pathPtr,
		(*uint64)(unsafe.Pointer(&freeBytesAvailable)),
		(*uint64)(unsafe.Pointer(&totalBytes)),
		(*uint64)(unsafe.Pointer(&totalFreeBytes)),
	); err != nil {
		return fmt.Errorf("GetDiskFreeSpaceEx(%s): %w", dm.config.DataDir, err)
	}

	// Use freeBytesAvailable (not totalFreeBytes) — respects user quotas
	// This is analogous to using Bavail instead of Bfree on Unix
	availBytes := freeBytesAvailable
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
