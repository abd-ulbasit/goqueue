// =============================================================================
// PID FILE MANAGEMENT
// =============================================================================
//
// WHAT IS A PID FILE?
// A PID file is a small file that contains the process ID (PID) of a running
// daemon. It serves two critical purposes:
//   1. Prevent multiple instances from running on the same data directory
//   2. Allow external tools (init systems, scripts) to find and signal the process
//
// WHY DO WE NEED THIS?
// Without PID file management, two goqueue brokers could run against the same
// data directory simultaneously, causing data corruption:
//
//   ┌─────────────┐                          ┌─────────────┐
//   │ goqueue #1   │──── writes to ────┐     ┌── writes to ────│ goqueue #2   │
//   │ PID 12345    │                   │     │                  │ PID 67890    │
//   └─────────────┘                   ▼     ▼                  └─────────────┘
//                              ┌──────────────────┐
//                              │   ./data/logs/   │  ← CORRUPTION!
//                              └──────────────────┘
//
// With PID files:
//   goqueue #1 starts → creates ./data/goqueue.pid (contains "12345")
//   goqueue #2 starts → reads ./data/goqueue.pid → finds PID 12345 alive → EXITS
//
// COMPARISON WITH OTHER SYSTEMS:
//   - Kafka:     Uses lock file (broker.lock) in log.dirs
//   - RabbitMQ:  PID file in /var/lib/rabbitmq/mnesia/
//   - Redis:     pidfile config option (default: /var/run/redis.pid)
//   - PostgreSQL: postmaster.pid in data directory
//   - goqueue:   goqueue.pid in DataDir (same pattern as Redis/PostgreSQL)
//
// LIFECYCLE:
//   ┌──────────┐    create    ┌──────────┐    running    ┌──────────┐
//   │  Start   │────────────►│ PID File │──────────────►│  Stop    │
//   └──────────┘             │ Created  │               │ Remove   │
//                            └──────────┘               │ PID File │
//                                 │                     └──────────┘
//                                 │ (crash)
//                                 ▼
//                            ┌──────────┐
//                            │ Stale PID│
//                            │ Detected │ → overwrite (process no longer alive)
//                            └──────────┘
//
// =============================================================================

package broker

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
)

// =============================================================================
// CONSTANTS
// =============================================================================

const (
	// pidFileName is the name of the PID file created in the data directory.
	// Follows the convention: <binary-name>.pid
	pidFileName = "goqueue.pid"

	// pidFilePermissions: read/write for owner, read for group/others.
	// Same as PostgreSQL's postmaster.pid permissions.
	pidFilePermissions = 0644
)

// =============================================================================
// ERROR DEFINITIONS
// =============================================================================

var (
	// ErrPIDFileExists means another instance is already running with this data directory.
	ErrPIDFileExists = errors.New("another goqueue instance is already running")

	// ErrPIDFileCreate means the PID file could not be created (permissions, disk full).
	ErrPIDFileCreate = errors.New("failed to create PID file")
)

// =============================================================================
// PID MANAGER
// =============================================================================

// PIDManager handles PID file creation, validation, and cleanup.
//
// THREAD SAFETY:
//
//	PIDManager is used only during startup and shutdown, so no mutex is needed.
//	The file system lock (PID file itself) provides the mutual exclusion.
type PIDManager struct {
	// pidPath is the full path to the PID file (e.g., ./data/goqueue.pid)
	pidPath string
}

// NewPIDManager creates a new PID manager for the given data directory.
//
// USAGE:
//
//	pm := NewPIDManager("/var/lib/goqueue")
//	if err := pm.Acquire(); err != nil {
//	    log.Fatal("another instance is running:", err)
//	}
//	defer pm.Release()
func NewPIDManager(dataDir string) *PIDManager {
	return &PIDManager{
		pidPath: filepath.Join(dataDir, pidFileName),
	}
}

// Acquire creates the PID file after verifying no other instance is running.
//
// ALGORITHM:
//  1. Check if PID file exists
//  2. If exists → read PID → check if process is alive
//  3. If alive → return error (another instance running)
//  4. If dead → stale PID file, overwrite it
//  5. If not exists → create new PID file
//
// WHY CHECK PROCESS LIVENESS?
//
//	A crash or SIGKILL won't run cleanup code, leaving a stale PID file.
//	We detect this by sending signal 0 to the PID (doesn't actually send
//	a signal, just checks if the process exists).
//
// COMPARISON:
//   - Redis: Same approach (check /proc/<pid> on Linux)
//   - PostgreSQL: Same approach (kill(pid, 0) check)
//   - Kafka: Uses file lock (flock) instead of PID check
func (pm *PIDManager) Acquire() error {
	// Check if PID file already exists
	data, err := os.ReadFile(pm.pidPath)
	if err == nil {
		// PID file exists — check if the process is still alive
		pidStr := strings.TrimSpace(string(data))
		if pid, parseErr := strconv.Atoi(pidStr); parseErr == nil {
			if isProcessAlive(pid) {
				return fmt.Errorf("%w: PID %d (file: %s)", ErrPIDFileExists, pid, pm.pidPath)
			}
			// Process is dead — stale PID file, safe to overwrite
		}
		// PID file exists but contains invalid data — overwrite
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("%w: %v", ErrPIDFileCreate, err)
	}

	// Write our PID to the file
	pid := os.Getpid()
	content := fmt.Sprintf("%d\n", pid)
	if err := os.WriteFile(pm.pidPath, []byte(content), pidFilePermissions); err != nil {
		return fmt.Errorf("%w: %v", ErrPIDFileCreate, err)
	}

	return nil
}

// Release removes the PID file during graceful shutdown.
//
// IMPORTANT:
//
//	Only remove the PID file if it contains OUR PID. This prevents a race
//	where a new instance starts before the old one finishes cleanup.
func (pm *PIDManager) Release() error {
	data, err := os.ReadFile(pm.pidPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // Already removed (e.g., external cleanup)
		}
		return fmt.Errorf("failed to read PID file for cleanup: %w", err)
	}

	// Only remove if it's our PID
	pidStr := strings.TrimSpace(string(data))
	if pid, parseErr := strconv.Atoi(pidStr); parseErr == nil {
		if pid != os.Getpid() {
			// Another instance wrote this — don't remove it
			return nil
		}
	}

	return os.Remove(pm.pidPath)
}

// Path returns the full path to the PID file.
func (pm *PIDManager) Path() string {
	return pm.pidPath
}

// =============================================================================
// PROCESS LIVENESS CHECK
// =============================================================================

// isProcessAlive checks if a process with the given PID exists.
//
// HOW IT WORKS:
//
//	On Unix systems, sending signal 0 to a process doesn't deliver any signal
//	but performs error checking:
//	  - If process exists → returns nil (no error)
//	  - If process doesn't exist → returns ESRCH ("no such process")
//	  - If no permission → returns EPERM (process exists but owned by another user)
//
// COMPARISON:
//   - Linux: Could check /proc/<pid>/status (more portable)
//   - macOS/BSD: kill(pid, 0) is the standard approach
//   - Windows: OpenProcess() with PROCESS_QUERY_LIMITED_INFORMATION
//   - Go stdlib: No direct API, so we use syscall.Kill
func isProcessAlive(pid int) bool {
	process, err := os.FindProcess(pid)
	if err != nil {
		return false
	}

	// Signal 0 checks existence without actually sending a signal
	err = process.Signal(syscall.Signal(0))
	return err == nil
}
