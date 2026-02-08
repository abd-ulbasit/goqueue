// =============================================================================
// TLS CERTIFICATE RELOADER - ZERO-DOWNTIME CERTIFICATE ROTATION
// =============================================================================
//
// ┌─────────────────────────────────────────────────────────────────────────────┐
// │ WHY CERTIFICATE ROTATION?                                                   │
// │                                                                             │
// │ TLS certificates expire. Without rotation:                                  │
// │   - Expired cert → clients refuse to connect → total outage                │
// │   - Manual restart required → downtime                                      │
// │   - In Kubernetes, cert-manager rotates certs automatically,               │
// │     but the server needs to pick up the new cert without restart            │
// │                                                                             │
// │ HOW IT WORKS:                                                               │
// │   1. CertReloader watches cert + key files for changes                     │
// │   2. When files change → reload certificate into memory                    │
// │   3. New TLS handshakes use the new certificate                            │
// │   4. Existing connections continue with old cert (until close)             │
// │                                                                             │
// │ COMPARISON:                                                                 │
// │   - Kafka: ssl.keystore.reload.interval (Kafka 2.2+)                       │
// │   - Nginx: hot reload via SIGHUP                                            │
// │   - Envoy: SDS (Secret Discovery Service) for dynamic certs                │
// │   - Go stdlib: tls.Config.GetCertificate callback (what we use)            │
// │                                                                             │
// │ GO'S tls.Config.GetCertificate:                                             │
// │   Go's TLS implementation calls GetCertificate for each new handshake.     │
// │   By returning a fresh certificate from this callback, we can rotate       │
// │   certificates without restarting the server.                              │
// │                                                                             │
// │ FLOW:                                                                       │
// │   ┌─────────────┐   watch    ┌───────────────┐                             │
// │   │ cert.pem    │──────────►│ CertReloader  │                             │
// │   │ key.pem     │           │ (background)  │                             │
// │   └─────────────┘           └───────┬───────┘                             │
// │         │ file changed              │ atomic swap                          │
// │         ▼                           ▼                                      │
// │   ┌─────────────┐           ┌───────────────┐                             │
// │   │ cert-manager│           │ tls.Config    │                             │
// │   │ rotates     │           │ GetCertificate│                             │
// │   └─────────────┘           └───────────────┘                             │
// └─────────────────────────────────────────────────────────────────────────────┘
//
// =============================================================================

package security

import (
	"crypto/tls"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"
)

// =============================================================================
// CERTIFICATE RELOADER
// =============================================================================

// CertReloader watches TLS certificate and key files for changes and reloads
// them automatically. It provides a GetCertificate callback compatible with
// tls.Config.GetCertificate.
//
// THREAD SAFETY:
//
//	Uses sync.RWMutex to allow concurrent TLS handshakes while
//	still supporting atomic certificate swaps during reload.
//
// USAGE:
//
//	reloader := NewCertReloader("/path/to/cert.pem", "/path/to/key.pem")
//	reloader.Start()
//	defer reloader.Stop()
//
//	tlsConfig := &tls.Config{
//	    GetCertificate: reloader.GetCertificate,
//	}
type CertReloader struct {
	// certFile and keyFile are the paths to the PEM-encoded certificate and key
	certFile string
	keyFile  string

	// mu protects the cert field during hot reload
	mu sync.RWMutex

	// cert is the current TLS certificate (atomically swapped on reload)
	cert *tls.Certificate

	// lastModTime tracks file modification time to detect changes
	lastCertModTime time.Time
	lastKeyModTime  time.Time

	// checkInterval is how often we poll for file changes
	// Default: 30 seconds
	//
	// WHY POLLING INSTEAD OF FSNOTIFY?
	//   - Kubernetes Secrets/ConfigMaps use symlinks that fsnotify can miss
	//   - Polling is simpler, more reliable across platforms
	//   - 30s polling is negligible overhead (single stat() call)
	//   - cert-manager rotates certs days before expiry, 30s is fine
	checkInterval time.Duration

	// logger for reload events
	logger *slog.Logger

	// stopCh signals the background goroutine to stop
	stopCh chan struct{}

	// stopped tracks whether Stop() has been called
	stopped bool
}

// CertReloaderConfig holds configuration for the certificate reloader.
type CertReloaderConfig struct {
	// CertFile is the path to the PEM-encoded certificate
	CertFile string

	// KeyFile is the path to the PEM-encoded private key
	KeyFile string

	// CheckInterval is how often to check for file changes
	// Default: 30 seconds
	CheckInterval time.Duration
}

// DefaultCertReloaderConfig returns sensible defaults.
func DefaultCertReloaderConfig(certFile, keyFile string) CertReloaderConfig {
	return CertReloaderConfig{
		CertFile:      certFile,
		KeyFile:       keyFile,
		CheckInterval: 30 * time.Second,
	}
}

// NewCertReloader creates a new certificate reloader.
//
// It immediately loads the certificate from disk. If the initial load fails,
// an error is returned (fail-fast at startup).
func NewCertReloader(config CertReloaderConfig) (*CertReloader, error) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil)).With("component", "cert-reloader")

	cr := &CertReloader{
		certFile:      config.CertFile,
		keyFile:       config.KeyFile,
		checkInterval: config.CheckInterval,
		logger:        logger,
		stopCh:        make(chan struct{}),
	}

	// Initial load - fail fast if certs are missing/invalid
	if err := cr.reload(); err != nil {
		return nil, fmt.Errorf("initial certificate load failed: %w", err)
	}

	logger.Info("certificate loaded successfully",
		"cert_file", config.CertFile,
		"key_file", config.KeyFile,
		"check_interval", config.CheckInterval.String(),
	)

	return cr, nil
}

// Start begins the background file watcher goroutine.
//
// The goroutine polls cert and key files at CheckInterval for modification
// time changes. When a change is detected, it reloads the certificate.
func (cr *CertReloader) Start() {
	go cr.watchLoop()
}

// Stop terminates the background watcher.
func (cr *CertReloader) Stop() {
	cr.mu.Lock()
	if cr.stopped {
		cr.mu.Unlock()
		return
	}
	cr.stopped = true
	cr.mu.Unlock()

	close(cr.stopCh)
	cr.logger.Info("certificate reloader stopped")
}

// GetCertificate returns the current certificate for TLS handshakes.
//
// This method is designed to be used as tls.Config.GetCertificate:
//
//	tlsConfig := &tls.Config{
//	    GetCertificate: reloader.GetCertificate,
//	}
//
// PERFORMANCE:
//
//	Uses RLock (shared lock) so multiple concurrent TLS handshakes
//	don't block each other. Only blocks during certificate swap
//	(a few microseconds).
func (cr *CertReloader) GetCertificate(_ *tls.ClientHelloInfo) (*tls.Certificate, error) {
	cr.mu.RLock()
	defer cr.mu.RUnlock()

	if cr.cert == nil {
		return nil, fmt.Errorf("no certificate loaded")
	}
	return cr.cert, nil
}

// GetClientCertificate returns the current certificate for mTLS client connections.
//
// This method is designed to be used as tls.Config.GetClientCertificate:
//
//	tlsConfig := &tls.Config{
//	    GetClientCertificate: reloader.GetClientCertificate,
//	}
func (cr *CertReloader) GetClientCertificate(_ *tls.CertificateRequestInfo) (*tls.Certificate, error) {
	cr.mu.RLock()
	defer cr.mu.RUnlock()

	if cr.cert == nil {
		return nil, fmt.Errorf("no certificate loaded")
	}
	return cr.cert, nil
}

// =============================================================================
// INTERNAL: FILE WATCHING AND RELOAD
// =============================================================================

// watchLoop polls cert/key files for changes.
//
// WHY POLLING?
//   - Kubernetes mounts Secrets as symlinks that get atomically swapped
//   - fsnotify doesn't reliably detect symlink target changes
//   - Polling with stat() is simple, reliable, and low-cost
//   - 30s interval = ~2 stat() calls per minute per file = negligible
func (cr *CertReloader) watchLoop() {
	ticker := time.NewTicker(cr.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-cr.stopCh:
			return
		case <-ticker.C:
			if cr.filesChanged() {
				cr.logger.Info("certificate file change detected, reloading",
					"cert_file", cr.certFile,
					"key_file", cr.keyFile,
				)
				if err := cr.reload(); err != nil {
					// Log error but keep using the old certificate
					// This is safer than crashing - the old cert may still be valid
					cr.logger.Error("failed to reload certificate, keeping old cert",
						"error", err,
					)
				} else {
					cr.logger.Info("certificate reloaded successfully")
				}
			}
		}
	}
}

// filesChanged checks if cert or key files have been modified since last load.
func (cr *CertReloader) filesChanged() bool {
	certInfo, err := os.Stat(cr.certFile)
	if err != nil {
		return false // File gone, don't trigger reload
	}

	keyInfo, err := os.Stat(cr.keyFile)
	if err != nil {
		return false
	}

	return certInfo.ModTime().After(cr.lastCertModTime) ||
		keyInfo.ModTime().After(cr.lastKeyModTime)
}

// reload reads the certificate and key from disk and swaps the in-memory cert.
//
// ATOMICITY:
//
//	The certificate is fully loaded and validated BEFORE acquiring the write lock.
//	This minimizes the time the lock is held (just a pointer swap).
func (cr *CertReloader) reload() error {
	// Load and validate certificate outside the lock
	cert, err := tls.LoadX509KeyPair(cr.certFile, cr.keyFile)
	if err != nil {
		return fmt.Errorf("failed to load certificate pair: %w", err)
	}

	// Record file modification times
	certInfo, err := os.Stat(cr.certFile)
	if err != nil {
		return fmt.Errorf("failed to stat cert file: %w", err)
	}
	keyInfo, err := os.Stat(cr.keyFile)
	if err != nil {
		return fmt.Errorf("failed to stat key file: %w", err)
	}

	// Atomic swap under write lock (very brief)
	cr.mu.Lock()
	cr.cert = &cert
	cr.lastCertModTime = certInfo.ModTime()
	cr.lastKeyModTime = keyInfo.ModTime()
	cr.mu.Unlock()

	return nil
}
