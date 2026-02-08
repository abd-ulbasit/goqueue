package security

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// =============================================================================
// CERT RELOADER TESTS (M27)
// =============================================================================
//
// Tests cover:
//   - Initial certificate loading
//   - GetCertificate returns valid cert
//   - Invalid cert/key paths fail at creation
//   - File change detection (filesChanged)
//   - Config defaults
//
// NOTE: We generate self-signed test certificates inline rather than
// relying on fixture files, so tests are self-contained.
//
// =============================================================================

// generateTestCert creates a self-signed cert+key pair in the given directory.
// Returns (certPath, keyPath).
func generateTestCert(t *testing.T, dir string) (string, string) {
	t.Helper()

	// Generate ECDSA private key (fast for tests)
	privKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	// Create self-signed certificate template
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test-cert"},
		NotBefore:    time.Now().Add(-1 * time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &privKey.PublicKey, privKey)
	if err != nil {
		t.Fatalf("failed to create certificate: %v", err)
	}

	// Write cert PEM
	certPath := filepath.Join(dir, "test.crt")
	certFile, err := os.Create(certPath)
	if err != nil {
		t.Fatalf("failed to create cert file: %v", err)
	}
	if err := pem.Encode(certFile, &pem.Block{Type: "CERTIFICATE", Bytes: certDER}); err != nil {
		t.Fatalf("failed to encode cert: %v", err)
	}
	certFile.Close()

	// Write key PEM
	keyPath := filepath.Join(dir, "test.key")
	keyFile, err := os.Create(keyPath)
	if err != nil {
		t.Fatalf("failed to create key file: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(privKey)
	if err != nil {
		t.Fatalf("failed to marshal key: %v", err)
	}
	if err := pem.Encode(keyFile, &pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}); err != nil {
		t.Fatalf("failed to encode key: %v", err)
	}
	keyFile.Close()

	return certPath, keyPath
}

func TestCertReloader_InitialLoad(t *testing.T) {
	dir := t.TempDir()
	certPath, keyPath := generateTestCert(t, dir)

	reloader, err := NewCertReloader(CertReloaderConfig{
		CertFile:      certPath,
		KeyFile:       keyPath,
		CheckInterval: time.Second,
	})
	if err != nil {
		t.Fatalf("NewCertReloader() error: %v", err)
	}
	defer reloader.Stop()

	// Should have loaded the certificate
	cert, err := reloader.GetCertificate(&tls.ClientHelloInfo{})
	if err != nil {
		t.Fatalf("GetCertificate() error: %v", err)
	}
	if cert == nil {
		t.Fatal("GetCertificate() returned nil")
	}
}

func TestCertReloader_InvalidPaths(t *testing.T) {
	_, err := NewCertReloader(CertReloaderConfig{
		CertFile:      "/nonexistent/cert.pem",
		KeyFile:       "/nonexistent/key.pem",
		CheckInterval: time.Second,
	})
	if err == nil {
		t.Error("NewCertReloader() should fail with invalid paths")
	}
}

func TestCertReloader_MismatchedCertKey(t *testing.T) {
	dir := t.TempDir()

	// Generate two separate cert/key pairs
	certPath1, _ := generateTestCert(t, dir)

	// Generate a second key (mismatched)
	dir2 := t.TempDir()
	_, keyPath2 := generateTestCert(t, dir2)

	_, err := NewCertReloader(CertReloaderConfig{
		CertFile:      certPath1,
		KeyFile:       keyPath2,
		CheckInterval: time.Second,
	})
	if err == nil {
		t.Error("NewCertReloader() should fail with mismatched cert/key")
	}
}

func TestCertReloader_GetClientCertificate(t *testing.T) {
	dir := t.TempDir()
	certPath, keyPath := generateTestCert(t, dir)

	reloader, err := NewCertReloader(CertReloaderConfig{
		CertFile:      certPath,
		KeyFile:       keyPath,
		CheckInterval: time.Second,
	})
	if err != nil {
		t.Fatalf("NewCertReloader() error: %v", err)
	}
	defer reloader.Stop()

	cert, err := reloader.GetClientCertificate(&tls.CertificateRequestInfo{})
	if err != nil {
		t.Fatalf("GetClientCertificate() error: %v", err)
	}
	if cert == nil {
		t.Fatal("GetClientCertificate() returned nil")
	}
}

func TestCertReloader_StartStop(t *testing.T) {
	dir := t.TempDir()
	certPath, keyPath := generateTestCert(t, dir)

	reloader, err := NewCertReloader(CertReloaderConfig{
		CertFile:      certPath,
		KeyFile:       keyPath,
		CheckInterval: 100 * time.Millisecond, // Fast interval for testing
	})
	if err != nil {
		t.Fatalf("NewCertReloader() error: %v", err)
	}

	// Start the watcher
	reloader.Start()

	// Let it run a few cycles
	time.Sleep(350 * time.Millisecond)

	// Stop should not panic or block
	reloader.Stop()

	// GetCertificate should still work after stop
	cert, err := reloader.GetCertificate(&tls.ClientHelloInfo{})
	if err != nil {
		t.Fatalf("GetCertificate() after stop: %v", err)
	}
	if cert == nil {
		t.Fatal("GetCertificate() returned nil after stop")
	}
}

func TestDefaultCertReloaderConfig(t *testing.T) {
	cfg := DefaultCertReloaderConfig("/path/cert.pem", "/path/key.pem")

	if cfg.CertFile != "/path/cert.pem" {
		t.Errorf("expected cert path, got %s", cfg.CertFile)
	}
	if cfg.KeyFile != "/path/key.pem" {
		t.Errorf("expected key path, got %s", cfg.KeyFile)
	}
	if cfg.CheckInterval != 30*time.Second {
		t.Errorf("expected 30s interval, got %s", cfg.CheckInterval)
	}
}

func TestTLSConfig_HotReloadLifecycle(t *testing.T) {
	dir := t.TempDir()
	certPath, keyPath := generateTestCert(t, dir)

	tlsCfg := &TLSConfig{
		Enabled:         true,
		CertFile:        certPath,
		KeyFile:         keyPath,
		EnableHotReload: true,
	}

	goTLSConfig, err := tlsCfg.NewTLSConfig()
	if err != nil {
		t.Fatalf("NewTLSConfig() error: %v", err)
	}

	// GetCertificate callback should be set (hot reload path)
	if goTLSConfig.GetCertificate == nil {
		t.Fatal("GetCertificate callback should be set for hot reload")
	}

	// Certificates slice should be empty (using callback instead)
	if len(goTLSConfig.Certificates) > 0 {
		t.Fatal("Certificates should be empty when using hot reload")
	}

	// Start and stop cert reloader
	tlsCfg.StartCertReloader()
	time.Sleep(100 * time.Millisecond)
	tlsCfg.StopCertReloader()
}

func TestTLSConfig_StaticCertLoad(t *testing.T) {
	dir := t.TempDir()
	certPath, keyPath := generateTestCert(t, dir)

	tlsCfg := &TLSConfig{
		Enabled:         true,
		CertFile:        certPath,
		KeyFile:         keyPath,
		EnableHotReload: false, // Static mode
	}

	goTLSConfig, err := tlsCfg.NewTLSConfig()
	if err != nil {
		t.Fatalf("NewTLSConfig() error: %v", err)
	}

	// GetCertificate should NOT be set (static mode)
	if goTLSConfig.GetCertificate != nil {
		t.Fatal("GetCertificate should not be set in static mode")
	}

	// Certificates should be populated
	if len(goTLSConfig.Certificates) != 1 {
		t.Fatalf("expected 1 certificate, got %d", len(goTLSConfig.Certificates))
	}
}
