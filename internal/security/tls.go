// =============================================================================
// TLS CONFIGURATION - TRANSPORT LAYER SECURITY FOR GOQUEUE
// =============================================================================
//
// ┌─────────────────────────────────────────────────────────────────────────────┐
// │ WHY TLS?                                                                    │
// │                                                                             │
// │ TLS (Transport Layer Security) provides:                                    │
// │   1. ENCRYPTION: Data in transit is encrypted (AES-256-GCM typically)       │
// │   2. INTEGRITY: Tampering detection via HMAC                                │
// │   3. AUTHENTICATION: Server proves identity via certificate                 │
// │   4. MUTUAL AUTH (mTLS): Client also proves identity (inter-node)           │
// │                                                                             │
// │ THREAT MODEL:                                                               │
// │   Without TLS:                                                              │
// │     - Network sniffing → Messages readable in plaintext                     │
// │     - Man-in-the-middle → Attacker can intercept/modify                     │
// │     - Spoofing → Fake broker can impersonate real one                       │
// │                                                                             │
// │   With TLS:                                                                 │
// │     - All traffic encrypted                                                 │
// │     - Certificate validation prevents MITM                                  │
// │     - mTLS ensures only trusted nodes can join cluster                      │
// │                                                                             │
// │ COMPARISON:                                                                 │
// │   - Kafka: SSL/TLS for clients, mTLS for inter-broker                       │
// │   - RabbitMQ: TLS via listener config                                       │
// │   - NATS: TLS with optional client certs                                    │
// │   - goqueue: TLS for HTTP/gRPC + mTLS for cluster communication             │
// └─────────────────────────────────────────────────────────────────────────────┘
//
// =============================================================================

package security

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"log/slog"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"
)

// =============================================================================
// TLS CONFIGURATION
// =============================================================================

// TLSConfig holds TLS configuration for the broker.
type TLSConfig struct {
	// Enabled turns on TLS for the specified listener
	Enabled bool

	// CertFile is the path to the server certificate (PEM format)
	CertFile string

	// KeyFile is the path to the private key (PEM format)
	KeyFile string

	// CAFile is the path to the CA certificate for client verification (mTLS)
	CAFile string

	// ClientAuth specifies the client authentication policy
	// Options: NoClientCert, RequestClientCert, RequireAnyClientCert, VerifyClientCertIfGiven, RequireAndVerifyClientCert
	ClientAuth tls.ClientAuthType

	// MinVersion is the minimum TLS version (TLS 1.2 recommended minimum)
	MinVersion uint16

	// InsecureSkipVerify disables certificate verification (FOR TESTING ONLY)
	InsecureSkipVerify bool

	// ServerName for certificate verification (SNI)
	ServerName string

	// GenerateSelfSigned generates self-signed certificates if no certs provided
	GenerateSelfSigned bool

	// CertDir is the directory to store generated certificates
	CertDir string
}

// DefaultTLSConfig returns a secure default TLS configuration.
//
// ┌─────────────────────────────────────────────────────────────────────────────┐
// │ DEFAULT SECURITY SETTINGS                                                   │
// │                                                                             │
// │ MinVersion: TLS 1.2                                                         │
// │   - TLS 1.0/1.1 have known vulnerabilities (BEAST, POODLE)                  │
// │   - TLS 1.2 is minimum for PCI-DSS compliance                               │
// │   - TLS 1.3 preferred for new deployments (faster, simpler)                 │
// │                                                                             │
// │ ClientAuth: NoClientCert (default)                                          │
// │   - For public APIs, we use API keys instead                                │
// │   - For inter-node (cluster), we use RequireAndVerifyClientCert             │
// └─────────────────────────────────────────────────────────────────────────────┘
func DefaultTLSConfig() TLSConfig {
	return TLSConfig{
		Enabled:            false,
		MinVersion:         tls.VersionTLS12,
		ClientAuth:         tls.NoClientCert,
		InsecureSkipVerify: false,
		GenerateSelfSigned: false,
		CertDir:            "/tmp/goqueue-certs",
	}
}

// DefaultMTLSConfig returns TLS config for inter-node communication (mTLS).
//
// ┌─────────────────────────────────────────────────────────────────────────────┐
// │ MUTUAL TLS (mTLS) FOR CLUSTER COMMUNICATION                                 │
// │                                                                             │
// │ Regular TLS:                                                                │
// │   Client ──verify──► Server (only server proves identity)                   │
// │                                                                             │
// │ Mutual TLS:                                                                 │
// │   Client ◄──verify──► Server (BOTH prove identity)                          │
// │                                                                             │
// │ WHY mTLS FOR CLUSTER?                                                       │
// │   - Prevents rogue nodes from joining cluster                               │
// │   - Each node has its own certificate signed by cluster CA                  │
// │   - Zero-trust: even internal network traffic is encrypted                  │
// │                                                                             │
// │ KUBERNETES INTEGRATION:                                                     │
// │   - CA cert stored as Secret, mounted to pods                               │
// │   - Per-node certs generated by cert-manager or init container              │
// └─────────────────────────────────────────────────────────────────────────────┘
func DefaultMTLSConfig() TLSConfig {
	return TLSConfig{
		Enabled:            false,
		MinVersion:         tls.VersionTLS13, // TLS 1.3 for internal traffic
		ClientAuth:         tls.RequireAndVerifyClientCert,
		InsecureSkipVerify: false,
		GenerateSelfSigned: false,
		CertDir:            "/tmp/goqueue-certs",
	}
}

// NewTLSConfig creates a tls.Config from our TLSConfig.
func (c *TLSConfig) NewTLSConfig() (*tls.Config, error) {
	if !c.Enabled {
		return nil, nil
	}

	// Enforce minimum TLS 1.2 to prevent downgrade attacks (gosec G402).
	// Even if config specifies a lower version (or zero-value), floor at TLS 1.2.
	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
		ClientAuth: c.ClientAuth,
	}
	// Allow higher TLS versions if explicitly configured.
	if c.MinVersion > tls.VersionTLS12 {
		tlsConfig.MinVersion = c.MinVersion
	}

	// Load or generate server certificate
	var cert tls.Certificate
	var err error

	switch {
	case c.CertFile != "" && c.KeyFile != "":
		// ┌───────────────────────────────────────────────────────────────────────┐
		// │ LOADING CERTIFICATES FROM FILES                                       │
		// │                                                                        │
		// │ In production, certificates are typically:                             │
		// │   - Issued by a trusted CA (Let's Encrypt, DigiCert, etc.)            │
		// │   - Mounted from Kubernetes Secrets                                    │
		// │   - Managed by cert-manager                                            │
		// │                                                                        │
		// │ KUBERNETES EXAMPLE:                                                    │
		// │   kubectl create secret tls goqueue-tls \                              │
		// │     --cert=server.crt --key=server.key                                 │
		// └───────────────────────────────────────────────────────────────────────┘
		cert, err = tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load certificate: %w", err)
		}
		slog.Info("loaded TLS certificate", "cert", c.CertFile)
	case c.GenerateSelfSigned:
		// Generate self-signed certificate for development/testing
		cert, err = c.generateSelfSignedCert()
		if err != nil {
			return nil, fmt.Errorf("failed to generate self-signed cert: %w", err)
		}
		slog.Warn("using self-signed certificate - NOT FOR PRODUCTION")
	default:
		return nil, fmt.Errorf("TLS enabled but no certificate provided")
	}

	tlsConfig.Certificates = []tls.Certificate{cert}

	// Load CA certificate for client verification (mTLS)
	if c.CAFile != "" {
		caCert, err := os.ReadFile(c.CAFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA cert: %w", err)
		}

		caPool := x509.NewCertPool()
		if !caPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse CA cert")
		}

		tlsConfig.ClientCAs = caPool
		tlsConfig.RootCAs = caPool
		slog.Info("loaded CA certificate for client verification", "ca", c.CAFile)
	}

	// For client connections
	tlsConfig.InsecureSkipVerify = c.InsecureSkipVerify
	if c.ServerName != "" {
		tlsConfig.ServerName = c.ServerName
	}

	return tlsConfig, nil
}

// generateSelfSignedCert creates a self-signed certificate for development.
//
// ┌─────────────────────────────────────────────────────────────────────────────┐
// │ SELF-SIGNED CERTIFICATES                                                    │
// │                                                                             │
// │ ⚠️  FOR DEVELOPMENT AND TESTING ONLY                                        │
// │                                                                             │
// │ Self-signed certs:                                                          │
// │   - Not trusted by browsers/clients by default                              │
// │   - Require InsecureSkipVerify or explicit CA trust                         │
// │   - Good for local development, CI/CD testing                               │
// │                                                                             │
// │ For production, use:                                                        │
// │   - cert-manager with Let's Encrypt                                         │
// │   - AWS ACM (for ALB/NLB termination)                                       │
// │   - HashiCorp Vault PKI                                                     │
// └─────────────────────────────────────────────────────────────────────────────┘
func (c *TLSConfig) generateSelfSignedCert() (tls.Certificate, error) {
	// Generate ECDSA private key (faster than RSA, smaller keys)
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("failed to generate private key: %w", err)
	}

	// Create certificate template
	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("failed to generate serial number: %w", err)
	}

	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{"GoQueue Development"},
			CommonName:   "goqueue",
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(365 * 24 * time.Hour), // 1 year
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  false,

		// SANs (Subject Alternative Names) - critical for modern TLS
		DNSNames: []string{
			"localhost",
			"goqueue",
			"goqueue-headless",
			"*.goqueue-headless",
			"*.goqueue-headless.goqueue.svc.cluster.local",
		},
		IPAddresses: []net.IP{
			net.ParseIP("127.0.0.1"),
			net.ParseIP("::1"),
		},
	}

	// Self-sign the certificate
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &privateKey.PublicKey, privateKey)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("failed to create certificate: %w", err)
	}

	// Encode to PEM
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})

	keyDER, err := x509.MarshalECPrivateKey(privateKey)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("failed to marshal private key: %w", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	// Optionally save to disk for debugging
	if c.CertDir != "" {
		if err := os.MkdirAll(c.CertDir, 0o700); err == nil {
			_ = os.WriteFile(filepath.Join(c.CertDir, "server.crt"), certPEM, 0o600)
			_ = os.WriteFile(filepath.Join(c.CertDir, "server.key"), keyPEM, 0o600)
		}
	}

	return tls.X509KeyPair(certPEM, keyPEM)
}

// LoadTLSConfigFromEnv creates TLS config from environment variables.
//
// Environment variables:
//
//	GOQUEUE_TLS_ENABLED=true
//	GOQUEUE_TLS_CERT_FILE=/path/to/cert.pem
//	GOQUEUE_TLS_KEY_FILE=/path/to/key.pem
//	GOQUEUE_TLS_CA_FILE=/path/to/ca.pem
//	GOQUEUE_TLS_MIN_VERSION=1.2
//	GOQUEUE_TLS_CLIENT_AUTH=require (none, request, require, verify, require-verify)
//	GOQUEUE_TLS_SELF_SIGNED=true
func LoadTLSConfigFromEnv(prefix string) TLSConfig {
	config := DefaultTLSConfig()

	if os.Getenv(prefix+"_ENABLED") == "true" {
		config.Enabled = true
	}

	if cert := os.Getenv(prefix + "_CERT_FILE"); cert != "" {
		config.CertFile = cert
	}

	if key := os.Getenv(prefix + "_KEY_FILE"); key != "" {
		config.KeyFile = key
	}

	if ca := os.Getenv(prefix + "_CA_FILE"); ca != "" {
		config.CAFile = ca
	}

	if selfSigned := os.Getenv(prefix + "_SELF_SIGNED"); selfSigned == "true" {
		config.GenerateSelfSigned = true
	}

	// Parse client auth mode
	switch os.Getenv(prefix + "_CLIENT_AUTH") {
	case "none":
		config.ClientAuth = tls.NoClientCert
	case "request":
		config.ClientAuth = tls.RequestClientCert
	case "require":
		config.ClientAuth = tls.RequireAnyClientCert
	case "verify":
		config.ClientAuth = tls.VerifyClientCertIfGiven
	case "require-verify":
		config.ClientAuth = tls.RequireAndVerifyClientCert
	}

	// Parse min TLS version
	switch os.Getenv(prefix + "_MIN_VERSION") {
	case "1.2":
		config.MinVersion = tls.VersionTLS12
	case "1.3":
		config.MinVersion = tls.VersionTLS13
	}

	return config
}
