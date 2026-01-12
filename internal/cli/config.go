// =============================================================================
// CLI CONFIGURATION - CONFIG FILE AND CONTEXT MANAGEMENT
// =============================================================================
//
// WHAT IS THIS?
// Configuration management for the goqueue CLI, supporting:
//   - Multiple cluster contexts (like kubectl contexts)
//   - Config file (~/.goqueue/config.yaml)
//   - Environment variable overrides
//   - Command-line flag overrides
//
// CONFIGURATION PRECEDENCE (highest to lowest):
//   1. Command-line flags (--server, --context)
//   2. Environment variables (GOQUEUE_SERVER, GOQUEUE_CONTEXT)
//   3. Config file (current-context determines active cluster)
//   4. Default values (http://localhost:8080)
//
// CONFIG FILE FORMAT (~/.goqueue/config.yaml):
//
//   current-context: production
//   contexts:
//     local:
//       server: http://localhost:8080
//       api-key: ""
//     staging:
//       server: https://goqueue.staging.example.com
//       api-key: "staging-key-123"
//     production:
//       server: https://goqueue.prod.example.com
//       api-key: "prod-key-456"
//
// COMPARISON WITH OTHER CLIs:
//   - kubectl: Uses ~/.kube/config with contexts, clusters, users
//   - aws: Uses ~/.aws/config with profiles
//   - docker: Uses ~/.docker/config.json
//   - goqueue: Similar to kubectl but simpler (no users/auth complexity)
//
// =============================================================================

package cli

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

// =============================================================================
// CONFIGURATION STRUCTURES
// =============================================================================

// Config represents the CLI configuration file.
type Config struct {
	// CurrentContext is the name of the active context
	CurrentContext string `yaml:"current-context"`

	// Contexts maps context names to their configurations
	Contexts map[string]*ContextConfig `yaml:"contexts"`
}

// ContextConfig contains configuration for a single cluster context.
type ContextConfig struct {
	// Server is the base URL of the goqueue server
	Server string `yaml:"server"`

	// APIKey for authentication (optional)
	APIKey string `yaml:"api-key,omitempty"`

	// Timeout in seconds (optional, default 30)
	Timeout int `yaml:"timeout,omitempty"`
}

// =============================================================================
// DEFAULT PATHS
// =============================================================================

// DefaultConfigDir returns the default config directory (~/.goqueue).
func DefaultConfigDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ".goqueue"
	}
	return filepath.Join(home, ".goqueue")
}

// DefaultConfigPath returns the default config file path.
func DefaultConfigPath() string {
	return filepath.Join(DefaultConfigDir(), "config.yaml")
}

// =============================================================================
// CONFIGURATION LOADING
// =============================================================================

// LoadConfig loads configuration from the default path.
func LoadConfig() (*Config, error) {
	return LoadConfigFromPath(DefaultConfigPath())
}

// LoadConfigFromPath loads configuration from a specific path.
func LoadConfigFromPath(path string) (*Config, error) {
	// Check if file exists
	if _, err := os.Stat(path); os.IsNotExist(err) {
		// Return default config if file doesn't exist
		return DefaultConfig(), nil
	}

	// Read file
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	// Parse YAML
	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	// Ensure contexts map exists
	if config.Contexts == nil {
		config.Contexts = make(map[string]*ContextConfig)
	}

	return &config, nil
}

// DefaultConfig returns a default configuration.
func DefaultConfig() *Config {
	return &Config{
		CurrentContext: "local",
		Contexts: map[string]*ContextConfig{
			"local": {
				Server:  "http://localhost:8080",
				Timeout: 30,
			},
		},
	}
}

// =============================================================================
// CONFIGURATION SAVING
// =============================================================================

// Save saves the configuration to the default path.
func (c *Config) Save() error {
	return c.SaveToPath(DefaultConfigPath())
}

// SaveToPath saves the configuration to a specific path.
func (c *Config) SaveToPath(path string) error {
	// Create directory if needed
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return fmt.Errorf("failed to create config directory: %w", err)
	}

	// Encode YAML
	data, err := yaml.Marshal(c)
	if err != nil {
		return fmt.Errorf("failed to encode config: %w", err)
	}

	// Write file with restricted permissions
	if err := os.WriteFile(path, data, 0600); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	return nil
}

// =============================================================================
// CONTEXT OPERATIONS
// =============================================================================

// GetCurrentContext returns the current context configuration.
func (c *Config) GetCurrentContext() (*ContextConfig, error) {
	if c.CurrentContext == "" {
		return nil, errors.New("no current context set")
	}

	ctx, ok := c.Contexts[c.CurrentContext]
	if !ok {
		return nil, fmt.Errorf("context %q not found", c.CurrentContext)
	}

	return ctx, nil
}

// GetContext returns a specific context by name.
func (c *Config) GetContext(name string) (*ContextConfig, error) {
	ctx, ok := c.Contexts[name]
	if !ok {
		return nil, fmt.Errorf("context %q not found", name)
	}
	return ctx, nil
}

// SetContext sets or updates a context.
func (c *Config) SetContext(name string, ctx *ContextConfig) {
	if c.Contexts == nil {
		c.Contexts = make(map[string]*ContextConfig)
	}
	c.Contexts[name] = ctx
}

// DeleteContext removes a context.
func (c *Config) DeleteContext(name string) error {
	if _, ok := c.Contexts[name]; !ok {
		return fmt.Errorf("context %q not found", name)
	}

	delete(c.Contexts, name)

	// Clear current context if it was the deleted one
	if c.CurrentContext == name {
		c.CurrentContext = ""
	}

	return nil
}

// UseContext sets the current context.
func (c *Config) UseContext(name string) error {
	if _, ok := c.Contexts[name]; !ok {
		return fmt.Errorf("context %q not found", name)
	}
	c.CurrentContext = name
	return nil
}

// ListContexts returns all context names.
func (c *Config) ListContexts() []string {
	names := make([]string, 0, len(c.Contexts))
	for name := range c.Contexts {
		names = append(names, name)
	}
	return names
}

// =============================================================================
// ENVIRONMENT VARIABLE OVERRIDES
// =============================================================================

// Environment variable names
const (
	EnvServer  = "GOQUEUE_SERVER"
	EnvContext = "GOQUEUE_CONTEXT"
	EnvAPIKey  = "GOQUEUE_API_KEY"
	EnvTimeout = "GOQUEUE_TIMEOUT"
)

// ResolveServer determines the server URL to use with proper precedence.
// Precedence: flag > env > config
func ResolveServer(flagValue string, config *Config) string {
	// Flag takes highest precedence
	if flagValue != "" {
		return flagValue
	}

	// Environment variable next
	if env := os.Getenv(EnvServer); env != "" {
		return env
	}

	// Config file
	if config != nil {
		if ctx, err := config.GetCurrentContext(); err == nil && ctx.Server != "" {
			return ctx.Server
		}
	}

	// Default
	return "http://localhost:8080"
}

// ResolveAPIKey determines the API key to use with proper precedence.
func ResolveAPIKey(flagValue string, config *Config) string {
	if flagValue != "" {
		return flagValue
	}

	if env := os.Getenv(EnvAPIKey); env != "" {
		return env
	}

	if config != nil {
		if ctx, err := config.GetCurrentContext(); err == nil {
			return ctx.APIKey
		}
	}

	return ""
}
