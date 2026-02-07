// =============================================================================
// OBSERVABILITY WITH PROMETHEUS - CORE METRICS INFRASTRUCTURE
// =============================================================================
//
// WHAT IS OBSERVABILITY?
// Observability is the ability to understand a system's internal state by
// examining its external outputs. The "three pillars" of observability are:
//
//   ┌─────────────────────────────────────────────────────────────────────────┐
//   │                    THREE PILLARS OF OBSERVABILITY                       │
//   │                                                                         │
//   │   ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                  │
//   │   │    LOGS      │  │   METRICS    │  │   TRACES     │                  │
//   │   │              │  │              │  │              │                  │
//   │   │ • Events     │  │ • Numbers    │  │ • Request    │                  │
//   │   │ • Text-based │  │ • Aggregated │  │   journey    │                  │
//   │   │ • High       │  │ • Time-series│  │ • Spans      │                  │
//   │   │   cardinality│  │ • Low cost   │  │ • Context    │                  │
//   │   │              │  │              │  │   propagation│                  │
//   │   │ "What        │  │ "How many/   │  │ "What path   │                  │
//   │   │  happened?"  │  │  how fast?"  │  │ did it take?"│                  │
//   │   └──────────────┘  └──────────────┘  └──────────────┘                  │
//   │                                                                         │
//   │   goqueue: Already has logs (slog) and traces (M7). Now adding metrics. │
//   └─────────────────────────────────────────────────────────────────────────┘
//
// WHAT IS PROMETHEUS?
// Prometheus is a time-series database and monitoring system. It:
//   1. SCRAPES metrics from your application's /metrics endpoint
//   2. STORES them in a time-series database
//   3. Allows QUERYING via PromQL
//   4. Triggers ALERTS based on conditions
//
// PULL vs PUSH MODEL:
//
//   PUSH MODEL (StatsD, Graphite):
//   ┌─────────┐  push   ┌─────────────┐
//   │   App   │────────►│  Collector  │
//   └─────────┘         └─────────────┘
//   - App decides when to send
//   - Network spikes possible
//   - Hard to know if app is dead
//
//   PULL MODEL (Prometheus):
//   ┌─────────┐  scrape ┌─────────────┐
//   │   App   │◄────────│ Prometheus  │
//   │ /metrics│         │   Server    │
//   └─────────┘         └─────────────┘
//   - Prometheus controls pace
//   - Constant network load
//   - Missing scrape = app might be down
//   - Simpler application code
//
// METRIC TYPES IN PROMETHEUS:
//
//   ┌─────────────────────────────────────────────────────────────────────────┐
//   │                      PROMETHEUS METRIC TYPES                            │
//   │                                                                         │
//   │   COUNTER                                                               │
//   │   ────────                                                              │
//   │   - Only goes up (or resets to 0 on restart)                            │
//   │   - Use for: requests, errors, bytes sent                               │
//   │   - Example: goqueue_messages_published_total                           │
//   │                                                                         │
//   │   GAUGE                                                                 │
//   │   ─────                                                                 │
//   │   - Can go up or down                                                   │
//   │   - Use for: current values, temperatures, queue depth                  │
//   │   - Example: goqueue_active_connections                                 │
//   │                                                                         │
//   │   HISTOGRAM                                                             │
//   │   ─────────                                                             │
//   │   - Samples observations into configurable buckets                      │
//   │   - Use for: latencies, request sizes                                   │
//   │   - Creates 3 metrics: _bucket, _sum, _count                            │
//   │   - Example: goqueue_publish_latency_seconds                            │
//   │                                                                         │
//   │   SUMMARY (less common)                                                 │
//   │   ───────                                                               │
//   │   - Like histogram but calculates quantiles client-side                 │
//   │   - Use when you need exact percentiles (not aggregatable)              │
//   │   - goqueue uses histograms (Prometheus best practice)                  │
//   │                                                                         │
//   └─────────────────────────────────────────────────────────────────────────┘
//
// LABELS (DIMENSIONS):
// Labels add dimensions to metrics. Be CAREFUL with cardinality!
//
//   GOOD LABELS (bounded cardinality):
//   - topic (tens to hundreds)
//   - consumer_group (tens)
//   - node_id (single digits)
//   - status (success, error)
//
//   BAD LABELS (unbounded cardinality):
//   - message_id (millions - NEVER!)
//   - client_id (thousands)
//   - timestamp (infinite)
//   - stack_trace (thousands)
//
//   WHY CARDINALITY MATTERS:
//   Each unique label combination = new time series
//   10 topics × 10 partitions × 5 nodes = 500 time series (OK)
//   10 topics × 10 partitions × 1000 clients = 100,000 time series (BAD!)
//
// NAMING CONVENTIONS:
// We follow Prometheus naming conventions:
//
//   {namespace}_{subsystem}_{name}_{unit}
//
//   - namespace: goqueue (the application)
//   - subsystem: broker, storage, consumer, cluster
//   - name: descriptive name
//   - unit: seconds, bytes, total (for counters)
//
//   Examples:
//   - goqueue_broker_messages_published_total
//   - goqueue_storage_bytes_written_total
//   - goqueue_consumer_lag_messages
//   - goqueue_broker_publish_latency_seconds
//
// COMPARISON WITH OTHER MESSAGE QUEUES:
//
//   ┌─────────────┬──────────────────────┬─────────────────────────────────┐
//   │ System      │ Metrics Approach     │ Key Metrics                     │
//   ├─────────────┼──────────────────────┼─────────────────────────────────┤
//   │ Kafka       │ JMX → Prometheus     │ BytesInPerSec, MessagesInPerSec │
//   │             │ via JMX Exporter     │ UnderReplicatedPartitions       │
//   │             │                      │ RequestLatency, ISRShrinkRate   │
//   ├─────────────┼──────────────────────┼─────────────────────────────────┤
//   │ RabbitMQ    │ Native Prometheus    │ queue_messages_ready            │
//   │             │ plugin               │ queue_messages_unacked          │
//   │             │                      │ connection_created_total        │
//   ├─────────────┼──────────────────────┼─────────────────────────────────┤
//   │ SQS         │ CloudWatch metrics   │ ApproximateNumberOfMessages     │
//   │             │                      │ NumberOfMessagesSent/Received   │
//   ├─────────────┼──────────────────────┼─────────────────────────────────┤
//   │ goqueue     │ Native Prometheus    │ messages_published_total        │
//   │             │ + OpenTelemetry      │ consumer_lag_messages           │
//   │             │                      │ publish_latency_seconds         │
//   └─────────────┴──────────────────────┴─────────────────────────────────┘
//
// =============================================================================

package metrics

import (
	"context"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// =============================================================================
// METRICS REGISTRY
// =============================================================================
//
// The Registry is a container for all metrics. Prometheus client uses a global
// default registry, but we create our own for:
//   - Testing isolation (each test gets fresh registry)
//   - Custom collectors (Go runtime stats, process stats)
//   - Multiple registries if needed (unlikely)
//
// REGISTRATION FLOW:
//
//   ┌───────────────┐  Create   ┌─────────────┐
//   │ NewCounter()  │──────────►│   Metric    │
//   └───────────────┘           └──────┬──────┘
//                                      │
//                                      │ MustRegister
//                                      ▼
//   ┌────────────────────────────────────────────────────────────────────┐
//   │                         REGISTRY                                   │
//   │  ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐       │
//   │  │ messages_total  │ │ bytes_written   │ │ latency_seconds │ ...   │
//   │  └─────────────────┘ └─────────────────┘ └─────────────────┘       │
//   └────────────────────────────────────────────────────────────────────┘
//                                      │
//                               GET /metrics
//                                      │
//                                      ▼
//   ┌────────────────────────────────────────────────────────────────────┐
//   │  # HELP goqueue_messages_total Total messages published            │
//   │  # TYPE goqueue_messages_total counter                             │
//   │  goqueue_messages_total{topic="orders"} 12345                      │
//   │  ...                                                               │
//   └────────────────────────────────────────────────────────────────────┘
//
// =============================================================================

// Registry holds all goqueue metrics and the Prometheus registry.
//
// WHY WRAP prometheus.Registry?
//   - Provides single initialization point
//   - Groups related metrics by subsystem
//   - Simplifies access from broker/storage/etc
//   - Allows metrics to be disabled via config
type Registry struct {
	// promRegistry is the underlying Prometheus registry
	promRegistry *prometheus.Registry

	// config holds metrics configuration
	config Config

	// logger for metrics operations
	logger *slog.Logger

	// enabled tracks if metrics collection is enabled
	enabled bool

	// Subsystem metrics - grouped for clarity
	Broker   *BrokerMetrics
	Storage  *StorageMetrics
	Consumer *ConsumerMetrics
	Cluster  *ClusterMetrics
}

// Config holds metrics configuration.
type Config struct {
	// Enabled turns metrics collection on/off
	// When disabled, all metric operations are no-ops
	Enabled bool

	// Namespace is the prefix for all metrics (default: "goqueue")
	Namespace string

	// IncludePartitionLabel adds partition label to metrics
	// WARNING: High cardinality if you have many partitions!
	// Set to false for clusters with 1000+ partitions
	IncludePartitionLabel bool

	// IncludeGoCollector adds Go runtime metrics (goroutines, GC, memory)
	// Recommended: true for debugging, may disable in high-load production
	IncludeGoCollector bool

	// IncludeProcessCollector adds process metrics (CPU, memory, file descriptors)
	// Recommended: true
	IncludeProcessCollector bool

	// HistogramBuckets for latency measurements (in seconds)
	// Default is optimized for sub-millisecond to multi-second operations
	HistogramBuckets []float64
}

// DefaultConfig returns sensible defaults for metrics configuration.
//
// BUCKET DESIGN:
// Our targets are p99 < 10ms publish, p99 < 5ms consume.
// Buckets should be dense around target latencies for accurate percentiles.
//
//	┌────────────────────────────────────────────────────────────────────────┐
//	│                 HISTOGRAM BUCKETS (in seconds)                         │
//	│                                                                        │
//	│   0.0005  0.001  0.002  0.005  0.01  0.025  0.05  0.1  0.25  0.5 1 2 5 │
//	│   (0.5ms) (1ms)  (2ms)  (5ms)  (10ms)       ...   ...        ...       │
//	│      │      │      │      │      │                                     │
//	│      └──────┴──────┴──────┴──────┴── Dense around targets              │
//	│                                       for accurate p50/p95/p99         │
//	└────────────────────────────────────────────────────────────────────────┘
func DefaultConfig() Config {
	return Config{
		Enabled:                 true,
		Namespace:               "goqueue",
		IncludePartitionLabel:   false, // Safe default - enable if needed
		IncludeGoCollector:      true,
		IncludeProcessCollector: true,
		HistogramBuckets: []float64{
			0.0005, // 0.5ms - for very fast operations
			0.001,  // 1ms
			0.002,  // 2ms
			0.005,  // 5ms - target for consume p99
			0.01,   // 10ms - target for publish p99
			0.025,  // 25ms
			0.05,   // 50ms
			0.1,    // 100ms
			0.25,   // 250ms
			0.5,    // 500ms
			1,      // 1s
			2,      // 2s - slow operations
			5,      // 5s - very slow / timeout
		},
	}
}

// =============================================================================
// GLOBAL REGISTRY (SINGLETON PATTERN)
// =============================================================================
//
// WHY SINGLETON?
// Metrics need to be accessible from many places:
//   - Broker (publish/consume)
//   - Storage (bytes read/written)
//   - Consumer groups (lag, rebalances)
//   - Cluster (leader elections)
//
// Two approaches:
//   1. DEPENDENCY INJECTION: Pass registry to every component
//      + Explicit, testable
//      - Verbose, must thread through many layers
//
//   2. SINGLETON: Global access via GetRegistry()
//      + Simple access from anywhere
//      - Harder to test (must reset between tests)
//
// WE USE HYBRID:
//   - Global singleton for production simplicity
//   - NewRegistry() for isolated testing
//
// =============================================================================

var (
	globalRegistry *Registry
	globalOnce     sync.Once
)

// Init initializes the global metrics registry with the given config.
// Should be called once at application startup.
//
// USAGE:
//
//	func main() {
//	    // Initialize metrics
//	    metrics.Init(metrics.DefaultConfig())
//	    defer metrics.Shutdown()
//
//	    // Start broker...
//	}
func Init(config Config) *Registry {
	globalOnce.Do(func() {
		globalRegistry = NewRegistry(config)
	})
	return globalRegistry
}

// Get returns the global metrics registry.
// Returns nil if Init() was not called.
func Get() *Registry {
	return globalRegistry
}

// Handler returns the HTTP handler for the /metrics endpoint.
// Returns nil if metrics are not initialized.
//
// USAGE:
//
//	router.Get("/metrics", metrics.Handler().ServeHTTP)
//
// Or check for nil:
//
//	if handler := metrics.Handler(); handler != nil {
//	    handler.ServeHTTP(w, r)
//	}
func Handler() http.Handler {
	if globalRegistry == nil {
		return nil
	}
	return globalRegistry.Handler()
}

// MustGet returns the global metrics registry, panics if not initialized.
// Use in code paths where metrics must be available.
func MustGet() *Registry {
	if globalRegistry == nil {
		panic("metrics: registry not initialized - call metrics.Init() first")
	}
	return globalRegistry
}

// =============================================================================
// REGISTRY CREATION
// =============================================================================

// NewRegistry creates a new metrics registry.
// Use Init() for the global singleton, or NewRegistry() for testing.
func NewRegistry(config Config) *Registry {
	logger := slog.Default().With("component", "metrics")

	r := &Registry{
		promRegistry: prometheus.NewRegistry(),
		config:       config,
		logger:       logger,
		enabled:      config.Enabled,
	}

	if !config.Enabled {
		logger.Info("metrics collection disabled")
		return r
	}

	// Register standard collectors
	//
	// GO COLLECTOR:
	// Exposes Go runtime metrics:
	//   - go_goroutines: Number of goroutines
	//   - go_gc_duration_seconds: GC pause duration
	//   - go_memstats_*: Memory statistics
	//
	// These are invaluable for debugging memory leaks and performance issues.
	if config.IncludeGoCollector {
		r.promRegistry.MustRegister(collectors.NewGoCollector())
	}

	// PROCESS COLLECTOR:
	// Exposes process metrics:
	//   - process_cpu_seconds_total: CPU usage
	//   - process_resident_memory_bytes: Memory usage
	//   - process_open_fds: Open file descriptors
	//   - process_start_time_seconds: When process started
	//
	// Essential for resource monitoring and alerting.
	if config.IncludeProcessCollector {
		r.promRegistry.MustRegister(collectors.NewProcessCollector(
			collectors.ProcessCollectorOpts{},
		))
	}

	// Initialize subsystem metrics
	r.Broker = newBrokerMetrics(r)
	r.Storage = newStorageMetrics(r)
	r.Consumer = newConsumerMetrics(r)
	r.Cluster = newClusterMetrics(r)

	logger.Info("metrics registry initialized",
		"namespace", config.Namespace,
		"include_partition_label", config.IncludePartitionLabel,
	)

	return r
}

// =============================================================================
// HTTP HANDLER
// =============================================================================
//
// The /metrics endpoint serves metrics in Prometheus exposition format.
// Prometheus scrapes this endpoint at regular intervals (default: 15s).
//
// EXPOSITION FORMAT:
//
//   # HELP goqueue_broker_messages_published_total Total messages published
//   # TYPE goqueue_broker_messages_published_total counter
//   goqueue_broker_messages_published_total{topic="orders"} 12345
//   goqueue_broker_messages_published_total{topic="events"} 67890
//
// CONTENT NEGOTIATION:
// promhttp.Handler automatically handles:
//   - text/plain (default, human readable)
//   - application/openmetrics-text (newer format with more features)
//
// =============================================================================

// Handler returns an HTTP handler for the /metrics endpoint.
//
// USAGE:
//
//	router.Handle("/metrics", metrics.Get().Handler())
//
// Or mount on a separate port for isolation:
//
//	http.ListenAndServe(":9090", metrics.Get().Handler())
func (r *Registry) Handler() http.Handler {
	if !r.enabled {
		// Return empty handler if metrics disabled
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("# Metrics disabled\n"))
		})
	}

	return promhttp.HandlerFor(r.promRegistry, promhttp.HandlerOpts{
		// EnableOpenMetrics enables the OpenMetrics format
		// This is the newer format with better support for exemplars
		EnableOpenMetrics: true,

		// ErrorLog logs errors during metric collection
		ErrorLog: &promLogger{logger: r.logger},

		// Registry is the source of metrics
		Registry: r.promRegistry,
	})
}

// promLogger adapts slog to Prometheus error logging interface.
type promLogger struct {
	logger *slog.Logger
}

func (l *promLogger) Println(v ...interface{}) {
	l.logger.Error("prometheus handler error", "error", v)
}

// =============================================================================
// UTILITY METHODS
// =============================================================================

// Enabled returns true if metrics collection is enabled.
func (r *Registry) Enabled() bool {
	return r.enabled
}

// Namespace returns the configured namespace.
func (r *Registry) Namespace() string {
	return r.config.Namespace
}

// Config returns the metrics configuration.
func (r *Registry) Config() Config {
	return r.config
}

// PrometheusRegistry returns the underlying Prometheus registry.
// Use sparingly - prefer using the subsystem metrics.
func (r *Registry) PrometheusRegistry() *prometheus.Registry {
	return r.promRegistry
}

// =============================================================================
// METRIC REGISTRATION HELPERS
// =============================================================================
//
// These helpers create metrics with consistent naming and labels.
// They handle:
//   - Namespacing (goqueue_)
//   - Subsystem prefixing (broker_, storage_, etc)
//   - Registration with the registry
//   - Error handling (panic on duplicate registration)
//

// newCounter creates and registers a new counter metric.
//
// COUNTER PROPERTIES:
//   - Only increases (or resets to 0 on restart)
//   - Use rate() in PromQL to get per-second rate
//   - Always append _total suffix for clarity
func (r *Registry) newCounter(opts prometheus.CounterOpts) prometheus.Counter {
	opts.Namespace = r.config.Namespace
	counter := prometheus.NewCounter(opts)
	r.promRegistry.MustRegister(counter)
	return counter
}

// newCounterVec creates and registers a new counter vector (with labels).
//
// COUNTER VECTOR:
// Like Counter, but with label dimensions.
//
//	counterVec.WithLabelValues("orders").Inc()
//	counterVec.WithLabelValues("events").Inc()
//
// Creates separate time series for each label combination.
func (r *Registry) newCounterVec(opts prometheus.CounterOpts, labelNames []string) *prometheus.CounterVec {
	opts.Namespace = r.config.Namespace
	counterVec := prometheus.NewCounterVec(opts, labelNames)
	r.promRegistry.MustRegister(counterVec)
	return counterVec
}

// newGauge creates and registers a new gauge metric.
//
// GAUGE PROPERTIES:
//   - Can go up or down
//   - Represents current value (not rate)
//   - Don't use rate() in PromQL (use deriv() if needed)
func (r *Registry) newGauge(opts prometheus.GaugeOpts) prometheus.Gauge {
	opts.Namespace = r.config.Namespace
	gauge := prometheus.NewGauge(opts)
	r.promRegistry.MustRegister(gauge)
	return gauge
}

// newGaugeVec creates and registers a new gauge vector (with labels).
func (r *Registry) newGaugeVec(opts prometheus.GaugeOpts, labelNames []string) *prometheus.GaugeVec {
	opts.Namespace = r.config.Namespace
	gaugeVec := prometheus.NewGaugeVec(opts, labelNames)
	r.promRegistry.MustRegister(gaugeVec)
	return gaugeVec
}

// newHistogram creates and registers a new histogram metric.
//
// HISTOGRAM PROPERTIES:
//   - Samples observations into buckets
//   - Creates 3 metrics: _bucket, _sum, _count
//   - Use histogram_quantile() in PromQL for percentiles
//
// BUCKET SELECTION IS CRITICAL:
//   - Too few buckets = inaccurate percentiles
//   - Too many buckets = higher cardinality
//   - Buckets should be dense around expected values
func (r *Registry) newHistogram(opts prometheus.HistogramOpts) prometheus.Histogram {
	opts.Namespace = r.config.Namespace
	if opts.Buckets == nil {
		opts.Buckets = r.config.HistogramBuckets
	}
	histogram := prometheus.NewHistogram(opts)
	r.promRegistry.MustRegister(histogram)
	return histogram
}

// newHistogramVec creates and registers a new histogram vector (with labels).
func (r *Registry) newHistogramVec(opts prometheus.HistogramOpts, labelNames []string) *prometheus.HistogramVec {
	opts.Namespace = r.config.Namespace
	if opts.Buckets == nil {
		opts.Buckets = r.config.HistogramBuckets
	}
	histogramVec := prometheus.NewHistogramVec(opts, labelNames)
	r.promRegistry.MustRegister(histogramVec)
	return histogramVec
}

// =============================================================================
// TIMING HELPERS
// =============================================================================
//
// These helpers make it easy to measure operation latencies.
// They use time.Since() and convert to seconds for Prometheus.
//
// USAGE PATTERN:
//
//	start := time.Now()
//	// ... do operation ...
//	metrics.Get().Broker.PublishLatency.WithLabelValues("orders").Observe(
//	    time.Since(start).Seconds(),
//	)
//
// Or use the Timer helper:
//
//	timer := metrics.NewTimer(metrics.Get().Broker.PublishLatency.WithLabelValues("orders"))
//	defer timer.ObserveDuration()
//	// ... do operation ...
//

// Timer measures the duration of an operation.
type Timer struct {
	start    time.Time
	observer prometheus.Observer
}

// NewTimer creates a new timer that will observe the given histogram/summary.
func NewTimer(observer prometheus.Observer) *Timer {
	return &Timer{
		start:    time.Now(),
		observer: observer,
	}
}

// ObserveDuration records the elapsed time since the timer was created.
// Typically called with defer:
//
//	timer := metrics.NewTimer(histogram)
//	defer timer.ObserveDuration()
func (t *Timer) ObserveDuration() time.Duration {
	elapsed := time.Since(t.start)
	if t.observer != nil {
		t.observer.Observe(elapsed.Seconds())
	}
	return elapsed
}

// =============================================================================
// OPENTELEMETRY INTEGRATION
// =============================================================================
//
// WHY BOTH PROMETHEUS AND OPENTELEMETRY?
//
// Prometheus is the de-facto standard for Kubernetes monitoring:
//   - Native support in most K8s tools
//   - Efficient pull-based model
//   - Excellent for infrastructure metrics
//
// OpenTelemetry is the future of observability:
//   - Vendor-neutral (works with any backend)
//   - Unified model for metrics/traces/logs
//   - Better for application-level instrumentation
//
// OUR APPROACH:
//   - Prometheus for core metrics (exposed via /metrics)
//   - OpenTelemetry for traces (already in M7)
//   - Bridge: OTLP can export metrics to Prometheus remote-write
//
// FUTURE CONSIDERATION:
// When OpenTelemetry metrics mature, we could use OTel SDK for metrics
// and export to both Prometheus and other backends. For now, direct
// Prometheus client is simpler and more battle-tested.
//
// =============================================================================

// WithOTLP adds OpenTelemetry metric export capability.
// This is a placeholder for future OTLP metrics integration.
//
// CURRENT STATE: Not implemented - using Prometheus directly
// FUTURE: Could add go.opentelemetry.io/otel/exporters/prometheus
func (r *Registry) WithOTLP(ctx context.Context, endpoint string) error {
	// TODO: Implement OTLP metric export when needed
	// For now, we use Prometheus directly for metrics
	// and OpenTelemetry for traces (M7)
	r.logger.Info("OTLP metrics export not yet implemented",
		"endpoint", endpoint,
	)
	return nil
}

// =============================================================================
// SHUTDOWN
// =============================================================================

// Shutdown gracefully shuts down the metrics registry.
// Currently a no-op, but reserved for future cleanup.
func (r *Registry) Shutdown() error {
	r.logger.Info("metrics registry shutdown")
	return nil
}

// Shutdown shuts down the global registry.
func Shutdown() error {
	if globalRegistry != nil {
		return globalRegistry.Shutdown()
	}
	return nil
}
