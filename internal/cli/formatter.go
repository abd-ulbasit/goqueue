// =============================================================================
// CLI OUTPUT FORMATTER - TABLE, JSON, YAML OUTPUT SUPPORT
// =============================================================================
//
// WHAT IS THIS?
// Output formatting utilities for the CLI, supporting multiple output formats:
//   - Table (default): Human-readable ASCII tables
//   - JSON: Machine-readable, for scripting with jq
//   - YAML: Machine-readable, configuration-friendly
//
// WHY MULTIPLE FORMATS?
//
//   ┌─────────────────────────────────────────────────────────────────────────┐
//   │  DIFFERENT USERS, DIFFERENT NEEDS                                       │
//   │                                                                         │
//   │  Human (Terminal):                                                      │
//   │    $ goqueue topic list                                                 │
//   │    NAME           PARTITIONS   MESSAGES   SIZE                          │
//   │    orders         3            15234      2.1 MB                        │
//   │    payments       6            98421      12.4 MB                       │
//   │                                                                         │
//   │  Script (JSON + jq):                                                    │
//   │    $ goqueue topic list -o json | jq '.[].name'                         │
//   │    "orders"                                                             │
//   │    "payments"                                                           │
//   │                                                                         │
//   │  Config (YAML):                                                         │
//   │    $ goqueue topic describe orders -o yaml > backup.yaml                │
//   │    name: orders                                                         │
//   │    partitions: 3                                                        │
//   └─────────────────────────────────────────────────────────────────────────┘
//
// COMPARISON:
//   - kubectl: Supports -o json, yaml, wide, name, custom-columns, jsonpath
//   - aws cli: Supports --output json, text, table, yaml
//   - docker: Supports --format with Go templates
//   - goqueue: Supports -o table, json, yaml (simple, covers 90% of use cases)
//
// =============================================================================

package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"

	"gopkg.in/yaml.v3"
)

// =============================================================================
// OUTPUT FORMAT
// =============================================================================

// OutputFormat represents the output format type.
type OutputFormat string

// Supported output formats
const (
	OutputTable OutputFormat = "table"
	OutputJSON  OutputFormat = "json"
	OutputYAML  OutputFormat = "yaml"
)

// ParseOutputFormat parses an output format string.
func ParseOutputFormat(s string) (OutputFormat, error) {
	switch strings.ToLower(s) {
	case "table", "":
		return OutputTable, nil
	case "json":
		return OutputJSON, nil
	case "yaml", "yml":
		return OutputYAML, nil
	default:
		return "", fmt.Errorf("unknown output format: %s (supported: table, json, yaml)", s)
	}
}

// =============================================================================
// FORMATTER
// =============================================================================

// Formatter handles output formatting for CLI commands.
type Formatter struct {
	format OutputFormat
	writer io.Writer
}

// NewFormatter creates a new formatter with the specified format.
func NewFormatter(format OutputFormat) *Formatter {
	return &Formatter{
		format: format,
		writer: os.Stdout,
	}
}

// SetWriter sets the output writer (for testing).
func (f *Formatter) SetWriter(w io.Writer) {
	f.writer = w
}

// Format outputs data in the configured format.
func (f *Formatter) Format(data interface{}) error {
	switch f.format {
	case OutputJSON:
		return f.formatJSON(data)
	case OutputYAML:
		return f.formatYAML(data)
	default:
		// Table format requires specific handling per data type
		return fmt.Errorf("use specific table method for data type")
	}
}

// formatJSON outputs data as JSON.
func (f *Formatter) formatJSON(data interface{}) error {
	encoder := json.NewEncoder(f.writer)
	encoder.SetIndent("", "  ")
	return encoder.Encode(data)
}

// formatYAML outputs data as YAML.
func (f *Formatter) formatYAML(data interface{}) error {
	encoder := yaml.NewEncoder(f.writer)
	encoder.SetIndent(2)
	return encoder.Encode(data)
}

// =============================================================================
// TABLE FORMATTING
// =============================================================================

// Table creates a new table writer.
func (f *Formatter) Table() *TableWriter {
	return &TableWriter{
		tw:      tabwriter.NewWriter(f.writer, 0, 0, 2, ' ', 0),
		headers: nil,
	}
}

// TableWriter wraps tabwriter for convenient table output.
type TableWriter struct {
	tw      *tabwriter.Writer
	headers []string
}

// SetHeaders sets the table headers.
func (t *TableWriter) SetHeaders(headers ...string) {
	t.headers = headers
}

// WriteHeaders writes the headers row.
func (t *TableWriter) WriteHeaders() {
	if len(t.headers) == 0 {
		return
	}
	// Convert to uppercase for visual distinction
	upper := make([]string, len(t.headers))
	for i, h := range t.headers {
		upper[i] = strings.ToUpper(h)
	}
	fmt.Fprintln(t.tw, strings.Join(upper, "\t"))
}

// WriteRow writes a single row.
func (t *TableWriter) WriteRow(values ...interface{}) {
	strs := make([]string, len(values))
	for i, v := range values {
		strs[i] = fmt.Sprint(v)
	}
	fmt.Fprintln(t.tw, strings.Join(strs, "\t"))
}

// Flush flushes the table writer.
func (t *TableWriter) Flush() error {
	return t.tw.Flush()
}

// =============================================================================
// SPECIFIC DATA TYPE FORMATTERS
// =============================================================================

// FormatTopics outputs a list of topics.
func (f *Formatter) FormatTopics(topics []string) error {
	if f.format == OutputJSON {
		return f.formatJSON(topics)
	}
	if f.format == OutputYAML {
		return f.formatYAML(topics)
	}

	// Table format
	table := f.Table()
	table.SetHeaders("NAME")
	table.WriteHeaders()
	for _, topic := range topics {
		table.WriteRow(topic)
	}
	return table.Flush()
}

// TopicDetail contains topic information for formatting.
type TopicDetail struct {
	Name           string `json:"name" yaml:"name"`
	Partitions     int    `json:"partitions" yaml:"partitions"`
	TotalMessages  int64  `json:"total_messages" yaml:"total_messages"`
	TotalSizeBytes int64  `json:"total_size_bytes" yaml:"total_size_bytes"`
}

// FormatTopicDetails outputs detailed topic information.
func (f *Formatter) FormatTopicDetails(topics []TopicDetail) error {
	if f.format == OutputJSON {
		return f.formatJSON(topics)
	}
	if f.format == OutputYAML {
		return f.formatYAML(topics)
	}

	// Table format
	table := f.Table()
	table.SetHeaders("NAME", "PARTITIONS", "MESSAGES", "SIZE")
	table.WriteHeaders()
	for _, t := range topics {
		table.WriteRow(t.Name, t.Partitions, t.TotalMessages, formatBytes(t.TotalSizeBytes))
	}
	return table.Flush()
}

// FormatTopicInfo outputs a single topic's detailed info.
func (f *Formatter) FormatTopicInfo(info *TopicInfo) error {
	if f.format == OutputJSON {
		return f.formatJSON(info)
	}
	if f.format == OutputYAML {
		return f.formatYAML(info)
	}

	// Table format - key-value style
	fmt.Fprintf(f.writer, "Name:           %s\n", info.Name)
	fmt.Fprintf(f.writer, "Partitions:     %d\n", info.Partitions)
	fmt.Fprintf(f.writer, "Total Messages: %d\n", info.TotalMessages)
	fmt.Fprintf(f.writer, "Total Size:     %s\n", formatBytes(info.TotalSizeBytes))
	fmt.Fprintln(f.writer)
	fmt.Fprintln(f.writer, "PARTITION OFFSETS:")

	table := f.Table()
	table.SetHeaders("PARTITION", "EARLIEST", "LATEST")
	table.WriteHeaders()
	for part, offsets := range info.PartitionOffsets {
		table.WriteRow(part, offsets["earliest"], offsets["latest"])
	}
	return table.Flush()
}

// FormatGroups outputs a list of consumer groups.
func (f *Formatter) FormatGroups(groups []string) error {
	if f.format == OutputJSON {
		return f.formatJSON(groups)
	}
	if f.format == OutputYAML {
		return f.formatYAML(groups)
	}

	// Table format
	table := f.Table()
	table.SetHeaders("GROUP ID")
	table.WriteHeaders()
	for _, g := range groups {
		table.WriteRow(g)
	}
	return table.Flush()
}

// FormatGroupInfo outputs detailed group information.
func (f *Formatter) FormatGroupInfo(info *GroupInfo) error {
	if f.format == OutputJSON {
		return f.formatJSON(info)
	}
	if f.format == OutputYAML {
		return f.formatYAML(info)
	}

	// Table format - key-value style
	fmt.Fprintf(f.writer, "Group ID:    %s\n", info.ID)
	fmt.Fprintf(f.writer, "State:       %s\n", info.State)
	fmt.Fprintf(f.writer, "Generation:  %d\n", info.Generation)
	fmt.Fprintf(f.writer, "Topics:      %s\n", strings.Join(info.Topics, ", "))
	fmt.Fprintln(f.writer)
	fmt.Fprintln(f.writer, "MEMBERS:")

	table := f.Table()
	table.SetHeaders("MEMBER ID", "CLIENT ID", "PARTITIONS", "LAST HEARTBEAT")
	table.WriteHeaders()
	for _, m := range info.Members {
		partitions := formatPartitions(m.AssignedPartitions)
		table.WriteRow(m.ID, m.ClientID, partitions, m.LastHeartbeat)
	}
	return table.Flush()
}

// FormatOffsets outputs offset information.
func (f *Formatter) FormatOffsets(resp *GetOffsetsResponse) error {
	if f.format == OutputJSON {
		return f.formatJSON(resp)
	}
	if f.format == OutputYAML {
		return f.formatYAML(resp)
	}

	// Table format
	fmt.Fprintf(f.writer, "Group ID: %s\n\n", resp.GroupID)

	table := f.Table()
	table.SetHeaders("TOPIC", "PARTITION", "OFFSET", "LAG")
	table.WriteHeaders()
	for _, o := range resp.Offsets {
		table.WriteRow(o.Topic, o.Partition, o.Offset, o.Lag)
	}
	return table.Flush()
}

// FormatMessages outputs consumed messages.
func (f *Formatter) FormatMessages(resp *ConsumeResponse) error {
	if f.format == OutputJSON {
		return f.formatJSON(resp)
	}
	if f.format == OutputYAML {
		return f.formatYAML(resp)
	}

	// Table format
	table := f.Table()
	table.SetHeaders("OFFSET", "TIMESTAMP", "KEY", "VALUE", "PRIORITY")
	table.WriteHeaders()
	for _, m := range resp.Messages {
		// Truncate value if too long
		value := m.Value
		if len(value) > 50 {
			value = value[:47] + "..."
		}
		table.WriteRow(m.Offset, m.Timestamp, m.Key, value, m.Priority)
	}
	return table.Flush()
}

// FormatPublishResults outputs publish results.
func (f *Formatter) FormatPublishResults(resp *PublishResponse) error {
	if f.format == OutputJSON {
		return f.formatJSON(resp)
	}
	if f.format == OutputYAML {
		return f.formatYAML(resp)
	}

	// Table format
	table := f.Table()
	table.SetHeaders("PARTITION", "OFFSET", "PRIORITY", "DELAYED", "ERROR")
	table.WriteHeaders()
	for _, r := range resp.Results {
		delayed := ""
		if r.Delayed {
			delayed = r.DeliverAt
		}
		errStr := r.Error
		if errStr == "" {
			errStr = "-"
		}
		table.WriteRow(r.Partition, r.Offset, r.Priority, delayed, errStr)
	}
	return table.Flush()
}

// FormatTraces outputs trace information.
func (f *Formatter) FormatTraces(traces []TraceInfo) error {
	if f.format == OutputJSON {
		return f.formatJSON(traces)
	}
	if f.format == OutputYAML {
		return f.formatYAML(traces)
	}

	// Table format
	table := f.Table()
	table.SetHeaders("TRACE ID", "TOPIC", "PARTITION", "OFFSET", "STATUS", "DURATION")
	table.WriteHeaders()
	for _, t := range traces {
		table.WriteRow(t.TraceID, t.Topic, t.Partition, t.Offset, t.Status, t.Duration)
	}
	return table.Flush()
}

// FormatTraceDetail outputs detailed trace information.
func (f *Formatter) FormatTraceDetail(trace *TraceInfo) error {
	if f.format == OutputJSON {
		return f.formatJSON(trace)
	}
	if f.format == OutputYAML {
		return f.formatYAML(trace)
	}

	// Key-value style
	fmt.Fprintf(f.writer, "Trace ID:   %s\n", trace.TraceID)
	fmt.Fprintf(f.writer, "Topic:      %s\n", trace.Topic)
	fmt.Fprintf(f.writer, "Partition:  %d\n", trace.Partition)
	fmt.Fprintf(f.writer, "Offset:     %d\n", trace.Offset)
	fmt.Fprintf(f.writer, "Status:     %s\n", trace.Status)
	fmt.Fprintf(f.writer, "Start:      %s\n", trace.StartTime)
	fmt.Fprintf(f.writer, "End:        %s\n", trace.EndTime)
	fmt.Fprintf(f.writer, "Duration:   %s\n", trace.Duration)
	fmt.Fprintln(f.writer)
	fmt.Fprintln(f.writer, "EVENTS:")

	table := f.Table()
	table.SetHeaders("TYPE", "TIMESTAMP", "DETAILS")
	table.WriteHeaders()
	for _, e := range trace.Events {
		table.WriteRow(e.Type, e.Timestamp, e.Details)
	}
	return table.Flush()
}

// FormatBrokerStats outputs broker statistics.
func (f *Formatter) FormatBrokerStats(stats *BrokerStats) error {
	if f.format == OutputJSON {
		return f.formatJSON(stats)
	}
	if f.format == OutputYAML {
		return f.formatYAML(stats)
	}

	// Key-value style
	fmt.Fprintf(f.writer, "Node ID:      %s\n", stats.NodeID)
	fmt.Fprintf(f.writer, "Uptime:       %s\n", stats.Uptime)
	fmt.Fprintf(f.writer, "Topics:       %d\n", stats.Topics)
	fmt.Fprintf(f.writer, "Total Size:   %s\n", formatBytes(stats.TotalSizeBytes))
	return nil
}

// FormatClusterState outputs cluster state.
func (f *Formatter) FormatClusterState(state *ClusterState) error {
	if f.format == OutputJSON {
		return f.formatJSON(state)
	}
	if f.format == OutputYAML {
		return f.formatYAML(state)
	}

	// Key-value + table
	fmt.Fprintf(f.writer, "Controller:    %s\n", state.ControllerID)
	fmt.Fprintf(f.writer, "Version:       %d\n", state.Version)
	fmt.Fprintf(f.writer, "Epoch:         %d\n", state.ControllerEpoch)
	fmt.Fprintln(f.writer)
	fmt.Fprintln(f.writer, "NODES:")

	table := f.Table()
	table.SetHeaders("ID", "CLIENT ADDRESS", "PEER ADDRESS", "STATUS", "CONTROLLER")
	table.WriteHeaders()
	for _, n := range state.Nodes {
		controller := ""
		if n.IsController {
			controller = "✓"
		}
		table.WriteRow(n.ID, n.ClientAddress, n.PeerAddress, n.Status, controller)
	}
	return table.Flush()
}

// FormatHealth outputs health status.
func (f *Formatter) FormatHealth(health *HealthResponse) error {
	if f.format == OutputJSON {
		return f.formatJSON(health)
	}
	if f.format == OutputYAML {
		return f.formatYAML(health)
	}

	fmt.Fprintf(f.writer, "Status:    %s\n", health.Status)
	fmt.Fprintf(f.writer, "Timestamp: %s\n", health.Timestamp)
	return nil
}

// FormatVersion outputs version information.
func (f *Formatter) FormatVersion(info *VersionInfo) error {
	if f.format == OutputJSON {
		return f.formatJSON(info)
	}
	if f.format == OutputYAML {
		return f.formatYAML(info)
	}

	fmt.Fprintf(f.writer, "Client Version: %s\n", info.ClientVersion)
	if info.ServerVersion != "" {
		fmt.Fprintf(f.writer, "Server Version: %s\n", info.ServerVersion)
	}
	return nil
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

// formatBytes formats a byte count as human-readable.
func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

// formatPartitions formats a list of partitions for display.
func formatPartitions(partitions []TopicPartition) string {
	if len(partitions) == 0 {
		return "-"
	}
	parts := make([]string, len(partitions))
	for i, p := range partitions {
		parts[i] = fmt.Sprintf("%s:%d", p.Topic, p.Partition)
	}
	return strings.Join(parts, ", ")
}

// PrintError prints an error message to stderr.
func PrintError(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "Error: "+format+"\n", args...)
}

// PrintSuccess prints a success message.
func PrintSuccess(format string, args ...interface{}) {
	fmt.Printf("✓ "+format+"\n", args...)
}

// PrintInfo prints an info message.
func PrintInfo(format string, args ...interface{}) {
	fmt.Printf(format+"\n", args...)
}
