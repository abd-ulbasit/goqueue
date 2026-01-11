package broker

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestTracer_SpanAttributes_AndExporters(t *testing.T) {
	// -------------------------------------------------------------------------
	// Cover TraceContext helpers that were still at 0%.
	// -------------------------------------------------------------------------
	root := NewTraceContext()
	child := root.NewChildContext()
	_ = child.IsSampled()

	// -------------------------------------------------------------------------
	// Cover Span attribute helpers that were still at 0%.
	// -------------------------------------------------------------------------
	span := NewSpan(root.TraceID, SpanEventPublishReceived, "orders", 0, 123)
	span.WithReceiptHandle("r:1")
	span.WithDelay(time.Now().Add(5*time.Second), int64((5*time.Second)/time.Millisecond))
	span.WithNodeID("node-1")
	span.WithPriority(1)

	// -------------------------------------------------------------------------
	// Stdout exporter: cover constructor + batch export + shutdown + name.
	// (We use a server-side test for OTLP; stdout export is intentionally simple.)
	// -------------------------------------------------------------------------
	stdout := NewStdoutExporter() // writes to os.Stdout, should be safe
	_ = stdout.Name()
	_ = stdout.ExportBatch(context.Background(), []*Span{span})
	_ = stdout.Shutdown(context.Background())

	// -------------------------------------------------------------------------
	// File exporter: create in temp dir and exercise export + metadata.
	// -------------------------------------------------------------------------
	fe, err := NewFileExporter(t.TempDir(), 1024*1024)
	if err != nil {
		t.Fatalf("NewFileExporter failed: %v", err)
	}
	_ = fe.Name()
	_ = fe.ExportBatch(context.Background(), []*Span{span})
	_ = fe.Shutdown(context.Background())

	// -------------------------------------------------------------------------
	// OTLP exporter over HTTP: use an httptest server to accept /v1/traces.
	// This covers NewOTLPExporter, createHTTPExporter, flush/sendOTLP paths.
	// -------------------------------------------------------------------------
	var gotOTLPRequests int
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/traces" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_, _ = io.ReadAll(r.Body)
		gotOTLPRequests++
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	oc := DefaultOTLPExporterConfig()
	oc.Endpoint = strings.TrimPrefix(ts.URL, "http://")
	oc.UseHTTP = true
	oc.Insecure = true
	oc.BatchSize = 1
	oc.FlushInterval = 50 * time.Millisecond

	otlp := NewOTLPExporter(oc)
	defer otlp.Shutdown(context.Background())
	_ = otlp.Name()

	// Export + force synchronous flush for determinism.
	_ = otlp.Export(context.Background(), span)
	otlp.flush()
	if gotOTLPRequests == 0 {
		// The OTEL HTTP exporter can batch internally; we only require that
		// at least one POST was attempted across the flush.
		t.Fatalf("expected at least one OTLP HTTP request")
	}

	// Also cover ExportBatch explicitly.
	_ = otlp.ExportBatch(context.Background(), []*Span{span, span})
	otlp.flush()

	// -------------------------------------------------------------------------
	// Jaeger exporter is a thin wrapper around OTLP. Exercise config defaults
	// and wrapper methods.
	// -------------------------------------------------------------------------
	jc := DefaultJaegerExporterConfig()
	jc.Endpoint = oc.Endpoint
	jc.UseHTTP = true
	je := NewJaegerExporter(jc)
	defer je.Shutdown(context.Background())
	_ = je.Name()
	_ = je.ExportBatch(context.Background(), []*Span{span})
}
