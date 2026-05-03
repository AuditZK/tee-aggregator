package metrics

import (
	"context"
	"fmt"
	"net/http"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
)

// QUAL-001: Prometheus exposition format strings, extracted to remove the
// 3-way duplications across the counter/gauge/histogram render paths.
const (
	promFmtHelp          = "# HELP %s %s\n"
	promFmtMetric        = "%s %g\n"
	promFmtMetricLabeled = "%s{labels=\"%s\"} %g\n"
)

// Metrics collects and exposes Prometheus-compatible metrics.
type Metrics struct {
	counters   map[string]*counter
	gauges     map[string]*gauge
	histograms map[string]*histogram
	mu         sync.RWMutex
	server     *http.Server
	logger     *zap.Logger
}

type counter struct {
	name   string
	help   string
	values map[string]float64
	mu     sync.Mutex
}

type gauge struct {
	name   string
	help   string
	values map[string]float64
	mu     sync.Mutex
}

type histogram struct {
	name   string
	help   string
	values map[string]float64
	mu     sync.Mutex
}

// New creates a new metrics service with predefined metrics.
func New(logger *zap.Logger) *Metrics {
	m := &Metrics{
		counters:   make(map[string]*counter),
		gauges:     make(map[string]*gauge),
		histograms: make(map[string]*histogram),
		logger:     logger,
	}

	// Counters
	m.registerCounter("grpc_requests_total", "Total gRPC requests")
	m.registerCounter("db_queries_total", "Total database queries")
	m.registerCounter("sync_jobs_total", "Total sync jobs")
	m.registerCounter("snapshots_created_total", "Total snapshots created")
	m.registerCounter("exchange_connections_total", "Total exchange connections created")
	m.registerCounter("enclave_attestation_success_total", "Successful attestations")
	m.registerCounter("enclave_attestation_failure_total", "Failed attestations")

	// Gauges
	m.registerGauge("grpc_active_connections", "Active gRPC connections")
	m.registerGauge("process_memory_bytes", "Process memory usage in bytes")
	m.registerGauge("process_goroutines", "Number of goroutines")

	// Histograms (simplified: stores latest value per label)
	m.registerHistogram("grpc_request_duration_seconds", "gRPC request duration")
	m.registerHistogram("db_query_duration_seconds", "Database query duration")

	return m
}

// Start starts the metrics HTTP server.
func (m *Metrics) Start(port int) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", m.handleMetrics)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok","service":"metrics"}`))
	})

	m.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}

	m.logger.Info("metrics server starting", zap.Int("port", port))
	go func() {
		if err := m.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			m.logger.Error("metrics server error", zap.Error(err))
		}
	}()

	return nil
}

// Stop gracefully stops the metrics server.
func (m *Metrics) Stop() error {
	if m.server == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return m.server.Shutdown(ctx)
}

// IncrCounter increments a counter by 1.
func (m *Metrics) IncrCounter(name string, labels ...string) {
	m.mu.RLock()
	c, ok := m.counters[name]
	m.mu.RUnlock()
	if !ok {
		return
	}
	key := strings.Join(labels, ",")
	c.mu.Lock()
	c.values[key]++
	c.mu.Unlock()
}

// SetGauge sets a gauge value.
func (m *Metrics) SetGauge(name string, value float64, labels ...string) {
	m.mu.RLock()
	g, ok := m.gauges[name]
	m.mu.RUnlock()
	if !ok {
		return
	}
	key := strings.Join(labels, ",")
	g.mu.Lock()
	g.values[key] = value
	g.mu.Unlock()
}

// ObserveHistogram records a histogram observation.
func (m *Metrics) ObserveHistogram(name string, value float64, labels ...string) {
	m.mu.RLock()
	h, ok := m.histograms[name]
	m.mu.RUnlock()
	if !ok {
		return
	}
	key := strings.Join(labels, ",")
	h.mu.Lock()
	h.values[key] = value
	h.mu.Unlock()
}

// UpdateSystemMetrics updates process-level metrics.
func (m *Metrics) UpdateSystemMetrics() {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	m.SetGauge("process_memory_bytes", float64(memStats.Alloc))
	m.SetGauge("process_goroutines", float64(runtime.NumGoroutine()))
}

func (m *Metrics) registerCounter(name, help string) {
	m.counters[name] = &counter{name: name, help: help, values: make(map[string]float64)}
}

func (m *Metrics) registerGauge(name, help string) {
	m.gauges[name] = &gauge{name: name, help: help, values: make(map[string]float64)}
}

func (m *Metrics) registerHistogram(name, help string) {
	m.histograms[name] = &histogram{name: name, help: help, values: make(map[string]float64)}
}

func (m *Metrics) handleMetrics(w http.ResponseWriter, r *http.Request) {
	m.UpdateSystemMetrics()

	var sb strings.Builder

	// Counters
	names := make([]string, 0, len(m.counters))
	for name := range m.counters {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		c := m.counters[name]
		c.mu.Lock()
		sb.WriteString(fmt.Sprintf(promFmtHelp, c.name, c.help))
		sb.WriteString(fmt.Sprintf("# TYPE %s counter\n", c.name))
		if len(c.values) == 0 {
			sb.WriteString(fmt.Sprintf("%s 0\n", c.name))
		}
		for labels, val := range c.values {
			if labels == "" {
				sb.WriteString(fmt.Sprintf(promFmtMetric, c.name, val))
			} else {
				sb.WriteString(fmt.Sprintf(promFmtMetricLabeled, c.name, labels, val))
			}
		}
		c.mu.Unlock()
	}

	// Gauges
	names = names[:0]
	for name := range m.gauges {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		g := m.gauges[name]
		g.mu.Lock()
		sb.WriteString(fmt.Sprintf(promFmtHelp, g.name, g.help))
		sb.WriteString(fmt.Sprintf("# TYPE %s gauge\n", g.name))
		if len(g.values) == 0 {
			sb.WriteString(fmt.Sprintf("%s 0\n", g.name))
		}
		for labels, val := range g.values {
			if labels == "" {
				sb.WriteString(fmt.Sprintf(promFmtMetric, g.name, val))
			} else {
				sb.WriteString(fmt.Sprintf(promFmtMetricLabeled, g.name, labels, val))
			}
		}
		g.mu.Unlock()
	}

	// Histograms (simplified)
	names = names[:0]
	for name := range m.histograms {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		h := m.histograms[name]
		h.mu.Lock()
		sb.WriteString(fmt.Sprintf(promFmtHelp, h.name, h.help))
		sb.WriteString(fmt.Sprintf("# TYPE %s gauge\n", h.name))
		for labels, val := range h.values {
			if labels == "" {
				sb.WriteString(fmt.Sprintf(promFmtMetric, h.name, val))
			} else {
				sb.WriteString(fmt.Sprintf(promFmtMetricLabeled, h.name, labels, val))
			}
		}
		h.mu.Unlock()
	}

	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	w.Write([]byte(sb.String()))
}
