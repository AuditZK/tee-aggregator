package logstream

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/trackrecord/enclave/internal/attestation"
	"go.uber.org/zap"
)

const maxBufferedLogs = 1000

// LogEntry represents a single log entry for streaming.
type LogEntry struct {
	Timestamp string                 `json:"timestamp"`
	Level     string                 `json:"level"`
	Message   string                 `json:"message"`
	Fields    map[string]interface{} `json:"fields,omitempty"`
}

// Server provides HTTP log streaming via SSE.
type Server struct {
	logger  *zap.Logger
	apiKey  string
	port    int
	attest  AttestationProvider
	clients map[chan LogEntry]struct{}
	logs    []LogEntry
	mu      sync.RWMutex
	server  *http.Server
}

// AttestationProvider describes the subset used by the log server.
type AttestationProvider interface {
	GetAttestation(ctx context.Context) (*attestation.AttestationReport, error)
}

// NewServer creates a new log streaming server.
func NewServer(port int, apiKey string, logger *zap.Logger) *Server {
	return &Server{
		logger:  logger,
		apiKey:  apiKey,
		port:    port,
		clients: make(map[chan LogEntry]struct{}),
	}
}

// SetAttestationService configures attestation endpoints backing service.
func (s *Server) SetAttestationService(provider AttestationProvider) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.attest = provider
}

// Start starts the log streaming HTTP server.
func (s *Server) Start(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/attestation", s.handleAttestation)
	mux.HandleFunc("/attestation/info", s.handleAttestationInfo)
	mux.HandleFunc("/logs", s.authMiddleware(s.handleLogs))
	mux.HandleFunc("/logs/stream", s.authMiddleware(s.handleStream))
	mux.HandleFunc("/logs/clear", s.authMiddleware(s.handleClear))

	s.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", s.port),
		Handler: mux,
	}

	s.logger.Info("log stream server starting", zap.Int("port", s.port))
	go func() {
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.logger.Error("log stream server error", zap.Error(err))
		}
	}()

	return nil
}

// Stop gracefully stops the server.
func (s *Server) Stop() error {
	if s.server == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.server.Shutdown(ctx)
}

// Broadcast sends a log entry to all connected SSE clients and buffers it.
func (s *Server) Broadcast(entry LogEntry) {
	s.mu.Lock()
	s.logs = append(s.logs, entry)
	if len(s.logs) > maxBufferedLogs {
		s.logs = s.logs[len(s.logs)-maxBufferedLogs:]
	}

	// Copy clients to avoid holding lock during send
	clients := make([]chan LogEntry, 0, len(s.clients))
	for ch := range s.clients {
		clients = append(clients, ch)
	}
	s.mu.Unlock()

	for _, ch := range clients {
		select {
		case ch <- entry:
		default:
			// Client is slow, skip
		}
	}
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status":  "ok",
		"service": "enclave-log-server",
	})
}

func (s *Server) handleAttestation(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	s.mu.RLock()
	attest := s.attest
	s.mu.RUnlock()
	if attest == nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{
			"verified":      false,
			"enclave":       false,
			"sevSnpEnabled": false,
			"error":         "Failed to retrieve attestation",
			"timestamp":     time.Now().UTC().Format(time.RFC3339Nano),
		})
		return
	}

	report, err := attest.GetAttestation(r.Context())
	if err != nil {
		s.logger.Error("attestation endpoint failed", zap.Error(err))
		writeJSON(w, http.StatusInternalServerError, map[string]any{
			"verified":      false,
			"enclave":       false,
			"sevSnpEnabled": false,
			"error":         "Failed to retrieve attestation",
			"timestamp":     time.Now().UTC().Format(time.RFC3339Nano),
		})
		return
	}

	att := report.Attestation
	if att == nil {
		writeJSON(w, http.StatusOK, map[string]any{
			"verified":        false,
			"enclave":         false,
			"sevSnpEnabled":   false,
			"measurement":     nil,
			"reportData":      nil,
			"platformVersion": nil,
			"timestamp":       time.Now().UTC().Format(time.RFC3339Nano),
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"verified":        att.Verified,
		"enclave":         report.Platform == "sev-snp",
		"sevSnpEnabled":   att.SevSnpEnabled,
		"measurement":     att.Measurement,
		"reportData":      att.ReportData,
		"platformVersion": att.PlatformVersion,
		"errorMessage":    "",
		"timestamp":       time.Now().UTC().Format(time.RFC3339Nano),
	})
}

func (s *Server) handleAttestationInfo(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	s.mu.RLock()
	attest := s.attest
	s.mu.RUnlock()
	if attest == nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{
			"error":     "Failed to retrieve attestation info",
			"timestamp": time.Now().UTC().Format(time.RFC3339Nano),
		})
		return
	}

	report, err := attest.GetAttestation(r.Context())
	if err != nil {
		s.logger.Error("attestation info endpoint failed", zap.Error(err))
		writeJSON(w, http.StatusInternalServerError, map[string]any{
			"error":     "Failed to retrieve attestation info",
			"timestamp": time.Now().UTC().Format(time.RFC3339Nano),
		})
		return
	}

	platform := "unknown"
	attestationMethod := "none"
	if report.Platform == "sev-snp" {
		platform = "AMD SEV-SNP"
		attestationMethod = "hardware"
	}
	sevSnpAvailable := report.Attestation != nil && report.Attestation.SevSnpEnabled

	writeJSON(w, http.StatusOK, map[string]any{
		"platform":          platform,
		"sevSnpAvailable":   sevSnpAvailable,
		"attestationMethod": attestationMethod,
		"timestamp":         time.Now().UTC().Format(time.RFC3339Nano),
	})
}

func (s *Server) handleLogs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	s.mu.RLock()
	logs := make([]LogEntry, len(s.logs))
	copy(logs, s.logs)
	s.mu.RUnlock()

	writeJSON(w, http.StatusOK, map[string]any{
		"logs":  logs,
		"count": len(logs),
	})
}

func (s *Server) handleStream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	// Register client
	ch := make(chan LogEntry, 100)
	s.mu.Lock()
	s.clients[ch] = struct{}{}
	s.mu.Unlock()

	// Cleanup on disconnect
	defer func() {
		s.mu.Lock()
		delete(s.clients, ch)
		s.mu.Unlock()
		close(ch)
	}()

	ctx := r.Context()
	for {
		select {
		case entry := <-ch:
			data, _ := json.Marshal(entry)
			fmt.Fprintf(w, "data: %s\n\n", data)
			flusher.Flush()
		case <-ctx.Done():
			return
		}
	}
}

func (s *Server) handleClear(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	s.mu.Lock()
	s.logs = nil
	s.mu.Unlock()

	writeJSON(w, http.StatusOK, map[string]any{
		"success": true,
		"message": "Logs cleared",
	})
}

func (s *Server) authMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.apiKey == "" {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{
				"error": "Log server not configured. Set LOG_SERVER_API_KEY environment variable.",
			})
			return
		}

		key := r.Header.Get("X-Api-Key")
		if key == "" {
			key = r.URL.Query().Get("apiKey")
		}

		if key != s.apiKey {
			writeJSON(w, http.StatusUnauthorized, map[string]any{
				"error": "Invalid or missing API key. Provide X-Api-Key header or ?apiKey= query param.",
			})
			return
		}

		next(w, r)
	}
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}
