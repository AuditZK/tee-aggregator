package logstream

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/trackrecord/enclave/internal/attestation"
	"go.uber.org/zap"
)

type fakeAttestationProvider struct {
	report *attestation.AttestationReport
	err    error
}

func (f *fakeAttestationProvider) GetAttestation(_ context.Context) (*attestation.AttestationReport, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.report, nil
}

func TestAuthMiddleware_NoAPIKey_ReturnsServiceUnavailable(t *testing.T) {
	s := NewServer(0, "", zap.NewNop())
	h := s.authMiddleware(s.handleLogs)

	req := httptest.NewRequest(http.MethodGet, "/logs", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rec.Code)
	}

	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("failed to parse JSON response: %v", err)
	}
	if _, ok := body["error"]; !ok {
		t.Fatalf("expected error field, got %v", body)
	}
}

func TestAuthMiddleware_InvalidAPIKey_ReturnsUnauthorized(t *testing.T) {
	s := NewServer(0, "secret", zap.NewNop())
	h := s.authMiddleware(s.handleLogs)

	req := httptest.NewRequest(http.MethodGet, "/logs", nil)
	req.Header.Set("X-Api-Key", "bad")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}

func TestHandleHealth_ResponseShape(t *testing.T) {
	s := NewServer(0, "secret", zap.NewNop())
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	s.handleHealth(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("failed to parse JSON response: %v", err)
	}
	if body["service"] != "enclave-log-server" {
		t.Fatalf("unexpected service name: %v", body["service"])
	}
}

func TestHandleLogs_ResponseShape(t *testing.T) {
	s := NewServer(0, "secret", zap.NewNop())
	s.logs = []LogEntry{
		{Timestamp: "2026-02-28T00:00:00Z", Level: "info", Message: "hello"},
	}

	h := s.authMiddleware(s.handleLogs)
	req := httptest.NewRequest(http.MethodGet, "/logs", nil)
	req.Header.Set("X-Api-Key", "secret")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var body struct {
		Logs  []LogEntry `json:"logs"`
		Count int        `json:"count"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("failed to parse JSON response: %v", err)
	}
	if body.Count != 1 || len(body.Logs) != 1 {
		t.Fatalf("unexpected logs payload: count=%d len=%d", body.Count, len(body.Logs))
	}
}

func TestHandleClear_ResponseShape(t *testing.T) {
	s := NewServer(0, "secret", zap.NewNop())
	s.logs = []LogEntry{{Timestamp: "t", Level: "info", Message: "msg"}}

	h := s.authMiddleware(s.handleClear)
	req := httptest.NewRequest(http.MethodPost, "/logs/clear", nil)
	req.Header.Set("X-Api-Key", "secret")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("failed to parse JSON response: %v", err)
	}
	if body["success"] != true {
		t.Fatalf("expected success=true, got %v", body["success"])
	}
}

func TestHandleAttestation_Success(t *testing.T) {
	s := NewServer(0, "secret", zap.NewNop())
	s.SetAttestationService(&fakeAttestationProvider{
		report: &attestation.AttestationReport{
			Platform: "sev-snp",
			Attestation: &attestation.SevSnpReport{
				Verified:      true,
				SevSnpEnabled: true,
				Measurement:   "abc",
				ReportData:    "def",
			},
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/attestation", nil)
	rec := httptest.NewRecorder()
	s.handleAttestation(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("failed to parse JSON response: %v", err)
	}
	if body["verified"] != true || body["sevSnpEnabled"] != true {
		t.Fatalf("unexpected attestation payload: %v", body)
	}
}

func TestHandleAttestationInfo_Success(t *testing.T) {
	s := NewServer(0, "secret", zap.NewNop())
	s.SetAttestationService(&fakeAttestationProvider{
		report: &attestation.AttestationReport{
			Platform: "sev-snp",
			Attestation: &attestation.SevSnpReport{
				SevSnpEnabled: true,
			},
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/attestation/info", nil)
	rec := httptest.NewRecorder()
	s.handleAttestationInfo(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("failed to parse JSON response: %v", err)
	}
	if body["platform"] != "AMD SEV-SNP" {
		t.Fatalf("unexpected platform: %v", body["platform"])
	}
	if body["sevSnpAvailable"] != true {
		t.Fatalf("unexpected sevSnpAvailable: %v", body["sevSnpAvailable"])
	}
}
