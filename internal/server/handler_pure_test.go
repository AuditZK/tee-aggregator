package server

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	tlspkg "github.com/trackrecord/enclave/internal/tls"
	"go.uber.org/zap/zaptest"
)

func TestConnectionKey(t *testing.T) {
	cases := []struct {
		name     string
		exchange string
		label    string
		want     string
	}{
		{"both lowercased", "Binance", "Main", "binance/main"},
		{"trims whitespace", "  binance  ", "  main  ", "binance/main"},
		{"empty label drops slash", "binance", "", "binance"},
		{"whitespace-only label drops slash", "binance", "   ", "binance"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := connectionKey(tc.exchange, tc.label); got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestIsConnectionExcluded(t *testing.T) {
	excluded := map[string]struct{}{
		"binance/main":  {},
		"kraken":        {},
	}
	cases := []struct {
		name     string
		exchange string
		label    string
		want     bool
	}{
		{"exact match exchange/label", "binance", "main", true},
		{"exchange-only entry matches any label", "kraken", "anything", true},
		{"miss", "okx", "main", false},
		{"label miss", "binance", "secondary", false},
		{"empty excluded → never excluded", "x", "y", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			set := excluded
			if tc.name == "empty excluded → never excluded" {
				set = nil
			}
			if got := isConnectionExcluded(set, tc.exchange, tc.label); got != tc.want {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}

// SRV-001: sanitizeErr collapses every error to a generic string in
// production, preserves the underlying message in dev so operators can
// debug locally.
func TestSanitizeErr(t *testing.T) {
	h := &Handler{}

	t.Setenv("ENV", "production")
	if got := h.sanitizeErr(errors.New("pgx: relation does not exist")); got != genericInternalError {
		t.Fatalf("prod: got %q, want generic", got)
	}
	if h.sanitizeErr(nil) != "" {
		t.Fatal("nil err must yield empty string")
	}

	t.Setenv("ENV", "development")
	if got := h.sanitizeErr(errors.New("specific detail")); got != "specific detail" {
		t.Fatalf("dev: got %q", got)
	}
}

func TestIsProduction(t *testing.T) {
	t.Setenv("NODE_ENV", "")
	t.Setenv("ENV", "production")
	if !isProduction() {
		t.Error("ENV=production")
	}
	t.Setenv("ENV", "Production") // case-insensitive (lowercased internally)
	if !isProduction() {
		t.Error("Production should match")
	}
	t.Setenv("ENV", "development")
	t.Setenv("NODE_ENV", "production")
	if !isProduction() {
		t.Error("NODE_ENV=production should match")
	}
	t.Setenv("ENV", "")
	t.Setenv("NODE_ENV", "")
	if isProduction() {
		t.Error("both empty → not production")
	}
	// Restore: the test framework will reset env vars after Setenv on
	// test exit.
	_ = os.Getenv("ENV")
}

func TestHexDecode(t *testing.T) {
	got, err := hexDecode("deadbeef")
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if hex.EncodeToString(got) != "deadbeef" {
		t.Fatalf("got %x", got)
	}
	if _, err := hexDecode("zzzz"); err == nil {
		t.Fatal("expected error on non-hex input")
	}
}

func TestHandler_HealthCheck(t *testing.T) {
	h := &Handler{logger: zaptest.NewLogger(t)}

	rec := httptest.NewRecorder()
	h.HealthCheck(rec, httptest.NewRequest(http.MethodGet, "/health", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d", rec.Code)
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("ct=%q", got)
	}
	var out map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out["status"] != "ok" {
		t.Fatalf("status=%v", out["status"])
	}
	if out["service"] != "enclave-rest" {
		t.Fatalf("service=%v", out["service"])
	}
}

func TestHandler_HealthCheck_RejectsNonGET(t *testing.T) {
	h := &Handler{logger: zaptest.NewLogger(t)}
	rec := httptest.NewRecorder()
	h.HealthCheck(rec, httptest.NewRequest(http.MethodPost, "/health", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status=%d", rec.Code)
	}
}

func TestHandler_GetTLSFingerprint_NoTLS(t *testing.T) {
	h := &Handler{logger: zaptest.NewLogger(t)}
	rec := httptest.NewRecorder()
	h.GetTLSFingerprint(rec, httptest.NewRequest(http.MethodGet, "/api/v1/tls/fingerprint", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d, want 503", rec.Code)
	}
}

func TestHandler_GetTLSFingerprint_RejectsNonGET(t *testing.T) {
	h := &Handler{logger: zaptest.NewLogger(t)}
	rec := httptest.NewRecorder()
	h.GetTLSFingerprint(rec, httptest.NewRequest(http.MethodPost, "/api/v1/tls/fingerprint", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status=%d", rec.Code)
	}
}

func TestHandler_GetTLSFingerprint_ReturnsJSON(t *testing.T) {
	kg, err := tlspkg.NewKeyGenerator()
	if err != nil {
		t.Fatalf("keygen: %v", err)
	}
	h := &Handler{logger: zaptest.NewLogger(t), tlsKeygen: kg}

	rec := httptest.NewRecorder()
	h.GetTLSFingerprint(rec, httptest.NewRequest(http.MethodGet, "/api/v1/tls/fingerprint", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d", rec.Code)
	}
	var out map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	fp, ok := out["fingerprint"].(string)
	if !ok || fp == "" {
		t.Fatalf("missing fingerprint field: %v", out)
	}
	if !strings.EqualFold(fp, kg.Fingerprint()) {
		t.Fatalf("mismatch: got %q, kg %q", fp, kg.Fingerprint())
	}
	if out["algorithm"] != "SHA-256" {
		t.Fatalf("algorithm=%v", out["algorithm"])
	}
}

func TestHandler_GetAttestation_RejectsBadMethod(t *testing.T) {
	h := &Handler{logger: zaptest.NewLogger(t)}
	rec := httptest.NewRecorder()
	h.GetAttestation(rec, httptest.NewRequest(http.MethodDelete, "/api/v1/attestation", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status=%d", rec.Code)
	}
}

func TestHandler_GetAttestation_NoServiceConfigured(t *testing.T) {
	h := &Handler{logger: zaptest.NewLogger(t)}
	rec := httptest.NewRecorder()
	h.GetAttestation(rec, httptest.NewRequest(http.MethodGet, "/api/v1/attestation", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d, want 503", rec.Code)
	}
}
