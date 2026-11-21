package server

// AUDIT — REST JWT IDOR regression tests.
//
// Original finding: doc/audit/findings/AUTH-001-rest-jwt-idor.md.
// The REST handlers used to read user_uid directly from the request body /
// query string despite jwtRequired having injected the verified claims.Sub
// into the context. Any holder of a valid JWT could act on any victim's uid.
//
// Status: **fixed**. Every REST handler now runs through resolveUserUID,
// which prefers auth.UserUIDFromContext over the request-supplied value.
// These tests assert the new behaviour and will fail if a future refactor
// drops the override.
import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/trackrecord/enclave/internal/auth"
	"go.uber.org/zap"
)

// TestAuditRESTJWTIDORCreateUserConnection pins the body-vs-JWT precedence
// on POST /api/v1/connection. Attacker's JWT sub wins over the victim uid
// smuggled in the body, and the connection service receives attackerUID.
func TestAuditRESTJWTIDORCreateUserConnection(t *testing.T) {
	const (
		victimUID   = "user_victim1234567890"
		attackerUID = "user_attacker098765432"
	)

	fake := &fakeHandlerConnectionService{}
	h := &Handler{logger: zap.NewNop(), connSvc: fake}

	body := map[string]any{
		"user_uid":   victimUID, // attacker smuggles victim's uid
		"exchange":   "alpaca",
		"label":      "main",
		"api_key":    "k",
		"api_secret": "s",
	}
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/connection", bytes.NewReader(raw))
	// jwtRequired would inject attackerUID for a valid attacker token.
	req = req.WithContext(auth.WithUserUID(req.Context(), attackerUID))
	rr := httptest.NewRecorder()
	h.CreateUserConnection(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("unexpected status %d body=%s", rr.Code, rr.Body.String())
	}
	if fake.lastReq == nil {
		t.Fatal("connection service Create was not called")
	}
	if fake.lastReq.UserUID != attackerUID {
		t.Fatalf("handler did not override body user_uid with JWT sub: got %q want %q (body was %q)", fake.lastReq.UserUID, attackerUID, victimUID)
	}
}

// TestAuditRESTJWTIDORDevModeFallback documents the dev-mode contract:
// when no JWT is injected into the context (ENCLAVE_JWT_SECRET unset, so
// jwtRequired skips auth), resolveUserUID falls back to the body value.
// This keeps the local dev harness working without env setup.
func TestAuditRESTJWTIDORDevModeFallback(t *testing.T) {
	const bodyUID = "user_devuser1234567890"

	fake := &fakeHandlerConnectionService{}
	h := &Handler{logger: zap.NewNop(), connSvc: fake}

	body := map[string]any{
		"user_uid":   bodyUID,
		"exchange":   "alpaca",
		"label":      "main",
		"api_key":    "k",
		"api_secret": "s",
	}
	raw, _ := json.Marshal(body)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/connection", bytes.NewReader(raw))
	// No auth.WithUserUID on the context — simulates dev mode.
	rr := httptest.NewRecorder()
	h.CreateUserConnection(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("unexpected status %d body=%s", rr.Code, rr.Body.String())
	}
	if fake.lastReq == nil {
		t.Fatal("connection service Create was not called")
	}
	if fake.lastReq.UserUID != bodyUID {
		t.Fatalf("dev mode should use body user_uid: got %q want %q", fake.lastReq.UserUID, bodyUID)
	}
}
