package server

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// SEC-10: /api/v1/admin/raw-statement hands back live account data, so it
// validates its inputs like every other REST entrypoint and refuses anything
// but a read.
func TestHandleAdminRawStatement_Validation(t *testing.T) {
	s := &Server{}

	get := func(qs string) int {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/raw-statement?"+qs, nil)
		s.handleAdminRawStatement(rec, req)
		return rec.Code
	}

	bad := []struct{ name, qs string }{
		{"missing user_uid", "exchange=ibkr"},
		{"bad user_uid", "user_uid=bad%20uid%21&exchange=ibkr"},
		{"missing exchange", "user_uid=user_abc1234567890"},
		{"bad exchange", "user_uid=user_abc1234567890&exchange=BAD%2FEX"},
		{"label with delimiter", "user_uid=user_abc1234567890&exchange=ibkr&label=a%2Fb"},
	}
	for _, tc := range bad {
		if got := get(tc.qs); got != http.StatusBadRequest {
			t.Errorf("%s: got %d, want 400", tc.name, got)
		}
	}

	if got := get("user_uid=user_abc1234567890&exchange=ibkr&label=main"); got == http.StatusBadRequest {
		t.Errorf("valid inputs were wrongly rejected as 400")
	}

	rec := httptest.NewRecorder()
	s.handleAdminRawStatement(rec, httptest.NewRequest(http.MethodPost,
		"/api/v1/admin/raw-statement?user_uid=user_abc1234567890&exchange=ibkr", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("POST: got %d, want 405", rec.Code)
	}
}
