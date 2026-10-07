package server

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHandleAdminSyncConnection_Validation(t *testing.T) {
	s := &Server{}

	call := func(method, qs string) int {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(method, "/api/v1/admin/sync-connection?"+qs, nil)
		s.handleAdminSyncConnection(rec, req)
		return rec.Code
	}

	if got := call(http.MethodGet, "user_uid=user_abc1234567890&exchange=ibkr&label=main"); got != http.StatusMethodNotAllowed {
		t.Errorf("GET: got %d, want 405", got)
	}

	bad := []struct{ name, qs string }{
		{"missing user_uid", "exchange=ibkr"},
		{"bad user_uid", "user_uid=bad%20uid%21&exchange=ibkr"},
		{"missing exchange", "user_uid=user_abc1234567890"},
		{"bad exchange", "user_uid=user_abc1234567890&exchange=BAD%2FEX"},
		{"label with delimiter", "user_uid=user_abc1234567890&exchange=ibkr&label=a%2Fb"},
	}
	for _, tc := range bad {
		if got := call(http.MethodPost, tc.qs); got != http.StatusBadRequest {
			t.Errorf("%s: got %d, want 400", tc.name, got)
		}
	}

	if got := call(http.MethodPost, "user_uid=user_abc1234567890&exchange=ibkr&label=main"); got != http.StatusServiceUnavailable {
		t.Errorf("valid inputs without a sync service: got %d, want 503", got)
	}
}
