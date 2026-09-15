package server

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// SEC-10: /api/v1/admin/refresh-metadata validates user_uid/exchange/label at
// the trust boundary. exchange and label are optional here — they narrow the
// pass rather than address a single connection — but must still be validated
// when present.
func TestHandleAdminRefreshMetadata_Validation(t *testing.T) {
	s := &Server{}

	post := func(qs string) int {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/refresh-metadata?"+qs, nil)
		s.handleAdminRefreshMetadata(rec, req)
		return rec.Code
	}

	bad := []struct{ name, qs string }{
		{"missing user_uid", ""},
		{"bad user_uid", "user_uid=bad%20uid%21"},
		{"bad exchange", "user_uid=user_abc1234567890&exchange=BAD%2FEX"},
		{"label with delimiter", "user_uid=user_abc1234567890&label=a%2Fb"},
	}
	for _, tc := range bad {
		if got := post(tc.qs); got != http.StatusBadRequest {
			t.Errorf("%s: got %d, want 400", tc.name, got)
		}
	}

	// user_uid alone is enough: the whole point is re-probing every connection
	// a user has. It must reach the service check (503, no sync service wired).
	if got := post("user_uid=user_abc1234567890"); got == http.StatusBadRequest {
		t.Errorf("user_uid alone was wrongly rejected as 400")
	}

	rec := httptest.NewRecorder()
	s.handleAdminRefreshMetadata(rec, httptest.NewRequest(http.MethodGet, "/api/v1/admin/refresh-metadata?user_uid=user_abc1234567890", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET: got %d, want 405 — a re-probe writes, it is not a read", rec.Code)
	}
}
