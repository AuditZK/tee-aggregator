package server

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// SEC-10: /api/v1/admin/reconstruct validates user_uid/exchange/label at the
// trust boundary, like every other REST entrypoint.
func TestHandleAdminReconstruct_Validation(t *testing.T) {
	s := &Server{}

	post := func(qs string) int {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/reconstruct?"+qs, nil)
		s.handleAdminReconstruct(rec, req)
		return rec.Code
	}

	bad := []struct{ name, qs string }{
		{"missing user_uid", "exchange=alpaca"},
		{"bad user_uid", "user_uid=bad%20uid%21&exchange=alpaca"},
		{"missing exchange", "user_uid=user_abc1234567890"},
		{"bad exchange", "user_uid=user_abc1234567890&exchange=BAD%2FEX"},
		{"label with delimiter", "user_uid=user_abc1234567890&exchange=alpaca&label=a%2Fb"},
	}
	for _, tc := range bad {
		if got := post(tc.qs); got != http.StatusBadRequest {
			t.Errorf("%s: got %d, want 400", tc.name, got)
		}
	}

	// Valid inputs pass validation and reach the service check (503 here, since
	// no sync service is wired) — proving they were NOT rejected as 400.
	if got := post("user_uid=user_abc1234567890&exchange=alpaca&label=main"); got == http.StatusBadRequest {
		t.Errorf("valid inputs were wrongly rejected as 400")
	}
}
