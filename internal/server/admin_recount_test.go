package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/trackrecord/enclave/internal/service"
)

type fakeRecounter struct {
	calls int
	apply bool
}

func (f *fakeRecounter) RecountActivity(_ context.Context, _, _, _ string, from, _ time.Time, apply bool) ([]service.RecountDay, error) {
	f.calls++
	f.apply = apply
	return []service.RecountDay{{Day: from}}, nil
}

func TestAdminRecountActivity(t *testing.T) {
	const uid = "0f1e2d3c-4b5a-5968-8776-a5b4c3d2e1f0"
	const window = "&from=2026-09-13&to=2026-10-06"
	cases := []struct {
		name      string
		method    string
		query     string
		status    int
		wantCall  bool
		wantApply bool
	}{
		{"GET refused", http.MethodGet, "user_uid=" + uid + "&exchange=bybit&label=main" + window, http.StatusMethodNotAllowed, false, false},
		{"bad uid", http.MethodPost, "user_uid=nope&exchange=bybit&label=main" + window, http.StatusBadRequest, false, false},
		{"bad label", http.MethodPost, "user_uid=" + uid + "&exchange=bybit&label=a/b" + window, http.StatusBadRequest, false, false},
		{"missing window", http.MethodPost, "user_uid=" + uid + "&exchange=bybit&label=main", http.StatusBadRequest, false, false},
		{"dry run by default", http.MethodPost, "user_uid=" + uid + "&exchange=bybit&label=main" + window, http.StatusOK, true, false},
		{"apply only when asked", http.MethodPost, "user_uid=" + uid + "&exchange=bybit&label=main" + window + "&apply=1", http.StatusOK, true, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeRecounter{}
			s := &Server{logger: zap.NewNop(), recountSvc: fake}
			w := httptest.NewRecorder()
			s.handleAdminRecountActivity(w, httptest.NewRequest(tc.method, "/api/v1/admin/recount-activity?"+tc.query, nil))

			if w.Code != tc.status {
				t.Fatalf("status = %d, want %d (%s)", w.Code, tc.status, w.Body.String())
			}
			if (fake.calls == 1) != tc.wantCall || fake.apply != tc.wantApply {
				t.Fatalf("calls = %d apply = %v, want call %v apply %v", fake.calls, fake.apply, tc.wantCall, tc.wantApply)
			}
		})
	}
}
