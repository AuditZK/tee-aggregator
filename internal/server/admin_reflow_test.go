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

type fakeReflower struct {
	calls int
	apply bool
	from  time.Time
}

func (f *fakeReflower) ReflowCashflows(_ context.Context, _, _, _ string, from, _ time.Time, apply bool) ([]service.ReflowDay, error) {
	f.calls++
	f.apply, f.from = apply, from
	return []service.ReflowDay{{Day: from}}, nil
}

func TestAdminReflow(t *testing.T) {
	const uid = "0f1e2d3c-4b5a-5968-8776-a5b4c3d2e1f0"
	cases := []struct {
		name      string
		method    string
		query     string
		status    int
		wantCall  bool
		wantApply bool
	}{
		{"GET refused", http.MethodGet, "user_uid=" + uid + "&exchange=binance&label=main&from=2026-08-01&to=2026-09-01", http.StatusMethodNotAllowed, false, false},
		{"bad uid", http.MethodPost, "user_uid=nope&exchange=binance&label=main&from=2026-08-01&to=2026-09-01", http.StatusBadRequest, false, false},
		{"bad label", http.MethodPost, "user_uid=" + uid + "&exchange=binance&label=a/b&from=2026-08-01&to=2026-09-01", http.StatusBadRequest, false, false},
		{"missing window", http.MethodPost, "user_uid=" + uid + "&exchange=binance&label=main", http.StatusBadRequest, false, false},
		{"dry run by default", http.MethodPost, "user_uid=" + uid + "&exchange=binance&label=main&from=2026-08-01&to=2026-09-01", http.StatusOK, true, false},
		{"apply only when asked", http.MethodPost, "user_uid=" + uid + "&exchange=binance&label=main&from=2026-08-01&to=2026-09-01&apply=1", http.StatusOK, true, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeReflower{}
			s := &Server{logger: zap.NewNop(), reflowSvc: fake}
			w := httptest.NewRecorder()
			s.handleAdminReflow(w, httptest.NewRequest(tc.method, "/api/v1/admin/reflow?"+tc.query, nil))

			if w.Code != tc.status {
				t.Fatalf("status = %d, want %d (%s)", w.Code, tc.status, w.Body.String())
			}
			if (fake.calls == 1) != tc.wantCall || fake.apply != tc.wantApply {
				t.Fatalf("calls = %d apply = %v, want call %v apply %v", fake.calls, fake.apply, tc.wantCall, tc.wantApply)
			}
		})
	}
}
