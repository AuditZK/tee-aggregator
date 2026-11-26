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

type fakeFreeMargin struct {
	calls int
	apply bool
	err   error
}

func (f *fakeFreeMargin) RewriteRebuiltFreeMargin(_ context.Context, _, _, _ string, from, _ time.Time, apply bool) (*service.FreeMarginRewrite, error) {
	f.calls++
	f.apply = apply
	if f.err != nil {
		return nil, f.err
	}
	return &service.FreeMarginRewrite{Apply: apply, Days: []service.FreeMarginDay{{Day: from}}}, nil
}

func TestAdminRewriteFreeMargin(t *testing.T) {
	const uid = "0134b0b7-6aec-50ec-b0bf-bde0ae11fcbd"
	const window = "&from=2026-08-29&to=2026-11-19"
	cases := []struct {
		name      string
		method    string
		query     string
		err       error
		status    int
		wantCall  bool
		wantApply bool
	}{
		{"GET refused", http.MethodGet, "user_uid=" + uid + "&exchange=binance&label=efrxx" + window, nil, http.StatusMethodNotAllowed, false, false},
		{"bad uid", http.MethodPost, "user_uid=nope&exchange=binance&label=efrxx" + window, nil, http.StatusBadRequest, false, false},
		{"missing window", http.MethodPost, "user_uid=" + uid + "&exchange=binance&label=efrxx", nil, http.StatusBadRequest, false, false},
		{"dry run by default", http.MethodPost, "user_uid=" + uid + "&exchange=binance&label=efrxx" + window, nil, http.StatusOK, true, false},
		{"apply only when asked", http.MethodPost, "user_uid=" + uid + "&exchange=binance&label=efrxx" + window + "&apply=1", nil, http.StatusOK, true, true},
		{"apply without dry run", http.MethodPost, "user_uid=" + uid + "&exchange=binance&label=efrxx" + window + "&apply=1", service.ErrFreeMarginNoDryRun, http.StatusConflict, true, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeFreeMargin{err: tc.err}
			s := &Server{logger: zap.NewNop(), freeMarginSvc: fake}
			w := httptest.NewRecorder()
			s.handleAdminRewriteFreeMargin(w, httptest.NewRequest(tc.method, "/api/v1/admin/rewrite-free-margin?"+tc.query, nil))
			if w.Code != tc.status {
				t.Fatalf("status = %d, want %d (%s)", w.Code, tc.status, w.Body.String())
			}
			if (fake.calls == 1) != tc.wantCall || fake.apply != tc.wantApply {
				t.Fatalf("calls = %d apply = %v, want call %v apply %v", fake.calls, fake.apply, tc.wantCall, tc.wantApply)
			}
		})
	}
}
