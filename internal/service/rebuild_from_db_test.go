package service

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/trackrecord/enclave/internal/rebuilderclient"
)

// The start month is stored only for a connection that asked for the rebuild.
func TestDBCreateStoresTheRebuildMonthWithTheRebuild(t *testing.T) {
	month := utcDay(2025, 3, 1)
	for _, tc := range []struct {
		name    string
		rebuild bool
		want    *time.Time
	}{
		{"rebuild asked", true, &month},
		{"no rebuild", false, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("ENV", "")
			t.Setenv("NODE_ENV", "")
			h := newDBHarness(t)
			if _, err := h.users.GetOrCreate(h.ctx, dbUser); err != nil {
				t.Fatal(err)
			}
			fired := make(chan struct{}, 1)
			h.connSvc.SetPostCreateRebuildHook(func(context.Context, string, string, string) { fired <- struct{}{} })

			if err := h.connSvc.Create(h.ctx, &CreateConnectionRequest{
				UserUID: dbUser, Exchange: "mock", Label: "main", APIKey: dbKey, APISecret: dbSecret,
				RebuildHistory: tc.rebuild, RebuildFrom: month,
			}); err != nil {
				t.Fatalf("create: %v", err)
			}
			if tc.rebuild {
				select {
				case <-fired:
				case <-time.After(5 * time.Second):
					t.Fatal("rebuild hook never fired")
				}
			}

			conn, err := h.conns.GetByUserExchangeLabel(h.ctx, dbUser, "mock", "main")
			if err != nil {
				t.Fatal(err)
			}
			switch {
			case tc.want == nil && conn.RebuildFrom != nil:
				t.Errorf("start month stored without a rebuild: %v", *conn.RebuildFrom)
			case tc.want != nil && (conn.RebuildFrom == nil || !conn.RebuildFrom.Equal(*tc.want)):
				t.Errorf("stored start month = %v, want %v", conn.RebuildFrom, *tc.want)
			}
		})
	}
}

// The connect hook and the operator reconstruct both ask the rebuilder for the
// stored month; an operator's own start wins over it.
func TestDBRebuildsAskForTheStoredMonth(t *testing.T) {
	h := newDBHarness(t)
	h.seedConnection(t, "binance", "main", dbKey, dbSecret)
	conn, err := h.conns.GetByUserExchangeLabel(h.ctx, dbUser, "binance", "main")
	if err != nil {
		t.Fatal(err)
	}
	if err := h.conns.SetRebuildFrom(h.ctx, conn.ID, utcDay(2025, 3, 1)); err != nil {
		t.Fatal(err)
	}

	var since atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req rebuilderclient.RebuildRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		since.Store(req.Since)
		_, _ = w.Write([]byte(`{"exchange":"binance","count":0,"snapshots":[]}`))
	}))
	t.Cleanup(srv.Close)
	h.sync.SetRebuilderClient(rebuilderclient.New(srv.URL, rebuilderToken, zap.NewNop()))

	for _, tc := range []struct {
		name    string
		rebuild func()
		want    string
	}{
		{"connect hook", func() { h.sync.ReconstructHistoryOnConnect(h.ctx, dbUser, "binance", "main") }, "2025-03-01"},
		{"operator reconstruct", func() { h.sync.ReconstructHistoryAdmin(h.ctx, dbUser, "binance", "main", time.Time{}) }, "2025-03-01"},
		{"operator start wins", func() { h.sync.ReconstructHistoryAdmin(h.ctx, dbUser, "binance", "main", utcDay(2026, 6, 1)) }, "2026-06-01"},
	} {
		since.Store("unset")
		tc.rebuild()
		if got, _ := since.Load().(string); got != tc.want {
			t.Errorf("%s: since = %q, want %q", tc.name, got, tc.want)
		}
	}
}
