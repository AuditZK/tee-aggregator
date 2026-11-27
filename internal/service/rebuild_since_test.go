package service

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/trackrecord/enclave/internal/rebuilderclient"
	"github.com/trackrecord/enclave/internal/repository"
	"go.uber.org/zap"
)

// An operator's start date reaches the rebuilder as a day; without one the
// request carries none and the rebuilder keeps its own window.
func TestReconstructHistory_SendsTheRequestedStart(t *testing.T) {
	for _, tc := range []struct {
		name  string
		since time.Time
		want  string
	}{
		{"asked", time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC), "2026-06-01"},
		{"default", time.Time{}, ""},
	} {
		var got map[string]any
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(body, &got)
			_, _ = w.Write([]byte(`{"exchange":"binance","count":0,"snapshots":[]}`))
		}))
		svc := &SyncService{
			logger:    zap.NewNop(),
			rebuilder: rebuilderclient.New(srv.URL, "internal-token-0123456789", zap.NewNop()),
		}
		connMeta := &repository.ExchangeConnection{UserUID: "u1", Exchange: "binance", Label: "main"}
		svc.reconstructHistory(context.Background(), connMeta, &fakeHistConnector{exchange: "binance"},
			&Credentials{APIKey: "k", APISecret: "s"}, reconstructOpts{since: tc.since})
		srv.Close()

		v, present := got["since"]
		if tc.want == "" && present {
			t.Errorf("%s: since sent as %v, want absent", tc.name, v)
		}
		if tc.want != "" && v != tc.want {
			t.Errorf("%s: since = %v, want %s", tc.name, v, tc.want)
		}
	}
}

// Every rebuild of a connection asks for the month stored at connect, unless
// the caller names its own start.
func TestRebuildStart_FallsBackToTheStoredMonth(t *testing.T) {
	stored := time.Date(2025, 3, 1, 0, 0, 0, 0, time.UTC)
	asked := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name   string
		asked  time.Time
		stored *time.Time
		want   string
	}{
		{"stored month", time.Time{}, &stored, "2025-03-01"},
		{"explicit start wins", asked, &stored, "2026-06-01"},
		{"explicit start alone", asked, nil, "2026-06-01"},
		{"neither", time.Time{}, nil, ""},
	} {
		conn := &repository.ExchangeConnection{UserUID: "u1", Exchange: "binance", Label: "main", RebuildFrom: tc.stored}
		if got := rebuildSince(rebuildStart(tc.asked, conn)); got != tc.want {
			t.Errorf("%s: since = %q, want %q", tc.name, got, tc.want)
		}
	}
}
