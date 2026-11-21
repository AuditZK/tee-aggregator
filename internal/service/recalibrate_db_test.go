package service

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/trackrecord/enclave/internal/rebuilderclient"
	"github.com/trackrecord/enclave/internal/repository"
)

const rebuilderToken = "synthetic-internal-token"

type fakeRebuilder struct {
	fail     bool
	calls    atomic.Int32
	override atomic.Value
}

func (f *fakeRebuilder) serve(t *testing.T) *httptest.Server {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.calls.Add(1)
		if r.Header.Get("X-Internal-Token") != rebuilderToken {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		var req rebuilderclient.RebuildRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		f.override.Store(req.EndEquityOverride)
		if f.fail {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		today := startOfTodayUTC()
		days := []map[string]any{}
		for i := 3; i >= 1; i-- {
			days = append(days, map[string]any{
				"date":            today.AddDate(0, 0, -i),
				"totalEquity":     1000 - float64(i),
				"realizedBalance": 1000 - float64(i),
			})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"exchange":    req.Exchange,
			"count":       len(days),
			"snapshots":   days,
			"coveredFrom": today.AddDate(0, 0, -3),
		})
	}))
	t.Cleanup(srv.Close)
	return srv
}

// seedRecalibrationCandidate leaves a connection in the state the midnight
// pass looks for: created before today, rebuild consented, today's live
// snapshot written.
func (h *dbHarness) seedRecalibrationCandidate(t *testing.T, consented bool) *repository.ExchangeConnection {
	t.Helper()
	return h.seedRecalibrationCandidateCreated(t, consented, startOfTodayUTC().Add(-12*time.Hour))
}

func (h *dbHarness) seedRecalibrationCandidateCreated(t *testing.T, consented bool, createdAt time.Time) *repository.ExchangeConnection {
	t.Helper()
	h.seedConnection(t, "binance", "main", dbKey, dbSecret)
	conn, err := h.conns.GetByUserExchangeLabel(h.ctx, dbUser, "binance", "main")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.pool.Exec(h.ctx, `UPDATE exchange_connections SET "createdAt" = $1`, createdAt); err != nil {
		t.Fatal(err)
	}
	if consented {
		if err := h.conns.MarkRebuildRequested(h.ctx, conn.ID, startOfTodayUTC().Add(-12*time.Hour)); err != nil {
			t.Fatal(err)
		}
	}
	if err := h.snaps.Upsert(h.ctx, &repository.Snapshot{
		UserUID: dbUser, Exchange: "binance", Label: "main", Timestamp: startOfTodayUTC(),
		TotalEquity: 1000, RealizedBalance: 1000,
	}); err != nil {
		t.Fatal(err)
	}
	return conn
}

func (h *dbHarness) finalizedAt(t *testing.T) *time.Time {
	t.Helper()
	var at *time.Time
	if err := h.pool.QueryRow(h.ctx, `SELECT "rebuildFinalizedAt" FROM exchange_connections`).Scan(&at); err != nil {
		t.Fatal(err)
	}
	return at
}

func TestDBRecalibrationRewritesHistoryAndFinalizes(t *testing.T) {
	h := newDBHarness(t)
	h.seedRecalibrationCandidate(t, true)
	fake := &fakeRebuilder{}
	h.sync.SetRebuilderClient(rebuilderclient.New(fake.serve(t).URL, rebuilderToken, zap.NewNop()))

	h.sync.RecalibrateRebuiltHistories(h.ctx)

	if got, _ := fake.override.Load().(float64); got != 1000 {
		t.Errorf("endEquityOverride = %v, want today's live equity 1000", got)
	}
	if h.finalizedAt(t) == nil {
		t.Error("connection not stamped finalized after a successful recalibration")
	}
	// Read the way the report signer and contradictedDay do: only this range
	// query surfaces from_external_rebuilder on the production schema.
	rows, err := h.snaps.GetByUserAndDateRange(h.ctx, dbUser, startOfTodayUTC().AddDate(0, 0, -7), startOfTodayUTC())
	if err != nil {
		t.Fatal(err)
	}
	byDay := map[time.Time]*repository.Snapshot{}
	for _, s := range rows {
		byDay[s.Timestamp.UTC()] = s
	}
	if len(byDay) != 4 {
		t.Fatalf("stored days = %d, want 3 recalibrated + today", len(byDay))
	}
	if d := byDay[startOfTodayUTC().AddDate(0, 0, -1)]; d == nil || d.TotalEquity != 999 || !d.FromExternalRebuilder {
		t.Errorf("recalibrated day = %+v, want equity 999 marked external", d)
	}
	if d := byDay[startOfTodayUTC()]; d == nil || d.TotalEquity != 1000 || d.FromExternalRebuilder {
		t.Errorf("today's live row = %+v, want untouched", d)
	}
}

type countingMetrics struct{ counts map[string]int }

func (c *countingMetrics) IncrCounter(name string, _ ...string) { c.counts[name]++ }

func TestDBRecalibrationFailureLeavesConnectionForRetry(t *testing.T) {
	cases := []struct {
		name      string
		createdAt time.Time
		level     zapcore.Level
	}{
		{"first night stays a warning", startOfTodayUTC().Add(-12 * time.Hour), zapcore.WarnLevel},
		{"third night is raised", startOfTodayUTC().Add(-60 * time.Hour), zapcore.ErrorLevel},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			core, logs := observer.New(zapcore.DebugLevel)
			h := newDBHarness(t)
			h.sync.logger = zap.New(core)
			counters := &countingMetrics{counts: map[string]int{}}
			h.sync.SetMetrics(counters)
			h.seedRecalibrationCandidateCreated(t, true, tc.createdAt)
			fake := &fakeRebuilder{fail: true}
			h.sync.SetRebuilderClient(rebuilderclient.New(fake.serve(t).URL, rebuilderToken, zap.NewNop()))

			h.sync.RecalibrateRebuiltHistories(h.ctx)

			if fake.calls.Load() != 1 {
				t.Fatalf("rebuilder calls = %d, want 1", fake.calls.Load())
			}
			if at := h.finalizedAt(t); at != nil {
				t.Errorf("failed recalibration stamped finalized at %v", at)
			}
			failed := logs.FilterMessage("midnight recalibration: connection failed").All()
			if len(failed) != 1 || failed[0].Level != tc.level {
				t.Fatalf("failure log = %+v, want one at %v", failed, tc.level)
			}
			if got := counters.counts["recalibration_failures_total"]; got != 1 {
				t.Errorf("recalibration_failures_total += %d, want 1", got)
			}
		})
	}
}

func TestDBRecalibrationSkipsConnectionsWithoutConsent(t *testing.T) {
	h := newDBHarness(t)
	h.seedRecalibrationCandidate(t, false)
	fake := &fakeRebuilder{}
	h.sync.SetRebuilderClient(rebuilderclient.New(fake.serve(t).URL, rebuilderToken, zap.NewNop()))

	h.sync.RecalibrateRebuiltHistories(h.ctx)

	if n := fake.calls.Load(); n != 0 {
		t.Errorf("credentials left the enclave %d time(s) for a connection that never consented", n)
	}
}
