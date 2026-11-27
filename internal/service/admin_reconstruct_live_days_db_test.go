package service

import (
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/trackrecord/enclave/internal/rebuilderclient"
	"github.com/trackrecord/enclave/internal/repository"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// rebuiltDay is one day of a fake rebuilder answer.
type rebuiltDay struct {
	Date        time.Time `json:"date"`
	TotalEquity float64   `json:"totalEquity"`
}

// serveRebuild answers every rebuild request with days, covering from
// coveredFrom.
func serveRebuild(t *testing.T, days []rebuiltDay, coveredFrom time.Time) *httptest.Server {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"exchange": "binance", "count": len(days), "snapshots": days, "coveredFrom": coveredFrom,
	})
	if err != nil {
		t.Fatalf("encode rebuild: %v", err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// eachDay calls f for every day of [from, to].
func eachDay(from, to time.Time, f func(time.Time)) {
	for d := from; !d.After(to); d = d.AddDate(0, 0, 1) {
		f(d)
	}
}

func binanceRow(ts time.Time, equity float64, rebuilt bool) *repository.Snapshot {
	return &repository.Snapshot{
		UserUID: dbUser, Exchange: "binance", Label: "main", Timestamp: ts,
		TotalEquity: equity, RealizedBalance: equity, FromExternalRebuilder: rebuilt,
		Breakdown: &repository.MarketBreakdown{Global: &repository.MarketMetrics{Equity: equity}},
	}
}

func (h *dbHarness) binanceRows(t *testing.T) map[time.Time]*repository.Snapshot {
	t.Helper()
	rows, err := h.snaps.GetByUserAndDateRange(h.ctx, dbUser, utcDay(2026, 1, 1), time.Now().UTC().Add(24*time.Hour))
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	out := map[time.Time]*repository.Snapshot{}
	for _, r := range rows {
		if r.Exchange == "binance" && r.Label == "main" {
			out[r.Timestamp.UTC()] = r
		}
	}
	return out
}

// The case behind this guard: a Binance Portfolio Margin connection holding
// rebuilt days through Oct 23 and live days since Oct 24, re-reconstructed by
// an operator. The rebuild covers Aug 29 to Nov 26; from Nov 20 the live sync
// measures on another equity basis, 0.15% under the rebuild. Before, the
// witness refused the whole write over Nov 20, and when the bases agreed the
// upsert flipped every live day it covered to rebuilt, which the report reads
// as the end of the reconstructed period: the verified record lost a month.
// The admin path must write the days before the first live one and leave every
// live day as the live sync measured it.
func TestDBAdminReconstructKeepsEveryLiveDay(t *testing.T) {
	const (
		storedRebuilt = 1000.0
		freshRebuild  = 1100.0
		live          = 2000.0
	)
	firstLive := utcDay(2026, 10, 24)
	lastLive := utcDay(2026, 11, 25)
	basisShift := utcDay(2026, 11, 20)

	for _, tc := range []struct {
		name    string
		gap     float64 // relative gap of the rebuild over the live days from Nov 20
		refused bool
	}{
		{"live days on another basis from Nov 20", 0.0015, false},
		{"live days reproduced", 0, false},
		// The kept days meet no measured day, so the live days the rebuild is
		// cut at are its only witness: a rebuild of another wallet set, a
		// fifth off, is refused whole rather than written before them.
		{"rebuild of another account", 0.20, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newDBHarness(t)
			core, logs := observer.New(zapcore.InfoLevel)
			h.sync.logger = zap.New(core)
			h.seedConnection(t, "binance", "main", dbKey, dbSecret)

			var seed []*repository.Snapshot
			eachDay(utcDay(2026, 8, 1), firstLive.AddDate(0, 0, -1), func(d time.Time) {
				seed = append(seed, binanceRow(d, storedRebuilt, true))
			})
			eachDay(firstLive, lastLive, func(d time.Time) {
				seed = append(seed, binanceRow(d, live, false))
			})
			if err := h.snaps.UpsertBatch(h.ctx, seed); err != nil {
				t.Fatalf("seed: %v", err)
			}

			var days []rebuiltDay
			eachDay(utcDay(2026, 8, 29), utcDay(2026, 11, 26), func(d time.Time) {
				eq := freshRebuild
				if !d.Before(firstLive) {
					eq = live
					if !d.Before(basisShift) {
						eq = live * (1 + tc.gap)
					}
				}
				days = append(days, rebuiltDay{Date: d, TotalEquity: eq})
			})
			srv := serveRebuild(t, days, utcDay(2026, 8, 29))
			h.sync.SetRebuilderClient(rebuilderclient.New(srv.URL, "internal-token-0123456789", zap.NewNop()))

			h.sync.ReconstructHistoryAdmin(h.ctx, dbUser, "binance", "main", time.Time{})

			got := h.binanceRows(t)
			eachDay(utcDay(2026, 8, 1), utcDay(2026, 8, 28), func(d time.Time) {
				if r := got[d]; r == nil || r.TotalEquity != storedRebuilt {
					t.Fatalf("%s, older than the rebuild's horizon, was changed: %+v", d.Format(time.DateOnly), r)
				}
			})
			wantBefore := freshRebuild
			if tc.refused {
				wantBefore = storedRebuilt
			}
			eachDay(utcDay(2026, 8, 29), firstLive.AddDate(0, 0, -1), func(d time.Time) {
				r := got[d]
				if r == nil || r.TotalEquity != wantBefore || !r.FromExternalRebuilder {
					t.Fatalf("%s before the first live day: %+v, want equity %v from the rebuilder", d.Format(time.DateOnly), r, wantBefore)
				}
			})
			eachDay(firstLive, lastLive, func(d time.Time) {
				r := got[d]
				if r == nil {
					t.Fatalf("live day %s deleted", d.Format(time.DateOnly))
				}
				if r.TotalEquity != live || r.FromExternalRebuilder || r.IsHistorical {
					t.Fatalf("live day %s rewritten: equity %v, from_external_rebuilder %v, is_historical %v",
						d.Format(time.DateOnly), r.TotalEquity, r.FromExternalRebuilder, r.IsHistorical)
				}
			})
			refusals := logs.FilterMessageSnippet("rejected").All()
			if tc.refused {
				if len(refusals) != 1 || refusals[0].Level != zapcore.ErrorLevel {
					t.Fatalf("%d refusal lines, want one at Error", len(refusals))
				}
				return
			}
			if len(refusals) != 0 {
				t.Fatalf("%d refusal lines, want none", len(refusals))
			}

			lines := logs.FilterMessage("history reconstruction stopped at the first live day").All()
			if len(lines) != 1 {
				t.Fatalf("%d lines reporting the live-day cut, want 1", len(lines))
			}
			f := lines[0].ContextMap()
			if f["first_live_day"] != "2026-10-24" || f["days_kept"] != int64(56) {
				t.Fatalf("cut line: %v", f)
			}
			if g, _ := f["max_relative_gap"].(float64); math.Abs(g-tc.gap) > 1e-9 {
				t.Fatalf("max_relative_gap = %v, want %v", f["max_relative_gap"], tc.gap)
			}
			for _, money := range []string{"total_equity", "rebuilt_equity", "measured_equity"} {
				if _, ok := f[money]; ok {
					t.Fatalf("cut line carries a balance (%s): %v", money, f)
				}
			}
		})
	}
}

// The dry run of a window shows what the real run would write: the same cut,
// and no live day in the answer.
func TestDBAdminReconstructDryRunStopsAtTheFirstLiveDay(t *testing.T) {
	h := newDBHarness(t)
	h.seedConnection(t, "binance", "main", dbKey, dbSecret)
	firstLive := utcDay(2026, 10, 24)
	var seed []*repository.Snapshot
	eachDay(firstLive, utcDay(2026, 11, 25), func(d time.Time) {
		seed = append(seed, binanceRow(d, 2000, false))
	})
	if err := h.snaps.UpsertBatch(h.ctx, seed); err != nil {
		t.Fatalf("seed: %v", err)
	}
	var days []rebuiltDay
	eachDay(utcDay(2026, 8, 29), utcDay(2026, 11, 26), func(d time.Time) {
		days = append(days, rebuiltDay{Date: d, TotalEquity: 2003}) // 0.15% over every live day
	})
	srv := serveRebuild(t, days, utcDay(2026, 8, 29))
	h.sync.SetRebuilderClient(rebuilderclient.New(srv.URL, "internal-token-0123456789", zap.NewNop()))

	out, cut := h.sync.DryRunReconstructRange(h.ctx, dbUser, "binance", "main", utcDay(2026, 8, 29), utcDay(2026, 11, 26), time.Time{})

	if cut == nil || !cut.FirstLiveDay.Equal(firstLive) || cut.DaysKept != 56 || cut.DaysDroppedOverLive != 33 ||
		math.Abs(cut.MaxRelativeGap-0.0015) > 1e-9 {
		t.Fatalf("dry run cut: %+v", cut)
	}
	if len(out) != 56 {
		t.Fatalf("dry run would write %d days, want the 56 before Oct 24", len(out))
	}
	for _, sn := range out {
		if !sn.Timestamp.Before(firstLive) {
			t.Fatalf("dry run would write %s, a live day", sn.Timestamp.Format(time.DateOnly))
		}
	}
	for d, r := range h.binanceRows(t) {
		if r.TotalEquity != 2000 || r.FromExternalRebuilder {
			t.Fatalf("dry run wrote %s", d.Format(time.DateOnly))
		}
	}
}

// The connect-time backfill is not the admin path: its rebuilt rows still
// upsert over the connect day the first live sync wrote (externalRebuilderExchanges).
func TestDBConnectReconstructStillOverwritesTheConnectDay(t *testing.T) {
	h := newDBHarness(t)
	h.seedConnection(t, "binance", "main", dbKey, dbSecret)
	connectDay := utcDay(2026, 11, 25)
	if err := h.snaps.UpsertBatch(h.ctx, []*repository.Snapshot{binanceRow(connectDay, 2000, false)}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	var days []rebuiltDay
	eachDay(utcDay(2026, 10, 27), connectDay, func(d time.Time) {
		days = append(days, rebuiltDay{Date: d, TotalEquity: 2000.5})
	})
	srv := serveRebuild(t, days, utcDay(2026, 10, 27))
	h.sync.SetRebuilderClient(rebuilderclient.New(srv.URL, "internal-token-0123456789", zap.NewNop()))

	h.sync.ReconstructHistoryOnConnect(h.ctx, dbUser, "binance", "main")

	got := h.binanceRows(t)
	if r := got[connectDay]; r == nil || !r.FromExternalRebuilder || r.TotalEquity != 2000.5 {
		t.Fatalf("connect day not overwritten by the rebuild: %+v", r)
	}
	if len(got) != len(days) {
		t.Fatalf("%d rows after the connect backfill, want %d", len(got), len(days))
	}
}

// A dry run cut whole still says why it has no day to show.
func TestDBAdminReconstructDryRunCutWholeReportsTheCut(t *testing.T) {
	h := newDBHarness(t)
	h.seedConnection(t, "binance", "main", dbKey, dbSecret)
	firstLive := utcDay(2026, 10, 24)
	var seed []*repository.Snapshot
	eachDay(firstLive, utcDay(2026, 11, 25), func(d time.Time) {
		seed = append(seed, binanceRow(d, 2000, false))
	})
	if err := h.snaps.UpsertBatch(h.ctx, seed); err != nil {
		t.Fatalf("seed: %v", err)
	}
	var days []rebuiltDay
	eachDay(utcDay(2026, 11, 1), utcDay(2026, 11, 20), func(d time.Time) {
		days = append(days, rebuiltDay{Date: d, TotalEquity: 2000})
	})
	srv := serveRebuild(t, days, utcDay(2026, 11, 1))
	h.sync.SetRebuilderClient(rebuilderclient.New(srv.URL, "internal-token-0123456789", zap.NewNop()))

	out, cut := h.sync.DryRunReconstructRange(h.ctx, dbUser, "binance", "main", utcDay(2026, 11, 1), utcDay(2026, 11, 20), time.Time{})

	if len(out) != 0 {
		t.Fatalf("dry run would write %d live days", len(out))
	}
	if cut == nil || !cut.FirstLiveDay.Equal(firstLive) || cut.DaysKept != 0 || cut.DaysDropped != 20 {
		t.Fatalf("dry run cut whole: %+v, want first live day %s, 0 kept, 20 dropped", cut, firstLive.Format(time.DateOnly))
	}
}
