package service

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/trackrecord/enclave/internal/rebuilderclient"
	"github.com/trackrecord/enclave/internal/repository"
)

// fmRow is everything a snapshot_data row holds, split the way the
// free-margin rewrite must treat it.
type fmRow struct {
	columns   string // every column but the breakdown and updatedAt, as JSON
	breakdown map[string]map[string]json.RawMessage
	updatedAt time.Time
}

func (h *dbHarness) storedRows(t *testing.T) map[time.Time]fmRow {
	t.Helper()
	rows, err := h.pool.Query(h.ctx, `SELECT timestamp, (to_jsonb(s) - 'breakdown_by_market' - 'updatedAt')::text, breakdown_by_market::text, "updatedAt" FROM snapshot_data s`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[time.Time]fmRow{}
	for rows.Next() {
		var ts time.Time
		var r fmRow
		var raw string
		if err := rows.Scan(&ts, &r.columns, &raw, &r.updatedAt); err != nil {
			t.Fatal(err)
		}
		top := map[string]json.RawMessage{}
		if err := json.Unmarshal([]byte(raw), &top); err != nil {
			t.Fatal(err)
		}
		r.breakdown = map[string]map[string]json.RawMessage{}
		for k, v := range top {
			e := map[string]json.RawMessage{}
			if err := json.Unmarshal(v, &e); err != nil {
				t.Fatal(err)
			}
			r.breakdown[k] = e
		}
		out[ts.UTC()] = r
	}
	return out
}

func freeOf(t *testing.T, r fmRow, key string) float64 {
	t.Helper()
	var v float64
	if raw := r.breakdown[key]["available_margin"]; len(raw) > 0 {
		if err := json.Unmarshal(raw, &v); err != nil {
			t.Fatal(err)
		}
	}
	return v
}

// sameButFreeMargin fails unless b differs from a at most in the
// available_margin of its breakdown entries and its updatedAt.
func sameButFreeMargin(t *testing.T, day time.Time, a, b fmRow) {
	t.Helper()
	if a.columns != b.columns {
		t.Fatalf("%s: a column changed:\nbefore %s\nafter  %s", day.Format(time.DateOnly), a.columns, b.columns)
	}
	if len(a.breakdown) != len(b.breakdown) {
		t.Fatalf("%s: breakdown entries %d -> %d", day.Format(time.DateOnly), len(a.breakdown), len(b.breakdown))
	}
	for key, before := range a.breakdown {
		after := b.breakdown[key]
		for field, v := range before {
			if field != "available_margin" && string(after[field]) != string(v) {
				t.Fatalf("%s: %s.%s changed %s -> %s", day.Format(time.DateOnly), key, field, v, after[field])
			}
		}
		for field := range after {
			if _, had := before[field]; !had && field != "available_margin" {
				t.Fatalf("%s: %s.%s added", day.Format(time.DateOnly), key, field)
			}
		}
	}
}

func fmIdentical(t *testing.T, day time.Time, a, b fmRow) {
	t.Helper()
	sameButFreeMargin(t, day, a, b)
	for key := range a.breakdown {
		if string(a.breakdown[key]["available_margin"]) != string(b.breakdown[key]["available_margin"]) {
			t.Fatalf("%s: %s free margin changed", day.Format(time.DateOnly), key)
		}
	}
	if !a.updatedAt.Equal(b.updatedAt) {
		t.Fatalf("%s: row rewritten (updatedAt moved)", day.Format(time.DateOnly))
	}
}

func fmProRow(day time.Time, rebuilt bool) *repository.Snapshot {
	r := classicProRow(180000, 8000, 0, rebuilt)
	r.ID = ""
	r.UserUID, r.Exchange, r.Label, r.Timestamp = dbUser, "binance", "main", day
	r.RealizedBalance, r.Deposits, r.Withdrawals = 185000, 125, 40
	return r
}

// serveProRebuild answers like the rebuilder's Portfolio Margin Pro walk:
// swap is the pool with free = pool − held, marginHeld on every row.
func serveProRebuild(t *testing.T, days []time.Time, held float64, coveredFrom time.Time, replayed bool) *httptest.Server {
	t.Helper()
	var snaps []map[string]any
	for _, d := range days {
		snaps = append(snaps, map[string]any{
			"date": d, "totalEquity": 195000.0, "realizedBalance": 190000.0, "deposits": 9999.0, "totalTrades": 99,
			"marginHeld": held,
			"breakdown": map[string]any{
				"swap": map[string]any{"marketType": "swap", "equity": 192000.0, "availableMargin": 192000.0 - held},
				"spot": map[string]any{"marketType": "spot", "equity": 3000.0, "availableMargin": 3000.0},
			},
		})
	}
	body, err := json.Marshal(map[string]any{
		"exchange": "binance", "count": len(snaps), "snapshots": snaps, "coveredFrom": coveredFrom,
		"marginHeldReplayed": replayed, "warnings": []string{"binance_pm.leverage_assumed_constant"},
	})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(body) }))
	t.Cleanup(srv.Close)
	return srv
}

// End to end on the production schema: a dry run writes nothing, apply
// writes the dry run's free margins into the rebuilt rows only, and every
// other field of every row, live or rebuilt, stays byte-identical.
func TestDBRewriteRebuiltFreeMargin(t *testing.T) {
	h := newDBHarness(t)
	h.seedConnection(t, "binance", "main", dbKey, dbSecret)

	d0 := utcDay(2026, 9, 1) // rebuilt, before the rebuild's reach
	d1 := utcDay(2026, 9, 2) // rebuilt
	d3 := utcDay(2026, 9, 4) // rebuilt
	d4 := utcDay(2026, 9, 5) // live
	d5 := utcDay(2026, 9, 6) // live
	var seed []*repository.Snapshot
	eachDay(d0, d3, func(d time.Time) { seed = append(seed, fmProRow(d, true)) })
	eachDay(d4, d5, func(d time.Time) { seed = append(seed, fmProRow(d, false)) })
	if err := h.snaps.UpsertBatch(h.ctx, seed); err != nil {
		t.Fatalf("seed: %v", err)
	}
	var served []time.Time
	eachDay(d1, d5, func(d time.Time) { served = append(served, d) })
	const held = 30000.0
	srv := serveProRebuild(t, served, held, d1, true)
	h.sync.SetRebuilderClient(rebuilderclient.New(srv.URL, "internal-token-0123456789", zap.NewNop()))

	if _, err := h.sync.RewriteRebuiltFreeMargin(h.ctx, dbUser, "binance", "main", d0, d5, true); !errors.Is(err, ErrFreeMarginNoDryRun) {
		t.Fatalf("apply without a dry run: %v, want ErrFreeMarginNoDryRun", err)
	}

	before := h.storedRows(t)
	if len(before) != 6 {
		t.Fatalf("seeded %d rows", len(before))
	}
	dry, err := h.sync.RewriteRebuiltFreeMargin(h.ctx, dbUser, "binance", "main", d0, d5, false)
	if err != nil {
		t.Fatalf("dry run: %v", err)
	}
	if dry.Rewritable != 3 || len(dry.Days) != 6 {
		t.Fatalf("dry run: %d rewritable of %d days, want 3 of 6: %+v", dry.Rewritable, len(dry.Days), dry.Days)
	}
	afterDry := h.storedRows(t)
	for day, b := range before {
		fmIdentical(t, day, b, afterDry[day])
	}

	applied, err := h.sync.RewriteRebuiltFreeMargin(h.ctx, dbUser, "binance", "main", d0, d5, true)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if applied.Written != 3 {
		t.Fatalf("written %d, want 3", applied.Written)
	}
	after := h.storedRows(t)
	// held 30000: the wallet's 8000 to zero, 22000 off the pool in spot.
	wantSpot := 180000.0 - (held - 8000)
	for day, b := range before {
		a := after[day]
		if day.Before(d1) || !day.Before(d4) {
			fmIdentical(t, day, b, a) // out of reach, or live: untouched
			continue
		}
		sameButFreeMargin(t, day, b, a)
		if got := freeOf(t, a, "spot"); !fmNear(got, wantSpot) {
			t.Fatalf("%s spot free %.2f, want %.2f", day.Format(time.DateOnly), got, wantSpot)
		}
		if got := freeOf(t, a, "swap"); got != 0 {
			t.Fatalf("%s swap free %.2f, want 0", day.Format(time.DateOnly), got)
		}
		if got := freeOf(t, a, "global"); !fmNear(got, wantSpot) {
			t.Fatalf("%s global free %.2f, want %.2f", day.Format(time.DateOnly), got, wantSpot)
		}
	}
	for _, d := range applied.Days {
		if d.Day.Equal(d0) && d.Kept == "" {
			t.Fatal("the day before the rebuild's reach was not kept")
		}
		if !d.Day.Before(d4) && d.Kept != "live-measured day" {
			t.Fatalf("live day %s: kept %q", d.Day.Format(time.DateOnly), d.Kept)
		}
	}

	// The repository refuses a live row on its own, whatever the caller asks.
	var liveID string
	if err := h.pool.QueryRow(h.ctx, `SELECT id FROM snapshot_data WHERE timestamp = $1`, d4).Scan(&liveID); err != nil {
		t.Fatal(err)
	}
	ok, err := h.snaps.UpdateRebuiltAvailableMargin(h.ctx, liveID,
		map[string]repository.MarketMargin{"spot": {Equity: 180000, AvailableMargin: 180000}}, map[string]float64{"spot": 1})
	if err != nil || ok {
		t.Fatalf("live row written by the repository: ok=%v err=%v", ok, err)
	}
	fmIdentical(t, d4, before[d4], h.storedRows(t)[d4])

	// The plan is spent: a second apply needs a new dry run.
	if _, err := h.sync.RewriteRebuiltFreeMargin(h.ctx, dbUser, "binance", "main", d0, d5, true); !errors.Is(err, ErrFreeMarginNoDryRun) {
		t.Fatalf("second apply: %v, want ErrFreeMarginNoDryRun", err)
	}
	// A new dry run of the rewritten days plans nothing: the rewrite is
	// idempotent.
	again, err := h.sync.RewriteRebuiltFreeMargin(h.ctx, dbUser, "binance", "main", d0, d5, false)
	if err != nil || again.Rewritable != 0 {
		t.Fatalf("dry run after apply: %v, %d rewritable, want 0", err, again.Rewritable)
	}
}

// A rebuild that does not state the margin held was replayed (a classic
// rebuild, or an older rebuilder) is refused: its zero is not a measure.
func TestDBRewriteRebuiltFreeMarginRefusesUnreplayedRebuild(t *testing.T) {
	h := newDBHarness(t)
	h.seedConnection(t, "binance", "main", dbKey, dbSecret)
	d1 := utcDay(2026, 9, 2)
	if err := h.snaps.UpsertBatch(h.ctx, []*repository.Snapshot{fmProRow(d1, true)}); err != nil {
		t.Fatal(err)
	}
	srv := serveProRebuild(t, []time.Time{d1}, 0, d1, false)
	h.sync.SetRebuilderClient(rebuilderclient.New(srv.URL, "internal-token-0123456789", zap.NewNop()))
	before := h.storedRows(t)
	if _, err := h.sync.RewriteRebuiltFreeMargin(h.ctx, dbUser, "binance", "main", d1, d1, false); err == nil {
		t.Fatal("a rebuild without marginHeldReplayed was accepted")
	}
	if _, err := h.sync.RewriteRebuiltFreeMargin(h.ctx, dbUser, "binance", "main", d1, d1, true); !errors.Is(err, ErrFreeMarginNoDryRun) {
		t.Fatalf("apply after a refused dry run: %v", err)
	}
	fmIdentical(t, d1, before[d1], h.storedRows(t)[d1])
}

// A row rewritten between the dry run and the apply (here: a recalibration
// moved its free margin) is left as it now is.
func TestDBRewriteRebuiltFreeMarginSkipsAMovedRow(t *testing.T) {
	h := newDBHarness(t)
	h.seedConnection(t, "binance", "main", dbKey, dbSecret)
	d1 := utcDay(2026, 9, 2)
	if err := h.snaps.UpsertBatch(h.ctx, []*repository.Snapshot{fmProRow(d1, true)}); err != nil {
		t.Fatal(err)
	}
	srv := serveProRebuild(t, []time.Time{d1}, 30000, d1, true)
	h.sync.SetRebuilderClient(rebuilderclient.New(srv.URL, "internal-token-0123456789", zap.NewNop()))
	if _, err := h.sync.RewriteRebuiltFreeMargin(h.ctx, dbUser, "binance", "main", d1, d1, false); err != nil {
		t.Fatal(err)
	}
	moved := fmProRow(d1, true)
	moved.Breakdown.Spot.AvailableMargin = 200000
	if err := h.snaps.UpsertBatch(h.ctx, []*repository.Snapshot{moved}); err != nil {
		t.Fatal(err)
	}
	before := h.storedRows(t)
	out, err := h.sync.RewriteRebuiltFreeMargin(h.ctx, dbUser, "binance", "main", d1, d1, true)
	if err != nil {
		t.Fatal(err)
	}
	if out.Written != 0 || out.Days[0].Kept != "row changed since the dry run" {
		t.Fatalf("moved row: %+v", out.Days)
	}
	fmIdentical(t, d1, before[d1], h.storedRows(t)[d1])
}
