package service

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/trackrecord/enclave/internal/connector"
	"github.com/trackrecord/enclave/internal/rebuilderclient"
	"github.com/trackrecord/enclave/internal/repository"
)

// Free-margin rewrite of rebuilt days.
//
// A Binance Portfolio Margin Pro account (fapi still answers its key) was
// rebuilt on the classic path: its cross margin pool sat in the spot bucket at
// available = equity, and the USDⓈ-M positions' margin, which Pro draws from
// that pool, was deducted from the USDⓈ-M wallet alone and floored there at
// zero. The stored free margin of those days is the whole pool. The live
// connector deducts it since tee-aggregator #133; this rewrites the rebuilt
// days only, and only their free margin.
//
// Source of the margin held: the history rebuilder's Portfolio Margin replay
// (portfolio_margin_held.go): today's positions rewound through the fills, per
// symbol |net position| × that day's close ÷ today's leverage. It is the
// convention the live connector applies (binanceUMPositionMargin), so a
// rewritten rebuilt day and a live day hold margin the same way. The rebuilder
// must say that every row's margin held was replayed (marginHeldReplayed):
// without it a zero is "not measured", and the rewrite refuses.
//
// What changes: available_margin of the spot, swap and global entries of rows
// whose origin is the external rebuilder. Never a live-measured row, never an
// equity, flow, activity or origin field. The rebuilt row's equity is not
// taken: it may differ from the stored one (the classic path held the cross
// margin flat where the pool walk marks it), and the curve is not what this
// path rewrites.
//
// Per row: the margin held, capped at the pool the rebuild measured, comes off
// the swap bucket (the USDⓈ-M wallet) down to zero, the rest off the spot
// bucket (which holds the cross margin pool) down to zero. Neither bucket's
// free margin ever exceeds its equity or falls below zero; global is the sum,
// clamped to the global equity like every rebuilt row
// (clampAvailableMargin). When the held margin exceeds what the stored buckets
// hold, both end at zero and the day says so (held_exceeds_stored_pool), as
// the live connector floors free margin at zero.
//
// Two steps, like recount-activity: a dry run reads the rebuild and keeps its
// plan in memory; apply writes that plan, and only rows that still hold what
// the dry run read. Apply never calls the rebuilder (credentials leave the
// enclave once per rewrite).

// freeMarginPlanTTL bounds how long a dry run can be applied.
const freeMarginPlanTTL = 12 * time.Hour

// freeMarginBlockingWarnings are rebuilder warnings under which some margin
// held went uncounted: a symbol without leverage or without closes counts 0.
var freeMarginBlockingWarnings = []string{
	"binance_pm.leverage_unknown",
	"binance_pm.unpriced_contract",
	"binance_pm.cm_contract_size_unknown",
}

// FreeMarginMarket is one breakdown entry's free margin, as stored and as
// rewritten. Equity is the stored one and is never written.
type FreeMarginMarket struct {
	Equity       float64 `json:"equity"`
	OldAvailable float64 `json:"old_available"`
	NewAvailable float64 `json:"new_available"`
}

// FreeMarginDay is one stored row in the window.
type FreeMarginDay struct {
	Day time.Time `json:"day"`
	// Equity is the stored equity, unchanged by the rewrite.
	Equity float64 `json:"equity"`
	// RebuiltEquity and MarginHeld are the rebuild's, for reading the dry
	// run: only MarginHeld is used.
	RebuiltEquity float64                     `json:"rebuilt_equity,omitempty"`
	MarginHeld    float64                     `json:"margin_held"`
	OldAvailable  float64                     `json:"old_available"`
	NewAvailable  float64                     `json:"new_available"`
	Markets       map[string]FreeMarginMarket `json:"markets,omitempty"`
	// HeldExceedsStoredPool: the margin held is more than the stored spot and
	// swap buckets hold; both end at zero.
	HeldExceedsStoredPool bool `json:"held_exceeds_stored_pool,omitempty"`
	// Kept names why the row is left as stored; empty otherwise.
	Kept    string `json:"kept,omitempty"`
	Written bool   `json:"written,omitempty"`

	rowID  string
	expect map[string]repository.MarketMargin
	next   map[string]float64
}

func (d FreeMarginDay) changed() bool { return len(d.next) > 0 }

// FreeMarginRewrite is the answer of a dry run or an apply.
type FreeMarginRewrite struct {
	Apply    bool      `json:"apply"`
	DryRunAt time.Time `json:"dry_run_at"`
	// CoveredFrom is the earliest instant the rebuild could see.
	CoveredFrom time.Time       `json:"covered_from,omitempty"`
	Warnings    []string        `json:"warnings,omitempty"`
	Rewritable  int             `json:"rewritable"`
	Written     int             `json:"written"`
	Days        []FreeMarginDay `json:"days"`
}

type freeMarginPlan struct {
	at   time.Time
	resp FreeMarginRewrite
}

type freeMarginPlanStore struct {
	mu    sync.Mutex
	plans map[string]freeMarginPlan
}

func freeMarginPlanKey(userUID, exchange, label string, from, to time.Time) string {
	return strings.Join([]string{userUID, strings.ToLower(exchange), label, from.Format(time.DateOnly), to.Format(time.DateOnly)}, "|")
}

func (p *freeMarginPlanStore) put(key string, plan freeMarginPlan) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.plans == nil {
		p.plans = map[string]freeMarginPlan{}
	}
	p.plans[key] = plan
}

// take hands out the plan once, if it is still fresh.
func (p *freeMarginPlanStore) take(key string, now time.Time) (freeMarginPlan, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	plan, ok := p.plans[key]
	delete(p.plans, key)
	if !ok || now.Sub(plan.at) > freeMarginPlanTTL {
		return freeMarginPlan{}, false
	}
	return plan, true
}

// ErrFreeMarginNoDryRun is returned by an apply that has no fresh dry run of
// the same connection and window to write.
var ErrFreeMarginNoDryRun = errors.New("free-margin rewrite: no dry run of this connection and window in the last 12 hours, run it first (apply writes the dry run's numbers)")

// RewriteRebuiltFreeMargin rewrites the free margin of a connection's rebuilt
// days between from and to (snapshot days, inclusive) from the margin held
// the history rebuilder replays. With apply false it reads the rebuild and
// returns, per stored row, the free margin as stored and as it would be
// written; with apply true it writes the last dry run of the same window.
func (s *SyncService) RewriteRebuiltFreeMargin(ctx context.Context, userUID, exchange, label string, from, to time.Time, apply bool) (*FreeMarginRewrite, error) {
	from, to = from.UTC().Truncate(24*time.Hour), to.UTC().Truncate(24*time.Hour)
	if err := checkReflowWindow(from, to); err != nil {
		return nil, err
	}
	key := freeMarginPlanKey(userUID, exchange, label, from, to)
	if apply {
		return s.applyFreeMarginPlan(ctx, userUID, exchange, label, key)
	}

	connMeta, err := s.connSvc.GetActiveConnectionByLabel(ctx, userUID, exchange, label)
	if err != nil {
		return nil, fmt.Errorf("look up connection: %w", err)
	}
	rows, err := s.freeMarginRows(ctx, connMeta, from, to)
	if err != nil {
		return nil, err
	}
	res, err := s.rebuildForFreeMargin(ctx, connMeta)
	if err != nil {
		return nil, err
	}
	if !res.MarginHeldReplayed {
		return nil, fmt.Errorf("free-margin rewrite: the rebuilder did not replay the margin held for %s (account not rebuilt as Portfolio Margin, or a rebuilder without marginHeldReplayed)", connMeta.Exchange)
	}
	if blocking := freeMarginBlocking(res.Warnings); len(blocking) > 0 {
		return nil, fmt.Errorf("free-margin rewrite: margin held incomplete in the rebuild (%s)", strings.Join(blocking, "; "))
	}

	rebuilt := make(map[time.Time]*connector.HistoricalSnapshot, len(res.Snapshots))
	for _, h := range res.Snapshots {
		rebuilt[h.Date.UTC().Truncate(24*time.Hour)] = h
	}
	now := time.Now().UTC()
	out := FreeMarginRewrite{DryRunAt: now, CoveredFrom: res.CoveredFrom, Warnings: res.Warnings}
	for _, r := range rows {
		d := freeMarginDay(r, rebuilt[r.Timestamp.UTC()], res.CoveredFrom)
		if d.Kept == "" && d.changed() {
			out.Rewritable++
		}
		out.Days = append(out.Days, d)
	}
	s.freeMarginPlans.put(key, freeMarginPlan{at: now, resp: out})
	s.logger.Info("free-margin rewrite dry run",
		zap.String("user_uid", userUID),
		zap.String("exchange", connMeta.Exchange),
		zap.String("label", connMeta.Label),
		zap.Int("days", len(out.Days)),
		zap.Int("rewritable", out.Rewritable),
		zap.Time("covered_from", res.CoveredFrom),
	)
	return &out, nil
}

func (s *SyncService) applyFreeMarginPlan(ctx context.Context, userUID, exchange, label, key string) (*FreeMarginRewrite, error) {
	plan, ok := s.freeMarginPlans.take(key, time.Now().UTC())
	if !ok {
		return nil, ErrFreeMarginNoDryRun
	}
	out := plan.resp
	out.Apply = true
	out.Days = append([]FreeMarginDay(nil), plan.resp.Days...)
	for i := range out.Days {
		d := &out.Days[i]
		if d.Kept != "" || !d.changed() {
			continue
		}
		ok, err := s.snapshotRepo.UpdateRebuiltAvailableMargin(ctx, d.rowID, d.expect, d.next)
		if err != nil {
			return &out, err
		}
		if !ok {
			d.Kept = "row changed since the dry run"
			continue
		}
		d.Written = true
		out.Written++
	}
	s.logger.Info("free-margin rewrite applied",
		zap.String("user_uid", userUID),
		zap.String("exchange", exchange),
		zap.String("label", label),
		zap.Int("days", len(out.Days)),
		zap.Int("written", out.Written),
	)
	if out.Written > 0 {
		s.notifyHistoryRebuilt(ctx, userUID)
	}
	return &out, nil
}

// freeMarginRows returns the connection's stored rows in [from, to], oldest
// first.
func (s *SyncService) freeMarginRows(ctx context.Context, connMeta *repository.ExchangeConnection, from, to time.Time) ([]*repository.Snapshot, error) {
	all, err := s.snapshotRepo.GetByUserAndDateRange(ctx, connMeta.UserUID, from, to)
	if err != nil {
		return nil, fmt.Errorf("read snapshots: %w", err)
	}
	var rows []*repository.Snapshot
	for _, r := range all {
		day := r.Timestamp.UTC()
		if r.Exchange == connMeta.Exchange && r.Label == connMeta.Label && !day.Before(from) && !day.After(to) {
			rows = append(rows, r)
		}
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Timestamp.Before(rows[j].Timestamp) })
	return rows, nil
}

// rebuildForFreeMargin asks the external rebuilder for the connection's
// history, on the credential path of every rebuild (SEC-ZK-001), anchored on
// the latest stored equity like the admin reconstruct. Nothing is persisted.
func (s *SyncService) rebuildForFreeMargin(ctx context.Context, connMeta *repository.ExchangeConnection) (*rebuilderclient.RebuildResult, error) {
	if s.rebuilder == nil || !s.rebuilder.Configured() {
		return nil, errors.New("free-margin rewrite: rebuilder not configured")
	}
	if !externalRebuilderSupports(connMeta.Exchange) {
		return nil, fmt.Errorf("free-margin rewrite: %s is not rebuilt by the external rebuilder", connMeta.Exchange)
	}
	var endEquity float64
	if latest, err := s.snapshotRepo.GetLatestByUserExchangeLabel(ctx, connMeta.UserUID, connMeta.Exchange, connMeta.Label); err == nil && latest != nil {
		endEquity = latest.TotalEquity
	}
	creds, err := s.connSvc.GetDecryptedCredentialsByLabel(ctx, connMeta.UserUID, connMeta.Exchange, connMeta.Label)
	if err != nil {
		return nil, fmt.Errorf("decrypt credentials: %w", err)
	}
	res, err := s.rebuilder.Rebuild(ctx, rebuilderclient.RebuildRequest{
		UserUID:  connMeta.UserUID,
		Exchange: connMeta.Exchange,
		Label:    connMeta.Label,
		Credentials: rebuilderclient.Credentials{
			// LOG-CREDS-001: plaintext; never logged, dropped below.
			WalletAddress: creds.APIKey,
			APIKey:        creds.APIKey,
			APISecret:     creds.APISecret,
			Passphrase:    creds.Passphrase,
		},
		EndEquityOverride: endEquity,
	})
	creds = nil
	if err != nil {
		return nil, fmt.Errorf("free-margin rewrite: rebuilder request: %w", err)
	}
	return res, nil
}

func freeMarginBlocking(warnings []string) []string {
	var out []string
	for _, w := range warnings {
		for _, code := range freeMarginBlockingWarnings {
			if w == code || strings.HasPrefix(w, code+":") {
				out = append(out, w)
			}
		}
	}
	return out
}

// freeMarginDay computes the rewrite of one stored row from the rebuilt row
// of the same day. Pure.
func freeMarginDay(stored *repository.Snapshot, rebuilt *connector.HistoricalSnapshot, coveredFrom time.Time) FreeMarginDay {
	day := stored.Timestamp.UTC()
	d := FreeMarginDay{Day: day, Equity: stored.TotalEquity, rowID: stored.ID}
	b := stored.Breakdown
	if b != nil && b.Global != nil {
		d.OldAvailable, d.NewAvailable = b.Global.AvailableMargin, b.Global.AvailableMargin
	}
	if rebuilt != nil {
		d.RebuiltEquity, d.MarginHeld = rebuilt.TotalEquity, rebuilt.MarginHeld
	}

	switch {
	case !stored.FromExternalRebuilder:
		d.Kept = "live-measured day"
	case b == nil || b.Global == nil:
		d.Kept = "no global entry"
	case b.Spot == nil && b.Swap == nil:
		d.Kept = "no spot or swap entry"
	case b.Global.NativeCurrency != "":
		d.Kept = "row converted from " + b.Global.NativeCurrency
	case !globalEquityAddsUp(b):
		d.Kept = "global equity is not the sum of its markets"
	case rebuilt == nil && !coveredFrom.IsZero() && day.Before(coveredFrom.UTC()):
		d.Kept = "before the rebuild's reach (covered from " + coveredFrom.UTC().Format(time.DateOnly) + ")"
	case rebuilt == nil:
		d.Kept = "day not in the rebuild"
	}
	if d.Kept != "" {
		return d
	}
	pool := rebuilt.Breakdown["swap"]
	switch {
	case pool == nil:
		d.Kept = "rebuilt row has no pool (swap) entry"
	case rebuilt.MarginHeld < 0 || math.IsNaN(rebuilt.MarginHeld):
		d.Kept = "rebuilt margin held is not a measure"
	case !poolFreeFollowsHeld(pool, rebuilt.MarginHeld):
		d.Kept = "rebuilt pool free margin does not follow its margin held"
	}
	if d.Kept != "" {
		return d
	}

	held := math.Min(rebuilt.MarginHeld, math.Max(pool.Equity, 0))
	markets := map[string]FreeMarginMarket{}
	expect := map[string]repository.MarketMargin{}
	next := map[string]float64{}
	note := func(key string, m *repository.MarketMetrics, free float64) {
		markets[key] = FreeMarginMarket{Equity: m.Equity, OldAvailable: m.AvailableMargin, NewAvailable: free}
		if math.Abs(free-m.AvailableMargin) > 1e-6 {
			expect[key] = repository.MarketMargin{Equity: m.Equity, AvailableMargin: m.AvailableMargin}
			next[key] = free
		}
	}

	// The USDⓈ-M wallet first: the positions' margin is drawn there.
	rest := held
	if m := b.Swap; m != nil {
		base := math.Max(m.Equity, 0)
		take := math.Min(rest, base)
		rest -= take
		note("swap", m, base-take)
	}
	// Then the cross margin pool, which the classic path keeps in spot. The
	// base is the bucket's equity, as the classic path set its free margin
	// (spot fully free), never the stored free margin: from that a second
	// run would take the held margin off twice.
	if m := b.Spot; m != nil {
		base := math.Max(m.Equity, 0)
		take := math.Min(rest, base)
		rest -= take
		note("spot", m, base-take)
	}
	d.HeldExceedsStoredPool = rest > 0.01

	sum := 0.0
	for key, m := range marketEntriesByKey(b) {
		if mk, ok := markets[key]; ok {
			sum += mk.NewAvailable
		} else {
			sum += m.AvailableMargin
		}
	}
	global := math.Max(clampAvailableMargin(sum, b.Global.Equity), 0)
	note("global", b.Global, global)
	d.NewAvailable = global
	d.Markets = markets

	if err := checkFreeMarginBuckets(markets); err != nil {
		d.Kept = err.Error()
		return d
	}
	d.expect, d.next = expect, next
	return d
}

// poolFreeFollowsHeld reports whether a rebuilt pool row's free margin is
// pool − held, floored at zero: the shape the rebuilder's pool walk writes.
// A row that does not follow it is not a pool row.
func poolFreeFollowsHeld(pool *connector.MarketBalance, held float64) bool {
	want := math.Max(math.Min(pool.Equity-held, pool.Equity), 0)
	return math.Abs(want-pool.AvailableMargin) <= 0.01+1e-6*math.Abs(pool.Equity)
}

// checkFreeMarginBuckets is the bucket invariant the rewrite holds: on every
// entry, 0 <= free margin <= max(equity, 0). A bucket whose equity is below
// zero has no free margin (the rebuilder's floor); global never exceeds the
// global equity.
func checkFreeMarginBuckets(markets map[string]FreeMarginMarket) error {
	keys := make([]string, 0, len(markets))
	for k := range markets {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		m := markets[k]
		if m.NewAvailable < -1e-9 || m.NewAvailable > math.Max(m.Equity, 0)+1e-6 {
			return fmt.Errorf("bucket invariant: %s free margin %.2f outside [0, equity %.2f]", k, m.NewAvailable, m.Equity)
		}
	}
	return nil
}

// globalEquityAddsUp reports whether the global equity is the sum of the
// markets' equities, within a cent.
func globalEquityAddsUp(b *repository.MarketBreakdown) bool {
	sum := 0.0
	for _, m := range marketEntriesByKey(b) {
		sum += m.Equity
	}
	return math.Abs(sum-b.Global.Equity) <= math.Max(0.01, 1e-9*math.Abs(b.Global.Equity))
}

// marketEntriesByKey lists the market entries of a breakdown, global excluded.
func marketEntriesByKey(b *repository.MarketBreakdown) map[string]*repository.MarketMetrics {
	out := map[string]*repository.MarketMetrics{}
	for key, m := range map[string]*repository.MarketMetrics{
		"stocks": b.Stocks, "spot": b.Spot, "swap": b.Swap, "futures": b.Futures,
		"options": b.Options, "margin": b.Margin, "earn": b.Earn, "cfd": b.CFD,
		"forex": b.Forex, "commodities": b.Commodities,
	} {
		if m != nil {
			out[key] = m
		}
	}
	return out
}
