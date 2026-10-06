package service

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/trackrecord/enclave/internal/connector"
	"github.com/trackrecord/enclave/internal/repository"
)

// recountScope is what a venue's connector reads back over a past range: the
// markets whose fills it returns for any window, and how long the venue keeps
// them. A recount rewrites those markets only; the others keep what their
// sync stored.
type recountScope struct {
	markets []string
	horizon time.Duration
}

var activityRecountScopes = map[string]recountScope{
	"bybit": {markets: []string{connector.MarketSwap}},
	// Spot reads only the symbols held today, one day per call, so its past
	// days cannot be read back. UM fills are kept three months.
	"binance": {markets: []string{connector.MarketSwap}, horizon: 89 * 24 * time.Hour},
}

func recountScopeFor(exchange string) (recountScope, bool) {
	sc, ok := activityRecountScopes[strings.ToLower(exchange)]
	return sc, ok
}

// RecountDay is one live snapshot's activity, as stored and as the connector
// reads it today.
type RecountDay struct {
	Day            time.Time `json:"day"`
	OldTrades      int       `json:"old_trades"`
	NewTrades      int       `json:"new_trades"`
	OldVolume      float64   `json:"old_volume"`
	NewVolume      float64   `json:"new_volume"`
	OldTradingFees float64   `json:"old_trading_fees"`
	NewTradingFees float64   `json:"new_trading_fees"`
	OldFundingFees float64   `json:"old_funding_fees"`
	NewFundingFees float64   `json:"new_funding_fees"`
	OldLongTrades  int       `json:"old_long_trades"`
	NewLongTrades  int       `json:"new_long_trades"`
	OldShortTrades int       `json:"old_short_trades"`
	NewShortTrades int       `json:"new_short_trades"`
	// Kept names why the row was left as stored; empty otherwise.
	Kept    string `json:"kept,omitempty"`
	Written bool   `json:"written,omitempty"`

	breakdown *repository.MarketBreakdown
}

func (d RecountDay) changed() bool {
	return d.NewTrades != d.OldTrades ||
		d.NewLongTrades != d.OldLongTrades ||
		d.NewShortTrades != d.OldShortTrades ||
		math.Abs(d.NewVolume-d.OldVolume) > 1e-9 ||
		math.Abs(d.NewTradingFees-d.OldTradingFees) > 1e-9 ||
		math.Abs(d.NewFundingFees-d.OldFundingFees) > 1e-9
}

// RecountActivity re-derives the trades, volume, fees, funding and buy/sell
// split of a connection's live snapshots between from and to (snapshot days,
// inclusive), after a fix to how a connector reads its fills left earlier
// days counted under the old reading. Equity, margin and flows are never
// touched. Each day's window is the one its sync used. With apply false
// nothing is written.
func (s *SyncService) RecountActivity(ctx context.Context, userUID, exchange, label string, from, to time.Time, apply bool) ([]RecountDay, error) {
	from, to = from.UTC().Truncate(24*time.Hour), to.UTC().Truncate(24*time.Hour)
	if err := checkReflowWindow(from, to); err != nil {
		return nil, err
	}
	scope, ok := recountScopeFor(exchange)
	if !ok {
		return nil, fmt.Errorf("recount: %s cannot read a past range of fills", exchange)
	}
	connMeta, conn, err := s.recountSource(ctx, userUID, exchange, label)
	if err != nil {
		return nil, err
	}
	rows, _, err := s.reflowRows(ctx, connMeta, from, to)
	if err != nil {
		return nil, err
	}

	since := reflowSince(rows, from)
	// A venue asked past its horizon answers with nothing, which would empty
	// the day rather than fix it.
	if scope.horizon > 0 && since.Before(time.Now().Add(-scope.horizon)) {
		return nil, fmt.Errorf("recount: %s keeps fills for %d days", exchange, int(scope.horizon/(24*time.Hour)))
	}
	trades, err := conn.GetTrades(ctx, since, to)
	if err != nil {
		return nil, fmt.Errorf("read trades: %w", err)
	}
	var funding []*connector.FundingFee
	ff, readsFunding := conn.(connector.FundingFeesFetcher)
	if readsFunding {
		if funding, err = ff.GetFundingFees(ctx, nil, since); err != nil {
			return nil, fmt.Errorf("read funding: %w", err)
		}
	}

	var out []RecountDay
	written := 0
	for i, r := range rows {
		if day := r.Timestamp.UTC(); day.Before(from) || day.After(to) {
			continue
		}
		d := s.recountDay(rows, i, connMeta.Exchange, scope.markets, trades, funding, readsFunding)
		if apply && d.Kept == "" && d.changed() {
			ok, err := s.snapshotRepo.UpdateActivity(ctx, r.ID, d.OldTrades, d.OldVolume, d.breakdown)
			if err != nil {
				return out, err
			}
			d.Written = ok
			if ok {
				written++
			}
		}
		out = append(out, d)
	}

	s.logger.Info("activity recount",
		zap.String("user_uid", userUID),
		zap.String("exchange", connMeta.Exchange),
		zap.String("label", connMeta.Label),
		zap.Bool("apply", apply),
		zap.Int("days", len(out)),
		zap.Int("written", written),
	)
	if written > 0 {
		s.notifyHistoryRebuilt(ctx, userUID)
	}
	return out, nil
}

// recountSource returns the connection and its connector, refusing an
// account held outside USD: its stored activity was converted at the day's
// rate, which a recount would have to redo.
func (s *SyncService) recountSource(ctx context.Context, userUID, exchange, label string) (*repository.ExchangeConnection, connector.Connector, error) {
	connMeta, err := s.connSvc.GetActiveConnectionByLabel(ctx, userUID, exchange, label)
	if err != nil {
		return nil, nil, fmt.Errorf("look up connection: %w", err)
	}
	creds, err := s.connSvc.GetDecryptedCredentialsByLabel(ctx, userUID, connMeta.Exchange, connMeta.Label)
	if err != nil {
		return nil, nil, fmt.Errorf("decrypt credentials: %w", err)
	}
	conn, err := s.getOrCreateConnector(connMeta.Exchange, userUID, connMeta.Label, creds)
	if err != nil {
		return nil, nil, fmt.Errorf("build connector: %w", err)
	}
	balance, err := conn.GetBalance(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("read balance: %w", err)
	}
	if currency, convert := fiatToConvert(balance.Currency); convert {
		return nil, nil, fmt.Errorf("recount: account held in %s", currency)
	}
	return connMeta, conn, nil
}

// recountDay rebuilds the activity of row i's recounted markets from the fills
// and funding in its window. The row is kept as stored when it was
// reconstructed, or when its global entry does not add up to its markets: a
// row written before the per-market breakdown existed, which a recount would
// double.
func (s *SyncService) recountDay(rows []*repository.Snapshot, i int, exchange string, markets []string, trades []*connector.Trade, funding []*connector.FundingFee, readsFunding bool) RecountDay {
	r := rows[i]
	day := r.Timestamp.UTC()
	d := RecountDay{Day: day}
	if r.Breakdown != nil && r.Breakdown.Global != nil {
		g := r.Breakdown.Global
		d.OldTrades, d.OldVolume = g.Trades, g.Volume
		d.OldTradingFees, d.OldFundingFees = g.TradingFees, g.FundingFees
		d.OldLongTrades, d.OldShortTrades = g.LongTrades, g.ShortTrades
	}
	switch {
	case r.FromExternalRebuilder || r.IsHistorical:
		d.Kept = "reconstructed day"
	case !marketsAddUp(r.Breakdown):
		d.Kept = "global does not add up to its markets"
	}
	if d.Kept != "" {
		d.NewTrades, d.NewVolume = d.OldTrades, d.OldVolume
		d.NewTradingFees, d.NewFundingFees = d.OldTradingFees, d.OldFundingFees
		d.NewLongTrades, d.NewShortTrades = d.OldLongTrades, d.OldShortTrades
		return d
	}

	start := reflowWindowStart(rows, i)
	var inWindow []*connector.Trade
	for _, t := range trades {
		if ts := t.Timestamp.UTC(); !ts.Before(start) && ts.Before(day) && containsMarket(markets, t.MarketType) {
			inWindow = append(inWindow, t)
		}
	}
	agg := s.aggregateTrades(inWindow)
	if readsFunding {
		var fees []*connector.FundingFee
		for _, f := range funding {
			if ts := f.Timestamp.UTC(); !ts.Before(start) && ts.Before(day) {
				fees = append(fees, f)
			}
		}
		applyFundingFees(agg, exchange, fees)
	}

	d.breakdown = withActivity(r.Breakdown, agg, markets, readsFunding, r.TotalEquity)
	g := d.breakdown.Global
	d.NewTrades, d.NewVolume = g.Trades, g.Volume
	d.NewTradingFees, d.NewFundingFees = g.TradingFees, g.FundingFees
	d.NewLongTrades, d.NewShortTrades = g.LongTrades, g.ShortTrades
	return d
}

func containsMarket(markets []string, m string) bool {
	for _, x := range markets {
		if x == m {
			return true
		}
	}
	return false
}

type marketSlot struct {
	name string
	m    **repository.MarketMetrics
	agg  *marketAgg
}

func marketSlots(b *repository.MarketBreakdown, agg *aggregatedBreakdown) []marketSlot {
	return []marketSlot{
		{connector.MarketStocks, &b.Stocks, &agg.stocks}, {connector.MarketSpot, &b.Spot, &agg.spot},
		{connector.MarketSwap, &b.Swap, &agg.swap}, {connector.MarketFutures, &b.Futures, &agg.futures},
		{connector.MarketOptions, &b.Options, &agg.options}, {connector.MarketMargin, &b.Margin, &agg.margin},
		{connector.MarketEarn, &b.Earn, &agg.earn}, {connector.MarketCFD, &b.CFD, &agg.cfd},
		{connector.MarketForex, &b.Forex, &agg.forex}, {connector.MarketCommodities, &b.Commodities, &agg.commodities},
	}
}

// marketsAddUp reports whether the global entry's trades, volume and fees are
// the sum of the markets'.
func marketsAddUp(b *repository.MarketBreakdown) bool {
	if b == nil || b.Global == nil {
		return true
	}
	trades, volume, fees := 0, 0.0, 0.0
	for _, sl := range marketSlots(b, &aggregatedBreakdown{}) {
		if m := *sl.m; m != nil {
			trades += m.Trades
			volume += m.Volume
			fees += m.TradingFees
		}
	}
	g := b.Global
	return trades == g.Trades &&
		math.Abs(volume-g.Volume) <= 1e-6*math.Max(1, g.Volume) &&
		math.Abs(fees-g.TradingFees) <= 1e-6*math.Max(1, math.Abs(g.TradingFees))
}

// withActivity returns a copy of stored whose recounted markets carry agg's
// activity, funding included only when the connector read it. Every other
// market, and every equity, margin and FX field, stays as stored; global's
// activity is the sum of the markets.
func withActivity(stored *repository.MarketBreakdown, agg *aggregatedBreakdown, markets []string, withFunding bool, equity float64) *repository.MarketBreakdown {
	out := &repository.MarketBreakdown{}
	if stored != nil {
		*out = *stored
	}
	slots := marketSlots(out, agg)
	for _, sl := range slots {
		if !containsMarket(markets, sl.name) {
			continue
		}
		if *sl.m == nil && sl.agg.trades == 0 && (!withFunding || sl.agg.fundingFees == 0) {
			continue
		}
		m := &repository.MarketMetrics{}
		if *sl.m != nil {
			*m = **sl.m
		}
		funding := m.FundingFees
		if withFunding {
			funding = sl.agg.fundingFees
		}
		setActivity(m, sl.agg.volume, sl.agg.trades, sl.agg.fees, funding,
			sl.agg.longTrades, sl.agg.shortTrades, sl.agg.longVolume, sl.agg.shortVolume)
		*sl.m = m
	}

	sum := &repository.MarketMetrics{}
	for _, sl := range slots {
		if m := *sl.m; m != nil {
			sum.Volume += m.Volume
			sum.Trades += m.Trades
			sum.TradingFees += m.TradingFees
			sum.FundingFees += m.FundingFees
			sum.LongTrades += m.LongTrades
			sum.ShortTrades += m.ShortTrades
			sum.LongVolume += m.LongVolume
			sum.ShortVolume += m.ShortVolume
		}
	}
	g := &repository.MarketMetrics{Equity: equity}
	if out.Global != nil {
		*g = *out.Global
	}
	setActivity(g, sum.Volume, sum.Trades, sum.TradingFees, sum.FundingFees,
		sum.LongTrades, sum.ShortTrades, sum.LongVolume, sum.ShortVolume)
	out.Global = g
	return out
}

func setActivity(m *repository.MarketMetrics, volume float64, trades int, fees, funding float64, longT, shortT int, longV, shortV float64) {
	m.Volume, m.Trades, m.TradingFees, m.FundingFees = volume, trades, fees, funding
	m.LongTrades, m.ShortTrades, m.LongVolume, m.ShortVolume = longT, shortT, longV, shortV
}
