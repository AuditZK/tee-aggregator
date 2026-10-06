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

// activityRecountExchanges are the venues whose GetTrades and GetFundingFees
// read any past range on request. The others read a fixed recent window or
// per symbol, and a recount would empty the days they cannot reach.
var activityRecountExchanges = []string{"bybit"}

func recountsActivity(exchange string) bool {
	e := strings.ToLower(exchange)
	for _, x := range activityRecountExchanges {
		if x == e {
			return true
		}
	}
	return false
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
	if !recountsActivity(exchange) {
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
	trades, err := conn.GetTrades(ctx, since, to)
	if err != nil {
		return nil, fmt.Errorf("read trades: %w", err)
	}
	var funding []*connector.FundingFee
	if ff, ok := conn.(connector.FundingFeesFetcher); ok {
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
		d := s.recountDay(rows, i, connMeta.Exchange, trades, funding)
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

// recountDay rebuilds row i's activity from the fills and funding in its
// window, kept as stored when the row was reconstructed.
func (s *SyncService) recountDay(rows []*repository.Snapshot, i int, exchange string, trades []*connector.Trade, funding []*connector.FundingFee) RecountDay {
	r := rows[i]
	day := r.Timestamp.UTC()
	d := RecountDay{Day: day}
	if r.Breakdown != nil && r.Breakdown.Global != nil {
		g := r.Breakdown.Global
		d.OldTrades, d.OldVolume = g.Trades, g.Volume
		d.OldTradingFees, d.OldFundingFees = g.TradingFees, g.FundingFees
		d.OldLongTrades, d.OldShortTrades = g.LongTrades, g.ShortTrades
	}
	if r.FromExternalRebuilder || r.IsHistorical {
		d.Kept = "reconstructed day"
		d.NewTrades, d.NewVolume = d.OldTrades, d.OldVolume
		d.NewTradingFees, d.NewFundingFees = d.OldTradingFees, d.OldFundingFees
		d.NewLongTrades, d.NewShortTrades = d.OldLongTrades, d.OldShortTrades
		return d
	}

	start := reflowWindowStart(rows, i)
	var inWindow []*connector.Trade
	for _, t := range trades {
		if ts := t.Timestamp.UTC(); !ts.Before(start) && ts.Before(day) {
			inWindow = append(inWindow, t)
		}
	}
	var fees []*connector.FundingFee
	for _, f := range funding {
		if ts := f.Timestamp.UTC(); !ts.Before(start) && ts.Before(day) {
			fees = append(fees, f)
		}
	}
	agg := s.aggregateTrades(inWindow)
	applyFundingFees(agg, exchange, fees)

	d.breakdown = withActivity(r.Breakdown, agg, len(inWindow), r.TotalEquity)
	g := d.breakdown.Global
	d.NewTrades, d.NewVolume = g.Trades, g.Volume
	d.NewTradingFees, d.NewFundingFees = g.TradingFees, g.FundingFees
	d.NewLongTrades, d.NewShortTrades = g.LongTrades, g.ShortTrades
	return d
}

// withActivity returns a copy of stored whose activity fields are agg's, every
// equity, margin and FX field left as stored.
func withActivity(stored *repository.MarketBreakdown, agg *aggregatedBreakdown, trades int, equity float64) *repository.MarketBreakdown {
	out := &repository.MarketBreakdown{}
	if stored != nil {
		*out = *stored
	}
	slots := []struct {
		m   **repository.MarketMetrics
		agg *marketAgg
	}{
		{&out.Stocks, &agg.stocks}, {&out.Spot, &agg.spot}, {&out.Swap, &agg.swap},
		{&out.Futures, &agg.futures}, {&out.Options, &agg.options}, {&out.Margin, &agg.margin},
		{&out.Earn, &agg.earn}, {&out.CFD, &agg.cfd}, {&out.Forex, &agg.forex},
		{&out.Commodities, &agg.commodities},
	}
	for _, sl := range slots {
		if *sl.m == nil && sl.agg.trades == 0 && sl.agg.fundingFees == 0 {
			continue
		}
		m := &repository.MarketMetrics{}
		if *sl.m != nil {
			*m = **sl.m
		}
		setActivity(m, sl.agg.volume, sl.agg.trades, sl.agg.fees, sl.agg.fundingFees,
			sl.agg.longTrades, sl.agg.shortTrades, sl.agg.longVolume, sl.agg.shortVolume)
		*sl.m = m
	}

	g := &repository.MarketMetrics{Equity: equity}
	if out.Global != nil {
		*g = *out.Global
	}
	setActivity(g, agg.totalVolume(), trades, agg.totalFees(), agg.totalFundingFees(),
		agg.totalLongTrades(), agg.totalShortTrades(), agg.totalLongVolume(), agg.totalShortVolume())
	out.Global = g
	return out
}

func setActivity(m *repository.MarketMetrics, volume float64, trades int, fees, funding float64, longT, shortT int, longV, shortV float64) {
	m.Volume, m.Trades, m.TradingFees, m.FundingFees = volume, trades, fees, funding
	m.LongTrades, m.ShortTrades, m.LongVolume, m.ShortVolume = longT, shortT, longV, shortV
}
