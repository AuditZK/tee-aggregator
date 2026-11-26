package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/trackrecord/enclave/internal/connector"
	"github.com/trackrecord/enclave/internal/repository"
	"go.uber.org/zap"
)

type feeLedgerConnector struct {
	plainStubConnector
	trades       []*connector.Trade
	fees         float64
	err          error
	since, until time.Time
}

func (c *feeLedgerConnector) GetTrades(context.Context, time.Time, time.Time) ([]*connector.Trade, error) {
	return c.trades, nil
}

func (c *feeLedgerConnector) GetNetTradingFees(_ context.Context, since, until time.Time) (float64, error) {
	c.since, c.until = since, until
	return c.fees, c.err
}

func TestReadActivity_FeeLedgerBooksNetFeesOnTheTradedMarket(t *testing.T) {
	from := time.Date(2031, time.March, 4, 0, 0, 0, 0, time.UTC)
	now := from.Add(24*time.Hour + 5*time.Minute)
	meta := &repository.ExchangeConnection{UserUID: "user_synthetic", Exchange: "polymarket", Label: "main"}
	trade := &connector.Trade{ID: "t1", Side: "buy", Price: 0.5, Quantity: 10, MarketType: connector.MarketSpot, Timestamp: from.Add(time.Hour)}

	t.Run("rebates above the charges read as a negative fee", func(t *testing.T) {
		conn := &feeLedgerConnector{trades: []*connector.Trade{trade}, fees: -7.5}
		act := (&SyncService{logger: zap.NewNop()}).readActivity(context.Background(), conn, meta, from, now, nil)
		if !conn.since.Equal(from) || !conn.until.Equal(now) {
			t.Errorf("ledger read over %v..%v, want the activity window %v..%v", conn.since, conn.until, from, now)
		}
		bd := act.breakdown.toRepo(100, 0, len(act.trades))
		if bd.Spot == nil || !near(bd.Spot.TradingFees, -7.5, 1e-9) {
			t.Fatalf("spot = %+v, want trading fees -7.5", bd.Spot)
		}
		if bd.Global == nil || !near(bd.Global.TradingFees, -7.5, 1e-9) || !near(act.breakdown.totalFees(), -7.5, 1e-9) {
			t.Fatalf("global = %+v, total %v, want -7.5 on both", bd.Global, act.breakdown.totalFees())
		}
	})

	t.Run("a rebate on a day without a fill is kept", func(t *testing.T) {
		conn := &feeLedgerConnector{fees: -2}
		act := (&SyncService{logger: zap.NewNop()}).readActivity(context.Background(), conn, meta, from, now, nil)
		bd := act.breakdown.toRepo(100, 0, 0)
		if bd.Spot == nil || !near(bd.Spot.TradingFees, -2, 1e-9) || !near(bd.Global.TradingFees, -2, 1e-9) {
			t.Fatalf("breakdown = %+v, want the -2 rebate on spot and global", bd)
		}
	})

	t.Run("an unreadable ledger leaves the fills' own fees", func(t *testing.T) {
		paid := *trade
		paid.Fee = 0.25
		conn := &feeLedgerConnector{trades: []*connector.Trade{&paid}, fees: -9, err: errors.New("unreachable")}
		act := (&SyncService{logger: zap.NewNop()}).readActivity(context.Background(), conn, meta, from, now, nil)
		if got := act.breakdown.totalFees(); !near(got, 0.25, 1e-9) {
			t.Fatalf("fees = %v, want 0.25", got)
		}
	})
}
