package service

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/trackrecord/enclave/internal/connector"
	"github.com/trackrecord/enclave/internal/repository"
)

// partialTradesConnector fails its trade read after reading some fills, the
// way Binance does on a Portfolio Margin account whose papi read breaks, and
// raises its marker through CapabilityWarner.
type partialTradesConnector struct {
	trades  []*connector.Trade
	err     error
	warning string
	funding []*connector.FundingFee
	raised  []string
}

func (c *partialTradesConnector) GetBalance(context.Context) (*connector.Balance, error) {
	return &connector.Balance{Equity: 1000}, nil
}
func (c *partialTradesConnector) GetPositions(context.Context) ([]*connector.Position, error) {
	return nil, nil
}
func (c *partialTradesConnector) GetTrades(context.Context, time.Time, time.Time) ([]*connector.Trade, error) {
	if c.warning != "" {
		c.raised = append(c.raised, c.warning)
	}
	return c.trades, c.err
}
func (c *partialTradesConnector) GetFundingFees(context.Context, []string, time.Time) ([]*connector.FundingFee, error) {
	return c.funding, nil
}
func (c *partialTradesConnector) TestConnection(context.Context) error { return nil }
func (c *partialTradesConnector) Exchange() string                     { return "binance" }
func (c *partialTradesConnector) CapabilityWarnings() []string         { return slices.Clone(c.raised) }

func TestReadActivity_FailedTradeReadKeepsFillsAndRaisesMarker(t *testing.T) {
	core, logs := observer.New(zapcore.WarnLevel)
	s := NewSyncService(nil, nil, nil, zap.New(core))
	now := time.Now().UTC()
	conn := &partialTradesConnector{
		trades: []*connector.Trade{
			{ID: "1", Symbol: "ETHUSDT", Side: "buy", Price: 3000, Quantity: 1, Fee: 0.6, Timestamp: now.Add(-time.Hour), MarketType: connector.MarketSpot},
		},
		err:     errors.New("portfolio margin trades: UM fills BTCUSDT: -1000"),
		warning: "pm_trades_unreadable",
		funding: []*connector.FundingFee{
			{Amount: -1.5, Symbol: "BTCUSDT", Timestamp: calendarDay(now).Add(-2 * time.Hour)},
			{Amount: -5, Symbol: "BTCUSD_PERP", Timestamp: calendarDay(now).Add(-3 * time.Hour)},
		},
	}
	meta := &repository.ExchangeConnection{UserUID: "u", Exchange: "binance", Label: "main"}

	act := s.readActivity(context.Background(), conn, meta, now.Add(-24*time.Hour), now, nil)
	if len(act.trades) != 1 || act.breakdown.totalVolume() != 3000 {
		t.Fatalf("trades = %d, volume %v; want the one fill read before the failure", len(act.trades), act.breakdown.totalVolume())
	}
	if act.fundingCharges != -6.5 || act.breakdown.totalFundingFees() != 6.5 {
		t.Fatalf("funding = %v (breakdown %v), want -6.5 booked as a 6.5 cost", act.fundingCharges, act.breakdown.totalFundingFees())
	}
	if logs.FilterMessageSnippet("trade fetch failed").Len() != 1 {
		t.Fatal("the trade read error was dropped without a log line")
	}

	result := &SyncResult{}
	s.collectCapabilityWarnings(conn, meta, result)
	if !slices.Contains(result.CapabilityWarnings, "pm_trades_unreadable") {
		t.Fatalf("warnings = %v, want pm_trades_unreadable on the sync status", result.CapabilityWarnings)
	}
}
