package connector

import (
	"context"
	"math/rand"
	"time"
)

// MockConnector is an exchange connector serving canned data for stress testing.
type MockConnector struct {
	exchange string
	balance  *Balance
}

// NewMock creates a mock connector with a fixed balance and generated trades.
func NewMock() *MockConnector {
	m := &MockConnector{
		exchange: "mock",
		balance: &Balance{
			Available:     10000.0,
			Equity:        12500.0,
			UnrealizedPnL: 2500.0,
			Currency:      "USD",
		},
	}
	return m
}

func (m *MockConnector) GetBalance(ctx context.Context) (*Balance, error) {
	return m.balance, nil
}

func (m *MockConnector) GetPositions(ctx context.Context) ([]*Position, error) {
	return []*Position{
		{
			Symbol:        "BTC/USD",
			Side:          "long",
			Size:          0.5,
			EntryPrice:    45000,
			MarkPrice:     50000,
			UnrealizedPnL: 2500,
			MarketType:    MarketSwap,
		},
	}, nil
}

func (m *MockConnector) GetTrades(ctx context.Context, start, end time.Time) ([]*Trade, error) {
	// Generate some random trades
	var trades []*Trade
	for t := start; t.Before(end); t = t.Add(24 * time.Hour) {
		trades = append(trades, &Trade{
			ID:          t.Format("20060102") + "-mock",
			Symbol:      "BTC/USD",
			Side:        "buy",
			Price:       45000 + rand.Float64()*5000,
			Quantity:    0.01 + rand.Float64()*0.09,
			Fee:         0.5,
			FeeCurrency: "USD",
			Timestamp:   t,
			MarketType:  MarketSwap,
		})
	}
	return trades, nil
}

func (m *MockConnector) TestConnection(ctx context.Context) error {
	return nil
}

func (m *MockConnector) Exchange() string {
	return m.exchange
}
