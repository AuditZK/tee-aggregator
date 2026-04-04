package connector

import (
	"context"
	"testing"
	"time"
)

func TestCashflowPositiveIsDeposit(t *testing.T) {
	cf := Cashflow{
		Amount:    500.0,
		Currency:  "USD",
		Timestamp: time.Date(2026, 3, 15, 10, 0, 0, 0, time.UTC),
	}

	if cf.Amount <= 0 {
		t.Fatalf("expected positive amount for deposit, got %f", cf.Amount)
	}
	if cf.Currency != "USD" {
		t.Fatalf("currency: got %q, want USD", cf.Currency)
	}
	if cf.Timestamp.IsZero() {
		t.Fatal("expected non-zero timestamp")
	}
}

func TestCashflowNegativeIsWithdrawal(t *testing.T) {
	cf := Cashflow{
		Amount:    -250.0,
		Currency:  "USDT",
		Timestamp: time.Date(2026, 3, 16, 14, 30, 0, 0, time.UTC),
	}

	if cf.Amount >= 0 {
		t.Fatalf("expected negative amount for withdrawal, got %f", cf.Amount)
	}
	if cf.Currency != "USDT" {
		t.Fatalf("currency: got %q, want USDT", cf.Currency)
	}
}

func TestMarketBalanceStruct(t *testing.T) {
	tests := []struct {
		name            string
		marketType      string
		equity          float64
		availableMargin float64
	}{
		{"futures with margin", MarketFutures, 10000, 5000},
		{"spot equity only", MarketSpot, 3000, 0},
		{"swap zero balance", MarketSwap, 0, 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mb := MarketBalance{
				MarketType:      tc.marketType,
				Equity:          tc.equity,
				AvailableMargin: tc.availableMargin,
			}

			if mb.MarketType != tc.marketType {
				t.Fatalf("MarketType: got %q, want %q", mb.MarketType, tc.marketType)
			}
			if mb.Equity != tc.equity {
				t.Fatalf("Equity: got %f, want %f", mb.Equity, tc.equity)
			}
			if mb.AvailableMargin != tc.availableMargin {
				t.Fatalf("AvailableMargin: got %f, want %f", mb.AvailableMargin, tc.availableMargin)
			}
		})
	}
}

func TestHistoricalSnapshotStruct(t *testing.T) {
	date := time.Date(2025, 12, 1, 0, 0, 0, 0, time.UTC)
	hs := HistoricalSnapshot{
		Date:            date,
		TotalEquity:     50000,
		RealizedBalance: 48000,
		Deposits:        1000,
		Withdrawals:     500,
	}

	if !hs.Date.Equal(date) {
		t.Fatalf("Date: got %s, want %s", hs.Date, date)
	}
	if hs.TotalEquity != 50000 {
		t.Fatalf("TotalEquity: got %f, want 50000", hs.TotalEquity)
	}
	if hs.RealizedBalance != 48000 {
		t.Fatalf("RealizedBalance: got %f, want 48000", hs.RealizedBalance)
	}
	if hs.Deposits != 1000 {
		t.Fatalf("Deposits: got %f, want 1000", hs.Deposits)
	}
	if hs.Withdrawals != 500 {
		t.Fatalf("Withdrawals: got %f, want 500", hs.Withdrawals)
	}

	// Verify unrealized PnL can be derived
	unrealized := hs.TotalEquity - hs.RealizedBalance
	if unrealized != 2000 {
		t.Fatalf("derived unrealized PnL: got %f, want 2000", unrealized)
	}
}

func TestFundingFeeStruct(t *testing.T) {
	ts := time.Date(2026, 3, 20, 8, 0, 0, 0, time.UTC)
	ff := FundingFee{
		Amount:    -0.0042,
		Symbol:    "BTCUSDT",
		Timestamp: ts,
	}

	if ff.Amount != -0.0042 {
		t.Fatalf("Amount: got %f, want -0.0042", ff.Amount)
	}
	if ff.Symbol != "BTCUSDT" {
		t.Fatalf("Symbol: got %q, want BTCUSDT", ff.Symbol)
	}
	if !ff.Timestamp.Equal(ts) {
		t.Fatalf("Timestamp: got %s, want %s", ff.Timestamp, ts)
	}
}

func TestMarketTypeConstants(t *testing.T) {
	all := []string{
		MarketSpot,
		MarketStocks,
		MarketSwap,
		MarketFutures,
		MarketOptions,
		MarketMargin,
		MarketEarn,
		MarketCFD,
		MarketForex,
		MarketCommodities,
	}

	if len(all) != 10 {
		t.Fatalf("expected 10 market type constants, got %d", len(all))
	}

	seen := make(map[string]bool)
	for _, mt := range all {
		if mt == "" {
			t.Fatal("found empty market type constant")
		}
		if seen[mt] {
			t.Fatalf("duplicate market type constant: %q", mt)
		}
		seen[mt] = true
	}
}

func TestTokenPersisterType(t *testing.T) {
	// Verify TokenPersister is a func type by assigning a compatible function
	var p TokenPersister = func(_ context.Context, accessToken, refreshToken string) error {
		return nil
	}
	if p == nil {
		t.Fatal("expected non-nil TokenPersister")
	}
}
