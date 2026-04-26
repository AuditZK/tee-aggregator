package connector

import (
	"context"
	"time"
)

// Market type constants
const (
	MarketSpot        = "spot"
	MarketStocks      = "stocks"
	MarketSwap        = "swap"
	MarketFutures     = "futures"
	MarketOptions     = "options"
	MarketMargin      = "margin"
	MarketEarn        = "earn"
	MarketCFD         = "cfd"
	MarketForex       = "forex"
	MarketCommodities = "commodities"
)

// Connector defines the interface for exchange connectors
type Connector interface {
	// GetBalance returns current account balance and equity
	GetBalance(ctx context.Context) (*Balance, error)

	// GetPositions returns open positions
	GetPositions(ctx context.Context) ([]*Position, error)

	// GetTrades returns trades within the time range (memory only, never persisted)
	GetTrades(ctx context.Context, start, end time.Time) ([]*Trade, error)

	// TestConnection verifies API credentials
	TestConnection(ctx context.Context) error

	// Exchange returns the exchange identifier
	Exchange() string
}

// KYCLevelFetcher optionally provides exchange KYC level metadata.
// Implementations should return empty string when unsupported.
type KYCLevelFetcher interface {
	FetchKYCLevel(ctx context.Context) (string, error)
}

// PaperAccountDetector optionally detects paper/demo account mode.
type PaperAccountDetector interface {
	DetectIsPaper(ctx context.Context) (bool, error)
}

// Cashflow represents a deposit or withdrawal event.
type Cashflow struct {
	Amount    float64   `json:"amount"` // positive = deposit, negative = withdrawal
	Currency  string    `json:"currency"`
	Timestamp time.Time `json:"timestamp"`
}

// CashflowFetcher optionally provides deposit/withdrawal history.
// Connectors that support capital flow tracking implement this interface.
type CashflowFetcher interface {
	GetCashflows(ctx context.Context, since time.Time) ([]*Cashflow, error)
}

// MarketBalance holds equity data for a specific market type.
type MarketBalance struct {
	MarketType      string  `json:"market_type"`
	Equity          float64 `json:"equity"`
	AvailableMargin float64 `json:"available_margin"`
}

// BalanceByMarketFetcher optionally provides per-market balance breakdown.
// Connectors that support market-specific balance queries implement this.
type BalanceByMarketFetcher interface {
	GetBalanceByMarket(ctx context.Context) ([]*MarketBalance, error)
}

// FundingFee represents a single funding fee payment on a perpetual/swap position.
type FundingFee struct {
	Amount    float64   `json:"amount"`
	Symbol    string    `json:"symbol"`
	Timestamp time.Time `json:"timestamp"`
}

// FundingFeesFetcher optionally provides funding fee history for swap/perpetual markets.
type FundingFeesFetcher interface {
	GetFundingFees(ctx context.Context, symbols []string, since time.Time) ([]*FundingFee, error)
}

// EarnBalanceFetcher optionally provides earn/staking balance.
type EarnBalanceFetcher interface {
	GetEarnBalance(ctx context.Context) (float64, error)
}

// MarketTypeDetector optionally detects which market types an exchange supports.
// Returns a slice of market type constants (e.g., ["spot", "swap", "futures"]).
type MarketTypeDetector interface {
	DetectMarketTypes(ctx context.Context) ([]string, error)
}

// PerMarketTradeFetcher optionally fetches trades for a specific market type.
type PerMarketTradeFetcher interface {
	GetTradesByMarket(ctx context.Context, marketType string, since time.Time) ([]*Trade, error)
}

// TokenPersister is called when OAuth tokens are refreshed, to persist them to DB.
type TokenPersister func(ctx context.Context, accessToken, refreshToken string) error

// TokenRefreshable optionally allows setting a callback for token persistence.
type TokenRefreshable interface {
	SetTokenPersister(persister TokenPersister)
}

// HistoricalSnapshotProvider optionally provides historical daily snapshots.
// Used by IBKR Flex for 365-day backfill on first sync.
type HistoricalSnapshotProvider interface {
	GetHistoricalSnapshots(ctx context.Context, since time.Time) ([]*HistoricalSnapshot, error)
}

// HistoricalSnapshot represents a historical daily equity snapshot from the exchange.
type HistoricalSnapshot struct {
	Date            time.Time                 `json:"date"`
	TotalEquity     float64                   `json:"total_equity"`
	RealizedBalance float64                   `json:"realized_balance"`
	Deposits        float64                   `json:"deposits"`
	Withdrawals     float64                   `json:"withdrawals"`
	Breakdown       map[string]*MarketBalance `json:"breakdown,omitempty"` // per-asset breakdown (stocks, options, futures, etc.)
}

// Balance represents account balance data
type Balance struct {
	Available     float64 `json:"available"`      // Free balance
	Equity        float64 `json:"equity"`         // Total equity (balance + unrealized)
	UnrealizedPnL float64 `json:"unrealized_pnl"` // Unrealized P&L
	Currency      string  `json:"currency"`
}

// Position represents an open position
type Position struct {
	Symbol        string  `json:"symbol"`
	Side          string  `json:"side"` // "long" or "short"
	Size          float64 `json:"size"`
	EntryPrice    float64 `json:"entry_price"`
	MarkPrice     float64 `json:"mark_price"`
	UnrealizedPnL float64 `json:"unrealized_pnl"`
	MarketType    string  `json:"market_type"` // "spot", "stocks", "swap", "futures"
}

// Trade represents a single trade (memory only)
type Trade struct {
	ID          string    `json:"id"`
	Symbol      string    `json:"symbol"`
	Side        string    `json:"side"` // "buy" or "sell"
	Price       float64   `json:"price"`
	Quantity    float64   `json:"quantity"`
	Fee         float64   `json:"fee"`
	FeeCurrency string    `json:"fee_currency"`
	RealizedPnL float64   `json:"realized_pnl"`
	Timestamp   time.Time `json:"timestamp"`
	MarketType  string    `json:"market_type"` // "spot", "stocks", "swap", "futures", "options"
}

// Credentials holds decrypted API credentials
type Credentials struct {
	Exchange   string
	APIKey     string
	APISecret  string
	Passphrase string // Optional, for exchanges like OKX

	// DEX connectors (Hyperliquid, Lighter) - read-only, wallet address only
	WalletAddress string

	// OAuth connectors (cTrader)
	AccessToken  string
	ClientID     string
	ClientSecret string
}
