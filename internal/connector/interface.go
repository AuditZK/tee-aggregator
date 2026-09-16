package connector

import (
	"context"
	"errors"
	"sort"
	"time"
)

// ErrTransient marks errors that are temporary upstream conditions (rate limit,
// generation backlog, service unavailable) and NOT credential failures. The
// connection service treats these as non-fatal during initial credential
// validation: the connection is saved and the daily scheduler retries.
var ErrTransient = errors.New("transient connector error")

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

// RawBalanceOp is a single broker balance-operation ledger entry with its raw
// operation type preserved — including the ones GetCashflows drops (swap,
// commission, and any untyped adjustment/reset). Diagnostic surface for
// classifying balance jumps a deposit/withdraw filter can't explain.
type RawBalanceOp struct {
	OperationType int       `json:"operationType"`
	Delta         float64   `json:"delta"`        // signed change to the balance
	BalanceAfter  float64   `json:"balanceAfter"` // account balance after the operation; 0 on venues whose ledger carries no running balance
	Timestamp     time.Time `json:"timestamp"`
	// Label carries the venue's own type name where it is not numeric, so a
	// ledger entry can be read without a code table.
	Label    string `json:"label,omitempty"`
	Currency string `json:"currency,omitempty"`
	Symbol   string `json:"symbol,omitempty"`
	// PositionValue is the market value an entry moves without moving cash.
	// A position transfer is invisible in Delta by construction, and that is
	// exactly the entry worth seeing.
	PositionValue float64 `json:"positionValue,omitempty"`
}

// RawCashflowFetcher optionally exposes the unfiltered balance-operation ledger.
type RawCashflowFetcher interface {
	GetRawCashflowEntries(ctx context.Context, since time.Time) ([]RawBalanceOp, error)
}

// RawStatementProvider optionally exposes the venue's own statement document,
// whole and unparsed. It exists for venues that ration statements: IBKR allows
// roughly one Flex request per token per six hours and counts refusals, so a
// question the parsed views cannot answer costs another six-hour wait. Taking
// the document once makes every later question free. Secrets are scrubbed by
// the implementation; the caller still holds live account data and must keep
// it inside the enclave.
type RawStatementProvider interface {
	GetRawStatement(ctx context.Context) (document []byte, contentType string, err error)
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

// BalanceProbe is one venue balance payload shown as it arrived, next to the
// Balance the connector derived from it. It exists so a free-margin figure can
// be checked against a real account without anybody reading the account's
// credentials or its trades: every amount is a verbatim string from the venue,
// the derived side says which field pair produced it, and nothing here is
// persisted or logged. Served only to a loopback caller
// (POST /api/v1/admin/balance-probe).
type BalanceProbe struct {
	Exchange string `json:"exchange"`

	// AccountMode is the venue's own name for the account's margin mode, when
	// the connector could read it; empty when it could not.
	AccountMode string `json:"account_mode,omitempty"`

	// MarginBasis names the fields Available was computed from, in the venue's
	// own field names — e.g. "account.adjEq - account.imr". This is the answer
	// the probe exists to give: which reading of the payload is in force.
	MarginBasis string `json:"margin_basis"`

	// Account holds the account-level fields verbatim, Currencies the
	// per-currency lines. Strings, not floats: an inapplicable field is "" at
	// the venue and must not arrive here as a zero.
	Account    map[string]string   `json:"account"`
	Currencies []map[string]string `json:"currencies"`

	// Derived is what GetBalance returns from this very payload.
	Derived *Balance `json:"derived"`

	// Notes carry anything degraded about the probe itself (a mode lookup that
	// failed, say) rather than about the account.
	Notes []string `json:"notes,omitempty"`
}

// BalanceProber is implemented by connectors that can show their work: the
// venue's raw balance payload alongside the Balance they computed from it.
// A connector that does not implement it answers the admin probe with 501
// rather than a wrong-looking empty result.
type BalanceProber interface {
	ProbeBalance(ctx context.Context) (*BalanceProbe, error)
}

// FundingProber is implemented by connectors whose venue keeps a second wallet
// outside the tracked perimeter. It reports that wallet's balances and ledger
// verbatim, so a transfer that left the perimeter can be told apart from money
// that left the venue.
type FundingProber interface {
	ProbeFunding(ctx context.Context, since time.Time) (*FundingProbe, error)
}

// BalanceFreshnessProvider is implemented by statement-based connectors (IBKR
// Flex) whose GetBalance can only return the newest figure present in an
// already-generated statement — data that lags real time by one to two days.
// The sync layer uses it to refuse to stamp a stale figure with today's date:
// stamped stale, the value replays the equity of two days earlier under a new
// date and, on non-trading days, is never corrected by the backfill (the
// "phantom snapshot" defect, audit 2026-08-01).
type BalanceFreshnessProvider interface {
	// BalanceAsOf returns the statement date of the equity returned by the
	// last GetBalance call, or the zero time when the statement carried no
	// usable date (callers must then fail open).
	BalanceAsOf() time.Time
}

// CapabilityWarner is implemented by connectors that can discover, during a
// balance fetch, that the stored API key cannot read part of the account —
// e.g. a Binance key without the Futures scope answering -2015 on every fapi
// endpoint while spot/margin read fine. The sync layer forwards the warnings
// into sync_statuses (errorMessage, "warning:" prefix, status stays
// completed) so the frontend can ask the user to widen the key instead of
// silently publishing a partial equity (2026-08-05: a UM wallet moving ±15k
// stayed invisible for a month; the live equity overstated the account by
// the wallet's debt).
type CapabilityWarner interface {
	// CapabilityWarnings returns machine-readable markers (e.g.
	// "futures_permission_missing") discovered by the LAST GetBalance call;
	// empty when the key covers everything the connector tried to read.
	CapabilityWarnings() []string
}

// Wallet read outcomes. A wallet the connector reached is Read whatever the
// balance turned out to be — a wallet holding nothing is measured, not missing.
// Separating the two is the whole point: the breakdown alone cannot, because an
// empty market carries no bucket and reads exactly like one we never saw.
const (
	WalletRead       = "read"
	WalletUnreadable = "unreadable"
	WalletNotOpened  = "not_opened"
)

// WalletCoverage is one wallet's outcome for one balance fetch. Reason is
// diagnostic only — nothing downstream branches on it.
type WalletCoverage struct {
	Wallet string `json:"wallet"`
	Status string `json:"status"`
	Reason string `json:"reason,omitempty"`
}

// CoverageReporter is implemented by connectors that can say which parts of an
// account the last balance fetch actually reached.
//
// The venue-specific knowledge stops here: a connector names its own wallets
// and grades each one, and every layer above works on the resulting set of
// names alone. That is what makes the rule replicable — nothing upstream of a
// connector knows that Binance has a UM wallet or that OKX is unified.
//
// Coverage is what separates "this account holds nothing there" from "we could
// not look". Without it a wallet that leaves the measured perimeter — a scope
// the key never had, a statement window that ran out, a switch between the live
// connector and the rebuilder — reads as a change in equity, and the return
// calculation books it as performance.
type CoverageReporter interface {
	// Coverage returns one entry per wallet the connector is responsible for,
	// graded by the LAST GetBalance call. The wallet list is fixed per
	// connector; only the statuses vary between calls.
	Coverage() []WalletCoverage
}

// CoveredWallets returns the wallets actually read, sorted, which is the set
// the comparison rules operate on. Nil coverage yields nil: a connector that
// does not report yet is "unknown", never "complete".
func CoveredWallets(coverage []WalletCoverage) []string {
	if len(coverage) == 0 {
		return nil
	}
	read := make([]string, 0, len(coverage))
	for _, c := range coverage {
		if c.Status == WalletRead {
			read = append(read, c.Wallet)
		}
	}
	sort.Strings(read)
	return read
}

// UnreadableWallets returns the wallets the connector could not reach, sorted.
func UnreadableWallets(coverage []WalletCoverage) []string {
	missed := make([]string, 0, len(coverage))
	for _, c := range coverage {
		if c.Status == WalletUnreadable {
			missed = append(missed, c.Wallet)
		}
	}
	if len(missed) == 0 {
		return nil
	}
	sort.Strings(missed)
	return missed
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

// Connectors that hold a long-lived resource — a WebSocket with its read loop
// and heartbeat, a pooled session — implement io.Closer. The connector cache
// calls it whenever an instance leaves the cache (LRU eviction, TTL cleanup,
// or being replaced under the same key). Nothing else may call it: the
// instance is shared, and closing one in use breaks the caller's requests.
//
// E-H6: without this, an OAuth token rotation (which changes the cache key,
// so a fresh instance is built) left the previous cTrader instance heartbeating
// at Spotware forever.

// TokenPersister is called when OAuth tokens are refreshed, to persist them to DB.
type TokenPersister func(ctx context.Context, accessToken, refreshToken string) error

// TokenRefreshable optionally allows setting a callback for token persistence.
type TokenRefreshable interface {
	SetTokenPersister(persister TokenPersister)
}

// OAuthCredentialSource exposes the tokens a connector is ACTUALLY holding,
// which are not necessarily the ones it was built with: validating a
// connection can itself trigger a refresh, and cTrader rotates the refresh
// token on every one.
//
// E-H4: the connect path stored req.APIKey/req.APISecret, so when
// TestConnection or DetectIsPaper refreshed, the row was written with the
// pair whose refresh token the broker had just invalidated — the connection
// was born dead and failed on its first nightly sync with ACCESS_DENIED.
type OAuthCredentialSource interface {
	// CurrentCredentials returns the live access and refresh tokens.
	CurrentCredentials() (accessToken, refreshToken string)
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
	TotalTrades     int                       `json:"total_trades"`
	TotalVolume     float64                   `json:"total_volume"`
	TotalFees       float64                   `json:"total_fees"`
	FundingFees     float64                   `json:"funding_fees"`
	LongTrades      int                       `json:"long_trades"`
	ShortTrades     int                       `json:"short_trades"`
	LongVolume      float64                   `json:"long_volume"`
	ShortVolume     float64                   `json:"short_volume"`
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
	ID       string  `json:"id"`
	Symbol   string  `json:"symbol"`
	Side     string  `json:"side"` // "buy" or "sell"
	Price    float64 `json:"price"`
	Quantity float64 `json:"quantity"`
	// Underlying units per unit of Quantity, for a venue that counts
	// contracts while quoting the price per underlying unit: 100 on an
	// equity option, 50 on an ES future. Zero means Quantity is already in
	// the price's unit, which is every spot venue and every connector not
	// yet taught its own contract sizes.
	Multiplier  float64   `json:"multiplier,omitempty"`
	Fee         float64   `json:"fee"`
	FeeCurrency string    `json:"fee_currency"`
	RealizedPnL float64   `json:"realized_pnl"`
	Timestamp   time.Time `json:"timestamp"`
	MarketType  string    `json:"market_type"` // "spot", "stocks", "swap", "futures", "options"
}

// Notional is the value traded, in the currency Price is quoted in. Reading
// Price * Quantity off a contract undercounts it by the multiplier, which put
// a day of equity options on the dashboard at one hundredth of its size.
func (t *Trade) Notional() float64 {
	if t.Multiplier > 0 {
		return t.Price * t.Quantity * t.Multiplier
	}
	return t.Price * t.Quantity
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
