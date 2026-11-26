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

// AccountScoped is implemented by connectors whose credentials can reach
// several broker accounts (an IBKR Flex query ticks any number). The sync
// layer pins the accounts a connection measures, so a query edited to cover
// other accounts cannot splice their balance into the stored history.
type AccountScoped interface {
	// SetAccountGuard installs a check every statement must pass, with the
	// broker account ids it measures, before any figure is read from it. past
	// marks a statement over a chosen past period, which may predate one of
	// the accounts.
	SetAccountGuard(guard func(accounts []string, past bool) error)
	// MeasuredAccounts returns the account ids the last balance read measured.
	MeasuredAccounts() []string
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

// CumulativePnLProvider is a venue that publishes an account's cumulative PnL
// but none of its deposits and withdrawals. The sync books as capital flow
// what moved the equity between two reads without moving that PnL.
type CumulativePnLProvider interface {
	// CumulativePnL returns the PnL in USD at a moment, or now for a zero time.
	CumulativePnL(ctx context.Context, at time.Time) (float64, error)
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

// ErrFundingNotRead is what a FundingFeesFetcher answers for an account it
// reads no funding on (a classic Binance account: only Portfolio Margin
// funding is read). It is not a zero: a caller rewriting stored funding
// must keep what it holds.
var ErrFundingNotRead = errors.New("funding not read for this account")

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

// HistoricalSnapshotProvider optionally provides historical daily snapshots
// since a date, for the reconstruction every sync of IBKR and cTrader re-runs.
type HistoricalSnapshotProvider interface {
	GetHistoricalSnapshots(ctx context.Context, since time.Time) ([]*HistoricalSnapshot, error)
}

// HistoryCoverageReporter is a history provider whose reconstruction depends
// on how much the venue agreed to serve, so a short history can be told apart
// from a young account.
type HistoryCoverageReporter interface {
	HistoryCoverage() HistoryCoverage
}

// HistoryCoverage counts what the last reconstruction read. It carries no
// amount and no instrument.
type HistoryCoverage struct {
	LedgerLines        int
	OldestLine         time.Time
	HeldPositions      int
	Instruments        int
	PricePoints        int
	PriceAllowanceLeft int
}

// PeriodHistoryProvider reads the daily timeline of a chosen period: a
// connection's year a month at a time (the statement backfill), and history
// older than a year (the deep reconstruction). One call covers at most 365
// days, IBKR's ceiling per statement.
type PeriodHistoryProvider interface {
	GetHistoricalSnapshotsForPeriod(ctx context.Context, from, to time.Time) ([]*HistoricalSnapshot, error)
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

	// MarginHeld is the initial margin the positions held at the end of the
	// day (USD), as the external rebuilder replayed it (Binance Portfolio
	// Margin). Not persisted: the rebuilt row's free margin already carries
	// it. Read by the free-margin rewrite of rebuilt days.
	MarginHeld float64 `json:"margin_held,omitempty"`

	// Currency is the ISO code the balances and flows above are in, for a
	// broker that reports an account in its own base currency (IBKR, cTrader).
	// Empty means USD. The sync converts the row before anything is stored.
	Currency string `json:"currency,omitempty"`
	// MeasuredOn is the day whose close TotalEquity describes, when that is
	// not Date: a cTrader row dated D holds the balance at the midnight that
	// opens D, so the day before. Zero means Date. It picks the FX rate.
	MeasuredOn time.Time `json:"measured_on,omitempty"`
	// Cashflows are the deposits and withdrawals summed into Deposits and
	// Withdrawals, each in its own currency, for a broker whose flows can be
	// in another currency than the account (IBKR: USD wired into a EUR
	// account). When set, the conversion rebuilds the two sums from them.
	Cashflows []*Cashflow `json:"-"`
	// FXRateToUSD is the rate the balances were multiplied by, set by the
	// conversion. Zero on a row that was never converted.
	FXRateToUSD float64 `json:"fx_rate_to_usd,omitempty"`
	// BaseRates are the venue's own rates for the row's day: units of
	// Currency per one unit of the keyed currency. They let the sync state a
	// statement in another denomination than the one it arrived in exactly
	// as the venue valued it.
	BaseRates map[string]float64 `json:"-"`
}

// Balance represents account balance data
type Balance struct {
	Available     float64 `json:"available"`      // Free balance
	Equity        float64 `json:"equity"`         // Total equity (balance + unrealized)
	UnrealizedPnL float64 `json:"unrealized_pnl"` // Unrealized P&L
	Currency      string  `json:"currency"`
	// BaseRates are the venue's own rates for the balance's day, as on
	// HistoricalSnapshot.
	BaseRates map[string]float64 `json:"-"`
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
	Multiplier float64 `json:"multiplier,omitempty"`
	// USDNotional is the dollar value traded, for a venue that states it. A
	// CFD book is quoted in a dozen currencies, and summing Price*Quantity
	// across it added a yen notional to a dollar total at 150 times its size.
	USDNotional float64   `json:"usd_notional,omitempty"`
	Fee         float64   `json:"fee"`
	FeeCurrency string    `json:"fee_currency"`
	RealizedPnL float64   `json:"realized_pnl"`
	Timestamp   time.Time `json:"timestamp"`
	MarketType  string    `json:"market_type"` // "spot", "stocks", "swap", "futures", "options"
}

// Notional is the value traded, in dollars when the venue states it and in the
// currency Price is quoted in otherwise. Reading Price * Quantity off a
// contract undercounts it by the multiplier, which put a day of equity options
// on the dashboard at one hundredth of its size.
func (t *Trade) Notional() float64 {
	if t.USDNotional > 0 {
		return t.USDNotional
	}
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

	// DEX connectors (Hyperliquid, Lighter, Polymarket, predict.fun) - read-only, wallet address only
	WalletAddress string

	// OAuth connectors (cTrader)
	AccessToken  string
	ClientID     string
	ClientSecret string

	// Demo is the connection's stored paper flag, for venues where one key
	// reaches both the live and the demo account (BingX).
	Demo bool
}

// BalanceCrossCheck compares part of the equity a GetBalance measured with
// the venue's own figure for it. The sync logs it for operators; nothing
// stores it.
type BalanceCrossCheck struct {
	Name      string
	Measured  float64
	Reference float64
}

// BalanceCrossChecker is implemented by connectors that can compare part of
// their equity with the venue's own figure for it. A check is handed out
// once, by the call after the balance read that took it; ok is false
// until another is taken.
type BalanceCrossChecker interface {
	BalanceCrossCheck() (BalanceCrossCheck, bool)
}

// EquityBasisNote says which basis the last GetBalance valued part of the
// account on, and how far the venue's own figure for it sits from that value.
// It carries no balance: the sync logs it once per sync without the user or
// the label, and nothing stores it.
type EquityBasisNote struct {
	// Name says which part of the account the note is about.
	Name string
	// Basis is what the stored equity is: EquityBasisMarketValue, or
	// EquityBasisVenue when the venue's figure was kept.
	Basis string
	// Residual is the market value less the venue's figure, in USD, and
	// ResidualShare the same as a share of the venue's figure. Both are 0
	// when the market value could not be taken.
	Residual      float64
	ResidualShare float64
	// Fallback says why the venue's figure was kept; empty when it was not.
	Fallback string
}

// The two bases an EquityBasisNote can report.
const (
	EquityBasisMarketValue = "market_value"
	EquityBasisVenue       = "venue_equity"
)

// EquityBasisReporter is implemented by connectors that value part of an
// account themselves rather than take the venue's total for it. The note of
// a balance read is handed out once, by the call after that read; ok is false
// until another read takes one.
type EquityBasisReporter interface {
	EquityBasisNote() (EquityBasisNote, bool)
}

type tradeMarketsKey struct{}

// WithTradeMarkets scopes the GetTrades calls made with the returned context
// to the given markets: a connector may skip the reads of the others. The
// activity recount rewrites swap alone, and the Binance margin fills it would
// otherwise read and drop cost most of a long recount's time.
func WithTradeMarkets(ctx context.Context, markets []string) context.Context {
	return context.WithValue(ctx, tradeMarketsKey{}, append([]string(nil), markets...))
}

// TradeMarketWanted reports whether a GetTrades call made with ctx wants the
// fills of market; every market is wanted when ctx carries no scope.
func TradeMarketWanted(ctx context.Context, market string) bool {
	markets, ok := ctx.Value(tradeMarketsKey{}).([]string)
	if !ok {
		return true
	}
	for _, m := range markets {
		if m == market {
			return true
		}
	}
	return false
}

// BalanceDegradationReporter is implemented by connectors whose balance read
// can fall back to a coarser reading. BalanceDegradations hands out, once,
// the causes the last GetBalance fell back for, worded without account
// identifiers or amounts. The sync logs them for operators; they are not
// capability markers, which the frontend shows the user.
type BalanceDegradationReporter interface {
	BalanceDegradations() []string
}
