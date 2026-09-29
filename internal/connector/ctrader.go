package connector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

const (
	ctraderWSLiveURL = "wss://live.ctraderapi.com:5036"
	ctraderWSDemoURL = "wss://demo.ctraderapi.com:5036"
	ctraderAuthURL   = "https://openapi.ctrader.com/apps"

	ctraderPayloadAppAuthReq     = 2100
	ctraderPayloadAppAuthRes     = 2101
	ctraderPayloadAccountAuthReq = 2102
	ctraderPayloadAccountAuthRes = 2103

	ctraderPayloadGetAccountsReq = 2149
	ctraderPayloadGetAccountsRes = 2150
	ctraderPayloadTraderReq      = 2121
	ctraderPayloadTraderRes      = 2122
	ctraderPayloadReconcileReq   = 2124
	ctraderPayloadReconcileRes   = 2125
	ctraderPayloadDealListReq    = 2133
	ctraderPayloadDealListRes    = 2134
	// ProtoOACashFlowHistoryList — the dedicated deposit/withdrawal endpoint.
	// cTrader does NOT expose cash flows in the deal list (2133/2134).
	ctraderPayloadCashFlowHistoryReq = 2143
	ctraderPayloadCashFlowHistoryRes = 2144
	ctraderPayloadSymbolByIDReq      = 2116
	ctraderPayloadSymbolByIDRes      = 2117

	// ProtoOAGetPositionUnrealizedPnLReq/Res — the ONLY place cTrader exposes
	// unrealized PnL. ProtoOAPosition (from the reconcile, 2124/2125) carries
	// no PnL field at all: the connector used to read a non-existent
	// "unrealizedNetProfit" off it, so uPnL decoded as 0 and live equity was
	// always the settled balance (audit E-C1, 2026-09-09).
	ctraderPayloadUnrealizedPnLReq = 2187
	ctraderPayloadUnrealizedPnLRes = 2188

	// ProtoOAAssetListReq/Res — resolves the account's depositAssetId to a
	// currency name (EUR, GBP, …). Without it every account was labelled USD.
	ctraderPayloadAssetListReq = 2112
	ctraderPayloadAssetListRes = 2113

	ctraderPayloadHeartbeatEvent = 51
	ctraderPayloadErrorRes       = 2142
	// ProtoErrorRes — the COMMON-layer error (open api common messages), sent
	// instead of ProtoOAErrorRes for protocol-level failures. It carries the
	// same errorCode/description shape; treating it as an unknown payload type
	// turned a described error into "unexpected cTrader payload type 50".
	ctraderPayloadCommonErrorRes = 50
	// ProtoOAAccountsTokenInvalidatedEvent — pushed WITHOUT a clientMsgId when
	// the user revokes the application or the token dies server-side.
	ctraderPayloadTokenInvalidatedEvent = 2147

	// ctraderRequestTimeout bounds one request/response round trip.
	ctraderRequestTimeout = 30 * time.Second
	// ctraderHeartbeatInterval is cTrader's required client heartbeat period.
	ctraderHeartbeatInterval = 10 * time.Second
	// ctraderReadDeadline is how long the socket may produce nothing at all
	// before the read loop tears it down. Every inbound frame and every
	// WebSocket pong (the heartbeat pings each tick) pushes it back, so on a
	// live socket it is never reached; on a half-open one — the TCP peer gone
	// without a FIN — it is the ONLY thing that notices, and it must, because
	// a "connected" socket that answers nothing poisoned the cached instance
	// for the full hour TTL and burned the 5-minute sync budget 30 s at a
	// time (audit E-H2).
	ctraderReadDeadline = 90 * time.Second
)

// wsResponse is the result delivered to a request waiter when the cTrader
// server replies (or when the read loop fails before a response arrives).
type wsResponse struct {
	payloadType int
	payload     json.RawMessage
	err         error
}

type wsInboundMessage struct {
	ClientMsgID string          `json:"clientMsgId"`
	PayloadType int             `json:"payloadType"`
	Payload     json.RawMessage `json:"payload"`
}

type wsOutboundMessage struct {
	ClientMsgID string         `json:"clientMsgId,omitempty"`
	PayloadType int            `json:"payloadType"`
	Payload     map[string]any `json:"payload"`
}

type cTraderErrorPayload struct {
	ErrorCode   string `json:"errorCode"`
	Description string `json:"description"`
	// RetryAfter (ProtoOAErrorRes.retryAfter) is the epoch millisecond after
	// which the blocked payload type may be sent again. Never decoded before,
	// so a BLOCKED_PAYLOAD_TYPE mid-walk aborted the whole reconstruction
	// (V-E1).
	RetryAfter uint64 `json:"retryAfter"`
	// MaintenanceEndTimestamp is set when the broker is in maintenance.
	MaintenanceEndTimestamp uint64 `json:"maintenanceEndTimestamp"`
}

// ctraderRateLimitErr carries the retry hint of a BLOCKED_PAYLOAD_TYPE /
// maintenance error so the paginated walks can wait exactly as long as the
// broker asked instead of giving up (V-E1).
type ctraderRateLimitErr struct {
	msg        string
	retryAfter time.Time
}

func (e *ctraderRateLimitErr) Error() string { return e.msg }

// ctraderTokenInvalidatedError is the message raised once cTrader has pushed
// ProtoOAAccountsTokenInvalidatedEvent. It is worded so classifySyncError and
// errsanitize both route it to the OAuth/re-authorization category.
const ctraderTokenInvalidatedError = "cTrader error ACCESS_DENIED: the broker invalidated this access token (ProtoOAAccountsTokenInvalidatedEvent); the account must be re-authorized"

type cTraderAccount struct {
	CtidTraderAccountID int64  `json:"ctidTraderAccountId"`
	IsLive              bool   `json:"isLive"`
	BrokerName          string `json:"brokerName"`
}

type cTraderTrader struct {
	CtidTraderAccountID int64 `json:"ctidTraderAccountId"`
	Balance             int64 `json:"balance"`
	MoneyDigits         int   `json:"moneyDigits"`
	// DepositAssetID identifies the account's deposit currency. Resolved to a
	// name through ProtoOAAssetListReq (2112); everything used to be stamped
	// "USD" regardless (audit E-C2).
	DepositAssetID int64 `json:"depositAssetId"`
}

// tradeSide accepts either the string name ("BUY"/"SELL") or the
// ProtoOATradeSide enum integer (BUY=1, SELL=2) that cTrader sometimes
// returns over the JSON Open API. It normalizes to the string name so
// downstream EqualFold comparisons keep working.
type tradeSide string

func (t *tradeSide) UnmarshalJSON(b []byte) error {
	if len(b) > 0 && b[0] == '"' {
		var s string
		if err := json.Unmarshal(b, &s); err != nil {
			return err
		}
		*t = tradeSide(s)
		return nil
	}
	var n int
	if err := json.Unmarshal(b, &n); err != nil {
		return err
	}
	switch n {
	case 1:
		*t = "BUY"
	case 2:
		*t = "SELL"
	}
	return nil
}

// dealStatus accepts either the string name or the ProtoOADealStatus enum
// integer that cTrader returns (FILLED=2, PARTIALLY_FILLED=3). It normalizes to
// the string name so the GetTrades filter keeps working.
type dealStatus string

func (d *dealStatus) UnmarshalJSON(b []byte) error {
	if len(b) > 0 && b[0] == '"' {
		var s string
		if err := json.Unmarshal(b, &s); err != nil {
			return err
		}
		*d = dealStatus(s)
		return nil
	}
	var n int
	if err := json.Unmarshal(b, &n); err != nil {
		return err
	}
	switch n {
	case 2:
		*d = "FILLED"
	case 3:
		*d = "PARTIALLY_FILLED"
	}
	return nil
}

type cTraderPosition struct {
	PositionID int64 `json:"positionId"`
	TradeData  struct {
		SymbolID  int64     `json:"symbolId"`
		Volume    int64     `json:"volume"`
		TradeSide tradeSide `json:"tradeSide"`
	} `json:"tradeData"`
	// cTrader's ProtoOAPosition.price is a double (the actual entry price, e.g.
	// 1.15229), NOT a scaled integer — decode it as float64 and use it directly.
	Price float64 `json:"price"`
	// NOTE: ProtoOAPosition has NO unrealized-PnL field. The connector used to
	// decode "unrealizedNetProfit" here; it never exists on the wire, so every
	// position reported 0 and equity collapsed to the settled balance. uPnL
	// comes from ProtoOAGetPositionUnrealizedPnLReq (2187) instead.
	UsedMargin int64 `json:"usedMargin"`
	// MoneyDigits scales THIS position's money fields (usedMargin). It is not
	// the trader-level moneyDigits: using the account's scale on a position
	// whose own scale differs mis-scales used margin by a power of ten (V-E3).
	MoneyDigits int `json:"moneyDigits"`
}

// cTraderPositionPnL is one entry of ProtoOAGetPositionUnrealizedPnLRes.
// Amounts are scaled by the RESPONSE's moneyDigits, which is independent of
// the trader's own moneyDigits.
type cTraderPositionPnL struct {
	PositionID         int64 `json:"positionId"`
	GrossUnrealizedPnL int64 `json:"grossUnrealizedPnL"`
	NetUnrealizedPnL   int64 `json:"netUnrealizedPnL"`
}

// cTraderAsset is one entry of ProtoOAAssetListRes, used to turn the trader's
// depositAssetId into the account currency.
type cTraderAsset struct {
	AssetID     int64  `json:"assetId"`
	Name        string `json:"name"`
	DisplayName string `json:"displayName"`
}

type cTraderDeal struct {
	DealID             int64      `json:"dealId"`
	OrderID            int64      `json:"orderId"`
	SymbolID           int64      `json:"symbolId"`
	TradeSide          tradeSide  `json:"tradeSide"`
	FilledVolume       int64      `json:"filledVolume"`
	ExecutionPrice     float64    `json:"executionPrice"`
	ExecutionTimestamp int64      `json:"executionTimestamp"`
	Commission         int64      `json:"commission"`
	DealStatus         dealStatus `json:"dealStatus"`
	MoneyDigits        int        `json:"moneyDigits"`
	// BaseToUsdConversionRate prices one unit of the symbol's base asset in
	// dollars at execution, which is what makes a notional comparable across
	// a book quoted in yen, francs and dollars.
	BaseToUsdConversionRate float64 `json:"baseToUsdConversionRate"`
	ClosePositionDetail     *struct {
		GrossProfit int64 `json:"grossProfit"`
		Commission  int64 `json:"commission"`
		Swap        int64 `json:"swap"`
		// Balance is the authoritative account balance AFTER this closing deal
		// (scaled by MoneyDigits). Used to reconstruct the historical equity curve.
		// A POINTER so an absent field and a genuine zero stay
		// distinguishable: the old `Balance == 0` filter dropped every
		// deal that closed the account at exactly 0.00, and the
		// carry-forward builder then drew a flat line at the last
		// positive balance — a blown account rendered as a plateau (E-B).
		Balance     *int64 `json:"balance"`
		MoneyDigits int    `json:"moneyDigits"`
	} `json:"closePositionDetail"`
}

type cTraderSymbol struct {
	SymbolID   int64  `json:"symbolId"`
	SymbolName string `json:"symbolName"`
}

type cTraderBalanceInfo struct {
	Balance         float64
	Equity          float64
	UnrealizedPnL   float64
	MarginUsed      float64
	MarginAvailable float64
	Currency        string
}

// CTrader is a CFD/Forex broker connector using cTrader Open API WebSocket flow.
type CTrader struct {
	clientID     string
	clientSecret string

	tokenMu      sync.RWMutex
	accessToken  string
	refreshToken string

	isLive bool

	httpClient *http.Client
	wsDialer   *websocket.Dialer

	wsLiveURL string
	wsDemoURL string
	authURL   string

	connMu           sync.Mutex
	ws               *websocket.Conn
	appAuthenticated bool
	heartbeatStop    chan struct{}
	writeMu          sync.Mutex

	// requestTimeout bounds one request/response round trip. Overridable so
	// tests can exercise the timeout path without waiting 30 s.
	requestTimeout time.Duration
	// readDeadline is how long the socket may stay silent before the read
	// loop declares it dead. Refreshed by every inbound frame and by the
	// WebSocket pong the heartbeat's ping elicits.
	readDeadline time.Duration
	// rateLimitBackoff is the first wait after a BLOCKED_PAYLOAD_TYPE, then
	// doubled per attempt. Overridable so tests don't sleep for seconds.
	rateLimitBackoff time.Duration
	// maxDealPages and histRequestDelay bound and pace the historical
	// pagination. Fields rather than bare constants so a test can walk to
	// the cap without sitting through 200 throttled round trips.
	maxDealPages     int
	histRequestDelay time.Duration

	pendingMu sync.Mutex
	pending   map[string]chan wsResponse
	msgID     uint64

	accountMu sync.Mutex
	accountID int64
	// pinnedAccountID is the ctidTraderAccountId the connection was created
	// for. Zero leaves the choice to selectCTraderAccount's fallback.
	pinnedAccountID int64
	// accountUnpinned is set when the token grants several live accounts and
	// nothing names the one this connection syncs.
	accountUnpinned atomic.Bool
	// authenticatedAccountID is the account the CURRENT socket has already
	// run ProtoOAAccountAuthReq for. Re-sending it before every request was a
	// free contributor to the per-payload-type rate limit (E-M7). Reset by
	// disconnect/markDisconnected, which is what invalidates the session.
	authenticatedAccountID int64

	symbolMu    sync.RWMutex
	symbolCache map[int64]string
	// symbolMisses memoizes symbol ids the broker could not resolve, so an
	// unknown id costs one request per TTL instead of one per deal (E-M8).
	symbolMisses map[int64]time.Time

	// currencyMu guards the resolved account currency (E-C2).
	currencyMu sync.RWMutex
	currency   string
	// currencyResolveFailed stops us re-requesting the asset list on every
	// balance fetch when the broker will not answer it.
	currencyResolveFailed bool

	// warnMu guards capabilityWarnings, the markers the LAST GetBalance
	// discovered (connector.CapabilityWarner).
	warnMu             sync.Mutex
	capabilityWarnings []string

	// tokenInvalidated is set by the unsolicited
	// ProtoOAAccountsTokenInvalidatedEvent (2147). Once set, the next call
	// fails with an OAuth-shaped error instead of waiting for the following
	// request to be rejected (E-B).
	tokenInvalidated atomic.Bool

	// accessTokenExpiry is when the current access token stops working, from
	// the refresh response's expires_in. Zero = unknown (the connect-time
	// token carries no expiry here).
	accessTokenExpiry time.Time

	tokenPersister TokenPersister

	closed atomic.Bool
}

// NewCTrader creates a new cTrader connector.
// TS-parity credentials:
//   - apiKey = access_token
//   - apiSecret = refresh_token (optional)
//   - passphrase = "demo" to force demo WebSocket endpoint, or the numeric
//     ctidTraderAccountId the connection syncs
//   - CTRADER_CLIENT_ID / CTRADER_CLIENT_SECRET for app auth + refresh flow
func NewCTrader(creds *Credentials) *CTrader {
	clientID := firstNonEmpty(creds.ClientID, os.Getenv("CTRADER_CLIENT_ID"))
	clientSecret := firstNonEmpty(creds.ClientSecret, os.Getenv("CTRADER_CLIENT_SECRET"))
	accessToken := firstNonEmpty(creds.AccessToken, creds.APIKey)
	refreshToken := strings.TrimSpace(creds.APISecret)
	passphrase := strings.TrimSpace(creds.Passphrase)
	isLive := !strings.EqualFold(passphrase, "demo")
	pinned, err := strconv.ParseInt(passphrase, 10, 64)
	if err != nil || pinned < 0 {
		pinned = 0
	}

	return &CTrader{
		clientID:        clientID,
		clientSecret:    clientSecret,
		accessToken:     accessToken,
		refreshToken:    refreshToken,
		isLive:          isLive,
		pinnedAccountID: pinned,
		httpClient:      &http.Client{Timeout: 30 * time.Second},
		wsDialer: &websocket.Dialer{
			HandshakeTimeout: 10 * time.Second,
		},
		wsLiveURL:    ctraderWSLiveURL,
		wsDemoURL:    ctraderWSDemoURL,
		authURL:      ctraderAuthURL,
		pending:      make(map[string]chan wsResponse),
		symbolCache:  make(map[int64]string),
		symbolMisses: make(map[int64]time.Time),
	}
}

func (c *CTrader) Exchange() string { return "ctrader" }

// Close releases the WebSocket, its read loop and its heartbeat. Implements
// io.Closer for the connector cache, which calls it when the instance is
// evicted, expires, or is replaced (E-H6). Idempotent, and safe to call on an
// instance that never connected.
func (c *CTrader) Close() error {
	if c.closed.Swap(true) {
		return nil
	}
	c.disconnect(errors.New("cTrader connector closed"))
	return nil
}

// SetTokenPersister sets a callback to persist refreshed OAuth tokens to DB.
func (c *CTrader) SetTokenPersister(persister TokenPersister) {
	c.tokenPersister = persister
}

// CurrentCredentials implements OAuthCredentialSource: the tokens this
// instance is actually holding, which differ from the ones it was built with
// as soon as a refresh has happened (E-H4).
func (c *CTrader) CurrentCredentials() (string, string) {
	c.tokenMu.RLock()
	defer c.tokenMu.RUnlock()
	return c.accessToken, c.refreshToken
}

// DetectIsPaper reports whether the connection's account is a cTrader demo
// account, from the authoritative per-account IsLive flag in the account list.
// c.isLive is only a WS-routing seed derived from the passphrase, and OAuth
// connections never carry "demo" there — so it defaults to live and every OAuth
// demo account was classified real, presenting a resettable demo balance as a
// verifiable track record. The selection is ensureAccountID's, so the flag
// matches the account that actually gets synced.
func (c *CTrader) DetectIsPaper(ctx context.Context) (bool, error) {
	accounts, err := c.getAccounts(ctx)
	if err != nil {
		return false, err
	}
	selected, _, err := selectCTraderAccount(accounts, c.pinnedAccountID)
	if err != nil {
		return false, err
	}
	return !selected.IsLive, nil
}

// errCTraderPinnedAccountNotGranted: the connection names an account its token
// no longer grants. Falling back to another account would splice a different
// account's balance into this connection's history.
var errCTraderPinnedAccountNotGranted = errors.New("the cTrader account this connection was created for is not granted by its token; the account must be re-authorized")

// selectCTraderAccount picks the account a connection syncs: the pinned one
// when the connection names it, otherwise the first live account the token
// grants, or the first account when none is live. unpinned reports a choice
// made between several live accounts with nothing to say which one was meant.
func selectCTraderAccount(accounts []cTraderAccount, pinned int64) (selected cTraderAccount, unpinned bool, err error) {
	if len(accounts) == 0 {
		return cTraderAccount{}, false, fmt.Errorf("no cTrader accounts found")
	}
	if pinned != 0 {
		for _, acct := range accounts {
			if acct.CtidTraderAccountID == pinned {
				return acct, false, nil
			}
		}
		return cTraderAccount{}, false, errCTraderPinnedAccountNotGranted
	}
	selected = accounts[0]
	live := 0
	for _, acct := range accounts {
		if !acct.IsLive {
			continue
		}
		if live == 0 {
			selected = acct
		}
		live++
	}
	return selected, live > 1, nil
}

func (c *CTrader) TestConnection(ctx context.Context) error {
	// Try connecting with the current access token first.
	// Only refresh if the connection fails with a token error.
	accounts, err := c.getAccounts(ctx)
	if err != nil {
		return err
	}
	if len(accounts) == 0 {
		return fmt.Errorf("no cTrader accounts found")
	}
	return nil
}

func (c *CTrader) GetBalance(ctx context.Context) (*Balance, error) {
	accountID, err := c.ensureAccountID(ctx)
	if err != nil {
		return nil, err
	}

	info, err := c.getAccountBalance(ctx, accountID)
	if err != nil {
		return nil, err
	}

	return &Balance{
		Available:     info.MarginAvailable,
		Equity:        info.Equity,
		UnrealizedPnL: info.UnrealizedPnL,
		Currency:      info.Currency,
	}, nil
}

func (c *CTrader) GetPositions(ctx context.Context) ([]*Position, error) {
	accountID, err := c.ensureAccountID(ctx)
	if err != nil {
		return nil, err
	}

	rawPositions, err := c.getPositionsRaw(ctx, accountID)
	if err != nil {
		return nil, err
	}

	// uPnL is a separate request (2187); the reconcile payload carries none.
	// A failure here is not fatal for the position LIST — the sizes and entry
	// prices are still correct — so it degrades to 0 with the warning recorded.
	pnlByPosition, _, _ := c.positionUnrealizedPnL(ctx, accountID, rawPositions)

	positions := make([]*Position, 0, len(rawPositions))
	for _, p := range rawPositions {
		symbol := c.getSymbolName(ctx, p.TradeData.SymbolID, accountID)
		side := "long"
		if strings.EqualFold(string(p.TradeData.TradeSide), "SELL") {
			side = "short"
		}

		positions = append(positions, &Position{
			Symbol:        symbol,
			Side:          side,
			Size:          float64(p.TradeData.Volume) / 100.0,
			EntryPrice:    p.Price,
			MarkPrice:     0,
			UnrealizedPnL: pnlByPosition[p.PositionID],
			MarketType:    MarketCFD,
		})
	}

	return positions, nil
}

func (c *CTrader) GetTrades(ctx context.Context, start, end time.Time) ([]*Trade, error) {
	accountID, err := c.ensureAccountID(ctx)
	if err != nil {
		return nil, err
	}

	deals, err := c.getAllDeals(ctx, accountID, start, end)
	if err != nil {
		return nil, err
	}

	trades := make([]*Trade, 0, len(deals))
	for _, d := range deals {
		if !d.filled() {
			continue
		}

		side := "buy"
		if strings.EqualFold(string(d.TradeSide), "SELL") {
			side = "sell"
		}

		trades = append(trades, &Trade{
			ID:          strconv.FormatInt(d.DealID, 10),
			Symbol:      c.getSymbolName(ctx, d.SymbolID, accountID),
			Side:        side,
			Price:       d.ExecutionPrice,
			Quantity:    d.units(),
			USDNotional: d.usdNotional(),
			Fee:         d.fee(),
			FeeCurrency: c.accountCurrency(),
			RealizedPnL: d.realizedPnL(),
			Timestamp:   time.UnixMilli(d.ExecutionTimestamp).UTC(),
			// The bucket the equity is filed under (primaryMarketType). Gold
			// and indices used to land in forex or commodities, markets that
			// then carried trades and no equity.
			MarketType: MarketCFD,
		})
	}

	return trades, nil
}

func (d cTraderDeal) filled() bool {
	return d.DealStatus == "FILLED" || d.DealStatus == "PARTIALLY_FILLED"
}

// units is the filled size in base-asset units. Volumes do not follow
// moneyDigits: cTrader scales them by a fixed 1/100 (E-M6).
func (d cTraderDeal) units() float64 {
	return float64(d.FilledVolume) / 100.0
}

// usdNotional is the dollar value traded, 0 when the deal does not carry the
// rate that prices it.
func (d cTraderDeal) usdNotional() float64 {
	if d.BaseToUsdConversionRate <= 0 {
		return 0
	}
	return d.units() * d.BaseToUsdConversionRate
}

func (d cTraderDeal) notional() float64 {
	if v := d.usdNotional(); v > 0 {
		return v
	}
	return d.units() * d.ExecutionPrice
}

// fee is the commission as the positive cost every other venue reports;
// cTrader signs a charge negative.
func (d cTraderDeal) fee() float64 {
	return -float64(d.Commission) / ctraderMoneyDivisor(d.MoneyDigits)
}

// realizedPnL is a closing deal's net result. Gross profit, swap and
// commission all arrive signed from the account's side, so they add; the
// closing balance moves by exactly that sum.
func (d cTraderDeal) realizedPnL() float64 {
	cpd := d.ClosePositionDetail
	if cpd == nil {
		return 0
	}
	md := cpd.MoneyDigits
	if md == 0 {
		md = d.MoneyDigits
	}
	return float64(cpd.GrossProfit+cpd.Swap+cpd.Commission) / ctraderMoneyDivisor(md)
}

func (c *CTrader) ensureState() {
	if c.httpClient == nil {
		c.httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	if c.wsDialer == nil {
		c.wsDialer = &websocket.Dialer{HandshakeTimeout: 10 * time.Second}
	}
	if c.wsLiveURL == "" {
		c.wsLiveURL = ctraderWSLiveURL
	}
	if c.wsDemoURL == "" {
		c.wsDemoURL = ctraderWSDemoURL
	}
	if c.authURL == "" {
		c.authURL = ctraderAuthURL
	}
	if c.pending == nil {
		c.pending = make(map[string]chan wsResponse)
	}
	if c.symbolCache == nil {
		c.symbolCache = make(map[int64]string)
	}
	if c.symbolMisses == nil {
		c.symbolMisses = make(map[int64]time.Time)
	}
	if c.requestTimeout <= 0 {
		c.requestTimeout = ctraderRequestTimeout
	}
	if c.readDeadline <= 0 {
		c.readDeadline = ctraderReadDeadline
	}
	if c.rateLimitBackoff <= 0 {
		c.rateLimitBackoff = ctraderRateLimitBaseBackoff
	}
	if c.maxDealPages <= 0 {
		c.maxDealPages = ctraderMaxDealPages
	}
	if c.histRequestDelay <= 0 {
		c.histRequestDelay = ctraderHistRequestDelay
	}
}

func (c *CTrader) currentAccessToken() string {
	c.tokenMu.RLock()
	defer c.tokenMu.RUnlock()
	return c.accessToken
}

// currentRefreshToken reads the refresh token under the token lock. The
// refresh path writes it, so every reader must take the lock (E-M2).
func (c *CTrader) currentRefreshToken() string {
	c.tokenMu.RLock()
	defer c.tokenMu.RUnlock()
	return strings.TrimSpace(c.refreshToken)
}

func (c *CTrader) ensureAccountID(ctx context.Context) (int64, error) {
	c.accountMu.Lock()
	if c.accountID != 0 {
		id := c.accountID
		c.accountMu.Unlock()
		return id, nil
	}
	c.accountMu.Unlock()

	accounts, err := c.getAccounts(ctx)
	if err != nil {
		return 0, err
	}
	selected, unpinned, err := selectCTraderAccount(accounts, c.pinnedAccountID)
	if err != nil {
		return 0, err
	}
	c.accountUnpinned.Store(unpinned)

	// Route the WS endpoint by the selected account's live/demo flag. c.isLive is
	// initially derived from the passphrase, but OAuth connections never carry
	// "demo" there, so it defaults to true — a demo account then gets queried on
	// the live endpoint and cTrader rejects account auth with CANT_ROUTE_REQUEST.
	// When the account's endpoint differs from the one we're connected to, switch
	// and drop the socket so the next connect dials the matching host (getAccounts
	// works on either endpoint, so listing accounts first is safe).
	c.connMu.Lock()
	endpointMismatch := selected.IsLive != c.isLive
	if endpointMismatch {
		c.isLive = selected.IsLive
	}
	c.connMu.Unlock()
	if endpointMismatch {
		mode := "demo"
		if selected.IsLive {
			mode = "live"
		}
		c.disconnect(fmt.Errorf("cTrader: routing to %s endpoint for account %d", mode, selected.CtidTraderAccountID))
	}

	c.accountMu.Lock()
	c.accountID = selected.CtidTraderAccountID
	c.accountMu.Unlock()

	return selected.CtidTraderAccountID, nil
}

func (c *CTrader) getAccounts(ctx context.Context) ([]cTraderAccount, error) {
	if err := c.ensureConnected(ctx); err != nil {
		return nil, err
	}
	if err := c.authenticateApp(ctx); err != nil {
		return nil, err
	}

	raw, err := c.sendWithTokenRefresh(ctx, func() (json.RawMessage, error) {
		return c.sendMessage(
			ctx,
			ctraderPayloadGetAccountsReq,
			map[string]any{"accessToken": c.currentAccessToken()},
			ctraderPayloadGetAccountsRes,
		)
	})
	if err != nil {
		return nil, err
	}

	var resp struct {
		Accounts []cTraderAccount `json:"ctidTraderAccount"`
	}
	if err := decodeRawPayload(raw, &resp); err != nil {
		return nil, err
	}
	if resp.Accounts == nil {
		return []cTraderAccount{}, nil
	}
	return resp.Accounts, nil
}

func (c *CTrader) getAccountBalance(ctx context.Context, accountID int64) (*cTraderBalanceInfo, error) {
	c.resetCapabilityWarnings()
	if c.accountUnpinned.Load() {
		c.addCapabilityWarning("multiple_live_accounts_unpinned")
	}

	trader, err := c.getTraderInfo(ctx, accountID)
	if err != nil {
		return nil, err
	}

	moneyDigits := trader.MoneyDigits
	if moneyDigits <= 0 {
		moneyDigits = 2
	}
	divisor := math.Pow10(moneyDigits)

	positions, err := c.getPositionsRaw(ctx, accountID)
	if err != nil {
		return nil, err
	}

	// E-C1: unrealized PnL is a request of its own (2187). Reading it off
	// ProtoOAPosition returned 0 forever, so equity WAS the settled balance:
	// a step curve for anyone holding overnight, and understated drawdowns.
	_, unrealizedPnL, pnlErr := c.positionUnrealizedPnL(ctx, accountID, positions)
	if pnlErr != nil {
		// Deliberately not fatal: refusing the whole snapshot would leave a
		// hole in the equity curve, which analytics reads as a gap. The
		// warning rides to sync_statuses.errorMessage under the "warning:"
		// prefix so a broker that cannot answer 2187 is visible instead of
		// silently publishing balance-only equity.
		c.addCapabilityWarning("unrealized_pnl_unavailable")
	}

	marginUsed := 0.0
	for _, p := range positions {
		// V-E3: usedMargin is scaled by the POSITION's moneyDigits, not the
		// trader's. Using the account scale mis-scaled margin by 10^Δ.
		posDivisor := divisor
		if p.MoneyDigits > 0 {
			posDivisor = math.Pow10(p.MoneyDigits)
		}
		if p.UsedMargin > 0 {
			marginUsed += float64(p.UsedMargin) / posDivisor
		}
	}

	balance := float64(trader.Balance) / divisor
	equity := balance + unrealizedPnL

	currency := c.resolveAccountCurrency(ctx, accountID, trader.DepositAssetID)
	if currency != "" && currency != "USD" {
		// E-C2: no FX conversion is performed anywhere downstream, so a
		// non-USD account is reported in its own units. Say so instead of
		// stamping "USD" on EUR figures.
		c.addCapabilityWarning("account_currency_" + strings.ToLower(currency))
	}
	if currency == "" {
		currency = "USD"
	}

	return &cTraderBalanceInfo{
		Balance:         balance,
		Equity:          equity,
		UnrealizedPnL:   unrealizedPnL,
		MarginUsed:      marginUsed,
		MarginAvailable: equity - marginUsed,
		Currency:        currency,
	}, nil
}

// positionUnrealizedPnL asks cTrader for the live unrealized PnL of the open
// positions (ProtoOAGetPositionUnrealizedPnLReq, 2187) and returns it per
// positionId plus the total, both already scaled by the RESPONSE's own
// moneyDigits. No open positions = no request and a zero total.
func (c *CTrader) positionUnrealizedPnL(ctx context.Context, accountID int64, positions []cTraderPosition) (map[int64]float64, float64, error) {
	if len(positions) == 0 {
		return map[int64]float64{}, 0, nil
	}
	if err := c.authenticateAccount(ctx, accountID); err != nil {
		return map[int64]float64{}, 0, err
	}

	raw, err := c.sendMessage(
		ctx,
		ctraderPayloadUnrealizedPnLReq,
		map[string]any{"ctidTraderAccountId": accountID},
		ctraderPayloadUnrealizedPnLRes,
	)
	if err != nil {
		return map[int64]float64{}, 0, err
	}

	var resp struct {
		PositionUnrealizedPnL []cTraderPositionPnL `json:"positionUnrealizedPnL"`
		MoneyDigits           int                  `json:"moneyDigits"`
	}
	if err := decodeRawPayload(raw, &resp); err != nil {
		return map[int64]float64{}, 0, err
	}

	divisor := ctraderMoneyDivisor(resp.MoneyDigits)
	byPosition := make(map[int64]float64, len(resp.PositionUnrealizedPnL))
	total := 0.0
	for _, p := range resp.PositionUnrealizedPnL {
		v := float64(p.NetUnrealizedPnL) / divisor
		byPosition[p.PositionID] = v
		total += v
	}
	return byPosition, total, nil
}

// resolveAccountCurrency turns the trader's depositAssetId into a currency
// name via ProtoOAAssetListReq (2112), cached for the connector's lifetime.
// Returns "" when it cannot be resolved — callers then keep the legacy "USD"
// label rather than inventing one.
func (c *CTrader) resolveAccountCurrency(ctx context.Context, accountID, depositAssetID int64) string {
	c.currencyMu.RLock()
	cached, failed := c.currency, c.currencyResolveFailed
	c.currencyMu.RUnlock()
	if cached != "" {
		return cached
	}
	if failed || depositAssetID <= 0 {
		return ""
	}

	giveUp := func() string {
		c.currencyMu.Lock()
		c.currencyResolveFailed = true
		c.currencyMu.Unlock()
		return ""
	}

	if err := c.authenticateAccount(ctx, accountID); err != nil {
		return giveUp()
	}
	raw, err := c.sendMessage(
		ctx,
		ctraderPayloadAssetListReq,
		map[string]any{"ctidTraderAccountId": accountID},
		ctraderPayloadAssetListRes,
	)
	if err != nil {
		return giveUp()
	}
	var resp struct {
		Asset []cTraderAsset `json:"asset"`
	}
	if err := decodeRawPayload(raw, &resp); err != nil {
		return giveUp()
	}
	for _, a := range resp.Asset {
		if a.AssetID != depositAssetID {
			continue
		}
		name := strings.ToUpper(strings.TrimSpace(firstNonEmpty(a.Name, a.DisplayName)))
		if name == "" {
			return giveUp()
		}
		c.currencyMu.Lock()
		c.currency = name
		c.currencyMu.Unlock()
		return name
	}
	return giveUp()
}

// accountCurrency returns the resolved account currency, or "USD" when the
// asset list has not been read yet. Money-labelling only; no conversion.
func (c *CTrader) accountCurrency() string {
	c.currencyMu.RLock()
	defer c.currencyMu.RUnlock()
	if c.currency == "" {
		return "USD"
	}
	return c.currency
}

// CapabilityWarnings implements connector.CapabilityWarner: markers the last
// GetBalance discovered (uPnL unreadable, non-USD account).
func (c *CTrader) CapabilityWarnings() []string {
	c.warnMu.Lock()
	defer c.warnMu.Unlock()
	if len(c.capabilityWarnings) == 0 {
		return nil
	}
	out := make([]string, len(c.capabilityWarnings))
	copy(out, c.capabilityWarnings)
	return out
}

func (c *CTrader) addCapabilityWarning(marker string) {
	c.warnMu.Lock()
	defer c.warnMu.Unlock()
	for _, w := range c.capabilityWarnings {
		if w == marker {
			return
		}
	}
	c.capabilityWarnings = append(c.capabilityWarnings, marker)
}

func (c *CTrader) resetCapabilityWarnings() {
	c.warnMu.Lock()
	c.capabilityWarnings = nil
	c.warnMu.Unlock()
}

func (c *CTrader) getTraderInfo(ctx context.Context, accountID int64) (*cTraderTrader, error) {
	if err := c.authenticateAccount(ctx, accountID); err != nil {
		return nil, err
	}

	raw, err := c.sendMessage(
		ctx,
		ctraderPayloadTraderReq,
		map[string]any{"ctidTraderAccountId": accountID},
		ctraderPayloadTraderRes,
	)
	if err != nil {
		return nil, err
	}

	var resp struct {
		Trader cTraderTrader `json:"trader"`
	}
	if err := decodeRawPayload(raw, &resp); err != nil {
		return nil, err
	}
	if resp.Trader.CtidTraderAccountID == 0 {
		resp.Trader.CtidTraderAccountID = accountID
	}

	return &resp.Trader, nil
}

func (c *CTrader) getPositionsRaw(ctx context.Context, accountID int64) ([]cTraderPosition, error) {
	if err := c.authenticateAccount(ctx, accountID); err != nil {
		return nil, err
	}

	raw, err := c.sendMessage(
		ctx,
		ctraderPayloadReconcileReq,
		map[string]any{"ctidTraderAccountId": accountID},
		ctraderPayloadReconcileRes,
	)
	if err != nil {
		return nil, err
	}

	var resp struct {
		Position []cTraderPosition `json:"position"`
	}
	if err := decodeRawPayload(raw, &resp); err != nil {
		return nil, err
	}
	if resp.Position == nil {
		return []cTraderPosition{}, nil
	}
	return resp.Position, nil
}

func (c *CTrader) getSymbolName(ctx context.Context, symbolID, accountID int64) string {
	if symbolID <= 0 {
		return ""
	}
	c.ensureState()

	c.symbolMu.RLock()
	name, ok := c.symbolCache[symbolID]
	missedAt, missed := c.symbolMisses[symbolID]
	c.symbolMu.RUnlock()
	if ok && name != "" {
		return name
	}
	// E-M8: only successful lookups were remembered, so a symbol the broker
	// will not resolve cost one request per deal — hundreds per sync, all of
	// them feeding the per-payload-type rate limit that then blocks the
	// history walk. Remember the miss too, briefly: short enough that a newly
	// listed symbol resolves on the next sync.
	if missed && time.Since(missedAt) < ctraderSymbolMissTTL {
		return fmt.Sprintf("SYMBOL_%d", symbolID)
	}

	resolved, err := c.getSymbolByID(ctx, symbolID, accountID)
	if err == nil && resolved != "" {
		return resolved
	}

	c.symbolMu.Lock()
	c.symbolMisses[symbolID] = time.Now()
	c.symbolMu.Unlock()

	return fmt.Sprintf("SYMBOL_%d", symbolID)
}

func (c *CTrader) getSymbolByID(ctx context.Context, symbolID, accountID int64) (string, error) {
	if err := c.authenticateAccount(ctx, accountID); err != nil {
		return "", err
	}

	raw, err := c.sendMessage(
		ctx,
		ctraderPayloadSymbolByIDReq,
		map[string]any{
			"ctidTraderAccountId": accountID,
			"symbolId":            []int64{symbolID},
		},
		ctraderPayloadSymbolByIDRes,
	)
	if err != nil {
		return "", err
	}

	var resp struct {
		Symbols []cTraderSymbol `json:"symbol"`
	}
	if err := decodeRawPayload(raw, &resp); err != nil {
		return "", err
	}
	if len(resp.Symbols) == 0 {
		return "", nil
	}

	name := strings.TrimSpace(resp.Symbols[0].SymbolName)
	if name != "" {
		c.symbolMu.Lock()
		c.symbolCache[symbolID] = name
		c.symbolMu.Unlock()
	}

	return name, nil
}

func (c *CTrader) authenticateApp(ctx context.Context) error {
	c.ensureState()

	c.connMu.Lock()
	alreadyAuthed := c.appAuthenticated
	c.connMu.Unlock()
	if alreadyAuthed {
		return nil
	}

	if strings.TrimSpace(c.clientID) == "" || strings.TrimSpace(c.clientSecret) == "" {
		return fmt.Errorf("cTrader requires CTRADER_CLIENT_ID and CTRADER_CLIENT_SECRET environment variables")
	}

	if _, err := c.sendMessage(
		ctx,
		ctraderPayloadAppAuthReq,
		map[string]any{
			"clientId":     strings.TrimSpace(c.clientID),
			"clientSecret": strings.TrimSpace(c.clientSecret),
		},
		ctraderPayloadAppAuthRes,
	); err != nil {
		return err
	}

	c.connMu.Lock()
	c.appAuthenticated = true
	c.connMu.Unlock()
	return nil
}

func (c *CTrader) authenticateAccount(ctx context.Context, accountID int64) error {
	if err := c.ensureConnected(ctx); err != nil {
		return err
	}
	if err := c.authenticateApp(ctx); err != nil {
		return err
	}

	// E-M7: the account session lives on the socket, so re-sending
	// ProtoOAAccountAuthReq before every single request bought nothing and
	// fed the per-payload-type rate limit. Any teardown (disconnect,
	// markDisconnected, endpoint switch) clears the memo.
	c.connMu.Lock()
	alreadyAuthed := c.authenticatedAccountID == accountID
	c.connMu.Unlock()
	if alreadyAuthed {
		return nil
	}

	_, err := c.sendWithTokenRefresh(ctx, func() (json.RawMessage, error) {
		return c.sendMessage(
			ctx,
			ctraderPayloadAccountAuthReq,
			map[string]any{
				"ctidTraderAccountId": accountID,
				"accessToken":         c.currentAccessToken(),
			},
			ctraderPayloadAccountAuthRes,
		)
	})
	if err != nil {
		return err
	}

	c.connMu.Lock()
	c.authenticatedAccountID = accountID
	c.connMu.Unlock()
	return nil
}

func (c *CTrader) sendWithTokenRefresh(ctx context.Context, call func() (json.RawMessage, error)) (json.RawMessage, error) {
	// E-B: refresh a token we KNOW is about to expire instead of waiting for
	// the request it breaks. Only armed once a previous refresh told us the
	// lifetime — the connect-time token arrives without one.
	if c.needsProactiveRefresh() {
		if err := c.refreshAndReconnect(ctx, "cTrader reconnect after proactive token refresh"); err != nil {
			return nil, err
		}
	}

	raw, err := call()
	if err == nil {
		return raw, nil
	}

	// ALREADY_LOGGED_IN: previous WS session still active — just reconnect, no token refresh.
	if isAlreadyLoggedIn(err) {
		c.disconnect(errors.New("cTrader reconnect: ALREADY_LOGGED_IN"))
		if err := c.ensureConnected(ctx); err != nil {
			return nil, err
		}
		if err := c.authenticateApp(ctx); err != nil {
			return nil, err
		}
		return call()
	}

	// E-M2: read the refresh token through the accessor. Reading the field
	// bare raced with refreshAccessToken's write on any shared instance.
	if !isAccessTokenInvalid(err) || c.currentRefreshToken() == "" {
		return nil, err
	}

	if err := c.refreshAndReconnect(ctx, "cTrader reconnect after token refresh"); err != nil {
		return nil, err
	}
	return call()
}

// refreshAndReconnect rotates the token and opens an app-authenticated session
// for it. The proactive path used to stop at the disconnect: the next request
// then went out on a fresh socket that had never sent the application auth
// cTrader requires first on every connection, and failed.
func (c *CTrader) refreshAndReconnect(ctx context.Context, reason string) error {
	if err := c.refreshAccessToken(ctx); err != nil {
		return err
	}
	c.dropRefreshedSession(errors.New(reason))
	if err := c.ensureConnected(ctx); err != nil {
		return err
	}
	return c.authenticateApp(ctx)
}

// dropRefreshedSession closes the socket the old token authenticated. cTrader
// answers a refresh by pushing ProtoOAAccountsTokenInvalidatedEvent onto that
// socket; the flag is cleared only once disconnect has run, because from then
// on the read loop ignores anything the old socket still delivers.
func (c *CTrader) dropRefreshedSession(cause error) {
	c.disconnect(cause)
	c.tokenInvalidated.Store(false)
}

func (c *CTrader) refreshAccessToken(ctx context.Context) error {
	c.ensureState()

	c.tokenMu.Lock()
	defer c.tokenMu.Unlock()

	if strings.TrimSpace(c.refreshToken) == "" {
		return fmt.Errorf("missing refresh token")
	}
	if strings.TrimSpace(c.clientID) == "" || strings.TrimSpace(c.clientSecret) == "" {
		return fmt.Errorf("missing cTrader client credentials (set CTRADER_CLIENT_ID/CTRADER_CLIENT_SECRET)")
	}

	form := url.Values{}
	form.Set("grant_type", "refresh_token")
	form.Set("refresh_token", strings.TrimSpace(c.refreshToken))
	form.Set("client_id", strings.TrimSpace(c.clientID))
	form.Set("client_secret", strings.TrimSpace(c.clientSecret))

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(c.authURL, "/")+"/token", strings.NewReader(form.Encode()))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}

	// CONN-AUDIT-001: bounded read.
	body, err := ReadCappedBody(resp.Body, DefaultMaxResponseBytes)
	if err != nil {
		return err
	}
	if resp.StatusCode == http.StatusTooManyRequests {
		return fmt.Errorf("cTrader token refresh rate-limited (429), retry later")
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("token refresh failed (HTTP %d): %s", resp.StatusCode, TruncatedBody([]byte(strings.TrimSpace(string(body)))))
	}

	var tokenResp struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
		ExpiresIn    int    `json:"expires_in"`
		// cTrader error shape (HTTP 200 with error body)
		ErrorCode   string `json:"errorCode"`
		Description string `json:"description"`
		// Standard OAuth2 error shape
		OAuthError string `json:"error"`
	}
	if err := json.Unmarshal(body, &tokenResp); err != nil {
		return err
	}
	if tokenResp.ErrorCode != "" {
		return fmt.Errorf("token refresh rejected: %s - %s", tokenResp.ErrorCode, tokenResp.Description)
	}
	if tokenResp.OAuthError != "" {
		return fmt.Errorf("token refresh rejected: %s", tokenResp.OAuthError)
	}
	if strings.TrimSpace(tokenResp.AccessToken) == "" {
		return fmt.Errorf("token refresh response missing access_token")
	}

	newAccess := strings.TrimSpace(tokenResp.AccessToken)
	newRefresh := c.refreshToken
	if rotated := strings.TrimSpace(tokenResp.RefreshToken); rotated != "" {
		newRefresh = rotated
	}

	// The broker has already rotated: the refresh token we arrived with is
	// dead from this instant, whatever happens next. So the new pair goes to
	// RAM unconditionally — restoring the old one would guarantee failure —
	// and the DB write is what decides whether the connection survives a
	// restart.
	c.accessToken = newAccess
	c.refreshToken = newRefresh
	if tokenResp.ExpiresIn > 0 {
		c.accessTokenExpiry = time.Now().Add(time.Duration(tokenResp.ExpiresIn) * time.Second)
	}

	// Persist refreshed tokens to DB SYNCHRONOUSLY, with the error checked.
	// cTrader rotates single-use refresh tokens (each refresh invalidates the
	// previous one), so the new token MUST be durably saved before we return.
	// The previous fire-and-forget goroutine with a swallowed error lost the
	// rotated token whenever the persist failed or the process moved on/restarted
	// before it landed, leaving only the now-consumed token in the DB — every
	// subsequent refresh then failed with ACCESS_DENIED, bricking the connection.
	//
	// E-H3: one attempt was not enough. A pool hiccup, a failover, an UPDATE
	// that matched no row — any of them silently traded a working connection
	// for a dead one. Retry a few times before declaring the loss, and make
	// the failure loud: the returned error is what the sync layer logs under
	// "sync: OAuth refresh failed" and stores as needs-reauth.
	if c.tokenPersister != nil {
		var persistErr error
		for attempt := 0; attempt < ctraderPersistAttempts; attempt++ {
			if attempt > 0 {
				time.Sleep(time.Duration(attempt) * ctraderPersistBackoff)
			}
			persistCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			persistErr = c.tokenPersister(persistCtx, newAccess, newRefresh)
			cancel()
			if persistErr == nil {
				return nil
			}
		}
		return fmt.Errorf("persist rotated cTrader tokens after %d attempts (the stored refresh token is now dead; the connection must be re-authorized): %w",
			ctraderPersistAttempts, persistErr)
	}

	return nil
}

// needsProactiveRefresh reports whether the access token is close enough to
// its known expiry that refreshing now beats being rejected mid-sync. Zero
// expiry (the connect-time token, whose lifetime we never learn) means no.
func (c *CTrader) needsProactiveRefresh() bool {
	c.tokenMu.RLock()
	defer c.tokenMu.RUnlock()
	if c.accessTokenExpiry.IsZero() || c.refreshToken == "" {
		return false
	}
	return time.Until(c.accessTokenExpiry) < ctraderProactiveRefreshWindow
}

func (c *CTrader) ensureConnected(ctx context.Context) error {
	c.ensureState()

	c.connMu.Lock()
	if c.ws != nil {
		c.connMu.Unlock()
		return nil
	}
	endpoint := c.wsLiveURL
	if !c.isLive {
		endpoint = c.wsDemoURL
	}
	c.connMu.Unlock()

	ws, _, err := c.wsDialer.DialContext(ctx, endpoint, nil)
	if err != nil {
		return err
	}

	// A silent socket must eventually fail a read; the pong the heartbeat's
	// ping elicits is what keeps a healthy one alive (E-H2).
	deadline := c.readDeadline
	_ = ws.SetReadDeadline(time.Now().Add(deadline))
	ws.SetPongHandler(func(string) error {
		return ws.SetReadDeadline(time.Now().Add(deadline))
	})

	c.connMu.Lock()
	if c.ws != nil {
		c.connMu.Unlock()
		_ = ws.Close()
		return nil
	}
	c.ws = ws
	c.appAuthenticated = false
	c.authenticatedAccountID = 0
	stop := make(chan struct{})
	c.heartbeatStop = stop
	c.connMu.Unlock()

	go c.readLoop(ws)
	go c.heartbeatLoop(ws, stop)
	return nil
}

func (c *CTrader) heartbeatLoop(ws *websocket.Conn, stop <-chan struct{}) {
	ticker := time.NewTicker(ctraderHeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			if err := c.writeMessage(ws, wsOutboundMessage{
				PayloadType: ctraderPayloadHeartbeatEvent,
				Payload:     map[string]any{},
			}); err != nil {
				c.markDisconnected(ws, err)
				return
			}
			// The application heartbeat proves nothing about the socket: a
			// half-open TCP connection swallows writes into the send buffer
			// for minutes. The WebSocket ping does — its pong is what pushes
			// the read deadline back (E-H2).
			c.writeMu.Lock()
			pingErr := ws.WriteControl(websocket.PingMessage, nil, time.Now().Add(5*time.Second))
			c.writeMu.Unlock()
			if pingErr != nil && !errors.Is(pingErr, websocket.ErrCloseSent) {
				c.markDisconnected(ws, pingErr)
				return
			}
		}
	}
}

func (c *CTrader) readLoop(ws *websocket.Conn) {
	for {
		_, data, err := ws.ReadMessage()
		if err != nil {
			c.markDisconnected(ws, err)
			return
		}
		// Any inbound frame is proof of life.
		_ = ws.SetReadDeadline(time.Now().Add(c.readDeadline))

		var msg wsInboundMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			continue
		}
		if msg.PayloadType == ctraderPayloadHeartbeatEvent {
			continue
		}
		// E-B: the token-invalidated event is PUSHED without a clientMsgId
		// (user revoked the app, or the broker killed the token). Dropping it
		// as "no waiter" meant the next request failed with whatever cTrader
		// answered next; flagging it here makes the very next call surface an
		// OAuth-shaped error the sync layer can classify as reauth_required.
		if msg.PayloadType == ctraderPayloadTokenInvalidatedEvent {
			c.invalidateToken(ws)
			continue
		}
		if msg.ClientMsgID == "" {
			continue
		}

		c.pendingMu.Lock()
		respCh := c.pending[msg.ClientMsgID]
		c.pendingMu.Unlock()
		if respCh == nil {
			continue
		}

		// ProtoOAErrorRes (2142) and the common-layer ProtoErrorRes (50) carry
		// the same errorCode/description shape. Treating 50 as an ordinary
		// payload turned a described failure into "unexpected cTrader payload
		// type 50" and threw the reason away (E-B).
		if msg.PayloadType == ctraderPayloadErrorRes || msg.PayloadType == ctraderPayloadCommonErrorRes {
			// CONN-007: use a short timeout instead of an unconditional
			// `default:` drop. The respCh is a 1-buffered channel per
			// request, so this only backs off when a duplicate response
			// or a slow consumer already has the slot.
			deliverCTraderResponse(respCh, wsResponse{err: ctraderErrorFromPayload(msg.Payload)})
			continue
		}

		deliverCTraderResponse(respCh, wsResponse{payloadType: msg.PayloadType, payload: msg.Payload})
	}
}

// invalidateToken records ProtoOAAccountsTokenInvalidatedEvent unless it came
// in on a socket already replaced. cTrader also pushes it when this instance
// refreshes the token itself, and honouring that late copy failed every call
// made with the fresh token as needing re-authorization.
func (c *CTrader) invalidateToken(ws *websocket.Conn) {
	c.connMu.Lock()
	current := c.ws == ws
	if current {
		c.tokenInvalidated.Store(true)
	}
	c.connMu.Unlock()
	if current {
		c.failPending(errors.New(ctraderTokenInvalidatedError))
	}
}

// ctraderErrorFromPayload turns an error payload into an error, keeping the
// broker's retry hint when the failure is a rate limit or a maintenance
// window so the paginated walks can honour it (V-E1).
func ctraderErrorFromPayload(raw json.RawMessage) error {
	var payload cTraderErrorPayload
	_ = json.Unmarshal(raw, &payload)

	msg := "cTrader unknown error"
	if payload.ErrorCode != "" {
		msg = fmt.Sprintf("cTrader error %s: %s", payload.ErrorCode, payload.Description)
	}

	hint := payload.RetryAfter
	if hint == 0 {
		hint = payload.MaintenanceEndTimestamp
	}
	if isCTraderRateLimitCode(payload.ErrorCode, payload.Description) || hint != 0 {
		var until time.Time
		if hint != 0 {
			until = time.UnixMilli(int64(hint)).UTC()
		}
		return &ctraderRateLimitErr{msg: msg, retryAfter: until}
	}
	return errors.New(msg)
}

// isCTraderRateLimitCode reports whether the broker is asking us to slow down
// rather than telling us something is wrong with the request.
func isCTraderRateLimitCode(code, description string) bool {
	s := strings.ToUpper(code + " " + description)
	return strings.Contains(s, "BLOCKED_PAYLOAD_TYPE") ||
		strings.Contains(s, "RATE LIMIT") ||
		strings.Contains(s, "RATE-LIMIT") ||
		strings.Contains(s, "TOO MANY REQUEST")
}

// deliverCTraderResponse pushes a response to respCh with a short timeout
// (CONN-007). Previously the dispatcher used a non-blocking `default:` which
// silently dropped real responses under reconnect churn — the requester
// would then time out at the outer RTT budget instead of receiving the
// real reply that just landed. A 200 ms window is plenty for any consumer
// already waiting on the channel and small enough that a duplicate response
// for an unknown request doesn't stall the dispatcher.
func deliverCTraderResponse(respCh chan<- wsResponse, resp wsResponse) {
	select {
	case respCh <- resp:
	case <-time.After(200 * time.Millisecond):
		// Consumer has gone away or the slot is still held by a prior
		// duplicate. Drop — no alternative exists at this layer — but
		// the timeout instead of instant-drop covers the common case of
		// "consumer was mid-flight switching goroutines".
	}
}

func (c *CTrader) sendMessage(
	ctx context.Context,
	payloadType int,
	payload map[string]any,
	expectedPayloadType int,
) (json.RawMessage, error) {
	// E-B: once cTrader has told us the token is dead, every further request
	// is a wasted round trip whose failure reason would be whatever came
	// next. Fail on the known cause instead.
	if c.tokenInvalidated.Load() {
		return nil, errors.New(ctraderTokenInvalidatedError)
	}
	if err := c.ensureConnected(ctx); err != nil {
		return nil, err
	}

	clientMsgID := fmt.Sprintf("msg_%d_%d", atomic.AddUint64(&c.msgID, 1), time.Now().UnixMilli())
	respCh := make(chan wsResponse, 1)

	c.pendingMu.Lock()
	c.pending[clientMsgID] = respCh
	c.pendingMu.Unlock()
	defer func() {
		c.pendingMu.Lock()
		delete(c.pending, clientMsgID)
		c.pendingMu.Unlock()
	}()

	msg := wsOutboundMessage{
		ClientMsgID: clientMsgID,
		PayloadType: payloadType,
		Payload:     payload,
	}

	c.connMu.Lock()
	ws := c.ws
	c.connMu.Unlock()
	if ws == nil {
		return nil, fmt.Errorf("cTrader WebSocket disconnected")
	}

	if err := c.writeMessage(ws, msg); err != nil {
		c.markDisconnected(ws, err)
		return nil, err
	}

	timer := time.NewTimer(c.requestTimeout)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		// E-H2: a request that never came back means the socket is not
		// carrying traffic. Leaving it "connected" made every subsequent
		// call on the cached instance pay the same 30 s for the rest of the
		// cache TTL. Tear it down so the next call redials.
		err := fmt.Errorf("cTrader request timeout for payloadType %d", payloadType)
		c.markDisconnected(ws, err)
		return nil, err
	case resp := <-respCh:
		if resp.err != nil {
			return nil, resp.err
		}
		if resp.payloadType != expectedPayloadType {
			return nil, fmt.Errorf("unexpected cTrader payload type %d (expected %d)", resp.payloadType, expectedPayloadType)
		}
		return resp.payload, nil
	}
}

func (c *CTrader) writeMessage(ws *websocket.Conn, msg wsOutboundMessage) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return ws.WriteMessage(websocket.TextMessage, data)
}

func (c *CTrader) markDisconnected(ws *websocket.Conn, cause error) {
	c.connMu.Lock()
	if c.ws != ws {
		c.connMu.Unlock()
		_ = ws.Close()
		return
	}

	c.ws = nil
	c.appAuthenticated = false
	c.authenticatedAccountID = 0
	stop := c.heartbeatStop
	c.heartbeatStop = nil
	c.connMu.Unlock()

	if stop != nil {
		close(stop)
	}
	_ = ws.Close()
	c.failPending(cause)
}

// disconnect drops the socket and fails every request currently in flight on
// it. V-E2: that is deliberate but blunt — a token refresh calls it, so a
// concurrent GetTrades on the same instance dies with "cTrader connection
// closed" and the caller retries at a higher level. Re-authenticating the app
// on the existing socket would avoid it; the reconnect is kept because
// cTrader ties the session to the token that opened it and ALREADY_LOGGED_IN
// recovery already relies on a fresh socket.
func (c *CTrader) disconnect(cause error) {
	c.connMu.Lock()
	ws := c.ws
	stop := c.heartbeatStop
	c.ws = nil
	c.heartbeatStop = nil
	c.appAuthenticated = false
	c.authenticatedAccountID = 0
	c.connMu.Unlock()

	if stop != nil {
		close(stop)
	}
	if ws != nil {
		_ = ws.Close()
	}
	c.failPending(cause)
}

func (c *CTrader) failPending(cause error) {
	if cause == nil {
		cause = errors.New("cTrader connection closed")
	}

	c.pendingMu.Lock()
	pending := c.pending
	c.pending = make(map[string]chan wsResponse)
	c.pendingMu.Unlock()

	for _, ch := range pending {
		select {
		case ch <- wsResponse{err: cause}:
		default:
		}
	}
}

func decodeRawPayload(raw json.RawMessage, out any) error {
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" || trimmed == "null" {
		return nil
	}
	return json.Unmarshal(raw, out)
}

func isAccessTokenInvalid(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	// Only refresh on truly invalid tokens. ALREADY_LOGGED_IN means the
	// previous WS session is still active — reconnecting fixes it, no refresh needed.
	return strings.Contains(msg, "CH_ACCESS_TOKEN_INVALID")
}

func isAlreadyLoggedIn(err error) bool {
	return err != nil && strings.Contains(err.Error(), "ALREADY_LOGGED_IN")
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		trimmed := strings.TrimSpace(v)
		if trimmed != "" {
			return trimmed
		}
	}
	return ""
}

// GetCashflows returns user deposits/withdrawals from cTrader's dedicated
// cash-flow history (ProtoOACashFlowHistoryListReq, 2143). cTrader does NOT
// surface cash flows in the deal list, so the deal-list approach never
// detected anything. Each entry is a ProtoOADepositWithdraw with an
// operationType (ProtoOAChangeBalanceType) and a delta scaled by moneyDigits.
// Which operation types are capital is ctraderCapitalOps' call.
func (c *CTrader) GetCashflows(ctx context.Context, since time.Time) ([]*Cashflow, error) {
	entries, err := c.getRawCashflows(ctx, since)
	if err != nil {
		return nil, err
	}
	flows := ctraderCashflowsFromEntries(entries)
	// The ledger carries no currency of its own: it is the account's deposit
	// currency, resolved from depositAssetId (E-C2). No conversion.
	currency := c.accountCurrency()
	for _, f := range flows {
		f.Currency = currency
	}
	return flows, nil
}

// getRawCashflows fetches every balance-operation entry in [since, now],
// weekly-paginated (cTrader caps a single ProtoOACashFlowHistoryListReq to 7
// days; a single-shot request over a wider range fails INCORRECT_BOUNDARIES).
func (c *CTrader) getRawCashflows(ctx context.Context, since time.Time) ([]ctraderDepositWithdraw, error) {
	accountID, err := c.ensureAccountID(ctx)
	if err != nil {
		return nil, err
	}
	return c.getAllCashflows(ctx, accountID, since, time.Now().UTC())
}

// GetRawCashflowEntries returns cTrader's balance-operation ledger for
// [since, now] with every operationType included and money values decoded.
// GetCashflows keeps only the capital operations; this exposes everything a
// balance jump may hide behind (demo resets, charges, adjustments).
func (c *CTrader) GetRawCashflowEntries(ctx context.Context, since time.Time) ([]RawBalanceOp, error) {
	entries, err := c.getRawCashflows(ctx, since)
	if err != nil {
		return nil, err
	}
	ops := make([]RawBalanceOp, 0, len(entries))
	for _, dw := range entries {
		div := ctraderMoneyDivisor(dw.MoneyDigits)
		ops = append(ops, RawBalanceOp{
			OperationType: dw.OperationType,
			Delta:         float64(dw.Delta) / div,
			BalanceAfter:  float64(dw.Balance) / div,
			Timestamp:     time.UnixMilli(dw.Timestamp).UTC(),
		})
	}
	return ops, nil
}

// ctraderDepositWithdraw is a single ProtoOADepositWithdraw entry from a
// ProtoOACashFlowHistoryListRes payload.
type ctraderDepositWithdraw struct {
	OperationType int   `json:"operationType"`
	Balance       int64 `json:"balance"` // account balance AFTER the operation (scaled by MoneyDigits)
	Delta         int64 `json:"delta"`   // signed change to the balance
	Timestamp     int64 `json:"changeBalanceTimestamp"`
	MoneyDigits   int   `json:"moneyDigits"`
}

const (
	ctraderOpDeposit  = 0 // ProtoOAChangeBalanceType.BALANCE_DEPOSIT
	ctraderOpWithdraw = 1 // ProtoOAChangeBalanceType.BALANCE_WITHDRAW
)

// ctraderCapitalOps holds the ProtoOAChangeBalanceType values that bring money
// in or take it out without the account having traded for it, with the
// direction each one moves the balance. Only deposits and withdrawals used to
// count, so a transfer from the client's other account on the same server
// read as profit.
//
// Everything absent stays in performance: swaps, dividends, volume rebates,
// GSL and rollover charges, the fees a copier pays (4, 12, 28, 34), the
// inactivity fee, and negative balance protection, which floors a loss at
// zero rather than funding the account. ProtoOATrader keeps the
// non-withdrawable bonus (19, 20) in a field of its own, apart from the
// `balance` the equity reads; its conversion into real balance (38) is capital.
var ctraderCapitalOps = map[int]float64{
	ctraderOpDeposit:  +1,
	ctraderOpWithdraw: -1,
	3:                 +1, // BALANCE_DEPOSIT_STRATEGY_COMMISSION_INNER, received from copiers
	5:                 +1, // BALANCE_DEPOSIT_IB_COMMISSIONS
	6:                 -1, // BALANCE_WITHDRAW_IB_SHARED_PERCENTAGE
	7:                 +1, // BALANCE_DEPOSIT_IB_SHARED_PERCENTAGE_FROM_SUB_IB
	8:                 +1, // BALANCE_DEPOSIT_IB_SHARED_PERCENTAGE_FROM_BROKER
	11:                +1, // BALANCE_DEPOSIT_STRATEGY_COMMISSION_OUTER
	13:                -1, // BALANCE_WITHDRAW_BONUS_COMPENSATION, IB commission shared with the broker
	14:                -1, // BALANCE_WITHDRAW_IB_SHARED_PERCENTAGE_TO_BROKER
	27:                +1, // BALANCE_DEPOSIT_MANAGEMENT_FEE, received from copiers
	29:                +1, // BALANCE_DEPOSIT_PERFORMANCE_FEE, received from copiers
	30:                -1, // BALANCE_WITHDRAW_FOR_SUBACCOUNT
	31:                +1, // BALANCE_DEPOSIT_TO_SUBACCOUNT
	32:                -1, // BALANCE_WITHDRAW_FROM_SUBACCOUNT
	33:                +1, // BALANCE_DEPOSIT_FROM_SUBACCOUNT
	36:                +1, // BALANCE_DEPOSIT_TRANSFER, from another account on the same server
	37:                -1, // BALANCE_WITHDRAW_TRANSFER, to another account on the same server
	38:                +1, // BALANCE_DEPOSIT_CONVERTED_BONUS
}

// ctraderMoneyDivisor returns 10^moneyDigits (default 100 = 2 digits) used to
// convert cTrader's integer money values to a decimal amount.
func ctraderMoneyDivisor(moneyDigits int) float64 {
	if moneyDigits > 0 {
		return math.Pow10(moneyDigits)
	}
	return 100.0
}

// parseCTraderDepositWithdraws decodes the depositWithdraw entries from a
// ProtoOACashFlowHistoryListRes payload (all operation types, unfiltered).
func parseCTraderDepositWithdraws(raw json.RawMessage) ([]ctraderDepositWithdraw, error) {
	var resp struct {
		DepositWithdraw []ctraderDepositWithdraw `json:"depositWithdraw"`
	}
	if err := decodeRawPayload(raw, &resp); err != nil {
		return nil, err
	}
	return resp.DepositWithdraw, nil
}

// ctraderCashflowAmount returns the signed amount of a capital entry (positive
// in, negative out), or ok=false for an entry that is a result of trading.
func ctraderCashflowAmount(dw ctraderDepositWithdraw) (float64, bool) {
	sign, ok := ctraderCapitalOps[dw.OperationType]
	if !ok {
		return 0, false
	}
	amount := math.Abs(float64(dw.Delta)) / ctraderMoneyDivisor(dw.MoneyDigits)
	if amount == 0 {
		return 0, false
	}
	return sign * amount, true
}

// parseCTraderCashflows extracts user deposits/withdrawals from a
// ProtoOACashFlowHistoryListRes payload.
func parseCTraderCashflows(raw json.RawMessage) ([]*Cashflow, error) {
	entries, err := parseCTraderDepositWithdraws(raw)
	if err != nil {
		return nil, err
	}
	return ctraderCashflowsFromEntries(entries), nil
}

// ctraderCashflowsFromEntries keeps the recognized deposit/withdraw entries
// (op 0/1) and drops trading effects (swap/commission/…). Shared by the raw
// payload parser and the paginated GetCashflows so both filter identically.
func ctraderCashflowsFromEntries(entries []ctraderDepositWithdraw) []*Cashflow {
	var cashflows []*Cashflow
	for _, dw := range entries {
		amount, ok := ctraderCashflowAmount(dw)
		if !ok {
			continue
		}
		cashflows = append(cashflows, &Cashflow{
			Amount:    amount,
			Currency:  "USD",
			Timestamp: time.UnixMilli(dw.Timestamp).UTC(),
		})
	}
	return cashflows
}

// --- in-enclave history reconstruction --------------------------------------

const (
	// ctraderCashflowWindow is cTrader's max range for a single
	// ProtoOACashFlowHistoryListReq (toTimestamp - fromTimestamp <= 1 week).
	ctraderCashflowWindow = 7 * 24 * time.Hour
	// ctraderMaxLookback bounds the reconstruction when `since` is the zero
	// time (= "from inception"), keeping the weekly cash-flow pagination tractable.
	ctraderMaxLookback = 2 * 365 * 24 * time.Hour
	// ctraderInceptionBuffer extends the cash-flow scan before the first trade
	// so the inception deposit (which usually precedes trading) is captured.
	ctraderInceptionBuffer = 90 * 24 * time.Hour
	// ctraderMaxDealPages caps deal-list pagination as a runaway guard.
	ctraderMaxDealPages = 200
	// ctraderHistRequestDelay paces historical pagination under cTrader's
	// per-payload-type rate limit — rapid DealList / CashFlowHistory requests
	// trigger "BLOCKED_PAYLOAD_TYPE: You are being rate limited".
	ctraderHistRequestDelay = 600 * time.Millisecond

	// V-E1: the throttle above is a guess, not a guarantee. When cTrader does
	// block a payload type it says so, and ProtoOAErrorRes.retryAfter says
	// until when. Before this, one BLOCKED_PAYLOAD_TYPE mid-walk aborted the
	// whole reconstruction, which then restarted from scratch on the next
	// sync with the exact same request profile — the most likely way a real
	// rebuild dies.
	ctraderRateLimitAttempts    = 5
	ctraderRateLimitBaseBackoff = 2 * time.Second
	ctraderRateLimitMaxBackoff  = 30 * time.Second
	// ctraderRateLimitMaxWait caps how long a single broker-supplied
	// retryAfter may park the walk — a bogus far-future timestamp must not
	// hang the sync for hours.
	ctraderRateLimitMaxWait = 60 * time.Second

	// ctraderPersistAttempts bounds the retries around the rotated-token
	// write. Losing that write costs the connection (E-H3).
	ctraderPersistAttempts = 3
	ctraderPersistBackoff  = 250 * time.Millisecond
	// ctraderProactiveRefreshWindow refreshes an access token about to
	// expire rather than waiting for the request that it breaks.
	ctraderProactiveRefreshWindow = 5 * time.Minute
	// ctraderSymbolMissTTL bounds the negative symbol cache (E-M8).
	ctraderSymbolMissTTL = 5 * time.Minute
)

// throttle sleeps between paginated historical requests to stay under
// cTrader's rate limit, returning early if the context is cancelled.
func (c *CTrader) throttle(ctx context.Context) error {
	c.ensureState()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(c.histRequestDelay):
		return nil
	}
}

// sendPaged issues one page of a paginated historical request and, when
// cTrader answers BLOCKED_PAYLOAD_TYPE (or a maintenance window), waits and
// retries the SAME page instead of failing the walk (V-E1). It waits until
// the broker's retryAfter when it gave one, otherwise on an exponential
// backoff, and gives up after ctraderRateLimitAttempts so a permanently
// blocked account still surfaces an error.
func (c *CTrader) sendPaged(ctx context.Context, payloadType int, payload map[string]any, expected int) (json.RawMessage, error) {
	c.ensureState()
	backoff := c.rateLimitBackoff
	var lastErr error

	for attempt := 0; attempt < ctraderRateLimitAttempts; attempt++ {
		raw, err := c.sendWithTokenRefresh(ctx, func() (json.RawMessage, error) {
			return c.sendMessage(ctx, payloadType, payload, expected)
		})
		if err == nil {
			return raw, nil
		}
		var rateLimited *ctraderRateLimitErr
		if !errors.As(err, &rateLimited) {
			return nil, err
		}
		lastErr = err

		wait := backoff
		if !rateLimited.retryAfter.IsZero() {
			if until := time.Until(rateLimited.retryAfter); until > 0 {
				wait = until
			}
		}
		if wait > ctraderRateLimitMaxWait {
			wait = ctraderRateLimitMaxWait
		}
		if wait <= 0 {
			wait = backoff
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(wait):
		}
		if backoff *= 2; backoff > ctraderRateLimitMaxBackoff {
			backoff = ctraderRateLimitMaxBackoff
		}
	}
	return nil, fmt.Errorf("cTrader rate limited on payloadType %d after %d attempts: %w", payloadType, ctraderRateLimitAttempts, lastErr)
}

// GetHistoricalSnapshots reconstructs the account's daily equity timeline
// entirely in-enclave (ZK-native — no credentials leave the SEV-SNP perimeter).
//
// cTrader exposes no daily-NAV history, so the curve is computed from
// authoritative balance-after values: every closing deal and every ledger
// entry carries the account balance after it (buildCTraderHistoricalSnapshots
// says which instant each row holds).
//
// Caveat: historical UNREALIZED PnL on positions held overnight is not
// reconstructed (no historical mark prices here) — the realized balance is used
// as the daily equity. Accurate for accounts that are flat or short-held
// intraday; slightly understates equity while a position is carried overnight.
func (c *CTrader) GetHistoricalSnapshots(ctx context.Context, since time.Time) ([]*HistoricalSnapshot, error) {
	accountID, err := c.ensureAccountID(ctx)
	if err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	start := since.UTC()
	if start.IsZero() || start.Before(now.Add(-ctraderMaxLookback)) {
		start = now.Add(-ctraderMaxLookback)
	}
	// Whole days only: a scan starting mid-day hands the first row part of
	// that day's flows.
	start = truncUTCDay(start)

	deals, err := c.getAllDeals(ctx, accountID, start, now)
	if err != nil {
		return nil, err
	}

	// Bound the weekly-paginated cash-flow scan to the trading period (plus a
	// buffer for the inception deposit that precedes the first trade), so a
	// recently-active account doesn't scan the entire lookback window.
	cashflowStart := start
	if len(deals) > 0 {
		earliest := deals[0].ExecutionTimestamp
		for _, d := range deals {
			if d.ExecutionTimestamp < earliest {
				earliest = d.ExecutionTimestamp
			}
		}
		if cs := truncUTCDay(time.UnixMilli(earliest).UTC().Add(-ctraderInceptionBuffer)); cs.After(cashflowStart) {
			cashflowStart = cs
		}
	}
	cashflows, err := c.getAllCashflows(ctx, accountID, cashflowStart, now)
	if err != nil {
		return nil, err
	}

	return buildCTraderHistoricalSnapshots(deals, cashflows, now), nil
}

// getAllDeals fetches every deal in [start, end], following hasMore pagination
// forward in time from the newest deal each page holds.
func (c *CTrader) getAllDeals(ctx context.Context, accountID int64, start, end time.Time) ([]cTraderDeal, error) {
	if err := c.authenticateAccount(ctx, accountID); err != nil {
		return nil, err
	}
	from := start.UnixMilli()
	to := end.UnixMilli()
	var all []cTraderDeal
	seen := make(map[int64]struct{})
	complete := false
	c.ensureState()
	for page := 0; page < c.maxDealPages; page++ {
		if page > 0 {
			if err := c.throttle(ctx); err != nil {
				return nil, err
			}
		}
		raw, err := c.sendPaged(ctx, ctraderPayloadDealListReq, map[string]any{
			"ctidTraderAccountId": accountID,
			"fromTimestamp":       from,
			"toTimestamp":         to,
			"maxRows":             1000,
		}, ctraderPayloadDealListRes)
		if err != nil {
			return nil, err
		}
		var resp struct {
			Deal    []cTraderDeal `json:"deal"`
			HasMore bool          `json:"hasMore"`
		}
		if err := decodeRawPayload(raw, &resp); err != nil {
			return nil, err
		}
		var added int
		all, added = appendUnseenDeals(all, seen, resp.Deal)
		if !resp.HasMore || len(resp.Deal) == 0 {
			complete = true
			break
		}
		// CONN-05: re-fetch from the newest deal's millisecond (not +1) so a
		// deal sharing it isn't skipped; the dedup-by-dealId above drops the
		// re-read overlap. The newest rather than the last listed: Spotware
		// documents no order within a page, and a page listed newest-first
		// would send the cursor back to its oldest deal, re-read the same page
		// and end the walk as if the ledger were exhausted.
		newest := newestExecution(resp.Deal)
		if newest <= from && added == 0 {
			// A full page inside one millisecond: no way past it without
			// skipping deals, so the history is not complete.
			break
		}
		from = newest
	}

	// E-H1: a walk that cannot reach the end of the ledger has lost its most
	// RECENT deals — the carry-forward builder then draws a flat line from the
	// truncation point to today and calls it a track record. Same stance as
	// ig.go: refuse a history we know is incomplete rather than hand back a
	// plausible one.
	if !complete {
		return nil, fmt.Errorf("ctrader deal history for %s..%s could not be paged to its end within %d pages of 1000: refusing a truncated history",
			start.UTC().Format(time.DateOnly), end.UTC().Format(time.DateOnly), c.maxDealPages)
	}
	return all, nil
}

func newestExecution(deals []cTraderDeal) int64 {
	var newest int64
	for _, d := range deals {
		if d.ExecutionTimestamp > newest {
			newest = d.ExecutionTimestamp
		}
	}
	return newest
}

// appendUnseenDeals appends deals whose dealId hasn't been seen yet (recording
// them in seen) and returns the grown slice plus the count of newly-added
// deals. Pagination re-reads the boundary millisecond (from = last), so dedup
// by dealId is what keeps a re-fetched deal from being counted twice (CONN-05).
func appendUnseenDeals(all []cTraderDeal, seen map[int64]struct{}, page []cTraderDeal) ([]cTraderDeal, int) {
	added := 0
	for _, d := range page {
		if _, ok := seen[d.DealID]; ok {
			continue
		}
		seen[d.DealID] = struct{}{}
		all = append(all, d)
		added++
	}
	return all, added
}

// getAllCashflows fetches deposits/withdrawals in [start, end] in <=1-week
// chunks (cTrader caps ProtoOACashFlowHistoryListReq to a 7-day range).
func (c *CTrader) getAllCashflows(ctx context.Context, accountID int64, start, end time.Time) ([]ctraderDepositWithdraw, error) {
	if err := c.authenticateAccount(ctx, accountID); err != nil {
		return nil, err
	}
	var all []ctraderDepositWithdraw
	for chunkStart := start; chunkStart.Before(end); chunkStart = chunkStart.Add(ctraderCashflowWindow) {
		if err := c.throttle(ctx); err != nil {
			return nil, err
		}
		chunkEnd := chunkStart.Add(ctraderCashflowWindow)
		if chunkEnd.After(end) {
			chunkEnd = end
		}
		raw, err := c.sendPaged(ctx, ctraderPayloadCashFlowHistoryReq, map[string]any{
			"ctidTraderAccountId": accountID,
			"fromTimestamp":       chunkStart.UnixMilli(),
			"toTimestamp":         chunkEnd.UnixMilli(),
		}, ctraderPayloadCashFlowHistoryRes)
		if err != nil {
			return nil, err
		}
		entries, err := parseCTraderDepositWithdraws(raw)
		if err != nil {
			return nil, err
		}
		all = append(all, entries...)
	}
	return all, nil
}

// ctraderBalPoint is a (timestamp, account balance) sample used to rebuild the
// daily equity curve.
type ctraderBalPoint struct {
	t   time.Time
	bal float64
}

type ctraderCashflowDay struct{ deposits, withdrawals float64 }

type ctraderTradeDay struct {
	count       int
	volume      float64
	fees        float64
	longTrades  int
	shortTrades int
	longVolume  float64
	shortVolume float64
}

func truncUTCDay(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}

// ctraderBalancePoints returns sorted authoritative balance-after samples from
// closing deals and from every ledger entry. Swaps, dividends, charges and
// transfers move the balance too; a curve fed only by deposits and
// withdrawals held their effect back until the next closing deal.
func ctraderBalancePoints(deals []cTraderDeal, cashflows []ctraderDepositWithdraw) []ctraderBalPoint {
	points := ctraderDealBalancePoints(deals)
	for _, cf := range cashflows {
		points = append(points, ctraderBalPoint{
			t:   time.UnixMilli(cf.Timestamp).UTC(),
			bal: float64(cf.Balance) / ctraderMoneyDivisor(cf.MoneyDigits),
		})
	}
	sort.Slice(points, func(i, j int) bool { return points[i].t.Before(points[j].t) })
	return points
}

// ctraderBalanceAt returns the latest balance strictly before t (carry-forward),
// or 0 when no point precedes t (pre-inception).
func ctraderBalanceAt(points []ctraderBalPoint, t time.Time) float64 {
	bal := 0.0
	for _, p := range points {
		if !p.t.Before(t) {
			break
		}
		bal = p.bal
	}
	return bal
}

// ctraderResetPriorFloor is the minimum pre-event balance (USD) for a
// zero-then-fund deposit to be treated as an account reset rather than a
// genuine inception deposit onto an empty account.
const ctraderResetPriorFloor = 1.0

// ctraderCashflowsByDay sums each day's recognized deposits/withdrawals,
// correcting demo-reset deposits. A cTrader demo reset zeroes the account and
// re-deposits the new starting balance, so its ledger entry reports
// delta == balanceAfter (ledger balanceBefore == 0) even though the account
// actually held a non-zero balance. Recording that raw delta counts the
// discarded prior balance as fresh capital — cumulative deposits then exceed
// equity and the account shows a phantom loss. When the ledger claims a
// near-empty prior balance but the reconstructed balance curve says otherwise,
// record the NET instead: balanceAfter minus the balance immediately before.
// Reset-to-lower yields a negative net (a withdrawal), which is also correct.
func ctraderCashflowsByDay(deals []cTraderDeal, cashflows []ctraderDepositWithdraw) map[string]*ctraderCashflowDay {
	dealBals := ctraderDealBalancePoints(deals)
	sorted := make([]ctraderDepositWithdraw, len(cashflows))
	copy(sorted, cashflows)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Timestamp < sorted[j].Timestamp })

	byDay := map[string]*ctraderCashflowDay{}
	var running float64
	di := 0
	for _, cf := range sorted {
		t := time.UnixMilli(cf.Timestamp).UTC()
		for di < len(dealBals) && dealBals[di].t.Before(t) {
			running = dealBals[di].bal
			di++
		}

		amount, ok := ctraderResetAwareAmount(cf, running)

		// E-M5: EVERY ledger entry reports the balance AFTER it — swaps,
		// commissions, rebates, dividends included — and `running` used to
		// advance only on the deposits and withdrawals we recognise. Between
		// two cash flows it therefore drifted away from the true balance, and
		// the reset heuristic below compares the ledger's claimed prior
		// balance against it: drift far enough and a genuine deposit is
		// rewritten as a demo reset, erasing real capital from the curve.
		if cf.Balance != 0 {
			running = float64(cf.Balance) / ctraderMoneyDivisor(cf.MoneyDigits)
		}

		if !ok {
			continue
		}

		key := t.Format("20060102")
		e := byDay[key]
		if e == nil {
			e = &ctraderCashflowDay{}
			byDay[key] = e
		}
		if amount > 0 {
			e.deposits += amount
		} else {
			e.withdrawals += -amount
		}
	}
	return byDay
}

// ctraderResetAwareAmount returns the signed recognized cashflow amount for cf,
// correcting a demo reset — a zero-then-fund deposit whose ledger delta equals
// its balanceAfter — to the net capital change against the reconstructed prior
// balance `running`. Non-reset deposits/withdrawals return their raw amount.
func ctraderResetAwareAmount(cf ctraderDepositWithdraw, running float64) (float64, bool) {
	amount, ok := ctraderCashflowAmount(cf)
	if !ok {
		return 0, false
	}
	if cf.OperationType != ctraderOpDeposit || running <= ctraderResetPriorFloor {
		return amount, true
	}
	div := ctraderMoneyDivisor(cf.MoneyDigits)
	balanceAfter := float64(cf.Balance) / div
	ledgerBefore := balanceAfter - float64(cf.Delta)/div
	if ledgerBefore < 0.5*running {
		return balanceAfter - running, true
	}
	return amount, true
}

// ctraderDealBalancePoints returns time-sorted (timestamp, balance-after)
// samples from closing deals — the balance curve between cashflows, used to
// know the true balance just before a reset deposit.
func ctraderDealBalancePoints(deals []cTraderDeal) []ctraderBalPoint {
	var points []ctraderBalPoint
	for _, d := range deals {
		if d.ClosePositionDetail == nil || d.ClosePositionDetail.Balance == nil {
			continue
		}
		md := d.ClosePositionDetail.MoneyDigits
		if md == 0 {
			md = d.MoneyDigits
		}
		points = append(points, ctraderBalPoint{
			t:   time.UnixMilli(d.ExecutionTimestamp).UTC(),
			bal: float64(*d.ClosePositionDetail.Balance) / ctraderMoneyDivisor(md),
		})
	}
	sort.Slice(points, func(i, j int) bool { return points[i].t.Before(points[j].t) })
	return points
}

func ctraderTradesByDay(deals []cTraderDeal) map[string]*ctraderTradeDay {
	byDay := map[string]*ctraderTradeDay{}
	for _, d := range deals {
		if !d.filled() {
			continue
		}
		key := time.UnixMilli(d.ExecutionTimestamp).UTC().Format("20060102")
		e := byDay[key]
		if e == nil {
			e = &ctraderTradeDay{}
			byDay[key] = e
		}
		e.count++
		notional := d.notional()
		e.volume += notional
		e.fees += d.fee()
		if strings.EqualFold(string(d.TradeSide), "SELL") {
			e.shortTrades++
			e.shortVolume += notional
		} else {
			e.longTrades++
			e.longVolume += notional
		}
	}
	return byDay
}

// buildCTraderHistoricalSnapshots turns raw deal + cash-flow history into a
// daily equity timeline. Pure (no I/O) so it is unit-testable against captured
// payloads.
//
// A row holds the balance at the midnight that opens its date, with the flows
// and trades of the day before: what the live sync reads at 00:00 UTC, over
// the window it reads, under the date it stamps. Rows keyed by the day that
// had just closed sat one day off the live series and every other venue.
// The first row is the day after the first balance change; today's row is the
// live sync's.
func buildCTraderHistoricalSnapshots(deals []cTraderDeal, cashflows []ctraderDepositWithdraw, now time.Time) []*HistoricalSnapshot {
	points := ctraderBalancePoints(deals, cashflows)
	if len(points) == 0 {
		return nil
	}
	cfByDay := ctraderCashflowsByDay(deals, cashflows)
	tByDay := ctraderTradesByDay(deals)

	firstRow := truncUTCDay(points[0].t).Add(24 * time.Hour)
	lastRow := truncUTCDay(now).Add(-24 * time.Hour)

	var out []*HistoricalSnapshot
	for row := firstRow; !row.After(lastRow); row = row.Add(24 * time.Hour) {
		bal := ctraderBalanceAt(points, row)
		snap := &HistoricalSnapshot{Date: row, TotalEquity: bal, RealizedBalance: bal}
		key := row.Add(-24 * time.Hour).Format("20060102")
		if e := cfByDay[key]; e != nil {
			snap.Deposits = e.deposits
			snap.Withdrawals = e.withdrawals
		}
		if e := tByDay[key]; e != nil {
			snap.TotalTrades = e.count
			snap.TotalVolume = e.volume
			snap.TotalFees = e.fees
			snap.LongTrades = e.longTrades
			snap.ShortTrades = e.shortTrades
			snap.LongVolume = e.longVolume
			snap.ShortVolume = e.shortVolume
		}
		out = append(out, snap)
	}
	return out
}
