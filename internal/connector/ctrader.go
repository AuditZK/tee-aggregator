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
	ctraderPayloadSymbolByIDReq  = 2116
	ctraderPayloadSymbolByIDRes  = 2117

	ctraderPayloadHeartbeatEvent = 51
	ctraderPayloadErrorRes       = 2142
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
}

type cTraderAccount struct {
	CtidTraderAccountID int64  `json:"ctidTraderAccountId"`
	IsLive              bool   `json:"isLive"`
	BrokerName          string `json:"brokerName"`
}

type cTraderTrader struct {
	CtidTraderAccountID int64 `json:"ctidTraderAccountId"`
	Balance             int64 `json:"balance"`
	MoneyDigits         int   `json:"moneyDigits"`
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
		SymbolID   int64     `json:"symbolId"`
		Volume     int64     `json:"volume"`
		TradeSide  tradeSide `json:"tradeSide"`
		UsedMargin int64     `json:"usedMargin"`
	} `json:"tradeData"`
	// cTrader's ProtoOAPosition.price is a double (the actual entry price, e.g.
	// 1.15229), NOT a scaled integer — decode it as float64 and use it directly.
	Price               float64 `json:"price"`
	UnrealizedNetProfit int64   `json:"unrealizedNetProfit"`
	UsedMargin          int64   `json:"usedMargin"`
}

type cTraderDeal struct {
	DealID              int64     `json:"dealId"`
	OrderID             int64     `json:"orderId"`
	SymbolID            int64     `json:"symbolId"`
	TradeSide           tradeSide `json:"tradeSide"`
	FilledVolume        int64     `json:"filledVolume"`
	ExecutionPrice      float64    `json:"executionPrice"`
	ExecutionTimestamp  int64      `json:"executionTimestamp"`
	Commission          int64      `json:"commission"`
	DealStatus          dealStatus `json:"dealStatus"`
	MoneyDigits         int        `json:"moneyDigits"`
	ClosePositionDetail *struct {
		GrossProfit int64 `json:"grossProfit"`
		Commission  int64 `json:"commission"`
		Swap        int64 `json:"swap"`
		// Balance is the authoritative account balance AFTER this closing deal
		// (scaled by MoneyDigits). Used to reconstruct the historical equity curve.
		Balance     int64 `json:"balance"`
		MoneyDigits int   `json:"moneyDigits"`
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

	pendingMu sync.Mutex
	pending   map[string]chan wsResponse
	msgID     uint64

	accountMu sync.Mutex
	accountID int64

	symbolMu    sync.RWMutex
	symbolCache map[int64]string

	tokenPersister TokenPersister
}

// NewCTrader creates a new cTrader connector.
// TS-parity credentials:
// - apiKey = access_token
// - apiSecret = refresh_token (optional)
// - passphrase = "demo" to force demo WebSocket endpoint
// - CTRADER_CLIENT_ID / CTRADER_CLIENT_SECRET for app auth + refresh flow
func NewCTrader(creds *Credentials) *CTrader {
	clientID := firstNonEmpty(creds.ClientID, os.Getenv("CTRADER_CLIENT_ID"))
	clientSecret := firstNonEmpty(creds.ClientSecret, os.Getenv("CTRADER_CLIENT_SECRET"))
	accessToken := firstNonEmpty(creds.AccessToken, creds.APIKey)
	refreshToken := strings.TrimSpace(creds.APISecret)
	isLive := strings.ToLower(strings.TrimSpace(creds.Passphrase)) != "demo"

	return &CTrader{
		clientID:     clientID,
		clientSecret: clientSecret,
		accessToken:  accessToken,
		refreshToken: refreshToken,
		isLive:       isLive,
		httpClient:   &http.Client{Timeout: 30 * time.Second},
		wsDialer: &websocket.Dialer{
			HandshakeTimeout: 10 * time.Second,
		},
		wsLiveURL:   ctraderWSLiveURL,
		wsDemoURL:   ctraderWSDemoURL,
		authURL:     ctraderAuthURL,
		pending:     make(map[string]chan wsResponse),
		symbolCache: make(map[int64]string),
	}
}

func (c *CTrader) Exchange() string { return "ctrader" }

// SetTokenPersister sets a callback to persist refreshed OAuth tokens to DB.
func (c *CTrader) SetTokenPersister(persister TokenPersister) {
	c.tokenPersister = persister
}

// DetectIsPaper mirrors TS behavior:
// passphrase=\"demo\" selects demo endpoint, otherwise live.
func (c *CTrader) DetectIsPaper(_ context.Context) (bool, error) {
	return !c.isLive, nil
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
			UnrealizedPnL: float64(p.UnrealizedNetProfit) / 100.0,
			MarketType:    detectCTraderMarketType(symbol),
		})
	}

	return positions, nil
}

func (c *CTrader) GetTrades(ctx context.Context, start, end time.Time) ([]*Trade, error) {
	accountID, err := c.ensureAccountID(ctx)
	if err != nil {
		return nil, err
	}

	deals, err := c.getDealsRaw(ctx, accountID, start.UnixMilli(), end.UnixMilli())
	if err != nil {
		return nil, err
	}

	trades := make([]*Trade, 0, len(deals))
	for _, d := range deals {
		if d.DealStatus != "FILLED" && d.DealStatus != "PARTIALLY_FILLED" {
			continue
		}

		symbol := c.getSymbolName(ctx, d.SymbolID, accountID)
		side := "buy"
		if strings.EqualFold(string(d.TradeSide), "SELL") {
			side = "sell"
		}

		realizedPnL := 0.0
		if d.ClosePositionDetail != nil {
			realizedPnL = float64(d.ClosePositionDetail.GrossProfit-d.ClosePositionDetail.Commission-d.ClosePositionDetail.Swap) / 100.0
		}

		trades = append(trades, &Trade{
			ID:          strconv.FormatInt(d.DealID, 10),
			Symbol:      symbol,
			Side:        side,
			Price:       d.ExecutionPrice,
			Quantity:    float64(d.FilledVolume) / 100.0,
			Fee:         float64(d.Commission) / 100.0,
			FeeCurrency: "USD",
			RealizedPnL: realizedPnL,
			Timestamp:   time.UnixMilli(d.ExecutionTimestamp).UTC(),
			MarketType:  detectCTraderMarketType(symbol),
		})
	}

	return trades, nil
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
}

func (c *CTrader) currentAccessToken() string {
	c.tokenMu.RLock()
	defer c.tokenMu.RUnlock()
	return c.accessToken
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
	if len(accounts) == 0 {
		return 0, fmt.Errorf("no cTrader accounts found")
	}

	selected := accounts[0]
	for _, acct := range accounts {
		if acct.IsLive {
			selected = acct
			break
		}
	}

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

	unrealizedPnL := 0.0
	marginUsed := 0.0
	for _, p := range positions {
		unrealizedPnL += float64(p.UnrealizedNetProfit) / divisor

		used := p.UsedMargin
		if used <= 0 {
			used = p.TradeData.UsedMargin
		}
		if used > 0 {
			marginUsed += float64(used) / divisor
		}
	}

	balance := float64(trader.Balance) / divisor
	equity := balance + unrealizedPnL

	return &cTraderBalanceInfo{
		Balance:         balance,
		Equity:          equity,
		UnrealizedPnL:   unrealizedPnL,
		MarginUsed:      marginUsed,
		MarginAvailable: equity - marginUsed,
		Currency:        "USD",
	}, nil
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

func (c *CTrader) getDealsRaw(ctx context.Context, accountID, fromTS, toTS int64) ([]cTraderDeal, error) {
	if err := c.authenticateAccount(ctx, accountID); err != nil {
		return nil, err
	}

	raw, err := c.sendMessage(
		ctx,
		ctraderPayloadDealListReq,
		map[string]any{
			"ctidTraderAccountId": accountID,
			"fromTimestamp":       fromTS,
			"toTimestamp":         toTS,
			"maxRows":             1000,
		},
		ctraderPayloadDealListRes,
	)
	if err != nil {
		return nil, err
	}

	var resp struct {
		Deal []cTraderDeal `json:"deal"`
	}
	if err := decodeRawPayload(raw, &resp); err != nil {
		return nil, err
	}
	if resp.Deal == nil {
		return []cTraderDeal{}, nil
	}
	return resp.Deal, nil
}

func (c *CTrader) getSymbolName(ctx context.Context, symbolID, accountID int64) string {
	if symbolID <= 0 {
		return ""
	}

	c.symbolMu.RLock()
	name, ok := c.symbolCache[symbolID]
	c.symbolMu.RUnlock()
	if ok && name != "" {
		return name
	}

	resolved, err := c.getSymbolByID(ctx, symbolID, accountID)
	if err == nil && resolved != "" {
		return resolved
	}

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
	return err
}

func (c *CTrader) sendWithTokenRefresh(ctx context.Context, call func() (json.RawMessage, error)) (json.RawMessage, error) {
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

	if !isAccessTokenInvalid(err) || strings.TrimSpace(c.refreshToken) == "" {
		return nil, err
	}

	if err := c.refreshAccessToken(ctx); err != nil {
		return nil, err
	}

	c.disconnect(errors.New("cTrader reconnect after token refresh"))
	if err := c.ensureConnected(ctx); err != nil {
		return nil, err
	}
	if err := c.authenticateApp(ctx); err != nil {
		return nil, err
	}

	return call()
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

	c.accessToken = strings.TrimSpace(tokenResp.AccessToken)
	if strings.TrimSpace(tokenResp.RefreshToken) != "" {
		c.refreshToken = strings.TrimSpace(tokenResp.RefreshToken)
	}

	// Persist refreshed tokens to DB if callback is set (TS parity)
	if c.tokenPersister != nil {
		go func() {
			persistCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_ = c.tokenPersister(persistCtx, c.accessToken, c.refreshToken)
		}()
	}

	return nil
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

	c.connMu.Lock()
	if c.ws != nil {
		c.connMu.Unlock()
		_ = ws.Close()
		return nil
	}
	c.ws = ws
	c.appAuthenticated = false
	stop := make(chan struct{})
	c.heartbeatStop = stop
	c.connMu.Unlock()

	go c.readLoop(ws)
	go c.heartbeatLoop(ws, stop)
	return nil
}

func (c *CTrader) heartbeatLoop(ws *websocket.Conn, stop <-chan struct{}) {
	ticker := time.NewTicker(10 * time.Second)
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

		var msg wsInboundMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			continue
		}
		if msg.PayloadType == ctraderPayloadHeartbeatEvent {
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

		if msg.PayloadType == ctraderPayloadErrorRes {
			var payload cTraderErrorPayload
			_ = json.Unmarshal(msg.Payload, &payload)

			errMsg := "cTrader unknown error"
			if payload.ErrorCode != "" {
				errMsg = fmt.Sprintf("cTrader error %s: %s", payload.ErrorCode, payload.Description)
			}

			// CONN-007: use a short timeout instead of an unconditional
			// `default:` drop. The respCh is a 1-buffered channel per
			// request, so this only backs off when a duplicate response
			// or a slow consumer already has the slot.
			deliverCTraderResponse(respCh, wsResponse{err: errors.New(errMsg)})
			continue
		}

		deliverCTraderResponse(respCh, wsResponse{payloadType: msg.PayloadType, payload: msg.Payload})
	}
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

	timer := time.NewTimer(30 * time.Second)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		return nil, fmt.Errorf("cTrader request timeout for payloadType %d", payloadType)
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
	stop := c.heartbeatStop
	c.heartbeatStop = nil
	c.connMu.Unlock()

	if stop != nil {
		close(stop)
	}
	_ = ws.Close()
	c.failPending(cause)
}

func (c *CTrader) disconnect(cause error) {
	c.connMu.Lock()
	ws := c.ws
	stop := c.heartbeatStop
	c.ws = nil
	c.heartbeatStop = nil
	c.appAuthenticated = false
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

// detectCTraderMarketType guesses market type from symbol name.
func detectCTraderMarketType(symbol string) string {
	// Forex pairs typically have 6 chars (EURUSD, GBPJPY, etc.)
	if len(symbol) == 6 {
		return MarketForex
	}
	// Indices
	indices := []string{"US500", "US30", "US100", "DE30", "UK100", "JP225", "AU200"}
	for _, idx := range indices {
		if symbol == idx {
			return MarketCFD
		}
	}
	// Commodities
	commodities := []string{"XAUUSD", "XAGUSD", "XPTUSD", "USOIL", "UKOIL"}
	for _, c := range commodities {
		if symbol == c {
			return MarketCommodities
		}
	}
	return MarketCFD
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
//
// Only BALANCE_DEPOSIT (0) and BALANCE_WITHDRAW (1) are treated as cash flows:
// every other operationType (swap, commission, rebate, dividend, fee…) is a
// trading effect already reflected in equity/PnL and must not be double-counted.
func (c *CTrader) GetCashflows(ctx context.Context, since time.Time) ([]*Cashflow, error) {
	accountID, err := c.ensureAccountID(ctx)
	if err != nil {
		return nil, err
	}

	if err := c.authenticateAccount(ctx, accountID); err != nil {
		return nil, err
	}

	raw, err := c.sendWithTokenRefresh(ctx, func() (json.RawMessage, error) {
		return c.sendMessage(
			ctx,
			ctraderPayloadCashFlowHistoryReq,
			map[string]any{
				"ctidTraderAccountId": accountID,
				"fromTimestamp":       since.UnixMilli(),
				"toTimestamp":         time.Now().UTC().UnixMilli(),
			},
			ctraderPayloadCashFlowHistoryRes,
		)
	})
	if err != nil {
		return nil, err
	}

	return parseCTraderCashflows(raw)
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

// ctraderCashflowAmount returns the signed amount for a deposit/withdraw entry
// (positive deposit, negative withdrawal), or ok=false when the entry is not a
// user capital flow (swap/commission/rebate/dividend/fee/etc., which are
// trading effects already reflected in equity/PnL).
func ctraderCashflowAmount(dw ctraderDepositWithdraw) (float64, bool) {
	if dw.OperationType != ctraderOpDeposit && dw.OperationType != ctraderOpWithdraw {
		return 0, false
	}
	amount := math.Abs(float64(dw.Delta)) / ctraderMoneyDivisor(dw.MoneyDigits)
	if amount == 0 {
		return 0, false
	}
	if dw.OperationType == ctraderOpWithdraw {
		amount = -amount
	}
	return amount, true
}

// parseCTraderCashflows extracts user deposits/withdrawals from a
// ProtoOACashFlowHistoryListRes payload.
func parseCTraderCashflows(raw json.RawMessage) ([]*Cashflow, error) {
	entries, err := parseCTraderDepositWithdraws(raw)
	if err != nil {
		return nil, err
	}
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
	return cashflows, nil
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
)

// ctraderThrottle sleeps between paginated historical requests to stay under
// cTrader's rate limit, returning early if the context is cancelled.
func ctraderThrottle(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(ctraderHistRequestDelay):
		return nil
	}
}

// GetHistoricalSnapshots reconstructs the account's daily equity timeline
// entirely in-enclave (ZK-native — no credentials leave the SEV-SNP perimeter).
//
// cTrader exposes no daily-NAV history, so the curve is computed from
// authoritative balance-after values: every closing deal and every
// deposit/withdrawal carries the account balance after it. Daily equity is the
// latest such balance at or before the end of each day (carry-forward).
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
		if cs := time.UnixMilli(earliest).UTC().Add(-ctraderInceptionBuffer); cs.After(cashflowStart) {
			cashflowStart = cs
		}
	}
	cashflows, err := c.getAllCashflows(ctx, accountID, cashflowStart, now)
	if err != nil {
		return nil, err
	}

	return buildCTraderHistoricalSnapshots(deals, cashflows, now), nil
}

// getAllDeals fetches every deal in [start, end], following hasMore pagination.
// Deals are returned ascending by executionTimestamp, so each page advances the
// window past the last deal seen.
func (c *CTrader) getAllDeals(ctx context.Context, accountID int64, start, end time.Time) ([]cTraderDeal, error) {
	if err := c.authenticateAccount(ctx, accountID); err != nil {
		return nil, err
	}
	from := start.UnixMilli()
	to := end.UnixMilli()
	var all []cTraderDeal
	for page := 0; page < ctraderMaxDealPages; page++ {
		if page > 0 {
			if err := ctraderThrottle(ctx); err != nil {
				return nil, err
			}
		}
		fromTS := from
		raw, err := c.sendWithTokenRefresh(ctx, func() (json.RawMessage, error) {
			return c.sendMessage(ctx, ctraderPayloadDealListReq, map[string]any{
				"ctidTraderAccountId": accountID,
				"fromTimestamp":       fromTS,
				"toTimestamp":         to,
				"maxRows":             1000,
			}, ctraderPayloadDealListRes)
		})
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
		all = append(all, resp.Deal...)
		if !resp.HasMore || len(resp.Deal) == 0 {
			break
		}
		last := resp.Deal[len(resp.Deal)-1].ExecutionTimestamp
		if last <= from {
			break // no forward progress — stop to avoid a loop
		}
		from = last + 1
	}
	return all, nil
}

// getAllCashflows fetches deposits/withdrawals in [start, end] in <=1-week
// chunks (cTrader caps ProtoOACashFlowHistoryListReq to a 7-day range).
func (c *CTrader) getAllCashflows(ctx context.Context, accountID int64, start, end time.Time) ([]ctraderDepositWithdraw, error) {
	if err := c.authenticateAccount(ctx, accountID); err != nil {
		return nil, err
	}
	var all []ctraderDepositWithdraw
	for chunkStart := start; chunkStart.Before(end); chunkStart = chunkStart.Add(ctraderCashflowWindow) {
		if err := ctraderThrottle(ctx); err != nil {
			return nil, err
		}
		chunkEnd := chunkStart.Add(ctraderCashflowWindow)
		if chunkEnd.After(end) {
			chunkEnd = end
		}
		fromTS, toTS := chunkStart.UnixMilli(), chunkEnd.UnixMilli()
		raw, err := c.sendWithTokenRefresh(ctx, func() (json.RawMessage, error) {
			return c.sendMessage(ctx, ctraderPayloadCashFlowHistoryReq, map[string]any{
				"ctidTraderAccountId": accountID,
				"fromTimestamp":       fromTS,
				"toTimestamp":         toTS,
			}, ctraderPayloadCashFlowHistoryRes)
		})
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
	count  int
	volume float64
	fees   float64
}

func truncUTCDay(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}

// ctraderBalancePoints returns sorted authoritative balance-after samples from
// closing deals and cash flows (cTrader reports the account balance after each).
func ctraderBalancePoints(deals []cTraderDeal, cashflows []ctraderDepositWithdraw) []ctraderBalPoint {
	var points []ctraderBalPoint
	for _, d := range deals {
		if d.ClosePositionDetail == nil || d.ClosePositionDetail.Balance == 0 {
			continue
		}
		md := d.ClosePositionDetail.MoneyDigits
		if md == 0 {
			md = d.MoneyDigits
		}
		points = append(points, ctraderBalPoint{
			t:   time.UnixMilli(d.ExecutionTimestamp).UTC(),
			bal: float64(d.ClosePositionDetail.Balance) / ctraderMoneyDivisor(md),
		})
	}
	for _, cf := range cashflows {
		if _, ok := ctraderCashflowAmount(cf); !ok {
			continue
		}
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

func ctraderCashflowsByDay(cashflows []ctraderDepositWithdraw) map[string]*ctraderCashflowDay {
	byDay := map[string]*ctraderCashflowDay{}
	for _, cf := range cashflows {
		amount, ok := ctraderCashflowAmount(cf)
		if !ok {
			continue
		}
		key := time.UnixMilli(cf.Timestamp).UTC().Format("20060102")
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

func ctraderTradesByDay(deals []cTraderDeal) map[string]*ctraderTradeDay {
	byDay := map[string]*ctraderTradeDay{}
	for _, d := range deals {
		if d.DealStatus != "FILLED" && d.DealStatus != "PARTIALLY_FILLED" {
			continue
		}
		key := time.UnixMilli(d.ExecutionTimestamp).UTC().Format("20060102")
		e := byDay[key]
		if e == nil {
			e = &ctraderTradeDay{}
			byDay[key] = e
		}
		e.count++
		e.volume += (float64(d.FilledVolume) / 100.0) * d.ExecutionPrice
		e.fees += float64(d.Commission) / ctraderMoneyDivisor(d.MoneyDigits)
	}
	return byDay
}

// buildCTraderHistoricalSnapshots turns raw deal + cash-flow history into a
// daily equity timeline. Pure (no I/O) so it is unit-testable against captured
// payloads. Today is intentionally excluded — it is owned by the live sync.
func buildCTraderHistoricalSnapshots(deals []cTraderDeal, cashflows []ctraderDepositWithdraw, now time.Time) []*HistoricalSnapshot {
	points := ctraderBalancePoints(deals, cashflows)
	if len(points) == 0 {
		return nil
	}
	cfByDay := ctraderCashflowsByDay(cashflows)
	tByDay := ctraderTradesByDay(deals)

	firstDay := truncUTCDay(points[0].t)
	lastDay := truncUTCDay(now).Add(-24 * time.Hour) // yesterday; today is the live branch's

	var out []*HistoricalSnapshot
	for day := firstDay; !day.After(lastDay); day = day.Add(24 * time.Hour) {
		bal := ctraderBalanceAt(points, day.Add(24*time.Hour))
		snap := &HistoricalSnapshot{Date: day, TotalEquity: bal, RealizedBalance: bal}
		key := day.Format("20060102")
		if e := cfByDay[key]; e != nil {
			snap.Deposits = e.deposits
			snap.Withdrawals = e.withdrawals
		}
		if e := tByDay[key]; e != nil {
			snap.TotalTrades = e.count
			snap.TotalVolume = e.volume
			snap.TotalFees = e.fees
		}
		out = append(out, snap)
	}
	return out
}
