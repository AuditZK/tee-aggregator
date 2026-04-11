package connector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
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
	ctraderPayloadSymbolByIDReq  = 2116
	ctraderPayloadSymbolByIDRes  = 2117

	ctraderPayloadHeartbeatEvent = 51
	ctraderPayloadErrorRes       = 2142
)

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

type cTraderPosition struct {
	PositionID int64 `json:"positionId"`
	TradeData  struct {
		SymbolID   int64  `json:"symbolId"`
		Volume     int64  `json:"volume"`
		TradeSide  string `json:"tradeSide"`
		UsedMargin int64  `json:"usedMargin"`
	} `json:"tradeData"`
	Price               int64 `json:"price"`
	UnrealizedNetProfit int64 `json:"unrealizedNetProfit"`
	UsedMargin          int64 `json:"usedMargin"`
}

type cTraderDeal struct {
	DealID              int64  `json:"dealId"`
	OrderID             int64  `json:"orderId"`
	SymbolID            int64  `json:"symbolId"`
	TradeSide           string `json:"tradeSide"`
	FilledVolume        int64  `json:"filledVolume"`
	ExecutionPrice      int64  `json:"executionPrice"`
	ExecutionTimestamp  int64  `json:"executionTimestamp"`
	Commission          int64  `json:"commission"`
	DealStatus          string `json:"dealStatus"`
	ClosePositionDetail *struct {
		GrossProfit int64 `json:"grossProfit"`
		Commission  int64 `json:"commission"`
		Swap        int64 `json:"swap"`
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
		if strings.EqualFold(p.TradeData.TradeSide, "SELL") {
			side = "short"
		}

		positions = append(positions, &Position{
			Symbol:        symbol,
			Side:          side,
			Size:          float64(p.TradeData.Volume) / 100.0,
			EntryPrice:    float64(p.Price) / 100000.0,
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
		if strings.EqualFold(d.TradeSide, "SELL") {
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
			Price:       float64(d.ExecutionPrice) / 100000.0,
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
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode == http.StatusTooManyRequests {
		return fmt.Errorf("cTrader token refresh rate-limited (429), retry later")
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Token refresh failed (HTTP %d): %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var tokenResp struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}
	if err := json.Unmarshal(body, &tokenResp); err != nil {
		return err
	}
	if strings.TrimSpace(tokenResp.AccessToken) == "" {
		return fmt.Errorf("token refresh response missing access_token")
	}

	c.accessToken = strings.TrimSpace(tokenResp.AccessToken)

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

			select {
			case respCh <- wsResponse{err: errors.New(errMsg)}:
			default:
			}
			continue
		}

		select {
		case respCh <- wsResponse{payloadType: msg.PayloadType, payload: msg.Payload}:
		default:
		}
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

// GetCashflows returns deposits/withdrawals from cTrader deal list.
// cTrader amounts are in cents -- divide by 100.
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
			ctraderPayloadDealListReq,
			map[string]any{
				"ctidTraderAccountId": accountID,
				"fromTimestamp":       since.UnixMilli(),
				"toTimestamp":         time.Now().UTC().UnixMilli(),
			},
			ctraderPayloadDealListRes,
		)
	})
	if err != nil {
		return nil, err
	}

	var resp struct {
		Deposit  []json.RawMessage `json:"deposit"`
		Withdraw []json.RawMessage `json:"withdraw"`
	}
	if err := decodeRawPayload(raw, &resp); err != nil {
		return nil, err
	}

	var cashflows []*Cashflow

	for _, d := range resp.Deposit {
		var m struct {
			Balance            int64 `json:"balance"`
			ExecutionTimestamp int64 `json:"executionTimestamp"`
		}
		if json.Unmarshal(d, &m) != nil || m.Balance <= 0 {
			continue
		}
		cashflows = append(cashflows, &Cashflow{
			Amount:    float64(m.Balance) / 100,
			Currency:  "USD",
			Timestamp: time.UnixMilli(m.ExecutionTimestamp).UTC(),
		})
	}

	for _, w := range resp.Withdraw {
		var m struct {
			Balance            int64 `json:"balance"`
			ExecutionTimestamp int64 `json:"executionTimestamp"`
		}
		if json.Unmarshal(w, &m) != nil || m.Balance <= 0 {
			continue
		}
		cashflows = append(cashflows, &Cashflow{
			Amount:    -float64(m.Balance) / 100,
			Currency:  "USD",
			Timestamp: time.UnixMilli(m.ExecutionTimestamp).UTC(),
		})
	}

	return cashflows, nil
}
