package connector

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// QUAL-001: extracted to remove a 3-way duplication of the bridge sessions path.
const metatraderPathSessions = "/api/v1/sessions/"

// mtServerName is what an MT5 server name or a bare hostname looks like. The
// name is typed by the account holder and decides where the bridge connects,
// so a port, a scheme or a path never leaves the enclave. The bridge applies
// the same rule.
var mtServerName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._ -]{0,63}$`)

// mtDealClockSlack is how far ahead of the clock the bridge can date a deal: it
// guesses the server's UTC offset from the latest deal or position, and the
// guess only comes out low for a server east of UTC.
const mtDealClockSlack = 6 * time.Hour

// mtDealsUntil is where a deal read ending at end stops. A read ending now runs
// past it by mtDealClockSlack, since no deal dated later was made after now.
func mtDealsUntil(end, now time.Time) time.Time {
	if end.Before(now.Add(-time.Minute)) {
		return end
	}
	if ahead := now.Add(mtDealClockSlack); ahead.After(end) {
		return ahead
	}
	return end
}

// MetaTrader implements Connector for MT4/MT5 through mt-bridge service.
type MetaTrader struct {
	protocol string // mt4 or mt5
	login    int
	password string
	server   string

	bridgeURL  string
	hmacSecret string
	client     *http.Client

	mu        sync.Mutex
	sessionID string

	// What the bridge read from the broker's account-info frame, kept after the
	// first account-info call so DetectIsPaper does not have to ask again.
	// Empty when the bridge predates these fields.
	accountInfoRead bool
	reportedServer  string
	accountType     string
	company         string
}

// mtAccountInfo is the bridge's /account-info payload.
type mtAccountInfo struct {
	Balance       float64 `json:"balance"`
	Equity        float64 `json:"equity"`
	UnrealizedPnL float64 `json:"unrealized_pnl"`
	Currency      string  `json:"currency"`
	MarginFree    float64 `json:"margin_free"`
	// Server is the server name the broker reports, not the one typed.
	Server string `json:"server"`
	// AccountType is "real" | "demo" | "contest" | "preliminary" | "unknown",
	// read by the bridge from byte 0 of the MT5 account-info frame.
	AccountType string `json:"account_type"`
	Company     string `json:"company"`
}

// mtBridgeMinSecretLen is the floor CFG-005 enforces at boot; repeated here
// because the connector is also reachable from tools that skip main.go.
const mtBridgeMinSecretLen = 24

// NewMetaTrader creates a MetaTrader connector.
// creds.APIKey=login, creds.APISecret=investor password, creds.Passphrase=server name.
//
// SEC-13: the bridge connect call carries the investor password in cleartext
// JSON, so production refuses to build the connector unless the link is
// https:// and the request signature is keyed on a real secret. Development
// keeps the loopback http default.
func NewMetaTrader(creds *Credentials) (*MetaTrader, error) {
	login, _ := strconv.Atoi(strings.TrimSpace(creds.APIKey))
	exchange := strings.ToLower(strings.TrimSpace(creds.Exchange))
	switch exchange {
	case "mt5", "exness":
		exchange = "mt5"
	default:
		exchange = "mt4"
	}

	bridgeURL := strings.TrimRight(strings.TrimSpace(os.Getenv("MT_BRIDGE_URL")), "/")
	hmacSecret := strings.TrimSpace(os.Getenv("MT_BRIDGE_HMAC_SECRET"))
	if isProductionEnv() {
		if !strings.HasPrefix(strings.ToLower(bridgeURL), "https://") {
			return nil, fmt.Errorf("mt-bridge unavailable: MT_BRIDGE_URL must be https:// in production")
		}
		if len(hmacSecret) < mtBridgeMinSecretLen {
			return nil, fmt.Errorf("mt-bridge unavailable: MT_BRIDGE_HMAC_SECRET must be at least %d characters in production", mtBridgeMinSecretLen)
		}
	} else if bridgeURL == "" {
		bridgeURL = "http://mt-bridge:8090"
	}

	return &MetaTrader{
		protocol:   exchange,
		login:      login,
		password:   creds.APISecret,
		server:     strings.TrimSpace(creds.Passphrase),
		bridgeURL:  bridgeURL,
		hmacSecret: hmacSecret,
		client:     &http.Client{Timeout: 30 * time.Second},
	}, nil
}

func (m *MetaTrader) Exchange() string {
	return m.protocol
}

func (m *MetaTrader) TestConnection(ctx context.Context) error {
	_, err := m.ensureSession(ctx)
	return err
}

// DetectIsPaper reports a demo account. The name the holder typed is checked
// first; when it does not say demo, the broker's own account-info is read and
// its reported server name and account type are checked too. A failed read
// is an error: the caller then keeps whatever verdict is stored instead of
// writing "live" over a demo flag the bridge set on an earlier read.
func (m *MetaTrader) DetectIsPaper(ctx context.Context) (bool, error) {
	if mtServerNameLooksPaper(m.server) {
		return true, nil
	}
	m.mu.Lock()
	read := m.accountInfoRead
	m.mu.Unlock()
	if !read {
		if _, err := m.fetchAccountInfo(ctx); err != nil {
			return false, fmt.Errorf("mt5 account-info for paper check: %w", err)
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return mtIsPaper(m.server, m.reportedServer, m.accountType), nil
}

// mtServerNameLooksPaper is the original name check: "demo" or "trial" in the
// server name, case-insensitive.
func mtServerNameLooksPaper(server string) bool {
	lower := strings.ToLower(server)
	return strings.Contains(lower, "demo") || strings.Contains(lower, "trial")
}

// mtIsPaper combines every demo signal, and only ever adds to the name check.
//
// The typed server name alone could be bypassed: the bridge also accepts a raw
// host or IP, which carries no "demo". The broker-reported server name closes
// that. The account type comes from byte 0 of the MT5 account-info frame, with
// the byte-to-name mapping taken from pymt5's reverse engineering of the Web
// Terminal (0 real, 1 demo, 2 contest, 3 preliminary), not from MetaQuotes
// documentation. Since 0 means real, a zeroed or misread frame also reads as
// real, so "real" (or "unknown", or empty from an older bridge) never clears a
// demo name: it can only add a demo verdict, never remove one.
func mtIsPaper(typedServer, reportedServer, accountType string) bool {
	if mtServerNameLooksPaper(typedServer) || mtServerNameLooksPaper(reportedServer) {
		return true
	}
	switch strings.ToLower(strings.TrimSpace(accountType)) {
	case "demo", "contest", "preliminary":
		return true
	}
	return false
}

// fetchAccountInfo reads /account-info and keeps the broker-reported fields.
func (m *MetaTrader) fetchAccountInfo(ctx context.Context) (*mtAccountInfo, error) {
	sessionID, err := m.ensureSession(ctx)
	if err != nil {
		return nil, err
	}
	var info mtAccountInfo
	if err := m.callBridge(ctx, http.MethodGet, metatraderPathSessions+sessionID+"/account-info", nil, &info); err != nil {
		return nil, err
	}
	m.mu.Lock()
	m.accountInfoRead = true
	m.reportedServer = strings.TrimSpace(info.Server)
	m.accountType = strings.TrimSpace(info.AccountType)
	m.company = strings.TrimSpace(info.Company)
	m.mu.Unlock()
	return &info, nil
}

func (m *MetaTrader) GetBalance(ctx context.Context) (*Balance, error) {
	info, err := m.fetchAccountInfo(ctx)
	if err != nil {
		return nil, err
	}

	unreal := info.UnrealizedPnL

	// Parity with TS connector: when unrealized PnL is available (from position-level data),
	// always derive equity as balance + unrealized. This avoids relying on accProfit from
	// the binary account-info frame, which reads the wrong offset on some brokers (e.g. Exness).
	var equity float64
	if unreal != 0 {
		equity = info.Balance + unreal
	} else {
		equity = info.Equity
	}

	currency := strings.TrimSpace(info.Currency)
	if currency == "" {
		currency = "USD"
	}

	// Use balance (realized cash) as Available, not margin_free.
	// margin_free = equity - margin_used and can be deeply negative on leveraged CFD accounts.
	return &Balance{
		Available:     info.Balance,
		Equity:        equity,
		UnrealizedPnL: unreal,
		Currency:      currency,
	}, nil
}

func (m *MetaTrader) GetPositions(ctx context.Context) ([]*Position, error) {
	sessionID, err := m.ensureSession(ctx)
	if err != nil {
		return nil, err
	}

	var rows []struct {
		Symbol        string  `json:"symbol"`
		Side          string  `json:"side"`
		Size          float64 `json:"size"`
		EntryPrice    float64 `json:"entry_price"`
		MarkPrice     float64 `json:"mark_price"`
		UnrealizedPnL float64 `json:"unrealized_pnl"`
		Swap          float64 `json:"swap"`
		Commission    float64 `json:"commission"`
	}
	if err := m.callBridge(ctx, http.MethodGet, metatraderPathSessions+sessionID+"/positions", nil, &rows); err != nil {
		return nil, err
	}

	out := make([]*Position, 0, len(rows))
	for _, p := range rows {
		side := strings.ToLower(strings.TrimSpace(p.Side))
		if side != "short" {
			side = "long"
		}
		out = append(out, &Position{
			Symbol:        p.Symbol,
			Side:          side,
			Size:          p.Size,
			EntryPrice:    p.EntryPrice,
			MarkPrice:     p.MarkPrice,
			UnrealizedPnL: p.UnrealizedPnL + p.Swap + p.Commission,
			MarketType:    MarketCFD,
		})
	}

	return out, nil
}

func (m *MetaTrader) GetTrades(ctx context.Context, start, end time.Time) ([]*Trade, error) {
	sessionID, err := m.ensureSession(ctx)
	if err != nil {
		return nil, err
	}

	until := mtDealsUntil(end, time.Now())
	from := strconv.FormatInt(start.Unix(), 10)
	to := strconv.FormatInt(until.Unix(), 10)
	path := metatraderPathSessions + sessionID + "/history-deals?from=" + url.QueryEscape(from) + "&to=" + url.QueryEscape(to)

	var rows []struct {
		Ticket      int64   `json:"ticket"`
		Symbol      string  `json:"symbol"`
		Side        string  `json:"side"`
		Size        float64 `json:"size"`
		OpenPrice   float64 `json:"open_price"`
		ClosePrice  float64 `json:"close_price"`
		RealizedPnL float64 `json:"realized_pnl"`
		Commission  float64 `json:"commission"`
		Swap        float64 `json:"swap"`
		CloseTime   string  `json:"close_time"`
	}
	if err := m.callBridge(ctx, http.MethodGet, path, nil, &rows); err != nil {
		return nil, err
	}

	out := make([]*Trade, 0, len(rows))
	for _, d := range rows {
		ts, _ := time.Parse(time.RFC3339, d.CloseTime)
		if ts.IsZero() {
			ts = start
		}
		// Bridge ignores from/to params — filter client-side
		if ts.Before(start) || ts.After(until) {
			continue
		}
		side := strings.ToLower(strings.TrimSpace(d.Side))
		if side != "sell" {
			side = "buy"
		}

		out = append(out, &Trade{
			ID:          strconv.FormatInt(d.Ticket, 10),
			Symbol:      d.Symbol,
			Side:        side,
			Price:       priceWithFallback(d.ClosePrice, d.OpenPrice),
			Quantity:    d.Size,
			Fee:         mathAbs(d.Commission) + mathAbs(d.Swap),
			FeeCurrency: "USD",
			RealizedPnL: d.RealizedPnL,
			Timestamp:   ts.UTC(),
			MarketType:  MarketCFD,
		})
	}

	return out, nil
}

func (m *MetaTrader) ensureSession(ctx context.Context) (string, error) {
	m.mu.Lock()
	if m.sessionID != "" {
		defer m.mu.Unlock()
		return m.sessionID, nil
	}
	m.mu.Unlock()

	if m.login <= 0 || m.password == "" || m.server == "" {
		return "", fmt.Errorf("metatrader requires login(api_key), investor password(api_secret), and server(passphrase)")
	}
	if !mtServerName.MatchString(m.server) {
		return "", fmt.Errorf("mt-bridge [UNKNOWN_SERVER]: not an MT5 server name")
	}

	payload := map[string]any{
		"protocol": m.protocol,
		"server":   m.server,
		"login":    m.login,
		"password": m.password,
	}

	var resp struct {
		SessionID string `json:"session_id"`
	}
	if err := m.callBridge(ctx, http.MethodPost, "/api/v1/connect", payload, &resp); err != nil {
		return "", err
	}
	if strings.TrimSpace(resp.SessionID) == "" {
		return "", fmt.Errorf("mt-bridge did not return session_id")
	}

	m.mu.Lock()
	m.sessionID = resp.SessionID
	m.mu.Unlock()
	return resp.SessionID, nil
}

func (m *MetaTrader) callBridge(ctx context.Context, method, path string, body any, out any) error {
	var bodyBytes []byte
	var err error
	if body != nil {
		bodyBytes, err = json.Marshal(body)
		if err != nil {
			return err
		}
	}

	ts := strconv.FormatInt(time.Now().Unix(), 10)
	sigPayload := ts + "." + string(bodyBytes)
	mac := hmac.New(sha256.New, []byte(m.hmacSecret))
	mac.Write([]byte(sigPayload))
	signature := hex.EncodeToString(mac.Sum(nil))

	req, err := http.NewRequestWithContext(ctx, method, m.bridgeURL+path, bytes.NewReader(bodyBytes))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-MT-Bridge-Timestamp", ts)
	req.Header.Set("X-MT-Bridge-Signature", signature)

	resp, err := m.client.Do(req)
	if err != nil {
		return err
	}

	// CONN-AUDIT-001 + 002: bounded read + truncated body in errors.
	raw, err := ReadCappedBody(resp.Body, DefaultMaxResponseBytes)
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("mt-bridge status %d: %s", resp.StatusCode, TruncatedBody(raw))
	}

	var env struct {
		Success bool            `json:"success"`
		Data    json.RawMessage `json:"data"`
		Error   *struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		return err
	}
	if !env.Success {
		if env.Error != nil {
			return fmt.Errorf("mt-bridge [%s]: %s", env.Error.Code, env.Error.Message)
		}
		return fmt.Errorf("mt-bridge unknown error")
	}
	if out == nil {
		return nil
	}
	if len(env.Data) == 0 {
		return nil
	}
	return json.Unmarshal(env.Data, out)
}

// isBalanceSymbol returns true for symbols that represent balance operations.
// The bridge normalizes all balance deals (empty symbol, "Deposit", "Withdrawal",
// "Bonus", "Credit") to "BALANCE" with a correct side based on profit sign, so
// we only need to recognize the canonical form here.
func isBalanceSymbol(symbol string) bool {
	return symbol == "BALANCE"
}

func priceWithFallback(closePrice, openPrice float64) float64 {
	if closePrice != 0 {
		return closePrice
	}
	return openPrice
}

func mathAbs(v float64) float64 {
	if v < 0 {
		return -v
	}
	return v
}

// GetCashflows returns deposits/withdrawals from MT history-deals with symbol=BALANCE.
func (m *MetaTrader) GetCashflows(ctx context.Context, since time.Time) ([]*Cashflow, error) {
	sessionID, err := m.ensureSession(ctx)
	if err != nil {
		return nil, err
	}

	until := time.Now().UTC().Add(mtDealClockSlack)
	path := fmt.Sprintf("/api/v1/sessions/%s/history-deals?from=%d&to=%d",
		sessionID, since.Unix(), until.Unix())

	// Bridge returns a direct array of deals (same format as GetTrades)
	var deals []struct {
		Symbol      string  `json:"symbol"`
		Side        string  `json:"side"`
		RealizedPnl float64 `json:"realized_pnl"`
		CloseTime   string  `json:"close_time"`
	}

	if err := m.callBridge(ctx, "GET", path, nil, &deals); err != nil {
		return nil, err
	}

	var cashflows []*Cashflow
	for _, deal := range deals {
		if !isBalanceSymbol(deal.Symbol) {
			continue
		}
		ts, _ := time.Parse(time.RFC3339, deal.CloseTime)
		if ts.IsZero() || ts.Before(since) {
			continue
		}
		// Bridge sets side based on profit sign for every balance deal.
		var amount float64
		switch deal.Side {
		case "deposit":
			amount = mathAbs(deal.RealizedPnl)
		case "withdrawal":
			amount = -mathAbs(deal.RealizedPnl)
		default:
			continue
		}
		if amount == 0 {
			continue
		}
		cashflows = append(cashflows, &Cashflow{
			Amount:    amount,
			Currency:  "USD",
			Timestamp: ts,
		})
	}

	return cashflows, nil
}
