package connector

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

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
}

// NewMetaTrader creates a MetaTrader connector.
// creds.APIKey=login, creds.APISecret=investor password, creds.Passphrase=server[:port].
func NewMetaTrader(creds *Credentials) *MetaTrader {
	login, _ := strconv.Atoi(strings.TrimSpace(creds.APIKey))
	exchange := strings.ToLower(strings.TrimSpace(creds.Exchange))
	if exchange != "mt5" {
		exchange = "mt4"
	}

	bridgeURL := strings.TrimRight(strings.TrimSpace(os.Getenv("MT_BRIDGE_URL")), "/")
	if bridgeURL == "" {
		bridgeURL = "http://mt-bridge:8090"
	}

	return &MetaTrader{
		protocol:   exchange,
		login:      login,
		password:   creds.APISecret,
		server:     strings.TrimSpace(creds.Passphrase),
		bridgeURL:  bridgeURL,
		hmacSecret: os.Getenv("MT_BRIDGE_HMAC_SECRET"),
		client:     &http.Client{Timeout: 30 * time.Second},
	}
}

func (m *MetaTrader) Exchange() string {
	return m.protocol
}

func (m *MetaTrader) TestConnection(ctx context.Context) error {
	_, err := m.ensureSession(ctx)
	return err
}

func (m *MetaTrader) GetBalance(ctx context.Context) (*Balance, error) {
	sessionID, err := m.ensureSession(ctx)
	if err != nil {
		return nil, err
	}

	var info struct {
		Balance       float64 `json:"balance"`
		Equity        float64 `json:"equity"`
		UnrealizedPnL float64 `json:"unrealized_pnl"`
		Currency      string  `json:"currency"`
		MarginFree    float64 `json:"margin_free"`
	}
	if err := m.callBridge(ctx, http.MethodGet, "/api/v1/sessions/"+sessionID+"/account-info", nil, &info); err != nil {
		return nil, err
	}

	unreal := info.UnrealizedPnL
	if unreal == 0 {
		unreal = info.Equity - info.Balance
	}

	currency := strings.TrimSpace(info.Currency)
	if currency == "" {
		currency = "USD"
	}

	return &Balance{
		Available:     info.MarginFree,
		Equity:        info.Equity,
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
	if err := m.callBridge(ctx, http.MethodGet, "/api/v1/sessions/"+sessionID+"/positions", nil, &rows); err != nil {
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

	from := strconv.FormatInt(start.Unix(), 10)
	to := strconv.FormatInt(end.Unix(), 10)
	path := "/api/v1/sessions/" + sessionID + "/history-deals?from=" + url.QueryEscape(from) + "&to=" + url.QueryEscape(to)

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
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("mt-bridge status %d: %s", resp.StatusCode, string(raw))
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

	now := time.Now().UTC()
	path := fmt.Sprintf("/api/v1/sessions/%s/history-deals?from=%d&to=%d",
		sessionID, since.Unix(), now.Unix())

	var resp struct {
		Deals []struct {
			Symbol      string  `json:"symbol"`
			Side        string  `json:"side"`
			RealizedPnL float64 `json:"realized_pnl"`
			CloseTime   string  `json:"close_time"`
		} `json:"deals"`
	}

	if err := m.callBridge(ctx, "GET", path, nil, &resp); err != nil {
		return nil, err
	}

	var cashflows []*Cashflow
	for _, deal := range resp.Deals {
		if deal.Symbol != "BALANCE" {
			continue
		}
		ts, _ := time.Parse(time.RFC3339, deal.CloseTime)
		if ts.IsZero() {
			continue
		}
		amount := deal.RealizedPnL
		if deal.Side == "withdrawal" {
			amount = -mathAbs(amount)
		} else if deal.Side == "deposit" {
			amount = mathAbs(amount)
		} else {
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
