package connector

import (
	"context"
	"crypto/hmac"
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const gateAPI = "https://api.gateio.ws"

// Gate implements Connector for Gate.io exchange using native HTTP.
// Uses HMAC-SHA512 signing (no passphrase).
type Gate struct {
	base CryptoBase
}

// NewGate creates a new Gate.io connector.
func NewGate(creds *Credentials) *Gate {
	return &Gate{
		base: NewCryptoBase(creds.APIKey, creds.APISecret, gateAPI),
	}
}

func (g *Gate) Exchange() string { return "gate" }

func (g *Gate) hashBody(body string) string {
	h := sha512.New()
	h.Write([]byte(body))
	return hex.EncodeToString(h.Sum(nil))
}

func (g *Gate) sign(method, path, query, hashedBody, timestamp string) string {
	message := method + "\n" + path + "\n" + query + "\n" + hashedBody + "\n" + timestamp
	mac := hmac.New(sha512.New, []byte(g.base.APISecret))
	mac.Write([]byte(message))
	return hex.EncodeToString(mac.Sum(nil))
}

func (g *Gate) doRequest(ctx context.Context, method, path, query string) ([]byte, error) {
	timestamp := strconv.FormatInt(time.Now().Unix(), 10)
	hashedBody := g.hashBody("")
	signature := g.sign(method, path, query, hashedBody, timestamp)

	reqURL := g.base.BaseURL + path
	if query != "" {
		reqURL += "?" + query
	}

	req, err := http.NewRequestWithContext(ctx, method, reqURL, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("KEY", g.base.APIKey)
	req.Header.Set("SIGN", signature)
	req.Header.Set("Timestamp", timestamp)
	req.Header.Set("Content-Type", "application/json")

	return g.base.DoRequest(req)
}

func (g *Gate) TestConnection(ctx context.Context) error {
	_, err := g.doRequest(ctx, "GET", "/api/v4/spot/accounts", "")
	return err
}

func (g *Gate) GetBalance(ctx context.Context) (*Balance, error) {
	// Spot balance
	spotBody, err := g.doRequest(ctx, "GET", "/api/v4/spot/accounts", "")
	if err != nil {
		return nil, fmt.Errorf("spot balance: %w", err)
	}

	var spotResp []struct {
		Currency  string `json:"currency"`
		Available string `json:"available"`
		Locked    string `json:"locked"`
	}
	if err := json.Unmarshal(spotBody, &spotResp); err != nil {
		return nil, fmt.Errorf("parse spot balance: %w", err)
	}

	stablecoins := []string{"USDT", "USDC", "USD"}
	spotEquity := 0.0
	spotAvailable := 0.0
	for _, a := range spotResp {
		for _, sc := range stablecoins {
			if strings.EqualFold(a.Currency, sc) {
				avail, _ := strconv.ParseFloat(a.Available, 64)
				locked, _ := strconv.ParseFloat(a.Locked, 64)
				spotEquity += avail + locked
				spotAvailable += avail
			}
		}
	}

	// Futures balance (ignore error — account may not have futures)
	futuresEquity := 0.0
	futuresUnrealized := 0.0
	futBody, err := g.doRequest(ctx, "GET", "/api/v4/futures/usdt/accounts", "")
	if err == nil {
		var futResp struct {
			Total        string `json:"total"`
			UnrealisedPnl string `json:"unrealised_pnl"`
			Available    string `json:"available"`
		}
		if json.Unmarshal(futBody, &futResp) == nil {
			futuresEquity, _ = strconv.ParseFloat(futResp.Total, 64)
			futuresUnrealized, _ = strconv.ParseFloat(futResp.UnrealisedPnl, 64)
		}
	}

	return &Balance{
		Equity:        spotEquity + futuresEquity,
		Available:     spotAvailable,
		UnrealizedPnL: futuresUnrealized,
		Currency:      "USDT",
	}, nil
}

func (g *Gate) GetPositions(ctx context.Context) ([]*Position, error) {
	body, err := g.doRequest(ctx, "GET", "/api/v4/futures/usdt/positions", "")
	if err != nil {
		return nil, nil
	}

	var resp []struct {
		Contract      string `json:"contract"`
		Size          int64  `json:"size"`
		EntryPrice    string `json:"entry_price"`
		MarkPrice     string `json:"mark_price"`
		UnrealisedPnl string `json:"unrealised_pnl"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}

	positions := make([]*Position, 0, len(resp))
	for _, p := range resp {
		if p.Size == 0 {
			continue
		}
		entry, _ := strconv.ParseFloat(p.EntryPrice, 64)
		mark, _ := strconv.ParseFloat(p.MarkPrice, 64)
		upl, _ := strconv.ParseFloat(p.UnrealisedPnl, 64)

		side := "long"
		size := float64(p.Size)
		if p.Size < 0 {
			side = "short"
			size = -size
		}

		positions = append(positions, &Position{
			Symbol:        p.Contract,
			Side:          side,
			Size:          size,
			EntryPrice:    entry,
			MarkPrice:     mark,
			UnrealizedPnL: upl,
			MarketType:    MarketSwap,
		})
	}

	return positions, nil
}

func (g *Gate) GetTrades(ctx context.Context, start, end time.Time) ([]*Trade, error) {
	query := fmt.Sprintf("from=%d&to=%d&limit=1000", start.Unix(), end.Unix())
	body, err := g.doRequest(ctx, "GET", "/api/v4/spot/my_trades", query)
	if err != nil {
		return nil, err
	}

	var resp []struct {
		ID            string `json:"id"`
		CurrencyPair  string `json:"currency_pair"`
		Side          string `json:"side"`
		Price         string `json:"price"`
		Amount        string `json:"amount"`
		Fee           string `json:"fee"`
		FeeCurrency   string `json:"fee_currency"`
		CreateTimeMs  string `json:"create_time_ms"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}

	trades := make([]*Trade, 0, len(resp))
	for _, t := range resp {
		price, _ := strconv.ParseFloat(t.Price, 64)
		qty, _ := strconv.ParseFloat(t.Amount, 64)
		fee, _ := strconv.ParseFloat(t.Fee, 64)
		tsMs, _ := strconv.ParseInt(t.CreateTimeMs, 10, 64)

		trades = append(trades, &Trade{
			ID:          t.ID,
			Symbol:      t.CurrencyPair,
			Side:        strings.ToLower(t.Side),
			Price:       price,
			Quantity:    qty,
			Fee:         fee,
			FeeCurrency: t.FeeCurrency,
			Timestamp:   time.UnixMilli(tsMs),
			MarketType:  MarketSpot,
		})
	}

	return trades, nil
}

// GetCashflows returns nil — not reliably available on Gate.io.
func (g *Gate) GetCashflows(_ context.Context, _ time.Time) ([]*Cashflow, error) {
	return nil, nil
}
