package connector

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

const deribitAPI = "https://www.deribit.com/api/v2"

// QUAL-001: extracted to remove a 3-way duplication of the account-summary path.
const deribitPathAccountSummary = "/private/get_account_summary"

// Deribit implements Connector for Deribit exchange.
// It authenticates using OAuth client_credentials (api_key/api_secret).
type Deribit struct {
	apiKey    string
	apiSecret string
	client    *http.Client

	mu         sync.Mutex
	token      string
	tokenExp   time.Time
	priceCache map[string]cachedPrice
}

type cachedPrice struct {
	value   float64
	expires time.Time
}

// NewDeribit creates a Deribit connector.
func NewDeribit(creds *Credentials) *Deribit {
	return &Deribit{
		apiKey:     creds.APIKey,
		apiSecret:  creds.APISecret,
		client:     &http.Client{Timeout: 30 * time.Second},
		priceCache: make(map[string]cachedPrice),
	}
}

func (d *Deribit) Exchange() string {
	return "deribit"
}

// DetectIsPaper mirrors TS behavior: Deribit connector targets production API.
func (d *Deribit) DetectIsPaper(_ context.Context) (bool, error) {
	return false, nil
}

func (d *Deribit) TestConnection(ctx context.Context) error {
	_, err := d.privateGET(ctx, deribitPathAccountSummary, url.Values{
		"currency": {"BTC"},
	})
	return err
}

func (d *Deribit) GetBalance(ctx context.Context) (*Balance, error) {
	currencies := []string{"BTC", "ETH", "USDC", "USDT"}

	var (
		totalEquity    float64
		totalBalance   float64
		totalAvail     float64
		totalUnrealPNL float64
	)

	for _, ccy := range currencies {
		body, err := d.privateGET(ctx, deribitPathAccountSummary, url.Values{
			"currency": {ccy},
		})
		if err != nil {
			// Currency sub-account may not exist.
			continue
		}

		var resp struct {
			Result struct {
				Equity         float64 `json:"equity"`
				Balance        float64 `json:"balance"`
				AvailableFunds float64 `json:"available_funds"`
			} `json:"result"`
		}
		if err := json.Unmarshal(body, &resp); err != nil {
			continue
		}

		m := d.usdMultiplier(ctx, ccy)
		eq := resp.Result.Equity * m
		bl := resp.Result.Balance * m
		av := resp.Result.AvailableFunds * m

		totalEquity += eq
		totalBalance += bl
		totalAvail += av
		totalUnrealPNL += (eq - bl)
	}

	return &Balance{
		Available:     totalAvail,
		Equity:        totalEquity,
		UnrealizedPnL: totalUnrealPNL,
		Currency:      "USD",
	}, nil
}

func (d *Deribit) GetPositions(ctx context.Context) ([]*Position, error) {
	currencies := []string{"BTC", "ETH", "USDC", "USDT"}
	out := make([]*Position, 0)

	for _, ccy := range currencies {
		body, err := d.privateGET(ctx, "/private/get_positions", url.Values{
			"currency": {ccy},
		})
		if err != nil {
			continue
		}

		var resp struct {
			Result []struct {
				InstrumentName     string  `json:"instrument_name"`
				Direction          string  `json:"direction"` // buy/sell
				Size               float64 `json:"size"`
				AveragePrice       float64 `json:"average_price"`
				MarkPrice          float64 `json:"mark_price"`
				FloatingProfitLoss float64 `json:"floating_profit_loss"`
				Kind               string  `json:"kind"` // future, option
			} `json:"result"`
		}
		if err := json.Unmarshal(body, &resp); err != nil {
			continue
		}

		m := d.usdMultiplier(ctx, ccy)
		for _, p := range resp.Result {
			if p.Size == 0 {
				continue
			}

			side := "long"
			if strings.EqualFold(p.Direction, "sell") {
				side = "short"
			}

			market := MarketSwap
			upper := strings.ToUpper(p.InstrumentName)
			if strings.Contains(upper, "PERPETUAL") {
				market = MarketSwap
			} else if strings.Count(upper, "-") >= 2 || strings.EqualFold(p.Kind, "option") {
				market = MarketOptions
			} else {
				market = MarketFutures
			}

			out = append(out, &Position{
				Symbol:        p.InstrumentName,
				Side:          side,
				Size:          math.Abs(p.Size),
				EntryPrice:    p.AveragePrice,
				MarkPrice:     p.MarkPrice,
				UnrealizedPnL: p.FloatingProfitLoss * m,
				MarketType:    market,
			})
		}
	}

	return out, nil
}

func (d *Deribit) GetTrades(ctx context.Context, start, end time.Time) ([]*Trade, error) {
	currencies := []string{"BTC", "ETH", "USDC", "USDT"}
	out := make([]*Trade, 0)

	for _, ccy := range currencies {
		body, err := d.privateGET(ctx, "/private/get_user_trades_by_currency_and_time", url.Values{
			"currency":        {ccy},
			"start_timestamp": {strconv.FormatInt(start.UnixMilli(), 10)},
			"end_timestamp":   {strconv.FormatInt(end.UnixMilli(), 10)},
			"count":           {"1000"},
			"sorting":         {"asc"},
		})
		if err != nil {
			continue
		}

		var resp struct {
			Result struct {
				Trades []struct {
					TradeID        any     `json:"trade_id"`
					InstrumentName string  `json:"instrument_name"`
					Direction      string  `json:"direction"` // buy/sell
					Amount         float64 `json:"amount"`
					Price          float64 `json:"price"`
					Fee            float64 `json:"fee"`
					Timestamp      int64   `json:"timestamp"`
					ProfitLoss     float64 `json:"profit_loss"`
				} `json:"trades"`
			} `json:"result"`
		}
		if err := json.Unmarshal(body, &resp); err != nil {
			continue
		}

		m := d.usdMultiplier(ctx, ccy)
		for _, t := range resp.Result.Trades {
			id := fmt.Sprintf("%v", t.TradeID)
			if id == "" || id == "<nil>" {
				id = strconv.FormatInt(t.Timestamp, 10)
			}

			market := marketTypeFromDeribitSymbol(t.InstrumentName)

			out = append(out, &Trade{
				ID:          id,
				Symbol:      t.InstrumentName,
				Side:        strings.ToLower(t.Direction),
				Price:       t.Price,
				Quantity:    t.Amount,
				Fee:         t.Fee * m,
				FeeCurrency: "USD",
				RealizedPnL: t.ProfitLoss * m,
				Timestamp:   time.UnixMilli(t.Timestamp).UTC(),
				MarketType:  market,
			})
		}
	}

	return out, nil
}

func marketTypeFromDeribitSymbol(symbol string) string {
	upper := strings.ToUpper(symbol)
	if strings.Contains(upper, "PERPETUAL") {
		return MarketSwap
	}
	if strings.Count(upper, "-") >= 2 {
		return MarketOptions
	}
	return MarketFutures
}

func (d *Deribit) usdMultiplier(ctx context.Context, currency string) float64 {
	switch strings.ToUpper(strings.TrimSpace(currency)) {
	case "USDT", "USDC", "USD":
		return 1
	}

	ccy := strings.ToUpper(strings.TrimSpace(currency))
	d.mu.Lock()
	if cached, ok := d.priceCache[ccy]; ok && time.Now().Before(cached.expires) {
		d.mu.Unlock()
		return cached.value
	}
	d.mu.Unlock()

	price := d.fetchUSDPrice(ctx, ccy)
	if price <= 0 {
		return 0
	}

	d.mu.Lock()
	d.priceCache[ccy] = cachedPrice{value: price, expires: time.Now().Add(5 * time.Minute)}
	d.mu.Unlock()
	return price
}

func (d *Deribit) fetchUSDPrice(ctx context.Context, currency string) float64 {
	body, err := d.publicGET(ctx, "/public/ticker", url.Values{
		"instrument_name": {strings.ToUpper(currency) + "-PERPETUAL"},
	})
	if err != nil {
		return 0
	}

	var resp struct {
		Result struct {
			LastPrice  float64 `json:"last_price"`
			MarkPrice  float64 `json:"mark_price"`
			IndexPrice float64 `json:"index_price"`
		} `json:"result"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return 0
	}

	if resp.Result.LastPrice > 0 {
		return resp.Result.LastPrice
	}
	if resp.Result.MarkPrice > 0 {
		return resp.Result.MarkPrice
	}
	return resp.Result.IndexPrice
}

func (d *Deribit) ensureToken(ctx context.Context) error {
	d.mu.Lock()
	if d.token != "" && time.Now().Before(d.tokenExp) {
		d.mu.Unlock()
		return nil
	}
	d.mu.Unlock()

	// CONN-001: send client_id / client_secret in an HTTP Basic Authorization
	// header rather than as URL query parameters. URLs are logged by every
	// intermediate proxy, CDN, TLS terminator, and crash-dump pipeline; Deribit's
	// own documentation recommends Basic auth precisely for that reason.
	values := url.Values{}
	values.Set("grant_type", "client_credentials")

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, deribitAPI+"/public/auth?"+values.Encode(), nil)
	if err != nil {
		return err
	}
	basic := base64.StdEncoding.EncodeToString([]byte(d.apiKey + ":" + d.apiSecret))
	req.Header.Set("Authorization", "Basic "+basic)

	resp, err := d.client.Do(req)
	if err != nil {
		return err
	}

	// CONN-AUDIT-001: bounded read.
	body, err := ReadCappedBody(resp.Body, DefaultMaxResponseBytes)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("deribit auth status %d: %s", resp.StatusCode, TruncatedBody(body))
	}

	var authResp struct {
		Result struct {
			AccessToken string `json:"access_token"`
			ExpiresIn   int64  `json:"expires_in"`
		} `json:"result"`
		Error any `json:"error"`
	}
	if err := json.Unmarshal(body, &authResp); err != nil {
		return err
	}
	if authResp.Result.AccessToken == "" {
		return fmt.Errorf("deribit auth failed")
	}

	exp := time.Now().Add(time.Duration(authResp.Result.ExpiresIn) * time.Second)
	if authResp.Result.ExpiresIn <= 30 {
		exp = time.Now().Add(30 * time.Second)
	}

	d.mu.Lock()
	d.token = authResp.Result.AccessToken
	d.tokenExp = exp.Add(-10 * time.Second)
	d.mu.Unlock()
	return nil
}

func (d *Deribit) privateGET(ctx context.Context, path string, params url.Values) ([]byte, error) {
	if err := d.ensureToken(ctx); err != nil {
		return nil, err
	}

	u := deribitAPI + path
	if len(params) > 0 {
		u += "?" + params.Encode()
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}

	d.mu.Lock()
	token := d.token
	d.mu.Unlock()
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := d.client.Do(req)
	if err != nil {
		return nil, err
	}

	// CONN-AUDIT-001 + 002: bounded read + truncated body in errors.
	body, err := ReadCappedBody(resp.Body, DefaultMaxResponseBytes)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("deribit API status %d: %s", resp.StatusCode, TruncatedBody(body))
	}

	var envelope struct {
		Error any `json:"error"`
	}
	_ = json.Unmarshal(body, &envelope)
	if envelope.Error != nil {
		return nil, fmt.Errorf("deribit API error: %v", envelope.Error)
	}

	return body, nil
}

func (d *Deribit) publicGET(ctx context.Context, path string, params url.Values) ([]byte, error) {
	u := deribitAPI + path
	if len(params) > 0 {
		u += "?" + params.Encode()
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}

	resp, err := d.client.Do(req)
	if err != nil {
		return nil, err
	}

	// CONN-AUDIT-001 + 002: bounded read + truncated body in errors.
	body, err := ReadCappedBody(resp.Body, DefaultMaxResponseBytes)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("deribit public API status %d: %s", resp.StatusCode, TruncatedBody(body))
	}
	return body, nil
}

// GetCashflows returns deposits and withdrawals with USD conversion.
func (d *Deribit) GetCashflows(ctx context.Context, since time.Time) ([]*Cashflow, error) {
	currencies := []string{"BTC", "ETH", "USDC", "USDT"}
	var cashflows []*Cashflow

	for _, ccy := range currencies {
		mul := d.usdMultiplier(ctx, ccy)
		if mul <= 0 {
			continue
		}

		// Deposits
		depBody, err := d.privateGET(ctx, "/private/get_deposits", url.Values{
			"currency": {ccy},
			"count":    {"100"},
		})
		if err == nil {
			var depResp struct {
				Result struct {
					Data []struct {
						Amount    float64 `json:"amount"`
						State     string  `json:"state"`
						Timestamp int64   `json:"received_timestamp"`
					} `json:"data"`
				} `json:"result"`
			}
			if json.Unmarshal(depBody, &depResp) == nil {
				for _, dep := range depResp.Result.Data {
					ts := time.UnixMilli(dep.Timestamp)
					if ts.Before(since) || dep.State != "completed" {
						continue
					}
					cashflows = append(cashflows, &Cashflow{
						Amount:    dep.Amount * mul,
						Currency:  ccy,
						Timestamp: ts,
					})
				}
			}
		}

		// Withdrawals
		wdBody, err := d.privateGET(ctx, "/private/get_withdrawals", url.Values{
			"currency": {ccy},
			"count":    {"100"},
		})
		if err == nil {
			var wdResp struct {
				Result struct {
					Data []struct {
						Amount    float64 `json:"amount"`
						State     string  `json:"state"`
						Timestamp int64   `json:"confirmed_timestamp"`
					} `json:"data"`
				} `json:"result"`
			}
			if json.Unmarshal(wdBody, &wdResp) == nil {
				for _, wd := range wdResp.Result.Data {
					ts := time.UnixMilli(wd.Timestamp)
					if ts.Before(since) || wd.State != "completed" {
						continue
					}
					cashflows = append(cashflows, &Cashflow{
						Amount:    -wd.Amount * mul,
						Currency:  ccy,
						Timestamp: ts,
					})
				}
			}
		}
	}

	return cashflows, nil
}

// GetBalanceByMarket returns per-market equity (swap/options) with USD conversion.
func (d *Deribit) GetBalanceByMarket(ctx context.Context) ([]*MarketBalance, error) {
	currencies := []string{"BTC", "ETH", "USDC", "USDT"}
	swapEquity := 0.0
	optionsEquity := 0.0

	for _, ccy := range currencies {
		mul := d.usdMultiplier(ctx, ccy)
		if mul <= 0 {
			continue
		}

		body, err := d.privateGET(ctx, deribitPathAccountSummary, url.Values{
			"currency": {ccy},
			"extended": {"true"},
		})
		if err != nil {
			continue
		}

		var resp struct {
			Result struct {
				Equity         float64 `json:"equity"`
				FuturesPl      float64 `json:"futures_pl"`
				OptionsPl      float64 `json:"options_pl"`
				AvailableFunds float64 `json:"available_funds"`
			} `json:"result"`
		}

		if json.Unmarshal(body, &resp) != nil {
			continue
		}

		equity := resp.Result.Equity * mul
		if resp.Result.OptionsPl != 0 {
			optionsEquity += resp.Result.OptionsPl * mul
		}
		swapEquity += equity - (resp.Result.OptionsPl * mul)
	}

	var balances []*MarketBalance
	if swapEquity > 0 {
		balances = append(balances, &MarketBalance{MarketType: MarketSwap, Equity: swapEquity})
	}
	if optionsEquity > 0 {
		balances = append(balances, &MarketBalance{MarketType: MarketOptions, Equity: optionsEquity})
	}
	return balances, nil
}

// GetFundingFees returns funding fee history for perpetual positions.
// On Deribit, funding is embedded in settlement PnL; this returns an empty
// slice rather than failing (TS parity: Deribit getFundingFees returns []).
func (d *Deribit) GetFundingFees(_ context.Context, _ []string, _ time.Time) ([]*FundingFee, error) {
	return nil, nil
}
