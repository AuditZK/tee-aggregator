package connector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

const kucoinAPI = "https://api.kucoin.com"

// kucoinFallbackHosts are the regional KuCoin sites tried, in order, when the
// global one answers 400003. A key exists only on the site that issued it, and
// KuCoin EU (EEA accounts) serves its keys from api.kucoin.eu; the global site
// answers "The API key does not exist or site mismatch" for them. The connect
// form does not ask for the site, so the connector probes and pins the first
// one that recognises the key, as the OKX connector does for its regions.
var kucoinFallbackHosts = []string{
	"https://api.kucoin.eu",
}

const kucoinCodeKeyUnknown = "400003"

// kucoinCodeHints spell out the caller-fixable auth codes in the error that
// reaches the logs. 400006 is not here: it becomes ErrIPRestricted.
var kucoinCodeHints = map[string]string{
	"400001": "a signing header is missing",
	"400002": "request timestamp outside the accepted window",
	"400003": "API key unknown on this KuCoin site",
	"400004": "passphrase rejected, it is the API passphrase set when the key was created, not the login password",
	"400005": "signature rejected, check the API secret",
	"400007": "the key lacks the General permission",
	"400008": "version 1 keys are disabled, a new key is needed",
	"400009": "API key version not accepted",
	"400302": "access refused for the caller's IP address or region",
	"400303": "the account must complete KYC verification",
	"400304": "the master account must complete KYC verification",
	"411100": "the account is frozen or restricted by KuCoin",
}

// KuCoin implements Connector for KuCoin exchange using native HTTP.
// Uses HMAC-SHA256 + HMAC-signed passphrase.
type KuCoin struct {
	base       CryptoBase
	passphrase string

	mu        sync.Mutex
	fallbacks []string
}

// NewKuCoin creates a new KuCoin connector.
func NewKuCoin(creds *Credentials) *KuCoin {
	return &KuCoin{
		base:       NewCryptoBase(creds.APIKey, creds.APISecret, kucoinAPI),
		passphrase: creds.Passphrase,
		fallbacks:  append([]string(nil), kucoinFallbackHosts...),
	}
}

func (k *KuCoin) Exchange() string { return "kucoin" }

func (k *KuCoin) hmacSign(message string) string {
	return signHMACBase64(k.base.APISecret, message)
}

func (k *KuCoin) signedPassphrase() string {
	return k.hmacSign(k.passphrase)
}

func (k *KuCoin) hosts() []string {
	k.mu.Lock()
	defer k.mu.Unlock()
	return append([]string{k.base.BaseURL}, k.fallbacks...)
}

func (k *KuCoin) baseURL() string {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.base.BaseURL
}

func (k *KuCoin) pinHost(host string) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.base.BaseURL = host
	k.fallbacks = nil
}

func (k *KuCoin) doRequest(ctx context.Context, method, path string) ([]byte, error) {
	hosts := k.hosts()
	var err error
	for i, host := range hosts {
		var body []byte
		var code string
		body, code, err = k.doRequestAt(ctx, host, method, path)
		if err == nil {
			if i > 0 {
				k.pinHost(host)
			}
			return body, nil
		}
		if code != kucoinCodeKeyUnknown {
			return nil, err
		}
	}
	if len(hosts) > 1 {
		return nil, fmt.Errorf("API key unknown on every KuCoin site (%s): %w", kucoinHostNames(hosts), err)
	}
	return nil, err
}

// doRequestAt returns the KuCoin code of a refusal alongside the error: auth
// failures come back as HTTP 401 carrying the usual {code,msg} envelope.
func (k *KuCoin) doRequestAt(ctx context.Context, host, method, path string) ([]byte, string, error) {
	body, httpErr := retryHTTP(k.base.Client, func() (*http.Request, error) {
		timestamp := strconv.FormatInt(time.Now().UnixMilli(), 10)
		signature := k.hmacSign(timestamp + method + path)

		req, err := http.NewRequestWithContext(ctx, method, host+path, nil)
		if err != nil {
			return nil, err
		}

		req.Header.Set("KC-API-KEY", k.base.APIKey)
		req.Header.Set("KC-API-SIGN", signature)
		req.Header.Set("KC-API-TIMESTAMP", timestamp)
		req.Header.Set("KC-API-PASSPHRASE", k.signedPassphrase())
		req.Header.Set("KC-API-KEY-VERSION", "2")
		req.Header.Set("Content-Type", "application/json")
		return req, nil
	})

	var result struct {
		Code string `json:"code"`
		Msg  string `json:"msg"`
	}
	if json.Unmarshal(body, &result) != nil || result.Code == "" {
		if httpErr != nil {
			return nil, "", httpErr
		}
		return nil, "", fmt.Errorf("kucoin API error: unreadable response: %s", TruncatedBody(body))
	}
	if result.Code != "200000" {
		return nil, result.Code, kucoinAPIError(result.Code, result.Msg, httpErr)
	}
	if httpErr != nil {
		return nil, "", httpErr
	}
	return body, "", nil
}

func kucoinAPIError(code, msg string, httpErr error) error {
	detail := vendorErrorDetail(msg)
	if code == "400006" {
		return fmt.Errorf("%w: kucoin API error: %s (code %s)", ErrIPRestricted, detail, code)
	}
	err := fmt.Errorf("kucoin API error: %s (code %s)", detail, code)
	if hint, ok := kucoinCodeHints[code]; ok {
		err = fmt.Errorf("kucoin API error: %s (code %s: %s)", detail, code, hint)
	}
	if errors.Is(httpErr, ErrTransient) {
		return fmt.Errorf("%w: %w", ErrTransient, err)
	}
	return err
}

func kucoinHostNames(hosts []string) string {
	names := make([]string, 0, len(hosts))
	for _, h := range hosts {
		names = append(names, strings.TrimPrefix(strings.TrimPrefix(h, "https://"), "http://"))
	}
	return strings.Join(names, ", ")
}

func (k *KuCoin) TestConnection(ctx context.Context) error {
	_, err := k.doRequest(ctx, "GET", "/api/v1/accounts?type=trade")
	return err
}

func (k *KuCoin) GetBalance(ctx context.Context) (*Balance, error) {
	// No type filter: the old "?type=trade" hid the main (funding) sub-account
	// entirely, so even STABLECOINS parked there were dropped from equity.
	// One call returns every sub-account, one row per (currency, type).
	body, err := k.doRequest(ctx, "GET", "/api/v1/accounts")
	if err != nil {
		return nil, fmt.Errorf("balance: %w", err)
	}

	var resp struct {
		Data []struct {
			Currency  string `json:"currency"`
			Type      string `json:"type"`
			Balance   string `json:"balance"`
			Available string `json:"available"`
			Holds     string `json:"holds"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("parse balance: %w", err)
	}

	// CONN-12: value every holding, not just the stablecoins. Only the spot
	// wallets count — "margin" rows would double-count borrowed collateral.
	perCurrency := map[string]float64{}
	totalAvailable := 0.0
	hasNonStable := false
	for _, a := range resp.Data {
		if a.Type != "trade" && a.Type != "main" {
			continue
		}
		bal, _ := strconv.ParseFloat(a.Balance, 64)
		if bal <= 0 {
			continue
		}
		asset := strings.ToUpper(a.Currency)
		perCurrency[asset] += bal
		if IsStablecoinUSD(asset) {
			avail, _ := strconv.ParseFloat(a.Available, 64)
			totalAvailable += avail
		} else {
			hasNonStable = true
		}
	}
	var holdings []SpotHolding
	for asset, amount := range perCurrency {
		holdings = append(holdings, SpotHolding{Asset: asset, Amount: amount})
	}
	priceMap := map[string]float64{}
	if hasNonStable {
		pm, perr := k.fetchPriceMap(ctx)
		if perr != nil {
			return nil, fmt.Errorf("%w: kucoin all tickers: %v", ErrSpotPricingUnavailable, perr)
		}
		priceMap = pm
	}
	totalEquity := ValueSpotHoldingsUSD(holdings, priceMap)

	return &Balance{
		Equity:    totalEquity,
		Available: totalAvailable,
		Currency:  "USDT",
	}, nil
}

func (k *KuCoin) GetPositions(ctx context.Context) ([]*Position, error) {
	body, err := k.doRequest(ctx, "GET", "/api/v1/positions")
	if err != nil {
		// KuCoin spot-only accounts may not have futures enabled
		return nil, nil
	}

	var resp struct {
		Data []struct {
			Symbol        string `json:"symbol"`
			CurrentQty    int64  `json:"currentQty"`
			AvgEntryPrice string `json:"avgEntryPrice"`
			MarkPrice     string `json:"markPrice"`
			UnrealisedPnl string `json:"unrealisedPnl"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}

	positions := make([]*Position, 0, len(resp.Data))
	for _, p := range resp.Data {
		if p.CurrentQty == 0 {
			continue
		}
		entry, _ := strconv.ParseFloat(p.AvgEntryPrice, 64)
		mark, _ := strconv.ParseFloat(p.MarkPrice, 64)
		upl, _ := strconv.ParseFloat(p.UnrealisedPnl, 64)

		side := "long"
		size := float64(p.CurrentQty)
		if p.CurrentQty < 0 {
			side = "short"
			size = -size
		}

		positions = append(positions, &Position{
			Symbol:        p.Symbol,
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

func (k *KuCoin) GetTrades(ctx context.Context, start, end time.Time) ([]*Trade, error) {
	path := fmt.Sprintf("/api/v1/fills?startAt=%d&endAt=%d&pageSize=500",
		start.UnixMilli(), end.UnixMilli())

	body, err := k.doRequest(ctx, "GET", path)
	if err != nil {
		return nil, err
	}

	var resp struct {
		Data struct {
			Items []struct {
				TradeID   string `json:"tradeId"`
				Symbol    string `json:"symbol"`
				Side      string `json:"side"`
				Price     string `json:"price"`
				Size      string `json:"size"`
				Fee       string `json:"fee"`
				FeeCcy    string `json:"feeCurrency"`
				CreatedAt int64  `json:"createdAt"`
			} `json:"items"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}

	trades := make([]*Trade, 0, len(resp.Data.Items))
	for _, t := range resp.Data.Items {
		price, _ := strconv.ParseFloat(t.Price, 64)
		qty, _ := strconv.ParseFloat(t.Size, 64)
		fee, _ := strconv.ParseFloat(t.Fee, 64)

		trades = append(trades, &Trade{
			ID:          t.TradeID,
			Symbol:      t.Symbol,
			Side:        strings.ToLower(t.Side),
			Price:       price,
			Quantity:    qty,
			Fee:         fee,
			FeeCurrency: t.FeeCcy,
			Timestamp:   time.UnixMilli(t.CreatedAt),
			MarketType:  MarketSpot,
		})
	}

	return trades, nil
}

// GetCashflows returns nil — not reliably available on KuCoin.
func (k *KuCoin) GetCashflows(_ context.Context, _ time.Time) ([]*Cashflow, error) {
	return nil, nil
}

// fetchPriceMap loads every spot pair in one public call, keyed Binance-style
// (BTC-USDT -> BTCUSDT).
func (k *KuCoin) fetchPriceMap(ctx context.Context) (map[string]float64, error) {
	body, err := retryHTTP(k.base.Client, func() (*http.Request, error) {
		return http.NewRequestWithContext(ctx, "GET", k.baseURL()+"/api/v1/market/allTickers", nil)
	})
	if err != nil {
		return nil, err
	}
	var resp struct {
		Data struct {
			Ticker []struct {
				Symbol string          `json:"symbol"`
				Last   json.RawMessage `json:"last"`
			} `json:"ticker"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}
	prices := make(map[string]float64, len(resp.Data.Ticker))
	for _, t := range resp.Data.Ticker {
		if p := ParseLooseNumber(t.Last); p > 0 {
			prices[strings.ToUpper(strings.ReplaceAll(t.Symbol, "-", ""))] = p
		}
	}
	return prices, nil
}
