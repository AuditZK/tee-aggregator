package connector

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

const krakenAPI = "https://api.kraken.com"

// Kraken implements Connector for Kraken exchange.
type Kraken struct {
	apiKey    string
	apiSecret string
	client    *http.Client
}

// NewKraken creates a new Kraken connector.
func NewKraken(creds *Credentials) *Kraken {
	return &Kraken{
		apiKey:    creds.APIKey,
		apiSecret: creds.APISecret,
		client:    &http.Client{Timeout: 30 * time.Second},
	}
}

func (k *Kraken) Exchange() string {
	return "kraken"
}

func (k *Kraken) sign(path, nonce, postData string) (string, error) {
	decodedSecret, err := base64.StdEncoding.DecodeString(k.apiSecret)
	if err != nil {
		return "", fmt.Errorf("decode kraken api secret (must be base64): %w", err)
	}

	sha := sha256.New()
	sha.Write([]byte(nonce + postData))
	shaSum := sha.Sum(nil)

	mac := hmac.New(sha512.New, decodedSecret)
	mac.Write([]byte(path))
	mac.Write(shaSum)

	return base64.StdEncoding.EncodeToString(mac.Sum(nil)), nil
}

func (k *Kraken) doPrivate(ctx context.Context, path string, params url.Values) ([]byte, error) {
	if params == nil {
		params = url.Values{}
	}

	nonce := strconv.FormatInt(time.Now().UnixNano()/int64(time.Millisecond), 10)
	params.Set("nonce", nonce)
	postData := params.Encode()

	signature, err := k.sign(path, nonce, postData)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, krakenAPI+path, strings.NewReader(postData))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("API-Key", k.apiKey)
	req.Header.Set("API-Sign", signature)

	resp, err := k.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("kraken API status %d: %s", resp.StatusCode, string(body))
	}

	var envelope struct {
		Error []string `json:"error"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return nil, err
	}
	if len(envelope.Error) > 0 {
		return nil, fmt.Errorf("kraken API error: %s", strings.Join(envelope.Error, ", "))
	}

	return body, nil
}

func (k *Kraken) TestConnection(ctx context.Context) error {
	_, err := k.doPrivate(ctx, "/0/private/Balance", nil)
	return err
}

func (k *Kraken) GetBalance(ctx context.Context) (*Balance, error) {
	body, err := k.doPrivate(ctx, "/0/private/Balance", nil)
	if err != nil {
		return nil, err
	}

	var resp struct {
		Result map[string]string `json:"result"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}

	var usdLikeTotal float64
	for asset, amountStr := range resp.Result {
		if !isUSDLikeKrakenAsset(asset) {
			continue
		}
		amount, _ := strconv.ParseFloat(amountStr, 64)
		usdLikeTotal += amount
	}

	// Fallback when account has no USD-like assets.
	if usdLikeTotal == 0 {
		for _, amountStr := range resp.Result {
			amount, _ := strconv.ParseFloat(amountStr, 64)
			usdLikeTotal += amount
		}
	}

	return &Balance{
		Available: usdLikeTotal,
		Equity:    usdLikeTotal,
		Currency:  "USD",
	}, nil
}

func isUSDLikeKrakenAsset(asset string) bool {
	asset = strings.ToUpper(strings.TrimSpace(asset))
	switch asset {
	case "ZUSD", "USD", "USDT", "USDC", "USDS", "USDP":
		return true
	default:
		return false
	}
}

func (k *Kraken) GetPositions(ctx context.Context) ([]*Position, error) {
	params := url.Values{}
	params.Set("docalcs", "true")

	body, err := k.doPrivate(ctx, "/0/private/OpenPositions", params)
	if err != nil {
		return nil, err
	}

	var resp struct {
		Result map[string]struct {
			Pair string `json:"pair"`
			Type string `json:"type"` // buy/sell
			Vol  string `json:"vol"`
			Cost string `json:"cost"`
			Net  string `json:"net"`
		} `json:"result"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}

	positions := make([]*Position, 0, len(resp.Result))
	for _, p := range resp.Result {
		vol, _ := strconv.ParseFloat(p.Vol, 64)
		if vol == 0 {
			continue
		}

		size := math.Abs(vol)
		cost, _ := strconv.ParseFloat(p.Cost, 64)
		entryPrice := 0.0
		if size > 0 {
			entryPrice = cost / size
		}
		net, _ := strconv.ParseFloat(p.Net, 64)

		side := "long"
		if strings.EqualFold(p.Type, "sell") || vol < 0 {
			side = "short"
		}

		positions = append(positions, &Position{
			Symbol:        p.Pair,
			Side:          side,
			Size:          size,
			EntryPrice:    entryPrice,
			MarkPrice:     entryPrice, // Kraken private endpoint does not return mark price directly.
			UnrealizedPnL: net,
			MarketType:    MarketMargin,
		})
	}

	return positions, nil
}

func (k *Kraken) GetTrades(ctx context.Context, start, end time.Time) ([]*Trade, error) {
	params := url.Values{}
	params.Set("type", "all")
	params.Set("start", strconv.FormatInt(start.Unix(), 10))
	params.Set("end", strconv.FormatInt(end.Unix(), 10))
	params.Set("trades", "true")

	body, err := k.doPrivate(ctx, "/0/private/TradesHistory", params)
	if err != nil {
		return nil, err
	}

	var resp struct {
		Result struct {
			Trades map[string]struct {
				Pair  string  `json:"pair"`
				Type  string  `json:"type"` // buy/sell
				Price string  `json:"price"`
				Vol   string  `json:"vol"`
				Fee   string  `json:"fee"`
				Time  float64 `json:"time"`
			} `json:"trades"`
		} `json:"result"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}

	trades := make([]*Trade, 0, len(resp.Result.Trades))
	for id, t := range resp.Result.Trades {
		price, _ := strconv.ParseFloat(t.Price, 64)
		qty, _ := strconv.ParseFloat(t.Vol, 64)
		fee, _ := strconv.ParseFloat(t.Fee, 64)

		trades = append(trades, &Trade{
			ID:          id,
			Symbol:      t.Pair,
			Side:        strings.ToLower(t.Type),
			Price:       price,
			Quantity:    qty,
			Fee:         fee,
			FeeCurrency: "USD",
			RealizedPnL: 0,
			Timestamp:   time.Unix(int64(t.Time), int64((t.Time-math.Floor(t.Time))*1e9)).UTC(),
			MarketType:  MarketSpot,
		})
	}

	sort.Slice(trades, func(i, j int) bool {
		return trades[i].Timestamp.Before(trades[j].Timestamp)
	})

	return trades, nil
}
