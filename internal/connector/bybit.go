package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

const bybitAPI = "https://api.bybit.com"

// Bybit implements Connector for Bybit exchange
type Bybit struct {
	apiKey    string
	apiSecret string
	client    *http.Client
	baseURL   string

	mu               sync.Mutex
	cashflowWarnings []string
}

// NewBybit creates a new Bybit connector
func NewBybit(creds *Credentials) *Bybit {
	return &Bybit{
		apiKey:    creds.APIKey,
		apiSecret: creds.APISecret,
		client:    &http.Client{Timeout: 30 * time.Second},
		baseURL:   bybitAPI,
	}
}

// NewBybitWithClient creates a Bybit connector using the provided HTTP client.
// Used to route through the egress proxy: api.bybit.com sits behind CloudFront
// with a country block that returns 403 to the enclave's region (verified
// 2026-08-08) — reached directly, every call fails before authentication and
// used to be misreported as bad credentials.
func NewBybitWithClient(creds *Credentials, client *http.Client) *Bybit {
	return &Bybit{
		apiKey:    creds.APIKey,
		apiSecret: creds.APISecret,
		client:    client,
		baseURL:   bybitAPI,
	}
}

func (b *Bybit) Exchange() string {
	return "bybit"
}

func (b *Bybit) sign(timestamp, params string) string {
	return signHMACHex(b.apiSecret, timestamp+b.apiKey+"5000"+params)
}

func (b *Bybit) doRequest(ctx context.Context, method, path, params string) ([]byte, error) {
	body, err := retryHTTP(b.client, func() (*http.Request, error) {
		timestamp := strconv.FormatInt(time.Now().UnixMilli(), 10)
		signature := b.sign(timestamp, params)

		url := b.baseURL + path
		if params != "" {
			url += "?" + params
		}

		req, err := http.NewRequestWithContext(ctx, method, url, nil)
		if err != nil {
			return nil, err
		}

		req.Header.Set("X-BAPI-API-KEY", b.apiKey)
		req.Header.Set("X-BAPI-TIMESTAMP", timestamp)
		req.Header.Set("X-BAPI-SIGN", signature)
		req.Header.Set("X-BAPI-RECV-WINDOW", "5000")
		return req, nil
	})
	if err != nil {
		return nil, err
	}

	var result struct {
		RetCode int    `json:"retCode"`
		RetMsg  string `json:"retMsg"`
	}
	json.Unmarshal(body, &result)
	if result.RetCode != 0 {
		return nil, fmt.Errorf("bybit API error: %s", vendorErrorDetail(result.RetMsg))
	}

	return body, nil
}

func (b *Bybit) TestConnection(ctx context.Context) error {
	_, err := b.doRequest(ctx, "GET", "/v5/account/wallet-balance", "accountType=UNIFIED")
	return err
}

func (b *Bybit) GetBalance(ctx context.Context) (*Balance, error) {
	body, err := b.doRequest(ctx, "GET", "/v5/account/wallet-balance", "accountType=UNIFIED")
	if err != nil {
		return nil, err
	}

	var resp struct {
		Result struct {
			List []struct {
				TotalEquity           string `json:"totalEquity"`
				TotalWalletBalance    string `json:"totalWalletBalance"`
				TotalPerpUPL          string `json:"totalPerpUPL"`
				TotalAvailableBalance string `json:"totalAvailableBalance"`
			} `json:"list"`
		} `json:"result"`
	}

	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}

	if len(resp.Result.List) == 0 {
		return &Balance{Currency: "USDT"}, nil
	}

	account := resp.Result.List[0]
	equity, _ := strconv.ParseFloat(account.TotalEquity, 64)
	available, _ := strconv.ParseFloat(account.TotalAvailableBalance, 64)
	unrealized, _ := strconv.ParseFloat(account.TotalPerpUPL, 64)

	return &Balance{
		Available:     available,
		Equity:        equity,
		UnrealizedPnL: unrealized,
		Currency:      "USDT",
	}, nil
}

func (b *Bybit) GetPositions(ctx context.Context) ([]*Position, error) {
	body, err := b.doRequest(ctx, "GET", "/v5/position/list", "category=linear&settleCoin=USDT")
	if err != nil {
		return nil, err
	}

	var resp struct {
		Result struct {
			List []struct {
				Symbol        string `json:"symbol"`
				Side          string `json:"side"`
				Size          string `json:"size"`
				AvgPrice      string `json:"avgPrice"`
				MarkPrice     string `json:"markPrice"`
				UnrealisedPnl string `json:"unrealisedPnl"`
			} `json:"list"`
		} `json:"result"`
	}

	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}

	var positions []*Position
	for _, p := range resp.Result.List {
		size, _ := strconv.ParseFloat(p.Size, 64)
		if size == 0 {
			continue
		}

		entry, _ := strconv.ParseFloat(p.AvgPrice, 64)
		mark, _ := strconv.ParseFloat(p.MarkPrice, 64)
		unrealized, _ := strconv.ParseFloat(p.UnrealisedPnl, 64)

		side := "long"
		if p.Side == "Sell" {
			side = "short"
		}

		positions = append(positions, &Position{
			Symbol:        p.Symbol,
			Side:          side,
			Size:          size,
			EntryPrice:    entry,
			MarkPrice:     mark,
			UnrealizedPnL: unrealized,
			MarketType:    "swap",
		})
	}

	return positions, nil
}

// Bybit caps one execution page at 100 fills, so a busy day spans several
// pages; the page budget bounds a runaway cursor, not a real account. The
// endpoint refuses a range wider than seven days.
const (
	bybitFillPageLimit = 100
	bybitMaxFillPages  = 50
	bybitExecWindow    = 7 * 24 * time.Hour
)

// bybitExecFunding marks a funding settlement in the execution list: it
// carries the whole position as its quantity and never changes it.
const bybitExecFunding = "Funding"

type bybitExecution struct {
	ExecID    string `json:"execId"`
	Symbol    string `json:"symbol"`
	Side      string `json:"side"`
	ExecType  string `json:"execType"`
	ExecPrice string `json:"execPrice"`
	ExecQty   string `json:"execQty"`
	ExecFee   string `json:"execFee"`
	ExecTime  string `json:"execTime"`
	ClosedPnl string `json:"closedPnl"`
}

// executions reads the linear execution list over [start, end], one window of
// at most seven days at a time, following each window's cursor. An empty
// execType reads every type.
func (b *Bybit) executions(ctx context.Context, start, end time.Time, execType string) ([]bybitExecution, error) {
	var out []bybitExecution
	seen := map[string]bool{}
	requests := 0
	for winStart := start; winStart.Before(end); winStart = winStart.Add(bybitExecWindow) {
		winEnd := winStart.Add(bybitExecWindow)
		if winEnd.After(end) {
			winEnd = end
		}

		cursor := ""
		for page := 0; page < bybitMaxFillPages; page++ {
			if requests > 0 {
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(bybitLogPagePace):
				}
			}
			requests++

			params := fmt.Sprintf("category=linear&startTime=%d&endTime=%d&limit=%d",
				winStart.UnixMilli(), winEnd.UnixMilli(), bybitFillPageLimit)
			if execType != "" {
				params += "&execType=" + execType
			}
			if cursor != "" {
				params += "&cursor=" + cursor
			}

			body, err := b.doRequest(ctx, "GET", "/v5/execution/list", params)
			if err != nil {
				return nil, err
			}

			var resp struct {
				Result struct {
					NextPageCursor string           `json:"nextPageCursor"`
					List           []bybitExecution `json:"list"`
				} `json:"result"`
			}
			if err := json.Unmarshal(body, &resp); err != nil {
				return nil, fmt.Errorf("decode bybit executions: %w", err)
			}

			// Adjacent windows share their boundary millisecond.
			for _, e := range resp.Result.List {
				if e.ExecID != "" && seen[e.ExecID] {
					continue
				}
				seen[e.ExecID] = true
				out = append(out, e)
			}

			cursor = resp.Result.NextPageCursor
			if cursor == "" || len(resp.Result.List) == 0 {
				break
			}
		}
	}
	return out, nil
}

func (b *Bybit) GetTrades(ctx context.Context, start, end time.Time) ([]*Trade, error) {
	execs, err := b.executions(ctx, start, end, "")
	if err != nil {
		return nil, err
	}

	var trades []*Trade
	for _, t := range execs {
		if t.ExecType == bybitExecFunding {
			continue
		}
		price, _ := strconv.ParseFloat(t.ExecPrice, 64)
		qty, _ := strconv.ParseFloat(t.ExecQty, 64)
		fee, _ := strconv.ParseFloat(t.ExecFee, 64)
		pnl, _ := strconv.ParseFloat(t.ClosedPnl, 64)
		execTime, _ := strconv.ParseInt(t.ExecTime, 10, 64)

		trades = append(trades, &Trade{
			ID:          t.ExecID,
			Symbol:      t.Symbol,
			Side:        strings.ToLower(t.Side),
			Price:       price,
			Quantity:    qty,
			Fee:         fee,
			FeeCurrency: "USDT",
			RealizedPnL: pnl,
			Timestamp:   time.UnixMilli(execTime),
			MarketType:  "swap",
		})
	}

	return trades, nil
}

// GetFundingFees reads the account's funding settlements. symbols is ignored:
// a position held through a day with no fill still pays funding on it.
func (b *Bybit) GetFundingFees(ctx context.Context, _ []string, since time.Time) ([]*FundingFee, error) {
	execs, err := b.executions(ctx, since, time.Now(), bybitExecFunding)
	if err != nil {
		return nil, err
	}

	var fees []*FundingFee
	for _, e := range execs {
		if e.ExecType != bybitExecFunding {
			continue
		}
		fee, _ := strconv.ParseFloat(e.ExecFee, 64)
		if fee == 0 {
			continue
		}
		ms, _ := strconv.ParseInt(e.ExecTime, 10, 64)
		// execFee is positive when the account paid; the contract is
		// negative when charged.
		fees = append(fees, &FundingFee{
			Amount:    -fee,
			Symbol:    e.Symbol,
			Timestamp: time.UnixMilli(ms).UTC(),
		})
	}
	return fees, nil
}
