package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	alpacaLiveAPI  = "https://api.alpaca.markets"
	alpacaPaperAPI = "https://paper-api.alpaca.markets"
	alpacaDataAPI  = "https://data.alpaca.markets"
)

// Alpaca implements Connector for Alpaca Markets
type Alpaca struct {
	apiKey    string
	apiSecret string
	client    *http.Client
	baseURL   string

	// capabilityWarnings carries what the last cashflow read found that it
	// could not book as capital — securities crossing the account boundary
	// with no cash leg.
	capabilityWarnings []string
}

// NewAlpaca creates a new Alpaca connector
func NewAlpaca(creds *Credentials) *Alpaca {
	baseURL := alpacaLiveAPI
	// Use paper trading if key starts with "PK"
	if len(creds.APIKey) > 2 && creds.APIKey[:2] == "PK" {
		baseURL = alpacaPaperAPI
	}

	return &Alpaca{
		apiKey:    creds.APIKey,
		apiSecret: creds.APISecret,
		client:    &http.Client{Timeout: 30 * time.Second},
		baseURL:   baseURL,
	}
}

func (a *Alpaca) Exchange() string {
	return "alpaca"
}

// DetectIsPaper reports whether credentials target Alpaca paper trading.
// TS parity: keys prefixed with "PK" indicate paper accounts.
func (a *Alpaca) DetectIsPaper(_ context.Context) (bool, error) {
	return strings.HasPrefix(strings.ToUpper(strings.TrimSpace(a.apiKey)), "PK"), nil
}

// CONN-15e: routed through the shared retry policy so a 429 or 5xx blip is
// retried and, if it persists, marked ErrTransient — the difference between a
// sync that retries and a snapshot written without this account.
func (a *Alpaca) doRequest(ctx context.Context, baseURL, path string) ([]byte, error) {
	body, err := retryHTTP(a.client, func() (*http.Request, error) {
		req, rerr := http.NewRequestWithContext(ctx, "GET", baseURL+path, nil)
		if rerr != nil {
			return nil, rerr
		}
		req.Header.Set("APCA-API-KEY-ID", a.apiKey)
		req.Header.Set("APCA-API-SECRET-KEY", a.apiSecret)
		return req, nil
	})
	if err != nil {
		return nil, fmt.Errorf("alpaca API error: %w", err)
	}
	return body, nil
}

func (a *Alpaca) TestConnection(ctx context.Context) error {
	_, err := a.doRequest(ctx, a.baseURL, "/v2/account")
	return err
}

func (a *Alpaca) GetBalance(ctx context.Context) (*Balance, error) {
	body, err := a.doRequest(ctx, a.baseURL, "/v2/account")
	if err != nil {
		return nil, err
	}

	var resp struct {
		Cash           string `json:"cash"`
		PortfolioValue string `json:"portfolio_value"`
		Equity         string `json:"equity"`
		BuyingPower    string `json:"buying_power"`
	}

	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}

	equity, _ := strconv.ParseFloat(resp.Equity, 64)
	cash, _ := strconv.ParseFloat(resp.Cash, 64)

	// Alpaca's /v2/account response does not expose an aggregate
	// unrealized_pl field (unlike most exchanges). The per-position
	// unrealized_pl is only available from /v2/positions. Fetch it
	// here so balance.UnrealizedPnL is accurate and sync.go can
	// derive realizedBalance = equity - unrealized correctly.
	unrealized := 0.0
	positions, posErr := a.GetPositions(ctx)
	if posErr == nil {
		for _, p := range positions {
			unrealized += p.UnrealizedPnL
		}
	}

	return &Balance{
		Available:     cash,
		Equity:        equity,
		UnrealizedPnL: unrealized,
		Currency:      "USD",
	}, nil
}

func (a *Alpaca) GetPositions(ctx context.Context) ([]*Position, error) {
	body, err := a.doRequest(ctx, a.baseURL, "/v2/positions")
	if err != nil {
		return nil, err
	}

	var resp []struct {
		Symbol        string `json:"symbol"`
		Qty           string `json:"qty"`
		Side          string `json:"side"`
		AvgEntryPrice string `json:"avg_entry_price"`
		CurrentPrice  string `json:"current_price"`
		UnrealizedPL  string `json:"unrealized_pl"`
		AssetClass    string `json:"asset_class"`
	}

	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}

	var positions []*Position
	for _, p := range resp {
		qty, _ := strconv.ParseFloat(p.Qty, 64)
		if qty == 0 {
			continue
		}

		entry, _ := strconv.ParseFloat(p.AvgEntryPrice, 64)
		current, _ := strconv.ParseFloat(p.CurrentPrice, 64)
		unrealized, _ := strconv.ParseFloat(p.UnrealizedPL, 64)

		side := "long"
		if qty < 0 {
			side = "short"
			qty = -qty
		}

		marketType := "stocks"
		if p.AssetClass == "crypto" {
			marketType = "spot"
		}

		positions = append(positions, &Position{
			Symbol:        p.Symbol,
			Side:          side,
			Size:          qty,
			EntryPrice:    entry,
			MarkPrice:     current,
			UnrealizedPnL: unrealized,
			MarketType:    marketType,
		})
	}

	return positions, nil
}

func (a *Alpaca) GetTrades(ctx context.Context, start, end time.Time) ([]*Trade, error) {
	path := fmt.Sprintf("/v2/account/activities/FILL?after=%s&until=%s",
		start.Format(time.RFC3339), end.Format(time.RFC3339))

	body, err := a.doRequest(ctx, a.baseURL, path)
	if err != nil {
		return nil, err
	}

	var resp []struct {
		ID              string `json:"id"`
		Symbol          string `json:"symbol"`
		Side            string `json:"side"`
		Price           string `json:"price"`
		Qty             string `json:"qty"`
		TransactionTime string `json:"transaction_time"`
	}

	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}

	var trades []*Trade
	for _, t := range resp {
		price, _ := strconv.ParseFloat(t.Price, 64)
		qty, _ := strconv.ParseFloat(t.Qty, 64)
		ts, _ := time.Parse(time.RFC3339, t.TransactionTime)

		trades = append(trades, &Trade{
			ID:          t.ID,
			Symbol:      t.Symbol,
			Side:        t.Side,
			Price:       price,
			Quantity:    qty,
			Fee:         0, // Alpaca is commission-free
			FeeCurrency: "USD",
			Timestamp:   ts,
			MarketType:  "stocks",
		})
	}

	return trades, nil
}

// alpacaCapitalActivityTypes are the non-trade activities that move money
// across the account boundary: cash deposit, cash withdrawal, a cash journal
// between two Alpaca accounts, and the cash leg of an ACATS transfer.
var alpacaCapitalActivityTypes = []string{"CSD", "CSW", "JNLC", "ACATC"}

// alpacaSecurityTransferTypes move holdings with no cash leg. Their market
// value arrives through the positions and lands in equity, where nothing
// distinguishes it from a gain — the same channel that let an IBKR account
// book an incoming portfolio as performance. They carry no amount to book as
// a cashflow, so they are reported rather than dropped.
var alpacaSecurityTransferTypes = []string{"ACATS", "JNLS"}

// alpacaActivityMaxPages bounds the walk. At 100 entries per page this covers
// 10k activities, far past any real funding history, and stops a paging bug or
// a hostile page_token from looping forever.
const alpacaActivityMaxPages = 100

// GetCashflows returns deposits and withdrawals from the account activity
// ledger.
//
// This used to return nothing at all, on the belief that Alpaca exposed no
// capital flows. It does, under /v2/account/activities, and the cost of the
// mistake was total: every deposit reaching a live-synced Alpaca account
// raised the equity with no recorded inflow, and the step read as a gain.
func (a *Alpaca) GetCashflows(ctx context.Context, since time.Time) ([]*Cashflow, error) {
	flows, err := a.fetchActivities(ctx, since, alpacaCapitalActivityTypes, func(act alpacaActivity) *Cashflow {
		amount, err := strconv.ParseFloat(act.NetAmount, 64)
		if err != nil || amount == 0 {
			return nil
		}
		ts, ok := alpacaActivityTime(act)
		if !ok {
			return nil
		}
		return &Cashflow{Amount: amount, Currency: "USD", Timestamp: ts}
	})
	if err != nil {
		return nil, err
	}

	a.noteSecurityTransfers(ctx, since)
	return flows, nil
}

// noteSecurityTransfers records that holdings crossed the account boundary
// without cash. Failure to read them is not failure to read the cashflows, so
// it leaves the warning unset rather than the deposits unreturned.
func (a *Alpaca) noteSecurityTransfers(ctx context.Context, since time.Time) {
	a.capabilityWarnings = nil
	transfers, err := a.fetchActivities(ctx, since, alpacaSecurityTransferTypes, func(act alpacaActivity) *Cashflow {
		return &Cashflow{}
	})
	if err != nil || len(transfers) == 0 {
		return
	}
	a.capabilityWarnings = append(a.capabilityWarnings,
		fmt.Sprintf("security_transfer_not_capital:%d", len(transfers)))
}

func (a *Alpaca) CapabilityWarnings() []string {
	return a.capabilityWarnings
}

// GetRawCashflowEntries returns every activity the account has, whatever its
// type, with the venue's own label attached.
//
// GetCashflows above asks for the four types that move capital, which is what
// a sync needs and all it should book. It is not what a question needs: an
// account whose equity jumped on a day with no deposit leaves nothing behind
// to look at, and the answer — a fill, an option assignment, a dividend, a
// journal — lives in the types the filter drops. Asking without a filter is
// the difference between deducing an account's activity from its equity and
// reading it.
func (a *Alpaca) GetRawCashflowEntries(ctx context.Context, since time.Time) ([]RawBalanceOp, error) {
	var ops []RawBalanceOp
	_, err := a.fetchActivities(ctx, since, nil, func(act alpacaActivity) *Cashflow {
		ts, ok := alpacaActivityTime(act)
		if !ok {
			return nil
		}
		label := act.ActivityType
		if act.Side != "" {
			label += "/" + act.Side
		}
		amount, _ := strconv.ParseFloat(act.NetAmount, 64)
		notional := 0.0
		if qty, price := parseAlpacaFloat(act.Qty), parseAlpacaFloat(act.Price); qty != 0 && price != 0 {
			notional = qty * price
		}
		ops = append(ops, RawBalanceOp{
			Label:         label,
			Delta:         amount,
			PositionValue: notional,
			Currency:      "USD",
			Symbol:        act.Symbol,
			Timestamp:     ts,
		})
		// The walk's own return value is unused here: every activity is kept,
		// and keeping them is this collector's job.
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(ops, func(i, j int) bool { return ops[i].Timestamp.Before(ops[j].Timestamp) })
	return ops, nil
}

func parseAlpacaFloat(s string) float64 {
	v, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if err != nil {
		return 0
	}
	return v
}

type alpacaActivity struct {
	ID              string `json:"id"`
	ActivityType    string `json:"activity_type"`
	Date            string `json:"date"`
	TransactionTime string `json:"transaction_time"`
	NetAmount       string `json:"net_amount"`
	Symbol          string `json:"symbol"`
	Side            string `json:"side"`
	Qty             string `json:"qty"`
	Price           string `json:"price"`
}

func alpacaActivityTime(act alpacaActivity) (time.Time, bool) {
	if act.TransactionTime != "" {
		if ts, err := time.Parse(time.RFC3339, act.TransactionTime); err == nil {
			return ts.UTC(), true
		}
	}
	if act.Date != "" {
		if ts, err := time.Parse("2006-01-02", act.Date); err == nil {
			return ts.UTC(), true
		}
	}
	return time.Time{}, false
}

// fetchActivities walks the activity ledger for the given types, following
// page_token until the venue stops returning entries.
func (a *Alpaca) fetchActivities(
	ctx context.Context,
	since time.Time,
	types []string,
	convert func(alpacaActivity) *Cashflow,
) ([]*Cashflow, error) {
	var (
		out       []*Cashflow
		pageToken string
	)

	for page := 0; page < alpacaActivityMaxPages; page++ {
		path := fmt.Sprintf("/v2/account/activities?after=%s&page_size=100",
			url.QueryEscape(since.UTC().Format(time.RFC3339)))
		if len(types) > 0 {
			path += "&activity_types=" + strings.Join(types, ",")
		}
		if pageToken != "" {
			path += "&page_token=" + url.QueryEscape(pageToken)
		}

		body, err := a.doRequest(ctx, a.baseURL, path)
		if err != nil {
			return nil, fmt.Errorf("fetch account activities: %w", err)
		}

		var batch []alpacaActivity
		if err := json.Unmarshal(body, &batch); err != nil {
			return nil, fmt.Errorf("parse account activities: %w", err)
		}
		if len(batch) == 0 {
			return out, nil
		}

		for _, act := range batch {
			if cf := convert(act); cf != nil {
				out = append(out, cf)
			}
		}
		pageToken = batch[len(batch)-1].ID
	}

	return out, nil
}
