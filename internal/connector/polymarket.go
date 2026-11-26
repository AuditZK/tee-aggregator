package connector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	polymarketDataAPI  = "https://data-api.polymarket.com/v2"
	polymarketPageSize = 1000
	// A wallet that outgrows either cap fails the read instead of being
	// measured on a truncated feed.
	polymarketMaxFlowPages  = 20
	polymarketMaxTradePages = 300
	polymarketMaxPosPages   = 20
	// Below a dollar a negative equity is rounding in the venue's own series.
	polymarketEquityDust = 1.0
)

// ErrPolymarketUnreconciled marks a wallet whose collateral transfers and PnL
// add up to a negative equity: value reached it by a route the activity feed
// does not state, typically outcome tokens received from another wallet.
var ErrPolymarketUnreconciled = errors.New("polymarket: transfers and pnl do not reconcile to an equity")

var polymarketWallet = regexp.MustCompile(`^0x[0-9a-fA-F]{40}$`)

// Polymarket is a read-only connector for a Polymarket predictions wallet,
// keyed by its public wallet address (stored as the API key).
//
// The data API states positions and PnL but no cash balance. Equity is what
// the venue's own figures add up to: every collateral transfer in or out of
// the wallet plus its cumulative economic PnL, which is cash plus marked
// positions by construction. The venue marks that PnL once an hour, so an
// equity is as of its latest mark. The Perps account is a separate,
// authenticated ledger and is not read.
type Polymarket struct {
	wallet  string
	client  *http.Client
	baseURL string
}

func NewPolymarket(creds *Credentials) *Polymarket {
	addr := creds.WalletAddress
	if addr == "" {
		addr = creds.APIKey
	}
	return &Polymarket{
		wallet:  strings.ToLower(strings.TrimSpace(addr)),
		client:  &http.Client{Timeout: 30 * time.Second},
		baseURL: polymarketDataAPI,
	}
}

func (p *Polymarket) Exchange() string { return "polymarket" }

func (p *Polymarket) DetectIsPaper(_ context.Context) (bool, error) { return false, nil }

func (p *Polymarket) TestConnection(ctx context.Context) error {
	_, err := p.GetBalance(ctx)
	return err
}

func (p *Polymarket) GetBalance(ctx context.Context) (*Balance, error) {
	stats, err := p.fetchStats(ctx)
	if err != nil {
		return nil, err
	}
	flows, err := p.fetchFlows(ctx, time.Time{})
	if err != nil {
		return nil, err
	}
	var equity, unrealized float64
	for _, f := range flows {
		equity += f.amount
	}
	if pnl := stats.AllTimePnL; pnl != nil {
		if pnl.Economic == nil {
			return nil, fmt.Errorf("%w: polymarket has no marked pnl for this wallet yet", ErrTransient)
		}
		equity += *pnl.Economic
		if pnl.Unrealized != nil {
			unrealized = *pnl.Unrealized
		}
	}
	if equity < -polymarketEquityDust {
		return nil, ErrPolymarketUnreconciled
	}
	equity = max(equity, 0)

	held, err := p.fetchPositionsValue(ctx)
	if err != nil {
		return nil, err
	}
	return &Balance{
		Equity:        equity,
		Available:     min(max(equity-held, 0), equity),
		UnrealizedPnL: unrealized,
		Currency:      "USD",
	}, nil
}

func (p *Polymarket) GetPositions(ctx context.Context) ([]*Position, error) {
	q := url.Values{
		"status":        {"OPEN"},
		"filter_type":   {"CASH"},
		"filter_amount": {"0.01"},
		"limit":         {"500"},
	}
	var positions []*Position
	complete, err := walkPolymarket(ctx, p, "/positions", q, polymarketMaxPosPages, func(r polymarketPosition) error {
		if r.CurrentSize <= 0 {
			return nil
		}
		positions = append(positions, &Position{
			Symbol:        polymarketSymbol(r.Slug, r.Outcome, r.TokenID),
			Side:          "long",
			Size:          r.CurrentSize,
			EntryPrice:    r.AvgPrice,
			MarkPrice:     r.CurrentPrice,
			UnrealizedPnL: r.UnrealizedPnL,
			MarketType:    MarketSpot,
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	if !complete {
		return nil, fmt.Errorf("polymarket: more than %d pages of open positions", polymarketMaxPosPages)
	}
	return positions, nil
}

// GetTrades returns the wallet's fills, without fees: the activity feed states
// none, GetNetTradingFees does. A window larger than the page cap comes back
// cut at the cap: trades feed activity counters, never the equity.
func (p *Polymarket) GetTrades(ctx context.Context, start, end time.Time) ([]*Trade, error) {
	q := url.Values{
		"type":           {"TRADE"},
		"start":          {polymarketEpoch(start)},
		"end":            {strconv.FormatInt(end.Unix(), 10)},
		"sort_direction": {"ASC"},
		"limit":          {strconv.Itoa(polymarketPageSize)},
	}
	var trades []*Trade
	_, err := walkPolymarket(ctx, p, "/activity", q, polymarketMaxTradePages, func(r polymarketActivity) error {
		side := strings.ToLower(r.Side)
		if r.Type != "TRADE" || (side != "buy" && side != "sell") {
			return nil
		}
		trades = append(trades, &Trade{
			ID:          r.TransactionHash,
			Symbol:      polymarketSymbol(r.Slug, r.Outcome, r.TokenID),
			Side:        side,
			Price:       r.Price,
			Quantity:    r.Size,
			USDNotional: r.USDCSize,
			FeeCurrency: "USDC",
			Timestamp:   time.Unix(r.Timestamp, 0).UTC(),
			MarketType:  MarketSpot,
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return trades, nil
}

// GetCashflows returns the collateral that entered or left the wallet without
// being traded: bridge deposits and withdrawals, and transfers to or from
// another wallet.
func (p *Polymarket) GetCashflows(ctx context.Context, since time.Time) ([]*Cashflow, error) {
	flows, err := p.fetchFlows(ctx, since)
	if err != nil {
		return nil, err
	}
	out := make([]*Cashflow, 0, len(flows))
	for _, f := range flows {
		out = append(out, &Cashflow{Amount: f.amount, Currency: "USDC", Timestamp: f.at})
	}
	return out, nil
}

// GetHistoricalSnapshots states each past midnight from the venue's daily PnL
// series and the transfers up to it, inside the enclave: the address is all it
// takes. A row dated D holds the equity at 00:00 UTC of D and the activity of
// the day before.
func (p *Polymarket) GetHistoricalSnapshots(ctx context.Context, since time.Time) ([]*HistoricalSnapshot, error) {
	flows, err := p.fetchFlows(ctx, time.Time{})
	if err != nil {
		return nil, err
	}
	var series struct {
		Points []polymarketPnL `json:"points"`
	}
	q := url.Values{"user": {p.wallet}, "interval": {"max"}, "fidelity": {"1d"}}
	if _, err := p.get(ctx, "/user-pnl", q, &series); err != nil {
		return nil, err
	}
	return polymarketDailyRows(flows, series.Points, since, time.Now().UTC())
}

// GetNetTradingFees states the fees between two instants from the venue's PnL
// series, on its hourly grid when the window fits in one.
func (p *Polymarket) GetNetTradingFees(ctx context.Context, since, until time.Time) (float64, error) {
	q := url.Values{"user": {p.wallet}, "interval": {"max"}, "fidelity": {"1d"}}
	switch span := until.Sub(since); {
	case span <= 6*24*time.Hour:
		q.Set("interval", "1w")
		q.Set("fidelity", "1h")
	case span <= 29*24*time.Hour:
		q.Set("interval", "1m")
		q.Set("fidelity", "1h")
	}
	var series struct {
		Points []polymarketPnL `json:"points"`
	}
	if _, err := p.get(ctx, "/user-pnl", q, &series); err != nil {
		return 0, err
	}
	return polymarketFeesBetween(series.Points, since, until)
}

// polymarketFeesBetween is the fee cost between the marks the series holds at
// since and at until. A wallet with no point by since had paid nothing yet.
func polymarketFeesBetween(points []polymarketPnL, since, until time.Time) (float64, error) {
	points = append([]polymarketPnL(nil), points...)
	sort.SliceStable(points, func(i, j int) bool { return points[i].Timestamp < points[j].Timestamp })
	var base, end polymarketPnL
	for _, pt := range points {
		if pt.Timestamp <= since.Unix() {
			base = pt
		}
		if pt.Timestamp <= until.Unix() {
			end = pt
		}
	}
	if end.Timestamp == 0 {
		return 0, nil
	}
	fees, ok := polymarketFeesSince(base, end)
	if !ok {
		return 0, errors.New("polymarket: fees not stated over the window")
	}
	return fees, nil
}

// polymarketFeesSince is the fee cost from mark base to mark at, unknown when
// either does not state its charges. A zero base is the wallet before its
// first mark.
func polymarketFeesSince(base, at polymarketPnL) (float64, bool) {
	cur, ok := at.netFees()
	from, known := base.netFees()
	if !ok || (!known && base.Timestamp != 0) {
		return 0, false
	}
	return cur - from, true
}

type polymarketFlow struct {
	at     time.Time
	amount float64
}

type polymarketActivity struct {
	Timestamp       int64   `json:"timestamp"`
	Type            string  `json:"type"`
	Side            string  `json:"side"`
	Size            float64 `json:"size"`
	USDCSize        float64 `json:"usdc_size"`
	Price           float64 `json:"price"`
	Slug            string  `json:"slug"`
	Outcome         string  `json:"outcome"`
	TokenID         string  `json:"token_id"`
	TransactionHash string  `json:"transaction_hash"`
}

type polymarketPosition struct {
	Slug          string  `json:"slug"`
	Outcome       string  `json:"outcome"`
	TokenID       string  `json:"token_id"`
	CurrentSize   float64 `json:"current_size"`
	AvgPrice      float64 `json:"avg_price"`
	CurrentPrice  float64 `json:"current_price"`
	UnrealizedPnL float64 `json:"unrealized_pnl"`
}

// polymarketPnL is one cumulative point of the venue's PnL series. A null
// amount means the venue could not state it, never zero.
type polymarketPnL struct {
	Timestamp  int64    `json:"timestamp"`
	Economic   *float64 `json:"economic_pnl"`
	Unrealized *float64 `json:"unrealized_pnl"`
	// Fees is the charges, negative; the refunds and rebates are positive.
	Fees         *float64 `json:"fees"`
	FeesRefunded *float64 `json:"fees_refunded"`
	MakerRebate  *float64 `json:"maker_rebate"`
	TakerRebate  *float64 `json:"taker_rebate"`
	VolumeUSDC   float64  `json:"volume_usdc"`
	TradeCount   int      `json:"trade_count"`
}

// netFees is the running fee cost, charges less refunds and rebates, unknown
// when the venue could not state the charges.
func (p polymarketPnL) netFees() (float64, bool) {
	if p.Fees == nil {
		return 0, false
	}
	return -*p.Fees - polymarketDeref(p.FeesRefunded) - polymarketDeref(p.MakerRebate) - polymarketDeref(p.TakerRebate), true
}

type polymarketStats struct {
	AllTimePnL *polymarketPnL `json:"all_time_pnl"`
}

func (p *Polymarket) fetchStats(ctx context.Context) (*polymarketStats, error) {
	var stats *polymarketStats
	if _, err := p.get(ctx, "/user-stats", url.Values{"user": {p.wallet}}, &stats); err != nil {
		return nil, err
	}
	if stats == nil {
		return nil, errors.New("polymarket: no account at this wallet address")
	}
	return stats, nil
}

func (p *Polymarket) fetchPositionsValue(ctx context.Context) (float64, error) {
	var v struct {
		Value float64 `json:"value"`
	}
	if _, err := p.get(ctx, "/value", url.Values{"user": {p.wallet}}, &v); err != nil {
		return 0, err
	}
	return v.Value, nil
}

// fetchFlows returns the wallet's collateral transfers since the given instant,
// oldest first, deposits positive.
func (p *Polymarket) fetchFlows(ctx context.Context, since time.Time) ([]polymarketFlow, error) {
	q := url.Values{
		// TIP, a transfer between two wallets, is served only when named.
		"type":                         {"DEPOSIT,WITHDRAWAL,TIP"},
		"exclude_deposits_withdrawals": {"false"},
		"start":                        {polymarketEpoch(since)},
		"sort_direction":               {"ASC"},
		"limit":                        {strconv.Itoa(polymarketPageSize)},
	}
	var flows []polymarketFlow
	complete, err := walkPolymarket(ctx, p, "/activity", q, polymarketMaxFlowPages, func(r polymarketActivity) error {
		amount := r.USDCSize
		if amount == 0 {
			amount = r.Size
		}
		switch {
		case r.Type == "DEPOSIT", r.Type == "TIP" && strings.EqualFold(r.Side, "IN"):
		case r.Type == "WITHDRAWAL", r.Type == "TIP" && strings.EqualFold(r.Side, "OUT"):
			amount = -amount
		default:
			return fmt.Errorf("polymarket: transfer of type %q has no direction", r.Type)
		}
		if amount != 0 {
			flows = append(flows, polymarketFlow{at: time.Unix(r.Timestamp, 0).UTC(), amount: amount})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if !complete {
		return nil, fmt.Errorf("polymarket: more than %d pages of transfers", polymarketMaxFlowPages)
	}
	sort.SliceStable(flows, func(i, j int) bool { return flows[i].at.Before(flows[j].at) })
	return flows, nil
}

// polymarketDailyRows builds one row per UTC midnight from the first one after
// the wallet's first event to today's. A midnight the series cannot mark is
// left out instead of stated at its settled value.
func polymarketDailyRows(flows []polymarketFlow, points []polymarketPnL, since, now time.Time) ([]*HistoricalSnapshot, error) {
	points = append([]polymarketPnL(nil), points...)
	sort.SliceStable(points, func(i, j int) bool { return points[i].Timestamp < points[j].Timestamp })

	var first time.Time
	if len(flows) > 0 {
		first = flows[0].at
	}
	if len(points) > 0 {
		if at := time.Unix(points[0].Timestamp, 0).UTC(); first.IsZero() || at.Before(first) {
			first = at
		}
	}
	if first.IsZero() {
		return nil, nil
	}
	from := truncUTCDay(since)

	var out []*HistoricalSnapshot
	var net float64
	var at, prev polymarketPnL
	fi, pi := 0, 0
	// A first event on a midnight belongs to that midnight's row.
	for day := truncUTCDay(first.Add(24*time.Hour - time.Second)); !day.After(truncUTCDay(now)); day = day.Add(24 * time.Hour) {
		var deposits, withdrawals float64
		for ; fi < len(flows) && !flows[fi].at.After(day); fi++ {
			net += flows[fi].amount
			if flows[fi].amount > 0 {
				deposits += flows[fi].amount
			} else {
				withdrawals -= flows[fi].amount
			}
		}
		for ; pi < len(points) && points[pi].Timestamp <= day.Unix(); pi++ {
			at = points[pi]
		}
		marked := at.Timestamp == 0 || at.Economic != nil
		row := &HistoricalSnapshot{
			Date:        day,
			Deposits:    deposits,
			Withdrawals: withdrawals,
			TotalTrades: at.TradeCount - prev.TradeCount,
			TotalVolume: at.VolumeUSDC - prev.VolumeUSDC,
		}
		row.TotalFees, _ = polymarketFeesSince(prev, at)
		prev = at
		if !marked || day.Before(from) {
			continue
		}
		equity := net + polymarketDeref(at.Economic)
		if equity < -polymarketEquityDust {
			return nil, ErrPolymarketUnreconciled
		}
		row.TotalEquity = max(equity, 0)
		row.RealizedBalance = row.TotalEquity - polymarketDeref(at.Unrealized)
		out = append(out, row)
	}
	return out, nil
}

func polymarketDeref(v *float64) float64 {
	if v == nil {
		return 0
	}
	return *v
}

// polymarketEpoch is a window start in epoch seconds. Zero means "three years
// back" to the venue, so the whole history is asked for as 1.
func polymarketEpoch(t time.Time) string {
	if t.IsZero() || t.Unix() < 1 {
		return "1"
	}
	return strconv.FormatInt(t.Unix(), 10)
}

func polymarketSymbol(slug, outcome, tokenID string) string {
	switch {
	case slug != "" && outcome != "":
		return slug + "/" + outcome
	case slug != "":
		return slug
	}
	return tokenID
}

// walkPolymarket follows a feed's cursor up to maxPages and reports whether it
// reached the end. The filters ride on every page: the cursor carries only its
// seek anchor.
func walkPolymarket[T any](ctx context.Context, p *Polymarket, path string, q url.Values, maxPages int, visit func(T) error) (bool, error) {
	q.Set("user", p.wallet)
	for page := 0; page < maxPages; page++ {
		var rows []T
		next, err := p.get(ctx, path, q, &rows)
		if err != nil {
			return false, err
		}
		for _, r := range rows {
			if err := visit(r); err != nil {
				return false, err
			}
		}
		if next == "" {
			return true, nil
		}
		q.Set("cursor", next)
	}
	return false, nil
}

// get decodes the data of one response into out and returns the next cursor.
// The address is the credential here, so errors never carry the request URL.
func (p *Polymarket) get(ctx context.Context, path string, q url.Values, out any) (string, error) {
	if !polymarketWallet.MatchString(p.wallet) {
		return "", errors.New("polymarket: credentials must be the account's 0x wallet address of 40 hex characters")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.baseURL+path+"?"+q.Encode(), nil)
	if err != nil {
		return "", fmt.Errorf("build polymarket request %s", path)
	}
	resp, err := p.client.Do(req)
	if err != nil {
		var ue *url.Error
		if errors.As(err, &ue) {
			err = ue.Err
		}
		return "", fmt.Errorf("%w: polymarket request %s: %w", ErrTransient, path, err)
	}
	data, err := ReadCappedBody(resp.Body, DefaultMaxResponseBytes)
	if err != nil {
		return "", fmt.Errorf("read polymarket response %s: %w", path, err)
	}
	if resp.StatusCode != http.StatusOK {
		detail := vendorErrorDetail(strings.ReplaceAll(string(data), p.wallet, walletPrefix(p.wallet)))
		if isRetryableStatus(resp.StatusCode) {
			return "", fmt.Errorf("%w: polymarket API error %d on %s: %s", ErrTransient, resp.StatusCode, path, detail)
		}
		return "", fmt.Errorf("polymarket API error %d on %s: %s", resp.StatusCode, path, detail)
	}
	var envelope struct {
		Data       json.RawMessage `json:"data"`
		Pagination *struct {
			NextCursor *string `json:"next_cursor"`
		} `json:"pagination"`
	}
	if err := json.Unmarshal(data, &envelope); err != nil {
		return "", fmt.Errorf("decode polymarket response %s: %w", path, err)
	}
	if len(envelope.Data) > 0 {
		if err := json.Unmarshal(envelope.Data, out); err != nil {
			return "", fmt.Errorf("decode polymarket %s data: %w", path, err)
		}
	}
	if envelope.Pagination != nil && envelope.Pagination.NextCursor != nil {
		return *envelope.Pagination.NextCursor, nil
	}
	return "", nil
}
