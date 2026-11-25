package connector

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	predictFunAPI = "https://api.predict.fun/v1"
	// A Predict account holds its cash as BEP-20 USDT, 18 decimals.
	predictFunRPC     = "https://bsc-dataseed.binance.org"
	predictFunUSDT    = "0x55d398326f99059fF775485246999027B3197955"
	predictFunPageMax = 100
	// A wallet that outgrows the cap fails the read instead of being valued on
	// a truncated book.
	predictFunMaxPositionPages = 20
	predictFunMaxMatchPages    = 100
)

// PredictFunAPIKeyEnv names the application key the venue requires on every
// request. It is AuditZK's, not the user's: the user only gives an address.
const PredictFunAPIKeyEnv = "PREDICTFUN_API_KEY"

// PredictFun is a read-only connector for a predict.fun account on BNB Chain,
// keyed by its public address (the Predict account, stored as the API key).
//
// The venue's API states positions and a cumulative PnL series but no cash
// and no deposits or withdrawals. Cash is read from the chain; the capital
// flows of a sync are what moved the equity without moving that PnL, which
// the sync derives through CumulativePnLProvider.
type PredictFun struct {
	wallet  string
	apiKey  string
	client  *http.Client
	baseURL string
	rpcURL  string
}

func NewPredictFun(creds *Credentials) *PredictFun {
	addr := creds.WalletAddress
	if addr == "" {
		addr = creds.APIKey
	}
	return &PredictFun{
		wallet:  strings.TrimSpace(addr),
		apiKey:  strings.TrimSpace(os.Getenv(PredictFunAPIKeyEnv)),
		client:  &http.Client{Timeout: 30 * time.Second},
		baseURL: predictFunAPI,
		rpcURL:  predictFunRPC,
	}
}

func (p *PredictFun) Exchange() string { return "predictfun" }

func (p *PredictFun) DetectIsPaper(_ context.Context) (bool, error) { return false, nil }

// TestConnection refuses an address that holds nothing and never traded on
// predict.fun. The venue answers any address, so the signer wallet pasted in
// place of the Predict account would otherwise connect and track a zero.
func (p *PredictFun) TestConnection(ctx context.Context) error {
	bal, err := p.GetBalance(ctx)
	if err != nil {
		return err
	}
	points, err := p.pnlSeries(ctx, "all")
	if err != nil {
		return err
	}
	if len(points) == 0 && bal.Equity <= 0 {
		return errors.New("predict.fun: invalid credentials, no Predict account activity at this address; use the Predict account address from the portfolio page")
	}
	return nil
}

func (p *PredictFun) GetBalance(ctx context.Context) (*Balance, error) {
	if err := p.ready(); err != nil {
		return nil, err
	}
	positions, err := p.fetchPositions(ctx)
	if err != nil {
		return nil, err
	}
	cash, err := p.fetchCash(ctx)
	if err != nil {
		return nil, err
	}
	var held, unrealized float64
	for _, r := range positions {
		held += float64(r.ValueUSD)
		unrealized += float64(r.PnLUSD)
	}
	return &Balance{
		Equity:        cash + held,
		Available:     cash,
		UnrealizedPnL: unrealized,
		Currency:      "USD",
	}, nil
}

func (p *PredictFun) GetPositions(ctx context.Context) ([]*Position, error) {
	if err := p.ready(); err != nil {
		return nil, err
	}
	rows, err := p.fetchPositions(ctx)
	if err != nil {
		return nil, err
	}
	positions := make([]*Position, 0, len(rows))
	for _, r := range rows {
		size := predictFunShares(r.Amount)
		if size <= 0 {
			continue
		}
		pos := &Position{
			Symbol:        predictFunSymbol(r.Market.CategorySlug, r.Outcome.Name),
			Side:          "long",
			Size:          size,
			EntryPrice:    float64(r.AverageBuyPriceUSD),
			UnrealizedPnL: float64(r.PnLUSD),
			MarketType:    MarketSpot,
		}
		if v := float64(r.ValueUSD); v > 0 {
			pos.MarkPrice = v / size
		}
		positions = append(positions, pos)
	}
	return positions, nil
}

// GetTrades returns the wallet's fills, as taker or maker. A window past the
// page cap comes back cut: trades feed activity counters, never the equity.
func (p *PredictFun) GetTrades(ctx context.Context, start, end time.Time) ([]*Trade, error) {
	if err := p.ready(); err != nil {
		return nil, err
	}
	q := url.Values{
		"signerAddress": {p.wallet},
		"first":         {"150"},
		"executedAfter": {strconv.FormatInt(start.UnixMilli()-1, 10)},
	}
	if !end.IsZero() {
		q.Set("executedBefore", strconv.FormatInt(end.UnixMilli()+1, 10))
	}
	var trades []*Trade
	for page := 0; page < predictFunMaxMatchPages; page++ {
		var rows []predictFunMatch
		next, err := p.get(ctx, "/orders/matches", q, &rows)
		if err != nil {
			return nil, err
		}
		for _, m := range rows {
			trades = append(trades, m.tradesOf(p.wallet)...)
		}
		if next == "" {
			break
		}
		q.Set("after", next)
	}
	return trades, nil
}

// CumulativePnL returns the venue's cumulative PnL in USD at a moment: the last
// point of its series at or before it, or the current value for a zero time.
// The series is hourly over the last day and daily before that, so an older
// moment is read at the midnight that opens its day.
func (p *PredictFun) CumulativePnL(ctx context.Context, at time.Time) (float64, error) {
	if err := p.ready(); err != nil {
		return 0, err
	}
	period := "all"
	if !at.IsZero() && time.Since(at) < 23*time.Hour {
		period = "1d"
	}
	pts, err := p.pnlSeries(ctx, period)
	if err != nil {
		return 0, err
	}
	if len(pts) == 0 {
		return 0, nil
	}
	if at.IsZero() {
		return pts[len(pts)-1].Y, nil
	}
	if at.Unix() < pts[0].X {
		if period == "all" {
			// Before the account's first point it had no PnL at all.
			return 0, nil
		}
		return 0, ErrPnLNotCovered
	}
	i := sort.Search(len(pts), func(i int) bool { return pts[i].X > at.Unix() })
	return pts[i-1].Y, nil
}

type predictFunPoint struct {
	X int64   `json:"x"`
	Y float64 `json:"y"`
}

// pnlSeries returns the venue's cumulative PnL points over a period, oldest
// first. An address without an account has none.
func (p *PredictFun) pnlSeries(ctx context.Context, period string) ([]predictFunPoint, error) {
	var series struct {
		Points []predictFunPoint `json:"points"`
	}
	if _, err := p.get(ctx, "/account/"+url.PathEscape(p.wallet)+"/pnl", url.Values{"period": {period}}, &series); err != nil {
		return nil, err
	}
	sort.Slice(series.Points, func(i, j int) bool { return series.Points[i].X < series.Points[j].X })
	return series.Points, nil
}

// ErrPnLNotCovered marks a moment the venue's PnL series does not reach.
var ErrPnLNotCovered = errors.New("pnl series does not cover that moment")

type predictFunAmount float64

func (a *predictFunAmount) UnmarshalJSON(b []byte) error {
	s := strings.Trim(strings.TrimSpace(string(b)), `"`)
	if s == "" || s == "null" {
		*a = 0
		return nil
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return fmt.Errorf("predict.fun amount %q is not a number", s)
	}
	*a = predictFunAmount(v)
	return nil
}

type predictFunPosition struct {
	Amount             string           `json:"amount"`
	AverageBuyPriceUSD predictFunAmount `json:"averageBuyPriceUsd"`
	PnLUSD             predictFunAmount `json:"pnlUsd"`
	ValueUSD           predictFunAmount `json:"valueUsd"`
	Market             struct {
		CategorySlug string `json:"categorySlug"`
	} `json:"market"`
	Outcome struct {
		Name string `json:"name"`
	} `json:"outcome"`
}

type predictFunFill struct {
	QuoteType string `json:"quoteType"`
	Amount    string `json:"amount"`
	Price     string `json:"price"`
	Signer    string `json:"signer"`
	Hash      string `json:"hash"`
	Outcome   struct {
		Name string `json:"name"`
	} `json:"outcome"`
	Fee struct {
		Amount string `json:"amount"`
		Type   string `json:"type"`
	} `json:"fee"`
}

type predictFunMatch struct {
	Market struct {
		Title string `json:"title"`
	} `json:"market"`
	Taker      predictFunFill   `json:"taker"`
	Makers     []predictFunFill `json:"makers"`
	ExecutedAt string           `json:"executedAt"`
}

// tradesOf returns the wallet's own sides of one match.
func (m *predictFunMatch) tradesOf(wallet string) []*Trade {
	at, err := time.Parse(time.RFC3339Nano, m.ExecutedAt)
	if err != nil {
		return nil
	}
	var out []*Trade
	for _, f := range append([]predictFunFill{m.Taker}, m.Makers...) {
		if !strings.EqualFold(f.Signer, wallet) {
			continue
		}
		qty := predictFunShares(f.Amount)
		price := predictFunShares(f.Price)
		if qty <= 0 {
			continue
		}
		side := "buy"
		if strings.EqualFold(f.QuoteType, "Ask") {
			side = "sell"
		}
		fee := predictFunShares(f.Fee.Amount)
		if strings.EqualFold(f.Fee.Type, "SHARES") {
			fee *= price
		}
		out = append(out, &Trade{
			ID:          f.Hash,
			Symbol:      predictFunSymbol(m.Market.Title, f.Outcome.Name),
			Side:        side,
			Price:       price,
			Quantity:    qty,
			USDNotional: price * qty,
			Fee:         fee,
			FeeCurrency: "USDT",
			Timestamp:   at.UTC(),
			MarketType:  MarketSpot,
		})
	}
	return out
}

func (p *PredictFun) fetchPositions(ctx context.Context) ([]predictFunPosition, error) {
	q := url.Values{"first": {strconv.Itoa(predictFunPageMax)}}
	var all []predictFunPosition
	for page := 0; page < predictFunMaxPositionPages; page++ {
		var rows []predictFunPosition
		next, err := p.get(ctx, "/positions/"+url.PathEscape(p.wallet), q, &rows)
		if err != nil {
			return nil, err
		}
		all = append(all, rows...)
		if next == "" {
			return all, nil
		}
		q.Set("after", next)
	}
	return nil, fmt.Errorf("predict.fun: more than %d pages of positions", predictFunMaxPositionPages)
}

// fetchCash reads the account's USDT from the chain.
func (p *PredictFun) fetchCash(ctx context.Context) (float64, error) {
	call := map[string]any{
		"jsonrpc": "2.0", "id": 1, "method": "eth_call",
		"params": []any{map[string]string{
			"to":   predictFunUSDT,
			"data": "0x70a08231" + strings.Repeat("0", 24) + strings.ToLower(p.wallet[2:]),
		}, "latest"},
	}
	body, _ := json.Marshal(call)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.rpcURL, bytes.NewReader(body))
	if err != nil {
		return 0, errors.New("predict.fun: build balance request")
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := p.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("%w: predict.fun balance read: %w", ErrTransient, unwrapURLError(err))
	}
	data, err := ReadCappedBody(resp.Body, DefaultMaxResponseBytes)
	if err != nil {
		return 0, fmt.Errorf("read predict.fun balance: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("%w: predict.fun balance read: chain node answered %d", ErrTransient, resp.StatusCode)
	}
	var out struct {
		Result string `json:"result"`
		Error  *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(data, &out); err != nil || out.Error != nil || !strings.HasPrefix(out.Result, "0x") {
		return 0, fmt.Errorf("%w: predict.fun balance read: unreadable chain answer", ErrTransient)
	}
	raw, ok := new(big.Int).SetString(strings.TrimPrefix(out.Result, "0x"), 16)
	if !ok {
		return 0, fmt.Errorf("%w: predict.fun balance read: unreadable chain answer", ErrTransient)
	}
	cash, _ := new(big.Float).Quo(new(big.Float).SetInt(raw), big.NewFloat(1e18)).Float64()
	return cash, nil
}

func (p *PredictFun) ready() error {
	if p.apiKey == "" {
		return errors.New("predict.fun is not configured on the enclave: " + strings.ToLower(PredictFunAPIKeyEnv) + " is missing")
	}
	if !polymarketWallet.MatchString(p.wallet) {
		return errors.New("predict.fun: invalid credentials, expected the Predict account's 0x address of 40 hex characters")
	}
	return nil
}

// get decodes data into out and returns the next cursor. The address is the
// credential, so errors never carry the request URL.
func (p *PredictFun) get(ctx context.Context, path string, q url.Values, out any) (string, error) {
	target := p.baseURL + path
	if len(q) > 0 {
		target += "?" + q.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return "", errors.New("predict.fun: build request")
	}
	req.Header.Set("x-api-key", p.apiKey)
	req.Header.Set("Accept", "application/json")
	resp, err := p.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("%w: predict.fun request: %w", ErrTransient, unwrapURLError(err))
	}
	data, err := ReadCappedBody(resp.Body, DefaultMaxResponseBytes)
	if err != nil {
		return "", fmt.Errorf("read predict.fun response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		var refusal struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal(data, &refusal)
		code := vendorErrorDetail(refusal.Error)
		switch {
		case resp.StatusCode == http.StatusTooManyRequests || isRetryableStatus(resp.StatusCode):
			return "", fmt.Errorf("%w: predict.fun API error %d (%s)", ErrTransient, resp.StatusCode, code)
		case resp.StatusCode == http.StatusUnauthorized:
			// The only credential the venue checks is the enclave's own key.
			return "", fmt.Errorf("predict.fun is not configured on the enclave: its API key was refused (%s)", code)
		case resp.StatusCode == http.StatusNotFound:
			return "", errors.New("predict.fun: invalid credentials, no Predict account at this address")
		default:
			return "", fmt.Errorf("predict.fun API error %d (%s)", resp.StatusCode, code)
		}
	}
	var envelope struct {
		Data   json.RawMessage `json:"data"`
		Cursor *string         `json:"cursor"`
	}
	if err := json.Unmarshal(data, &envelope); err != nil {
		return "", fmt.Errorf("decode predict.fun response: %w", err)
	}
	if len(envelope.Data) > 0 && string(envelope.Data) != "null" {
		if err := json.Unmarshal(envelope.Data, out); err != nil {
			return "", fmt.Errorf("decode predict.fun data: %w", err)
		}
	}
	if envelope.Cursor != nil {
		return *envelope.Cursor, nil
	}
	return "", nil
}

func unwrapURLError(err error) error {
	var ue *url.Error
	if errors.As(err, &ue) {
		return ue.Err
	}
	return err
}

// predictFunShares reads a wei string as a number of shares or dollars.
func predictFunShares(wei string) float64 {
	v, ok := new(big.Float).SetString(strings.TrimSpace(wei))
	if !ok {
		return 0
	}
	f, _ := new(big.Float).Quo(v, big.NewFloat(1e18)).Float64()
	if math.IsInf(f, 0) || math.IsNaN(f) {
		return 0
	}
	return f
}

func predictFunSymbol(market, outcome string) string {
	switch {
	case market != "" && outcome != "":
		return market + "/" + outcome
	case market != "":
		return market
	}
	return outcome
}
