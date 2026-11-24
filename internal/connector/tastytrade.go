package connector

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/trackrecord/enclave/internal/errsanitize"
)

const (
	tastytradeAPI = "https://api.tastyworks.com"
	// The venue answers 401 with an HTML page to a request without one.
	tastytradeUserAgent = "auditzk-enclave/1.0"
	// An access token lasts fifteen minutes; it is renewed after ten.
	tastytradeTokenLife = 10 * time.Minute
	tastytradePageSize  = 250
	tastytradeMaxPages  = 200

	tastytradeRefusalLimit  = 10
	tastytradeRefusalWindow = 10 * time.Minute
	tastytradeRefusalPause  = 15 * time.Minute
)

// TastytradePositionTransferWarning marks an account that received or delivered
// positions in kind: the transfer moved its value without a cash flow to book.
const TastytradePositionTransferWarning = "tastytrade_position_transfer_not_counted"

// signInGuard holds back token requests after a run of refusals. The venue
// blocks an address for about eight hours on repeated failed logins, and the
// enclave signs every connection in from the same one.
type signInGuard struct {
	mu       sync.Mutex
	refusals []time.Time
}

var tastytradeSignIn = &signInGuard{}

func (g *signInGuard) paused(now time.Time) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.refusals) < tastytradeRefusalLimit {
		return false
	}
	first, last := g.refusals[0], g.refusals[len(g.refusals)-1]
	return last.Sub(first) <= tastytradeRefusalWindow && now.Before(last.Add(tastytradeRefusalPause))
}

func (g *signInGuard) refused(now time.Time) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.refusals = append(g.refusals, now)
	if over := len(g.refusals) - tastytradeRefusalLimit; over > 0 {
		g.refusals = g.refusals[over:]
	}
}

// Tastytrade is a read-only connector for one tastytrade account.
//
// Credentials are those of the holder's own OAuth application: the grant's
// refresh token as the API key, the client secret as the API secret, and the
// account number as the passphrase when the login holds several accounts. The
// refresh token does not expire and is never rotated, so nothing is written
// back. Every access token is asked for with the read scope alone, whatever the
// grant allows.
type Tastytrade struct {
	refreshToken string
	clientSecret string
	wantAccount  string
	client       *http.Client
	baseURL      string
	guard        *signInGuard

	mu          sync.Mutex
	accessToken string
	tokenAt     time.Time
	account     string
	warnings    []string
}

func NewTastytrade(creds *Credentials) *Tastytrade {
	return &Tastytrade{
		refreshToken: strings.TrimSpace(creds.APIKey),
		clientSecret: strings.TrimSpace(creds.APISecret),
		wantAccount:  strings.ToUpper(strings.TrimSpace(creds.Passphrase)),
		client:       &http.Client{Timeout: 30 * time.Second},
		baseURL:      tastytradeAPI,
		guard:        tastytradeSignIn,
	}
}

func (t *Tastytrade) Exchange() string { return "tastytrade" }

// DetectIsPaper is false by construction: the sandbox is another host with its
// own credentials, and this connector only talks to production.
func (t *Tastytrade) DetectIsPaper(_ context.Context) (bool, error) { return false, nil }

func (t *Tastytrade) TestConnection(ctx context.Context) error {
	_, err := t.GetBalance(ctx)
	return err
}

func (t *Tastytrade) GetBalance(ctx context.Context) (*Balance, error) {
	account, err := t.accountNumber(ctx)
	if err != nil {
		return nil, err
	}
	var bal struct {
		Currency              string            `json:"currency"`
		NetLiquidatingValue   *tastytradeAmount `json:"net-liquidating-value"`
		DerivativeBuyingPower tastytradeAmount  `json:"derivative-buying-power"`
	}
	if _, err := t.get(ctx, "/accounts/"+url.PathEscape(account)+"/balances", nil, &bal); err != nil {
		return nil, err
	}
	if bal.NetLiquidatingValue == nil {
		return nil, errors.New("tastytrade: balance carries no net liquidating value")
	}
	positions, err := t.fetchPositions(ctx, account)
	if err != nil {
		return nil, err
	}
	var unrealized float64
	for _, p := range positions {
		unrealized += p.UnrealizedPnL
	}
	equity := float64(*bal.NetLiquidatingValue)
	currency := strings.ToUpper(strings.TrimSpace(bal.Currency))
	if currency == "" {
		currency = "USD"
	}
	return &Balance{
		Equity:        equity,
		Available:     math.Min(math.Max(float64(bal.DerivativeBuyingPower), 0), math.Max(equity, 0)),
		UnrealizedPnL: unrealized,
		Currency:      currency,
	}, nil
}

func (t *Tastytrade) GetPositions(ctx context.Context) ([]*Position, error) {
	account, err := t.accountNumber(ctx)
	if err != nil {
		return nil, err
	}
	return t.fetchPositions(ctx, account)
}

type tastytradePosition struct {
	Symbol           string            `json:"symbol"`
	InstrumentType   string            `json:"instrument-type"`
	Quantity         tastytradeAmount  `json:"quantity"`
	Direction        string            `json:"quantity-direction"`
	AverageOpenPrice tastytradeAmount  `json:"average-open-price"`
	Mark             *tastytradeAmount `json:"mark"`
	MarkPrice        *tastytradeAmount `json:"mark-price"`
	Multiplier       tastytradeAmount  `json:"multiplier"`
}

// fetchPositions returns the open positions with the venue's own marks. A
// position the venue did not mark carries no unrealized PnL rather than one
// measured against a stale close.
func (t *Tastytrade) fetchPositions(ctx context.Context, account string) ([]*Position, error) {
	var rows []tastytradePosition
	q := url.Values{"include-marks": {"true"}}
	if _, err := t.get(ctx, "/accounts/"+url.PathEscape(account)+"/positions", q, &rows); err != nil {
		return nil, err
	}
	positions := make([]*Position, 0, len(rows))
	for _, r := range rows {
		size := math.Abs(float64(r.Quantity))
		side := "long"
		switch {
		case size == 0 || strings.EqualFold(r.Direction, "Zero"):
			continue
		case strings.EqualFold(r.Direction, "Short"), r.Direction == "" && r.Quantity < 0:
			side = "short"
		}
		mark := r.Mark
		if mark == nil {
			mark = r.MarkPrice
		}
		p := &Position{
			Symbol:     r.Symbol,
			Side:       side,
			Size:       size,
			EntryPrice: float64(r.AverageOpenPrice),
			MarketType: tastytradeMarket(r.InstrumentType),
		}
		if mark != nil && *mark > 0 {
			multiplier := float64(r.Multiplier)
			if multiplier <= 0 {
				multiplier = 1
			}
			p.MarkPrice = float64(*mark)
			p.UnrealizedPnL = (p.MarkPrice - p.EntryPrice) * size * multiplier
			if side == "short" {
				p.UnrealizedPnL = -p.UnrealizedPnL
			}
		}
		positions = append(positions, p)
	}
	return positions, nil
}

type tastytradeTransaction struct {
	ID             int64            `json:"id"`
	Type           string           `json:"transaction-type"`
	SubType        string           `json:"transaction-sub-type"`
	Action         string           `json:"action"`
	Symbol         string           `json:"symbol"`
	InstrumentType string           `json:"instrument-type"`
	Quantity       tastytradeAmount `json:"quantity"`
	Price          tastytradeAmount `json:"price"`
	Value          tastytradeAmount `json:"value"`
	NetValue       tastytradeAmount `json:"net-value"`
	NetValueEffect string           `json:"net-value-effect"`
	ExecutedAt     string           `json:"executed-at"`
	Currency       string           `json:"currency"`

	Commission         tastytradeAmount `json:"commission"`
	CommissionEffect   string           `json:"commission-effect"`
	ClearingFees       tastytradeAmount `json:"clearing-fees"`
	ClearingFeesEffect string           `json:"clearing-fees-effect"`
	RegulatoryFees     tastytradeAmount `json:"regulatory-fees"`
	RegulatoryEffect   string           `json:"regulatory-fees-effect"`
	IndexOptionFees    tastytradeAmount `json:"proprietary-index-option-fees"`
	IndexOptionEffect  string           `json:"proprietary-index-option-fees-effect"`
	ConversionFees     tastytradeAmount `json:"currency-conversion-fees"`
	ConversionEffect   string           `json:"currency-conversion-fees-effect"`
	OtherCharge        tastytradeAmount `json:"other-charge"`
	OtherChargeEffect  string           `json:"other-charge-effect"`
}

// fees is what the transaction cost, positive for a charge.
func (x *tastytradeTransaction) fees() float64 {
	return -(tastytradeSigned(x.Commission, x.CommissionEffect) +
		tastytradeSigned(x.ClearingFees, x.ClearingFeesEffect) +
		tastytradeSigned(x.RegulatoryFees, x.RegulatoryEffect) +
		tastytradeSigned(x.IndexOptionFees, x.IndexOptionEffect) +
		tastytradeSigned(x.ConversionFees, x.ConversionEffect) +
		tastytradeSigned(x.OtherCharge, x.OtherChargeEffect))
}

func (t *Tastytrade) GetTrades(ctx context.Context, start, end time.Time) ([]*Trade, error) {
	rows, err := t.fetchTransactions(ctx, start, end, "Trade")
	if err != nil {
		return nil, err
	}
	trades := make([]*Trade, 0, len(rows))
	for _, r := range rows {
		var side string
		switch action := strings.ToLower(r.Action); {
		case strings.HasPrefix(action, "buy"):
			side = "buy"
		case strings.HasPrefix(action, "sell"):
			side = "sell"
		default:
			continue
		}
		at, err := tastytradeTime(r.ExecutedAt)
		if err != nil {
			continue
		}
		trades = append(trades, &Trade{
			ID:          strconv.FormatInt(r.ID, 10),
			Symbol:      r.Symbol,
			Side:        side,
			Price:       math.Abs(float64(r.Price)),
			Quantity:    math.Abs(float64(r.Quantity)),
			USDNotional: math.Abs(float64(r.Value)),
			Fee:         r.fees(),
			FeeCurrency: "USD",
			Timestamp:   at,
			MarketType:  tastytradeMarket(r.InstrumentType),
		})
	}
	return trades, nil
}

// GetCashflows returns the cash that entered or left the account: deposits,
// withdrawals, and transfers of cash between accounts. Dividends, interest,
// fees and the daily futures settlement are money movements too, and are
// performance.
//
// Positions moved in kind are not valued here; they raise a warning instead of
// passing for a gain or a loss nobody can see the cause of.
func (t *Tastytrade) GetCashflows(ctx context.Context, since time.Time) ([]*Cashflow, error) {
	rows, err := t.fetchTransactions(ctx, since, time.Time{}, "Money Movement", "Administrative Transfer", "Receive Deliver")
	if err != nil {
		return nil, err
	}
	var flows []*Cashflow
	inKind := false
	for _, r := range rows {
		if !r.movesCapital() {
			continue
		}
		if r.Symbol != "" && r.Quantity != 0 {
			inKind = true
			continue
		}
		at, err := tastytradeTime(r.ExecutedAt)
		if err != nil {
			return nil, errors.New("tastytrade: a capital movement carries no readable date")
		}
		if amount := tastytradeSigned(r.NetValue, r.NetValueEffect); amount != 0 {
			flows = append(flows, &Cashflow{Amount: amount, Currency: "USD", Timestamp: at})
		}
	}
	t.mu.Lock()
	t.warnings = nil
	if inKind {
		t.warnings = []string{TastytradePositionTransferWarning}
	}
	t.mu.Unlock()
	return flows, nil
}

// movesCapital reports a transaction that brings capital in or takes it out,
// in cash or in kind, as opposed to what the account earned or paid.
func (x *tastytradeTransaction) movesCapital() bool {
	sub := strings.ToLower(strings.TrimSpace(x.SubType))
	transfer := strings.Contains(sub, "transfer") || strings.Contains(sub, "acat")
	switch strings.ToLower(strings.TrimSpace(x.Type)) {
	case "money movement":
		return sub == "deposit" || sub == "withdrawal" || transfer
	case "administrative transfer":
		return true
	case "receive deliver":
		return transfer
	}
	return false
}

// CapabilityWarnings reports what the last cashflow read could not book.
func (t *Tastytrade) CapabilityWarnings() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]string(nil), t.warnings...)
}

// fetchTransactions walks the account's transactions of the given types between
// two instants, oldest first. A zero end means now.
func (t *Tastytrade) fetchTransactions(ctx context.Context, start, end time.Time, types ...string) ([]tastytradeTransaction, error) {
	account, err := t.accountNumber(ctx)
	if err != nil {
		return nil, err
	}
	q := url.Values{
		"per-page": {strconv.Itoa(tastytradePageSize)},
		"sort":     {"Asc"},
		"types[]":  types,
	}
	if !start.IsZero() {
		q.Set("start-at", start.UTC().Format(time.RFC3339))
	}
	if !end.IsZero() {
		q.Set("end-at", end.UTC().Format(time.RFC3339))
	}
	var all []tastytradeTransaction
	for page := 0; page < tastytradeMaxPages; page++ {
		q.Set("page-offset", strconv.Itoa(page))
		var rows []tastytradeTransaction
		pagination, err := t.get(ctx, "/accounts/"+url.PathEscape(account)+"/transactions", q, &rows)
		if err != nil {
			return nil, err
		}
		all = append(all, rows...)
		if pagination != nil && pagination.TotalPages > 0 {
			if page+1 >= pagination.TotalPages {
				return all, nil
			}
			continue
		}
		if len(rows) < tastytradePageSize {
			return all, nil
		}
	}
	return nil, fmt.Errorf("tastytrade: more than %d pages of transactions", tastytradeMaxPages)
}

// accountNumber resolves the one account this connection measures: the number
// it was given, or the login's only open account.
func (t *Tastytrade) accountNumber(ctx context.Context) (string, error) {
	t.mu.Lock()
	known := t.account
	t.mu.Unlock()
	if known != "" {
		return known, nil
	}
	var rows []struct {
		Account struct {
			Number   string `json:"account-number"`
			IsClosed bool   `json:"is-closed"`
		} `json:"account"`
	}
	if _, err := t.get(ctx, "/customers/me/accounts", nil, &rows); err != nil {
		return "", err
	}
	var open []string
	for _, r := range rows {
		number := strings.ToUpper(strings.TrimSpace(r.Account.Number))
		if number == "" || r.Account.IsClosed {
			continue
		}
		if number == t.wantAccount {
			open = []string{number}
			break
		}
		open = append(open, number)
	}
	switch {
	case t.wantAccount != "" && (len(open) != 1 || open[0] != t.wantAccount):
		return "", errors.New("tastytrade: that account number is not among this login's open accounts")
	case len(open) == 0:
		return "", errors.New("tastytrade: this login holds no trading account")
	case len(open) > 1:
		return "", errors.New("tastytrade: this login holds several accounts, give the number of the one to track")
	}
	t.mu.Lock()
	t.account = open[0]
	t.mu.Unlock()
	return open[0], nil
}

// token returns a valid access token, signing in again when the one held is
// old or, with renew, when the venue just refused it.
func (t *Tastytrade) token(ctx context.Context, renew bool) (string, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !renew && t.accessToken != "" && time.Since(t.tokenAt) < tastytradeTokenLife {
		return t.accessToken, nil
	}
	// The refresh token is a signed JWT: anything else never reaches the venue,
	// where it would count as a failed login.
	if strings.Count(t.refreshToken, ".") != 2 || t.clientSecret == "" {
		return "", errors.New("tastytrade: invalid credentials, expected the grant's refresh token and the application's client secret")
	}
	if t.guard.paused(time.Now()) {
		return "", fmt.Errorf("%w: tastytrade sign-in paused after repeated refusals: %s", ErrTransient, errsanitize.MsgRateLimited)
	}
	body, err := json.Marshal(map[string]string{
		"grant_type":    "refresh_token",
		"refresh_token": t.refreshToken,
		"client_secret": t.clientSecret,
		"scope":         "read",
	})
	if err != nil {
		return "", errors.New("tastytrade: build sign-in request")
	}
	status, data, err := t.send(ctx, http.MethodPost, "/oauth/token", nil, "", body)
	if err != nil {
		return "", err
	}
	if status != http.StatusOK {
		if isRetryableStatus(status) {
			return "", fmt.Errorf("%w: tastytrade sign-in error %d", ErrTransient, status)
		}
		t.guard.refused(time.Now())
		var refusal struct {
			Code string `json:"error_code"`
		}
		_ = json.Unmarshal(data, &refusal)
		return "", fmt.Errorf("tastytrade: sign-in refused, invalid credentials (%d %s)", status, vendorErrorDetail(refusal.Code))
	}
	var granted struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.Unmarshal(data, &granted); err != nil || granted.AccessToken == "" {
		return "", errors.New("tastytrade: sign-in answered without an access token")
	}
	t.accessToken, t.tokenAt = granted.AccessToken, time.Now()
	return t.accessToken, nil
}

type tastytradePagination struct {
	TotalPages int `json:"total-pages"`
}

// get reads one resource into out, which receives data.items when the venue
// answers with a list and data otherwise. A 401 is answered once by a fresh
// token.
func (t *Tastytrade) get(ctx context.Context, path string, q url.Values, out any) (*tastytradePagination, error) {
	var status int
	var data []byte
	for attempt := 0; attempt < 2; attempt++ {
		token, err := t.token(ctx, attempt > 0)
		if err != nil {
			return nil, err
		}
		status, data, err = t.send(ctx, http.MethodGet, path, q, token, nil)
		if err != nil {
			return nil, err
		}
		if status != http.StatusUnauthorized {
			break
		}
	}
	if status != http.StatusOK {
		var refusal struct {
			Error struct {
				Code string `json:"code"`
			} `json:"error"`
		}
		_ = json.Unmarshal(data, &refusal)
		code := vendorErrorDetail(refusal.Error.Code)
		switch {
		case status == http.StatusTooManyRequests:
			return nil, fmt.Errorf("%w: tastytrade: %s", ErrTransient, errsanitize.MsgRateLimited)
		case isRetryableStatus(status):
			return nil, fmt.Errorf("%w: tastytrade API error %d", ErrTransient, status)
		case status == http.StatusUnauthorized:
			return nil, fmt.Errorf("tastytrade: unauthorized (%s)", code)
		case status == http.StatusForbidden:
			return nil, fmt.Errorf("tastytrade: permission denied, the grant needs the read scope on this account (%s)", code)
		default:
			return nil, fmt.Errorf("tastytrade API error %d (%s)", status, code)
		}
	}
	var envelope struct {
		Data       json.RawMessage       `json:"data"`
		Pagination *tastytradePagination `json:"pagination"`
	}
	if err := json.Unmarshal(data, &envelope); err != nil {
		return nil, fmt.Errorf("decode tastytrade response: %w", err)
	}
	var list struct {
		Items json.RawMessage `json:"items"`
	}
	payload := envelope.Data
	if json.Unmarshal(envelope.Data, &list) == nil && len(list.Items) > 0 {
		payload = list.Items
	}
	if len(payload) > 0 {
		if err := json.Unmarshal(payload, out); err != nil {
			return nil, fmt.Errorf("decode tastytrade data: %w", err)
		}
	}
	return envelope.Pagination, nil
}

// send issues one request. Errors carry neither the URL, which names the
// account, nor anything the request was signed with.
func (t *Tastytrade) send(ctx context.Context, method, path string, q url.Values, token string, body []byte) (int, []byte, error) {
	target := t.baseURL + path
	if len(q) > 0 {
		target += "?" + q.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, method, target, bytes.NewReader(body))
	if err != nil {
		return 0, nil, errors.New("tastytrade: build request")
	}
	req.Header.Set("User-Agent", tastytradeUserAgent)
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := t.client.Do(req)
	if err != nil {
		var ue *url.Error
		if errors.As(err, &ue) {
			err = ue.Err
		}
		return 0, nil, fmt.Errorf("%w: tastytrade request: %w", ErrTransient, err)
	}
	data, err := ReadCappedBody(resp.Body, DefaultMaxResponseBytes)
	if err != nil {
		return 0, nil, fmt.Errorf("read tastytrade response: %w", err)
	}
	return resp.StatusCode, data, nil
}

// tastytradeAmount is a decimal the venue serializes as a string, and now and
// then as a number.
type tastytradeAmount float64

func (a *tastytradeAmount) UnmarshalJSON(b []byte) error {
	s := strings.Trim(strings.TrimSpace(string(b)), `"`)
	if s == "" || s == "null" {
		*a = 0
		return nil
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return fmt.Errorf("tastytrade amount %q is not a number", s)
	}
	*a = tastytradeAmount(v)
	return nil
}

// tastytradeSigned applies the effect the venue states next to an amount:
// a credit adds to the account, a debit takes from it.
func tastytradeSigned(amount tastytradeAmount, effect string) float64 {
	v := float64(amount)
	switch strings.ToLower(strings.TrimSpace(effect)) {
	case "credit":
		return math.Abs(v)
	case "debit":
		return -math.Abs(v)
	}
	return v
}

func tastytradeTime(s string) (time.Time, error) {
	at, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(s))
	if err != nil {
		return time.Time{}, err
	}
	return at.UTC(), nil
}

func tastytradeMarket(instrumentType string) string {
	switch strings.ToLower(strings.TrimSpace(instrumentType)) {
	case "equity option", "future option":
		return MarketOptions
	case "future":
		return MarketFutures
	case "cryptocurrency":
		return MarketSpot
	case "currency pair":
		return MarketForex
	}
	return MarketStocks
}
