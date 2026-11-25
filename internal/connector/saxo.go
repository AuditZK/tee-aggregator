package connector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	saxoLiveAPI  = "https://gateway.saxobank.com/openapi"
	saxoSimAPI   = "https://gateway.saxobank.com/sim/openapi"
	saxoLiveAuth = "https://live.logonvalidation.net"
	saxoSimAuth  = "https://sim.logonvalidation.net"

	saxoPageSize      = 500
	saxoMaxPages      = 50
	saxoTokenTimeout  = 30 * time.Second
	saxoTokenMaxBytes = 64 << 10
	saxoTxMemoTTL     = 2 * time.Minute

	saxoIntraClientTransfer = "IntraClientTransfer"
)

// ErrSaxoNotConfigured is the enclave missing, or Saxo refusing, the app
// credentials every refresh signs with. An operator problem, never the user's.
var ErrSaxoNotConfigured = errors.New("saxo is not configured on the enclave (set SAXO_APP_KEY and SAXO_APP_SECRET)")

// Saxo implements Connector for Saxo Bank's OpenAPI. The connection holds an
// OAuth pair: APIKey is the access token, APISecret the refresh token, which
// Saxo rotates on every refresh.
type Saxo struct {
	baseURL      string
	authURL      string
	isSim        bool
	client       *http.Client
	retryBackoff time.Duration

	appKey      string
	appSecret   string
	redirectURI string

	tokenMu      sync.RWMutex
	accessToken  string
	refreshToken string
	guard        TokenGuard
	accessTTL    time.Duration
	refreshTTL   time.Duration
	local        saxoLocalGuard

	clientMu     sync.Mutex
	clientKey    string
	baseCurrency string

	txMu   sync.Mutex
	txMemo *saxoTxMemo

	warnMu   sync.Mutex
	warnings []string
}

// NewSaxo creates a Saxo connector against the SIM gateway when sim is set.
func NewSaxo(creds *Credentials, sim bool) *Saxo {
	s := &Saxo{
		baseURL:      saxoLiveAPI,
		authURL:      saxoLiveAuth,
		isSim:        sim,
		client:       &http.Client{Timeout: 30 * time.Second},
		retryBackoff: baseBackoff,
		appKey:       firstNonEmpty(creds.ClientID, os.Getenv("SAXO_APP_KEY")),
		appSecret:    firstNonEmpty(creds.ClientSecret, os.Getenv("SAXO_APP_SECRET")),
		redirectURI:  strings.TrimSpace(os.Getenv("SAXO_REDIRECT_URI")),
		accessToken:  firstNonEmpty(creds.AccessToken, creds.APIKey),
		refreshToken: strings.TrimSpace(creds.APISecret),
	}
	if sim {
		s.baseURL, s.authURL = saxoSimAPI, saxoSimAuth
	}
	s.local.owner = s
	return s
}

func (s *Saxo) Exchange() string {
	if s.isSim {
		return "saxo_sim"
	}
	return "saxo"
}

// DetectIsPaper reports the SIM gateway: SIM and live are separate hosts with
// separate logins, so the gateway the connection was made for decides.
func (s *Saxo) DetectIsPaper(_ context.Context) (bool, error) {
	return s.isSim, nil
}

func (s *Saxo) configured() bool {
	return s.appKey != "" && s.appSecret != ""
}

// --- tokens -----------------------------------------------------------------

func (s *Saxo) SetRefreshGuard(guard TokenGuard) {
	s.tokenMu.Lock()
	s.guard = guard
	s.tokenMu.Unlock()
}

// RefreshGuard returns the guard a service wired in, nil when the connector
// refreshes on its own.
func (s *Saxo) RefreshGuard() TokenGuard {
	s.tokenMu.RLock()
	defer s.tokenMu.RUnlock()
	return s.guard
}

func (s *Saxo) refreshGuard() TokenGuard {
	if g := s.RefreshGuard(); g != nil {
		return g
	}
	return &s.local
}

func (s *Saxo) CurrentCredentials() (string, string) {
	t := s.tokens()
	return t.AccessToken, t.RefreshToken
}

// TokenLifetimes reports the lifetimes Saxo stated on the last refresh, zero
// before one.
func (s *Saxo) TokenLifetimes() (access, refresh time.Duration) {
	s.tokenMu.RLock()
	defer s.tokenMu.RUnlock()
	return s.accessTTL, s.refreshTTL
}

func (s *Saxo) tokens() OAuthTokens {
	s.tokenMu.RLock()
	defer s.tokenMu.RUnlock()
	return OAuthTokens{AccessToken: s.accessToken, RefreshToken: s.refreshToken}
}

func (s *Saxo) setTokens(t OAuthTokens) {
	s.tokenMu.Lock()
	defer s.tokenMu.Unlock()
	if t.AccessToken != "" {
		s.accessToken = t.AccessToken
	}
	if t.RefreshToken != "" {
		s.refreshToken = t.RefreshToken
	}
}

// saxoLocalGuard is the refresh slot of a connector no service shares: the pair
// it holds is the only copy, so a rotation is stored by keeping it.
type saxoLocalGuard struct {
	owner     *Saxo
	mu        sync.Mutex
	rotatedAt time.Time
}

func (g *saxoLocalGuard) Rotate(_ context.Context, fn func(OAuthTokens, time.Time) (*OAuthTokens, error)) (OAuthTokens, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	current := g.owner.tokens()
	next, err := fn(current, g.rotatedAt)
	if err != nil || next == nil {
		return current, err
	}
	g.rotatedAt = time.Now()
	g.owner.setTokens(*next)
	return *next, nil
}

// refreshRejected rotates the pair after the gateway refused access, unless
// another holder already replaced the refused access token.
func (s *Saxo) refreshRejected(ctx context.Context, rejected string) error {
	pair, err := s.refreshGuard().Rotate(ctx, func(current OAuthTokens, _ time.Time) (*OAuthTokens, error) {
		if current.AccessToken != "" && current.AccessToken != rejected {
			return nil, nil
		}
		return s.exchangeRefreshToken(ctx, current.RefreshToken)
	})
	if err != nil {
		return err
	}
	s.setTokens(pair)
	return nil
}

// KeepAlive rotates the pair unless it was rotated less than minAge ago.
func (s *Saxo) KeepAlive(ctx context.Context, minAge time.Duration) error {
	pair, err := s.refreshGuard().Rotate(ctx, func(current OAuthTokens, rotatedAt time.Time) (*OAuthTokens, error) {
		if !rotatedAt.IsZero() && time.Since(rotatedAt) < minAge {
			return nil, nil
		}
		return s.exchangeRefreshToken(ctx, current.RefreshToken)
	})
	if err != nil {
		return err
	}
	s.setTokens(pair)
	return nil
}

func (s *Saxo) exchangeRefreshToken(ctx context.Context, refreshToken string) (*OAuthTokens, error) {
	if !s.configured() {
		return nil, ErrSaxoNotConfigured
	}
	refreshToken = strings.TrimSpace(refreshToken)
	if refreshToken == "" {
		return nil, fmt.Errorf("saxo %w: missing refresh token", ErrRefreshRefused)
	}

	form := url.Values{}
	form.Set("grant_type", "refresh_token")
	form.Set("refresh_token", refreshToken)
	if s.redirectURI != "" {
		form.Set("redirect_uri", s.redirectURI)
	}

	// Detached: once the request is out Saxo may have rotated already, and a
	// caller giving up must not throw the only copy of the new pair away.
	reqCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), saxoTokenTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, s.authURL+"/token", strings.NewReader(form.Encode()))
	if err != nil {
		return nil, fmt.Errorf("build saxo token request: %w", err)
	}
	req.SetBasicAuth(s.appKey, s.appSecret)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w: reach saxo token endpoint: %w", ErrTransient, withoutURL(err))
	}
	body, err := ReadCappedBody(resp.Body, saxoTokenMaxBytes)
	if err != nil {
		return nil, fmt.Errorf("%w: read saxo token response: %w", ErrTransient, err)
	}
	return s.parseTokenResponse(resp.StatusCode, body, refreshToken)
}

func (s *Saxo) parseTokenResponse(status int, body []byte, previousRefresh string) (*OAuthTokens, error) {
	var tr struct {
		AccessToken           string `json:"access_token"`
		RefreshToken          string `json:"refresh_token"`
		ExpiresIn             int    `json:"expires_in"`
		RefreshTokenExpiresIn int    `json:"refresh_token_expires_in"`
		Error                 string `json:"error"`
	}
	decodeErr := json.Unmarshal(body, &tr)
	code := strings.ToLower(strings.TrimSpace(tr.Error))

	switch {
	case status == http.StatusOK:
		if decodeErr != nil || strings.TrimSpace(tr.AccessToken) == "" {
			return nil, fmt.Errorf("%w: saxo token endpoint answered without an access token", ErrTransient)
		}
		next := &OAuthTokens{
			AccessToken:  strings.TrimSpace(tr.AccessToken),
			RefreshToken: firstNonEmpty(tr.RefreshToken, previousRefresh),
		}
		s.tokenMu.Lock()
		s.accessTTL = time.Duration(tr.ExpiresIn) * time.Second
		s.refreshTTL = time.Duration(tr.RefreshTokenExpiresIn) * time.Second
		s.tokenMu.Unlock()
		return next, nil
	case status == http.StatusTooManyRequests:
		return nil, fmt.Errorf("%w: saxo token refresh rate-limited (429)", ErrTransient)
	case status >= http.StatusInternalServerError:
		return nil, fmt.Errorf("%w: saxo token endpoint unavailable (HTTP %d)", ErrTransient, status)
	case code == "invalid_client" || code == "unauthorized_client":
		return nil, fmt.Errorf("%w: the token endpoint refused the app credentials (%s)", ErrSaxoNotConfigured, code)
	case code == "invalid_request" || code == "unsupported_grant_type":
		return nil, fmt.Errorf("%w: the token endpoint refused the request (%s), check SAXO_REDIRECT_URI", ErrSaxoNotConfigured, code)
	case status == http.StatusBadRequest || status == http.StatusUnauthorized:
		return nil, fmt.Errorf("saxo %w (HTTP %d): %s", ErrRefreshRefused, status, vendorErrorDetail(firstNonEmpty(code, "no error code")))
	default:
		return nil, fmt.Errorf("%w: saxo token endpoint answered HTTP %d", ErrTransient, status)
	}
}

// withoutURL drops the request URL from a transport error: it carries the
// ClientKey in its query string.
func withoutURL(err error) error {
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		return urlErr.Err
	}
	return err
}

// --- transport --------------------------------------------------------------

// get sends an authorised GET, rotating the pair once when the gateway refuses
// the access token.
func (s *Saxo) get(ctx context.Context, op, path string, query url.Values) ([]byte, error) {
	access := s.tokens().AccessToken
	body, status, err := s.send(ctx, path, query, access)
	if status == http.StatusUnauthorized {
		if rerr := s.refreshRejected(ctx, access); rerr != nil {
			return nil, fmt.Errorf("%s: %w", op, rerr)
		}
		body, _, err = s.send(ctx, path, query, s.tokens().AccessToken)
	}
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	return body, nil
}

func (s *Saxo) send(ctx context.Context, path string, query url.Values, access string) ([]byte, int, error) {
	reqURL := s.baseURL + path
	if len(query) > 0 {
		reqURL += "?" + query.Encode()
	}

	backoff := s.retryBackoff
	var lastErr error
	for attempt := 1; attempt <= maxRetryAttempts; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
		if err != nil {
			return nil, 0, fmt.Errorf("build saxo request: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+access)
		req.Header.Set("Accept", "application/json")

		resp, err := s.client.Do(req)
		if err != nil {
			if ctx.Err() != nil {
				return nil, 0, ctx.Err()
			}
			lastErr = withoutURL(err)
		} else {
			body, rerr := ReadCappedBody(resp.Body, DefaultMaxResponseBytes)
			switch {
			case rerr != nil:
				return nil, resp.StatusCode, rerr
			case resp.StatusCode >= 200 && resp.StatusCode < 300:
				return body, resp.StatusCode, nil
			case !isRetryableStatus(resp.StatusCode):
				return nil, resp.StatusCode, saxoStatusError(resp.StatusCode, body)
			default:
				lastErr = saxoStatusError(resp.StatusCode, body)
				backoff = parseRetryAfter(resp.Header, backoff)
			}
		}

		if attempt == maxRetryAttempts {
			break
		}
		select {
		case <-ctx.Done():
			return nil, 0, ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, maxBackoff)
	}
	return nil, 0, fmt.Errorf("%w: %w", ErrTransient, lastErr)
}

func saxoStatusError(status int, body []byte) error {
	detail := vendorErrorDetail(string(body))
	switch status {
	case http.StatusUnauthorized:
		return fmt.Errorf("HTTP 401 (unauthorized): %s", detail)
	case http.StatusForbidden:
		return fmt.Errorf("HTTP 403 (permission denied): %s", detail)
	case http.StatusTooManyRequests:
		return fmt.Errorf("HTTP 429 (rate limited): %s", detail)
	default:
		return fmt.Errorf("HTTP %d: %s", status, detail)
	}
}

type saxoPage[T any] struct {
	Data []T    `json:"Data"`
	Next string `json:"__next"`
}

// saxoReadAll walks a paged feed. Running out of pages is refused rather than
// returned short: a ledger cut at the cap drops capital movements silently.
func saxoReadAll[T any](ctx context.Context, s *Saxo, op, path string, query url.Values) ([]T, error) {
	var out []T
	for page := 0; page < saxoMaxPages; page++ {
		q := url.Values{}
		for k, v := range query {
			q[k] = v
		}
		q.Set("$top", strconv.Itoa(saxoPageSize))
		q.Set("$skip", strconv.Itoa(len(out)))

		body, err := s.get(ctx, op, path, q)
		if err != nil {
			return nil, err
		}
		var p saxoPage[T]
		if err := json.Unmarshal(body, &p); err != nil {
			return nil, fmt.Errorf("%s: decode page: %w", op, err)
		}
		out = append(out, p.Data...)
		if p.Next == "" || len(p.Data) == 0 {
			return out, nil
		}
	}
	return nil, fmt.Errorf("%s: more than %d pages of %d rows, refusing a truncated read", op, saxoMaxPages, saxoPageSize)
}

// --- account ----------------------------------------------------------------

type saxoClientDetails struct {
	ClientKey       string `json:"ClientKey"`
	DefaultCurrency string `json:"DefaultCurrency"`
}

func (s *Saxo) ensureClient(ctx context.Context) (saxoClientDetails, error) {
	s.clientMu.Lock()
	defer s.clientMu.Unlock()

	if s.clientKey != "" {
		return saxoClientDetails{ClientKey: s.clientKey, DefaultCurrency: s.baseCurrency}, nil
	}

	body, err := s.get(ctx, "read saxo client", "/port/v1/clients/me", nil)
	if err != nil {
		return saxoClientDetails{}, err
	}
	var details saxoClientDetails
	if err := json.Unmarshal(body, &details); err != nil {
		return saxoClientDetails{}, fmt.Errorf("decode saxo client: %w", err)
	}
	if details.ClientKey == "" {
		return saxoClientDetails{}, errors.New("read saxo client: no ClientKey, no trading account behind this login")
	}

	s.clientKey = details.ClientKey
	s.baseCurrency = details.DefaultCurrency
	return details, nil
}

func (s *Saxo) TestConnection(ctx context.Context) error {
	if !s.configured() {
		return ErrSaxoNotConfigured
	}
	_, err := s.ensureClient(ctx)
	return err
}

// GetBalance reports the client-wide balance in the client's currency.
// UnrealizedPositionsValue, not UnrealizedMarginProfitLoss: Saxo's figures
// reconcile as CashBalance + UnrealizedPositionsValue = TotalValue, so the
// realised balance the sync derives lands on CashBalance.
func (s *Saxo) GetBalance(ctx context.Context) (*Balance, error) {
	s.resetWarnings()

	body, err := s.get(ctx, "read saxo balance", "/port/v1/balances/me", nil)
	if err != nil {
		return nil, err
	}
	var resp struct {
		Currency                  string   `json:"Currency"`
		TotalValue                *float64 `json:"TotalValue"`
		UnrealizedPositionsValue  float64  `json:"UnrealizedPositionsValue"`
		MarginAvailableForTrading *float64 `json:"MarginAvailableForTrading"`
		CashAvailableForTrading   *float64 `json:"CashAvailableForTrading"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("decode saxo balance: %w", err)
	}
	if resp.TotalValue == nil {
		return nil, errors.New("read saxo balance: no TotalValue in the response")
	}

	available := 0.0
	switch {
	case resp.MarginAvailableForTrading != nil:
		available = *resp.MarginAvailableForTrading
	case resp.CashAvailableForTrading != nil:
		available = *resp.CashAvailableForTrading
	}

	currency := resp.Currency
	if currency == "" {
		if details, cerr := s.ensureClient(ctx); cerr == nil {
			currency = details.DefaultCurrency
		}
	}

	return &Balance{
		Available:     available,
		Equity:        *resp.TotalValue,
		UnrealizedPnL: resp.UnrealizedPositionsValue,
		Currency:      currency,
	}, nil
}

// saxoMarketType maps Saxo's AssetType to a market bucket. Options first:
// FxVanillaOption is an option, not a forex line; CFD before stock for
// CfdOnStock.
func saxoMarketType(assetType string) string {
	a := strings.ToLower(strings.TrimSpace(assetType))
	switch {
	case strings.Contains(a, "option"):
		return MarketOptions
	case strings.HasPrefix(a, "cfd"):
		return MarketCFD
	case strings.Contains(a, "futures"):
		return MarketFutures
	case strings.HasPrefix(a, "fx"):
		return MarketForex
	default:
		return MarketStocks
	}
}

// saxoSymbol reads the instrument out of a NetPositionId, "<symbol>_<AssetType>"
// (some gateways double the underscore), which saves a DisplayAndFormat
// lookup per row.
func saxoSymbol(netPositionID, assetType string) string {
	id := strings.TrimSpace(netPositionID)
	if assetType != "" {
		if trimmed, ok := strings.CutSuffix(id, "_"+assetType); ok && trimmed != "" {
			return strings.TrimRight(trimmed, "_")
		}
	}
	if idx := strings.Index(id, "__"); idx > 0 {
		return id[:idx]
	}
	return id
}

type saxoPosition struct {
	NetPositionID string `json:"NetPositionId"`
	PositionBase  struct {
		Amount    float64 `json:"Amount"`
		AssetType string  `json:"AssetType"`
		OpenPrice float64 `json:"OpenPrice"`
	} `json:"PositionBase"`
	PositionView struct {
		CurrentPrice                    float64 `json:"CurrentPrice"`
		ProfitLossOnTradeInBaseCurrency float64 `json:"ProfitLossOnTradeInBaseCurrency"`
	} `json:"PositionView"`
}

func (s *Saxo) GetPositions(ctx context.Context) ([]*Position, error) {
	q := url.Values{}
	q.Set("FieldGroups", "PositionBase,PositionView")
	rows, err := saxoReadAll[saxoPosition](ctx, s, "read saxo positions", "/port/v1/positions/me", q)
	if err != nil {
		return nil, err
	}

	positions := make([]*Position, 0, len(rows))
	for _, p := range rows {
		if p.PositionBase.Amount == 0 {
			continue
		}
		side := "long"
		if p.PositionBase.Amount < 0 {
			side = "short"
		}
		positions = append(positions, &Position{
			Symbol:     saxoSymbol(p.NetPositionID, p.PositionBase.AssetType),
			Side:       side,
			Size:       math.Abs(p.PositionBase.Amount),
			EntryPrice: p.PositionBase.OpenPrice,
			MarkPrice:  p.PositionView.CurrentPrice,
			// Base currency, so it sums against the equity GetBalance reports.
			UnrealizedPnL: p.PositionView.ProfitLossOnTradeInBaseCurrency,
			MarketType:    saxoMarketType(p.PositionBase.AssetType),
		})
	}
	return positions, nil
}

type saxoClosedPosition struct {
	ClosedPositionUniqueID string `json:"ClosedPositionUniqueId"`
	NetPositionID          string `json:"NetPositionId"`
	ClosedPosition         struct {
		AssetType                      string  `json:"AssetType"`
		Amount                         float64 `json:"Amount"`
		BuyOrSell                      string  `json:"BuyOrSell"`
		ClosedProfitLossInBaseCurrency float64 `json:"ClosedProfitLossInBaseCurrency"`
		ClosingPrice                   float64 `json:"ClosingPrice"`
		CostClosingInBaseCurrency      float64 `json:"CostClosingInBaseCurrency"`
		CostOpeningInBaseCurrency      float64 `json:"CostOpeningInBaseCurrency"`
		ExecutionTimeClose             string  `json:"ExecutionTimeClose"`
	} `json:"ClosedPosition"`
}

// GetTrades reads the closed-positions view, one row per closed position,
// which Saxo serves the moment a position closes. It holds recent closes only:
// a long backfill comes up short rather than wrong.
func (s *Saxo) GetTrades(ctx context.Context, start, end time.Time) ([]*Trade, error) {
	q := url.Values{}
	q.Set("FieldGroups", "ClosedPosition")
	rows, err := saxoReadAll[saxoClosedPosition](ctx, s, "read saxo closed positions", "/port/v1/closedpositions/me", q)
	if err != nil {
		return nil, err
	}
	details, err := s.ensureClient(ctx)
	if err != nil {
		return nil, err
	}

	trades := make([]*Trade, 0, len(rows))
	for _, d := range rows {
		cp := d.ClosedPosition
		ts, terr := time.Parse(time.RFC3339, cp.ExecutionTimeClose)
		if terr != nil {
			continue
		}
		ts = ts.UTC()
		if ts.Before(start) || ts.After(end) {
			continue
		}

		side := "buy"
		if strings.EqualFold(cp.BuyOrSell, "Sell") {
			side = "sell"
		}

		trades = append(trades, &Trade{
			ID:       firstNonEmpty(d.ClosedPositionUniqueID, d.NetPositionID),
			Symbol:   saxoSymbol(d.NetPositionID, cp.AssetType),
			Side:     side,
			Price:    cp.ClosingPrice,
			Quantity: math.Abs(cp.Amount),
			// Saxo books costs negative and nets them into the closed P&L; Fee is
			// a positive magnitude everywhere in this package.
			Fee:         math.Abs(cp.CostOpeningInBaseCurrency) + math.Abs(cp.CostClosingInBaseCurrency),
			FeeCurrency: details.DefaultCurrency,
			RealizedPnL: cp.ClosedProfitLossInBaseCurrency,
			Timestamp:   ts,
			MarketType:  saxoMarketType(cp.AssetType),
		})
	}
	return trades, nil
}

// --- ledger -----------------------------------------------------------------

type saxoTransaction struct {
	TransactionType string  `json:"TransactionType"`
	Event           string  `json:"Event"`
	FundingSubType  string  `json:"FundingSubType"`
	Date            string  `json:"Date"`
	Currency        string  `json:"Currency"`
	BookedAmount    float64 `json:"BookedAmount"`
	IntradayAmount  float64 `json:"IntradayAmount"`
	IsIntradayData  bool    `json:"IsIntradayData"`
	Bookings        []struct {
		AmountType string `json:"AmountType"`
	} `json:"Bookings"`
	Instrument struct {
		Symbol string `json:"Symbol"`
	} `json:"Instrument"`
}

// amount is what the row moved in the client's currency. A transaction Saxo
// has not booked yet carries its amount in IntradayAmount alone.
func (t saxoTransaction) amount() float64 {
	if t.BookedAmount != 0 {
		return t.BookedAmount
	}
	return t.IntradayAmount
}

func (t saxoTransaction) label() string {
	for _, b := range t.Bookings {
		if strings.TrimSpace(b.AmountType) != "" {
			return strings.TrimSpace(b.AmountType)
		}
	}
	return firstNonEmpty(t.Event, t.Instrument.Symbol, t.TransactionType)
}

func (t saxoTransaction) is(transactionType string) bool {
	return strings.EqualFold(strings.TrimSpace(t.TransactionType), transactionType)
}

// isSecurityTransfer is shares moved in or out of the account: capital, but
// with no cash leg to book it by.
func (t saxoTransaction) isSecurityTransfer() bool {
	if t.is("CashTransfer") || t.is("CashAmount") {
		return false
	}
	return strings.Contains(strings.ToLower(t.Event), "transfer")
}

type saxoTxMemo struct {
	from, to time.Time
	at       time.Time
	rows     []saxoTransaction
}

// transactions reads the ledger for the booking days [from, to]. The sync asks
// for funding and cashflows over the same window back to back, so one read
// serves both.
func (s *Saxo) transactions(ctx context.Context, from, to time.Time) ([]saxoTransaction, error) {
	s.txMu.Lock()
	if m := s.txMemo; m != nil && m.from.Equal(from) && m.to.Equal(to) && time.Since(m.at) < saxoTxMemoTTL {
		rows := m.rows
		s.txMu.Unlock()
		return rows, nil
	}
	s.txMu.Unlock()

	details, err := s.ensureClient(ctx)
	if err != nil {
		return nil, err
	}
	q := url.Values{}
	q.Set("ClientKey", details.ClientKey)
	q.Set("FromDate", from.Format(time.DateOnly))
	q.Set("ToDate", to.Format(time.DateOnly))
	rows, err := saxoReadAll[saxoTransaction](ctx, s, "read saxo transactions", "/hist/v1/transactions", q)
	if err != nil {
		return nil, err
	}

	s.txMu.Lock()
	s.txMemo = &saxoTxMemo{from: from, to: to, at: time.Now(), rows: rows}
	s.txMu.Unlock()
	return rows, nil
}

func saxoDay(t time.Time) time.Time {
	t = t.UTC()
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}

// saxoDayWindow returns the booking days a read starting at since owns:
// [from, to), to being today. Saxo dates a transfer by day, not instant, so a
// day is read once, by the first sync after it closes; today's transfers
// belong to tomorrow's read.
func saxoDayWindow(since, now time.Time) (from, to time.Time) {
	to = saxoDay(now)
	if since.IsZero() {
		return to.AddDate(0, 0, -1), to
	}
	from = saxoDay(since)
	if from.After(to) {
		from = to
	}
	return from, to
}

func parseSaxoDate(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	if len(s) < len(time.DateOnly) {
		return time.Time{}, fmt.Errorf("parse saxo date %q: too short", s)
	}
	d, err := time.Parse(time.DateOnly, s[:len(time.DateOnly)])
	if err != nil {
		return time.Time{}, fmt.Errorf("parse saxo date %q: %w", s, err)
	}
	return d, nil
}

// saxoCashflows keeps the client's external cash transfers inside [from, to).
// Only CashTransfer moves capital: interest, fees, financing and dividends are
// CashAmount rows and belong to the return. A transfer between the client's own
// accounts has a leg on each side and no money from outside.
func saxoCashflows(rows []saxoTransaction, from, to time.Time, currency string) ([]*Cashflow, []string, error) {
	var flows []*Cashflow
	var warnings []string
	for _, r := range rows {
		flow, warning, err := saxoCapitalRow(r, from, to, currency)
		if err != nil {
			return nil, warnings, err
		}
		if warning != "" && !slices.Contains(warnings, warning) {
			warnings = append(warnings, warning)
		}
		if flow != nil {
			flows = append(flows, flow)
		}
	}
	return flows, warnings, nil
}

// saxoCapitalRow returns the flow one ledger row books, or a warning when the
// row moves capital it cannot book.
func saxoCapitalRow(r saxoTransaction, from, to time.Time, currency string) (*Cashflow, string, error) {
	switch {
	case r.is("CashTransfer"):
		day, err := parseSaxoDate(r.Date)
		if err != nil {
			// A capital row skipped is capital that reads as return.
			return nil, "", fmt.Errorf("saxo cash transfer: %w", err)
		}
		if !saxoInWindow(day, from, to) || strings.EqualFold(strings.TrimSpace(r.FundingSubType), saxoIntraClientTransfer) {
			return nil, "", nil
		}
		amount := r.amount()
		if amount == 0 {
			return nil, "saxo_cash_transfer_without_amount", nil
		}
		return &Cashflow{Amount: amount, Currency: firstNonEmpty(r.Currency, currency), Timestamp: day}, "", nil
	case r.isSecurityTransfer():
		if day, err := parseSaxoDate(r.Date); err == nil && saxoInWindow(day, from, to) {
			return nil, "saxo_security_transfer_unbooked", nil
		}
	}
	return nil, "", nil
}

func saxoInWindow(day, from, to time.Time) bool {
	return !day.Before(from) && day.Before(to)
}

// saxoFundingFees keeps the window's cost and income rows: interest, CFD
// financing, custody and subscription fees, dividends. Signed as booked,
// negative when charged.
func saxoFundingFees(rows []saxoTransaction, from, to time.Time) []*FundingFee {
	var fees []*FundingFee
	for _, r := range rows {
		if !r.is("CashAmount") && !r.is("CorporateAction") {
			continue
		}
		day, err := parseSaxoDate(r.Date)
		if err != nil || !saxoInWindow(day, from, to) {
			continue
		}
		amount := r.amount()
		if amount == 0 {
			continue
		}
		fees = append(fees, &FundingFee{Amount: amount, Symbol: r.label(), Timestamp: day})
	}
	return fees
}

func (s *Saxo) GetCashflows(ctx context.Context, since time.Time) ([]*Cashflow, error) {
	from, to := saxoDayWindow(since, time.Now())
	if !from.Before(to) {
		return nil, nil
	}
	rows, err := s.transactions(ctx, from, to.AddDate(0, 0, -1))
	if err != nil {
		s.noteWarning("saxo_cashflows_unreadable")
		return nil, err
	}
	currency := ""
	if details, cerr := s.ensureClient(ctx); cerr == nil {
		currency = details.DefaultCurrency
	}
	flows, warnings, err := saxoCashflows(rows, from, to, currency)
	for _, w := range warnings {
		s.noteWarning(w)
	}
	if err != nil {
		s.noteWarning("saxo_cashflows_unreadable")
		return nil, err
	}
	return flows, nil
}

// GetFundingFees ignores symbols: Saxo's rows carry their own instrument.
func (s *Saxo) GetFundingFees(ctx context.Context, _ []string, since time.Time) ([]*FundingFee, error) {
	from, to := saxoDayWindow(since, time.Now())
	if !from.Before(to) {
		return nil, nil
	}
	rows, err := s.transactions(ctx, from, to.AddDate(0, 0, -1))
	if err != nil {
		return nil, err
	}
	return saxoFundingFees(rows, from, to), nil
}

func (s *Saxo) resetWarnings() {
	s.warnMu.Lock()
	s.warnings = nil
	s.warnMu.Unlock()
}

func (s *Saxo) noteWarning(w string) {
	s.warnMu.Lock()
	defer s.warnMu.Unlock()
	if !slices.Contains(s.warnings, w) {
		s.warnings = append(s.warnings, w)
	}
}

func (s *Saxo) CapabilityWarnings() []string {
	s.warnMu.Lock()
	defer s.warnMu.Unlock()
	return slices.Clone(s.warnings)
}

// --- probe surface ------------------------------------------------------------

// SaxoRawTransaction is one ledger row as Saxo typed it, for checking the
// classification against a real account.
type SaxoRawTransaction struct {
	TransactionType string
	Event           string
	FundingSubType  string
	AmountType      string
	Date            string
	Amount          float64
	Intraday        bool
}

// RawTransactions returns the ledger for the booking days [from, to].
func (s *Saxo) RawTransactions(ctx context.Context, from, to time.Time) ([]SaxoRawTransaction, error) {
	rows, err := s.transactions(ctx, saxoDay(from), saxoDay(to))
	if err != nil {
		return nil, err
	}
	out := make([]SaxoRawTransaction, 0, len(rows))
	for _, r := range rows {
		amountType := ""
		if len(r.Bookings) > 0 {
			amountType = r.Bookings[0].AmountType
		}
		out = append(out, SaxoRawTransaction{
			TransactionType: r.TransactionType,
			Event:           r.Event,
			FundingSubType:  r.FundingSubType,
			AmountType:      amountType,
			Date:            r.Date,
			Amount:          r.amount(),
			Intraday:        r.IsIntradayData,
		})
	}
	return out, nil
}

// SaxoSeriesPoint is one day of a Saxo performance series.
type SaxoSeriesPoint struct {
	Date  string  `json:"Date"`
	Value float64 `json:"Value"`
}

// TransferSeries returns Saxo's own daily cash and securities transfer series
// over [from, to], an independent account of the capital the ledger moved.
func (s *Saxo) TransferSeries(ctx context.Context, from, to time.Time) (cash, securities []SaxoSeriesPoint, err error) {
	details, err := s.ensureClient(ctx)
	if err != nil {
		return nil, nil, err
	}
	q := url.Values{}
	q.Set("ClientKey", details.ClientKey)
	q.Set("FromDate", saxoDay(from).Format(time.DateOnly))
	q.Set("ToDate", saxoDay(to).Format(time.DateOnly))
	q.Set("FieldGroups", "Balance_CashTransfer,Balance_SecurityTransfer")
	body, err := s.get(ctx, "read saxo performance", "/hist/v4/performance/timeseries", q)
	if err != nil {
		return nil, nil, err
	}
	var resp struct {
		Balance struct {
			CashTransfer     []SaxoSeriesPoint `json:"CashTransfer"`
			SecurityTransfer []SaxoSeriesPoint `json:"SecurityTransfer"`
		} `json:"Balance"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, nil, fmt.Errorf("decode saxo performance: %w", err)
	}
	return resp.Balance.CashTransfer, resp.Balance.SecurityTransfer, nil
}
