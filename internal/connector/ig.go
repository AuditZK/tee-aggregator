package connector

import (
	"bytes"
	"context"
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

const (
	igLiveAPI = "https://api.ig.com/gateway/deal"
	igDemoAPI = "https://demo-api.ig.com/gateway/deal"

	igContentType = "application/json; charset=UTF-8"

	// IG's CST/X-SECURITY-TOKEN pair is valid for 6h and silently extended while
	// in use. Re-login an hour early so a long sync never crosses the boundary.
	igSessionTTL = 5 * time.Hour

	igTimeFormat = "2006-01-02T15:04:05"

	igTransactionPageSize = 200

	// Bounds the transaction walk: a paging upstream that never reports a final
	// page would otherwise pin the sync indefinitely.
	igMaxTransactionPages = 50
)

// igCashflowSign maps IG's cash-movement codes to a deposit/withdrawal sign.
// IG localises transactionType for deals to the account language but keeps
// these codes in English, so they are the only reliable classifier. Interest
// and dividend lines are also cash transactions yet are P&L, not capital — an
// allowlist keeps them out of the cash-flow ledger, where they would land as
// phantom deposits and wreck TWR.
var igCashflowSign = map[string]float64{
	"DEPO":   1,
	"CASHIN": 1,
	"WITH":   -1,
}

type igSession struct {
	cst       string
	xst       string
	accountID string
	expiry    time.Time
}

func (s igSession) valid() bool {
	return s.cst != "" && s.xst != "" && time.Now().Before(s.expiry)
}

// IG implements Connector for IG Group's REST trading API.
type IG struct {
	apiKey   string
	username string
	password string
	baseURL  string
	isDemo   bool
	client   *http.Client

	sessionMu sync.Mutex
	sess      igSession
}

// NewIG creates an IG connector. Credentials map as:
//   - apiKey = IG API key (X-IG-API-KEY)
//   - apiSecret = account password
//   - passphrase = account identifier (username)
//
// Demo and live are separate credential sets on separate hosts, so the caller
// picks the environment via the exchange id rather than a credential field.
func NewIG(creds *Credentials, demo bool) *IG {
	baseURL := igLiveAPI
	if demo {
		baseURL = igDemoAPI
	}
	return &IG{
		apiKey:   strings.TrimSpace(creds.APIKey),
		username: strings.TrimSpace(creds.Passphrase),
		password: creds.APISecret,
		baseURL:  baseURL,
		isDemo:   demo,
		client:   &http.Client{Timeout: 30 * time.Second},
	}
}

func (i *IG) Exchange() string {
	if i.isDemo {
		return "ig_demo"
	}
	return "ig"
}

// DetectIsPaper reports whether this connection targets IG's demo environment.
// A demo account is only reachable through the demo host, so the environment
// the connector was built for is authoritative.
func (i *IG) DetectIsPaper(_ context.Context) (bool, error) {
	return i.isDemo, nil
}

// igSessionRejected reports whether a response body is IG refusing the session
// tokens. The 6h TTL is a floor, not a guarantee — a login from the web
// platform invalidates the pair early — so a rejected token is recoverable by
// re-authenticating rather than a reason to fail the sync.
func igSessionRejected(body []byte) bool {
	return bytes.Contains(body, []byte("error.security.client-token-invalid")) ||
		bytes.Contains(body, []byte("error.security.oauth-token-invalid"))
}

func (i *IG) session(ctx context.Context) (igSession, error) {
	i.sessionMu.Lock()
	defer i.sessionMu.Unlock()

	if i.sess.valid() {
		return i.sess, nil
	}
	if err := i.loginLocked(ctx); err != nil {
		return igSession{}, err
	}
	return i.sess, nil
}

func (i *IG) invalidateSession() {
	i.sessionMu.Lock()
	i.sess = igSession{}
	i.sessionMu.Unlock()
}

func (i *IG) loginLocked(ctx context.Context) error {
	payload, err := json.Marshal(map[string]string{
		"identifier": i.username,
		"password":   i.password,
	})
	if err != nil {
		return fmt.Errorf("encode ig session request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", i.baseURL+"/session", bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("X-IG-API-KEY", i.apiKey)
	req.Header.Set("Version", "2")
	req.Header.Set("Content-Type", igContentType)
	req.Header.Set("Accept", igContentType)

	resp, err := i.client.Do(req)
	if err != nil {
		return fmt.Errorf("%w: post ig session: %w", ErrTransient, err)
	}

	// CONN-AUDIT-001: bounded read for both error and success paths.
	body, err := ReadCappedBody(resp.Body, DefaultMaxResponseBytes)
	if err != nil {
		return fmt.Errorf("read ig session: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		// A rate-limited or unavailable login is not a bad password. Marking it
		// transient stops connection create from rejecting a valid account and
		// lets the scheduler retry instead.
		if isRetryableStatus(resp.StatusCode) {
			return fmt.Errorf("%w: ig session HTTP %d: %s", ErrTransient, resp.StatusCode, vendorErrorDetail(string(body)))
		}
		return fmt.Errorf("ig session HTTP %d: %s", resp.StatusCode, vendorErrorDetail(string(body)))
	}

	cst := resp.Header.Get("CST")
	xst := resp.Header.Get("X-SECURITY-TOKEN")
	if cst == "" || xst == "" {
		return fmt.Errorf("ig session returned no CST/X-SECURITY-TOKEN")
	}

	var parsed struct {
		CurrentAccountID string `json:"currentAccountId"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return fmt.Errorf("decode ig session: %w", err)
	}

	i.sess = igSession{
		cst:       cst,
		xst:       xst,
		accountID: parsed.CurrentAccountID,
		expiry:    time.Now().Add(igSessionTTL),
	}
	return nil
}

func (i *IG) doAuthed(ctx context.Context, version, path string, query url.Values) ([]byte, error) {
	body, err := i.authedOnce(ctx, version, path, query)
	if err == nil || !igSessionRejected(body) {
		return body, err
	}
	i.invalidateSession()
	return i.authedOnce(ctx, version, path, query)
}

func (i *IG) authedOnce(ctx context.Context, version, path string, query url.Values) ([]byte, error) {
	sess, err := i.session(ctx)
	if err != nil {
		return nil, err
	}

	reqURL := i.baseURL + path
	if len(query) > 0 {
		reqURL += "?" + query.Encode()
	}

	return retryHTTP(i.client, func() (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("X-IG-API-KEY", i.apiKey)
		req.Header.Set("CST", sess.cst)
		req.Header.Set("X-SECURITY-TOKEN", sess.xst)
		req.Header.Set("Version", version)
		req.Header.Set("Accept", igContentType)
		return req, nil
	})
}

// parseIGDecimal reads IG's money and level fields, which arrive as display
// strings rather than numbers: profitAndLoss carries the account's currency
// symbol ("E12.34", "£-5.00"), sizes carry an explicit sign, and larger values
// carry thousands separators. Keeping only the numeric characters tolerates
// every variant instead of pinning a prefix that changes with the account.
func parseIGDecimal(s string) (float64, error) {
	var b strings.Builder
	for _, r := range s {
		if (r >= '0' && r <= '9') || r == '.' || r == '-' || r == '+' {
			b.WriteRune(r)
		}
	}
	cleaned := b.String()
	if cleaned == "" || cleaned == "-" || cleaned == "+" {
		return 0, fmt.Errorf("parse ig decimal %q", s)
	}
	return strconv.ParseFloat(cleaned, 64)
}

func parseIGTime(s string) (time.Time, error) {
	if ts, err := time.Parse(igTimeFormat, s); err == nil {
		return ts.UTC(), nil
	}
	ts, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse ig time %q", s)
	}
	return ts.UTC(), nil
}

type igAccount struct {
	AccountID string `json:"accountId"`
	Currency  string `json:"currency"`
	Balance   struct {
		Balance    float64 `json:"balance"`
		ProfitLoss float64 `json:"profitLoss"`
		Available  float64 `json:"available"`
	} `json:"balance"`
}

// currentAccount returns the account the session is scoped to. /positions and
// /history/transactions only ever report that account, while /accounts lists
// every account on the login — reading equity from a different row would pair
// one account's balance with another's track record.
func (i *IG) currentAccount(ctx context.Context) (*igAccount, error) {
	sess, err := i.session(ctx)
	if err != nil {
		return nil, err
	}

	body, err := i.doAuthed(ctx, "1", "/accounts", nil)
	if err != nil {
		return nil, fmt.Errorf("fetch ig accounts: %w", err)
	}

	var resp struct {
		Accounts []igAccount `json:"accounts"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("decode ig accounts: %w", err)
	}
	if len(resp.Accounts) == 0 {
		return nil, fmt.Errorf("no ig accounts found")
	}

	for idx := range resp.Accounts {
		if resp.Accounts[idx].AccountID == sess.accountID {
			return &resp.Accounts[idx], nil
		}
	}
	return nil, fmt.Errorf("ig session account absent from accounts list")
}

func (i *IG) TestConnection(ctx context.Context) error {
	_, err := i.currentAccount(ctx)
	return err
}

// GetBalance reports the session account's equity. IG splits it across two
// fields: `balance` is settled cash and `profitLoss` is the open positions'
// unrealised P&L, so equity is their sum. IG's `deposit` field is the margin
// currently tied up, not cash paid in — capital flows come from GetCashflows.
func (i *IG) GetBalance(ctx context.Context) (*Balance, error) {
	acct, err := i.currentAccount(ctx)
	if err != nil {
		return nil, err
	}

	return &Balance{
		Available:     acct.Balance.Available,
		Equity:        acct.Balance.Balance + acct.Balance.ProfitLoss,
		UnrealizedPnL: acct.Balance.ProfitLoss,
		Currency:      acct.Currency,
	}, nil
}

func igMarketType(instrumentType string) string {
	switch strings.ToUpper(strings.TrimSpace(instrumentType)) {
	case "CURRENCIES":
		return MarketForex
	case "COMMODITIES":
		return MarketCommodities
	case "SHARES":
		return MarketStocks
	default:
		return MarketCFD
	}
}

func (i *IG) GetPositions(ctx context.Context) ([]*Position, error) {
	body, err := i.doAuthed(ctx, "2", "/positions", nil)
	if err != nil {
		return nil, fmt.Errorf("fetch ig positions: %w", err)
	}

	var resp struct {
		Positions []struct {
			Market struct {
				InstrumentName string  `json:"instrumentName"`
				InstrumentType string  `json:"instrumentType"`
				Bid            float64 `json:"bid"`
				Offer          float64 `json:"offer"`
			} `json:"market"`
			Position struct {
				DealID       string  `json:"dealId"`
				Direction    string  `json:"direction"`
				Size         float64 `json:"size"`
				Level        float64 `json:"level"`
				ContractSize float64 `json:"contractSize"`
			} `json:"position"`
		} `json:"positions"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("decode ig positions: %w", err)
	}

	positions := make([]*Position, 0, len(resp.Positions))
	for _, p := range resp.Positions {
		if p.Position.Size == 0 {
			continue
		}

		// A long is closed at the bid and a short at the offer, so each side
		// marks against the price it would actually exit at.
		side, mark := "long", p.Market.Bid
		if strings.EqualFold(p.Position.Direction, "SELL") {
			side, mark = "short", p.Market.Offer
		}

		contractSize := p.Position.ContractSize
		if contractSize == 0 {
			contractSize = 1
		}
		move := mark - p.Position.Level
		if side == "short" {
			move = -move
		}
		// Correct while the instrument is denominated in the account currency;
		// IG applies its own conversion otherwise. The authoritative aggregate
		// is the account's profitLoss, which GetBalance reports.
		unrealized := move * p.Position.Size * contractSize

		positions = append(positions, &Position{
			Symbol:        p.Market.InstrumentName,
			Side:          side,
			Size:          p.Position.Size,
			EntryPrice:    p.Position.Level,
			MarkPrice:     mark,
			UnrealizedPnL: unrealized,
			MarketType:    igMarketType(p.Market.InstrumentType),
		})
	}
	return positions, nil
}

type igTransaction struct {
	CashTransaction bool   `json:"cashTransaction"`
	CloseLevel      string `json:"closeLevel"`
	Currency        string `json:"currency"`
	DateUTC         string `json:"dateUtc"`
	InstrumentName  string `json:"instrumentName"`
	ProfitAndLoss   string `json:"profitAndLoss"`
	Reference       string `json:"reference"`
	Size            string `json:"size"`
	TransactionType string `json:"transactionType"`
}

func (i *IG) fetchTransactions(ctx context.Context, from, to time.Time) ([]igTransaction, error) {
	var out []igTransaction

	for page := 1; page <= igMaxTransactionPages; page++ {
		q := url.Values{}
		q.Set("from", from.UTC().Format(igTimeFormat))
		q.Set("to", to.UTC().Format(igTimeFormat))
		q.Set("pageSize", strconv.Itoa(igTransactionPageSize))
		q.Set("pageNumber", strconv.Itoa(page))

		body, err := i.doAuthed(ctx, "2", "/history/transactions", q)
		if err != nil {
			return nil, fmt.Errorf("fetch ig transactions: %w", err)
		}

		var resp struct {
			Transactions []igTransaction `json:"transactions"`
			MetaData     struct {
				PageData struct {
					TotalPages int `json:"totalPages"`
				} `json:"pageData"`
			} `json:"metaData"`
		}
		if err := json.Unmarshal(body, &resp); err != nil {
			return nil, fmt.Errorf("decode ig transactions: %w", err)
		}

		out = append(out, resp.Transactions...)
		if len(resp.Transactions) == 0 || page >= resp.MetaData.PageData.TotalPages {
			break
		}
	}
	return out, nil
}

func (i *IG) GetTrades(ctx context.Context, start, end time.Time) ([]*Trade, error) {
	txs, err := i.fetchTransactions(ctx, start, end)
	if err != nil {
		return nil, err
	}

	trades := make([]*Trade, 0, len(txs))
	for _, t := range txs {
		if t.CashTransaction {
			continue
		}

		ts, err := parseIGTime(t.DateUTC)
		if err != nil {
			continue
		}
		size, err := parseIGDecimal(t.Size)
		if err != nil {
			continue
		}
		price, err := parseIGDecimal(t.CloseLevel)
		if err != nil {
			continue
		}
		pnl, err := parseIGDecimal(t.ProfitAndLoss)
		if err != nil {
			continue
		}

		side := "buy"
		if size < 0 {
			side = "sell"
		}

		trades = append(trades, &Trade{
			ID:          t.Reference,
			Symbol:      t.InstrumentName,
			Side:        side,
			Price:       price,
			Quantity:    math.Abs(size),
			FeeCurrency: t.Currency,
			RealizedPnL: pnl,
			Timestamp:   ts,
			MarketType:  MarketCFD,
		})
	}
	return trades, nil
}

// GetCashflows returns deposits and withdrawals from the transaction ledger.
// The amount's sign comes from the transaction code rather than the reported
// figure, so a withdrawal booked as a negative profitAndLoss and one booked as
// a positive figure both land as a withdrawal.
func (i *IG) GetCashflows(ctx context.Context, since time.Time) ([]*Cashflow, error) {
	txs, err := i.fetchTransactions(ctx, since, time.Now().UTC())
	if err != nil {
		return nil, err
	}

	flows := make([]*Cashflow, 0)
	for _, t := range txs {
		sign, known := igCashflowSign[strings.ToUpper(strings.TrimSpace(t.TransactionType))]
		if !known {
			continue
		}

		amount, err := parseIGDecimal(t.ProfitAndLoss)
		if err != nil {
			continue
		}
		ts, err := parseIGTime(t.DateUTC)
		if err != nil {
			continue
		}

		flows = append(flows, &Cashflow{
			Amount:    math.Abs(amount) * sign,
			Currency:  t.Currency,
			Timestamp: ts,
		})
	}
	return flows, nil
}
