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

	// Fallback when the login's expires_in is absent or unreadable — IG's v3
	// access tokens are documented at 60 seconds.
	igTokenFallbackLifetime = 60 * time.Second
	// Re-login this long before the stated expiry so a request in flight does
	// not straddle it.
	igTokenSafetyMargin = 10 * time.Second

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
	token string
	// accountID is the login's default account — only a bootstrap identity for
	// listing accounts. Business requests pin their own account explicitly.
	accountID string
	expiry    time.Time
	// gen identifies which login produced this token, so a caller holding a
	// rejected token can only retire that one.
	gen uint64
}

func (s igSession) valid() bool {
	return s.token != "" && time.Now().Before(s.expiry)
}

// IG implements Connector for IG Group's REST trading API.
type IG struct {
	apiKey   string
	username string
	password string
	// pinnedAccountID, when set, bypasses account discovery entirely — see
	// NewIG for why discovery can be structurally impossible.
	pinnedAccountID string
	baseURL         string
	isDemo          bool
	client          *http.Client

	sessionMu  sync.Mutex
	sess       igSession
	sessionGen uint64

	accountMu         sync.Mutex
	selectedAccountID string
}

// NewIG creates an IG connector. Credentials map as:
//   - apiKey = IG API key (X-IG-API-KEY)
//   - apiSecret = account password
//   - passphrase = account identifier (username), optionally
//     "identifier:ACCOUNTID" to pin the account to read
//
// The pin exists because account discovery can be structurally impossible:
// listing accounts requires acting under some account's identity, and when the
// login's default account is one the Web API refuses (share dealing,
// exchange-traded), that bootstrap identity is refused too — observed against a
// real login whose default was a Turbo24 account. The suffix rides in the
// identifier slot the way MetaTrader rides "broker:port" in its passphrase.
//
// Demo and live are separate credential sets on separate hosts, so the caller
// picks the environment via the exchange id rather than a credential field.
func NewIG(creds *Credentials, demo bool) *IG {
	baseURL := igLiveAPI
	if demo {
		baseURL = igDemoAPI
	}

	identifier := strings.TrimSpace(creds.Passphrase)
	pinned := ""
	if at := strings.IndexByte(identifier, ':'); at >= 0 {
		pinned = strings.TrimSpace(identifier[at+1:])
		identifier = strings.TrimSpace(identifier[:at])
	}

	return &IG{
		apiKey:          strings.TrimSpace(creds.APIKey),
		username:        identifier,
		password:        creds.APISecret,
		pinnedAccountID: pinned,
		baseURL:         baseURL,
		isDemo:          demo,
		client:          &http.Client{Timeout: 30 * time.Second},
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
// token. The stated lifetime is short and not a guarantee either way, so a
// rejected token is recoverable by re-authenticating rather than a reason to
// fail the sync.
func igSessionRejected(body []byte) bool {
	return bytes.Contains(body, []byte("error.security.client-token-invalid")) ||
		bytes.Contains(body, []byte("error.security.oauth-token-invalid")) ||
		bytes.Contains(body, []byte("error.security.oauth-token-expired"))
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

// invalidateSession retires the session whose generation was rejected. Clearing
// unconditionally would let a caller destroy a pair a concurrent caller had
// just logged in for: that caller's retry then goes out with a token this login
// invalidated, exhausts its single retry, and returns a bare 401 — which
// connection create reads as bad credentials and rejects a healthy account.
func (i *IG) invalidateSession(gen uint64) {
	i.sessionMu.Lock()
	if i.sess.gen == gen {
		i.sess = igSession{}
	}
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

	// Session v3, deliberately. The v1/v2 CST login is refused outright
	// (error.public-api.failure.stockbroking-not-supported, observed against a
	// real account) whenever the login's DEFAULT account is a share-dealing
	// one — even when an eligible CFD account sits right next to it, and the
	// login API offers no way to pick. v3 authenticates the client instead and
	// lets every request pin its account via IG-ACCOUNT-ID, so a user's
	// default-account setting can't brick the connection.
	req, err := http.NewRequestWithContext(ctx, "POST", i.baseURL+"/session", bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("X-IG-API-KEY", i.apiKey)
	req.Header.Set("Version", "3")
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
		// IG's Web API serves CFD and spread-bet accounts only; a share-dealing
		// login is refused at the session itself. Left as a bare status this
		// reads as a credential failure, sending the holder of a perfectly good
		// account to re-check a password that was never the problem.
		if bytes.Contains(body, []byte("stockbroking-not-supported")) {
			return fmt.Errorf("ig share-dealing accounts are not served by the Web API: connect a CFD or spread-bet account instead")
		}
		return fmt.Errorf("ig session HTTP %d: %s", resp.StatusCode, vendorErrorDetail(string(body)))
	}

	var parsed struct {
		AccountID  string `json:"accountId"`
		OAuthToken struct {
			AccessToken string `json:"access_token"`
			ExpiresIn   string `json:"expires_in"`
		} `json:"oauthToken"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return fmt.Errorf("decode ig session: %w", err)
	}
	if parsed.OAuthToken.AccessToken == "" {
		return fmt.Errorf("ig session returned no access token")
	}

	lifetime := igTokenFallbackLifetime
	if secs, perr := strconv.Atoi(strings.TrimSpace(parsed.OAuthToken.ExpiresIn)); perr == nil && secs > 0 {
		lifetime = time.Duration(secs) * time.Second
	}
	if lifetime > igTokenSafetyMargin {
		lifetime -= igTokenSafetyMargin
	}

	i.sessionGen++
	i.sess = igSession{
		token:     parsed.OAuthToken.AccessToken,
		accountID: parsed.AccountID,
		expiry:    time.Now().Add(lifetime),
		gen:       i.sessionGen,
	}
	return nil
}

// doAuthed issues an authenticated GET against accountID. An empty accountID
// falls back to the login's default account — only account discovery itself
// should do that; every business read names the account it wants.
func (i *IG) doAuthed(ctx context.Context, version, path string, query url.Values, accountID string) ([]byte, error) {
	body, gen, err := i.authedOnce(ctx, version, path, query, accountID)
	if err == nil || !igSessionRejected(body) {
		return body, err
	}
	i.invalidateSession(gen)
	body, _, err = i.authedOnce(ctx, version, path, query, accountID)
	return body, err
}

// authedOnce reports the session generation it signed the request with, so a
// rejection retires that token and not whichever one happens to be cached by
// the time the caller reacts.
func (i *IG) authedOnce(ctx context.Context, version, path string, query url.Values, accountID string) ([]byte, uint64, error) {
	sess, err := i.session(ctx)
	if err != nil {
		return nil, 0, err
	}
	if accountID == "" {
		accountID = sess.accountID
	}

	reqURL := i.baseURL + path
	if len(query) > 0 {
		reqURL += "?" + query.Encode()
	}

	body, err := retryHTTP(i.client, func() (*http.Request, error) {
		req, rerr := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
		if rerr != nil {
			return nil, rerr
		}
		req.Header.Set("X-IG-API-KEY", i.apiKey)
		req.Header.Set("Authorization", "Bearer "+sess.token)
		req.Header.Set("IG-ACCOUNT-ID", accountID)
		req.Header.Set("Version", version)
		req.Header.Set("Accept", igContentType)
		return req, nil
	})
	return body, sess.gen, err
}

// igDecimalSeparator reports which of '.' / ',' in digits acts as the decimal
// point, or 0 when both are grouping.
//
// IG renders these fields for display in the account's own locale, so both
// "1,234.56" and "1.234,56" occur and mean the same amount. Stripping one
// separator by convention silently turns the second form into 1.23456 — a
// thousandfold error that no later stage can detect.
func igDecimalSeparator(digits string) rune {
	lastDot := strings.LastIndexByte(digits, '.')
	lastComma := strings.LastIndexByte(digits, ',')

	// Both present: the rightmost one is the decimal point and the other groups.
	if lastDot >= 0 && lastComma >= 0 {
		if lastDot > lastComma {
			return '.'
		}
		return ','
	}

	sep, at := rune(0), -1
	switch {
	case lastDot >= 0:
		sep, at = '.', lastDot
	case lastComma >= 0:
		sep, at = ',', lastComma
	default:
		return 0
	}

	// Repeated, so it groups: "1.234.567".
	if strings.Count(digits, string(sep)) > 1 {
		return 0
	}
	// A lone separator trailed by exactly three digits is the ambiguous case
	// ("1,234"). Grouping is the reading that matches IG's English samples, and
	// it is also the safer error: mistaking a decimal point for a group marker
	// inflates by a thousand, the reverse only shaves a fraction.
	if len(digits)-at-1 == 3 {
		return 0
	}
	return sep
}

// ParseIGDecimal reads IG's money and level fields, which arrive as display
// strings rather than numbers: profitAndLoss carries the account's currency
// symbol ("E12.34", "£-5.00"), sizes carry an explicit sign, and larger values
// carry grouped digits in the account's locale.
func ParseIGDecimal(s string) (float64, error) {
	var sign, digits strings.Builder
	for _, r := range s {
		switch {
		case r >= '0' && r <= '9', r == '.', r == ',':
			digits.WriteRune(r)
		case (r == '-' || r == '+') && sign.Len() == 0 && digits.Len() == 0:
			sign.WriteRune(r)
		}
	}

	body := digits.String()
	if strings.Trim(body, ".,") == "" {
		return 0, fmt.Errorf("parse ig decimal %q", s)
	}

	switch sep := igDecimalSeparator(body); sep {
	case 0:
		body = strings.NewReplacer(".", "", ",", "").Replace(body)
	case ',':
		body = strings.ReplaceAll(body, ".", "")
		body = strings.Replace(body, ",", ".", 1)
	default:
		body = strings.ReplaceAll(body, ",", "")
	}

	return strconv.ParseFloat(sign.String()+body, 64)
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
	AccountID   string `json:"accountId"`
	AccountType string `json:"accountType"`
	Preferred   bool   `json:"preferred"`
	Currency    string `json:"currency"`
	Balance     struct {
		Balance    float64 `json:"balance"`
		ProfitLoss float64 `json:"profitLoss"`
		Available  float64 `json:"available"`
	} `json:"balance"`
}

// igEligibleAccount reports whether the Web API serves this account type. CFD
// and spread-bet are served; share-dealing (PHYSICAL) and the exchange-traded
// products are not — reading one of those is what the v1/v2 login refused
// wholesale.
func igEligibleAccount(a igAccount) bool {
	t := strings.ToUpper(strings.TrimSpace(a.AccountType))
	return t == "CFD" || t == "SPREADBET"
}

// selectIGAccount picks the account every read is pinned to: the preferred
// eligible account when the user marked one, the first eligible otherwise.
// Reading a different account than the one whose trades we fetch would pair
// one account's balance with another's track record.
func selectIGAccount(accounts []igAccount) (*igAccount, error) {
	var first *igAccount
	for idx := range accounts {
		if !igEligibleAccount(accounts[idx]) {
			continue
		}
		if accounts[idx].Preferred {
			return &accounts[idx], nil
		}
		if first == nil {
			first = &accounts[idx]
		}
	}
	if first == nil {
		return nil, fmt.Errorf("no CFD or spread-bet account on this IG login: the Web API serves only those")
	}
	return first, nil
}

// fetchSelectedAccount lists the login's accounts and picks the account to
// read. The listing itself must act under some account's identity: the pin
// when the user set one, the login's default otherwise. A default the Web API
// refuses (share dealing, exchange-traded) refuses the listing too — that is
// the one failure the user must resolve, so the error names both ways out.
func (i *IG) fetchSelectedAccount(ctx context.Context) (*igAccount, error) {
	body, err := i.doAuthed(ctx, "1", "/accounts", nil, i.pinnedAccountID)
	if err != nil {
		if strings.Contains(err.Error(), "stockbroking-not-supported") {
			return nil, fmt.Errorf("ig refuses this login's default account (share dealing / exchange-traded): set a CFD or spread-bet account as the default in My IG, or pin one as \"identifier:ACCOUNTID\"")
		}
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

	if i.pinnedAccountID != "" {
		for idx := range resp.Accounts {
			if resp.Accounts[idx].AccountID == i.pinnedAccountID {
				if !igEligibleAccount(resp.Accounts[idx]) {
					return nil, fmt.Errorf("pinned ig account %s is not a CFD or spread-bet account", i.pinnedAccountID)
				}
				return &resp.Accounts[idx], nil
			}
		}
		return nil, fmt.Errorf("pinned ig account %s absent from this login's accounts", i.pinnedAccountID)
	}
	return selectIGAccount(resp.Accounts)
}

// ensureAccountID returns the selected account's id, resolving it once. The
// selection is structural (account types on a login don't churn), so the id is
// cached; balances are NOT read through this cache.
func (i *IG) ensureAccountID(ctx context.Context) (string, error) {
	if i.pinnedAccountID != "" {
		return i.pinnedAccountID, nil
	}

	i.accountMu.Lock()
	defer i.accountMu.Unlock()

	if i.selectedAccountID != "" {
		return i.selectedAccountID, nil
	}
	acct, err := i.fetchSelectedAccount(ctx)
	if err != nil {
		return "", err
	}
	i.selectedAccountID = acct.AccountID
	return i.selectedAccountID, nil
}

func (i *IG) TestConnection(ctx context.Context) error {
	_, err := i.fetchSelectedAccount(ctx)
	return err
}

// GetBalance reports the selected account's equity, re-reading /accounts every
// time so the figures are fresh. IG splits equity across two fields: `balance`
// is settled cash and `profitLoss` is the open positions' unrealised P&L, so
// equity is their sum. IG's `deposit` field is the margin currently tied up,
// not cash paid in — capital flows come from GetCashflows.
func (i *IG) GetBalance(ctx context.Context) (*Balance, error) {
	acct, err := i.fetchSelectedAccount(ctx)
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

// igMarketType is MarketCFD for everything these accounts hold. IG's
// instrumentType describes the UNDERLYING, not the instrument: a live demo
// returned instrumentType "CURRENCIES" for "Crypto 10 Index", so routing on it
// files a crypto index under forex. What the account actually holds is a CFD
// (or a spread bet) whatever the underlying, which is also what the ledger's
// trades and the account's equity bucket say — so all three now agree. Mirrors
// MetaTrader, the other CFD-only connector.
func igMarketType(_ string) string {
	return MarketCFD
}

func (i *IG) GetPositions(ctx context.Context) ([]*Position, error) {
	accountID, err := i.ensureAccountID(ctx)
	if err != nil {
		return nil, err
	}

	body, err := i.doAuthed(ctx, "2", "/positions", nil, accountID)
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
		// Denominated in the INSTRUMENT's currency, which is often not the
		// account's: IG returns no per-position P&L and no conversion rate, so
		// this cannot be expressed in account terms and Position carries no
		// currency to say so. Measured against a live demo — a USD position on
		// a EUR account computed -79.79 where IG reported -70.79, off by the FX
		// rate. Correct per instrument, not summable across them. The
		// authoritative aggregate is the account's own profitLoss, which
		// GetBalance reports and the pipeline actually consumes; nothing reads
		// these positions today.
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

// IGRawTransaction is one unparsed line of IG's transaction ledger, with the
// vendor's own strings and codes preserved. The parsed views drop what they
// cannot classify, so this is the surface to read when a balance move does not
// reconcile — the same role cTrader's raw cash-flow probe plays.
type IGRawTransaction struct {
	CashTransaction bool   `json:"cashTransaction"`
	CloseLevel      string `json:"closeLevel"`
	Currency        string `json:"currency"`
	DateUTC         string `json:"dateUtc"`
	InstrumentName  string `json:"instrumentName"`
	OpenLevel       string `json:"openLevel"`
	ProfitAndLoss   string `json:"profitAndLoss"`
	Reference       string `json:"reference"`
	Size            string `json:"size"`
	TransactionType string `json:"transactionType"`
}

// RawTransactions returns the unfiltered ledger for [since, until].
func (i *IG) RawTransactions(ctx context.Context, since, until time.Time) ([]IGRawTransaction, error) {
	return i.fetchTransactions(ctx, since, until)
}

func (i *IG) fetchTransactions(ctx context.Context, from, to time.Time) ([]IGRawTransaction, error) {
	accountID, err := i.ensureAccountID(ctx)
	if err != nil {
		return nil, err
	}

	var out []IGRawTransaction
	complete := false

	for page := 1; page <= igMaxTransactionPages; page++ {
		q := url.Values{}
		q.Set("from", from.UTC().Format(igTimeFormat))
		q.Set("to", to.UTC().Format(igTimeFormat))
		q.Set("pageSize", strconv.Itoa(igTransactionPageSize))
		q.Set("pageNumber", strconv.Itoa(page))

		body, err := i.doAuthed(ctx, "2", "/history/transactions", q, accountID)
		if err != nil {
			return nil, fmt.Errorf("fetch ig transactions: %w", err)
		}

		var resp struct {
			Transactions []IGRawTransaction `json:"transactions"`
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
		if len(resp.Transactions) == 0 {
			complete = true
			break
		}

		// Trusting the reported page count alone would stop the walk after one
		// page whenever the count is absent or zero, silently returning a
		// truncated ledger — and a dropped deposit reads as return. Without a
		// count, a short page is the end.
		if total := resp.MetaData.PageData.TotalPages; total > 0 {
			if page >= total {
				complete = true
				break
			}
		} else if len(resp.Transactions) < igTransactionPageSize {
			complete = true
			break
		}
	}

	// Running out of pages is not an end-of-ledger. IG returns newest first, so
	// the rows beyond the cap are the OLDEST — the inception deposit among them,
	// and equity that appears from nowhere reads as pure return. Fail loudly
	// rather than hand back a ledger that looks whole.
	if !complete {
		return nil, fmt.Errorf("ig ledger exceeds %d pages of %d for %s..%s: refusing a truncated history",
			igMaxTransactionPages, igTransactionPageSize,
			from.UTC().Format(time.DateOnly), to.UTC().Format(time.DateOnly))
	}
	return out, nil
}

// igTradeFrom converts one ledger line to a Trade, reporting false when the
// line is not a dealt trade inside [start, end] or carries a field that will
// not parse.
func igTradeFrom(t IGRawTransaction, start, end time.Time) (*Trade, bool) {
	if t.CashTransaction {
		return nil, false
	}

	ts, err := parseIGTime(t.DateUTC)
	if err != nil {
		return nil, false
	}
	// The from/to query params carry no zone designator, so an upstream reading
	// them as account-local hands back lines outside the requested window;
	// dateUtc is explicitly UTC, so re-cut against it. A leaked line would
	// otherwise be counted on two consecutive sync days.
	if ts.Before(start) || ts.After(end) {
		return nil, false
	}

	// Size carries the direction and pnl is the headline figure, so neither can
	// be guessed at. The close level only feeds volume, and IG writes "-" for a
	// level that does not apply — letting that veto the row would discard a real
	// trade, and its P&L with it, over a cosmetic field.
	size, err := ParseIGDecimal(t.Size)
	if err != nil {
		return nil, false
	}
	pnl, err := ParseIGDecimal(t.ProfitAndLoss)
	if err != nil {
		return nil, false
	}
	price, err := ParseIGDecimal(t.CloseLevel)
	if err != nil {
		price = 0
	}

	side := "buy"
	if size < 0 {
		side = "sell"
	}

	// Fee stays 0 by decision, not omission: IG's ledger does not itemise
	// costs per deal — spread, commission and funding are already netted into
	// the line's profitAndLoss. Nothing is lost (realised P&L is complete);
	// snapshot TotalFees reads 0 for IG because the itemisation does not exist
	// upstream. Inventing a split here would double-count costs.
	return &Trade{
		ID:          t.Reference,
		Symbol:      t.InstrumentName,
		Side:        side,
		Price:       price,
		Quantity:    math.Abs(size),
		FeeCurrency: t.Currency,
		RealizedPnL: pnl,
		Timestamp:   ts,
		MarketType:  MarketCFD,
	}, true
}

func (i *IG) GetTrades(ctx context.Context, start, end time.Time) ([]*Trade, error) {
	txs, err := i.fetchTransactions(ctx, start, end)
	if err != nil {
		return nil, err
	}

	trades := make([]*Trade, 0, len(txs))
	for _, t := range txs {
		if trade, ok := igTradeFrom(t, start, end); ok {
			trades = append(trades, trade)
		}
	}
	return trades, nil
}

// isIGCapitalMovement reports whether a ledger line is a genuine deposit or
// withdrawal of client funds. A DEPO/CASHIN/WITH code alone is not enough: IG
// reuses WITH for charges (funding, commission) that carry cashTransaction=false
// — real capital always carries cashTransaction=true.
func isIGCapitalMovement(t IGRawTransaction) bool {
	_, isCashCode := igCashflowSign[strings.ToUpper(strings.TrimSpace(t.TransactionType))]
	return isCashCode && t.CashTransaction
}

// isIGTradeLine reports whether a ledger line is a dealt trade, whose P&L is
// already returned by GetTrades. A trade carries a parseable deal size and is
// never a cash transaction; charges write size="-", which does not parse.
func isIGTradeLine(t IGRawTransaction) bool {
	if t.CashTransaction {
		return false
	}
	_, err := ParseIGDecimal(t.Size)
	return err == nil
}

// GetFundingFees returns the ledger's cost and income lines: overnight
// funding, commission, interest, dividends.
//
// IG books these as their own rows rather than netting them into a deal's P&L
// ("overnight funding charges appear as separate transactions ... and won't
// affect your running profit/loss"), so a deal's profitAndLoss is complete for
// what the deal did and says nothing about what holding it cost. Without this
// they vanish entirely: equity falls and nothing records why.
//
// A cost line is whatever is neither a capital movement nor a dealt trade.
// Classifying by exclusion rather than by an allowlist of fee codes is
// deliberate — IG's fee codes are undocumented, and it books charges under the
// capital code WITH with cashTransaction=false as readily as under a bespoke
// fee code, so an allowlist would drop them in silence.
//
// symbols is ignored: IG's rows carry their own instrument and are not scoped
// to the swap symbols a crypto venue would want.
func (i *IG) GetFundingFees(ctx context.Context, _ []string, since time.Time) ([]*FundingFee, error) {
	txs, err := i.fetchTransactions(ctx, since, time.Now().UTC())
	if err != nil {
		return nil, err
	}

	fees := make([]*FundingFee, 0)
	for _, t := range txs {
		// A cost or income line is whatever is neither a real capital movement
		// (GetCashflows books those) nor a dealt trade (GetTrades books its
		// P&L). IG books charges under two shapes — an unknown fee code marked
		// a cash transaction, and the capital code WITH marked
		// cashTransaction=false — and exclusion catches both.
		if isIGCapitalMovement(t) || isIGTradeLine(t) {
			continue
		}

		// Unlike a capital line, a cost line that will not parse is skipped
		// rather than failing the sync: equity already carries the charge, so
		// performance stays correct and only the cost breakdown is short. A
		// dropped capital line, by contrast, corrupts TWR outright.
		amount, aerr := ParseIGDecimal(t.ProfitAndLoss)
		if aerr != nil || amount == 0 {
			continue
		}
		ts, terr := parseIGTime(t.DateUTC)
		if terr != nil || ts.Before(since) {
			continue
		}

		// Signed as IG reports it — negative when charged — matching the
		// convention the crypto connectors pass through.
		fees = append(fees, &FundingFee{
			Amount:    amount,
			Symbol:    t.InstrumentName,
			Timestamp: ts,
		})
	}
	return fees, nil
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
		// A cash-movement code is real capital only when IG flags it a cash
		// transaction. IG also books charges (funding, commission, interest)
		// under WITH with cashTransaction=false; counting those as withdrawals
		// hides the cost from TWR — the phantom-cashflow failure, inverted.
		if !known || !t.CashTransaction {
			continue
		}

		// The code already told us this row moves capital, so failing to read its
		// amount or date is not a row we can skip: dropping it understates
		// capital, and capital that never entered the ledger reads as return.
		amount, err := ParseIGDecimal(t.ProfitAndLoss)
		if err != nil {
			return nil, fmt.Errorf("ig %s cash line has an unreadable amount: %w", t.TransactionType, err)
		}
		ts, err := parseIGTime(t.DateUTC)
		if err != nil {
			return nil, fmt.Errorf("ig %s cash line has an unreadable date: %w", t.TransactionType, err)
		}
		// Same zone-designator caveat as GetTrades, and it bites harder here: a
		// deposit that leaks in from before the window was already booked by an
		// earlier sync, and counting it twice is exactly the phantom capital
		// inflow that craters TWR.
		if ts.Before(since) {
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
