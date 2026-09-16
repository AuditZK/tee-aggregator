package connector

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

const (
	ibkrFlexURL    = "https://gdcdyn.interactivebrokers.com/Universal/servlet/FlexStatementService.SendRequest"
	ibkrFlexGetURL = "https://gdcdyn.interactivebrokers.com/Universal/servlet/FlexStatementService.GetStatement"
)

// flexReportEntry holds a cached Flex XML report, shared across IBKR connector
// instances that use the same token:queryId. Required because IBKR enforces a
// token-level rate limit (error 1018) and users who link the same Flex
// credentials would otherwise hammer each other out.
type flexReportEntry struct {
	xml       []byte
	fetchedAt time.Time
}

var (
	flexReportCache   = make(map[string]*flexReportEntry)
	flexReportCacheMu sync.Mutex
	// flexSingleflight coalesces concurrent fetches for the same token:queryId.
	// It does NOT protect two different query IDs sharing one token — that is
	// what the token gate below is for.
	flexSingleflight singleflight.Group

	// flexTokenLastReq records the last time each Flex TOKEN went to the
	// network. IBKR's 1018 rate limit is per token, but the cache and
	// singleflight above are keyed token:queryID — so a user with two
	// accounts on one token (CTO+PEA, two query IDs) raced both requests
	// into IBKR within the same millisecond, every midnight, and the loser
	// burned a second request on its follow-up GetBalance for good measure.
	// The gate makes the loser fail locally, instantly and typed, without
	// spending IBKR's budget; the sync layer's deferred retry (6h) then
	// picks it up long after the cooldown.
	flexTokenLastReq   = make(map[string]time.Time)
	flexTokenLastReqMu sync.Mutex
)

const flexReportCacheTTL = 5 * time.Minute

// flexTokenCooldown paces requests on one Flex token. IBKR documents the
// limit with error 1018: one request per second, ten per minute, per token.
// This was 3h, read off an observation rather than the documentation, and the
// observation was a misreading: the refusals that suggested it are 1001, which
// IBKR defines as transient and answers with "please try again shortly". A
// three-hour gate turns that invitation into a day of silence, and it denied
// the enclave its own scheduled sync. One minute keeps the guard this exists
// for — two connections on one token racing into IBKR in the same millisecond,
// where the loser spends a request for nothing — with ten times the headroom
// the documented limit asks for.
const flexTokenCooldown = time.Minute

// flexStaleReuseWindow is how long a statement already fetched may still be
// served once the gate denies a fresh request. It is deliberately not the
// cooldown: pacing the network and deciding when a document goes stale are
// different questions, and tying them together meant shortening one silently
// shortened the other. IBKR statements carry a full period, so a copy a few
// hours old answers every question the fresh one would.
const flexStaleReuseWindow = 3 * time.Hour

// ErrFlexTokenBusy is returned instead of a network call when the shared
// Flex token was used too recently. It wraps ErrTransient (this is pacing,
// not a credential problem) and its message is matched by the sync layer's
// isRateLimitError, which arms the deferred retry.
var ErrFlexTokenBusy = fmt.Errorf("%w: shared flex token cooling down", ErrTransient)

// claimFlexToken reports whether the token may go to the network now, and
// claims it when allowed. The claim is taken BEFORE the request is sent and
// kept even if the request then fails: a failed send still spent IBKR-side
// budget, and holding the claim is what stops the same sync cycle from
// spending a second one.
func claimFlexToken(token string, now time.Time) (lastUse time.Time, ok bool) {
	flexTokenLastReqMu.Lock()
	defer flexTokenLastReqMu.Unlock()
	if last, used := flexTokenLastReq[token]; used && now.Sub(last) < flexTokenCooldown {
		return last, false
	}
	flexTokenLastReq[token] = now
	return time.Time{}, true
}

// IBKR implements Connector for Interactive Brokers via Flex Query
type IBKR struct {
	token   string // Flex Web Service Token
	queryID string // Flex Query ID
	client  *http.Client

	// capabilityWarnings carries statement-shape problems discovered by the
	// last Flex parse — cash transaction types this connector does not
	// classify. Surfaced through CapabilityWarner like Binance's key-scope
	// gaps, because the failure mode is the same shape: money moved, the
	// number on screen silently stopped being the account.
	capabilityWarnings []string

	// Cached from last GetBalance call (avoids extra Flex requests)
	cachedBreakdown []*MarketBalance
	cachedIsPaper   *bool
	// cachedBalanceAsOf is the reportDate of the summary the last GetBalance
	// call returned — the real age of the figure, which can lag today by two
	// days (Flex statements generate after close). Zero when no date parsed.
	cachedBalanceAsOf time.Time
}

// NewIBKR creates a new IBKR connector
func NewIBKR(creds *Credentials) *IBKR {
	return &IBKR{
		token:   creds.APIKey,
		queryID: creds.APISecret,
		// 5min per HTTP call — large Flex reports (YTD, 30 days) occasionally
		// take 1-2min to respond; with poll retries the total budget reaches
		// ~4min so no individual request should ever hit this ceiling.
		client: &http.Client{Timeout: 5 * time.Minute},
	}
}

// fetchFlexReport returns the raw Flex XML, reusing a cached copy if it is
// younger than flexReportCacheTTL. Callers that need fresh data across syncs
// rely on the daily sync cadence (24h apart, well beyond the cache TTL).
func (i *IBKR) fetchFlexReport(ctx context.Context) ([]byte, error) {
	key := i.token + ":" + i.queryID

	// Fast path: fresh cache hit.
	flexReportCacheMu.Lock()
	if entry, ok := flexReportCache[key]; ok && time.Since(entry.fetchedAt) < flexReportCacheTTL {
		xml := entry.xml
		flexReportCacheMu.Unlock()
		return xml, nil
	}
	flexReportCacheMu.Unlock()

	// Slow path: coalesce concurrent fetches for the same token so parallel
	// user syncs share one Flex API round-trip.
	v, err, _ := flexSingleflight.Do(key, func() (interface{}, error) {
		// Re-check the cache inside the flight in case another goroutine won
		// the singleflight race and already populated it.
		flexReportCacheMu.Lock()
		if entry, ok := flexReportCache[key]; ok && time.Since(entry.fetchedAt) < flexReportCacheTTL {
			xml := entry.xml
			flexReportCacheMu.Unlock()
			return xml, nil
		}
		flexReportCacheMu.Unlock()

		if last, ok := claimFlexToken(i.token, time.Now()); !ok {
			// Our own report from earlier in this window is still usable —
			// a slow sync whose 5-minute cache lapsed mid-cycle, or an admin
			// re-parse after a deploy, keeps working on the same-day XML.
			flexReportCacheMu.Lock()
			if entry, ok2 := flexReportCache[key]; ok2 && time.Since(entry.fetchedAt) < flexStaleReuseWindow {
				xml := entry.xml
				flexReportCacheMu.Unlock()
				return xml, nil
			}
			flexReportCacheMu.Unlock()
			return nil, fmt.Errorf("%w (in use %s ago; retry after ~%s UTC)",
				ErrFlexTokenBusy,
				time.Since(last).Round(time.Second),
				last.Add(flexTokenCooldown).UTC().Format("15:04"))
		}

		refCode, err := i.requestFlexReport(ctx)
		if err != nil {
			return nil, err
		}
		report, err := i.getFlexReport(ctx, refCode)
		if err != nil {
			return nil, err
		}

		flexReportCacheMu.Lock()
		flexReportCache[key] = &flexReportEntry{xml: report, fetchedAt: time.Now()}
		evictFlexReportCache(time.Now())
		flexReportCacheMu.Unlock()
		return report, nil
	})
	if err != nil {
		return nil, err
	}
	return v.([]byte), nil
}

// flexReportCacheMaxEntries caps the process-wide Flex cache. Each entry can
// hold up to IBKRFlexMaxResponseBytes of XML, and nothing ever removed one:
// the map grew one entry per token:queryID for the life of the process, which
// on a large IBKR estate is a slow OOM against the production GOMEMLIMIT.
// CONN-15b, bounded the way internal/errtrack/store.go bounds its groups.
const flexReportCacheMaxEntries = 256

// evictFlexReportCache drops entries no read can still use, then trims the
// oldest until the cap holds. Caller must hold flexReportCacheMu. Recency is
// fetchedAt: an entry is only ever rewritten by a fresh fetch, so insertion
// order and use order coincide.
func evictFlexReportCache(now time.Time) {
	for key, entry := range flexReportCache {
		if now.Sub(entry.fetchedAt) >= flexStaleReuseWindow {
			delete(flexReportCache, key)
		}
	}
	for len(flexReportCache) > flexReportCacheMaxEntries {
		var oldestKey string
		var oldest time.Time
		for key, entry := range flexReportCache {
			if oldestKey == "" || entry.fetchedAt.Before(oldest) {
				oldestKey, oldest = key, entry.fetchedAt
			}
		}
		delete(flexReportCache, oldestKey)
	}
}

func (i *IBKR) Exchange() string {
	return "ibkr"
}

// DetectIsPaper mirrors TS behavior:
// IBKR paper accounts typically use DU/DF account ID prefixes.
// Uses cached result from GetBalance to avoid extra Flex API call.
func (i *IBKR) DetectIsPaper(ctx context.Context) (bool, error) {
	if i.cachedIsPaper != nil {
		return *i.cachedIsPaper, nil
	}
	// Cache empty — call GetBalance to populate it (parses accountId from XML)
	if _, err := i.GetBalance(ctx); err != nil {
		return false, err
	}
	if i.cachedIsPaper != nil {
		return *i.cachedIsPaper, nil
	}
	return false, nil
}

func (i *IBKR) TestConnection(ctx context.Context) error {
	// Use GetBalance instead of just requestFlexReport — this also caches
	// the account ID (for paper detection) and breakdown (for GetBalanceByMarket).
	_, err := i.GetBalance(ctx)
	return err
}

// flexURL builds the IBKR Flex endpoint URL with properly escaped parameters
// (CONN-002). Using fmt.Sprintf with raw values risked corrupt requests if a
// token ever contained URL-reserved characters, and also meant the raw token
// flowed unescaped into preview/error strings.
func flexURL(base, token, query string) string {
	v := url.Values{"t": {token}, "q": {query}, "v": {"3"}}
	return base + "?" + v.Encode()
}

func (i *IBKR) requestFlexReport(ctx context.Context) (string, error) {
	u := flexURL(ibkrFlexURL, i.token, i.queryID)

	req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
	if err != nil {
		return "", err
	}

	resp, err := i.client.Do(req)
	if err != nil {
		return "", i.transportErr(err)
	}

	// CONN-AUDIT-001: bounded read with the higher IBKR Flex cap (statements
	// for long-history accounts can legitimately be tens of MiB).
	body, err := ReadCappedBody(resp.Body, IBKRFlexMaxResponseBytes)
	if err != nil {
		return "", err
	}

	if len(body) == 0 {
		return "", fmt.Errorf("flex SendRequest returned empty body (HTTP %d) — likely rate-limited or network blip", resp.StatusCode)
	}

	// Parse XML response
	var result struct {
		XMLName       xml.Name `xml:"FlexStatementResponse"`
		Status        string   `xml:"Status"`
		ReferenceCode string   `xml:"ReferenceCode"`
		ErrorCode     string   `xml:"ErrorCode"`
		ErrorMessage  string   `xml:"ErrorMessage"`
	}

	if err := xml.Unmarshal(body, &result); err != nil {
		// Log the raw body (truncated) so we can diagnose unexpected IBKR responses.
		// Scrub the token first in case IBKR echoed the request URL back (CONN-002).
		snippet := scrubSecret(string(body), i.token)
		if len(snippet) > 300 {
			snippet = snippet[:300] + "..."
		}
		return "", fmt.Errorf("parse flex response (body=%q): %w", snippet, err)
	}

	if result.Status != "Success" {
		base := fmt.Errorf("flex request failed: %s - %s", result.ErrorCode, result.ErrorMessage)
		if result.ErrorCode == flexIPRestrictionCode {
			return "", fmt.Errorf("%w: %w", ErrIPRestricted, base)
		}
		if isTransientFlexErrorCode(result.ErrorCode) {
			return "", fmt.Errorf("%w: %w", ErrTransient, base)
		}
		return "", base
	}

	return result.ReferenceCode, nil
}

// transientFlexErrorCodes lists the IBKR Flex error codes worth retrying.
// Descriptions are IBKR's own, from the Flex Web Service v3 error table; an
// earlier version of this list paraphrased them from memory and got four
// wrong, which sent 1014 and 1011 into endless retries and told users their
// token was bad when a P/L figure was simply not computed yet.
//
//	1001 - Statement could not be generated at this time. Please try again shortly.
//	1004 - Statement is incomplete at this time. Please try again shortly.
//	1005 - Settlement data is not ready at this time. Please try again shortly.
//	1006 - FIFO P/L data is not ready at this time. Please try again shortly.
//	1007 - MTM P/L data is not ready at this time. Please try again shortly.
//	1008 - MTM and FIFO P/L data is not ready at this time. Please try again shortly.
//	1009 - The server is under heavy load. Statement could not be generated at this time.
//	1019 - Statement generation in progress. Please try again shortly.
//	1021 - Statement could not be retrieved at this time. Please try again shortly.
//
// 1018 (too many requests: one per second, ten per minute per token) is
// transient too, but it is paced by claimFlexToken rather than retried blind.
//
// Deliberately absent, because the account or its configuration must change
// first: 1003 (statement not available), 1010 (legacy query unsupported),
// 1011 (service account inactive), 1012 (token expired), 1014 (query invalid),
// 1015 (token invalid), 1016 (account invalid), 1017 (reference code invalid),
// 1020 (invalid request). 1013 is an IP restriction and is neither — it is
// classified as such upstream so the holder is not sent to regenerate a token
// that was never the problem.
var transientFlexErrorCodes = map[string]struct{}{
	"1001": {},
	"1004": {},
	"1005": {},
	"1006": {},
	"1007": {},
	"1008": {},
	"1009": {},
	"1018": {},
	"1019": {},
	"1021": {},
}

// flexIPRestrictionCode is IBKR's "IP restriction": the token is valid and the
// source address is not allowed. Routing it through the credential path sends
// the holder to replace a working token, and the replacement fails the same
// way.
const flexIPRestrictionCode = "1013"

func isTransientFlexErrorCode(code string) bool {
	_, ok := transientFlexErrorCodes[code]
	return ok
}

func (i *IBKR) getFlexReport(ctx context.Context, refCode string) ([]byte, error) {
	u := flexURL(ibkrFlexGetURL, i.token, refCode)

	// Poll with exponential backoff. Small Flex reports (LastBusinessWeek)
	// are typically ready in 5-10s; 30-day / YTD reports on busy accounts
	// can take 1-3 minutes. Total budget here: ~4 minutes.
	delays := []time.Duration{
		3 * time.Second, 5 * time.Second, 5 * time.Second, 10 * time.Second, 10 * time.Second,
		15 * time.Second, 15 * time.Second, 20 * time.Second, 30 * time.Second, 30 * time.Second,
		30 * time.Second, 30 * time.Second, 30 * time.Second, 30 * time.Second,
	}
	for _, sleep := range delays {
		req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
		if err != nil {
			return nil, err
		}

		resp, err := i.client.Do(req)
		if err != nil {
			return nil, i.transportErr(err)
		}

		// CONN-AUDIT-001: Flex statement download — use the higher cap.
		body, err := ReadCappedBody(resp.Body, IBKRFlexMaxResponseBytes)
		if err != nil {
			return nil, err
		}

		// IBKR returns two root elements:
		//   - <FlexStatementResponse> → still processing OR hard error
		//   - <FlexQueryResponse> → the actual report is ready
		// Substring matching on "1019" was wrong because numeric values inside
		// a real report can trip it. The root-element distinction is exact.
		trimmed := strings.TrimSpace(string(body))
		if strings.HasPrefix(trimmed, "<FlexQueryResponse") {
			return body, nil
		}
		if strings.HasPrefix(trimmed, "<FlexStatementResponse") {
			// Error if Status="Error", otherwise keep polling.
			if strings.Contains(trimmed, "<Status>Error</Status>") {
				// CONN-002: scrub the token in case IBKR echoed our request URL.
				return nil, fmt.Errorf("flex report error: %s", preview(scrubSecret(trimmed, i.token), 300))
			}
			// CONN-005: ctx-aware wait so a cancelled sync propagates
			// immediately instead of blocking up to 30 s per tick.
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(sleep):
			}
			continue
		}
		// Unknown response shape — surface it (token scrubbed).
		return nil, fmt.Errorf("unexpected flex response: %s", preview(scrubSecret(trimmed, i.token), 300))
	}

	return nil, fmt.Errorf("flex report timeout after %d attempts", len(delays))
}

// preview returns the first n chars of s with an ellipsis when truncated.
// Used to include a snippet of unexpected IBKR responses in error messages.
func preview(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// transportErr rebuilds a *url.Error so the Flex token flexURL puts in the
// query string cannot ride out on the message (SEC-11). http.Client.Do prints
// the full URL, and net/http's own stripPassword only removes userinfo — the
// chain is dropped on purpose so no errors.As can recover the raw URL.
func (i *IBKR) transportErr(err error) error {
	if err == nil {
		return nil
	}
	msg := scrubSecret(err.Error(), i.token)
	if esc := url.QueryEscape(i.token); esc != i.token {
		msg = scrubSecret(msg, esc)
	}
	return errors.New(msg)
}

// scrubSecret replaces every literal occurrence of secret in s with "***"
// (CONN-002). Used before embedding third-party response text in an error
// message so an echoed request URL cannot leak the token to logs.
func scrubSecret(s, secret string) string {
	if secret == "" {
		return s
	}
	return strings.ReplaceAll(s, secret, "***")
}

func (i *IBKR) GetBalance(ctx context.Context) (*Balance, error) {
	report, err := i.fetchFlexReport(ctx)
	if err != nil {
		return nil, err
	}

	if err != nil {
		return nil, err
	}

	return i.parseBalanceFromReport(report)
}

func (i *IBKR) parseBalanceFromReport(report []byte) (*Balance, error) {
	var flex struct {
		XMLName        xml.Name `xml:"FlexQueryResponse"`
		FlexStatements struct {
			FlexStatement struct {
				AccountID           string `xml:"accountId,attr"`
				EquitySummaryInBase struct {
					EquitySummaryByReportDateInBase []struct {
						ReportDate           string `xml:"reportDate,attr"`
						Currency             string `xml:"currency,attr"`
						Total                string `xml:"total,attr"`
						Cash                 string `xml:"cash,attr"`
						Stock                string `xml:"stock,attr"`
						Options              string `xml:"options,attr"`
						Commodities          string `xml:"commodities,attr"`
						UnrealizedPnL        string `xml:"unrealizedPnL,attr"`
						CfdUnrealizedPl      string `xml:"cfdUnrealizedPl,attr"`
						ForexCfdUnrealizedPl string `xml:"forexCfdUnrealizedPl,attr"`
					} `xml:"EquitySummaryByReportDateInBase"`
				} `xml:"EquitySummaryInBase"`
				OpenPositions struct {
					OpenPosition []struct {
						FifoPnlUnrealized string `xml:"fifoPnlUnrealized,attr"`
					} `xml:"OpenPosition"`
				} `xml:"OpenPositions"`
			} `xml:"FlexStatement"`
		} `xml:"FlexStatements"`
	}

	if err := xml.Unmarshal(report, &flex); err != nil {
		return nil, fmt.Errorf("parse flex report: %w", err)
	}

	summaries := flex.FlexStatements.FlexStatement.EquitySummaryInBase.EquitySummaryByReportDateInBase
	i.cachedBalanceAsOf = time.Time{}
	if len(summaries) == 0 {
		return &Balance{Currency: "USD"}, nil
	}

	summary := summaries[len(summaries)-1] // Latest
	// Freshness: record the statement date of the figure we are about to
	// return. "Latest" here means latest IN THE STATEMENT — at midnight UTC
	// that is typically two days old, and the sync layer must know it
	// (BalanceFreshnessProvider) before stamping the value with today's date.
	if d, err := time.Parse("20060102", summary.ReportDate); err == nil {
		i.cachedBalanceAsOf = d
	}
	// Flex reports "in base currency" — the account's denomination, not always USD.
	currency := summary.Currency
	if currency == "" {
		currency = "USD"
	}
	total, _ := strconv.ParseFloat(summary.Total, 64)
	cash, _ := strconv.ParseFloat(summary.Cash, 64)
	unrealized, _ := strconv.ParseFloat(summary.UnrealizedPnL, 64)

	// IBKR EquitySummary only has cfd/forexCfd unrealized fields — not stocks/futures.
	// Sum fifoPnlUnrealized from OpenPositions for the complete picture.
	if unrealized == 0 {
		for _, pos := range flex.FlexStatements.FlexStatement.OpenPositions.OpenPosition {
			pnl, _ := strconv.ParseFloat(pos.FifoPnlUnrealized, 64)
			unrealized += pnl
		}
	}
	// Fallback to CFD fields if no open positions in the report
	if unrealized == 0 {
		cfdUnreal, _ := strconv.ParseFloat(summary.CfdUnrealizedPl, 64)
		fxCfdUnreal, _ := strconv.ParseFloat(summary.ForexCfdUnrealizedPl, 64)
		unrealized = cfdUnreal + fxCfdUnreal
	}
	stockVal, _ := strconv.ParseFloat(summary.Stock, 64)
	optionsVal, _ := strconv.ParseFloat(summary.Options, 64)
	commoditiesVal, _ := strconv.ParseFloat(summary.Commodities, 64)

	// Cache paper detection from account ID (avoids extra Flex call for DetectIsPaper)
	accountID := strings.ToUpper(strings.TrimSpace(flex.FlexStatements.FlexStatement.AccountID))
	if accountID != "" {
		isPaper := strings.HasPrefix(accountID, "DU") || strings.HasPrefix(accountID, "DF")
		i.cachedIsPaper = &isPaper
	}

	// Cache breakdown for GetBalanceByMarket (avoids 2nd Flex API call)
	i.cachedBreakdown = nil
	if stockVal != 0 {
		i.cachedBreakdown = append(i.cachedBreakdown, &MarketBalance{
			MarketType: MarketStocks, Equity: stockVal, AvailableMargin: cash,
		})
	}
	if optionsVal != 0 {
		i.cachedBreakdown = append(i.cachedBreakdown, &MarketBalance{
			MarketType: MarketOptions, Equity: optionsVal,
		})
	}
	if commoditiesVal != 0 {
		i.cachedBreakdown = append(i.cachedBreakdown, &MarketBalance{
			MarketType: MarketFutures, Equity: commoditiesVal,
		})
	}

	return &Balance{
		Available:     cash,
		Equity:        total,
		UnrealizedPnL: unrealized,
		Currency:      currency,
	}, nil
}

// BalanceAsOf implements BalanceFreshnessProvider: the reportDate of the
// summary the last GetBalance call returned. Zero when GetBalance has not run
// or the statement carried no parsable date (fail open).
func (i *IBKR) BalanceAsOf() time.Time {
	return i.cachedBalanceAsOf
}

// GetBalanceByMarket returns per-asset-class equity breakdown from IBKR Flex.
// GetBalanceByMarket returns cached breakdown from the last GetBalance call.
// No additional Flex API call needed (avoids IBKR rate limit error 1018).
func (i *IBKR) GetBalanceByMarket(_ context.Context) ([]*MarketBalance, error) {
	return i.cachedBreakdown, nil
}

func (i *IBKR) GetPositions(ctx context.Context) ([]*Position, error) {
	report, err := i.fetchFlexReport(ctx)
	if err != nil {
		return nil, err
	}

	if err != nil {
		return nil, err
	}

	return i.parsePositionsFromReport(report)
}

func (i *IBKR) parsePositionsFromReport(report []byte) ([]*Position, error) {
	var flex struct {
		XMLName        xml.Name `xml:"FlexQueryResponse"`
		FlexStatements struct {
			FlexStatement struct {
				OpenPositions struct {
					OpenPosition []struct {
						Symbol            string `xml:"symbol,attr"`
						Position          string `xml:"position,attr"`
						MarkPrice         string `xml:"markPrice,attr"`
						CostBasisMoney    string `xml:"costBasisMoney,attr"`
						FifoPnlUnrealized string `xml:"fifoPnlUnrealized,attr"`
						AssetCategory     string `xml:"assetCategory,attr"`
					} `xml:"OpenPosition"`
				} `xml:"OpenPositions"`
			} `xml:"FlexStatement"`
		} `xml:"FlexStatements"`
	}

	if err := xml.Unmarshal(report, &flex); err != nil {
		return nil, fmt.Errorf("parse flex positions: %w", err)
	}

	var positions []*Position
	for _, p := range flex.FlexStatements.FlexStatement.OpenPositions.OpenPosition {
		size, _ := strconv.ParseFloat(p.Position, 64)
		if size == 0 {
			continue
		}

		mark, _ := strconv.ParseFloat(p.MarkPrice, 64)
		costBasis, _ := strconv.ParseFloat(p.CostBasisMoney, 64)
		unrealized, _ := strconv.ParseFloat(p.FifoPnlUnrealized, 64)

		side := "long"
		if size < 0 {
			side = "short"
			size = -size
		}

		entryPrice := 0.0
		if size > 0 {
			entryPrice = costBasis / size
		}

		marketType := MarketStocks
		switch p.AssetCategory {
		case "FUT":
			marketType = MarketFutures
		case "OPT":
			marketType = MarketOptions
		case "CFD":
			marketType = MarketCFD
		case "CASH":
			marketType = MarketForex
		}

		positions = append(positions, &Position{
			Symbol:        p.Symbol,
			Side:          side,
			Size:          size,
			EntryPrice:    entryPrice,
			MarkPrice:     mark,
			UnrealizedPnL: unrealized,
			MarketType:    marketType,
		})
	}

	return positions, nil
}

// GetCashflows returns deposits and withdrawals since the given date.
// Uses IBKR Flex CashTransactions.
func (i *IBKR) GetCashflows(ctx context.Context, since time.Time) ([]*Cashflow, error) {
	report, err := i.fetchFlexReport(ctx)
	if err != nil {
		return nil, err
	}

	if err != nil {
		return nil, err
	}

	return i.parseCashflowsFromReport(report, since)
}

// GetRawCashflowEntries returns every CashTransaction and every Transfer the
// statement carries, the ones GetCashflows drops included. A position
// transferred in from another broker moves no cash, so the filtered view
// cannot tell it from a trading gain: the market value lands in equity and
// scores as performance. This is the view that separates the two.
func (i *IBKR) GetRawCashflowEntries(ctx context.Context, since time.Time) ([]RawBalanceOp, error) {
	report, err := i.fetchFlexReport(ctx)
	if err != nil {
		return nil, err
	}
	return parseRawLedgerFromReport(report, since)
}

// GetRawStatement returns the Flex XML as IBKR sent it. The token is scrubbed
// in case it was echoed back (CONN-002); nothing else is filtered, because a
// filter is exactly what this is for.
func (i *IBKR) GetRawStatement(ctx context.Context) ([]byte, string, error) {
	report, err := i.fetchFlexReport(ctx)
	if err != nil {
		return nil, "", err
	}
	return []byte(scrubSecret(string(report), i.token)), "application/xml", nil
}

func parseRawLedgerFromReport(report []byte, since time.Time) ([]RawBalanceOp, error) {
	var flex struct {
		XMLName        xml.Name `xml:"FlexQueryResponse"`
		FlexStatements struct {
			FlexStatement struct {
				CashTransactions struct {
					CashTransaction []struct {
						Type     string `xml:"type,attr"`
						Amount   string `xml:"amount,attr"`
						Currency string `xml:"currency,attr"`
						Symbol   string `xml:"symbol,attr"`
						DateTime string `xml:"dateTime,attr"`
					} `xml:"CashTransaction"`
				} `xml:"CashTransactions"`
				Transfers struct {
					Transfer []struct {
						Type           string `xml:"type,attr"`
						Direction      string `xml:"direction,attr"`
						CashTransfer   string `xml:"cashTransfer,attr"`
						PositionAmount string `xml:"positionAmount,attr"`
						Quantity       string `xml:"quantity,attr"`
						AssetCategory  string `xml:"assetCategory,attr"`
						Symbol         string `xml:"symbol,attr"`
						Currency       string `xml:"currency,attr"`
						DateTime       string `xml:"dateTime,attr"`
						Date           string `xml:"date,attr"`
					} `xml:"Transfer"`
				} `xml:"Transfers"`
			} `xml:"FlexStatement"`
		} `xml:"FlexStatements"`
	}

	if err := xml.Unmarshal(report, &flex); err != nil {
		return nil, fmt.Errorf("parse flex raw ledger: %w", err)
	}

	stmt := flex.FlexStatements.FlexStatement
	ops := make([]RawBalanceOp, 0, len(stmt.CashTransactions.CashTransaction)+len(stmt.Transfers.Transfer))

	for _, tx := range stmt.CashTransactions.CashTransaction {
		ts, ok := parseFlexTimestamp(tx.DateTime)
		if !ok || ts.Before(since) {
			continue
		}
		amount, _ := strconv.ParseFloat(tx.Amount, 64)
		ops = append(ops, RawBalanceOp{
			Label:     "cash:" + tx.Type,
			Delta:     amount,
			Currency:  tx.Currency,
			Symbol:    tx.Symbol,
			Timestamp: ts,
		})
	}

	for _, tr := range stmt.Transfers.Transfer {
		raw := tr.DateTime
		if raw == "" {
			raw = tr.Date
		}
		ts, ok := parseFlexTimestamp(raw)
		if !ok || ts.Before(since) {
			continue
		}
		cash, _ := strconv.ParseFloat(tr.CashTransfer, 64)
		posAmount, _ := strconv.ParseFloat(tr.PositionAmount, 64)
		label := "transfer:" + tr.Type
		if tr.Direction != "" {
			label += "/" + tr.Direction
		}
		if tr.AssetCategory != "" {
			label += "/" + tr.AssetCategory
		}
		ops = append(ops, RawBalanceOp{
			Label:         label,
			Delta:         cash,
			PositionValue: posAmount,
			Currency:      tr.Currency,
			Symbol:        tr.Symbol,
			Timestamp:     ts,
		})
	}

	sort.Slice(ops, func(a, b int) bool { return ops[a].Timestamp.Before(ops[b].Timestamp) })
	return ops, nil
}

// flexCapitalTypes are the CashTransaction types that move money across the
// account boundary. Both spellings of the umbrella type are real: current
// Flex statements write "Deposits & Withdrawals" (ampersand), older ones
// "Deposits/Withdrawals" (slash). This parser matched only the slash, so an
// ordinary wire deposit under the modern spelling was dropped without a
// trace — the equity stepped up with no recorded inflow, and the step read
// as a trading gain (a fresh account showed a +9,758 "gain" that was its
// own funding, 2026-08-25).
var flexCapitalTypes = map[string]bool{
	"Deposits":               true,
	"Withdrawals":            true,
	"Deposits/Withdrawals":   true,
	"Deposits & Withdrawals": true,
}

// flexPerformanceTypes are CashTransaction types that are returns or costs of
// holding the account — income and charges, not capital. They are excluded
// from cashflows ON PURPOSE: booking a dividend as a deposit would erase the
// very return it represents. Named so an absent type is a decision, not an
// accident. Vocabulary per IBKR Flex statements (CashAction).
var flexPerformanceTypes = map[string]bool{
	"Dividends":                    true,
	"Payment In Lieu Of Dividends": true,
	"Withholding Tax":              true,
	"Broker Interest Paid":         true,
	"Broker Interest Received":     true,
	"Bond Interest Paid":           true,
	"Bond Interest Received":       true,
	"Other Fees":                   true,
	"Commission Adjustments":       true,
	"Advisor Fees":                 true,
}

// parseFlexTimestamp accepts the two timestamp shapes Flex emits.
func parseFlexTimestamp(v string) (time.Time, bool) {
	if ts, err := time.Parse("20060102;150405", v); err == nil {
		return ts, true
	}
	if ts, err := time.Parse("20060102", v); err == nil {
		return ts, true
	}
	return time.Time{}, false
}

// CapabilityWarnings implements CapabilityWarner with the statement-shape
// gaps found by the last Flex parse.
func (i *IBKR) CapabilityWarnings() []string {
	return i.capabilityWarnings
}

func (i *IBKR) parseCashflowsFromReport(report []byte, since time.Time) ([]*Cashflow, error) {
	var flex struct {
		XMLName        xml.Name `xml:"FlexQueryResponse"`
		FlexStatements struct {
			FlexStatement struct {
				CashTransactions struct {
					CashTransaction []struct {
						Type     string `xml:"type,attr"`
						Amount   string `xml:"amount,attr"`
						Currency string `xml:"currency,attr"`
						DateTime string `xml:"dateTime,attr"`
					} `xml:"CashTransaction"`
				} `xml:"CashTransactions"`
				Transfers struct {
					Transfer []struct {
						Type         string `xml:"type,attr"`
						Direction    string `xml:"direction,attr"`
						CashTransfer string `xml:"cashTransfer,attr"`
						Currency     string `xml:"currency,attr"`
						DateTime     string `xml:"dateTime,attr"`
						Date         string `xml:"date,attr"`
					} `xml:"Transfer"`
				} `xml:"Transfers"`
			} `xml:"FlexStatement"`
		} `xml:"FlexStatements"`
	}

	if err := xml.Unmarshal(report, &flex); err != nil {
		return nil, fmt.Errorf("parse flex cashflows: %w", err)
	}

	// Statement-shape diagnostic: types and counts ONLY, never amounts. A
	// type in neither list above is money whose meaning we cannot state —
	// dropping it silently is what turned a funding wire into a phantom
	// gain, so it must at least leave a mark someone can find.
	unknownTypes := map[string]int{}

	var cashflows []*Cashflow
	for _, tx := range flex.FlexStatements.FlexStatement.CashTransactions.CashTransaction {
		ts, ok := parseFlexTimestamp(tx.DateTime)
		if !ok || ts.Before(since) {
			continue
		}
		amount, _ := strconv.ParseFloat(tx.Amount, 64)
		if amount == 0 {
			continue
		}
		if !flexCapitalTypes[tx.Type] {
			if !flexPerformanceTypes[tx.Type] {
				unknownTypes[tx.Type]++
			}
			continue
		}
		// The singular spellings are directional by name; the umbrella
		// spellings sign by amount. Either way the sign convention out of
		// here is positive=deposit, negative=withdrawal.
		if tx.Type == "Deposits" && amount < 0 {
			amount = -amount
		}
		if tx.Type == "Withdrawals" && amount > 0 {
			amount = -amount
		}
		cashflows = append(cashflows, &Cashflow{
			Amount:    amount,
			Currency:  tx.Currency,
			Timestamp: ts,
		})
	}

	// Transfers are the other funding channel: cash moved between accounts
	// (INTERNAL, ACATS, ATON…) never appears under CashTransactions, so a
	// user who funds this account from another one crosses the perimeter
	// invisibly without this section. Only the cash component counts — a
	// pure position transfer (cashTransfer 0) changes holdings, not cash,
	// and its market value shows up through equity like any position.
	for _, tr := range flex.FlexStatements.FlexStatement.Transfers.Transfer {
		raw := tr.DateTime
		if raw == "" {
			raw = tr.Date
		}
		ts, ok := parseFlexTimestamp(raw)
		if !ok || ts.Before(since) {
			continue
		}
		cash, _ := strconv.ParseFloat(tr.CashTransfer, 64)
		if cash == 0 {
			continue
		}
		switch tr.Direction {
		case "IN":
			if cash < 0 {
				cash = -cash
			}
		case "OUT":
			if cash > 0 {
				cash = -cash
			}
		default:
			unknownTypes["Transfer:"+tr.Direction]++
			continue
		}
		cashflows = append(cashflows, &Cashflow{
			Amount:    cash,
			Currency:  tr.Currency,
			Timestamp: ts,
		})
	}

	i.capabilityWarnings = nil
	for typ, n := range unknownTypes {
		i.capabilityWarnings = append(i.capabilityWarnings,
			fmt.Sprintf("ibkr_unclassified_cash_type:%s(x%d)", typ, n))
	}
	sort.Strings(i.capabilityWarnings)

	return cashflows, nil
}

// GetHistoricalSnapshots returns daily equity snapshots from IBKR Flex (up to 365 days).
// Used for backfill on first sync.
func (i *IBKR) GetHistoricalSnapshots(ctx context.Context, since time.Time) ([]*HistoricalSnapshot, error) {
	report, err := i.fetchFlexReport(ctx)
	if err != nil {
		return nil, err
	}
	return i.parseHistoricalSnapshotsFromReport(report, since)
}

// parseHistoricalSnapshotsFromReport extracts daily equity snapshots from a
// raw Flex XML payload. Split out from GetHistoricalSnapshots so callers can
// replay a captured XML offline (fixture-based tests, debug tools) without
// triggering an IBKR Flex generation cooldown.
// flexDayTrades accumulates one day's trade activity from the Flex report,
// split by direction so historical snapshots feed the long/short widgets.
type flexDayTrades struct {
	count       int
	volume      float64
	fees        float64
	longTrades  int
	shortTrades int
	longVolume  float64
	shortVolume float64
}

func aggregateFlexTradesByDate(trades []*Trade) map[string]flexDayTrades {
	byDate := make(map[string]flexDayTrades)
	for _, t := range trades {
		dateKey := t.Timestamp.Format("20060102")
		entry := byDate[dateKey]
		entry.count++
		notional := t.Price * t.Quantity
		entry.volume += notional
		entry.fees += t.Fee
		if t.Side == "sell" {
			entry.shortTrades++
			entry.shortVolume += notional
		} else {
			entry.longTrades++
			entry.longVolume += notional
		}
		byDate[dateKey] = entry
	}
	return byDate
}

func (i *IBKR) parseHistoricalSnapshotsFromReport(report []byte, since time.Time) ([]*HistoricalSnapshot, error) {
	// Parse all daily equity summaries with per-asset breakdown
	var flex struct {
		XMLName        xml.Name `xml:"FlexQueryResponse"`
		FlexStatements struct {
			FlexStatement struct {
				EquitySummaryInBase struct {
					EquitySummaryByReportDateInBase []struct {
						ReportDate    string `xml:"reportDate,attr"`
						Total         string `xml:"total,attr"`
						Cash          string `xml:"cash,attr"`
						Stock         string `xml:"stock,attr"`
						Options       string `xml:"options,attr"`
						Commodities   string `xml:"commodities,attr"`
						UnrealizedPnL string `xml:"unrealizedPnL,attr"`
					} `xml:"EquitySummaryByReportDateInBase"`
				} `xml:"EquitySummaryInBase"`
			} `xml:"FlexStatement"`
		} `xml:"FlexStatements"`
	}

	if err := xml.Unmarshal(report, &flex); err != nil {
		return nil, fmt.Errorf("parse flex historical: %w", err)
	}

	// Parse cashflows grouped by date for deposit/withdrawal assignment
	cashflows, _ := i.parseCashflowsFromReport(report, since)
	cashflowsByDate := make(map[string]struct{ deposits, withdrawals float64 })
	for _, cf := range cashflows {
		dateKey := cf.Timestamp.Format("20060102")
		entry := cashflowsByDate[dateKey]
		if cf.Amount > 0 {
			entry.deposits += cf.Amount
		} else {
			entry.withdrawals += -cf.Amount
		}
		cashflowsByDate[dateKey] = entry
	}

	// Parse trades grouped by date so daily snapshots carry trade count,
	// notional volume and commission fees. Reuses parseTradesFromReport so
	// the per-trade parsing logic stays in one place. Without this the
	// analytics dashboard's per-day volume / trades-per-day widgets show 0
	// for IBKR users (only the per-day equity walks).
	trades, _ := i.parseTradesFromReport(report, since, time.Now().Add(24*time.Hour))
	tradesByDate := aggregateFlexTradesByDate(trades)

	var snapshots []*HistoricalSnapshot
	for _, s := range flex.FlexStatements.FlexStatement.EquitySummaryInBase.EquitySummaryByReportDateInBase {
		date, err := time.Parse("20060102", s.ReportDate)
		if err != nil {
			continue
		}
		if date.Before(since) {
			continue
		}

		total, _ := strconv.ParseFloat(s.Total, 64)
		if total == 0 {
			continue // Skip zero-equity days
		}
		unrealized, _ := strconv.ParseFloat(s.UnrealizedPnL, 64)
		stockVal, _ := strconv.ParseFloat(s.Stock, 64)
		optionsVal, _ := strconv.ParseFloat(s.Options, 64)
		commoditiesVal, _ := strconv.ParseFloat(s.Commodities, 64)
		cashVal, _ := strconv.ParseFloat(s.Cash, 64)

		// Build per-asset breakdown (TS parity: getHistoricalSummaries)
		breakdown := make(map[string]*MarketBalance)
		if stockVal != 0 {
			breakdown[MarketStocks] = &MarketBalance{
				MarketType:      MarketStocks,
				Equity:          stockVal,
				AvailableMargin: cashVal,
			}
		}
		if optionsVal != 0 {
			breakdown[MarketOptions] = &MarketBalance{
				MarketType: MarketOptions,
				Equity:     optionsVal,
			}
		}
		if commoditiesVal != 0 {
			breakdown[MarketFutures] = &MarketBalance{
				MarketType: MarketFutures,
				Equity:     commoditiesVal,
			}
		}

		cf := cashflowsByDate[s.ReportDate]
		tr := tradesByDate[s.ReportDate]
		snapshots = append(snapshots, &HistoricalSnapshot{
			Date:            time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, time.UTC),
			TotalEquity:     total,
			RealizedBalance: total - unrealized,
			Deposits:        cf.deposits,
			Withdrawals:     cf.withdrawals,
			TotalTrades:     tr.count,
			TotalVolume:     tr.volume,
			TotalFees:       tr.fees,
			LongTrades:      tr.longTrades,
			ShortTrades:     tr.shortTrades,
			LongVolume:      tr.longVolume,
			ShortVolume:     tr.shortVolume,
			Breakdown:       breakdown,
		})
	}

	return snapshots, nil
}

func (i *IBKR) GetTrades(ctx context.Context, start, end time.Time) ([]*Trade, error) {
	report, err := i.fetchFlexReport(ctx)
	if err != nil {
		return nil, err
	}

	if err != nil {
		return nil, err
	}

	return i.parseTradesFromReport(report, start, end)
}

func (i *IBKR) parseTradesFromReport(report []byte, start, end time.Time) ([]*Trade, error) {
	var flex struct {
		XMLName        xml.Name `xml:"FlexQueryResponse"`
		FlexStatements struct {
			FlexStatement struct {
				Trades struct {
					Trade []struct {
						TradeID         string `xml:"tradeID,attr"`
						Symbol          string `xml:"symbol,attr"`
						BuySell         string `xml:"buySell,attr"`
						TradePrice      string `xml:"tradePrice,attr"`
						Quantity        string `xml:"quantity,attr"`
						IbCommission    string `xml:"ibCommission,attr"`
						Currency        string `xml:"currency,attr"`
						DateTime        string `xml:"dateTime,attr"`
						AssetCategory   string `xml:"assetCategory,attr"`
						FifoPnlRealized string `xml:"fifoPnlRealized,attr"`
					} `xml:"Trade"`
				} `xml:"Trades"`
			} `xml:"FlexStatement"`
		} `xml:"FlexStatements"`
	}

	if err := xml.Unmarshal(report, &flex); err != nil {
		return nil, fmt.Errorf("parse flex trades: %w", err)
	}

	var trades []*Trade
	for _, t := range flex.FlexStatements.FlexStatement.Trades.Trade {
		// Parse datetime (format: YYYYMMDD;HHMMSS)
		ts, err := time.Parse("20060102;150405", t.DateTime)
		if err != nil {
			continue
		}

		// Filter by date range
		if ts.Before(start) || ts.After(end) {
			continue
		}

		price, _ := strconv.ParseFloat(t.TradePrice, 64)
		qty, _ := strconv.ParseFloat(t.Quantity, 64)
		if qty < 0 {
			qty = -qty
		}
		fee, _ := strconv.ParseFloat(t.IbCommission, 64)
		if fee < 0 {
			fee = -fee
		}
		pnl, _ := strconv.ParseFloat(t.FifoPnlRealized, 64)

		side := "buy"
		if t.BuySell == "SELL" {
			side = "sell"
		}

		marketType := MarketStocks
		switch t.AssetCategory {
		case "FUT":
			marketType = MarketFutures
		case "OPT":
			marketType = MarketOptions
		case "CFD":
			marketType = MarketCFD
		case "CASH":
			marketType = MarketForex
		}

		trades = append(trades, &Trade{
			ID:          t.TradeID,
			Symbol:      t.Symbol,
			Side:        side,
			Price:       price,
			Quantity:    qty,
			Fee:         fee,
			FeeCurrency: t.Currency,
			RealizedPnL: pnl,
			Timestamp:   ts,
			MarketType:  marketType,
		})
	}

	return trades, nil
}
