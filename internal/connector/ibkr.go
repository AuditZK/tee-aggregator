package connector

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
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
	// Without it, two parallel syncs for users sharing a Flex token both see a
	// cache miss and race into IBKR, triggering rate limit 1018.
	flexSingleflight singleflight.Group
)

const flexReportCacheTTL = 5 * time.Minute

// IBKR implements Connector for Interactive Brokers via Flex Query
type IBKR struct {
	token   string // Flex Web Service Token
	queryID string // Flex Query ID
	client  *http.Client

	// Cached from last GetBalance call (avoids extra Flex requests)
	cachedBreakdown []*MarketBalance
	cachedIsPaper   *bool
}

// NewIBKR creates a new IBKR connector
func NewIBKR(creds *Credentials) *IBKR {
	return &IBKR{
		token:   creds.APIKey,
		queryID: creds.APISecret,
		client:  &http.Client{Timeout: 120 * time.Second}, // Flex can be slow
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
		flexReportCacheMu.Unlock()
		return report, nil
	})
	if err != nil {
		return nil, err
	}
	return v.([]byte), nil
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

func (i *IBKR) requestFlexReport(ctx context.Context) (string, error) {
	url := fmt.Sprintf("%s?t=%s&q=%s&v=3", ibkrFlexURL, i.token, i.queryID)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return "", err
	}

	resp, err := i.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
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
		return "", fmt.Errorf("parse flex response: %w", err)
	}

	if result.Status != "Success" {
		return "", fmt.Errorf("flex request failed: %s - %s", result.ErrorCode, result.ErrorMessage)
	}

	return result.ReferenceCode, nil
}

func (i *IBKR) getFlexReport(ctx context.Context, refCode string) ([]byte, error) {
	url := fmt.Sprintf("%s?t=%s&q=%s&v=3", ibkrFlexGetURL, i.token, refCode)

	// Poll with exponential backoff. Small Flex reports (LastBusinessWeek)
	// are typically ready in 5-10s; 30-day / YTD reports on busy accounts
	// can take 1-3 minutes. Total budget here: ~4 minutes.
	delays := []time.Duration{
		3 * time.Second, 5 * time.Second, 5 * time.Second, 10 * time.Second, 10 * time.Second,
		15 * time.Second, 15 * time.Second, 20 * time.Second, 30 * time.Second, 30 * time.Second,
		30 * time.Second, 30 * time.Second, 30 * time.Second, 30 * time.Second,
	}
	for _, sleep := range delays {
		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			return nil, err
		}

		resp, err := i.client.Do(req)
		if err != nil {
			return nil, err
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return nil, err
		}

		// Check if still processing
		if strings.Contains(string(body), "Statement generation in progress") {
			time.Sleep(sleep)
			continue
		}

		return body, nil
	}

	return nil, fmt.Errorf("flex report timeout after %d attempts", len(delays))
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
				AccountID       string `xml:"accountId,attr"`
				EquitySummaryInBase struct {
					EquitySummaryByReportDateInBase []struct {
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
	if len(summaries) == 0 {
		return &Balance{Currency: "USD"}, nil
	}

	summary := summaries[len(summaries)-1] // Latest
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
			} `xml:"FlexStatement"`
		} `xml:"FlexStatements"`
	}

	if err := xml.Unmarshal(report, &flex); err != nil {
		return nil, fmt.Errorf("parse flex cashflows: %w", err)
	}

	var cashflows []*Cashflow
	for _, tx := range flex.FlexStatements.FlexStatement.CashTransactions.CashTransaction {
		ts, err := time.Parse("20060102;150405", tx.DateTime)
		if err != nil {
			// Try date-only format
			ts, err = time.Parse("20060102", tx.DateTime)
			if err != nil {
				continue
			}
		}
		if ts.Before(since) {
			continue
		}

		amount, _ := strconv.ParseFloat(tx.Amount, 64)
		if amount == 0 {
			continue
		}

		isDeposit := tx.Type == "Deposits" || (tx.Type == "Deposits/Withdrawals" && amount > 0)
		isWithdrawal := tx.Type == "Withdrawals" || (tx.Type == "Deposits/Withdrawals" && amount < 0)

		if !isDeposit && !isWithdrawal {
			continue
		}

		cashflows = append(cashflows, &Cashflow{
			Amount:    amount, // positive=deposit, negative=withdrawal
			Currency:  tx.Currency,
			Timestamp: ts,
		})
	}

	return cashflows, nil
}

// GetHistoricalSnapshots returns daily equity snapshots from IBKR Flex (up to 365 days).
// Used for backfill on first sync.
func (i *IBKR) GetHistoricalSnapshots(ctx context.Context, since time.Time) ([]*HistoricalSnapshot, error) {
	report, err := i.fetchFlexReport(ctx)
	if err != nil {
		return nil, err
	}

	if err != nil {
		return nil, err
	}

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
		snapshots = append(snapshots, &HistoricalSnapshot{
			Date:            time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, time.UTC),
			TotalEquity:     total,
			RealizedBalance: total - unrealized,
			Deposits:        cf.deposits,
			Withdrawals:     cf.withdrawals,
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
