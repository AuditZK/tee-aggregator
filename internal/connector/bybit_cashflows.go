package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

// The tracked perimeter is the unified trading account. An on-chain deposit
// lands in the separate Funding account first, so the funding-to-unified
// transfer is the event where money enters the perimeter: every TRANSFER_IN /
// TRANSFER_OUT row crosses it and its counterpart — funding account,
// sub-account, another user — is never inside, so no leg-pairing pass exists.
// Every other type (TRADE, SETTLEMENT, liquidation, airdrop, interest) happens
// inside the perimeter and is performance, not capital.
//
// This is the history-rebuilder's exact classification; the two instruments
// must keep agreeing on shared days or the reconstruction gate reads their
// disagreement as a corrupted rebuild.
//
// /v5/account/transaction-log serves ~2 years but at most 7 days per query, so
// the sync window is split; adjacent windows touch at their boundary and the
// rows are deduped by id.

const (
	bybitLogPageLimit = 50
	bybitMaxLogPages  = 40
	// bybitLogWindow stays a day inside the documented 7-day maximum so clock
	// skew at a boundary cannot turn a legal range into retCode 10001.
	bybitLogWindow = 6 * 24 * time.Hour
	// bybitMaxLogWindows covers 48 days, comfortably past the sync layer's
	// 30-day activity lookback — a runaway guard, not a policy.
	bybitMaxLogWindows = 8
	bybitLogPagePace   = 120 * time.Millisecond
)

var bybitTransferTypes = map[string]bool{
	"TRANSFER_IN":  true,
	"TRANSFER_OUT": true,
}

type bybitLogRow struct {
	ID       string
	T        time.Time
	Coin     string
	Type     string
	CashFlow float64
}

func (b *Bybit) GetCashflows(ctx context.Context, since time.Time) ([]*Cashflow, error) {
	b.resetCashflowWarnings()

	now := time.Now().UTC()
	rows, err := b.fetchTransactionLog(ctx, since.UTC(), now)
	if err != nil {
		return nil, fmt.Errorf("fetch bybit transaction log: %w", err)
	}

	// One public all-tickers call prices every non-stable transfer coin, and is
	// skipped entirely when the window moved only stables.
	prices, priced := b.spotUSDPrices(ctx, bybitNonStableTransferCoins(rows))
	if !priced {
		b.noteCashflowWarning("bybit_spot_pricing_unavailable")
	}

	flows, warnings := bybitClassifyCashflows(rows, prices)
	for _, w := range warnings {
		b.noteCashflowWarning(w)
	}
	return flows, nil
}

// bybitClassifyCashflows is pure so the perimeter rule stays testable without
// IO. A transfer whose coin cannot be priced is skipped WITH a warning, never
// valued at zero silently: a missed deposit books as fabricated performance,
// which is the defect this file exists to close.
func bybitClassifyCashflows(rows []bybitLogRow, prices map[string]float64) ([]*Cashflow, []string) {
	warnings := map[string]bool{}
	var flows []*Cashflow

	for _, r := range rows {
		if !bybitTransferTypes[r.Type] || r.CashFlow == 0 {
			continue
		}
		usd, ok := bybitValueUSD(r.Coin, math.Abs(r.CashFlow), prices)
		if !ok {
			warnings["bybit_transfer_unpriced:"+r.Coin] = true
			continue
		}
		if r.CashFlow < 0 {
			usd = -usd
		}
		flows = append(flows, &Cashflow{Amount: usd, Currency: r.Coin, Timestamp: r.T})
	}

	keys := make([]string, 0, len(warnings))
	for k := range warnings {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return flows, keys
}

func bybitValueUSD(coin string, qty float64, prices map[string]float64) (float64, bool) {
	if IsStablecoinUSD(coin) {
		return qty, true
	}
	if usd := ValueSpotHoldingsUSD([]SpotHolding{{Asset: coin, Amount: qty}}, prices); usd > 0 {
		return usd, true
	}
	return 0, false
}

func bybitNonStableTransferCoins(rows []bybitLogRow) []string {
	seen := map[string]bool{}
	var coins []string
	for _, r := range rows {
		if !bybitTransferTypes[r.Type] || r.CashFlow == 0 || IsStablecoinUSD(r.Coin) || seen[r.Coin] {
			continue
		}
		seen[r.Coin] = true
		coins = append(coins, r.Coin)
	}
	return coins
}

// fetchTransactionLog walks [since, now] in windows the endpoint accepts. A
// window that fails aborts the whole call: a partial ledger under-reports a
// deposit, and an under-reported deposit is indistinguishable from profit once
// the snapshot is written.
func (b *Bybit) fetchTransactionLog(ctx context.Context, since, now time.Time) ([]bybitLogRow, error) {
	seen := map[string]bool{}
	var rows []bybitLogRow

	winStart := since
	for window := 0; winStart.Before(now); window++ {
		if window >= bybitMaxLogWindows {
			b.noteCashflowWarning("bybit_log_window_truncated")
			break
		}
		winEnd := winStart.Add(bybitLogWindow)
		if winEnd.After(now) {
			winEnd = now
		}
		batch, err := b.fetchLogWindow(ctx, winStart, winEnd, seen)
		if err != nil {
			return nil, err
		}
		rows = append(rows, batch...)
		winStart = winEnd
	}

	sort.Slice(rows, func(i, j int) bool {
		if rows[i].T.Equal(rows[j].T) {
			return rows[i].ID < rows[j].ID
		}
		return rows[i].T.Before(rows[j].T)
	})
	return rows, nil
}

func (b *Bybit) fetchLogWindow(ctx context.Context, winStart, winEnd time.Time, seen map[string]bool) ([]bybitLogRow, error) {
	var out []bybitLogRow
	cursor := ""

	for page := 0; page < bybitMaxLogPages; page++ {
		if page > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(bybitLogPagePace):
			}
		}

		params := fmt.Sprintf("accountType=UNIFIED&startTime=%d&endTime=%d&limit=%d",
			winStart.UnixMilli(), winEnd.UnixMilli(), bybitLogPageLimit)
		if cursor != "" {
			// Sent unescaped: the signature covers the query string exactly as
			// transmitted, and the venue returns the cursor already encoded.
			params += "&cursor=" + cursor
		}

		body, err := b.doRequest(ctx, "GET", "/v5/account/transaction-log", params)
		if err != nil {
			return nil, err
		}

		var resp struct {
			Result struct {
				NextPageCursor string `json:"nextPageCursor"`
				List           []struct {
					ID              string `json:"id"`
					Currency        string `json:"currency"`
					Type            string `json:"type"`
					CashFlow        string `json:"cashFlow"`
					CashBalance     string `json:"cashBalance"`
					TransactionTime string `json:"transactionTime"`
				} `json:"list"`
			} `json:"result"`
		}
		if err := json.Unmarshal(body, &resp); err != nil {
			return nil, fmt.Errorf("decode bybit transaction log: %w", err)
		}

		for _, row := range resp.Result.List {
			ms, perr := strconv.ParseInt(row.TransactionTime, 10, 64)
			if perr != nil {
				continue
			}
			key := row.ID
			if key == "" {
				key = row.TransactionTime + "|" + row.Currency + "|" + row.Type + "|" + row.CashBalance
			}
			if seen[key] {
				continue
			}
			seen[key] = true
			cashFlow, _ := strconv.ParseFloat(row.CashFlow, 64)
			out = append(out, bybitLogRow{
				ID:       key,
				T:        time.UnixMilli(ms).UTC(),
				Coin:     strings.ToUpper(row.Currency),
				Type:     strings.ToUpper(row.Type),
				CashFlow: cashFlow,
			})
		}

		cursor = resp.Result.NextPageCursor
		if cursor == "" || len(resp.Result.List) == 0 {
			break
		}
	}
	return out, nil
}

// spotUSDPrices returns a Binance-style symbol-to-price map for the shared
// valuation helper. ok=false means the call itself failed and non-stable
// valuation is unavailable this window.
func (b *Bybit) spotUSDPrices(ctx context.Context, coins []string) (map[string]float64, bool) {
	if len(coins) == 0 {
		return nil, true
	}

	body, err := b.doRequest(ctx, "GET", "/v5/market/tickers", "category=spot")
	if err != nil {
		return nil, false
	}

	var resp struct {
		Result struct {
			List []struct {
				Symbol    string `json:"symbol"`
				LastPrice string `json:"lastPrice"`
			} `json:"list"`
		} `json:"result"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, false
	}

	prices := make(map[string]float64, len(resp.Result.List))
	for _, t := range resp.Result.List {
		if p, perr := strconv.ParseFloat(t.LastPrice, 64); perr == nil && p > 0 {
			prices[strings.ToUpper(t.Symbol)] = p
		}
	}
	return prices, true
}

// CapabilityWarnings implements CapabilityWarner with markers from the LAST
// GetCashflows call — the non-atomic sync path fetches cashflows before it
// reads the warner, so they ride the same sync status as balance-scope gaps.
func (b *Bybit) CapabilityWarnings() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.cashflowWarnings...)
}

func (b *Bybit) resetCashflowWarnings() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.cashflowWarnings = nil
}

func (b *Bybit) noteCashflowWarning(w string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, have := range b.cashflowWarnings {
		if have == w {
			return
		}
	}
	b.cashflowWarnings = append(b.cashflowWarnings, w)
}
