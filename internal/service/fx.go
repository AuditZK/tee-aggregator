package service

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/trackrecord/enclave/internal/connector"
	"github.com/trackrecord/enclave/internal/repository"
	"go.uber.org/zap"
)

// Conversion of accounts a broker reports in another currency than USD.
//
// IBKR, cTrader, IG and MetaTrader read an account in its own base currency,
// and for a long time nothing converted it: a EUR account's equity travelled
// to the dashboard, the signed report and the "all" aggregate as EUR under a
// "$". Every balance is now multiplied by the USD rate of the day it was
// measured on, and every deposit or withdrawal by the rate of its own day,
// before anything is stored. A single rate for the whole history would not
// do: EUR/USD moves every day, and a track record held in EUR and shown in
// USD has to carry that move.
//
// Rates come from benchmark-service, the platform's only market-data source,
// like the benchmark series of the signed report. Only rates it calls final
// are used: a day whose own close is not published yet would convert one way
// tonight and another way tomorrow, and the reconstruction gate holds stored
// days to 0.1%.
//
// Trading activity (volume, fees) is left in the units the connector gives
// it: cTrader already reports USD notionals, IBKR reports each trade in its
// own currency, and a balance rate applied to either would be wrong.

// fxConvertible lists the currencies benchmark-service serves a daily USD
// rate for. A currency outside it, stablecoins included, is stored as the
// connector reports it: USDT and USDC are the crypto connectors' "dollar",
// and an exotic fiat with no rate is left as it was rather than blocked.
var fxConvertible = map[string]bool{
	"EUR": true, "GBP": true, "CHF": true, "JPY": true, "CAD": true, "AUD": true,
	"NZD": true, "HKD": true, "SGD": true, "SEK": true, "NOK": true, "DKK": true,
	"PLN": true, "CZK": true, "HUF": true, "ILS": true, "MXN": true, "ZAR": true,
	"CNY": true, "TRY": true, "INR": true, "KRW": true, "BRL": true,
}

// fiatToConvert returns the normalized currency and whether amounts in it
// are converted to USD.
func fiatToConvert(currency string) (string, bool) {
	c := strings.ToUpper(strings.TrimSpace(currency))
	return c, fxConvertible[c]
}

// trustsCashflowCurrency reports whether a connector's Cashflow.Currency is
// the real currency of the flow. IBKR's is (with BASE_SUMMARY for rows in
// the base currency); cTrader and MetaTrader label every flow "USD"
// whatever the account, so their flows are taken in the account currency.
func trustsCashflowCurrency(exchange string) bool {
	return strings.EqualFold(exchange, "ibkr")
}

// flowCurrency is the currency a flow's amount is in.
func flowCurrency(cf *connector.Cashflow, account string, trusted bool) string {
	if trusted {
		c := strings.ToUpper(strings.TrimSpace(cf.Currency))
		if len(c) == 3 && c != "BASE_SUMMARY" {
			return c
		}
	}
	return account
}

func dayKey(t time.Time) string { return t.UTC().Format("2006-01-02") }

func truncDay(t time.Time) time.Time {
	t = t.UTC()
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}

// fxRateSource gives final USD rates: USD per one unit of currency.
type fxRateSource interface {
	// FinalRatesToUSD returns, keyed YYYY-MM-DD, the final rate of every day
	// in [from, to] that has one. A day without one is absent.
	FinalRatesToUSD(ctx context.Context, currency string, from, to time.Time) (map[string]float64, error)
}

// fxRates is the lookup a conversion runs against, filled up front so the
// arithmetic below stays pure.
type fxRates map[string]map[string]float64 // currency -> day -> rate

func (r fxRates) rate(currency string, day time.Time) (float64, bool) {
	if currency == "USD" {
		return 1, true
	}
	v, ok := r[currency][dayKey(day)]
	return v, ok && v > 0
}

// loadFXRates reads the rates of every currency over [from, to].
func loadFXRates(ctx context.Context, src fxRateSource, currencies map[string]bool, from, to time.Time) (fxRates, error) {
	out := fxRates{}
	for c := range currencies {
		if c == "USD" {
			continue
		}
		if _, ok := fiatToConvert(c); !ok {
			return nil, fmt.Errorf("no USD rate is served for %s", c)
		}
		rates, err := src.FinalRatesToUSD(ctx, c, from, to)
		if err != nil {
			return nil, fmt.Errorf("fx rates %s: %w", c, err)
		}
		out[c] = rates
	}
	return out, nil
}

// convertFlows sums flows into USD deposits and withdrawals, each at the rate
// of its own day. A flow dated after `latest` (the newest day with a final
// rate) takes that day's rate. Returns the first currency and day missing a
// rate, if any.
func convertFlows(flows []*connector.Cashflow, account string, trusted bool, latest time.Time, rates fxRates) (deposits, withdrawals float64, missing string) {
	for _, cf := range flows {
		if cf == nil || cf.Amount == 0 {
			continue
		}
		ccy := flowCurrency(cf, account, trusted)
		day := truncDay(cf.Timestamp)
		if day.After(latest) {
			day = latest
		}
		r, ok := rates.rate(ccy, day)
		if !ok {
			return 0, 0, ccy + " " + dayKey(day)
		}
		if cf.Amount > 0 {
			deposits += cf.Amount * r
		} else {
			withdrawals -= cf.Amount * r
		}
	}
	return deposits, withdrawals, ""
}

// stampFX records on the global entry the currency a row was converted from
// and the rate its balances took.
func stampFX(b *repository.MarketBreakdown, currency string, rate float64) {
	if b == nil || rate <= 0 {
		return
	}
	if b.Global == nil {
		b.Global = &repository.MarketMetrics{}
	}
	b.Global.NativeCurrency = currency
	b.Global.FXRateToUSD = rate
}

// --- live snapshot ----------------------------------------------------------

// liveFX is how a live snapshot was converted. A zero value means it was not.
type liveFX struct {
	currency string
	rate     float64
}

// liveMeasuredOn is the day whose close a live balance describes: the
// statement date for a statement broker (IBKR), otherwise the day that just
// closed, since the sync reads at the midnight opening startOfDay.
func liveMeasuredOn(conn connector.Connector, startOfDay time.Time) time.Time {
	if fp, ok := conn.(connector.BalanceFreshnessProvider); ok {
		if asOf := fp.BalanceAsOf(); !asOf.IsZero() {
			return truncDay(asOf)
		}
	}
	return startOfDay.Add(-24 * time.Hour)
}

// applyLiveFX converts a live reading in place: the balance, the per-market
// balances, and the window's deposits and withdrawals rebuilt from its flows.
// Pure, so the arithmetic is tested without a sync.
func applyLiveFX(balance *connector.Balance, act *liveActivity, currency string, trusted bool, measured time.Time, rates fxRates) (liveFX, string) {
	rate, ok := rates.rate(currency, measured)
	if !ok {
		return liveFX{}, currency + " " + dayKey(measured)
	}
	deposits, withdrawals, missing := convertFlows(act.cashflows, currency, trusted, measured, rates)
	if missing != "" {
		return liveFX{}, missing
	}

	balance.Equity *= rate
	balance.Available *= rate
	balance.UnrealizedPnL *= rate
	balance.Currency = "USD"
	if act.breakdown != nil {
		for _, m := range act.breakdown.markets() {
			m.equity *= rate
			m.availableMargin *= rate
		}
	}
	act.deposits, act.withdrawals = deposits, withdrawals
	return liveFX{currency: currency, rate: rate}, ""
}

// convertLive converts a live reading when the account is held in a
// convertible currency. It returns false, having marked the result skipped,
// when the rate is not available yet: writing the day in EUR next to USD
// days, or with a provisional rate, would put a step into the curve that
// nothing later corrects. The deferred retry writes it once the rate lands.
func (s *SyncService) convertLive(ctx context.Context, conn connector.Connector, connMeta *repository.ExchangeConnection, balance *connector.Balance, act *liveActivity, startOfDay time.Time, result *SyncResult) (liveFX, bool) {
	currency, convert := fiatToConvert(balance.Currency)
	if !convert {
		return liveFX{}, true
	}
	if s.fx == nil {
		s.logger.Error("account held in a non-USD currency but no FX source is configured; stored unconverted",
			zap.String("user_uid", connMeta.UserUID),
			zap.String("exchange", connMeta.Exchange),
			zap.String("label", connMeta.Label),
			zap.String("currency", currency),
		)
		return liveFX{}, true
	}

	trusted := trustsCashflowCurrency(connMeta.Exchange)
	measured := liveMeasuredOn(conn, startOfDay)
	currencies := map[string]bool{currency: true}
	from := measured
	for _, cf := range act.cashflows {
		currencies[flowCurrency(cf, currency, trusted)] = true
		if d := truncDay(cf.Timestamp); d.Before(from) {
			from = d
		}
	}

	reason := ""
	rates, err := loadFXRates(ctx, s.fx, currencies, from, measured)
	var fx liveFX
	if err != nil {
		reason = err.Error()
	} else if fx, reason = applyLiveFX(balance, act, currency, trusted, measured, rates); reason != "" {
		reason = "no final USD rate for " + reason
	}
	if reason == "" {
		return fx, true
	}

	result.Skipped = true
	result.SkipReason = "fx conversion: " + reason
	s.logger.Warn("skipping live snapshot: FX rate unavailable",
		zap.String("user_uid", connMeta.UserUID),
		zap.String("exchange", connMeta.Exchange),
		zap.String("label", connMeta.Label),
		zap.String("reason", reason),
	)
	s.scheduleDeferredRetry(connMeta, rateLimitRetryDelay)
	return liveFX{}, false
}

// --- reconstructed history ----------------------------------------------------

// convertHistory converts reconstructed rows in place. Rows in USD (or a
// currency we do not convert) pass untouched. A row without a final rate is
// dropped when it sits at the end of the series, where it is the newest day
// and tomorrow's run writes it; anywhere else it fails the whole batch,
// because writing around a hole would leave EUR between USD days.
func convertHistory(rows []*connector.HistoricalSnapshot, trusted bool, rates fxRates) ([]*connector.HistoricalSnapshot, error) {
	out := rows[:0:0]
	var held []*connector.HistoricalSnapshot // rows waiting to learn whether a later row converts
	var heldReason string
	for _, h := range rows {
		currency, convert := fiatToConvert(h.Currency)
		if !convert {
			if len(held) > 0 {
				return nil, fmt.Errorf("no final USD rate for %s", heldReason)
			}
			out = append(out, h)
			continue
		}
		measured := truncDay(h.Date)
		if !h.MeasuredOn.IsZero() {
			measured = truncDay(h.MeasuredOn)
		}
		reason := convertHistoricalRow(h, currency, trusted, measured, rates)
		if reason != "" {
			if len(held) == 0 {
				heldReason = reason
			}
			held = append(held, h)
			continue
		}
		if len(held) > 0 {
			return nil, fmt.Errorf("no final USD rate for %s", heldReason)
		}
		out = append(out, h)
	}
	return out, nil
}

func convertHistoricalRow(h *connector.HistoricalSnapshot, currency string, trusted bool, measured time.Time, rates fxRates) string {
	rate, ok := rates.rate(currency, measured)
	if !ok {
		return currency + " " + dayKey(measured)
	}
	deposits, withdrawals := h.Deposits*rate, h.Withdrawals*rate
	if h.Cashflows != nil {
		var missing string
		if deposits, withdrawals, missing = convertFlows(h.Cashflows, currency, trusted, measured, rates); missing != "" {
			return missing
		}
	}
	h.TotalEquity *= rate
	h.RealizedBalance *= rate
	for _, mb := range h.Breakdown {
		if mb != nil {
			mb.Equity *= rate
			mb.AvailableMargin *= rate
		}
	}
	h.Deposits, h.Withdrawals = deposits, withdrawals
	h.FXRateToUSD = rate
	return ""
}

// historyFXWindow returns the currencies a batch needs rates for, and the
// days to read them over.
func historyFXWindow(rows []*connector.HistoricalSnapshot, trusted bool) (currencies map[string]bool, from, to time.Time) {
	currencies = map[string]bool{}
	for _, h := range rows {
		currency, convert := fiatToConvert(h.Currency)
		if !convert {
			continue
		}
		currencies[currency] = true
		for _, d := range []time.Time{h.Date, h.MeasuredOn} {
			if d.IsZero() {
				continue
			}
			d = truncDay(d)
			if from.IsZero() || d.Before(from) {
				from = d
			}
			if d.After(to) {
				to = d
			}
		}
		for _, cf := range h.Cashflows {
			currencies[flowCurrency(cf, currency, trusted)] = true
			if d := truncDay(cf.Timestamp); d.Before(from) {
				from = d
			}
		}
	}
	return currencies, from, to
}

// convertHistoryToUSD converts a reconstruction before the gate and the
// writes see it.
func (s *SyncService) convertHistoryToUSD(ctx context.Context, connMeta *repository.ExchangeConnection, rows []*connector.HistoricalSnapshot) ([]*connector.HistoricalSnapshot, error) {
	trusted := trustsCashflowCurrency(connMeta.Exchange)
	// Oldest first, so the rows convertHistory may hold back are the newest.
	rows = append([]*connector.HistoricalSnapshot(nil), rows...)
	sort.SliceStable(rows, func(i, j int) bool { return rows[i].Date.Before(rows[j].Date) })
	currencies, from, to := historyFXWindow(rows, trusted)
	if len(currencies) == 0 {
		return rows, nil
	}
	if s.fx == nil {
		s.logger.Error("reconstruction in a non-USD currency but no FX source is configured; stored unconverted",
			zap.String("user_uid", connMeta.UserUID),
			zap.String("exchange", connMeta.Exchange),
			zap.String("label", connMeta.Label),
		)
		return rows, nil
	}
	rates, err := loadFXRates(ctx, s.fx, currencies, from, to)
	if err != nil {
		return nil, err
	}
	converted, err := convertHistory(rows, trusted, rates)
	if err != nil {
		return nil, err
	}
	if dropped := len(rows) - len(converted); dropped > 0 {
		s.logger.Info("reconstruction: newest days held back until their FX rate is final",
			zap.String("user_uid", connMeta.UserUID),
			zap.String("exchange", connMeta.Exchange),
			zap.String("label", connMeta.Label),
			zap.Int("days", dropped),
		)
	}
	return converted, nil
}

// flowsInUSD returns an account's flows with each amount in USD at its own
// day's rate, for the admin reflow, which rewrites deposits and withdrawals
// on rows already stored in USD. Flows of a USD account come back as they
// are. Without an FX source it refuses rather than write EUR into USD rows.
func (s *SyncService) flowsInUSD(ctx context.Context, connMeta *repository.ExchangeConnection, accountCurrency string, flows []*connector.Cashflow) ([]*connector.Cashflow, error) {
	currency, convert := fiatToConvert(accountCurrency)
	if !convert || len(flows) == 0 {
		return flows, nil
	}
	if s.fx == nil {
		return nil, fmt.Errorf("account held in %s and no FX source configured", currency)
	}
	trusted := trustsCashflowCurrency(connMeta.Exchange)
	latest := truncDay(time.Now()).Add(-24 * time.Hour)
	currencies := map[string]bool{currency: true}
	from := latest
	for _, cf := range flows {
		currencies[flowCurrency(cf, currency, trusted)] = true
		if d := truncDay(cf.Timestamp); d.Before(from) {
			from = d
		}
	}
	rates, err := loadFXRates(ctx, s.fx, currencies, from, latest)
	if err != nil {
		return nil, err
	}
	out := make([]*connector.Cashflow, 0, len(flows))
	for _, cf := range flows {
		day := truncDay(cf.Timestamp)
		if day.After(latest) {
			day = latest
		}
		ccy := flowCurrency(cf, currency, trusted)
		r, ok := rates.rate(ccy, day)
		if !ok {
			return nil, fmt.Errorf("no final USD rate for %s %s", ccy, dayKey(day))
		}
		out = append(out, &connector.Cashflow{Amount: cf.Amount * r, Currency: "USD", Timestamp: cf.Timestamp})
	}
	return out, nil
}

// --- rate source: benchmark-service -------------------------------------------

// maxFXResponseBytes bounds the decode like the benchmark series: the rates
// end up in stored balances and in the signed report.
const maxFXResponseBytes = 4 << 20

// fxCache keeps final rates, which never change, for the enclave's lifetime.
type fxCache struct {
	mu    sync.Mutex
	rates map[string]map[string]float64
}

// FinalRatesToUSD implements fxRateSource on the benchmark-service client.
func (s *BenchmarkService) FinalRatesToUSD(ctx context.Context, currency string, from, to time.Time) (map[string]float64, error) {
	from, to = truncDay(from), truncDay(to)
	if to.Before(from) {
		return map[string]float64{}, nil
	}

	s.fxCache.mu.Lock()
	cached := s.fxCache.rates[currency]
	firstMissing := time.Time{}
	for d := from; !d.After(to); d = d.Add(24 * time.Hour) {
		if _, ok := cached[dayKey(d)]; !ok {
			firstMissing = d
			break
		}
	}
	s.fxCache.mu.Unlock()

	if !firstMissing.IsZero() {
		fetched, err := s.fetchFXRates(ctx, currency, firstMissing, to)
		if err != nil {
			return nil, err
		}
		s.fxCache.mu.Lock()
		if s.fxCache.rates == nil {
			s.fxCache.rates = map[string]map[string]float64{}
		}
		if s.fxCache.rates[currency] == nil {
			s.fxCache.rates[currency] = map[string]float64{}
		}
		for d, r := range fetched {
			s.fxCache.rates[currency][d] = r
		}
		s.fxCache.mu.Unlock()
	}

	out := map[string]float64{}
	s.fxCache.mu.Lock()
	defer s.fxCache.mu.Unlock()
	for d := from; !d.After(to); d = d.Add(24 * time.Hour) {
		if r, ok := s.fxCache.rates[currency][dayKey(d)]; ok {
			out[dayKey(d)] = r
		}
	}
	return out, nil
}

func (s *BenchmarkService) fetchFXRates(ctx context.Context, currency string, from, to time.Time) (map[string]float64, error) {
	if s.baseURL == "" {
		return nil, fmt.Errorf("BENCHMARK_SERVICE_URL not configured")
	}
	reqURL := fmt.Sprintf("%s/api/v1/benchmarks/fx/%s/daily?startDate=%s&endDate=%s",
		s.baseURL, url.PathEscape(currency), dayKey(from), dayKey(to))
	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "TrackRecord-Enclave/1.0")
	req.Header.Set("Accept", "application/json")
	if s.internalToken != "" {
		req.Header.Set("X-Internal-Token", s.internalToken)
	}
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("benchmark-service fx request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("benchmark-service returned %d for fx %s", resp.StatusCode, currency)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxFXResponseBytes+1))
	if err != nil {
		return nil, fmt.Errorf("benchmark-service fx read: %w", err)
	}
	if len(body) > maxFXResponseBytes {
		return nil, fmt.Errorf("benchmark-service fx response exceeds %d bytes for %s", maxFXResponseBytes, currency)
	}
	return parseFXRates(body, currency)
}

// parseFXRates keeps the final rates of a benchmark-service FX response.
func parseFXRates(body []byte, currency string) (map[string]float64, error) {
	var result struct {
		Success bool   `json:"success"`
		Error   string `json:"error"`
		Data    struct {
			Currency string `json:"currency"`
			Quote    string `json:"quote"`
			Rates    []struct {
				Date  string  `json:"date"`
				Rate  float64 `json:"rate"`
				Final bool    `json:"final"`
			} `json:"rates"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("benchmark-service fx decode: %w", err)
	}
	if !result.Success {
		return nil, fmt.Errorf("benchmark-service fx error for %s: %s", currency, result.Error)
	}
	if !strings.EqualFold(result.Data.Currency, currency) || !strings.EqualFold(result.Data.Quote, "USD") {
		return nil, fmt.Errorf("benchmark-service fx answered %s/%s for %s/USD", result.Data.Currency, result.Data.Quote, currency)
	}
	if len(result.Data.Rates) > maxBenchmarkPoints {
		return nil, fmt.Errorf("benchmark-service returned too many fx points (%d) for %s", len(result.Data.Rates), currency)
	}
	out := make(map[string]float64, len(result.Data.Rates))
	for _, r := range result.Data.Rates {
		if !r.Final || r.Rate <= 0 {
			continue
		}
		if _, err := time.Parse("2006-01-02", r.Date); err != nil {
			continue
		}
		out[r.Date] = r.Rate
	}
	return out, nil
}
