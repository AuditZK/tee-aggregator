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
// and for a long time nothing converted it (IG and MetaTrader still are not,
// see fxConvertedExchanges): a EUR account's equity travelled
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

// fxConvertedExchanges are the brokers whose non-USD accounts are converted.
//
// IG and MetaTrader report an account currency too, but they emit no marker
// saying which accounts are not in USD, so nobody can tell which stored
// histories would need converting along with them. Turning them on before
// that inventory would put a ~10% step between each such account's stored
// EUR days and its first USD day, read by analytics as a return. They join
// this list once their accounts are known and backfilled.
var fxConvertedExchanges = map[string]bool{"ibkr": true, "ctrader": true}

func convertsExchange(exchange string) bool {
	return fxConvertedExchanges[strings.ToLower(strings.TrimSpace(exchange))]
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

// denominationOf is what a statement in currency is denominated in for the
// stored series: the code itself when it converts, USD for USD or no code,
// false for a currency neither converted nor USD, which stays as reported.
func denominationOf(currency string) (string, bool) {
	c, convert := fiatToConvert(currency)
	switch {
	case convert:
		return c, true
	case c == "" || c == "USD":
		return "USD", true
	default:
		return c, false
	}
}

// stampedDenomination is the currency a stored row says it was written in,
// empty when it carries no stamp.
func stampedDenomination(r *repository.Snapshot) string {
	if r == nil || r.Breakdown == nil || r.Breakdown.Global == nil {
		return ""
	}
	return r.Breakdown.Global.NativeCurrency
}

// storedDayDenomination is the currency a stored day is in. An unstamped day
// is in dollars: every day stored in another currency was converted, and
// stamped, before the sync wrote its first converted one.
func storedDayDenomination(r *repository.Snapshot) string {
	if d := stampedDenomination(r); d != "" {
		return d
	}
	return "USD"
}

// storedDenomination is the currency a connection's history is written in:
// that of its newest stamped row, empty when no row is stamped.
func (s *SyncService) storedDenomination(ctx context.Context, connMeta *repository.ExchangeConnection) string {
	if s.snapshotRepo == nil {
		return ""
	}
	rows, err := s.snapshotRepo.GetByUserAndDateRange(ctx, connMeta.UserUID, time.Unix(0, 0).UTC(), time.Now().UTC().Add(24*time.Hour))
	if err != nil {
		return ""
	}
	return newestDenomination(rows, connMeta.Exchange, connMeta.Label)
}

func newestDenomination(rows []*repository.Snapshot, exchange, label string) string {
	var newest *repository.Snapshot
	for _, r := range rows {
		if r.Exchange != exchange || r.Label != label || stampedDenomination(r) == "" {
			continue
		}
		if newest == nil || r.Timestamp.After(newest.Timestamp) {
			newest = r
		}
	}
	return stampedDenomination(newest)
}

// rewritesInAnotherCurrency reports whether a reconstruction is stated in
// another currency than the connection's stored history, which happens when
// the statement changed denomination and carried no rate to state it back.
func rewritesInAnotherCurrency(rebuilt, stored []*repository.Snapshot, exchange, label string) (from, to string, ok bool) {
	for _, r := range rebuilt {
		if d := stampedDenomination(r); d != "" {
			to = d
		}
	}
	from = newestDenomination(stored, exchange, label)
	return from, to, from != "" && to != "" && from != to
}

// restateFlows states the flows in from in to, one unit of to being worth
// rate units of from, and returns the sums. Flows in any other currency keep
// theirs.
func restateFlows(flows []*connector.Cashflow, from, to string, rate float64) {
	for _, cf := range flows {
		if cf != nil && cf.Currency == from {
			cf.Amount /= rate
			cf.Currency = to
		}
	}
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
	if !convertsExchange(connMeta.Exchange) {
		return liveFX{}, true
	}
	denom, known := denominationOf(balance.Currency)
	if !known {
		return liveFX{}, true
	}

	// A statement that changed denomination (an IBKR account rebased from EUR
	// to USD) is stated back in the one its history is written in, at the
	// venue's own rate. Without that rate the day waits for the reconstruction
	// that rewrites the history in the new one: a day in another unit than the
	// days around it is a step that reads as a return.
	if stored := s.storedDenomination(ctx, connMeta); stored != "" && stored != denom {
		rate := balance.BaseRates[stored]
		if rate <= 0 {
			result.Skipped = true
			result.SkipReason = "account currency changed from " + stored + " to " + denom + "; waiting for its history to be restated"
			s.logger.Warn("skipping live snapshot: account currency changed",
				zap.String("user_uid", connMeta.UserUID),
				zap.String("exchange", connMeta.Exchange),
				zap.String("label", connMeta.Label),
				zap.String("stored", stored),
				zap.String("statement", denom),
			)
			return liveFX{}, false
		}
		restateLive(balance, act, denom, stored, rate)
		denom = stored
	}

	if s.fx == nil {
		if denom != "USD" {
			s.logger.Error("account held in a non-USD currency but no FX source is configured; stored unconverted",
				zap.String("user_uid", connMeta.UserUID),
				zap.String("exchange", connMeta.Exchange),
				zap.String("label", connMeta.Label),
				zap.String("currency", denom),
			)
			return liveFX{}, true
		}
		return liveFX{currency: "USD", rate: 1}, true
	}

	trusted := trustsCashflowCurrency(connMeta.Exchange)
	measured := liveMeasuredOn(conn, startOfDay)
	currencies := map[string]bool{denom: true}
	from := measured
	for _, cf := range act.cashflows {
		currencies[flowCurrency(cf, denom, trusted)] = true
		if d := truncDay(cf.Timestamp); d.Before(from) {
			from = d
		}
	}

	reason := ""
	rates, err := loadFXRates(ctx, s.fx, currencies, from, measured)
	var fx liveFX
	switch {
	case err != nil:
		reason = err.Error()
	case denom == "USD":
		fx, reason = applyLiveUSD(act, trusted, measured, rates)
	default:
		if err := s.ensureStoredConverted(ctx, connMeta, denom); err != nil {
			reason = err.Error()
		} else if fx, reason = applyLiveFX(balance, act, denom, trusted, measured, rates); reason != "" {
			reason = "no final USD rate for " + reason
		}
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

// restateLive states a live reading in to instead of from, one unit of to
// being worth rate units of from.
func restateLive(balance *connector.Balance, act *liveActivity, from, to string, rate float64) {
	balance.Equity /= rate
	balance.Available /= rate
	balance.UnrealizedPnL /= rate
	balance.Currency = to
	if act.breakdown != nil {
		for _, m := range act.breakdown.markets() {
			m.equity /= rate
			m.availableMargin /= rate
		}
	}
	if act.cashflows == nil {
		act.deposits /= rate
		act.withdrawals /= rate
		return
	}
	restateFlows(act.cashflows, from, to, rate)
}

// applyLiveUSD values the flows of a USD reading that are in another currency,
// and stamps the row USD so no later backfill mistakes it for one in the
// account's former currency.
func applyLiveUSD(act *liveActivity, trusted bool, measured time.Time, rates fxRates) (liveFX, string) {
	if act.cashflows != nil {
		deposits, withdrawals, missing := convertFlows(act.cashflows, "USD", trusted, measured, rates)
		if missing != "" {
			return liveFX{}, "no final USD rate for " + missing
		}
		act.deposits, act.withdrawals = deposits, withdrawals
	}
	return liveFX{currency: "USD", rate: 1}, ""
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
		denom, known := denominationOf(h.Currency)
		if !convert && !known {
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
		var reason string
		if convert {
			reason = convertHistoricalRow(h, currency, trusted, measured, rates)
		} else {
			reason = stampHistoricalUSD(h, denom, trusted, measured, rates)
		}
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

// stampHistoricalUSD values the flows of a USD row that are in another
// currency and stamps it USD, so no later backfill reads it as one in the
// account's former currency.
func stampHistoricalUSD(h *connector.HistoricalSnapshot, denom string, trusted bool, measured time.Time, rates fxRates) string {
	if h.Cashflows != nil {
		deposits, withdrawals, missing := convertFlows(h.Cashflows, denom, trusted, measured, rates)
		if missing != "" {
			return missing
		}
		h.Deposits, h.Withdrawals = deposits, withdrawals
	}
	h.Currency = denom
	h.FXRateToUSD = 1
	return ""
}

// historyFXWindow returns the currencies a batch needs rates for, and the
// days to read them over.
func historyFXWindow(rows []*connector.HistoricalSnapshot, trusted bool) (currencies map[string]bool, from, to time.Time) {
	currencies = map[string]bool{}
	for _, h := range rows {
		currency, known := denominationOf(h.Currency)
		if !known {
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

// restateHistory states, in place, every row denominated otherwise than the
// stored history in the stored denomination, at the venue's rate for the
// row's day. A row without that rate keeps its own denomination: the gate
// then holds it to nothing stored in another one, and the reconstruction
// rewrites the history in the new denomination. Returns how many rows were
// restated and from what.
func restateHistory(rows []*connector.HistoricalSnapshot, stored string) (int, string) {
	n, from := 0, ""
	for _, h := range rows {
		denom, known := denominationOf(h.Currency)
		if !known || denom == stored {
			continue
		}
		rate := h.BaseRates[stored]
		if rate <= 0 {
			continue
		}
		h.TotalEquity /= rate
		h.RealizedBalance /= rate
		for _, mb := range h.Breakdown {
			if mb != nil {
				mb.Equity /= rate
				mb.AvailableMargin /= rate
			}
		}
		if h.Cashflows != nil {
			restateFlows(h.Cashflows, denom, stored, rate)
		} else {
			h.Deposits /= rate
			h.Withdrawals /= rate
		}
		h.Currency = stored
		n, from = n+1, denom
	}
	return n, from
}

// accountCurrencyOf is the convertible currency the rows are held in.
func accountCurrencyOf(rows []*connector.HistoricalSnapshot) string {
	for _, h := range rows {
		if c, ok := fiatToConvert(h.Currency); ok {
			return c
		}
	}
	return ""
}

// convertHistoryToUSD converts a reconstruction before the gate and the
// writes see it.
func (s *SyncService) convertHistoryToUSD(ctx context.Context, connMeta *repository.ExchangeConnection, rows []*connector.HistoricalSnapshot) ([]*connector.HistoricalSnapshot, error) {
	if !convertsExchange(connMeta.Exchange) {
		return rows, nil
	}
	trusted := trustsCashflowCurrency(connMeta.Exchange)
	// Oldest first, so the rows convertHistory may hold back are the newest.
	rows = append([]*connector.HistoricalSnapshot(nil), rows...)
	sort.SliceStable(rows, func(i, j int) bool { return rows[i].Date.Before(rows[j].Date) })
	if stored := s.storedDenomination(ctx, connMeta); stored != "" {
		if restated, from := restateHistory(rows, stored); restated > 0 {
			s.logger.Info("reconstruction stated in the stored currency at the venue's rates",
				zap.String("user_uid", connMeta.UserUID),
				zap.String("exchange", connMeta.Exchange),
				zap.String("label", connMeta.Label),
				zap.String("statement", from),
				zap.String("stored", stored),
				zap.Int("days", restated),
			)
		}
	}
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
	// The gate compares these rows with the stored ones: those must be in
	// USD too. A reconstruction carries one account currency.
	if c := accountCurrencyOf(rows); c != "" && backfillsStoredRows(connMeta.Exchange) {
		if err := s.ensureStoredConverted(ctx, connMeta, c); err != nil {
			return nil, err
		}
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
	if !convert || len(flows) == 0 || !convertsExchange(connMeta.Exchange) {
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
