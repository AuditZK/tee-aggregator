package connector

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// binanceMove is one transfer leg, signed from this account's side: positive
// entered it.
type binanceMove struct {
	t    time.Time
	coin string
	qty  float64
}

var binanceUMMovementTypes = []string{"TRANSFER", "INTERNAL_TRANSFER", "STRATEGY_UMFUTURES_TRANSFER"}

// binanceUMCounterpartTypes are the universal transfers with a USDⓈ-M leg
// that the sync already accounts for, as an internal move (spot, margin) or
// as a booked flow (funding). Options is left out on purpose: nothing books a
// USDⓈ-M↔options transfer, so its income row has to surface as a stray flow.
// Mirrors the history rebuilder's umCounterpartTransferTypes.
var binanceUMCounterpartTypes = []string{
	"MAIN_UMFUTURE", "UMFUTURE_MAIN", "FUNDING_UMFUTURE", "UMFUTURE_FUNDING",
	"MARGIN_UMFUTURE", "UMFUTURE_MARGIN",
}

// binanceCounterpartWindow is how far apart the two legs of one transfer may
// be stamped; Binance writes both within a second.
const binanceCounterpartWindow = 10 * time.Minute

// fetchSubAccountAndStrayFlows books the transfers no other Binance ledger
// shows as a cashflow: a master's transfers to and from its sub-accounts, a
// sub-account's own transfers with its master, and any USDⓈ-M transfer row
// no known transfer explains (a futures transfer to a sub-account, a trading
// bot, copy trading). Without them the money reads as a trading loss or gain
// (field case: a master funded a new sub-account's futures wallet from the app
// and its whole track record read as a total loss). Best-effort throughout: a
// key that is not a master reads no sub-account list, a key without futures
// reads no income, and neither touches the flows already collected.
func (b *Binance) fetchSubAccountAndStrayFlows(ctx context.Context, since, now time.Time, add func(time.Time, float64), usdValue func(string, float64) float64) {
	booked := b.masterSubMoves(ctx, since, now, b.subAccountEmails(ctx))
	booked = append(booked, b.subUserMoves(ctx, since, now)...)
	for _, m := range booked {
		add(m.t, usdValue(m.coin, m.qty))
	}

	moves, err := b.umTransferRows(ctx, since, now)
	if err != nil || len(moves) == 0 {
		return
	}
	counterparts, ok := b.umCounterparts(ctx, since, now)
	if !ok {
		return
	}
	for _, m := range strayBinanceMoves(moves, append(counterparts, booked...)) {
		add(m.t, usdValue(m.coin, m.qty))
	}
}

// strayBinanceMoves keeps the USDⓈ-M transfer rows that no counterpart
// explains. A counterpart explains one row at most.
func strayBinanceMoves(moves, counterparts []binanceMove) []binanceMove {
	used := make([]bool, len(counterparts))
	var out []binanceMove
	for _, m := range moves {
		if m.qty == 0 {
			continue
		}
		matched := false
		for i, p := range counterparts {
			if used[i] || p.coin != m.coin || !binanceSameAmount(p.qty, m.qty) {
				continue
			}
			if d := p.t.Sub(m.t); d < -binanceCounterpartWindow || d > binanceCounterpartWindow {
				continue
			}
			used[i] = true
			matched = true
			break
		}
		if !matched {
			out = append(out, m)
		}
	}
	return out
}

func binanceSameAmount(a, b float64) bool {
	a, b = math.Abs(a), math.Abs(b)
	return math.Abs(a-b) <= 1e-8*math.Max(1, math.Max(a, b))
}

// binanceMasterSubSign reads one master↔sub transfer from the master's side:
// +1 when the master received, -1 when it sent, 0 when both ends are
// sub-accounts. The master is whichever end is not a known sub-account.
func binanceMasterSubSign(from, to string, subs map[string]bool) float64 {
	fromSub, toSub := subs[from], subs[to]
	switch {
	case fromSub && !toSub:
		return +1
	case toSub && !fromSub:
		return -1
	default:
		return 0
	}
}

// isBinanceRefusal reports a definitive 4xx answer: the key has no such
// product. Rate limits and bans (429, 418) and transport failures are not.
func isBinanceRefusal(err error) bool {
	if errors.Is(err, ErrTransient) {
		return false
	}
	msg := err.Error()
	return strings.HasPrefix(msg, "HTTP 4") && !strings.HasPrefix(msg, "HTTP 429") && !strings.HasPrefix(msg, "HTTP 418")
}

func (b *Binance) subAccountEmails(ctx context.Context) map[string]bool {
	subs := map[string]bool{}
	body, err := b.doRequest(ctx, "GET", binanceSpotAPI, "/sapi/v1/sub-account/list", url.Values{"limit": {"200"}}, true)
	if err != nil {
		return subs
	}
	var resp struct {
		SubAccounts []struct {
			Email string `json:"email"`
		} `json:"subAccounts"`
	}
	if json.Unmarshal(body, &resp) != nil {
		return subs
	}
	for _, s := range resp.SubAccounts {
		if s.Email != "" {
			subs[s.Email] = true
		}
	}
	return subs
}

// masterSubMoves reads the master's side of its sub-account transfers: the
// spot transfer history per sub-account in both directions, then the
// universal sub-account transfers. Both appear in no other ledger.
func (b *Binance) masterSubMoves(ctx context.Context, since, now time.Time, subs map[string]bool) []binanceMove {
	if len(subs) == 0 {
		return nil
	}
	ms := func(t time.Time) string { return strconv.FormatInt(t.UnixMilli(), 10) }
	seen := map[string]bool{}
	var out []binanceMove
	for email := range subs {
		for _, side := range []string{"fromEmail", "toEmail"} {
			body, err := b.doRequest(ctx, "GET", binanceSpotAPI, "/sapi/v1/sub-account/sub/transfer/history", url.Values{
				side: {email}, "startTime": {ms(since)}, "endTime": {ms(now)}, "limit": {"500"},
			}, true)
			if err != nil {
				continue
			}
			var rows []struct {
				From   string          `json:"from"`
				To     string          `json:"to"`
				Asset  string          `json:"asset"`
				Qty    string          `json:"qty"`
				Status string          `json:"status"`
				TranID json.RawMessage `json:"tranId"`
				Time   int64           `json:"time"`
			}
			if json.Unmarshal(body, &rows) != nil {
				continue
			}
			for _, r := range rows {
				sign := binanceMasterSubSign(r.From, r.To, subs)
				key := binanceTransferKey("spot", r.TranID, r.Time, r.Asset, r.Qty)
				if sign == 0 || seen[key] || (r.Status != "" && !strings.EqualFold(r.Status, "SUCCESS")) {
					continue
				}
				seen[key] = true
				qty, _ := strconv.ParseFloat(r.Qty, 64)
				out = append(out, binanceMove{t: time.UnixMilli(r.Time).UTC(), coin: strings.ToUpper(r.Asset), qty: sign * qty})
			}
		}
	}

	body, err := b.doRequest(ctx, "GET", binanceSpotAPI, "/sapi/v1/sub-account/universalTransfer", url.Values{
		"startTime": {ms(since)}, "endTime": {ms(now)}, "limit": {"500"},
	}, true)
	if err != nil {
		return out
	}
	var resp struct {
		Result []struct {
			FromEmail       string          `json:"fromEmail"`
			ToEmail         string          `json:"toEmail"`
			Asset           string          `json:"asset"`
			Amount          string          `json:"amount"`
			Status          string          `json:"status"`
			TranID          json.RawMessage `json:"tranId"`
			CreateTimeStamp int64           `json:"createTimeStamp"`
		} `json:"result"`
	}
	if json.Unmarshal(body, &resp) != nil {
		return out
	}
	for _, r := range resp.Result {
		sign := binanceMasterSubSign(r.FromEmail, r.ToEmail, subs)
		key := binanceTransferKey("universal", r.TranID, r.CreateTimeStamp, r.Asset, r.Amount)
		if sign == 0 || seen[key] || (r.Status != "" && !strings.EqualFold(r.Status, "SUCCESS")) {
			continue
		}
		seen[key] = true
		qty, _ := strconv.ParseFloat(r.Amount, 64)
		out = append(out, binanceMove{t: time.UnixMilli(r.CreateTimeStamp).UTC(), coin: strings.ToUpper(r.Asset), qty: sign * qty})
	}
	return out
}

// subUserMoves reads a sub-account's own transfers with its master (type 1 =
// in, 2 = out). A master key is refused, which leaves nothing to add.
func (b *Binance) subUserMoves(ctx context.Context, since, now time.Time) []binanceMove {
	body, err := b.doRequest(ctx, "GET", binanceSpotAPI, "/sapi/v1/sub-account/transfer/subUserHistory", url.Values{
		"startTime": {strconv.FormatInt(since.UnixMilli(), 10)},
		"endTime":   {strconv.FormatInt(now.UnixMilli(), 10)},
		"limit":     {"500"},
	}, true)
	if err != nil {
		return nil
	}
	var rows []struct {
		Asset  string `json:"asset"`
		Qty    string `json:"qty"`
		Type   int    `json:"type"`
		Status string `json:"status"`
		Time   int64  `json:"time"`
	}
	if json.Unmarshal(body, &rows) != nil {
		return nil
	}
	var out []binanceMove
	for _, r := range rows {
		if r.Status != "" && !strings.EqualFold(r.Status, "SUCCESS") {
			continue
		}
		qty, _ := strconv.ParseFloat(r.Qty, 64)
		if r.Type == 2 {
			qty = -qty
		}
		out = append(out, binanceMove{t: time.UnixMilli(r.Time).UTC(), coin: strings.ToUpper(r.Asset), qty: qty})
	}
	return out
}

// umTransferRows reads the USDⓈ-M income rows that move money in or out of
// the wallet without being trading.
func (b *Binance) umTransferRows(ctx context.Context, since, now time.Time) ([]binanceMove, error) {
	var out []binanceMove
	for _, typ := range binanceUMMovementTypes {
		body, err := b.doRequest(ctx, "GET", binanceFuturesAPI, "/fapi/v1/income", url.Values{
			"incomeType": {typ},
			"startTime":  {strconv.FormatInt(since.UnixMilli(), 10)},
			"endTime":    {strconv.FormatInt(now.UnixMilli(), 10)},
			"limit":      {"1000"},
		}, true)
		if err != nil {
			return nil, err
		}
		var rows []struct {
			Asset  string `json:"asset"`
			Income string `json:"income"`
			Time   int64  `json:"time"`
		}
		if err := json.Unmarshal(body, &rows); err != nil {
			return nil, err
		}
		for _, r := range rows {
			qty, _ := strconv.ParseFloat(r.Income, 64)
			out = append(out, binanceMove{t: time.UnixMilli(r.Time).UTC(), coin: strings.ToUpper(r.Asset), qty: qty})
		}
	}
	return out, nil
}

// umCounterparts reads the universal transfers that explain USDⓈ-M transfer
// rows. ok is false when a read failed for any reason other than a refusal:
// without its counterparts every ordinary spot↔futures shuttle would be booked
// as a deposit or a withdrawal.
func (b *Binance) umCounterparts(ctx context.Context, since, now time.Time) ([]binanceMove, bool) {
	var out []binanceMove
	for _, typ := range binanceUMCounterpartTypes {
		for page := 1; page <= 10; page++ {
			body, err := b.doRequest(ctx, "GET", binanceSpotAPI, "/sapi/v1/asset/transfer", url.Values{
				"type":      {typ},
				"startTime": {strconv.FormatInt(since.UnixMilli(), 10)},
				"endTime":   {strconv.FormatInt(now.UnixMilli(), 10)},
				"current":   {strconv.Itoa(page)},
				"size":      {"100"},
			}, true)
			if err != nil {
				if isBinanceRefusal(err) {
					break
				}
				return nil, false
			}
			var resp struct {
				Total int `json:"total"`
				Rows  []struct {
					Asset     string `json:"asset"`
					Amount    string `json:"amount"`
					Timestamp int64  `json:"timestamp"`
				} `json:"rows"`
			}
			if json.Unmarshal(body, &resp) != nil {
				return nil, false
			}
			for _, r := range resp.Rows {
				qty, _ := strconv.ParseFloat(r.Amount, 64)
				out = append(out, binanceMove{t: time.UnixMilli(r.Timestamp).UTC(), coin: strings.ToUpper(r.Asset), qty: qty})
			}
			if len(resp.Rows) == 0 || page*100 >= resp.Total {
				break
			}
		}
	}
	return out, true
}

// binanceTransferKey dedupes a transfer seen from both of its sides: the
// venue's id when present, otherwise the row itself.
func binanceTransferKey(kind string, id json.RawMessage, t int64, asset, qty string) string {
	if len(id) > 0 && string(id) != "null" {
		return kind + ":" + string(id)
	}
	return kind + ":" + strconv.FormatInt(t, 10) + ":" + asset + ":" + qty
}

// fetchFiatFlows books bank transfers (fiat orders) and card purchases and
// sales (fiat payments). Neither shows in the crypto deposit or withdrawal
// history, so an account used as an off-ramp (crypto in, converted to EUR,
// EUR wired out) read the withdrawal as a trading loss. Mirrors the history
// rebuilder's fetchFiatFlows. Best-effort: a refusal or an unreadable page
// adds nothing.
func (b *Binance) fetchFiatFlows(ctx context.Context, since, now time.Time, add func(time.Time, float64), usdValue func(string, float64) float64) {
	type fiatRow struct {
		FiatCurrency    string `json:"fiatCurrency"`
		IndicatedAmount string `json:"indicatedAmount"`
		Amount          string `json:"amount"`
		TotalFee        string `json:"totalFee"`
		SourceAmount    string `json:"sourceAmount"`
		ObtainAmount    string `json:"obtainAmount"`
		CryptoCurrency  string `json:"cryptoCurrency"`
		Status          string `json:"status"`
		CreateTime      int64  `json:"createTime"`
	}
	read := func(path, txType string) []fiatRow {
		body, err := b.doRequest(ctx, "GET", binanceSpotAPI, path, url.Values{
			"transactionType": {txType},
			"beginTime":       {strconv.FormatInt(since.UnixMilli(), 10)},
			"endTime":         {strconv.FormatInt(now.UnixMilli(), 10)},
			"rows":            {"500"},
		}, true)
		if err != nil {
			return nil
		}
		var resp struct {
			Data []fiatRow `json:"data"`
		}
		if json.Unmarshal(body, &resp) != nil {
			return nil
		}
		return resp.Data
	}
	parse := func(s string) float64 {
		v, _ := strconv.ParseFloat(s, 64)
		return v
	}

	for _, o := range read("/sapi/v1/fiat/orders", "0") {
		if strings.EqualFold(o.Status, "Successful") {
			add(time.UnixMilli(o.CreateTime).UTC(), usdValue(o.FiatCurrency, parse(o.Amount)))
		}
	}
	for _, o := range read("/sapi/v1/fiat/orders", "1") {
		if !strings.EqualFold(o.Status, "Successful") {
			continue
		}
		debited := parse(o.IndicatedAmount)
		if debited == 0 {
			debited = parse(o.Amount) + parse(o.TotalFee)
		}
		add(time.UnixMilli(o.CreateTime).UTC(), -usdValue(o.FiatCurrency, debited))
	}
	for _, p := range read("/sapi/v1/fiat/payments", "0") {
		if strings.EqualFold(p.Status, "Completed") {
			add(time.UnixMilli(p.CreateTime).UTC(), usdValue(p.CryptoCurrency, parse(p.ObtainAmount)))
		}
	}
	for _, p := range read("/sapi/v1/fiat/payments", "1") {
		if strings.EqualFold(p.Status, "Completed") {
			add(time.UnixMilli(p.CreateTime).UTC(), -usdValue(p.CryptoCurrency, parse(p.SourceAmount)))
		}
	}
}
