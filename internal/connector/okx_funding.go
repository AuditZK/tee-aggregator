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

// The funding account is the second wallet an OKX customer holds, and it is
// inside the tracked perimeter alongside the trading account. It was not: a
// transfer to it read as a withdrawal and the return as a deposit, so whatever
// the holding did in between became capital instead of performance — and a
// position parked over a drawdown lost the drawdown with it.
//
// The perimeter rule here must stay identical to the history rebuilder's. The
// two instruments meet on every reconstructed day, and the gate that compares
// them reads a disagreement as a corrupted rebuild rather than as two rules.
//
// ProbeFunding is the diagnostic half: it reports the ledger verbatim, which is
// how the type codes below were established in the first place.

const (
	okxAssetBillsPageLimit = 100
	okxAssetBillsMaxPages  = 40
)

// FundingProbe is the funding account as OKX describes it. Values stay the
// strings the venue sent — an inapplicable field is "" there and must not
// arrive here as a zero.
type FundingProbe struct {
	Balances []map[string]string `json:"balances"`
	Bills    []map[string]string `json:"bills"`

	// Notes carry what the probe could not read, so an empty ledger is never
	// mistaken for an idle account.
	Notes []string `json:"notes,omitempty"`
}

func (o *OKX) ProbeFunding(ctx context.Context, since time.Time) (*FundingProbe, error) {
	probe := &FundingProbe{Balances: []map[string]string{}, Bills: []map[string]string{}}

	balances, err := o.fetchAssetRows(ctx, "/api/v5/asset/balances")
	if err != nil {
		probe.Notes = append(probe.Notes, "funding balances unavailable: "+vendorErrorDetail(err.Error()))
	} else {
		probe.Balances = balances
	}

	// asset/bills serves a short recent window and asset/bills-history the
	// older one; a key without the funding permission fails both, which is an
	// answer worth reporting rather than an error to abort on.
	seen := map[string]bool{}
	var read int
	for _, endpoint := range []string{"/api/v5/asset/bills", "/api/v5/asset/bills-history"} {
		rows, err := o.fetchAssetBills(ctx, endpoint, since, time.Now().UTC(), seen)
		if err != nil {
			probe.Notes = append(probe.Notes, endpoint+" unavailable: "+vendorErrorDetail(err.Error()))
			continue
		}
		read++
		probe.Bills = append(probe.Bills, rows...)
	}
	if read == 0 && len(probe.Balances) == 0 {
		return nil, fmt.Errorf("okx: funding account unreadable with this key")
	}

	sort.SliceStable(probe.Bills, func(i, j int) bool { return probe.Bills[i]["ts"] < probe.Bills[j]["ts"] })
	return probe, nil
}

func (o *OKX) fetchAssetBills(ctx context.Context, endpoint string, since, now time.Time, seen map[string]bool) ([]map[string]string, error) {
	var out []map[string]string
	after := ""
	for page := 0; page < okxAssetBillsMaxPages; page++ {
		if page > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(okxBillsPagePace):
			}
		}

		path := fmt.Sprintf("%s?begin=%d&end=%d&limit=%d", endpoint, since.UnixMilli(), now.UnixMilli(), okxAssetBillsPageLimit)
		if after != "" {
			path += "&after=" + after
		}
		rows, err := o.fetchAssetRows(ctx, path)
		if err != nil {
			return nil, err
		}

		lastID := ""
		for _, r := range rows {
			id := r["billId"]
			lastID = id
			if id != "" && seen[id] {
				continue
			}
			if id != "" {
				seen[id] = true
			}
			r["iso_time"] = okxMillisToISO(r["ts"])
			out = append(out, r)
		}
		if len(rows) < okxAssetBillsPageLimit || lastID == "" || lastID == after {
			break
		}
		after = lastID
	}
	return out, nil
}

// fetchAssetRows decodes into string maps so a field this code has never heard
// of still reaches the operator — the point of the probe is the fields we do
// not yet know the meaning of.
func (o *OKX) fetchAssetRows(ctx context.Context, path string) ([]map[string]string, error) {
	body, err := o.doRequest(ctx, "GET", path)
	if err != nil {
		return nil, err
	}

	var resp struct {
		Data []map[string]json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("decode okx asset rows: %w", err)
	}

	rows := make([]map[string]string, 0, len(resp.Data))
	for _, d := range resp.Data {
		row := make(map[string]string, len(d))
		for k, raw := range d {
			var s string
			if json.Unmarshal(raw, &s) != nil {
				s = string(raw)
			}
			row[k] = s
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func okxMillisToISO(ms string) string {
	var n int64
	if _, err := fmt.Sscanf(ms, "%d", &n); err != nil || n == 0 {
		return ""
	}
	return time.UnixMilli(n).UTC().Format(time.RFC3339)
}

// fundingInternalTypes are the funding rows that name the trading account as
// their counterpart. Both wallets are inside the tracked perimeter, so such a
// row moves nothing across it. The codes come off a real ledger — OKX spells
// each one out in the notes field, and the tables published for this endpoint
// disagree with each other:
//
//	  1  Deposit                        external in
//	  2  Withdrawal                     external out
//	 20  Transferred to sub-account     external out
//	 21  Received from sub-account      external in
//	130  Received from trading account  internal
//	131  Transferred to trading account internal
//	326  Data migration out             zero-amount
//	327  Data migration in              zero-amount
//
// An unrecognised type books as a crossing. The two mistakes are not
// symmetric: a real flow mistaken for an internal move becomes invented
// performance, while the reverse only understates the record.
var fundingInternalTypes = map[string]bool{"130": true, "131": true}

// okxTransferMatchWindow is how far apart the two legs of one trading↔funding
// move may be booked and still pair. Observed: under a second, both ways.
const okxTransferMatchWindow = 10 * time.Minute

// fundingBalances returns the funding wallet's quantities per currency.
func (o *OKX) fundingBalances(ctx context.Context) (map[string]float64, error) {
	rows, err := o.fetchAssetRows(ctx, "/api/v5/asset/balances")
	if err != nil {
		return nil, err
	}
	out := make(map[string]float64, len(rows))
	for _, r := range rows {
		if v, err := strconv.ParseFloat(r["bal"], 64); err == nil && v != 0 {
			out[strings.ToUpper(r["ccy"])] = v
		}
	}
	return out, nil
}

// fundingEquityUSD marks the funding wallet with the same public tickers the
// cashflow path uses. An unpriceable coin contributes zero and raises a
// warning rather than a guess.
func (o *OKX) fundingEquityUSD(ctx context.Context) (float64, error) {
	balances, err := o.fundingBalances(ctx)
	if err != nil {
		return 0, err
	}
	if len(balances) == 0 {
		return 0, nil
	}

	var needPrice []string
	total := 0.0
	for ccy, qty := range balances {
		if okxStableCoins[ccy] {
			total += qty
			continue
		}
		needPrice = append(needPrice, ccy)
	}
	if len(needPrice) == 0 {
		return total, nil
	}

	prices, ok := o.spotUSDPrices(ctx, needPrice)
	if !ok {
		return total, fmt.Errorf("okx: funding wallet holds %d unpriced currencies", len(needPrice))
	}
	for _, ccy := range needPrice {
		px, priced := prices[ccy]
		if !priced {
			o.noteCashflowWarning("okx_funding_unpriced:" + ccy)
			continue
		}
		total += balances[ccy] * px
	}
	return total, nil
}

// fundingBills reads the funding ledger over the same window as the trading
// one. Both endpoints failing is reported; one failing only shortens the
// window, exactly as the trading ledger treats its archive.
func (o *OKX) fundingBills(ctx context.Context, since, now time.Time) ([]okxBill, error) {
	seen := map[string]bool{}
	var bills []okxBill
	var read int

	for _, endpoint := range []string{"/api/v5/asset/bills", "/api/v5/asset/bills-history"} {
		rows, err := o.fetchAssetBills(ctx, endpoint, since, now, seen)
		if err != nil {
			o.noteCashflowWarning("okx_funding_window_unavailable")
			continue
		}
		read++
		for _, r := range rows {
			ms, _ := strconv.ParseInt(r["ts"], 10, 64)
			balChg, _ := strconv.ParseFloat(r["balChg"], 64)
			bills = append(bills, okxBill{
				BillID: r["billId"],
				T:      time.UnixMilli(ms).UTC(),
				Ccy:    strings.ToUpper(r["ccy"]),
				BalChg: balChg,
				Type:   r["type"],
			})
		}
	}
	if read == 0 {
		return nil, fmt.Errorf("okx: funding ledger unreadable")
	}

	sort.Slice(bills, func(i, j int) bool { return bills[i].T.Before(bills[j].T) })
	return bills, nil
}

// pairInternalTransfers marks each trading type-1 row whose other leg sits in
// the funding ledger. Those two rows are one move between wallets we both
// hold, so neither books. An unpaired trading row kept its old meaning: the
// counterpart is a sub-account or another user, outside either way.
func pairInternalTransfers(trading, funding []okxBill) map[int]bool {
	matched := map[int]bool{}
	used := map[int]bool{}

	for fi, fb := range funding {
		if !fundingInternalTypes[fb.Type] || fb.BalChg == 0 {
			continue
		}
		for ti, tb := range trading {
			if matched[ti] || tb.Type != "1" || tb.Ccy != fb.Ccy {
				continue
			}
			if !sameAmount(tb.BalChg, -fb.BalChg) {
				continue
			}
			dt := tb.T.Sub(fb.T)
			if dt < -okxTransferMatchWindow || dt > okxTransferMatchWindow {
				continue
			}
			matched[ti] = true
			used[fi] = true
			break
		}
	}
	return matched
}

// sameAmount compares the two quantities OKX printed on either side of one
// move. They agree exactly in practice, but a coin with eighteen decimals
// leaves no room for an absolute epsilon alone.
func sameAmount(a, b float64) bool {
	diff := math.Abs(a - b)
	if diff <= 1e-12 {
		return true
	}
	scale := math.Max(math.Abs(a), math.Abs(b))
	return scale > 0 && diff/scale <= 1e-9
}
