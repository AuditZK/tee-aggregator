package connector

import (
	"math"
	"testing"
	"time"
)

func bgAt(s string) time.Time {
	t, err := time.Parse("2006-01-02T15:04:05Z", s)
	if err != nil {
		panic(err)
	}
	return t.UTC()
}

func bgApprox(a, b float64) bool { return math.Abs(a-b) < 1e-9 }

func bgSum(flows []*Cashflow) (dep, wd float64) {
	for _, f := range flows {
		if f.Amount > 0 {
			dep += f.Amount
		} else {
			wd += -f.Amount
		}
	}
	return
}

// A matched spot↔futures transfer pair is one internal move and must produce
// NO cashflow; unmatched legs crossed the perimeter and must. This is the
// funding→spot→futures chain that used to swallow real deposits.
func TestBitgetCashflows_TransferPairing(t *testing.T) {
	spot := []bitgetSpotBill{
		{t: bgAt("2026-07-08T10:00:00Z"), coin: "USDT", size: 21.54, groupType: "transfer"},  // in from funding — UNPAIRED
		{t: bgAt("2026-07-08T10:01:00Z"), coin: "USDT", size: -21.54, groupType: "transfer"}, // out to futures — pairs
	}
	mix := []bitgetMixBill{
		{t: bgAt("2026-07-08T10:01:30Z"), delta: 21.54, businessType: "trans_from_exchange"}, // pairs with leg 2
		{t: bgAt("2026-07-08T15:00:00Z"), delta: 50, businessType: "trans_from_exchange"},    // no spot leg — UNPAIRED
	}
	dep, wd := bgSum(classifyBitgetCashflows(spot, mix, nil))
	if !bgApprox(dep, 21.54+50) || wd != 0 {
		t.Fatalf("got dep=%v wd=%v, want dep=71.54 wd=0", dep, wd)
	}
}

// Perimeter-crossing futures businessTypes (strategy, grants, Bitget's own
// "captital" spelling) book as signed cashflows; trading PnL never does.
func TestBitgetCashflows_FuturesExternalTypes(t *testing.T) {
	mix := []bitgetMixBill{
		{t: bgAt("2026-07-08T10:00:00Z"), delta: 100, businessType: "trans_from_strategy"},
		{t: bgAt("2026-07-08T11:00:00Z"), delta: -40, businessType: "trans_to_otc"},
		{t: bgAt("2026-07-08T12:00:00Z"), delta: 5, businessType: "user_grants_issue"},
		{t: bgAt("2026-07-08T13:00:00Z"), delta: 1, businessType: "risk_captital_user_transfer"}, // prod spelling
		{t: bgAt("2026-07-08T14:00:00Z"), delta: 300, businessType: "close_long"},                // pnl — not cashflow
		{t: bgAt("2026-07-08T15:00:00Z"), delta: -0.2, businessType: "contract_settle_fee"},      // fee — not cashflow
	}
	dep, wd := bgSum(classifyBitgetCashflows(nil, mix, nil))
	if !bgApprox(dep, 106) || !bgApprox(wd, 40) {
		t.Fatalf("got dep=%v wd=%v, want dep=106 wd=40", dep, wd)
	}
}

// Spot groups: on-chain and Earn ("financial") flows use the signed size;
// withdrawals are negative; non-stable coins are valued via the ticker map
// and unpriceable coins contribute nothing (never fabricate a flow).
func TestBitgetCashflows_SpotGroupsAndValuation(t *testing.T) {
	prices := map[string]float64{"ETHUSDT": 3000}
	spot := []bitgetSpotBill{
		{t: bgAt("2026-07-08T10:00:00Z"), coin: "ETH", size: 2, groupType: "deposit"},        // +6000
		{t: bgAt("2026-07-08T11:00:00Z"), coin: "USDT", size: -50, groupType: "financial"},   // Earn subscribe → −50
		{t: bgAt("2026-07-08T12:00:00Z"), coin: "USDT", size: 0.03, groupType: "financial"},  // interest → +0.03
		{t: bgAt("2026-07-08T13:00:00Z"), coin: "USDT", size: -400, groupType: "withdraw"},   // −400
		{t: bgAt("2026-07-08T14:00:00Z"), coin: "SCAM", size: 999, groupType: "deposit"},     // unpriceable → 0
		{t: bgAt("2026-07-08T15:00:00Z"), coin: "USDT", size: 7, groupType: "transaction"},   // trade — not cashflow
	}
	dep, wd := bgSum(classifyBitgetCashflows(spot, nil, prices))
	if !bgApprox(dep, 6000+0.03) || !bgApprox(wd, 450) {
		t.Fatalf("got dep=%v wd=%v, want dep=6000.03 wd=450", dep, wd)
	}
}

// The Bitget connector must satisfy CashflowFetcher — the live daily sync
// only records deposits through this interface.
var _ CashflowFetcher = (*Bitget)(nil)
