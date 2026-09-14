package connector

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
)

// The bug these fixtures pin down: GetBalance summed details[].availBal, which
// OKX documents as "available balance of currency" — the balance not locked by
// ORDERS. In every account mode above spot it does not deduct the margin held
// by open cross positions, so it sums to the account's whole equity. Between
// 2026-08-31 and 2026-09-14 every live OKX snapshot carried free margin exactly
// equal to equity, including an account (77b398e9 / RAVCA_UW, 2026-09-11) whose
// short options straddle and perpetual hedge were showing an unrealized loss at
// the time — an account that cannot, by construction, have all of its equity
// free.
//
// The margin-aware fields, per the v5 docs' "distribution of applicable fields
// under each account level" table:
//
//	acctLv 1 spot       -> sum(details.availBal)   (correct there: no cross position)
//	acctLv 2 futures    -> sum(details.availEq)
//	acctLv 3 multi-ccy  -> account adjEq - account imr, account upl
//	acctLv 4 portfolio  -> account adjEq - account imr, account upl
func TestOKXGetBalance_MarginAwareFieldsPerAccountMode(t *testing.T) {
	cases := []struct {
		name string
		body string

		wantEquity     float64
		wantAvailable  float64
		wantUnrealized float64
		wantBasis      string

		// freeMustBeBelowEquity marks the fixtures that carry open cross
		// positions. On those, free == equity is the defect itself.
		freeMustBeBelowEquity bool
	}{
		{
			// acctLv 1. adjEq and imr exist here too (spot borrowing), and they
			// must NOT be used: adjEq is the haircut-discounted collateral
			// value, not free cash. An unencumbered spot account has all of its
			// equity free and saying so is right.
			name: "acctLv 1 spot: availBal is the free margin and free == equity is correct",
			body: `{"code":"0","data":[{` +
				`"totalEq":"50000","isoEq":"","adjEq":"48500","availEq":"","ordFroz":"0",` +
				`"imr":"0","mmr":"0","mgnRatio":"","upl":"","details":[` +
				`{"ccy":"USDT","eq":"10000","eqUsd":"10000","cashBal":"10000","availBal":"10000","availEq":"","frozenBal":"0","isoEq":"","upl":"","imr":"","mmr":""},` +
				`{"ccy":"BTC","eq":"0.4","eqUsd":"40000","cashBal":"0.4","availBal":"0.4","availEq":"","frozenBal":"0","isoEq":"","upl":"","imr":"","mmr":""}` +
				`]}]}`,
			wantEquity:     50000,
			wantAvailable:  50000,
			wantUnrealized: 0,
			wantBasis:      okxBasisSumAvailBal,
		},
		{
			// acctLv 2. The account-level USD trio is blank in this mode; the
			// margin-aware figure is per currency. availBal still reads 19 000
			// against 8 000 of initial margin held by the position.
			name: "acctLv 2 futures: per-currency availEq, not availBal",
			body: `{"code":"0","data":[{` +
				`"totalEq":"20000","isoEq":"0","adjEq":"","availEq":"","ordFroz":"",` +
				`"imr":"","mmr":"","mgnRatio":"","upl":"","details":[` +
				`{"ccy":"USDT","eq":"20000","eqUsd":"20000","cashBal":"19000","availBal":"19000","availEq":"12000","frozenBal":"0","isoEq":"0","upl":"1000","imr":"8000","mmr":"1600"}` +
				`]}]}`,
			wantEquity:            20000,
			wantAvailable:         12000,
			wantUnrealized:        1000,
			wantBasis:             okxBasisSumAvailEq,
			freeMustBeBelowEquity: true,
		},
		{
			// acctLv 3. Account-level adjEq/imr/upl are all in USD already.
			// availBal sums to the entire equity, which is the production
			// symptom; the account-level pair says 39 540 of 42 832 is free.
			name: "acctLv 3 multi-currency margin: adjEq minus imr",
			body: `{"code":"0","data":[{` +
				`"totalEq":"42832.11","isoEq":"0","adjEq":"42100.55","availEq":"39560.00","ordFroz":"0",` +
				`"imr":"2560.55","mmr":"512.11","mgnRatio":"82.13","upl":"-84.20","details":[` +
				`{"ccy":"USDT","eq":"42832.11","eqUsd":"42832.11","cashBal":"42916.31","availBal":"42832.11","availEq":"40271.56","frozenBal":"0","isoEq":"0","upl":"-84.20","imr":"","mmr":""}` +
				`]}]}`,
			wantEquity:            42832.11,
			wantAvailable:         42100.55 - 2560.55,
			wantUnrealized:        -84.20,
			wantBasis:             okxBasisAdjEqMinusIMR,
			freeMustBeBelowEquity: true,
		},
		{
			// acctLv 4, shaped like the account that exposed this: equity
			// mostly in a non-USDT coin, availBal equal to the balance on every
			// line, per-currency upl flat at 0 while the account-level upl
			// carries the cross-margin loss, imr > 0 from the open straddle.
			// Summing availBal here returns 22 077.42 — the equity, to the
			// cent — and summing per-currency upl returns 0.
			name: "acctLv 4 portfolio margin: the customer's shape must not return free == equity",
			body: `{"code":"0","data":[{` +
				`"totalEq":"22077.42","isoEq":"0","adjEq":"21880.10","availEq":"20950.33","ordFroz":"0",` +
				`"imr":"1214.88","mmr":"402.51","mgnRatio":"54.36","upl":"-97.34","details":[` +
				`{"ccy":"BTC","eq":"0.1801","eqUsd":"19056.32","cashBal":"0.1801","availBal":"0.1801","availEq":"0.1801","frozenBal":"0","isoEq":"0","upl":"0","imr":"","mmr":""},` +
				`{"ccy":"USDC","eq":"3021.10","eqUsd":"3021.10","cashBal":"3021.10","availBal":"3021.10","availEq":"3021.10","frozenBal":"0","isoEq":"0","upl":"0","imr":"","mmr":""}` +
				`]}]}`,
			wantEquity:            22077.42,
			wantAvailable:         21880.10 - 1214.88,
			wantUnrealized:        -97.34,
			wantBasis:             okxBasisAdjEqMinusIMR,
			freeMustBeBelowEquity: true,
		},
		{
			// An account past its initial-margin budget has no margin free.
			// Reporting the negative difference would put a negative free
			// margin on the dashboard and, once clamped, an arbitrary one.
			name: "imr above adjEq leaves no margin free, not a negative amount of it",
			body: `{"code":"0","data":[{` +
				`"totalEq":"9000","isoEq":"0","adjEq":"8800","availEq":"0","ordFroz":"0",` +
				`"imr":"9400","mmr":"8100","mgnRatio":"1.08","upl":"-1900","details":[` +
				`{"ccy":"USDT","eq":"9000","eqUsd":"9000","cashBal":"10900","availBal":"9000","availEq":"0","frozenBal":"0","isoEq":"0","upl":"-1900","imr":"","mmr":""}` +
				`]}]}`,
			wantEquity:            9000,
			wantAvailable:         0,
			wantUnrealized:        -1900,
			wantBasis:             okxBasisAdjEqMinusIMR,
			freeMustBeBelowEquity: true,
		},
		{
			// Degraded multi-currency payload: the account-level upl is absent,
			// so the account-level branch is not taken on trust. The
			// per-currency margin-aware figure is still better than availBal.
			name: "account-level trio incomplete falls back to per-currency availEq",
			body: `{"code":"0","data":[{` +
				`"totalEq":"15000","isoEq":"0","adjEq":"14800","availEq":"","ordFroz":"0",` +
				`"imr":"3000","mmr":"600","mgnRatio":"24.6","upl":"","details":[` +
				`{"ccy":"USDT","eq":"15000","eqUsd":"15000","cashBal":"14800","availBal":"15000","availEq":"11800","frozenBal":"0","isoEq":"0","upl":"200","imr":"","mmr":""}` +
				`]}]}`,
			wantEquity:            15000,
			wantAvailable:         11800,
			wantUnrealized:        200,
			wantBasis:             okxBasisSumAvailEq,
			freeMustBeBelowEquity: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			okx := newOKXAcross(newOKXFakeRegion(t, http.StatusOK, tc.body))

			balance, err := okx.GetBalance(context.Background())
			if err != nil {
				t.Fatalf("GetBalance: %v", err)
			}
			assertWithin(t, "equity", balance.Equity, tc.wantEquity, 1e-6)
			assertWithin(t, "available", balance.Available, tc.wantAvailable, 1e-4)
			assertWithin(t, "unrealized", balance.UnrealizedPnL, tc.wantUnrealized, 1e-4)

			if tc.freeMustBeBelowEquity && balance.Available >= balance.Equity {
				t.Fatalf("free margin %v is the whole equity %v on an account with open cross positions",
					balance.Available, balance.Equity)
			}

			var account okxAccountBalance
			var resp okxBalanceResponse
			if err := json.Unmarshal([]byte(tc.body), &resp); err != nil {
				t.Fatalf("fixture does not parse: %v", err)
			}
			account = resp.Data[0]
			if got := okxReadMargin(account).Basis; got != tc.wantBasis {
				t.Fatalf("margin basis = %q, want %q", got, tc.wantBasis)
			}
		})
	}
}

// Equity is totalEq in every mode. adjEq is a collateral valuation — totalEq
// minus haircuts — and letting it become the equity would move the curve of
// every OKX account holding a non-stablecoin.
func TestOKXGetBalance_EquityStaysTotalEqNotAdjEq(t *testing.T) {
	body := `{"code":"0","data":[{` +
		`"totalEq":"31500","adjEq":"29100","availEq":"26000","imr":"3100","mmr":"620","upl":"41.5","details":[` +
		`{"ccy":"BTC","eq":"0.3","eqUsd":"31500","cashBal":"0.3","availBal":"0.3","availEq":"0.25","upl":"0"}` +
		`]}]}`
	okx := newOKXAcross(newOKXFakeRegion(t, http.StatusOK, body))

	balance, err := okx.GetBalance(context.Background())
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	assertNear(t, "equity", balance.Equity, 31500)
	assertNear(t, "available", balance.Available, 29100-3100)
	assertNear(t, "unrealized", balance.UnrealizedPnL, 41.5)
}

// okxBodyPortfolioMargin is the customer's account shape: portfolio margin,
// equity mostly in a non-USDT coin, availBal equal to the balance on every
// line.
const okxBodyPortfolioMargin = `{"code":"0","data":[{` +
	`"totalEq":"22077.42","isoEq":"0","adjEq":"21880.10","availEq":"20950.33","ordFroz":"0",` +
	`"imr":"1214.88","mmr":"402.51","mgnRatio":"54.36","upl":"-97.34","details":[` +
	`{"ccy":"BTC","eq":"0.1801","eqUsd":"19056.32","cashBal":"0.1801","availBal":"0.1801","availEq":"0.1801","frozenBal":"0","isoEq":"0","upl":"0","imr":"","mmr":""},` +
	`{"ccy":"USDC","eq":"3021.10","eqUsd":"3021.10","cashBal":"3021.10","availBal":"3021.10","availEq":"3021.10","frozenBal":"0","isoEq":"0","upl":"0","imr":"","mmr":""}` +
	`]}]}`

// GetBalance must stay a single request. The mode is readable off the fields
// OKX left empty, so paying for /account/config on every sync of every OKX
// account would buy nothing.
func TestOKXGetBalance_SpendsOneRequest(t *testing.T) {
	region := newOKXFakeRegion(t, http.StatusOK, okxBodyPortfolioMargin)
	okx := newOKXAcross(region)

	if _, err := okx.GetBalance(context.Background()); err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if got := region.requests(); got != 1 {
		t.Fatalf("GetBalance made %d requests, want 1", got)
	}
}
