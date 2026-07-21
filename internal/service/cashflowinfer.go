package service

import "math"

// Balance-reconciled capital-flow detection.
//
// The settled balance is ground truth: whatever moves it that realized P&L and
// charges do not explain IS a capital movement, whether or not the broker
// surfaced it. IG demo top-ups, for one, move the balance but appear in no
// transaction endpoint (verified) — trusting GetCashflows alone would read them
// as return and let a track record be inflated by depositing. See
// doc/plan-balance-reconciled-cashflow.md.

const (
	// reconThresholdAbs is the account-currency floor below which a residual is
	// noise (rounding, FX conversion), not a flow. Kept low on purpose: a
	// reconciled connector settles to the cent, and the point is to catch small
	// recurring deposits. Connectors whose ledger is not this tight must not be
	// added to balanceReconciledExchanges.
	reconThresholdAbs = 0.50
	// reconThresholdRel scales the floor for large accounts without masking the
	// small-deposit case on ordinary ones (0.005% ≈ 0.50 on a 10k account).
	reconThresholdRel = 0.00005
)

// reconThreshold is the minimum residual treated as a real flow for an account
// of the given equity.
func reconThreshold(equity float64) float64 {
	return math.Max(reconThresholdAbs, reconThresholdRel*math.Abs(equity))
}

// ReconInputs carries one snapshot's cash-affecting figures. All amounts are in
// the account currency; charges and reported flow are signed the way they move
// the settled balance (a charge is negative, a deposit positive).
type ReconInputs struct {
	SettledBalance float64 // RealizedBalance = Equity − UnrealizedPnL
	RealizedPnL    float64 // Σ Trade.RealizedPnL over the window
	Charges        float64 // Σ funding/interest/commission not netted into a deal (signed)
	ReportedFlow   float64 // Σ GetCashflows over the window (signed, + = deposit)
}

// InferCapitalFlow reconciles the change in settled balance against what the
// ledger explains (realized P&L + charges) plus what the broker itself reported
// as cash flow. The unexplained residual is a capital movement the broker did
// not surface. Returns non-negative deposit and withdrawal amounts.
//
// When the broker reports flows accurately the residual is ~0 and the result
// equals ReportedFlow — the pre-existing behaviour, now cross-checked. When it
// hides a flow (IG demo) the residual is that flow, and it is booked as a "gap"
// deposit/withdrawal. Because total = ReportedFlow + residual and residual =
// inferred − ReportedFlow, the reported part is never double-counted.
func InferCapitalFlow(prev, cur ReconInputs, threshold float64) (deposit, withdrawal float64) {
	inferred := cur.SettledBalance - prev.SettledBalance - cur.RealizedPnL - cur.Charges
	residual := inferred - cur.ReportedFlow
	if math.Abs(residual) < threshold {
		residual = 0
	}
	total := cur.ReportedFlow + residual
	if total >= 0 {
		return total, 0
	}
	return 0, -total
}
