package service

import (
	"math"
	"testing"
)

func TestInferCapitalFlow(t *testing.T) {
	const thr = 0.50
	cases := []struct {
		name           string
		prev, cur      ReconInputs
		wantDeposit    float64
		wantWithdrawal float64
	}{
		{
			// The IG-demo case: balance jumps 500 with nothing in the ledger and
			// no reported flow. Must surface as a deposit, not read as return.
			name:        "hidden deposit",
			prev:        ReconInputs{SettledBalance: 9970.59},
			cur:         ReconInputs{SettledBalance: 10470.59},
			wantDeposit: 500,
		},
		{
			// A day of pure trading + funding, no capital. Everything the balance
			// did is explained → no flow.
			name: "explained pnl and charges, no flow",
			prev: ReconInputs{SettledBalance: 10000},
			cur:  ReconInputs{SettledBalance: 9970.59, RealizedPnL: -0.09, Charges: -29.32},
		},
		{
			// Broker reports the deposit and the balance agrees → residual ~0,
			// result equals the reported flow (legacy behaviour preserved).
			name:        "reported deposit agrees",
			prev:        ReconInputs{SettledBalance: 10000},
			cur:         ReconInputs{SettledBalance: 10500, ReportedFlow: 500},
			wantDeposit: 500,
		},
		{
			// Broker reports 100 but the balance moved 150 unexplained: 50 hidden
			// on top of the reported 100. Full flow captured, reported not doubled.
			name:        "reported plus hidden gap",
			prev:        ReconInputs{SettledBalance: 10000},
			cur:         ReconInputs{SettledBalance: 10150, ReportedFlow: 100},
			wantDeposit: 150,
		},
		{
			name:           "hidden withdrawal",
			prev:           ReconInputs{SettledBalance: 10000},
			cur:            ReconInputs{SettledBalance: 9700},
			wantWithdrawal: 300,
		},
		{
			// A sub-threshold residual (FX/rounding) is noise, not a flow.
			name: "noise below threshold",
			prev: ReconInputs{SettledBalance: 10000},
			cur:  ReconInputs{SettledBalance: 10000.30},
		},
		{
			// Realized gain with no capital: the balance rose by exactly the P&L.
			name: "realized gain only",
			prev: ReconInputs{SettledBalance: 10000},
			cur:  ReconInputs{SettledBalance: 10250, RealizedPnL: 250},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dep, wd := InferCapitalFlow(c.prev, c.cur, thr)
			if math.Abs(dep-c.wantDeposit) > 1e-6 {
				t.Errorf("deposit = %v, want %v", dep, c.wantDeposit)
			}
			if math.Abs(wd-c.wantWithdrawal) > 1e-6 {
				t.Errorf("withdrawal = %v, want %v", wd, c.wantWithdrawal)
			}
		})
	}
}

// A connector that under-reports a charge makes the missing cost look like a
// withdrawal — which is why eligibility is gated on ledger completeness. This
// documents the failure the gate exists to prevent.
func TestInferCapitalFlow_UnderreportedChargeLooksLikeWithdrawal(t *testing.T) {
	// Balance dropped 5 from a funding charge the connector failed to report.
	prev := ReconInputs{SettledBalance: 10000}
	cur := ReconInputs{SettledBalance: 9995} // Charges should be -5 but is 0
	dep, wd := InferCapitalFlow(prev, cur, 0.50)
	if dep != 0 || math.Abs(wd-5) > 1e-6 {
		t.Fatalf("got deposit=%v withdrawal=%v; want a phantom 5 withdrawal (the reason for the completeness gate)", dep, wd)
	}
}

func TestReconThreshold(t *testing.T) {
	if got := reconThreshold(10000); math.Abs(got-0.50) > 1e-9 {
		t.Errorf("threshold(10k) = %v, want 0.50 (abs floor dominates)", got)
	}
	if got := reconThreshold(1_000_000); math.Abs(got-50) > 1e-9 {
		t.Errorf("threshold(1M) = %v, want 50 (rel dominates)", got)
	}
}
