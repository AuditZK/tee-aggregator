package service

import (
	"context"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/trackrecord/enclave/internal/signing"
)

// BENCH-01: the benchmark fetch carries X-Internal-Token when a token is
// configured, and omits the header entirely when it isn't (dev/test stacks).
func TestBenchmarkService_InternalTokenHeader(t *testing.T) {
	const body = `{"success":true,"data":{"symbol":"SPY","data":[` +
		`{"date":"2026-01-01","close":100,"adjustedClose":100},` +
		`{"date":"2026-01-02","close":101,"adjustedClose":101}]}}`

	run := func(t *testing.T, token string) (present bool, value string) {
		var saw bool
		var val string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, saw = r.Header["X-Internal-Token"]
			val = r.Header.Get("X-Internal-Token")
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(body))
		}))
		defer srv.Close()

		s := NewBenchmarkService(srv.URL, token)
		if _, err := s.DailyReturnsByDate(context.Background(), "SPY", time.Now().AddDate(0, -1, 0), time.Now()); err != nil {
			t.Fatalf("DailyReturnsByDate: %v", err)
		}
		return saw, val
	}

	t.Run("token sent when configured", func(t *testing.T) {
		const tok = "bench-secret-token-abcdef123456"
		present, value := run(t, tok)
		if !present || value != tok {
			t.Fatalf("X-Internal-Token: present=%v value=%q, want the configured token", present, value)
		}
	})

	t.Run("header omitted when no token", func(t *testing.T) {
		present, _ := run(t, "")
		if present {
			t.Fatalf("X-Internal-Token must be absent when no token is configured")
		}
	})
}

// The enclave annualises on 365 calendar days everywhere — snapshots are taken
// 7/7, so every observation is a calendar day. Alpha, tracking error and the
// information ratio used to scale by a 252-day equity-market year while the
// volatility and Sharpe printed beside them scaled by 365, so the two halves
// of a Pro report were not on the same scale.
//
// Every expected number below is computed by hand from the series, not read
// back from the implementation.

// Hand computation:
//
//	B    = [2%, 0, 2%, 0, 2%, 0]            mean(B) = 1%, deviations ±1%
//	P    = [2%, 0, 2%, 0, 0.5%, 1.5%]       mean(P) = 6%/6 = 1%
//	dP·dB summed = 4×(1e-4) − 2×(0.5e-4) = 3e-4  →  Cov = 3e-4/6 = 5e-5
//	dB² summed   = 6×1e-4 = 6e-4                 →  Var(B) = 1e-4
//	beta  = 5e-5 / 1e-4 = 0.5
//	alpha = mean(P)·365 − beta·mean(B)·365 = 3.65 − 0.5×3.65 = 1.825
//	        (on a 252-day year the same series reads 2.52 − 1.26 = 1.26)
func TestBenchmark_AlphaAnnualisesOnCalendarDays(t *testing.T) {
	portfolio := []float64{0.02, 0.00, 0.02, 0.00, 0.005, 0.015}
	bench := []float64{0.02, 0.00, 0.02, 0.00, 0.02, 0.00}

	m, err := NewBenchmarkService("", "").CalculateFromSeries(portfolio, bench, "SPY")
	if err != nil {
		t.Fatalf("CalculateFromSeries: %v", err)
	}

	if math.Abs(m.Beta-0.5) > 1e-12 {
		t.Fatalf("beta = %v, want 0.5", m.Beta)
	}
	if math.Abs(m.Alpha-1.825) > 1e-12 {
		t.Fatalf("alpha = %v, want 1.825 (mean 1%%/day over a 365-day year, beta 0.5)", m.Alpha)
	}
	if math.Abs(m.Alpha-1.26) < 1e-9 {
		t.Fatal("alpha is still annualised on a 252-day trading year")
	}
}

// Hand computation:
//
//	P      = [1%, −1%, 1%, −1%, 1%, −1%]   B = 0.5% every day
//	excess = [0.5%, −1.5%, 0.5%, −1.5%, 0.5%, −1.5%]
//	mean(excess) = −3%/6 = −0.5%
//	deviations   = ±1% → Σd² = 6×1e-4 = 6e-4 → sample variance = 6e-4/5 = 1.2e-4
//	stddev = √1.2e-4 = 0.010954451150103323
//	TE  = stddev·√365 = √(1.2e-4 × 365) = √0.0438 = 0.2092844953645635
//	      (on a 252-day year: √(1.2e-4 × 252) = 0.17389652095427327)
//	IR  = mean(excess)/stddev · √365 = −0.005/0.010954451… × 19.104973…
//	    = −8.720187306856813   (on a 252-day year: −7.24568837309472)
func TestBenchmark_TrackingErrorAndInformationRatioAnnualiseOnCalendarDays(t *testing.T) {
	portfolio := []float64{0.01, -0.01, 0.01, -0.01, 0.01, -0.01}
	bench := []float64{0.005, 0.005, 0.005, 0.005, 0.005, 0.005}

	m, err := NewBenchmarkService("", "").CalculateFromSeries(portfolio, bench, "SPY")
	if err != nil {
		t.Fatalf("CalculateFromSeries: %v", err)
	}

	const (
		handStdDev = 0.010954451150103323 // √(1.2e-4)
		wantTE     = 0.2092844953645635   // √(1.2e-4 × 365)
		te252      = 0.17389652095427327  // the defect this test exists for
		wantIR     = -8.720187306856813
		ir252      = -7.24568837309472
	)

	// The relation the whole fix is about: annualising a daily standard
	// deviation multiplies it by √365, never √252.
	if math.Abs(m.TrackingError-handStdDev*math.Sqrt(365)) > 1e-12 {
		t.Fatalf("tracking error = %v, want stddev·√365 = %v", m.TrackingError, handStdDev*math.Sqrt(365))
	}
	if math.Abs(m.TrackingError-wantTE) > 1e-12 {
		t.Fatalf("tracking error = %v, want %v", m.TrackingError, wantTE)
	}
	if math.Abs(m.TrackingError-te252) < 1e-9 {
		t.Fatal("tracking error is still annualised on a 252-day trading year")
	}

	if math.Abs(m.InformationRatio-wantIR) > 1e-9 {
		t.Fatalf("information ratio = %v, want %v", m.InformationRatio, wantIR)
	}
	if math.Abs(m.InformationRatio-ir252) < 1e-6 {
		t.Fatal("information ratio is still annualised on a 252-day trading year")
	}
}

// The benchmark comparisons and the core metrics must share one year length,
// or a report prints a Sharpe on one basis and an alpha on another. This is
// the invariant, stated once: the package annualises on signing's declared
// basis and nothing else.
func TestBenchmark_SharesTheSingleAnnualizationBasis(t *testing.T) {
	if daysPerYear != float64(signing.AnnualizationDays) {
		t.Fatalf("daysPerYear = %v, want signing.AnnualizationDays = %d", daysPerYear, signing.AnnualizationDays)
	}
	if signing.AnnualizationDays != 365 {
		t.Fatalf("annualization basis = %d, want 365 calendar days", signing.AnnualizationDays)
	}

	excess := []float64{0.004, -0.002, 0.006, -0.001, 0.003, 0.000}
	bench := make([]float64, len(excess))
	m, err := NewBenchmarkService("", "").CalculateFromSeries(excess, bench, "SPY")
	if err != nil {
		t.Fatalf("CalculateFromSeries: %v", err)
	}
	// bench is flat zero, so the excess series IS the portfolio series.
	if want := stddev(excess) * math.Sqrt(daysPerYear); math.Abs(m.TrackingError-want) > 1e-12 {
		t.Fatalf("tracking error = %v, want stddev·√daysPerYear = %v", m.TrackingError, want)
	}
}
