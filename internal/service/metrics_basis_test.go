package service

import (
	"math"
	"testing"
	"time"

	"github.com/trackrecord/enclave/internal/repository"
)

func equitySeries(start time.Time, dailyReturns []float64) []*repository.Snapshot {
	equity := 10000.0
	out := []*repository.Snapshot{{Exchange: "okx", Timestamp: start, TotalEquity: equity}}
	for i, r := range dailyReturns {
		equity *= 1 + r
		out = append(out, &repository.Snapshot{
			Exchange:    "okx",
			Timestamp:   start.AddDate(0, 0, i+1),
			TotalEquity: equity,
		})
	}
	return out
}

func alternating(n int, up, down float64) []float64 {
	out := make([]float64, n)
	for i := range out {
		if i%2 == 0 {
			out[i] = up
		} else {
			out[i] = down
		}
	}
	return out
}

// The number that prompted this: a 42-day, +14.9 % record signed with an
// annualised return of +234.7 %. Under one year nothing is annualised and
// the report says so.
func TestMetrics_NothingIsAnnualisedUnderOneYear(t *testing.T) {
	m, err := NewMetricsService(nil).CalculateFromSnapshots(equitySeries(day(2026, time.July, 21), alternating(42, 0.01, -0.004)), 0)
	if err != nil {
		t.Fatal(err)
	}
	if m.Annualized {
		t.Fatal("a 42-day record was annualised")
	}
	if m.AnnualizedReturn != 0 || m.CalmarRatio != 0 {
		t.Fatalf("annualized=%v calmar=%v, want both zero by rule", m.AnnualizedReturn, m.CalmarRatio)
	}
	if m.PeriodDays != 42 {
		t.Fatalf("period_days = %d, want 42", m.PeriodDays)
	}
	if m.TotalReturn <= 0 || math.IsNaN(m.SharpeRatio) || m.SortinoRatio <= 0 {
		t.Fatalf("total=%v sharpe=%v sortino=%v: the ratios that need no yearly return must still exist", m.TotalReturn, m.SharpeRatio, m.SortinoRatio)
	}
}

// Past a year the return compounds over elapsed calendar days — 400 days of
// history are 400 days, whatever the row count.
func TestMetrics_AnnualisedOnElapsedCalendarDays(t *testing.T) {
	m, err := NewMetricsService(nil).CalculateFromSnapshots(equitySeries(day(2025, time.July, 1), alternating(400, 0.003, -0.001)), 0)
	if err != nil {
		t.Fatal(err)
	}
	if !m.Annualized || m.PeriodDays != 400 {
		t.Fatalf("annualized=%v period_days=%d, want true/400", m.Annualized, m.PeriodDays)
	}
	want := math.Pow(1+m.TotalReturn, 365.0/400) - 1
	if math.Abs(m.AnnualizedReturn-want) > 1e-12 {
		t.Fatalf("annualized = %v, want %v", m.AnnualizedReturn, want)
	}
	if m.CalmarRatio <= 0 {
		t.Fatalf("calmar = %v, want the annualised return over the max drawdown", m.CalmarRatio)
	}
}

// Volatility, Sharpe and Sortino scale by √365: every observation is a
// calendar day, and the analytics service uses the same basis.
func TestMetrics_RatiosOnA365DayBasis(t *testing.T) {
	returns := alternating(30, 0.02, -0.01)
	m, err := NewMetricsService(nil).CalculateFromSnapshots(equitySeries(day(2026, time.August, 1), returns), 0)
	if err != nil {
		t.Fatal(err)
	}
	sd := stddev(returns)
	if math.Abs(m.Volatility-sd*math.Sqrt(365)) > 1e-9 {
		t.Fatalf("volatility = %v, want %v", m.Volatility, sd*math.Sqrt(365))
	}
	wantSharpe := mean(returns) / sd * math.Sqrt(365)
	if math.Abs(m.SharpeRatio-wantSharpe) > 1e-9 {
		t.Fatalf("sharpe = %v, want %v", m.SharpeRatio, wantSharpe)
	}
}

// The downside deviation averages the squared shortfalls over every
// observation. Over the losing days alone, a steady -1 %/day scored the same
// downside risk as a single -1 % day.
func TestMetrics_DownsideDeviationIsAFullSampleSemiDeviation(t *testing.T) {
	s := NewMetricsService(nil)
	steady := s.downsideDeviation([]float64{-0.01, -0.01, -0.01, -0.01}, 0)
	if math.Abs(steady-0.01) > 1e-12 {
		t.Fatalf("steady loss: %v, want 0.01", steady)
	}
	mixed := s.downsideDeviation([]float64{-0.01, 0.02, 0.02, 0.02}, 0)
	if math.Abs(mixed-0.005) > 1e-12 {
		t.Fatalf("one losing day in four: %v, want sqrt(0.0001/4) = 0.005", mixed)
	}
}
