package service

import (
	"context"
	"errors"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/trackrecord/enclave/internal/repository"
	"github.com/trackrecord/enclave/internal/signing"
)

// PerformanceMetrics holds calculated performance metrics
type PerformanceMetrics struct {
	// Risk-adjusted returns
	SharpeRatio  float64 `json:"sharpe_ratio"`
	SortinoRatio float64 `json:"sortino_ratio"`
	CalmarRatio  float64 `json:"calmar_ratio"`

	// Volatility
	Volatility        float64 `json:"volatility"`         // Annualized
	DownsideDeviation float64 `json:"downside_deviation"` // Annualized

	// Drawdown
	MaxDrawdown         float64 `json:"max_drawdown"`          // Percentage
	MaxDrawdownDuration int     `json:"max_drawdown_duration"` // Days
	CurrentDrawdown     float64 `json:"current_drawdown"`

	// Win/Loss
	WinRate      float64 `json:"win_rate"`      // Percentage of positive days
	ProfitFactor float64 `json:"profit_factor"` // Gross profit / gross loss
	AvgWin       float64 `json:"avg_win"`
	AvgLoss      float64 `json:"avg_loss"`

	// Period info
	TotalReturn      float64 `json:"total_return"`
	AnnualizedReturn float64 `json:"annualized_return"`
	// Annualized reports whether AnnualizedReturn and CalmarRatio were
	// computed at all: both stay zero under one year of history, where
	// annualising would be a forecast (GIPS 2020, 2.A.32), and a reader must
	// be able to tell that from a flat year.
	Annualized  bool      `json:"annualized"`
	PeriodDays  int       `json:"period_days"`
	PeriodStart time.Time `json:"period_start"`
	PeriodEnd   time.Time `json:"period_end"`
	DataPoints  int       `json:"data_points"`
}

// Every daily figure here is a calendar-day observation (snapshots are taken
// 7/7), so a year is 365 of them — the same basis as the analytics service,
// or the signed report and the dashboard would disagree on every ratio.
//
// There is exactly one definition of that year length in the enclave,
// signing.AnnualizationDays, and the signed report declares it to its reader
// as annualization_days. Everything that annualises — this file and the
// benchmark comparisons in benchmark.go — scales by it, so the report can
// never print a Sharpe on one year and an alpha on another.
const daysPerYear = float64(signing.AnnualizationDays)

// MetricsService calculates performance metrics from snapshots
type MetricsService struct {
	snapshotRepo *repository.SnapshotRepo
}

// NewMetricsService creates a new metrics service
func NewMetricsService(snapshotRepo *repository.SnapshotRepo) *MetricsService {
	return &MetricsService{snapshotRepo: snapshotRepo}
}

// Calculate computes performance metrics for a user within a date range
func (s *MetricsService) Calculate(ctx context.Context, userUID string, start, end time.Time) (*PerformanceMetrics, error) {
	return s.CalculateWithFilters(ctx, userUID, start, end, "", nil)
}

// CalculateWithExcludedExchanges computes metrics while excluding configured exchanges.
func (s *MetricsService) CalculateWithExcludedExchanges(
	ctx context.Context,
	userUID string,
	start, end time.Time,
	excludedConnectionKeys map[string]struct{},
) (*PerformanceMetrics, error) {
	return s.CalculateWithFilters(ctx, userUID, start, end, "", excludedConnectionKeys)
}

// CalculateWithFilters computes metrics with optional exchange inclusion + exclusion filters.
func (s *MetricsService) CalculateWithFilters(
	ctx context.Context,
	userUID string,
	start, end time.Time,
	exchange string,
	excludedConnectionKeys map[string]struct{},
) (*PerformanceMetrics, error) {
	snapshots, err := s.snapshotRepo.GetByUserAndDateRange(ctx, userUID, start, end)
	if err != nil {
		return nil, err
	}

	filtered := filterSnapshots(snapshots, exchange, excludedConnectionKeys)
	return s.calculateFromSnapshots(filtered, 0)
}

// CalculateFromSnapshots computes metrics from an already-fetched snapshot
// slice. The report path uses this so it computes metrics over the SAME
// verifiable-only set it built the report from (SEC-001) — re-fetching via
// CalculateWithFilters would silently re-include external-rebuilder history.
// The caller is responsible for any exchange-exclusion filtering.
// riskFreeRatePct is the annual risk-free rate in percent (e.g. 2.5);
// zero keeps the legacy rf-free Sharpe/Sortino.
func (s *MetricsService) CalculateFromSnapshots(snapshots []*repository.Snapshot, riskFreeRatePct float64) (*PerformanceMetrics, error) {
	return s.calculateFromSnapshots(snapshots, riskFreeRatePct)
}

func (s *MetricsService) calculateFromSnapshots(snapshots []*repository.Snapshot, riskFreeRatePct float64) (*PerformanceMetrics, error) {
	if len(snapshots) < 2 {
		return nil, errors.New("insufficient data: need at least 2 snapshots")
	}

	// Sort by timestamp
	sort.Slice(snapshots, func(i, j int) bool {
		return snapshots[i].Timestamp.Before(snapshots[j].Timestamp)
	})

	// Use report-aligned TWR conversion to handle multi-exchange snapshots.
	snapshots = dropLeadingDustDays(snapshots)
	dailyReturns := convertSnapshotsToDailyReturns(snapshots)
	if len(dailyReturns) == 0 {
		return nil, errors.New("insufficient data: need at least 2 daily data points")
	}

	returns := make([]float64, 0, len(dailyReturns))
	navSeries := make([]float64, 1, len(dailyReturns)+1)
	navSeries[0] = 1
	for _, dr := range dailyReturns {
		returns = append(returns, dr.netReturn)
		navSeries = append(navSeries, dr.nav)
	}

	if len(returns) == 0 {
		return nil, errors.New("no valid returns calculated")
	}

	// Returns here are decimals, so the annual percent rate is converted and
	// spread over calendar days; rf=0 reproduces the rf-free ratios.
	rfDaily := riskFreeRatePct / 100 / daysPerYear

	avgReturn := mean(returns)
	stdDev := stddev(returns)
	downsideDev := s.downsideDeviation(returns, rfDaily)
	annualizedVol := stdDev * math.Sqrt(daysPerYear)
	annualizedDownside := downsideDev * math.Sqrt(daysPerYear)

	// Drawdown analysis on normalized NAV series.
	maxDD, maxDDDuration, currentDD := s.analyzeDrawdownNAV(navSeries)

	// Win/Loss analysis
	winRate, profitFactor, avgWin, avgLoss := s.analyzeWinLoss(returns)

	totalReturn := dailyReturns[len(dailyReturns)-1].cumulativeReturn
	periodStart, periodEnd, dataPoints := summarizePeriod(snapshots)
	periodDays := elapsedDays(periodStart, periodEnd)
	annualizedReturn, annualized := annualizeReturn(totalReturn, periodDays)

	// Sharpe and Sortino come from the daily sample scaled by √365, so they
	// exist for any record with a dispersion; Calmar needs a yearly return
	// and is left at zero with it.
	sharpe := 0.0
	if stdDev > 0 {
		sharpe = (avgReturn - rfDaily) / stdDev * math.Sqrt(daysPerYear)
	}

	sortino := 0.0
	if downsideDev > 0 {
		sortino = (avgReturn - rfDaily) / downsideDev * math.Sqrt(daysPerYear)
	}

	calmar := 0.0
	if annualized && maxDD > 0 {
		calmar = annualizedReturn / maxDD
	}

	return &PerformanceMetrics{
		SharpeRatio:         sharpe,
		SortinoRatio:        sortino,
		CalmarRatio:         calmar,
		Volatility:          annualizedVol,
		DownsideDeviation:   annualizedDownside,
		MaxDrawdown:         maxDD,
		MaxDrawdownDuration: int(maxDDDuration),
		CurrentDrawdown:     currentDD,
		WinRate:             winRate,
		ProfitFactor:        profitFactor,
		AvgWin:              avgWin,
		AvgLoss:             avgLoss,
		TotalReturn:         totalReturn,
		AnnualizedReturn:    annualizedReturn,
		Annualized:          annualized,
		PeriodDays:          periodDays,
		PeriodStart:         periodStart,
		PeriodEnd:           periodEnd,
		DataPoints:          dataPoints,
	}, nil
}

func filterSnapshots(snapshots []*repository.Snapshot, exchange string, excludedConnectionKeys map[string]struct{}) []*repository.Snapshot {
	normalizedExchange := strings.ToLower(exchange)
	filtered := make([]*repository.Snapshot, 0, len(snapshots))
	for _, snap := range snapshots {
		snapshotExchange := strings.ToLower(snap.Exchange)
		if normalizedExchange != "" && snapshotExchange != normalizedExchange {
			continue
		}
		if _, excluded := excludedConnectionKeys[snapshotConnectionKey(snap.Exchange, snap.Label)]; excluded {
			continue
		}
		if _, excluded := excludedConnectionKeys[snapshotExchange]; excluded {
			continue
		}
		filtered = append(filtered, snap)
	}
	return filtered
}

func filterSnapshotsByExcludedExchanges(snapshots []*repository.Snapshot, excludedConnectionKeys map[string]struct{}) []*repository.Snapshot {
	return filterSnapshots(snapshots, "", excludedConnectionKeys)
}

// downsideDeviation is the root mean square of the shortfalls below target
// over EVERY observation (Sortino & van der Meer, 1991). Averaging over the
// losing days alone — the previous denominator — made a steady daily loss
// read as no more downside risk than an occasional one.
func (s *MetricsService) downsideDeviation(returns []float64, target float64) float64 {
	if len(returns) == 0 {
		return 0
	}
	var sumSquares float64
	for _, r := range returns {
		if r < target {
			diff := r - target
			sumSquares += diff * diff
		}
	}
	return math.Sqrt(sumSquares / float64(len(returns)))
}

func elapsedDays(start, end time.Time) int {
	days := int(math.Round(end.Sub(start).Hours() / 24))
	if days < 1 {
		return 1
	}
	return days
}

// annualizeReturn compounds a period return over elapsed calendar days.
// Under one year it declines: the result would be an extrapolation, not a
// measurement, and a +5 % week reading +1 164 %/yr is exactly the number a
// signed report must never carry.
func annualizeReturn(total float64, days int) (float64, bool) {
	if float64(days) < daysPerYear {
		return 0, false
	}
	growth := 1 + total
	if growth <= 0 {
		return 0, false
	}
	annual := math.Pow(growth, daysPerYear/float64(days)) - 1
	if math.IsNaN(annual) || math.IsInf(annual, 0) {
		return 0, false
	}
	return annual, true
}

func (s *MetricsService) analyzeDrawdownNAV(navSeries []float64) (maxDD float64, maxDDDuration int, currentDD float64) {
	if len(navSeries) == 0 {
		return 0, 0, 0
	}

	peak := navSeries[0]
	peakIdx := 0

	for i, nav := range navSeries {
		if nav > peak {
			peak = nav
			peakIdx = i
		}

		dd := (peak - nav) / peak
		if dd > maxDD {
			maxDD = dd
			maxDDDuration = i - peakIdx
		}
	}

	lastNAV := navSeries[len(navSeries)-1]
	currentDD = (peak - lastNAV) / peak
	if currentDD < 0 {
		currentDD = 0
	}

	return maxDD, maxDDDuration, currentDD
}

func (s *MetricsService) analyzeWinLoss(returns []float64) (winRate, profitFactor, avgWin, avgLoss float64) {
	var wins, losses int
	var grossWin, grossLoss float64
	var totalWin, totalLoss float64

	for _, r := range returns {
		if r > 0 {
			wins++
			grossWin += r
			totalWin += r
		} else if r < 0 {
			losses++
			grossLoss += math.Abs(r)
			totalLoss += math.Abs(r)
		}
	}

	total := wins + losses
	if total > 0 {
		winRate = float64(wins) / float64(total)
	}

	if grossLoss > 0 {
		profitFactor = grossWin / grossLoss
	}

	if wins > 0 {
		avgWin = totalWin / float64(wins)
	}

	if losses > 0 {
		avgLoss = totalLoss / float64(losses)
	}

	return
}

// Helper functions
func mean(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	var sum float64
	for _, v := range values {
		sum += v
	}
	return sum / float64(len(values))
}

func stddev(values []float64) float64 {
	if len(values) < 2 {
		return 0
	}
	avg := mean(values)
	var sumSquares float64
	for _, v := range values {
		diff := v - avg
		sumSquares += diff * diff
	}
	return math.Sqrt(sumSquares / float64(len(values)-1))
}

func summarizePeriod(snapshots []*repository.Snapshot) (start, end time.Time, dataPoints int) {
	if len(snapshots) == 0 {
		return time.Time{}, time.Time{}, 0
	}

	start = snapshots[0].Timestamp
	end = snapshots[len(snapshots)-1].Timestamp

	byDate := make(map[string]struct{})
	for _, snap := range snapshots {
		byDate[snap.Timestamp.Format("2006-01-02")] = struct{}{}
	}

	return start, end, len(byDate)
}

func snapshotConnectionKey(exchange, label string) string {
	ex := strings.ToLower(strings.TrimSpace(exchange))
	lb := strings.ToLower(strings.TrimSpace(label))
	if lb == "" {
		return ex
	}
	return ex + "/" + lb
}
