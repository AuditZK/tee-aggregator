package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/trackrecord/enclave/internal/repository"
	"github.com/trackrecord/enclave/internal/signing"
	"go.uber.org/zap"
)

const tradingDaysPerYear = 252

// ReportService generates signed performance reports.
type ReportService struct {
	metricsSvc       *MetricsService
	snapshotRepo     *repository.SnapshotRepo
	connSvc          *ConnectionService
	signedReportRepo *repository.SignedReportRepo
	signer           *signing.ReportSigner
	benchmarkSvc     *BenchmarkService
	logger           *zap.Logger
}

// SetConnectionService configures optional exchange metadata enrichment.
func (s *ReportService) SetConnectionService(connSvc *ConnectionService) {
	s.connSvc = connSvc
}

// NewReportService creates a new report service
func NewReportService(metricsSvc *MetricsService, snapshotRepo *repository.SnapshotRepo, signer *signing.ReportSigner) *ReportService {
	return &ReportService{
		metricsSvc:   metricsSvc,
		snapshotRepo: snapshotRepo,
		signer:       signer,
	}
}

// NewReportServiceFull creates a report service with caching and benchmarks.
func NewReportServiceFull(
	metricsSvc *MetricsService,
	snapshotRepo *repository.SnapshotRepo,
	signedReportRepo *repository.SignedReportRepo,
	signer *signing.ReportSigner,
	benchmarkSvc *BenchmarkService,
	logger *zap.Logger,
) *ReportService {
	return &ReportService{
		metricsSvc:       metricsSvc,
		snapshotRepo:     snapshotRepo,
		signedReportRepo: signedReportRepo,
		signer:           signer,
		benchmarkSvc:     benchmarkSvc,
		logger:           logger,
	}
}

// GenerateReportRequest contains report generation parameters
type GenerateReportRequest struct {
	UserUID            string
	StartDate          time.Time
	EndDate            time.Time
	ReportName         string
	Benchmark          string
	BaseCurrency       string
	RiskFreeRate       float64 // annual %, e.g. 2.5 (0 = legacy rf-free ratios)
	IncludeRiskMetrics bool
	IncludeDrawdown    bool
	ExcludedExchanges  map[string]struct{} // keys: "exchange" or "exchange/label"
	// Display params (NOT signed, applied per request)
	Manager string
	Firm    string
}

// dailyReturn is the internal representation during computation
type dailyReturn struct {
	date             string
	netReturn        float64
	benchmarkReturn  float64
	outperformance   float64
	cumulativeReturn float64
	nav              float64
}

// monthlyReturn is the internal representation during computation
type monthlyReturn struct {
	date            string
	netReturn       float64
	benchmarkReturn float64
	outperformance  float64
	aum             float64
}

// riskMetrics holds computed risk metrics
type riskMetrics struct {
	var95             float64
	var99             float64
	expectedShortfall float64
	skewness          float64
	kurtosis          float64
}

// drawdownPeriod holds a single drawdown event
type drawdownPeriod struct {
	startDate string
	endDate   string
	depth     float64
	duration  int
	recovered bool
}

// drawdownData holds drawdown analysis results
type drawdownData struct {
	currentDrawdown     float64
	maxDrawdownDuration int
	periods             []*drawdownPeriod
}

// GenerateReport creates a signed performance report with full analytics
func (s *ReportService) GenerateReport(ctx context.Context, req *GenerateReportRequest) (*signing.SignedReport, error) {
	// 0. Check cache (dedup by user + dates + benchmark)
	if cached := s.checkReportCache(ctx, req); cached != nil {
		// Apply display params (not signed) to cached report
		cached.Manager = req.Manager
		cached.Firm = req.Firm
		return cached, nil
	}

	// 1. Fetch snapshots. As of PayloadVersion 1.3 the signed report covers
	// the full history (live + in-enclave reconstructed + external-rebuilder
	// reconstructed) and each daily return carries a verifiabilityClass label
	// derived from the underlying snapshots' provenance. Verifiers who need
	// strict in-enclave-only data filter the daily returns at consumption
	// time. The previous SEC-001 coarse gate was replaced by per-day
	// labelling so that user-facing PDFs and signed reports stay byte-for-
	// byte consistent on the equity curve.
	snapshots, err := s.snapshotRepo.GetVerifiableByUserAndDateRange(ctx, req.UserUID, req.StartDate, req.EndDate)
	if err != nil {
		return nil, fmt.Errorf("fetch snapshots: %w", err)
	}

	// Days that include at least one external-rebuilder snapshot — used to
	// label the verifiabilityClass of each daily return (1.3+ payload).
	externalRebuilderDays, err := s.snapshotRepo.GetExternalRebuilderDays(ctx, req.UserUID, req.StartDate, req.EndDate)
	if err != nil {
		// Non-fatal: degrade gracefully by treating the set as empty (no
		// per-day rebuilder taint). The signed report is still produced;
		// the worst case is a strict verifier accepting a day they would
		// otherwise filter — which the same verifier can detect by cross-
		// referencing the user's exchange list. Logged so an operator can
		// chase the underlying query failure.
		s.logger.Warn("fetch external-rebuilder day set failed; verifiabilityClass labels may underreport rebuilder taint",
			zap.String("user_uid", req.UserUID),
			zap.Error(err),
		)
		externalRebuilderDays = map[time.Time]struct{}{}
	}

	snapshots = filterSnapshotsByExcludedExchanges(snapshots, req.ExcludedExchanges)

	if len(snapshots) < 2 {
		return nil, fmt.Errorf("insufficient data: need at least 2 snapshots, got %d", len(snapshots))
	}

	// Sort by timestamp
	sort.Slice(snapshots, func(i, j int) bool {
		return snapshots[i].Timestamp.Before(snapshots[j].Timestamp)
	})

	// 2. Convert to daily returns (TWR with multi-exchange support)
	dailyReturns := convertSnapshotsToDailyReturns(snapshots)

	// 2b. Enrich daily returns with the benchmark series (same decimal unit
	// as netReturn) BEFORE monthly aggregation and signing, so the signed
	// report carries per-day benchmark data for charts (rolling beta,
	// portfolio-vs-benchmark) — not just the aggregate metrics. One fetch
	// serves both this enrichment and section 9's metrics. Failure is
	// non-fatal: the series stays at zero, as before.
	var benchmarkSeries map[string]float64
	if req.Benchmark != "" && s.benchmarkSvc != nil && len(dailyReturns) > 0 {
		series, err := s.benchmarkSvc.DailyReturnsByDate(ctx, req.Benchmark, req.StartDate, req.EndDate)
		if err != nil {
			if s.logger != nil {
				s.logger.Warn("benchmark series fetch failed, continuing without", zap.Error(err))
			}
		} else {
			benchmarkSeries = series
			for i := range dailyReturns {
				if br, ok := benchmarkSeries[dailyReturns[i].date]; ok {
					dailyReturns[i].benchmarkReturn = br
					dailyReturns[i].outperformance = dailyReturns[i].netReturn - br
				}
			}
		}
	}

	// 3. Calculate core metrics over the SAME verifiable, exchange-filtered
	// snapshot set. Re-fetching via the MetricsService would silently
	// re-include the external-rebuilder history excluded in step 1 (SEC-001).
	metrics, err := s.metricsSvc.CalculateFromSnapshots(snapshots, req.RiskFreeRate)
	if err != nil {
		return nil, fmt.Errorf("calculate metrics: %w", err)
	}

	// 4. Aggregate to monthly returns
	monthlyReturns := aggregateToMonthlyReturns(dailyReturns, snapshots)

	// 5. Collect exchanges
	exchangeSet := make(map[string]bool)
	for _, snap := range snapshots {
		exchangeSet[snap.Exchange] = true
	}
	var exchanges []string
	for ex := range exchangeSet {
		exchanges = append(exchanges, ex)
	}
	sort.Strings(exchanges)

	// 6. Build report input
	input := &signing.ReportInput{
		UserUID:          req.UserUID,
		ReportName:       req.ReportName,
		RiskFreeRate:     req.RiskFreeRate,
		PeriodStart:      metrics.PeriodStart,
		PeriodEnd:        metrics.PeriodEnd,
		TotalReturn:      metrics.TotalReturn,
		AnnualizedReturn: metrics.AnnualizedReturn,
		SharpeRatio:      metrics.SharpeRatio,
		SortinoRatio:     metrics.SortinoRatio,
		CalmarRatio:      metrics.CalmarRatio,
		MaxDrawdown:      metrics.MaxDrawdown,
		Volatility:       metrics.Volatility,
		WinRate:          metrics.WinRate,
		ProfitFactor:     metrics.ProfitFactor,
		DataPoints:       metrics.DataPoints,
		BaseCurrency:     req.BaseCurrency,
		BenchmarkUsed:    req.Benchmark,
		Exchanges:        exchanges,
		ExchangeDetails:  s.buildExchangeDetails(ctx, req.UserUID, exchanges),
		DailyReturns:     toSigningDailyReturns(dailyReturns, snapshots, externalRebuilderDays),
		MonthlyReturns:   toSigningMonthlyReturns(monthlyReturns),
	}

	if input.ReportName == "" {
		input.ReportName = fmt.Sprintf("Performance Report %s to %s",
			input.PeriodStart.Format(dateFormat),
			input.PeriodEnd.Format(dateFormat))
	}

	if input.BaseCurrency == "" {
		input.BaseCurrency = "USD"
	}

	// 7. Optional risk metrics
	if req.IncludeRiskMetrics && len(dailyReturns) > 0 {
		returns := make([]float64, len(dailyReturns))
		for i, dr := range dailyReturns {
			returns[i] = dr.netReturn
		}
		rm := calculateRiskMetrics(returns)
		input.RiskMetrics = toSigningRiskMetrics(rm)
	}

	// 8. Optional drawdown data
	if req.IncludeDrawdown && len(dailyReturns) > 0 {
		dd := calculateDrawdownData(dailyReturns)
		input.DrawdownData = toSigningDrawdownData(dd)
	}

	// 9. Optional benchmark metrics — computed over the date-aligned series
	// fetched in 2b (only days where both portfolio and benchmark have data).
	if benchmarkSeries != nil {
		alignedPortfolio := make([]float64, 0, len(dailyReturns))
		alignedBenchmark := make([]float64, 0, len(dailyReturns))
		for _, dr := range dailyReturns {
			if br, ok := benchmarkSeries[dr.date]; ok {
				alignedPortfolio = append(alignedPortfolio, dr.netReturn)
				alignedBenchmark = append(alignedBenchmark, br)
			}
		}
		bm, err := s.benchmarkSvc.CalculateFromSeries(alignedPortfolio, alignedBenchmark, req.Benchmark)
		if err != nil {
			if s.logger != nil {
				s.logger.Warn("benchmark calculation failed, continuing without", zap.Error(err))
			}
		} else {
			input.BenchmarkMetrics = &signing.BenchmarkMetrics{
				BenchmarkName:    bm.BenchmarkName,
				BenchmarkReturn:  bm.BenchmarkReturn,
				Alpha:            bm.Alpha,
				Beta:             bm.Beta,
				InformationRatio: bm.InformationRatio,
				TrackingError:    bm.TrackingError,
				Correlation:      bm.Correlation,
			}
		}
	}

	// 10. Sign
	report, err := s.signer.Sign(input)
	if err != nil {
		return nil, err
	}

	// 11. Cache the signed report
	s.cacheReport(ctx, req, report)

	// 12. Apply display params (not signed)
	report.Manager = req.Manager
	report.Firm = req.Firm

	return report, nil
}

const dateFormat = "2006-01-02"

// checkReportCache looks for a cached report matching user + dates + benchmark.
func (s *ReportService) checkReportCache(ctx context.Context, req *GenerateReportRequest) *signing.SignedReport {
	if s.signedReportRepo == nil {
		return nil
	}
	// Current cache key does not include exclusions; skip cache for filtered reports.
	if len(req.ExcludedExchanges) > 0 {
		return nil
	}
	// Cache key does not include the risk-free rate either; only the legacy
	// rf=0 reports are cacheable.
	if req.RiskFreeRate != 0 {
		return nil
	}

	cached, err := s.signedReportRepo.GetCached(ctx, req.UserUID, req.StartDate, req.EndDate, req.Benchmark)
	if err != nil {
		if !errors.Is(err, repository.ErrNotFound) && s.logger != nil {
			s.logger.Warn("report cache lookup failed", zap.Error(err))
		}
		return nil
	}

	var report signing.SignedReport
	if err := json.Unmarshal(cached.ReportData, &report); err != nil {
		if s.logger != nil {
			s.logger.Warn("report cache unmarshal failed", zap.Error(err))
		}
		return nil
	}

	return &report
}

// cacheReport stores a signed report for deduplication.
func (s *ReportService) cacheReport(ctx context.Context, req *GenerateReportRequest, report *signing.SignedReport) {
	if s.signedReportRepo == nil {
		return
	}
	// Current cache key does not include exclusions; avoid storing filtered variants.
	if len(req.ExcludedExchanges) > 0 {
		return
	}
	// Same for custom risk-free rates — the key has no rf dimension.
	if req.RiskFreeRate != 0 {
		return
	}

	reportData, err := json.Marshal(report)
	if err != nil {
		if s.logger != nil {
			s.logger.Warn("report cache marshal failed", zap.Error(err))
		}
		return
	}

	record := &repository.SignedReportRecord{
		ReportID:       report.ReportID,
		UserUID:        req.UserUID,
		StartDate:      req.StartDate,
		EndDate:        req.EndDate,
		Benchmark:      req.Benchmark,
		ReportData:     reportData,
		Signature:      report.Signature,
		ReportHash:     report.ReportHash,
		EnclaveVersion: report.EnclaveVersion,
	}

	if err := s.signedReportRepo.Create(ctx, record); err != nil {
		if s.logger != nil {
			s.logger.Warn("report cache store failed", zap.Error(err))
		}
	}
}

// convertSnapshotsToDailyReturns implements TWR with multi-connection support.
// Groups snapshots by date and connection key (exchange/label), handles virtual
// deposits when new connections appear, and forward-fills missing connection data.
func convertSnapshotsToDailyReturns(snapshots []*repository.Snapshot) []dailyReturn {
	if len(snapshots) == 0 {
		return nil
	}

	// Group snapshots by date -> exchange -> snapshot
	type dateGroup struct {
		date      string
		timestamp time.Time
		exchanges map[string]*repository.Snapshot
	}

	dateMap := make(map[string]*dateGroup)
	var dateOrder []string

	for _, snap := range snapshots {
		dateStr := snap.Timestamp.Format("2006-01-02")
		dg, exists := dateMap[dateStr]
		if !exists {
			dg = &dateGroup{
				date:      dateStr,
				timestamp: snap.Timestamp,
				exchanges: make(map[string]*repository.Snapshot),
			}
			dateMap[dateStr] = dg
			dateOrder = append(dateOrder, dateStr)
		}
		dg.exchanges[snapshotConnectionKey(snap.Exchange, snap.Label)] = snap
	}

	sort.Strings(dateOrder)

	if len(dateOrder) < 2 {
		return nil
	}

	// Track known connection keys and their last known equity.
	knownExchanges := make(map[string]float64) // connection key -> last equity
	var returns []dailyReturn
	cumulativeReturn := 0.0
	nav := 1.0

	for i, dateStr := range dateOrder {
		dg := dateMap[dateStr]

		if i == 0 {
			// Initialize known exchanges with first day's data
			for ex, snap := range dg.exchanges {
				knownExchanges[ex] = snap.TotalEquity
			}
			continue
		}

		// Calculate total previous equity (forward-fill for missing exchanges)
		totalPrevEquity := 0.0
		for _, lastEq := range knownExchanges {
			totalPrevEquity += lastEq
		}

		if totalPrevEquity == 0 {
			// Update known exchanges
			for ex, snap := range dg.exchanges {
				knownExchanges[ex] = snap.TotalEquity
			}
			continue
		}

		// Calculate current total, handling new exchanges as virtual deposits
		totalCurrentEquity := 0.0
		virtualDeposits := 0.0

		for ex, lastEq := range knownExchanges {
			if snap, exists := dg.exchanges[ex]; exists {
				// Exchange has data today
				adjustedEquity := snap.TotalEquity - snap.Deposits + snap.Withdrawals
				totalCurrentEquity += adjustedEquity
				knownExchanges[ex] = snap.TotalEquity
			} else {
				// Forward-fill: use last known equity
				totalCurrentEquity += lastEq
			}
		}

		// Check for new connection keys appearing today.
		for ex, snap := range dg.exchanges {
			if _, known := knownExchanges[ex]; !known {
				// New connection - treat as virtual deposit.
				virtualDeposits += snap.TotalEquity
				knownExchanges[ex] = snap.TotalEquity
			}
		}

		// TWR: adjust denominator for virtual deposits
		adjustedPrev := totalPrevEquity + virtualDeposits

		var dayReturn float64
		if adjustedPrev > 0 {
			dayReturn = (totalCurrentEquity + virtualDeposits - adjustedPrev) / adjustedPrev
		}

		cumulativeReturn = (1+cumulativeReturn)*(1+dayReturn) - 1
		nav = nav * (1 + dayReturn)

		returns = append(returns, dailyReturn{
			date:             dateStr,
			netReturn:        dayReturn,
			benchmarkReturn:  0, // Benchmark data not available from exchange APIs
			outperformance:   dayReturn,
			cumulativeReturn: cumulativeReturn,
			nav:              nav,
		})
	}

	return returns
}

// aggregateToMonthlyReturns groups daily returns by month and compounds them
func aggregateToMonthlyReturns(daily []dailyReturn, snapshots []*repository.Snapshot) []monthlyReturn {
	if len(daily) == 0 {
		return nil
	}

	// Group by YYYY-MM
	type monthGroup struct {
		month   string
		returns []dailyReturn
	}

	monthMap := make(map[string]*monthGroup)
	var monthOrder []string

	for _, dr := range daily {
		monthStr := dr.date[:7] // "YYYY-MM"
		mg, exists := monthMap[monthStr]
		if !exists {
			mg = &monthGroup{month: monthStr}
			monthMap[monthStr] = mg
			monthOrder = append(monthOrder, monthStr)
		}
		mg.returns = append(mg.returns, dr)
	}

	sort.Strings(monthOrder)

	var monthly []monthlyReturn
	prevCumulative := 0.0

	for _, monthStr := range monthOrder {
		mg := monthMap[monthStr]
		if len(mg.returns) == 0 {
			continue
		}

		lastDay := mg.returns[len(mg.returns)-1]
		endCumulative := lastDay.cumulativeReturn

		// Monthly return from cumulative ratios
		var monthlyRet float64
		if prevCumulative == 0 && endCumulative == 0 {
			monthlyRet = 0
		} else {
			monthlyRet = (1+endCumulative)/(1+prevCumulative) - 1
		}

		prevCumulative = endCumulative

		monthly = append(monthly, monthlyReturn{
			date:            monthStr,
			netReturn:       monthlyRet,
			benchmarkReturn: 0,
			outperformance:  monthlyRet,
			aum:             lastDay.nav,
		})
	}

	return monthly
}

// calculateRiskMetrics computes VaR, CVaR, skewness, and kurtosis
func calculateRiskMetrics(returns []float64) *riskMetrics {
	if len(returns) < 5 {
		return &riskMetrics{}
	}

	// Sort returns for percentile calculation
	sorted := make([]float64, len(returns))
	copy(sorted, returns)
	sort.Float64s(sorted)

	n := len(sorted)

	// VaR 95 (historical, 5th percentile)
	idx95 := int(math.Floor(float64(n) * 0.05))
	if idx95 >= n {
		idx95 = n - 1
	}
	var95 := -sorted[idx95]

	// VaR 99 (historical, 1st percentile)
	idx99 := int(math.Floor(float64(n) * 0.01))
	if idx99 >= n {
		idx99 = n - 1
	}
	var99 := -sorted[idx99]

	// Expected Shortfall (CVaR) - average of returns below VaR95
	var cvarSum float64
	var cvarCount int
	threshold := sorted[idx95]
	for _, r := range sorted {
		if r <= threshold {
			cvarSum += r
			cvarCount++
		}
	}
	expectedShortfall := 0.0
	if cvarCount > 0 {
		expectedShortfall = -cvarSum / float64(cvarCount)
	}

	// Mean and standard deviation for higher moments
	avg := 0.0
	for _, r := range returns {
		avg += r
	}
	avg /= float64(n)

	variance := 0.0
	for _, r := range returns {
		diff := r - avg
		variance += diff * diff
	}
	variance /= float64(n)
	sd := math.Sqrt(variance)

	// Skewness (third standardized moment)
	skewness := 0.0
	if sd > 0 {
		var m3 float64
		for _, r := range returns {
			diff := (r - avg) / sd
			m3 += diff * diff * diff
		}
		skewness = m3 / float64(n)
	}

	// Excess Kurtosis (fourth standardized moment - 3)
	kurtosis := 0.0
	if sd > 0 {
		var m4 float64
		for _, r := range returns {
			diff := (r - avg) / sd
			m4 += diff * diff * diff * diff
		}
		kurtosis = m4/float64(n) - 3
	}

	return &riskMetrics{
		var95:             var95,
		var99:             var99,
		expectedShortfall: expectedShortfall,
		skewness:          skewness,
		kurtosis:          kurtosis,
	}
}

// calculateDrawdownData tracks drawdown periods with recovery detection
func calculateDrawdownData(daily []dailyReturn) *drawdownData {
	if len(daily) == 0 {
		return &drawdownData{}
	}

	var periods []*drawdownPeriod
	var currentPeriod *drawdownPeriod
	peak := 1.0
	maxDDDuration := 0
	ddStartIdx := -1

	for i, dr := range daily {
		nav := dr.nav
		if nav > peak {
			// New peak - close current drawdown period if any
			if currentPeriod != nil {
				currentPeriod.recovered = true
				currentPeriod.endDate = dr.date
				periods = append(periods, currentPeriod)
				currentPeriod = nil
			}
			peak = nav
			ddStartIdx = -1
		} else if nav < peak {
			dd := (peak - nav) / peak
			if currentPeriod == nil {
				// Start new drawdown period
				ddStartIdx = i
				currentPeriod = &drawdownPeriod{
					startDate: dr.date,
					endDate:   dr.date,
					depth:     dd,
					duration:  1,
					recovered: false,
				}
			} else {
				// Update current period
				currentPeriod.endDate = dr.date
				currentPeriod.duration = i - ddStartIdx + 1
				if dd > currentPeriod.depth {
					currentPeriod.depth = dd
				}
			}

			if currentPeriod.duration > maxDDDuration {
				maxDDDuration = currentPeriod.duration
			}
		}
	}

	// Add unclosed period
	if currentPeriod != nil {
		periods = append(periods, currentPeriod)
	}

	// Keep last 5 periods
	if len(periods) > 5 {
		periods = periods[len(periods)-5:]
	}

	// Current drawdown
	lastNav := daily[len(daily)-1].nav
	currentDD := 0.0
	if peak > 0 && lastNav < peak {
		currentDD = (peak - lastNav) / peak
	}

	return &drawdownData{
		currentDrawdown:     currentDD,
		maxDrawdownDuration: maxDDDuration,
		periods:             periods,
	}
}

// Conversion helpers: internal types -> signing types

// toSigningDailyReturns converts internal daily returns to the signing-layer
// shape, attaching a verifiabilityClass label per day (PayloadVersion 1.3+).
//
// Per-day labelling rule (most-restrictive wins; surfaced for verifier
// policy):
//
//	- "rebuilder-service" if any snapshot for the day came from the external
//	  history-rebuilder (the day's data path briefly left the SEV-SNP
//	  perimeter).
//	- "in-enclave" if no rebuilder taint but at least one snapshot was
//	  reconstructed inside the enclave (IBKR Flex etc.).
//	- "live" if every snapshot for the day is a live daily-sync write.
//
// This decision is made off the snapshot set, NOT the daily-return slice, so
// the label reflects the underlying data even when the daily aggregation has
// already collapsed multiple exchanges into one row.
func toSigningDailyReturns(daily []dailyReturn, snapshots []*repository.Snapshot, externalRebuilderDays map[time.Time]struct{}) []signing.DailyReturn {
	// Index snapshots by day-key to derive in-enclave vs live without
	// hitting the rebuilder map twice.
	inEnclaveDays := make(map[time.Time]bool, len(snapshots))
	for _, s := range snapshots {
		if s.IsHistorical && !s.FromExternalRebuilder {
			day := time.Date(s.Timestamp.Year(), s.Timestamp.Month(), s.Timestamp.Day(), 0, 0, 0, 0, time.UTC)
			inEnclaveDays[day] = true
		}
	}

	result := make([]signing.DailyReturn, len(daily))
	for i, dr := range daily {
		dayKey, parseErr := time.Parse("2006-01-02", dr.date)
		var class string
		switch {
		case parseErr != nil:
			// Defensive: if the date format ever drifts, fall back to "live"
			// (the most permissive label). The signature still binds the
			// returned class, so any tamper detection downstream is preserved.
			class = signing.VerifiabilityClassLive
		default:
			dayKey = dayKey.UTC()
			if _, taint := externalRebuilderDays[dayKey]; taint {
				class = signing.VerifiabilityClassRebuilderService
			} else if inEnclaveDays[dayKey] {
				class = signing.VerifiabilityClassInEnclave
			} else {
				class = signing.VerifiabilityClassLive
			}
		}
		result[i] = signing.DailyReturn{
			Date:               dr.date,
			NetReturn:          dr.netReturn,
			BenchmarkReturn:    dr.benchmarkReturn,
			Outperformance:     dr.outperformance,
			CumulativeReturn:   dr.cumulativeReturn,
			NAV:                dr.nav,
			VerifiabilityClass: class,
		}
	}
	return result
}

func toSigningMonthlyReturns(monthly []monthlyReturn) []signing.MonthlyReturn {
	result := make([]signing.MonthlyReturn, len(monthly))
	for i, mr := range monthly {
		result[i] = signing.MonthlyReturn{
			Date:            mr.date,
			NetReturn:       mr.netReturn,
			BenchmarkReturn: mr.benchmarkReturn,
			Outperformance:  mr.outperformance,
			AUM:             mr.aum,
		}
	}
	return result
}

func toSigningRiskMetrics(rm *riskMetrics) *signing.RiskMetrics {
	return &signing.RiskMetrics{
		VaR95:             rm.var95,
		VaR99:             rm.var99,
		ExpectedShortfall: rm.expectedShortfall,
		Skewness:          rm.skewness,
		Kurtosis:          rm.kurtosis,
	}
}

func toSigningDrawdownData(dd *drawdownData) *signing.DrawdownData {
	periods := make([]*signing.DrawdownPeriod, len(dd.periods))
	for i, p := range dd.periods {
		periods[i] = &signing.DrawdownPeriod{
			StartDate: p.startDate,
			EndDate:   p.endDate,
			Depth:     p.depth,
			Duration:  p.duration,
			Recovered: p.recovered,
		}
	}
	return &signing.DrawdownData{
		CurrentDrawdown:     dd.currentDrawdown,
		MaxDrawdownDuration: dd.maxDrawdownDuration,
		Periods:             periods,
	}
}

func buildDefaultExchangeDetails(exchanges []string) []signing.ExchangeInfo {
	if len(exchanges) == 0 {
		return nil
	}

	details := make([]signing.ExchangeInfo, 0, len(exchanges))
	for _, ex := range exchanges {
		details = append(details, signing.ExchangeInfo{
			Name:     ex,
			KYCLevel: "",
			IsPaper:  false,
		})
	}
	return details
}

func (s *ReportService) buildExchangeDetails(ctx context.Context, userUID string, exchanges []string) []signing.ExchangeInfo {
	defaultDetails := buildDefaultExchangeDetails(exchanges)
	if s.connSvc == nil {
		return defaultDetails
	}

	metadata, err := s.connSvc.GetExchangeMetadata(ctx, userUID)
	if err != nil {
		if s.logger != nil {
			s.logger.Warn("failed to load exchange metadata; using defaults", zap.Error(err))
		}
		return defaultDetails
	}
	if len(metadata) == 0 {
		return defaultDetails
	}

	metaByExchange := make(map[string]*ExchangeMetadata, len(metadata))
	for _, m := range metadata {
		key := strings.ToLower(strings.TrimSpace(m.Exchange))
		metaByExchange[key] = m
	}

	merged := make([]signing.ExchangeInfo, 0, len(exchanges))
	for _, ex := range exchanges {
		key := strings.ToLower(strings.TrimSpace(ex))
		if md, ok := metaByExchange[key]; ok {
			merged = append(merged, signing.ExchangeInfo{
				Name:     ex,
				KYCLevel: md.KYCLevel,
				IsPaper:  md.IsPaper,
			})
			continue
		}
		merged = append(merged, signing.ExchangeInfo{
			Name:     ex,
			KYCLevel: "",
			IsPaper:  false,
		})
	}

	return merged
}

// VerifySignature checks if a report signature is valid. Uses the enclave's
// current SignatureAlgorithm (ECDSA-P256-SHA256) explicitly — no silent
// algorithm fallback (SEC-108).
func (s *ReportService) VerifySignature(reportHash, signature, publicKey string) (bool, error) {
	return signing.Verify(reportHash, signature, publicKey, signing.SignatureAlgorithm)
}

// PublicKey returns the signer's public key
func (s *ReportService) PublicKey() string {
	return s.signer.PublicKey()
}
