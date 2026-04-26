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

	// 1. Fetch snapshots
	snapshots, err := s.snapshotRepo.GetByUserAndDateRange(ctx, req.UserUID, req.StartDate, req.EndDate)
	if err != nil {
		return nil, fmt.Errorf("fetch snapshots: %w", err)
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

	// 3. Calculate core metrics
	metrics, err := s.metricsSvc.CalculateWithExcludedExchanges(ctx, req.UserUID, req.StartDate, req.EndDate, req.ExcludedExchanges)
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
		DailyReturns:     toSigningDailyReturns(dailyReturns),
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

	// 9. Optional benchmark metrics
	if req.Benchmark != "" && s.benchmarkSvc != nil && len(dailyReturns) > 0 {
		portfolioReturns := make([]float64, len(dailyReturns))
		for i, dr := range dailyReturns {
			portfolioReturns[i] = dr.netReturn
		}
		bm, err := s.benchmarkSvc.Calculate(ctx, portfolioReturns, req.Benchmark, req.StartDate, req.EndDate)
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

func toSigningDailyReturns(daily []dailyReturn) []signing.DailyReturn {
	result := make([]signing.DailyReturn, len(daily))
	for i, dr := range daily {
		result[i] = signing.DailyReturn{
			Date:             dr.date,
			NetReturn:        dr.netReturn,
			BenchmarkReturn:  dr.benchmarkReturn,
			Outperformance:   dr.outperformance,
			CumulativeReturn: dr.cumulativeReturn,
			NAV:              dr.nav,
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
