package service

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// BenchmarkMetrics holds computed benchmark comparison metrics.
type BenchmarkMetrics struct {
	BenchmarkName    string  `json:"benchmark_name"`
	BenchmarkReturn  float64 `json:"benchmark_return"`
	Alpha            float64 `json:"alpha"`
	Beta             float64 `json:"beta"`
	InformationRatio float64 `json:"information_ratio"`
	TrackingError    float64 `json:"tracking_error"`
	Correlation      float64 `json:"correlation"`
}

// BenchmarkService fetches benchmark data and calculates relative metrics.
//
// Benchmark prices come exclusively from the central benchmark-service
// (BENCHMARK_SERVICE_URL): the enclave never talks to market-data providers
// (Yahoo, CoinGecko, Binance...) directly, so the whole platform consumes a
// single price series per benchmark.
type BenchmarkService struct {
	httpClient *http.Client
	baseURL    string
}

// NewBenchmarkService creates a new benchmark service.
// baseURL is the benchmark-service root URL (BENCHMARK_SERVICE_URL). When
// empty, Calculate fails with a clear error and the report service falls back
// to generating reports without benchmark metrics.
func NewBenchmarkService(baseURL string) *BenchmarkService {
	return &BenchmarkService{
		httpClient: &http.Client{Timeout: 15 * time.Second},
		baseURL:    strings.TrimRight(baseURL, "/"),
	}
}

// Calculate computes benchmark comparison metrics.
// portfolioReturns are daily net returns; benchmark is any symbol the
// benchmark-service supports (SPY, QQQ, VTI, BTC-USD, CD20, CD100, ... —
// aliases like "BTC" are resolved server-side).
func (s *BenchmarkService) Calculate(ctx context.Context, portfolioReturns []float64, benchmark string, startDate, endDate time.Time) (*BenchmarkMetrics, error) {
	if len(portfolioReturns) < 5 {
		return nil, fmt.Errorf("need at least 5 data points for benchmark comparison")
	}

	benchReturns, err := s.fetchBenchmarkReturns(ctx, benchmark, startDate, endDate)
	if err != nil {
		return nil, fmt.Errorf("fetch benchmark data: %w", err)
	}

	// Align lengths
	n := len(portfolioReturns)
	if len(benchReturns) < n {
		n = len(benchReturns)
	}
	pReturns := portfolioReturns[:n]
	bReturns := benchReturns[:n]

	// Calculate means
	pMean := mean(pReturns)
	bMean := mean(bReturns)

	// Calculate Beta = Cov(P, B) / Var(B)
	var covPB, varB float64
	for i := 0; i < n; i++ {
		dp := pReturns[i] - pMean
		db := bReturns[i] - bMean
		covPB += dp * db
		varB += db * db
	}
	covPB /= float64(n)
	varB /= float64(n)

	beta := 0.0
	if varB > 0 {
		beta = covPB / varB
	}

	// Alpha (annualized) = annualized(P) - Beta * annualized(B)
	annP := pMean * tradingDaysPerYear
	annB := bMean * tradingDaysPerYear
	alpha := annP - beta*annB

	// Tracking Error = StdDev(P - B) annualized
	excessReturns := make([]float64, n)
	for i := 0; i < n; i++ {
		excessReturns[i] = pReturns[i] - bReturns[i]
	}
	te := stddev(excessReturns) * math.Sqrt(tradingDaysPerYear)

	// Information Ratio = Mean(excess) / StdDev(excess) * sqrt(252).
	// (Equivalent to Mean(excess)*sqrt(252) / StdDev(excess)*sqrt(252) with
	// the two sqrt(252) factors cancelled.)
	ir := 0.0
	if te > 0 {
		ir = mean(excessReturns) / stddev(excessReturns) * math.Sqrt(tradingDaysPerYear)
	}

	// Correlation = Cov(P, B) / (StdDev(P) * StdDev(B))
	sdP := stddev(pReturns)
	sdB := stddev(bReturns)
	corr := 0.0
	if sdP > 0 && sdB > 0 {
		corr = covPB / (sdP * sdB)
	}

	// Total benchmark return (compounded)
	benchTotal := 1.0
	for _, r := range bReturns {
		benchTotal *= (1 + r)
	}
	benchTotal -= 1

	return &BenchmarkMetrics{
		BenchmarkName:    benchmark,
		BenchmarkReturn:  benchTotal,
		Alpha:            alpha,
		Beta:             beta,
		InformationRatio: ir,
		TrackingError:    te,
		Correlation:      corr,
	}, nil
}

// fetchBenchmarkReturns fetches the daily close series from the central
// benchmark-service and converts it to daily returns. Symbol aliases
// (BTC-USD -> BTCUSDT...) are resolved by the benchmark-service itself.
func (s *BenchmarkService) fetchBenchmarkReturns(ctx context.Context, benchmark string, startDate, endDate time.Time) ([]float64, error) {
	if s.baseURL == "" {
		return nil, fmt.Errorf("BENCHMARK_SERVICE_URL not configured")
	}

	reqURL := fmt.Sprintf(
		"%s/api/v1/benchmarks/%s/daily?startDate=%s&endDate=%s",
		s.baseURL,
		url.PathEscape(benchmark),
		startDate.Format("2006-01-02"),
		endDate.Format("2006-01-02"),
	)

	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "TrackRecord-Enclave/1.0")
	req.Header.Set("Accept", "application/json")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("benchmark-service request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("benchmark-service returned %d for %s", resp.StatusCode, benchmark)
	}

	var result struct {
		Success bool   `json:"success"`
		Error   string `json:"error"`
		Data    struct {
			Symbol string `json:"symbol"`
			Data   []struct {
				Date          string  `json:"date"`
				Close         float64 `json:"close"`
				AdjustedClose float64 `json:"adjustedClose"`
			} `json:"data"`
		} `json:"data"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("benchmark-service decode: %w", err)
	}

	if !result.Success {
		return nil, fmt.Errorf("benchmark-service error for %s: %s", benchmark, result.Error)
	}

	if len(result.Data.Data) < 2 {
		return nil, fmt.Errorf("insufficient benchmark data for %s", benchmark)
	}

	prices := make([]float64, 0, len(result.Data.Data))
	for _, p := range result.Data.Data {
		price := p.AdjustedClose
		if price <= 0 {
			price = p.Close
		}
		if price > 0 {
			prices = append(prices, price)
		}
	}

	return pricesToReturns(prices), nil
}

// pricesToReturns converts a price series to daily returns.
func pricesToReturns(prices []float64) []float64 {
	if len(prices) < 2 {
		return nil
	}

	returns := make([]float64, 0, len(prices)-1)
	for i := 1; i < len(prices); i++ {
		if prices[i-1] > 0 {
			returns = append(returns, (prices[i]-prices[i-1])/prices[i-1])
		} else {
			returns = append(returns, 0)
		}
	}
	return returns
}

// mean and stddev helper functions are defined in metrics.go
