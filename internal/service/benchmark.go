package service

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
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
type BenchmarkService struct {
	httpClient *http.Client
}

// NewBenchmarkService creates a new benchmark service.
func NewBenchmarkService() *BenchmarkService {
	return &BenchmarkService{
		httpClient: &http.Client{Timeout: 15 * time.Second},
	}
}

// Calculate computes benchmark comparison metrics.
// portfolioReturns are daily net returns, benchmark is "SPY" or "BTC-USD".
func (s *BenchmarkService) Calculate(ctx context.Context, portfolioReturns []float64, benchmark string, startDate, endDate time.Time) (*BenchmarkMetrics, error) {
	if len(portfolioReturns) < 5 {
		return nil, fmt.Errorf("need at least 5 data points for benchmark comparison")
	}

	benchReturns, err := s.fetchBenchmarkReturns(ctx, benchmark, startDate, endDate, len(portfolioReturns))
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

func (s *BenchmarkService) fetchBenchmarkReturns(ctx context.Context, benchmark string, startDate, endDate time.Time, expectedLen int) ([]float64, error) {
	switch benchmark {
	case "SPY", "spy":
		return s.fetchYahooReturns(ctx, "SPY", startDate, endDate)
	case "BTC-USD", "btc-usd", "BTC", "btc":
		return s.fetchCoinGeckoReturns(ctx, "bitcoin", startDate, endDate)
	case "ETH-USD", "eth-usd", "ETH", "eth":
		return s.fetchCoinGeckoReturns(ctx, "ethereum", startDate, endDate)
	default:
		return nil, fmt.Errorf("unsupported benchmark: %s (supported: SPY, BTC-USD, ETH-USD)", benchmark)
	}
}

// fetchYahooReturns fetches daily returns from Yahoo Finance v8 API.
func (s *BenchmarkService) fetchYahooReturns(ctx context.Context, symbol string, startDate, endDate time.Time) ([]float64, error) {
	url := fmt.Sprintf(
		"https://query1.finance.yahoo.com/v8/finance/chart/%s?period1=%d&period2=%d&interval=1d",
		symbol, startDate.Unix(), endDate.Unix(),
	)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "TrackRecord-Enclave/1.0")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("yahoo request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("yahoo returned %d", resp.StatusCode)
	}

	var result struct {
		Chart struct {
			Result []struct {
				Indicators struct {
					AdjClose []struct {
						AdjClose []float64 `json:"adjclose"`
					} `json:"adjclose"`
				} `json:"indicators"`
			} `json:"result"`
		} `json:"chart"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("yahoo decode: %w", err)
	}

	if len(result.Chart.Result) == 0 || len(result.Chart.Result[0].Indicators.AdjClose) == 0 {
		return nil, fmt.Errorf("no data from Yahoo for %s", symbol)
	}

	prices := result.Chart.Result[0].Indicators.AdjClose[0].AdjClose
	return pricesToReturns(prices), nil
}

// fetchCoinGeckoReturns fetches daily returns from CoinGecko.
func (s *BenchmarkService) fetchCoinGeckoReturns(ctx context.Context, coinID string, startDate, endDate time.Time) ([]float64, error) {
	url := fmt.Sprintf(
		"https://api.coingecko.com/api/v3/coins/%s/market_chart/range?vs_currency=usd&from=%d&to=%d",
		coinID, startDate.Unix(), endDate.Unix(),
	)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("coingecko request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("coingecko returned %d", resp.StatusCode)
	}

	var result struct {
		Prices [][]float64 `json:"prices"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("coingecko decode: %w", err)
	}

	if len(result.Prices) < 2 {
		return nil, fmt.Errorf("insufficient data from CoinGecko for %s", coinID)
	}

	// Extract daily prices (CoinGecko returns [timestamp_ms, price])
	prices := make([]float64, len(result.Prices))
	for i, p := range result.Prices {
		if len(p) < 2 {
			continue
		}
		prices[i] = p[1]
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
