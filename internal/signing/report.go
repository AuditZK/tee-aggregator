package signing

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"sort"
	"time"

	"github.com/google/uuid"
)

const (
	SignatureAlgorithm = "ECDSA-P256-SHA256"
	EnclaveVersion     = "1.0.0-go"
)

// ReportSigner signs performance reports.
type ReportSigner struct {
	privateKey      *ecdsa.PrivateKey
	publicKeyBase64 string
}

// NewReportSigner creates a signer from a deterministic seed (32 bytes).
func NewReportSigner(seed []byte) (*ReportSigner, error) {
	if len(seed) != 32 {
		return nil, fmt.Errorf("seed must be 32 bytes")
	}

	curve := elliptic.P256()
	n := curve.Params().N

	seedHash := sha256.Sum256(seed)
	d := new(big.Int).SetBytes(seedHash[:])
	one := big.NewInt(1)
	max := new(big.Int).Sub(n, one)
	d.Mod(d, max)
	d.Add(d, one)

	x, y := curve.ScalarBaseMult(d.Bytes())
	privateKey := &ecdsa.PrivateKey{
		PublicKey: ecdsa.PublicKey{
			Curve: curve,
			X:     x,
			Y:     y,
		},
		D: d,
	}

	return newReportSignerFromPrivateKey(privateKey)
}

// NewReportSignerGenerate creates a signer with a new keypair.
// Returns an error if the system RNG fails or the public key cannot be
// serialized — callers (typically main.go startup) should treat failure as
// fatal since without a signer the enclave cannot produce verifiable reports.
func NewReportSignerGenerate() (*ReportSigner, error) {
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate report signer keypair: %w", err)
	}

	signer, err := newReportSignerFromPrivateKey(privateKey)
	if err != nil {
		return nil, fmt.Errorf("initialize report signer: %w", err)
	}
	return signer, nil
}

// MustNewReportSignerGenerate is a test-only helper that panics on error.
// Production code must use NewReportSignerGenerate and handle the error.
func MustNewReportSignerGenerate() *ReportSigner {
	signer, err := NewReportSignerGenerate()
	if err != nil {
		panic(err)
	}
	return signer
}

func newReportSignerFromPrivateKey(privateKey *ecdsa.PrivateKey) (*ReportSigner, error) {
	der, err := x509.MarshalPKIXPublicKey(&privateKey.PublicKey)
	if err != nil {
		return nil, fmt.Errorf("marshal public key: %w", err)
	}

	return &ReportSigner{
		privateKey:      privateKey,
		publicKeyBase64: base64.StdEncoding.EncodeToString(der),
	}, nil
}

// PublicKey returns the report signing public key (base64 DER-encoded SPKI).
func (s *ReportSigner) PublicKey() string {
	return s.publicKeyBase64
}

// PublicKeyHex is kept for compatibility; it now returns base64 DER public key.
func (s *ReportSigner) PublicKeyHex() string {
	return s.PublicKey()
}

// DailyReturn represents a single day's return data.
type DailyReturn struct {
	Date             string  `json:"date"`
	NetReturn        float64 `json:"net_return"`
	BenchmarkReturn  float64 `json:"benchmark_return"`
	Outperformance   float64 `json:"outperformance"`
	CumulativeReturn float64 `json:"cumulative_return"`
	NAV              float64 `json:"nav"`
}

// MonthlyReturn represents a single month's return data.
type MonthlyReturn struct {
	Date            string  `json:"date"`
	NetReturn       float64 `json:"net_return"`
	BenchmarkReturn float64 `json:"benchmark_return"`
	Outperformance  float64 `json:"outperformance"`
	AUM             float64 `json:"aum"`
}

// RiskMetrics contains risk analysis data.
type RiskMetrics struct {
	VaR95             float64 `json:"var_95"`
	VaR99             float64 `json:"var_99"`
	ExpectedShortfall float64 `json:"expected_shortfall"`
	Skewness          float64 `json:"skewness"`
	Kurtosis          float64 `json:"kurtosis"`
}

// DrawdownPeriod represents a single drawdown event.
type DrawdownPeriod struct {
	StartDate string  `json:"start_date"`
	EndDate   string  `json:"end_date"`
	Depth     float64 `json:"depth"`
	Duration  int     `json:"duration"`
	Recovered bool    `json:"recovered"`
}

// DrawdownData contains drawdown analysis.
type DrawdownData struct {
	CurrentDrawdown     float64           `json:"current_drawdown"`
	MaxDrawdownDuration int               `json:"max_drawdown_duration"`
	Periods             []*DrawdownPeriod `json:"periods"`
}

// BenchmarkMetrics holds benchmark comparison data for the report.
type BenchmarkMetrics struct {
	BenchmarkName    string  `json:"benchmark_name"`
	BenchmarkReturn  float64 `json:"benchmark_return"`
	Alpha            float64 `json:"alpha"`
	Beta             float64 `json:"beta"`
	InformationRatio float64 `json:"information_ratio"`
	TrackingError    float64 `json:"tracking_error"`
	Correlation      float64 `json:"correlation"`
}

// ExchangeInfo stores exchange-level metadata included in signed reports.
type ExchangeInfo struct {
	Name     string `json:"name"`
	KYCLevel string `json:"kyc_level"`
	IsPaper  bool   `json:"is_paper"`
}

// ReportInput contains the data to include in a signed report.
type ReportInput struct {
	UserUID     string
	ReportName  string
	PeriodStart time.Time
	PeriodEnd   time.Time

	// Metrics
	TotalReturn      float64
	AnnualizedReturn float64
	SharpeRatio      float64
	SortinoRatio     float64
	CalmarRatio      float64
	MaxDrawdown      float64
	Volatility       float64
	WinRate          float64
	ProfitFactor     float64
	DataPoints       int
	BaseCurrency     string
	BenchmarkUsed    string

	// Extended data
	Exchanges        []string
	ExchangeDetails  []ExchangeInfo
	DailyReturns     []DailyReturn
	MonthlyReturns   []MonthlyReturn
	RiskMetrics      *RiskMetrics
	DrawdownData     *DrawdownData
	BenchmarkMetrics *BenchmarkMetrics
}

// SignedReport is the output of signing.
type SignedReport struct {
	// Identification
	ReportID    string `json:"report_id"`
	UserUID     string `json:"user_uid"`
	ReportName  string `json:"report_name"`
	GeneratedAt string `json:"generated_at"`

	// Period
	PeriodStart string `json:"period_start"`
	PeriodEnd   string `json:"period_end"`

	// Metrics
	TotalReturn      float64 `json:"total_return"`
	AnnualizedReturn float64 `json:"annualized_return"`
	SharpeRatio      float64 `json:"sharpe_ratio"`
	SortinoRatio     float64 `json:"sortino_ratio"`
	CalmarRatio      float64 `json:"calmar_ratio"`
	MaxDrawdown      float64 `json:"max_drawdown"`
	Volatility       float64 `json:"volatility"`
	WinRate          float64 `json:"win_rate"`
	ProfitFactor     float64 `json:"profit_factor"`
	DataPoints       int     `json:"data_points"`
	BaseCurrency     string  `json:"base_currency"`
	Benchmark        string  `json:"benchmark"`

	// Extended data
	Exchanges        []string          `json:"exchanges,omitempty"`
	ExchangeDetails  []ExchangeInfo    `json:"exchange_details,omitempty"`
	DailyReturns     []DailyReturn     `json:"daily_returns,omitempty"`
	MonthlyReturns   []MonthlyReturn   `json:"monthly_returns,omitempty"`
	RiskMetrics      *RiskMetrics      `json:"risk_metrics,omitempty"`
	DrawdownData     *DrawdownData     `json:"drawdown_data,omitempty"`
	BenchmarkMetrics *BenchmarkMetrics `json:"benchmark_metrics,omitempty"`

	// Display params (NOT signed - applied per request)
	Manager string `json:"manager,omitempty"`
	Firm    string `json:"firm,omitempty"`

	// Signature
	Signature          string `json:"signature"`
	PublicKey          string `json:"public_key"`
	SignatureAlgorithm string `json:"signature_algorithm"`
	ReportHash         string `json:"report_hash"`
	EnclaveVersion     string `json:"enclave_version"`
}

// Sign creates a signed report from input.
// TS parity:
// 1. Build financial data
// 2. Deterministically serialize with sorted keys
// 3. reportHash = SHA-256(financialDataJSON) hex
// 4. signature = ECDSA-SHA256 sign(reportHash string)
func (s *ReportSigner) Sign(input *ReportInput) (*SignedReport, error) {
	report := &SignedReport{
		ReportID:           uuid.New().String(),
		UserUID:            input.UserUID,
		ReportName:         input.ReportName,
		GeneratedAt:        formatISO8601(time.Now().UTC()),
		PeriodStart:        formatISO8601(input.PeriodStart),
		PeriodEnd:          formatISO8601(input.PeriodEnd),
		TotalReturn:        input.TotalReturn,
		AnnualizedReturn:   input.AnnualizedReturn,
		SharpeRatio:        input.SharpeRatio,
		SortinoRatio:       input.SortinoRatio,
		CalmarRatio:        input.CalmarRatio,
		MaxDrawdown:        input.MaxDrawdown,
		Volatility:         input.Volatility,
		WinRate:            input.WinRate,
		ProfitFactor:       input.ProfitFactor,
		DataPoints:         input.DataPoints,
		BaseCurrency:       input.BaseCurrency,
		Benchmark:          input.BenchmarkUsed,
		Exchanges:          input.Exchanges,
		ExchangeDetails:    input.ExchangeDetails,
		DailyReturns:       input.DailyReturns,
		MonthlyReturns:     input.MonthlyReturns,
		RiskMetrics:        input.RiskMetrics,
		DrawdownData:       input.DrawdownData,
		BenchmarkMetrics:   input.BenchmarkMetrics,
		PublicKey:          s.PublicKey(),
		SignatureAlgorithm: SignatureAlgorithm,
		EnclaveVersion:     EnclaveVersion,
	}

	financialPayload := buildFinancialPayload(report)
	financialJSON, err := marshalSortedJSON(financialPayload)
	if err != nil {
		return nil, fmt.Errorf("serialize financial payload: %w", err)
	}

	hash := sha256.Sum256(financialJSON)
	report.ReportHash = hex.EncodeToString(hash[:])

	reportHashDigest := sha256.Sum256([]byte(report.ReportHash))
	signatureDER, err := ecdsa.SignASN1(rand.Reader, s.privateKey, reportHashDigest[:])
	if err != nil {
		return nil, fmt.Errorf("sign report hash: %w", err)
	}
	report.Signature = base64.StdEncoding.EncodeToString(signatureDER)

	return report, nil
}

func formatISO8601(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.000Z")
}

func buildFinancialPayload(report *SignedReport) map[string]any {
	payload := map[string]any{
		"reportId":     report.ReportID,
		"userUid":      report.UserUID,
		"generatedAt":  report.GeneratedAt,
		"periodStart":  report.PeriodStart,
		"periodEnd":    report.PeriodEnd,
		"baseCurrency": report.BaseCurrency,
		"dataPoints":   report.DataPoints,
		"exchanges":    report.Exchanges,
		"metrics": map[string]any{
			"totalReturn":      report.TotalReturn,
			"annualizedReturn": report.AnnualizedReturn,
			"volatility":       report.Volatility,
			"sharpeRatio":      report.SharpeRatio,
			"sortinoRatio":     report.SortinoRatio,
			"maxDrawdown":      report.MaxDrawdown,
			"calmarRatio":      report.CalmarRatio,
		},
		"dailyReturns":   toDailyReturnsPayload(report.DailyReturns),
		"monthlyReturns": toMonthlyReturnsPayload(report.MonthlyReturns),
	}

	if report.Benchmark != "" {
		payload["benchmark"] = report.Benchmark
	}
	if len(report.ExchangeDetails) > 0 {
		payload["exchangeDetails"] = toExchangeDetailsPayload(report.ExchangeDetails)
	}
	if report.RiskMetrics != nil {
		payload["metrics"].(map[string]any)["riskMetrics"] = map[string]any{
			"var95":             report.RiskMetrics.VaR95,
			"var99":             report.RiskMetrics.VaR99,
			"expectedShortfall": report.RiskMetrics.ExpectedShortfall,
			"skewness":          report.RiskMetrics.Skewness,
			"kurtosis":          report.RiskMetrics.Kurtosis,
		}
	}
	if report.BenchmarkMetrics != nil {
		payload["metrics"].(map[string]any)["benchmarkMetrics"] = map[string]any{
			"alpha":            report.BenchmarkMetrics.Alpha,
			"beta":             report.BenchmarkMetrics.Beta,
			"informationRatio": report.BenchmarkMetrics.InformationRatio,
			"trackingError":    report.BenchmarkMetrics.TrackingError,
			"correlation":      report.BenchmarkMetrics.Correlation,
		}
	}
	if report.DrawdownData != nil {
		payload["metrics"].(map[string]any)["drawdownData"] = map[string]any{
			"maxDrawdownDuration": report.DrawdownData.MaxDrawdownDuration,
			"currentDrawdown":     report.DrawdownData.CurrentDrawdown,
			"drawdownPeriods":     toDrawdownPeriodsPayload(report.DrawdownData.Periods),
		}
	}

	return payload
}

func toExchangeDetailsPayload(in []ExchangeInfo) []map[string]any {
	out := make([]map[string]any, 0, len(in))
	for _, ex := range in {
		out = append(out, map[string]any{
			"name":     ex.Name,
			"kycLevel": ex.KYCLevel,
			"isPaper":  ex.IsPaper,
		})
	}
	return out
}

func toDailyReturnsPayload(in []DailyReturn) []map[string]any {
	out := make([]map[string]any, 0, len(in))
	for _, dr := range in {
		out = append(out, map[string]any{
			"date":             dr.Date,
			"netReturn":        dr.NetReturn,
			"benchmarkReturn":  dr.BenchmarkReturn,
			"outperformance":   dr.Outperformance,
			"cumulativeReturn": dr.CumulativeReturn,
			"nav":              dr.NAV,
		})
	}
	return out
}

func toMonthlyReturnsPayload(in []MonthlyReturn) []map[string]any {
	out := make([]map[string]any, 0, len(in))
	for _, mr := range in {
		out = append(out, map[string]any{
			"date":            mr.Date,
			"netReturn":       mr.NetReturn,
			"benchmarkReturn": mr.BenchmarkReturn,
			"outperformance":  mr.Outperformance,
			"aum":             mr.AUM,
		})
	}
	return out
}

func toDrawdownPeriodsPayload(in []*DrawdownPeriod) []map[string]any {
	out := make([]map[string]any, 0, len(in))
	for _, p := range in {
		out = append(out, map[string]any{
			"startDate": p.StartDate,
			"endDate":   p.EndDate,
			"depth":     p.Depth,
			"duration":  p.Duration,
			"recovered": p.Recovered,
		})
	}
	return out
}

func marshalSortedJSON(v any) ([]byte, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}

	var normalized any
	if err := json.Unmarshal(raw, &normalized); err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	if err := writeSortedJSON(&buf, normalized); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func writeSortedJSON(buf *bytes.Buffer, v any) error {
	switch t := v.(type) {
	case map[string]any:
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		buf.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				buf.WriteByte(',')
			}
			keyBytes, _ := json.Marshal(k)
			buf.Write(keyBytes)
			buf.WriteByte(':')
			if err := writeSortedJSON(buf, t[k]); err != nil {
				return err
			}
		}
		buf.WriteByte('}')
		return nil
	case []any:
		buf.WriteByte('[')
		for i, item := range t {
			if i > 0 {
				buf.WriteByte(',')
			}
			if err := writeSortedJSON(buf, item); err != nil {
				return err
			}
		}
		buf.WriteByte(']')
		return nil
	default:
		b, err := json.Marshal(t)
		if err != nil {
			return err
		}
		buf.Write(b)
		return nil
	}
}

// Verify checks a signature against a report hash.
func Verify(reportHash, signatureB64, publicKey string) (bool, error) {
	// Preferred path: TS parity ECDSA (base64 DER public key).
	signatureDER, sigErr := base64.StdEncoding.DecodeString(signatureB64)
	publicKeyDER, pubErr := base64.StdEncoding.DecodeString(publicKey)
	if sigErr == nil && pubErr == nil {
		parsed, parseErr := x509.ParsePKIXPublicKey(publicKeyDER)
		if parseErr == nil {
			if ecdsaKey, ok := parsed.(*ecdsa.PublicKey); ok {
				reportHashDigest := sha256.Sum256([]byte(reportHash))
				return ecdsa.VerifyASN1(ecdsaKey, reportHashDigest[:], signatureDER), nil
			}
		}
	}

	// Backward compatibility for cached reports signed with legacy Ed25519.
	hash, err := hex.DecodeString(reportHash)
	if err != nil {
		return false, fmt.Errorf("invalid report hash: %w", err)
	}

	signature, err := base64.StdEncoding.DecodeString(signatureB64)
	if err != nil {
		return false, fmt.Errorf("invalid signature: %w", err)
	}

	publicKeyBytes, err := hex.DecodeString(publicKey)
	if err != nil {
		return false, fmt.Errorf("invalid public key: %w", err)
	}
	if len(publicKeyBytes) != ed25519.PublicKeySize {
		return false, fmt.Errorf("invalid public key format")
	}

	return ed25519.Verify(publicKeyBytes, hash, signature), nil
}
