package connector

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"testing"
	"time"
)

const (
	testSubEmail    = "sub-a@example.invalid"
	testMasterEmail = "master@example.invalid"
)

// masterAccountServer answers a master key with one sub-account, a spot
// transfer to it, and two USDⓈ-M transfer rows: one explained by a UM→spot
// universal transfer, one explained by nothing (a futures transfer to the
// sub-account made from the app). counterpartStatus, when non-zero, fails
// every universal transfer read of that type with the given status.
func masterAccountServer(t *testing.T, counterpartStatus int) (*Binance, time.Time) {
	t.Helper()
	day := time.Now().UTC().Add(-2 * time.Hour).Truncate(time.Minute)
	ms := func(d time.Duration) int64 { return day.Add(d).UnixMilli() }
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		q := r.URL.Query()
		switch {
		case strings.Contains(r.URL.Path, "/capital/deposit/hisrec"), strings.Contains(r.URL.Path, "/capital/withdraw/history"):
			_, _ = w.Write([]byte(`[]`))
		case strings.Contains(r.URL.Path, "/sub-account/list"):
			_, _ = w.Write([]byte(`{"subAccounts":[{"email":"` + testSubEmail + `"}]}`))
		case strings.Contains(r.URL.Path, "/sub-account/sub/transfer/history"):
			if q.Get("toEmail") == testSubEmail {
				_, _ = fmt.Fprintf(w, `[{"from":"%s","to":"%s","asset":"USDC","qty":"100","status":"SUCCESS","tranId":7,"time":%d}]`, testMasterEmail, testSubEmail, ms(0))
				return
			}
			_, _ = w.Write([]byte(`[]`))
		case strings.Contains(r.URL.Path, "/sub-account/universalTransfer"):
			_, _ = w.Write([]byte(`{"result":[]}`))
		case strings.Contains(r.URL.Path, "/sub-account/transfer/subUserHistory"):
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"code":-12022,"msg":"not a sub-account"}`))
		case strings.Contains(r.URL.Path, "/fapi/v1/income"):
			if q.Get("incomeType") == "TRANSFER" {
				_, _ = fmt.Fprintf(w, `[{"asset":"USDC","income":"-134","time":%d},{"asset":"USDC","income":"-42","time":%d}]`, ms(time.Minute), ms(2*time.Minute))
				return
			}
			_, _ = w.Write([]byte(`[]`))
		case strings.Contains(r.URL.Path, "/asset/transfer"):
			if counterpartStatus != 0 && q.Get("type") == "UMFUTURE_MAIN" {
				w.WriteHeader(counterpartStatus)
				_, _ = w.Write([]byte(`{"code":-1001,"msg":"internal error"}`))
				return
			}
			if q.Get("type") == "UMFUTURE_MAIN" {
				_, _ = fmt.Fprintf(w, `{"total":1,"rows":[{"asset":"USDC","amount":"42","timestamp":%d,"status":"CONFIRMED"}]}`, ms(2*time.Minute))
				return
			}
			_, _ = w.Write([]byte(`{"total":0,"rows":[]}`))
		default:
			_, _ = w.Write([]byte(`{}`))
		}
	}))
	t.Cleanup(srv.Close)
	target, _ := url.Parse(srv.URL)
	client := &http.Client{Transport: hostRewriter{base: http.DefaultTransport, target: target}}
	return NewBinanceWithClient(&Credentials{APIKey: "k", APISecret: "s"}, client), day.Add(-time.Hour)
}

func cashflowAmounts(flows []*Cashflow) []float64 {
	out := make([]float64, 0, len(flows))
	for _, f := range flows {
		out = append(out, f.Amount)
	}
	sort.Float64s(out)
	return out
}

// A master funding its sub-account books a withdrawal for the spot transfer
// and for the futures transfer row nothing else explains, and nothing for the
// futures→spot shuttle its universal transfer explains.
func TestBinance_Cashflows_MasterToSubAccountIsAWithdrawal(t *testing.T) {
	b, since := masterAccountServer(t, 0)
	flows, err := b.GetCashflows(context.Background(), since)
	if err != nil {
		t.Fatalf("GetCashflows: %v", err)
	}
	got := cashflowAmounts(flows)
	if len(got) != 2 || got[0] != -134 || got[1] != -100 {
		t.Fatalf("want withdrawals [-134 -100], got %v", got)
	}
}

// When the transfer history cannot be read, a futures row's counterpart is
// unknown: booking it would turn the futures→spot shuttle into a fabricated
// withdrawal, so the stray pass is skipped and only the sub-account transfer
// is booked.
func TestBinance_Cashflows_UnreadableCounterpartsSkipStrayRows(t *testing.T) {
	b, since := masterAccountServer(t, http.StatusServiceUnavailable)
	flows, err := b.GetCashflows(context.Background(), since)
	if err != nil {
		t.Fatalf("GetCashflows: %v", err)
	}
	got := cashflowAmounts(flows)
	if len(got) != 1 || got[0] != -100 {
		t.Fatalf("want only the sub-account transfer [-100], got %v", got)
	}
}

// subAccountServer answers a sub-account key whose master topped up its
// futures wallet: subUserHistory reports the transfer as type 2, the sub's
// USDⓈ-M income ledger shows the money arriving.
func subAccountServer(t *testing.T) (*Binance, time.Time) {
	t.Helper()
	day := time.Now().UTC().Add(-2 * time.Hour).Truncate(time.Minute)
	ms := day.UnixMilli()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(r.URL.Path, "/capital/deposit/hisrec"), strings.Contains(r.URL.Path, "/capital/withdraw/history"):
			_, _ = w.Write([]byte(`[]`))
		case strings.Contains(r.URL.Path, "/sub-account/list"):
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"code":-12022,"msg":"not a master"}`))
		case strings.Contains(r.URL.Path, "/sub-account/transfer/subUserHistory"):
			_, _ = fmt.Fprintf(w, `[{"counterParty":"master","email":"%s","type":2,"asset":"USDT","qty":"500","status":"SUCCESS","time":%d}]`, testMasterEmail, ms)
		case strings.Contains(r.URL.Path, "/fapi/v1/income"):
			if r.URL.Query().Get("incomeType") == "TRANSFER" {
				_, _ = fmt.Fprintf(w, `[{"asset":"USDT","income":"500","time":%d}]`, ms+1000)
				return
			}
			_, _ = w.Write([]byte(`[]`))
		case strings.Contains(r.URL.Path, "/asset/transfer"):
			_, _ = w.Write([]byte(`{"total":0,"rows":[]}`))
		default:
			_, _ = w.Write([]byte(`{}`))
		}
	}))
	t.Cleanup(srv.Close)
	target, _ := url.Parse(srv.URL)
	client := &http.Client{Transport: hostRewriter{base: http.DefaultTransport, target: target}}
	return NewBinanceWithClient(&Credentials{APIKey: "k", APISecret: "s"}, client), day.Add(-time.Hour)
}

// The master's top-up of the sub's futures wallet is a deposit: the sub's own
// ledger wins over subUserHistory's type, and the row is booked once.
func TestBinance_Cashflows_MasterFundingOfSubFuturesIsADeposit(t *testing.T) {
	b, since := subAccountServer(t)
	flows, err := b.GetCashflows(context.Background(), since)
	if err != nil {
		t.Fatalf("GetCashflows: %v", err)
	}
	got := cashflowAmounts(flows)
	if len(got) != 1 || got[0] != 500 {
		t.Fatalf("want one deposit [500], got %v", got)
	}
}

// Received in spot then shuttled to futures: the universal transfer explains
// the income row, so the sub-account transfer keeps its read.
func TestOrientBinanceSubMoves_ShuttleKeepsTheRead(t *testing.T) {
	at := time.Date(2026, 6, 1, 9, 0, 0, 0, time.UTC)
	read := binanceMove{t: at, coin: "USDT", qty: 500}
	out := orientBinanceSubMoves(
		[]binanceMove{read},
		[]binanceMove{{t: at.Add(2 * time.Minute), coin: "USDT", qty: 500}},
		[]binanceMove{{t: at.Add(2 * time.Minute), coin: "USDT", qty: 500}})
	if len(out) != 1 || out[0] != read {
		t.Fatalf("a shuttled transfer must stay as read, got %+v", out)
	}
}

func TestStrayBinanceMoves(t *testing.T) {
	at := time.Date(2026, 6, 1, 9, 0, 0, 0, time.UTC)
	moves := []binanceMove{
		{t: at, coin: "USDT", qty: -10},
		{t: at.Add(time.Minute), coin: "USDT", qty: -10},
		{t: at, coin: "USDC", qty: -5},
	}
	counterparts := []binanceMove{
		{t: at, coin: "USDT", qty: 10},
		{t: at.Add(3 * time.Hour), coin: "USDC", qty: 5},
	}
	out := strayBinanceMoves(moves, counterparts)
	if len(out) != 2 {
		t.Fatalf("one USDT row is explained, its twin and the distant USDC row are not; got %+v", out)
	}
}

func TestBinanceMasterSubSign(t *testing.T) {
	subs := map[string]bool{testSubEmail: true, "sub-b@example.invalid": true}
	cases := []struct {
		from, to string
		want     float64
	}{
		{testMasterEmail, testSubEmail, -1},
		{testSubEmail, testMasterEmail, +1},
		{"", testSubEmail, -1},
		{testSubEmail, "sub-b@example.invalid", 0},
	}
	for _, c := range cases {
		if got := binanceMasterSubSign(c.from, c.to, subs); got != c.want {
			t.Errorf("%s → %s: got %v, want %v", c.from, c.to, got, c.want)
		}
	}
}

func TestIsBinanceRefusal(t *testing.T) {
	cases := map[error]bool{
		errors.New("HTTP 400: {\"code\":-2015}"): true,
		errors.New("HTTP 403: forbidden"):        true,
		errors.New("HTTP 429: too many"):         false,
		errors.New("HTTP 418: banned"):           false,
		fmt.Errorf("%w: HTTP 503", ErrTransient): false,
		errors.New("dial tcp: i/o timeout"):      false,
	}
	for err, want := range cases {
		if got := isBinanceRefusal(err); got != want {
			t.Errorf("%v: got %v, want %v", err, got, want)
		}
	}
}

// A bank withdrawal is a cashflow: an account used as an off-ramp (crypto in,
// converted to EUR, EUR wired out) must not read the wire as a trading loss.
// The funding wallet sits outside the equity: an on-chain payout from it, or a
// deposit landing in it, moves nothing the equity measures. The deposit enters
// the equity later, when FUNDING_MAIN moves it to spot. Spot-side on-chain
// flows are booked as before.
func TestBinance_Cashflows_FundingWalletOnChainFlowsStayOutside(t *testing.T) {
	stamp := time.Now().UTC().Add(-time.Hour)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(r.URL.Path, "/capital/deposit/hisrec"):
			_, _ = fmt.Fprintf(w, `[{"coin":"USDT","amount":"40","insertTime":%d,"walletType":0},{"coin":"USDT","amount":"500","insertTime":%d,"walletType":1}]`, stamp.UnixMilli(), stamp.UnixMilli())
		case strings.Contains(r.URL.Path, "/capital/withdraw/history"):
			at := stamp.Format("2006-01-02 15:04:05")
			_, _ = fmt.Fprintf(w, `[{"coin":"USDT","amount":"9","transactionFee":"1","completeTime":"%s","walletType":0},{"coin":"USDT","amount":"299","transactionFee":"1","completeTime":"%s","walletType":1}]`, at, at)
		case strings.Contains(r.URL.Path, "/asset/transfer") && r.URL.Query().Get("type") == "FUNDING_MAIN":
			_, _ = fmt.Fprintf(w, `{"total":1,"rows":[{"asset":"USDT","amount":"200","timestamp":%d,"status":"CONFIRMED"}]}`, stamp.UnixMilli())
		case strings.Contains(r.URL.Path, "/asset/transfer"):
			_, _ = w.Write([]byte(`{"total":0,"rows":[]}`))
		case strings.Contains(r.URL.Path, "/fiat/"):
			_, _ = w.Write([]byte(`{"data":[]}`))
		case strings.Contains(r.URL.Path, "/sub-account/"):
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"code":-12022,"msg":"not a master"}`))
		case strings.Contains(r.URL.Path, "/fapi/v1/income"):
			_, _ = w.Write([]byte(`[]`))
		default:
			_, _ = w.Write([]byte(`{}`))
		}
	}))
	t.Cleanup(srv.Close)
	target, _ := url.Parse(srv.URL)
	client := &http.Client{Transport: hostRewriter{base: http.DefaultTransport, target: target}}
	b := NewBinanceWithClient(&Credentials{APIKey: "k", APISecret: "s"}, client)

	flows, err := b.GetCashflows(context.Background(), stamp.Add(-time.Hour))
	if err != nil {
		t.Fatalf("GetCashflows: %v", err)
	}
	got := cashflowAmounts(flows)
	want := []float64{-10, 40, 200}
	if len(got) != len(want) {
		t.Fatalf("want %v, got %v", want, got)
	}
	for i := range want {
		if math.Abs(got[i]-want[i]) > 1e-9 {
			t.Fatalf("want %v, got %v", want, got)
		}
	}
}

// The universal transfer history answers only what the master sent unless it
// is asked by sub-account email: money a sub-account sends back to its master
// is an inflow, read under the sub's email, and booked once.
func TestBinance_Cashflows_SubToMasterUniversalTransferIsADeposit(t *testing.T) {
	stamp := time.Now().UTC().Add(-time.Hour).UnixMilli()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		q := r.URL.Query()
		switch {
		case strings.Contains(r.URL.Path, "/capital/"):
			_, _ = w.Write([]byte(`[]`))
		case strings.Contains(r.URL.Path, "/sub-account/list"):
			_, _ = w.Write([]byte(`{"subAccounts":[{"email":"` + testSubEmail + `"}]}`))
		case strings.Contains(r.URL.Path, "/sub-account/sub/transfer/history"):
			_, _ = w.Write([]byte(`[]`))
		case strings.Contains(r.URL.Path, "/sub-account/universalTransfer") && q.Get("fromEmail") == testSubEmail:
			_, _ = fmt.Fprintf(w, `{"result":[{"tranId":9,"fromEmail":"%s","toEmail":"%s","asset":"USDT","amount":"250","status":"SUCCESS","createTimeStamp":%d}]}`, testSubEmail, testMasterEmail, stamp)
		case strings.Contains(r.URL.Path, "/sub-account/universalTransfer"):
			_, _ = fmt.Fprintf(w, `{"result":[{"tranId":8,"fromEmail":"%s","toEmail":"%s","asset":"USDT","amount":"100","status":"SUCCESS","createTimeStamp":%d}]}`, testMasterEmail, testSubEmail, stamp)
		case strings.Contains(r.URL.Path, "/sub-account/transfer/subUserHistory"):
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"code":-12022,"msg":"not a sub-account"}`))
		case strings.Contains(r.URL.Path, "/fapi/v1/income"):
			_, _ = w.Write([]byte(`[]`))
		case strings.Contains(r.URL.Path, "/asset/transfer"):
			_, _ = w.Write([]byte(`{"total":0,"rows":[]}`))
		case strings.Contains(r.URL.Path, "/fiat/"):
			_, _ = w.Write([]byte(`{"data":[]}`))
		default:
			_, _ = w.Write([]byte(`{}`))
		}
	}))
	t.Cleanup(srv.Close)
	target, _ := url.Parse(srv.URL)
	client := &http.Client{Transport: hostRewriter{base: http.DefaultTransport, target: target}}
	b := NewBinanceWithClient(&Credentials{APIKey: "k", APISecret: "s"}, client)

	flows, err := b.GetCashflows(context.Background(), time.Now().UTC().Add(-24*time.Hour))
	if err != nil {
		t.Fatalf("GetCashflows: %v", err)
	}
	got := cashflowAmounts(flows)
	if len(got) != 2 || got[0] != -100 || got[1] != 250 {
		t.Fatalf("want the master's transfer out [-100] and the sub's transfer back [250], got %v", got)
	}
}

// A Binance Pay receipt leaves no deposit record: read from the Pay ledger,
// it is booked as an inflow, and a payment out as an outflow.
func TestBinance_Cashflows_PayTransfersAreFlows(t *testing.T) {
	stamp := time.Now().UTC().Add(-time.Hour).UnixMilli()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(r.URL.Path, "/capital/"):
			_, _ = w.Write([]byte(`[]`))
		case strings.Contains(r.URL.Path, "/pay/transactions"):
			_, _ = fmt.Fprintf(w, `{"code":"000000","data":[{"amount":"300","currency":"USDT","transactionTime":%d},{"amount":"-20","currency":"USDT","transactionTime":%d}]}`, stamp, stamp)
		case strings.Contains(r.URL.Path, "/sub-account/"):
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"code":-12022,"msg":"not a master"}`))
		case strings.Contains(r.URL.Path, "/fapi/v1/income"):
			_, _ = w.Write([]byte(`[]`))
		case strings.Contains(r.URL.Path, "/asset/transfer"):
			_, _ = w.Write([]byte(`{"total":0,"rows":[]}`))
		case strings.Contains(r.URL.Path, "/fiat/"):
			_, _ = w.Write([]byte(`{"data":[]}`))
		default:
			_, _ = w.Write([]byte(`{}`))
		}
	}))
	t.Cleanup(srv.Close)
	target, _ := url.Parse(srv.URL)
	client := &http.Client{Transport: hostRewriter{base: http.DefaultTransport, target: target}}
	b := NewBinanceWithClient(&Credentials{APIKey: "k", APISecret: "s"}, client)

	flows, err := b.GetCashflows(context.Background(), time.Now().UTC().Add(-24*time.Hour))
	if err != nil {
		t.Fatalf("GetCashflows: %v", err)
	}
	got := cashflowAmounts(flows)
	if len(got) != 2 || got[0] != -20 || got[1] != 300 {
		t.Fatalf("want the payment out [-20] and the receipt [300], got %v", got)
	}
}

// The raw dump books the same flows as GetCashflows, each named by its ledger
// and asset, so a flow the equity never showed can be traced.
func TestBinance_RawCashflowEntries_NameTheirSource(t *testing.T) {
	b, since := masterAccountServer(t, 0)
	ops, err := b.GetRawCashflowEntries(context.Background(), since)
	if err != nil {
		t.Fatalf("GetRawCashflowEntries: %v", err)
	}
	got := map[string]float64{}
	for _, op := range ops {
		if op.Currency != "USDC" {
			t.Fatalf("want the moved asset on every entry, got %+v", op)
		}
		got[op.Label] += op.Delta
	}
	if len(got) != 2 || got["master_sub"] != -100 || got["stray_um"] != -134 {
		t.Fatalf("want master_sub -100 and stray_um -134, got %v", got)
	}
}

func TestBinance_Cashflows_BankWithdrawalIsAWithdrawal(t *testing.T) {
	stamp := time.Now().UTC().Add(-time.Hour).UnixMilli()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		q := r.URL.Query()
		switch {
		case strings.Contains(r.URL.Path, "/api/v3/ticker/price"):
			_, _ = w.Write([]byte(`[{"symbol":"EURUSDT","price":"1.10"}]`))
		case strings.Contains(r.URL.Path, "/capital/"):
			_, _ = w.Write([]byte(`[]`))
		case strings.Contains(r.URL.Path, "/fiat/orders") && q.Get("transactionType") == "1":
			_, _ = fmt.Fprintf(w, `{"data":[{"fiatCurrency":"EUR","indicatedAmount":"300","amount":"299","totalFee":"1","status":"Successful","createTime":%d},{"fiatCurrency":"EUR","indicatedAmount":"80","amount":"80","status":"Failed","createTime":%d}]}`, stamp, stamp)
		case strings.Contains(r.URL.Path, "/fiat/"):
			_, _ = w.Write([]byte(`{"data":[]}`))
		case strings.Contains(r.URL.Path, "/sub-account/"):
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"code":-12022,"msg":"not a master"}`))
		case strings.Contains(r.URL.Path, "/fapi/v1/income"):
			_, _ = w.Write([]byte(`[]`))
		case strings.Contains(r.URL.Path, "/asset/transfer"):
			_, _ = w.Write([]byte(`{"total":0,"rows":[]}`))
		default:
			_, _ = w.Write([]byte(`{}`))
		}
	}))
	t.Cleanup(srv.Close)
	target, _ := url.Parse(srv.URL)
	client := &http.Client{Transport: hostRewriter{base: http.DefaultTransport, target: target}}
	b := NewBinanceWithClient(&Credentials{APIKey: "k", APISecret: "s"}, client)

	flows, err := b.GetCashflows(context.Background(), time.Now().UTC().Add(-24*time.Hour))
	if err != nil {
		t.Fatalf("GetCashflows: %v", err)
	}
	got := cashflowAmounts(flows)
	if len(got) != 1 || math.Abs(got[0]+330) > 1e-9 {
		t.Fatalf("want one withdrawal of 300 EUR at 1.10 = -330, got %v", got)
	}
}
