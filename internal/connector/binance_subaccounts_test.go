package connector

import (
	"context"
	"errors"
	"fmt"
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
