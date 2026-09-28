package connector

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"
)

func binanceFlowServer(t *testing.T, route func(w http.ResponseWriter, r *http.Request) bool) *Binance {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if route(w, r) {
			return
		}
		switch {
		case strings.Contains(r.URL.Path, "/capital/"):
			_, _ = w.Write([]byte(`[]`))
		case strings.Contains(r.URL.Path, "/sub-account/"):
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"code":-12022,"msg":"not a master"}`))
		case strings.Contains(r.URL.Path, "/fapi/v1/income"):
			_, _ = w.Write([]byte(`[]`))
		case strings.Contains(r.URL.Path, "/fiat/"):
			_, _ = w.Write([]byte(`{"data":[]}`))
		case strings.Contains(r.URL.Path, "/asset/transfer"):
			_, _ = w.Write([]byte(`{"total":0,"rows":[]}`))
		case strings.Contains(r.URL.Path, "/api/v3/ticker/price"):
			_, _ = w.Write([]byte(`[]`))
		default:
			_, _ = w.Write([]byte(`{}`))
		}
	}))
	t.Cleanup(srv.Close)
	target, _ := url.Parse(srv.URL)
	client := &http.Client{Transport: hostRewriter{base: http.DefaultTransport, target: target}}
	return NewBinanceWithClient(&Credentials{APIKey: "k", APISecret: "s"}, client)
}

func TestBinance_Cashflows_UniversalTransferSigns(t *testing.T) {
	cases := []struct {
		typ  string
		want float64
	}{
		{"FUNDING_MAIN", 100}, {"MAIN_FUNDING", -100},
		{"FUNDING_UMFUTURE", 100}, {"UMFUTURE_FUNDING", -100},
		{"MAIN_OPTION", -100}, {"OPTION_MAIN", 100},
		{"MAIN_MINING", -100}, {"MINING_MAIN", 100},
		{"MAIN_UMFUTURE", 0},
		// CONN-07: margin and COIN-M sit inside the live equity, so these
		// move value between tracked wallets.
		{"MARGIN_MAIN", 100}, {"MAIN_MARGIN", -100},
		{"CMFUTURE_MAIN", 100}, {"MAIN_CMFUTURE", -100},
		// CONN-07: and these cross the perimeter without being read.
		{"FUNDING_MARGIN", 0}, {"MARGIN_FUNDING", 0},
		{"FUNDING_CMFUTURE", 0}, {"CMFUTURE_FUNDING", 0},
	}
	stamp := time.Now().UTC().Add(-time.Hour).UnixMilli()
	for _, tc := range cases {
		t.Run(tc.typ, func(t *testing.T) {
			b := binanceFlowServer(t, func(w http.ResponseWriter, r *http.Request) bool {
				if strings.Contains(r.URL.Path, "/asset/transfer") && r.URL.Query().Get("type") == tc.typ {
					_, _ = fmt.Fprintf(w, `{"total":1,"rows":[{"asset":"USDT","amount":"100","timestamp":%d,"status":"CONFIRMED"}]}`, stamp)
					return true
				}
				return false
			})
			flows, err := b.GetCashflows(context.Background(), time.Now().UTC().Add(-24*time.Hour))
			if err != nil {
				t.Fatalf("GetCashflows: %v", err)
			}
			var got float64
			for _, f := range flows {
				got += f.Amount
			}
			if got != tc.want {
				t.Errorf("%s booked %v, want %v", tc.typ, got, tc.want)
			}
		})
	}
}

func TestBinance_Cashflows_UnpricedDepositIsDroppedSilently(t *testing.T) {
	stamp := time.Now().UTC().Add(-time.Hour).UnixMilli()
	cases := []struct {
		name   string
		ticker func(w http.ResponseWriter)
	}{
		{"ticker down", func(w http.ResponseWriter) {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"code":-1003,"msg":"banned"}`))
		}},
		{"coin not listed", func(w http.ResponseWriter) {
			_, _ = w.Write([]byte(`[{"symbol":"ETHUSDT","price":"2000"}]`))
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := binanceFlowServer(t, func(w http.ResponseWriter, r *http.Request) bool {
				switch {
				case strings.Contains(r.URL.Path, "/capital/deposit/hisrec"):
					_, _ = fmt.Fprintf(w, `[{"coin":"BTC","amount":"0.5","insertTime":%d},{"coin":"USDT","amount":"100","insertTime":%d}]`, stamp, stamp)
					return true
				case strings.Contains(r.URL.Path, "/api/v3/ticker/price"):
					tc.ticker(w)
					return true
				}
				return false
			})
			flows, err := b.GetCashflows(context.Background(), time.Now().UTC().Add(-24*time.Hour))
			if err != nil {
				t.Fatalf("GetCashflows: %v", err)
			}
			if got := cashflowAmounts(flows); !slices.Equal(got, []float64{100}) {
				t.Errorf("flows = %v, want only the stablecoin deposit", got)
			}
			// The dropped BTC deposit leaves no trace.
			if w := b.CapabilityWarnings(); len(w) != 0 {
				t.Errorf("warnings = %v, want none", w)
			}
		})
	}
}

func TestMEXC_Cashflows_UnpricedDepositIsDroppedSilently(t *testing.T) {
	stamp := time.Now().UTC().Add(-time.Hour).UnixMilli()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(r.URL.Path, "/capital/deposit/hisrec"):
			_, _ = fmt.Fprintf(w, `[{"coin":"BTC","amount":"0.5","insertTime":%d,"status":1},{"coin":"USDT","amount":"100","insertTime":%d,"status":1}]`, stamp, stamp)
		case strings.Contains(r.URL.Path, "/ticker/price"):
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"code":700003,"msg":"banned"}`))
		default:
			_, _ = w.Write([]byte(`[]`))
		}
	}))
	t.Cleanup(srv.Close)
	m := NewMEXC(&Credentials{APIKey: "k", APISecret: "s"})
	m.base.BaseURL = srv.URL

	flows, err := m.GetCashflows(context.Background(), time.Now().UTC().Add(-24*time.Hour))
	if err != nil {
		t.Fatalf("GetCashflows: %v", err)
	}
	if got := cashflowAmounts(flows); !slices.Equal(got, []float64{100}) {
		t.Errorf("flows = %v, want only the stablecoin deposit", got)
	}
}

type bitgetLedger struct {
	spot       string
	mixStatus  int
	mixBody    string
	tickerDown bool
}

func bitgetFlowServer(t *testing.T, l bitgetLedger) *Bitget {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(r.URL.Path, "/spot/account/bills"):
			_, _ = fmt.Fprintf(w, `{"code":"00000","msg":"success","data":[%s]}`, l.spot)
		case strings.Contains(r.URL.Path, "/mix/account/bill"):
			if r.URL.Query().Get("productType") != "USDT-FUTURES" {
				_, _ = w.Write([]byte(`{"code":"00000","data":{"bills":[],"endId":""}}`))
				return
			}
			if l.mixStatus != 0 {
				w.WriteHeader(l.mixStatus)
			}
			_, _ = w.Write([]byte(l.mixBody))
		case strings.Contains(r.URL.Path, "/spot/market/tickers"):
			if l.tickerDown {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"code":"429","msg":"too many requests"}`))
				return
			}
			_, _ = w.Write([]byte(`{"code":"00000","data":[{"symbol":"BTCUSDT","lastPr":"60000"}]}`))
		default:
			_, _ = w.Write([]byte(`{"code":"00000","data":[]}`))
		}
	}))
	t.Cleanup(srv.Close)
	b := NewBitget(&Credentials{APIKey: "k", APISecret: "cw==", Passphrase: "p"})
	b.base.BaseURL = srv.URL
	return b
}

func bitgetSpotBills(stamp int64, rows ...string) string {
	out := make([]string, 0, len(rows))
	for i, r := range rows {
		parts := strings.Split(r, ":")
		out = append(out, fmt.Sprintf(`{"billId":"%d","cTime":"%d","coin":"%s","groupType":"%s","size":"%s"}`, 100-i, stamp, parts[0], parts[1], parts[2]))
	}
	return strings.Join(out, ",")
}

func TestBitget_Cashflows(t *testing.T) {
	stamp := time.Now().UTC().Add(-time.Hour).UnixMilli()
	spot := bitgetSpotBills(stamp, "USDT:deposit:100", "USDT:withdraw:-30", "USDT:transfer:-50")
	matchedMix := fmt.Sprintf(`{"code":"00000","data":{"bills":[{"cTime":"%d","amount":"50","businessType":"trans_from_exchange"}],"endId":""}}`, stamp)

	cases := []struct {
		name    string
		ledger  bitgetLedger
		want    []float64
		wantErr error
	}{
		{
			name:   "transfer to futures matched and cancelled",
			ledger: bitgetLedger{spot: spot, mixBody: matchedMix},
			want:   []float64{-30, 100},
		},
		{
			// Stays after CONN-09: without futures scope the futures wallet
			// is outside the equity too, so the leg did leave the perimeter.
			name: "futures ledger refused for permission",
			ledger: bitgetLedger{spot: spot, mixStatus: http.StatusBadRequest,
				mixBody: `{"code":"40014","msg":"Incorrect permissions"}`},
			want: []float64{-50, -30, 100},
		},
		{
			// CONN-09: the futures leg is unknown, not absent.
			name: "futures ledger failing otherwise",
			ledger: bitgetLedger{spot: spot, mixStatus: http.StatusBadRequest,
				mixBody: `{"code":"40808","msg":"Parameter verification exception"}`},
			want: []float64{-50, -30, 100},
		},
		{
			name: "non-stable deposit priced",
			ledger: bitgetLedger{spot: bitgetSpotBills(stamp, "BTC:deposit:0.5", "USDT:deposit:100"),
				mixBody: `{"code":"00000","data":{"bills":[],"endId":""}}`},
			want: []float64{100, 30000},
		},
		{
			// The stablecoin deposit is lost with the unpriced one.
			name: "non-stable deposit with tickers down",
			ledger: bitgetLedger{spot: bitgetSpotBills(stamp, "BTC:deposit:0.5", "USDT:deposit:100"),
				mixBody: `{"code":"00000","data":{"bills":[],"endId":""}}`, tickerDown: true},
			wantErr: ErrSpotPricingUnavailable,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := bitgetFlowServer(t, tc.ledger)
			flows, err := b.GetCashflows(context.Background(), time.Now().UTC().Add(-24*time.Hour))
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("err = %v, want %v", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("GetCashflows: %v", err)
			}
			if got := cashflowAmounts(flows); !slices.Equal(got, tc.want) {
				t.Errorf("flows = %v, want %v", got, tc.want)
			}
		})
	}
}
