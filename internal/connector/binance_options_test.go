package connector

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestParseBinanceOptionsEquity(t *testing.T) {
	body := []byte(`{"asset":[{"asset":"USDT","marginBalance":"90","equity":"120","available":"60","unrealizedPNL":"30"},{"asset":"BTC","equity":"0.001"},{"asset":"ETH","equity":"0"}],"canTrade":true}`)
	got, err := parseBinanceOptionsEquity(body, map[string]float64{"BTCUSDT": 100000})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	// equity, not marginBalance: a long option's mark value is still capital
	if got != 120+0.001*100000 {
		t.Fatalf("options equity %v, want 220", got)
	}
}

func TestBinance_OptionsCountInEquity(t *testing.T) {
	b := newBinanceEarnTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"rows":[],"total":0}`))
	})
	b.client.Transport = optionsAnswer(b.client.Transport, `{"asset":[{"asset":"USDT","equity":"250"}]}`, http.StatusOK)

	bal, err := b.GetBalance(context.Background())
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if bal.Equity != 10+4*2+250 {
		t.Fatalf("equity %v, want spot 18 + options 250", bal.Equity)
	}
	if !walletRead(b, binanceWalletOption) {
		t.Fatalf("coverage %+v, want options read", b.Coverage())
	}
}

// An account that never opened options is refused on that endpoint. The sync
// carries on, and no warning asks the user to fix a key that is fine.
func TestBinance_OptionsRefusedIsNotAWarning(t *testing.T) {
	b := newBinanceEarnTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"rows":[],"total":0}`))
	})
	b.client.Transport = optionsAnswer(b.client.Transport, `{"code":-2015,"msg":"Invalid API-key, IP, or permissions for action."}`, http.StatusUnauthorized)

	bal, err := b.GetBalance(context.Background())
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if bal.Equity != 10+4*2 {
		t.Fatalf("equity %v, want the spot 18", bal.Equity)
	}
	if walletRead(b, binanceWalletOption) {
		t.Fatalf("coverage %+v, want options unreadable", b.Coverage())
	}
	for _, w := range b.CapabilityWarnings() {
		if strings.Contains(w, "option") {
			t.Fatalf("warnings %v carry an options marker", b.CapabilityWarnings())
		}
	}
}

// A USDⓈ-M to options transfer shows twice: as a universal transfer and as a
// TRANSFER row of the futures ledger. It is one move, booked once at most.
func TestBinance_Cashflows_OptionsTransferBookedOnce(t *testing.T) {
	stamp := time.Now().UTC().Add(-time.Hour).UnixMilli()
	cases := []struct {
		name        string
		optionsRead bool
		want        float64
	}{
		{"options read", true, 0},
		{"options unreadable", false, -100},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := binanceFlowServer(t, func(w http.ResponseWriter, r *http.Request) bool {
				switch {
				case strings.Contains(r.URL.Path, "/eapi/") && !tc.optionsRead:
					w.WriteHeader(http.StatusUnauthorized)
					_, _ = w.Write([]byte(`{"code":-2015,"msg":"Invalid API-key, IP, or permissions for action."}`))
				case strings.Contains(r.URL.Path, "/asset/transfer") && r.URL.Query().Get("type") == "UMFUTURE_OPTION":
					_, _ = fmt.Fprintf(w, `{"total":1,"rows":[{"asset":"USDT","amount":"100","timestamp":%d,"status":"CONFIRMED"}]}`, stamp)
				case strings.Contains(r.URL.Path, "/fapi/v1/income") && r.URL.Query().Get("incomeType") == "TRANSFER":
					_, _ = fmt.Fprintf(w, `[{"asset":"USDT","income":"-100","time":%d}]`, stamp)
				default:
					return false
				}
				return true
			})
			if _, err := b.GetBalance(context.Background()); err != nil {
				t.Fatalf("GetBalance: %v", err)
			}
			flows, err := b.GetCashflows(context.Background(), time.Now().UTC().Add(-24*time.Hour))
			if err != nil {
				t.Fatalf("GetCashflows: %v", err)
			}
			var got float64
			for _, f := range flows {
				got += f.Amount
			}
			if got != tc.want {
				t.Fatalf("booked %v, want %v", got, tc.want)
			}
		})
	}
}

func walletRead(b *Binance, wallet string) bool {
	for _, c := range b.Coverage() {
		if c.Wallet == wallet {
			return c.Status == WalletRead
		}
	}
	return false
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// optionsAnswer answers the options account endpoint itself and hands every
// other request to next.
func optionsAnswer(next http.RoundTripper, body string, status int) http.RoundTripper {
	return roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if !strings.Contains(r.URL.Path, "/eapi/v1/marginAccount") {
			return next.RoundTrip(r)
		}
		return &http.Response{
			StatusCode: status,
			Header:     http.Header{"Content-Type": {"application/json"}},
			Body:       io.NopCloser(strings.NewReader(body)),
			Request:    r,
		}, nil
	})
}
