package connector

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"testing"
	"time"
)

// deribitStub answers by path so the tests never dial deribit.com — the API
// base is a const, so intercepting the transport is the only seam.
type deribitStub struct {
	summaryStatus map[string]int // currency -> HTTP status (0 or 200 = serve body)
	summary       map[string]float64
	tickerStatus  int
}

func (s deribitStub) RoundTrip(req *http.Request) (*http.Response, error) {
	reply := func(code int, v any) (*http.Response, error) {
		b, _ := json.Marshal(v)
		return &http.Response{
			StatusCode: code,
			Body:       io.NopCloser(bytes.NewReader(b)),
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Request:    req,
		}, nil
	}

	switch req.URL.Path {
	case "/api/v2/public/auth":
		return reply(200, map[string]any{"result": map[string]any{"access_token": "tok", "expires_in": 3600}})
	case "/api/v2/private/get_account_summary":
		ccy := req.URL.Query().Get("currency")
		if code := s.summaryStatus[ccy]; code != 0 && code != 200 {
			return reply(code, map[string]any{"error": map[string]any{"code": code, "message": "unavailable"}})
		}
		eq := s.summary[ccy]
		return reply(200, map[string]any{"result": map[string]any{
			"equity": eq, "balance": eq, "available_funds": eq,
		}})
	case "/api/v2/public/ticker":
		if s.tickerStatus != 0 && s.tickerStatus != 200 {
			return reply(s.tickerStatus, map[string]any{"error": "down"})
		}
		return reply(200, map[string]any{"result": map[string]any{"last_price": 60000.0}})
	}
	return reply(404, map[string]any{"error": "unknown path " + req.URL.Path})
}

func newStubbedDeribit(s deribitStub) *Deribit {
	d := NewDeribit(&Credentials{APIKey: "k", APISecret: "s"})
	d.client = &http.Client{Transport: s, Timeout: 5 * time.Second}
	return d
}

// CONN-14 path 1: every currency erroring is a failed read, not an empty
// wallet. Returning Equity 0 with a nil error persisted a -100% day followed
// by a rebound the next — the TWR reads that as a total loss then an infinite
// gain.
func TestDeribitGetBalance_AllCurrenciesFailIsAnError(t *testing.T) {
	d := newStubbedDeribit(deribitStub{
		summaryStatus: map[string]int{"BTC": 429, "ETH": 429, "USDC": 429, "USDT": 429},
	})

	bal, err := d.GetBalance(context.Background())
	if err == nil {
		t.Fatalf("want an error when no currency answered, got equity %v", bal.Equity)
	}
	if !errors.Is(err, ErrTransient) {
		t.Errorf("error must be transient so the sync retries, got: %v", err)
	}
}

// CONN-14 path 2: a failed ticker makes usdMultiplier return 0, which used to
// write the BTC sub-account off silently. GetCashflows and GetBalanceByMarket
// already guarded this; GetBalance did not.
func TestDeribitGetBalance_TickerDownFailsInsteadOfZeroing(t *testing.T) {
	d := newStubbedDeribit(deribitStub{
		summary:      map[string]float64{"BTC": 2},
		tickerStatus: http.StatusServiceUnavailable,
	})

	if _, err := d.GetBalance(context.Background()); err == nil {
		t.Fatal("want an error when the BTC ticker is down and BTC is held")
	} else if !errors.Is(err, ErrSpotPricingUnavailable) {
		t.Errorf("error must wrap ErrSpotPricingUnavailable, got: %v", err)
	}
}

// A sub-account that genuinely holds nothing needs no ticker, and one live
// currency is enough to report a balance.
func TestDeribitGetBalance_EmptySubAccountsNeedNoTicker(t *testing.T) {
	d := newStubbedDeribit(deribitStub{
		summary:      map[string]float64{"USDC": 500},
		tickerStatus: http.StatusServiceUnavailable,
	})

	bal, err := d.GetBalance(context.Background())
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if bal.Equity != 500 {
		t.Errorf("equity = %v, want 500", bal.Equity)
	}
}
