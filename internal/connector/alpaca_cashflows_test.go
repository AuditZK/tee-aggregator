package connector

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func newAlpacaTestConnector(t *testing.T, handler http.HandlerFunc) *Alpaca {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return &Alpaca{
		apiKey:    "AKTEST",
		apiSecret: "secret",
		client:    srv.Client(),
		baseURL:   srv.URL,
	}
}

// A deposit that is not returned here raises the equity with no recorded
// inflow, and the step scores as a gain. This connector returned nil for every
// account, so every Alpaca deposit was phantom performance.
func TestAlpacaGetCashflows_ReadsCapitalActivities(t *testing.T) {
	a := newAlpacaTestConnector(t, func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("activity_types"); got != "CSD,CSW,JNLC,ACATC" {
			// The security-transfer probe asks for its own types; answer empty.
			w.Write([]byte(`[]`))
			return
		}
		if r.URL.Query().Get("page_token") != "" {
			w.Write([]byte(`[]`))
			return
		}
		w.Write([]byte(`[
			{"id":"a1","activity_type":"CSD","date":"2026-08-04","net_amount":"73879.27"},
			{"id":"a2","activity_type":"CSW","date":"2026-08-20","net_amount":"-5000"},
			{"id":"a3","activity_type":"JNLC","date":"2026-08-06","net_amount":"56430.91"},
			{"id":"a4","activity_type":"ACATC","date":"2026-08-07","net_amount":"250.5"}
		]`))
	})

	flows, err := a.GetCashflows(context.Background(), time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("GetCashflows: %v", err)
	}
	if len(flows) != 4 {
		t.Fatalf("got %d flows, want 4: %+v", len(flows), flows)
	}

	want := []float64{73879.27, -5000, 56430.91, 250.5}
	for i, w := range want {
		if flows[i].Amount != w {
			t.Errorf("flow %d amount = %v, want %v", i, flows[i].Amount, w)
		}
		if flows[i].Currency != "USD" {
			t.Errorf("flow %d currency = %q, want USD", i, flows[i].Currency)
		}
	}
	if flows[0].Timestamp.Format("2006-01-02") != "2026-08-04" {
		t.Errorf("timestamp = %v, want 2026-08-04", flows[0].Timestamp)
	}
	// A withdrawal must stay negative: booked as a deposit it would erase the
	// very drawdown it caused.
	if flows[1].Amount >= 0 {
		t.Errorf("withdrawal booked as %v — sign lost", flows[1].Amount)
	}
}

func TestAlpacaGetCashflows_FollowsPagination(t *testing.T) {
	var calls int
	a := newAlpacaTestConnector(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("activity_types") != "CSD,CSW,JNLC,ACATC" {
			w.Write([]byte(`[]`))
			return
		}
		calls++
		switch r.URL.Query().Get("page_token") {
		case "":
			w.Write([]byte(`[{"id":"p1","activity_type":"CSD","date":"2026-08-01","net_amount":"100"}]`))
		case "p1":
			w.Write([]byte(`[{"id":"p2","activity_type":"CSD","date":"2026-08-02","net_amount":"200"}]`))
		default:
			w.Write([]byte(`[]`))
		}
	})

	flows, err := a.GetCashflows(context.Background(), time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("GetCashflows: %v", err)
	}
	if len(flows) != 2 {
		t.Fatalf("got %d flows, want 2 — pagination stopped early: %+v", len(flows), flows)
	}
	if calls < 3 {
		t.Errorf("made %d capital calls, want 3 (two pages then the empty one)", calls)
	}
}

// Securities crossing the account boundary carry no cash, so they cannot be
// booked as capital — and their market value still lands in equity, where it
// reads as a gain. They must leave a mark rather than vanish.
func TestAlpacaGetCashflows_FlagsSecurityTransfers(t *testing.T) {
	a := newAlpacaTestConnector(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Query().Get("activity_types") {
		case "ACATS,JNLS":
			if r.URL.Query().Get("page_token") != "" {
				w.Write([]byte(`[]`))
				return
			}
			w.Write([]byte(`[{"id":"t1","activity_type":"ACATS","date":"2026-08-06","net_amount":"0"}]`))
		default:
			w.Write([]byte(`[]`))
		}
	})

	flows, err := a.GetCashflows(context.Background(), time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("GetCashflows: %v", err)
	}
	if len(flows) != 0 {
		t.Fatalf("a share transfer must not be booked as a cashflow: %+v", flows)
	}

	warnings := a.CapabilityWarnings()
	if len(warnings) != 1 || warnings[0] != "security_transfer_not_capital:1" {
		t.Fatalf("warnings = %v, want the share transfer reported", warnings)
	}
}

func TestAlpacaGetCashflows_IgnoresUnparseableEntries(t *testing.T) {
	a := newAlpacaTestConnector(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("activity_types") != "CSD,CSW,JNLC,ACATC" {
			w.Write([]byte(`[]`))
			return
		}
		if r.URL.Query().Get("page_token") != "" {
			w.Write([]byte(`[]`))
			return
		}
		w.Write([]byte(`[
			{"id":"b1","activity_type":"CSD","date":"2026-08-04","net_amount":""},
			{"id":"b2","activity_type":"CSD","date":"","net_amount":"500"},
			{"id":"b3","activity_type":"CSD","date":"2026-08-05","net_amount":"0"},
			{"id":"b4","activity_type":"CSD","date":"2026-08-06","net_amount":"42"}
		]`))
	})

	flows, err := a.GetCashflows(context.Background(), time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("GetCashflows: %v", err)
	}
	if len(flows) != 1 || flows[0].Amount != 42 {
		t.Fatalf("only the well-formed entry should survive: %+v", flows)
	}
}
