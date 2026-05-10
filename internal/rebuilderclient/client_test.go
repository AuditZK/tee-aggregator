package rebuilderclient

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"
)

func TestClient_NotConfigured(t *testing.T) {
	c := New("", "", zap.NewNop())
	if c.Configured() {
		t.Fatal("expected Configured()=false when baseURL+token empty")
	}
	_, err := c.Rebuild(context.Background(), RebuildRequest{})
	if err == nil {
		t.Fatal("expected error when not configured")
	}
}

func TestClient_Rebuild_SendsAuthHeaderAndPayload(t *testing.T) {
	var gotToken string
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotToken = r.Header.Get("X-Internal-Token")
		body, _ := io.ReadAll(r.Body)
		gotBody = body
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"exchange":"hyperliquid","count":2,"durationMs":1234,
			"snapshots":[
				{"date":"2026-04-01T00:00:00Z","totalEquity":1000.5,"realizedBalance":900,"deposits":100,"withdrawals":0,"totalTrades":3,"totalVolume":50,"totalFees":0.25,
				 "breakdown":{"swap":{"marketType":"swap","equity":1000.5,"availableMargin":600}}},
				{"date":"2026-04-02T00:00:00Z","totalEquity":1010.75,"realizedBalance":900,"deposits":0,"withdrawals":0,"totalTrades":0,"totalVolume":0,"totalFees":0}
			]
		}`))
	}))
	defer srv.Close()

	c := New(srv.URL, "secret-token", zap.NewNop())
	res, err := c.Rebuild(context.Background(), RebuildRequest{
		UserUID:     "u1",
		Exchange:    "hyperliquid",
		Label:       "main",
		Credentials: Credentials{WalletAddress: "0xabc"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := len(res.Snapshots); got != 2 {
		t.Fatalf("snapshot count: got %d want 2", got)
	}
	if res.DurationMs != 1234 {
		t.Errorf("durationMs: got %d want 1234", res.DurationMs)
	}
	first := res.Snapshots[0]
	if first.TotalEquity != 1000.5 || first.TotalTrades != 3 || first.Deposits != 100 {
		t.Errorf("first snapshot mapping wrong: %+v", first)
	}
	if first.Date.Year() != 2026 || first.Date.Month() != time.April || first.Date.Day() != 1 {
		t.Errorf("first snapshot date: got %v want 2026-04-01", first.Date)
	}
	swap := first.Breakdown["swap"]
	if swap == nil || swap.MarketType != "swap" || swap.Equity != 1000.5 || swap.AvailableMargin != 600 {
		t.Errorf("breakdown not mapped: %+v", swap)
	}
	// Second snapshot has no breakdown — must round-trip as nil, not empty.
	if res.Snapshots[1].Breakdown != nil {
		t.Errorf("second snapshot should have nil breakdown, got %v", res.Snapshots[1].Breakdown)
	}

	if gotToken != "secret-token" {
		t.Errorf("X-Internal-Token: got %q want %q", gotToken, "secret-token")
	}
	var sent RebuildRequest
	if err := json.Unmarshal(gotBody, &sent); err != nil {
		t.Fatalf("body not valid JSON: %v", err)
	}
	if sent.Credentials.WalletAddress != "0xabc" {
		t.Errorf("wallet not propagated: %+v", sent)
	}
	if !strings.Contains(string(gotBody), `"hyperliquid"`) {
		t.Errorf("body missing exchange: %s", gotBody)
	}
}

func TestClient_Rebuild_PropagatesHTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":"unauthorized"}`))
	}))
	defer srv.Close()

	c := New(srv.URL, "wrong-token", zap.NewNop())
	_, err := c.Rebuild(context.Background(), RebuildRequest{UserUID: "u1", Exchange: "hyperliquid"})
	if err == nil {
		t.Fatal("expected error on 401")
	}
	if !strings.Contains(err.Error(), "401") {
		t.Errorf("error should mention HTTP 401: %v", err)
	}
}

// LOG-CREDS-001: errors returned from this client must never embed plaintext
// credentials, even when the upstream rebuilder echoes them in its response
// body. Regression guard against future "include the body in the error
// message for debugging" temptations.
func TestClient_Rebuild_ErrorDoesNotLeakCredentials(t *testing.T) {
	const sentinelKey = "REAL-API-KEY-00000000000000000"
	const sentinelSecret = "REAL-API-SECRET-1111111111111"
	const sentinelPass = "REAL-PASSPHRASE-22222222"
	const sentinelWallet = "0xREALWALLET3333333333333333333333"

	// Hostile rebuilder that echoes the entire request body back in the error
	// payload. We want our client to drop that body, not propagate it.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"internal","echo":` + string(mustJSON(string(body))) + `}`))
	}))
	defer srv.Close()

	c := New(srv.URL, "secret-token", zap.NewNop())
	_, err := c.Rebuild(context.Background(), RebuildRequest{
		UserUID:  "u1",
		Exchange: "hyperliquid",
		Credentials: Credentials{
			WalletAddress: sentinelWallet,
			APIKey:        sentinelKey,
			APISecret:     sentinelSecret,
			Passphrase:    sentinelPass,
		},
	})
	if err == nil {
		t.Fatal("expected error on 500")
	}
	msg := err.Error()
	for _, leak := range []string{sentinelKey, sentinelSecret, sentinelPass, sentinelWallet} {
		if strings.Contains(msg, leak) {
			t.Errorf("error message leaks credential %q: %s", leak, msg)
		}
	}
}

func mustJSON(v string) []byte {
	out, _ := jsonMarshal(v)
	return out
}

var jsonMarshal = func(v any) ([]byte, error) {
	return json.Marshal(v)
}
