package connector

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// okxFillsServer answers fills-history per instType, so a test can give one
// product line fills and leave the others empty or failing.
type okxFillsServer struct {
	srv  *httptest.Server
	mu   sync.Mutex
	seen []string
}

func newOKXFillsServer(t *testing.T, bodyByInstType map[string]string) *okxFillsServer {
	t.Helper()
	f := &okxFillsServer{}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		instType := r.URL.Query().Get("instType")
		f.mu.Lock()
		f.seen = append(f.seen, instType)
		f.mu.Unlock()

		body, ok := bodyByInstType[instType]
		w.Header().Set("Content-Type", "application/json")
		if !ok {
			w.WriteHeader(http.StatusBadRequest)
			io.WriteString(w, `{"code":"51000","msg":"instrument type not enabled"}`)
			return
		}
		io.WriteString(w, body)
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *okxFillsServer) queried() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.seen...)
}

func (f *okxFillsServer) connector() *OKX {
	creds := &Credentials{Exchange: "okx", APIKey: "key", APISecret: "secret", Passphrase: "pass"}
	return newOKXWithHosts(creds, &http.Client{Timeout: 5 * time.Second}, []string{f.srv.URL})
}

func okxFillBody(instType, instID, side, price, size, fee string) string {
	fill := map[string]string{
		"tradeId": "t-" + instID, "instId": instID, "side": side,
		"fillPx": price, "fillSz": size, "fee": fee, "feeCcy": "USDT",
		"ts": "1787788800000", "instType": instType,
	}
	payload, _ := json.Marshal(map[string]any{"code": "0", "data": []any{fill}})
	return string(payload)
}

func okxWindow() (time.Time, time.Time) {
	end := time.Date(2026, time.August, 30, 0, 0, 0, 0, time.UTC)
	return end.AddDate(0, 0, -12), end
}

// The defect: fills-history was queried on instType=SWAP alone. A spot-only
// account reported zero trades, zero volume and zero fees for every day of its
// history while its equity moved daily — the ledger held a buy and a sell the
// query could never return.
func TestOKXGetTrades_SeesSpotFills(t *testing.T) {
	srv := newOKXFillsServer(t, map[string]string{
		"SPOT": okxFillBody("SPOT", "BTC-USDT", "buy", "60000", "0.05", "-11.62"),
	})

	start, end := okxWindow()
	trades, err := srv.connector().GetTrades(context.Background(), start, end)
	if err != nil {
		t.Fatalf("GetTrades: %v", err)
	}
	if len(trades) != 1 {
		t.Fatalf("got %d trades, want 1 — a spot account still reports no activity", len(trades))
	}
	if trades[0].MarketType != MarketSwap {
		t.Fatalf("market type = %q, want %q — until equity can be split per product, "+
			"a fill must join the bucket the equity is filed in", trades[0].MarketType, MarketSwap)
	}
	if trades[0].Fee != 11.62 {
		t.Fatalf("fee = %v, want 11.62 (OKX signs a charge negative)", trades[0].Fee)
	}
}

func TestOKXGetTrades_QueriesEveryProductLine(t *testing.T) {
	srv := newOKXFillsServer(t, map[string]string{
		"SPOT": `{"code":"0","data":[]}`, "MARGIN": `{"code":"0","data":[]}`,
		"SWAP": `{"code":"0","data":[]}`, "FUTURES": `{"code":"0","data":[]}`,
		"OPTION": `{"code":"0","data":[]}`,
	})

	start, end := okxWindow()
	if _, err := srv.connector().GetTrades(context.Background(), start, end); err != nil {
		t.Fatalf("GetTrades: %v", err)
	}

	want := map[string]bool{"SPOT": true, "MARGIN": true, "SWAP": true, "FUTURES": true, "OPTION": true}
	for _, instType := range srv.queried() {
		delete(want, instType)
	}
	if len(want) != 0 {
		t.Fatalf("product lines never queried: %v", want)
	}
}

// Mixed accounts must come back whole. Every fill lands in the swap bucket,
// where the equity is filed — see okxMarketType.
func TestOKXGetTrades_MergesLinesAndTagsEach(t *testing.T) {
	srv := newOKXFillsServer(t, map[string]string{
		"SPOT":   okxFillBody("SPOT", "BTC-USDT", "buy", "60000", "0.05", "-3"),
		"MARGIN": okxFillBody("MARGIN", "ETH-USDT", "sell", "3000", "1", "-2"),
		"SWAP":   okxFillBody("SWAP", "BTC-USDT-SWAP", "buy", "60100", "2", "-1"),
	})

	start, end := okxWindow()
	trades, err := srv.connector().GetTrades(context.Background(), start, end)
	if err != nil {
		t.Fatalf("GetTrades: %v", err)
	}
	if len(trades) != 3 {
		t.Fatalf("got %d trades, want 3", len(trades))
	}

	symbols := map[string]bool{}
	for _, tr := range trades {
		symbols[tr.Symbol] = true
		if tr.MarketType != MarketSwap {
			t.Errorf("%s filed under %q — it would land in a bucket holding no equity",
				tr.Symbol, tr.MarketType)
		}
	}
	for _, want := range []string{"BTC-USDT", "ETH-USDT", "BTC-USDT-SWAP"} {
		if !symbols[want] {
			t.Errorf("fill %s never came back", want)
		}
	}
}

// A product line the account never enabled answers with an error. That must
// not bury the lines that did answer.
func TestOKXGetTrades_OneDisabledLineDoesNotSinkTheRest(t *testing.T) {
	srv := newOKXFillsServer(t, map[string]string{
		"SPOT": okxFillBody("SPOT", "BTC-USDT", "buy", "60000", "0.05", "-3"),
	})

	start, end := okxWindow()
	trades, err := srv.connector().GetTrades(context.Background(), start, end)
	if err != nil {
		t.Fatalf("GetTrades: %v — one unavailable product line failed the whole fetch", err)
	}
	if len(trades) != 1 {
		t.Fatalf("got %d trades, want 1", len(trades))
	}
}

// Every line failing is a real outage and must surface, not read as "no trades".
func TestOKXGetTrades_TotalFailureIsAnError(t *testing.T) {
	srv := newOKXFillsServer(t, map[string]string{})

	start, end := okxWindow()
	if _, err := srv.connector().GetTrades(context.Background(), start, end); err == nil {
		t.Fatal("every product line failed and GetTrades reported success with no trades")
	}
}
