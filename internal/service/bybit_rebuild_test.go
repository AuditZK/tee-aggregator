package service

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/trackrecord/enclave/internal/connector"
	"github.com/trackrecord/enclave/internal/rebuilderclient"
	"github.com/trackrecord/enclave/internal/repository"
	"go.uber.org/zap"
)

// Bybit only reaches the rebuilder because it is listed in
// externalRebuilderExchanges. Before 2026-08-25 it was not: the connect form
// still offered "rebuild my history", the enclave recorded the consent and then
// skipped the dispatch, and the user was left waiting for an import that was
// never going to start. Two of them wrote in the same evening.
func TestReconstructHistory_BybitReachesTheRebuilder(t *testing.T) {
	var hits int
	var gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		buf := make([]byte, r.ContentLength)
		_, _ = r.Body.Read(buf)
		gotBody = string(buf)
		_, _ = w.Write([]byte(`{"exchange":"bybit","count":0,"snapshots":[]}`))
	}))
	defer srv.Close()

	svc := &SyncService{
		logger:    zap.NewNop(),
		rebuilder: rebuilderclient.New(srv.URL, "internal-token-0123456789", zap.NewNop()),
	}
	conn := &fakeHistConnector{exchange: "bybit"}
	connMeta := &repository.ExchangeConnection{UserUID: "u1", Exchange: "bybit", Label: "main"}

	svc.reconstructHistory(context.Background(), connMeta, conn,
		&Credentials{APIKey: "k", APISecret: "s"}, reconstructOpts{})

	if hits != 1 {
		t.Fatalf("rebuilder received %d requests for bybit, want 1", hits)
	}
	if !strings.Contains(gotBody, "bybit") {
		t.Fatalf("rebuild request did not name the exchange: %s", gotBody)
	}
}

func TestExternalRebuilderSupports_Bybit(t *testing.T) {
	// kraken joined the gate on 2026-08-28, when its ledger-walk module
	// shipped; mexc and lighter came back the same day once their modules
	// honoured EndEquityOverride.
	for _, name := range []string{"bybit", "BYBIT", " Bybit ", "kraken", "mexc", "lighter"} {
		if !externalRebuilderSupports(name) {
			t.Fatalf("externalRebuilderSupports(%q) = false", name)
		}
	}
	// Still outside the gate: the rebuilder has no module for them, and
	// dispatching would egress credentials for a guaranteed 400.
	for _, name := range []string{"kucoin", "coinbase", "gate", "bingx", "huobi"} {
		if externalRebuilderSupports(name) {
			t.Fatalf("externalRebuilderSupports(%q) = true, but no module exists", name)
		}
	}
}

// Bybit's GetTrades reads category=linear, so every fill it returns is typed
// swap. Equity filed under the spot default would leave the trades in one
// bucket and the equity in another, and per-market return divides by that zero.
func TestPrimaryMarketType_BybitFilesEquityWithItsTrades(t *testing.T) {
	if got := primaryMarketType("bybit"); got != connector.MarketSwap {
		t.Fatalf("primaryMarketType(\"bybit\") = %q, want %q", got, connector.MarketSwap)
	}

	// Assert the property through the aggregation rather than trusting the
	// table: equity and trades have to meet in one bucket.
	svc := &SyncService{}
	agg := svc.aggregateTrades([]*connector.Trade{
		{ID: "f1", Price: 100, Quantity: 2, Side: "buy", MarketType: connector.MarketSwap, Timestamp: time.Now()},
	})
	tradeBucket := agg.getOrCreateMarket(connector.MarketSwap)
	agg.getOrCreateMarket(primaryMarketType("bybit")).equity = 10094.27

	if tradeBucket.equity != 10094.27 {
		t.Fatal("bybit equity filed in a different bucket than its trades")
	}
}
