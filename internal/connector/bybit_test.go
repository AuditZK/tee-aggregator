package connector

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func bybitFillPageJSON(cursor string, from, n int, side string) string {
	rows := make([]string, 0, n)
	for i := from; i < from+n; i++ {
		rows = append(rows, fmt.Sprintf(
			`{"execId":"e%d","symbol":"BTCUSDT","side":%q,"execPrice":"100","execQty":"1","execFee":"0.1","execTime":"1757000000000","closedPnl":"0"}`,
			i, side))
	}
	return fmt.Sprintf(`{"retCode":0,"retMsg":"OK","result":{"nextPageCursor":%q,"list":[%s]}}`,
		cursor, strings.Join(rows, ","))
}

// A day busier than one execution page must come back whole, with the
// venue's capitalised side folded to the lowercase the sync aggregates on.
func TestBybitGetTrades_FollowsCursorAndLowercasesSide(t *testing.T) {
	pages := []string{
		bybitFillPageJSON("page2", 0, 100, "Buy"),
		bybitFillPageJSON("", 100, 50, "Sell"),
	}
	var mu sync.Mutex
	var cursors []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		mu.Lock()
		idx := len(cursors)
		cursors = append(cursors, r.URL.Query().Get("cursor"))
		mu.Unlock()
		if idx >= len(pages) {
			io.WriteString(w, bybitFillPageJSON("", 0, 0, "Buy"))
			return
		}
		io.WriteString(w, pages[idx])
	}))
	t.Cleanup(srv.Close)

	b := NewBybitWithClient(
		&Credentials{Exchange: "bybit", APIKey: "key", APISecret: "secret"},
		&http.Client{Timeout: 5 * time.Second},
	)
	b.baseURL = srv.URL

	now := time.Now().UTC()
	trades, err := b.GetTrades(context.Background(), now.Add(-24*time.Hour), now)
	if err != nil {
		t.Fatalf("GetTrades: %v", err)
	}
	if len(trades) != 150 {
		t.Fatalf("trades = %d, want 150", len(trades))
	}

	buys, sells := 0, 0
	for _, tr := range trades {
		switch tr.Side {
		case "buy":
			buys++
		case "sell":
			sells++
		default:
			t.Fatalf("side = %q, want buy or sell", tr.Side)
		}
	}
	if buys != 100 || sells != 50 {
		t.Fatalf("buys=%d sells=%d, want 100/50", buys, sells)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(cursors) != 2 || cursors[0] != "" || cursors[1] != "page2" {
		t.Fatalf("cursors sent = %q, want [\"\" \"page2\"]", cursors)
	}
}
