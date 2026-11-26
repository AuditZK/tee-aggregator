package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

// hlPagingServer answers userFunding and userNonFundingLedgerUpdates the way
// Hyperliquid documents it: rows from startTime (inclusive), oldest first,
// at most 500 of them.
func hlPagingServer(t *testing.T, funding, ledger []map[string]any) (*Hyperliquid, *int) {
	t.Helper()
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		body, _ := io.ReadAll(r.Body)
		var req struct {
			Type      string `json:"type"`
			StartTime int64  `json:"startTime"`
		}
		_ = json.Unmarshal(body, &req)
		var src []map[string]any
		switch req.Type {
		case "userFunding":
			src = funding
		case "userNonFundingLedgerUpdates":
			src = ledger
		default:
			http.Error(w, "unexpected info type", http.StatusBadRequest)
			return
		}
		out := []map[string]any{}
		for _, row := range src {
			if row["time"].(int64) >= req.StartTime {
				out = append(out, row)
			}
			if len(out) == hlPageSize {
				break
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(out)
	}))
	t.Cleanup(srv.Close)
	srvURL, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	h := NewHyperliquid(&Credentials{WalletAddress: "0x000000000000000000000000000000000000dead"})
	h.client = &http.Client{Transport: hostRewriter{base: http.DefaultTransport, target: srvURL}}
	return h, &calls
}

// fundingRows is one settlement per coin per hour, as Hyperliquid pays it.
func fundingRows(start time.Time, hours, coins int) []map[string]any {
	var rows []map[string]any
	for hr := 0; hr < hours; hr++ {
		ts := start.Add(time.Duration(hr) * time.Hour).UnixMilli()
		for c := 0; c < coins; c++ {
			rows = append(rows, map[string]any{
				"time": ts,
				"hash": "0x0000000000000000000000000000000000000000000000000000000000000000",
				"delta": map[string]any{
					"type": "funding", "coin": fmt.Sprintf("C%03d", c),
					"usdc": "-1.0", "szi": "1.0", "fundingRate": "0.00001",
				},
			})
		}
	}
	return rows
}

// Twenty-five positions settle 600 rows a day, more than one answer holds.
// The reader used to stop at the first 500, so the last hours of funding, the
// ones nearest the sync, were never booked.
func TestHyperliquidFundingPagesPastFiveHundredRows(t *testing.T) {
	start := time.Date(2026, 11, 25, 0, 0, 0, 0, time.UTC)
	rows := fundingRows(start, 48, 25) // 1200 rows
	h, calls := hlPagingServer(t, rows, nil)

	fees, err := h.GetFundingFees(context.Background(), nil, start)
	if err != nil {
		t.Fatalf("GetFundingFees: %v", err)
	}
	if len(fees) != len(rows) {
		t.Fatalf("read %d settlements, want %d", len(fees), len(rows))
	}
	seen := map[string]bool{}
	total := 0.0
	for _, f := range fees {
		k := f.Timestamp.Format(time.RFC3339) + f.Symbol
		if seen[k] {
			t.Fatalf("settlement %s read twice", k)
		}
		seen[k] = true
		total += f.Amount
	}
	if total != -1200 {
		t.Errorf("funding total = %v, want -1200", total)
	}
	last := fees[len(fees)-1].Timestamp
	if want := start.Add(47 * time.Hour); !last.Equal(want) {
		t.Errorf("last settlement %s, want %s", last, want)
	}
	if *calls < 3 {
		t.Errorf("%d calls, want at least 3 pages", *calls)
	}
}

func TestHyperliquidFundingSinglePage(t *testing.T) {
	start := time.Date(2026, 11, 25, 0, 0, 0, 0, time.UTC)
	rows := fundingRows(start, 24, 2)
	h, calls := hlPagingServer(t, rows, nil)

	fees, err := h.GetFundingFees(context.Background(), nil, start)
	if err != nil {
		t.Fatalf("GetFundingFees: %v", err)
	}
	if len(fees) != 48 || *calls != 1 {
		t.Errorf("read %d settlements in %d calls, want 48 in 1", len(fees), *calls)
	}
}

// A full page at a single millisecond cannot be paged past with startTime.
// The read fails instead of returning the first 500 as if they were all.
func TestHyperliquidFundingFullPageAtOneMillisecondFails(t *testing.T) {
	start := time.Date(2026, 11, 25, 0, 0, 0, 0, time.UTC)
	rows := fundingRows(start, 1, 600)
	h, _ := hlPagingServer(t, rows, nil)

	if _, err := h.GetFundingFees(context.Background(), nil, start); err == nil ||
		!strings.Contains(err.Error(), "cannot page past") {
		t.Fatalf("want a paging error, got %v", err)
	}
}

// The ledger has the same 500-row answer. A deposit past the first page was
// never seen, and the money it brought in read as performance.
func TestHyperliquidCashflowsPagePastFiveHundredRows(t *testing.T) {
	start := time.Date(2026, 11, 25, 0, 0, 0, 0, time.UTC)
	var ledger []map[string]any
	for i := 0; i < 700; i++ {
		ledger = append(ledger, map[string]any{
			"time":  start.Add(time.Duration(i) * time.Minute).UnixMilli(),
			"hash":  fmt.Sprintf("0x%064x", i),
			"delta": map[string]any{"type": "spotTransfer", "usdc": "5"},
		})
	}
	ledger = append(ledger,
		map[string]any{"time": start.Add(800 * time.Minute).UnixMilli(), "hash": "0xdep",
			"delta": map[string]any{"type": "deposit", "usdc": "1000"}},
		map[string]any{"time": start.Add(801 * time.Minute).UnixMilli(), "hash": "0xwd",
			"delta": map[string]any{"type": "withdraw", "usdc": "-250"}},
	)
	h, _ := hlPagingServer(t, nil, ledger)

	flows, err := h.GetCashflows(context.Background(), start)
	if err != nil {
		t.Fatalf("GetCashflows: %v", err)
	}
	if len(flows) != 2 || flows[0].Amount != 1000 || flows[1].Amount != -250 {
		t.Fatalf("flows = %+v, want the deposit of 1000 and the withdrawal of 250", flows)
	}
}
