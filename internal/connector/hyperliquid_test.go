package connector

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

// TestHyperliquidGetBalance_SpotHold locks the collateral double-count fix
// (incident 2026-05-24). Spot USDC reserved as perp margin (`hold`) is already
// inside marginSummary.accountValue, so only the unreserved portion
// (total − hold) may be folded into equity. CONN-06 flagged that the
// hold-absent case (pure-spot users, or a payload omitting the field) had no
// regression coverage even though the zero default is correct.
func TestHyperliquidGetBalance_SpotHold(t *testing.T) {
	const perps = `{"marginSummary":{"accountValue":"1000","totalMarginUsed":"200"},` +
		`"assetPositions":[{"position":{"unrealizedPnl":"50"}}]}`

	tests := []struct {
		name          string
		spot          string
		wantEquity    float64
		wantAvailable float64
	}{
		{
			name:          "hold field absent folds full spot total",
			spot:          `{"balances":[{"coin":"USDC","total":"300"}]}`,
			wantEquity:    1300, // 1000 perps + 300 spot (hold defaults to 0)
			wantAvailable: 1100, // equity − totalMarginUsed
		},
		{
			name:          "hold subtracted to avoid double-count",
			spot:          `{"balances":[{"coin":"USDC","total":"300","hold":"250"}]}`,
			wantEquity:    1050, // 1000 + (300 − 250)
			wantAvailable: 850,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				var req struct {
					Type string `json:"type"`
				}
				_ = json.Unmarshal(body, &req)
				w.Header().Set("Content-Type", "application/json")
				switch req.Type {
				case "clearinghouseState":
					_, _ = io.WriteString(w, perps)
				case "spotClearinghouseState":
					_, _ = io.WriteString(w, tc.spot)
				default:
					http.Error(w, "unexpected info type", http.StatusBadRequest)
				}
			}))
			defer srv.Close()

			srvURL, err := url.Parse(srv.URL)
			if err != nil {
				t.Fatalf("parse test server URL: %v", err)
			}

			h := NewHyperliquid(&Credentials{WalletAddress: "0x000000000000000000000000000000000000dead"})
			h.client = &http.Client{Transport: hostRewriter{base: http.DefaultTransport, target: srvURL}}

			bal, err := h.GetBalance(context.Background())
			if err != nil {
				t.Fatalf("GetBalance: %v", err)
			}
			if bal.Equity != tc.wantEquity {
				t.Errorf("equity = %v, want %v", bal.Equity, tc.wantEquity)
			}
			if bal.Available != tc.wantAvailable {
				t.Errorf("available = %v, want %v", bal.Available, tc.wantAvailable)
			}
			if bal.UnrealizedPnL != 50 {
				t.Errorf("unrealizedPnL = %v, want 50", bal.UnrealizedPnL)
			}
		})
	}
}
