package connector

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// DetectIsPaper must trust cTrader's per-account IsLive flag, not the
// passphrase-seeded c.isLive. OAuth connections never carry "demo" in the
// passphrase, so c.isLive defaults to live; a demo account (IsLive=false) must
// still be reported as paper — the youceef.bouanani case where a demo balance
// reset to $1M surfaced as a verifiable +921% track record. Mixed accounts
// mirror ensureAccountID and prefer the live one.
func TestCTraderDetectIsPaper(t *testing.T) {
	cases := []struct {
		name      string
		accounts  []map[string]any
		wantPaper bool
	}{
		{
			name:      "demo only via oauth (no demo passphrase)",
			accounts:  []map[string]any{{"ctidTraderAccountId": 1, "isLive": false}},
			wantPaper: true,
		},
		{
			name:      "live only",
			accounts:  []map[string]any{{"ctidTraderAccountId": 1, "isLive": true}},
			wantPaper: false,
		},
		{
			name:      "mixed prefers live",
			accounts:  []map[string]any{{"ctidTraderAccountId": 1, "isLive": false}, {"ctidTraderAccountId": 2, "isLive": true}},
			wantPaper: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			wsServer := newCTraderWSServer(t, func(conn *websocket.Conn, msg wsTestMessage) {
				switch msg.PayloadType {
				case ctraderPayloadAppAuthReq:
					sendWSResponse(t, conn, msg.ClientMsgID, ctraderPayloadAppAuthRes, map[string]any{})
				case ctraderPayloadGetAccountsReq:
					sendWSResponse(t, conn, msg.ClientMsgID, ctraderPayloadGetAccountsRes, map[string]any{
						"ctidTraderAccount": tc.accounts,
					})
				default:
					t.Fatalf("unexpected payloadType: %d", msg.PayloadType)
				}
			})
			defer wsServer.Close()

			c := &CTrader{
				clientID:     "client-id",
				clientSecret: "client-secret",
				accessToken:  "token",
				isLive:       true, // OAuth default that used to misclassify every demo
				wsLiveURL:    toWSURL(wsServer.URL),
				wsDemoURL:    toWSURL(wsServer.URL),
				httpClient:   &http.Client{Timeout: 5 * time.Second},
			}

			isPaper, err := c.DetectIsPaper(context.Background())
			if err != nil {
				t.Fatalf("DetectIsPaper error: %v", err)
			}
			if isPaper != tc.wantPaper {
				t.Fatalf("isPaper: got %v, want %v", isPaper, tc.wantPaper)
			}
		})
	}
}
