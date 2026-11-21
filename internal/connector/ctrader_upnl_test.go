package connector

import (
	"context"
	"math"
	"net/http"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// ctraderBalanceServer answers the full GetBalance round trip: account list,
// app/account auth, trader info, reconcile, unrealized PnL (2187) and the
// asset list (2112). Handlers can be overridden per test through opts.
type ctraderBalanceOpts struct {
	// positions is the reconcile payload's "position" array.
	positions []map[string]any
	// unrealized is the 2188 payload; nil sends an error instead.
	unrealized map[string]any
	// assets is the 2113 payload's "asset" array; nil sends an error.
	assets []map[string]any
	// depositAssetID goes into the trader payload.
	depositAssetID int64
	// onUnrealized counts 2187 requests.
	seen map[int]int
	// accounts replaces the single live account the token grants.
	accounts []map[string]any
}

func newCTraderBalanceServer(t *testing.T, opts *ctraderBalanceOpts) *CTrader {
	t.Helper()
	if opts.seen == nil {
		opts.seen = map[int]int{}
	}

	server := newCTraderWSServer(t, func(conn *websocket.Conn, msg wsTestMessage) {
		opts.seen[msg.PayloadType]++
		switch msg.PayloadType {
		case ctraderPayloadAppAuthReq:
			sendWSResponse(t, conn, msg.ClientMsgID, ctraderPayloadAppAuthRes, map[string]any{})
		case ctraderPayloadGetAccountsReq:
			accounts := opts.accounts
			if accounts == nil {
				accounts = []map[string]any{{"ctidTraderAccountId": 12345, "isLive": true}}
			}
			sendWSResponse(t, conn, msg.ClientMsgID, ctraderPayloadGetAccountsRes, map[string]any{
				"ctidTraderAccount": accounts,
			})
		case ctraderPayloadAccountAuthReq:
			sendWSResponse(t, conn, msg.ClientMsgID, ctraderPayloadAccountAuthRes, map[string]any{})
		case ctraderPayloadTraderReq:
			sendWSResponse(t, conn, msg.ClientMsgID, ctraderPayloadTraderRes, map[string]any{
				"trader": map[string]any{
					"ctidTraderAccountId": 12345,
					"balance":             1000000, // synthetic 10 000.00
					"moneyDigits":         2,
					"depositAssetId":      opts.depositAssetID,
				},
			})
		case ctraderPayloadReconcileReq:
			sendWSResponse(t, conn, msg.ClientMsgID, ctraderPayloadReconcileRes, map[string]any{
				"position": opts.positions,
			})
		case ctraderPayloadUnrealizedPnLReq:
			if opts.unrealized == nil {
				sendWSError(t, conn, msg.ClientMsgID, "NOT_SUPPORTED", "no unrealized pnl here")
				return
			}
			sendWSResponse(t, conn, msg.ClientMsgID, ctraderPayloadUnrealizedPnLRes, opts.unrealized)
		case ctraderPayloadAssetListReq:
			if opts.assets == nil {
				sendWSError(t, conn, msg.ClientMsgID, "NOT_SUPPORTED", "no asset list here")
				return
			}
			sendWSResponse(t, conn, msg.ClientMsgID, ctraderPayloadAssetListRes, map[string]any{
				"asset": opts.assets,
			})
		default:
			t.Errorf("unexpected payloadType: %d", msg.PayloadType)
		}
	})
	t.Cleanup(server.Close)

	return &CTrader{
		clientID:     "client-id",
		clientSecret: "client-secret",
		accessToken:  "token",
		isLive:       true,
		wsLiveURL:    toWSURL(server.URL),
		httpClient:   &http.Client{Timeout: 5 * time.Second},
	}
}

// E-C1: equity must include the unrealized PnL of the open positions, which
// only ProtoOAGetPositionUnrealizedPnLReq (2187) exposes. Before the fix the
// connector read a non-existent field off ProtoOAPosition, so equity was the
// settled balance and this assertion failed with equity == balance.
func TestCTraderGetBalance_EquityIncludesUnrealizedPnL(t *testing.T) {
	c := newCTraderBalanceServer(t, &ctraderBalanceOpts{
		positions: []map[string]any{{
			"positionId":  264207985,
			"tradeData":   map[string]any{"symbolId": 1, "volume": 10000000, "tradeSide": 1},
			"price":       1.15229,
			"usedMargin":  100000,
			"moneyDigits": 2,
		}},
		unrealized: map[string]any{
			"ctidTraderAccountId": 12345,
			"moneyDigits":         2,
			"positionUnrealizedPnL": []map[string]any{{
				"positionId":         264207985,
				"grossUnrealizedPnL": 13000,
				"netUnrealizedPnL":   12345, // 123.45
			}},
		},
	})

	bal, err := c.GetBalance(context.Background())
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if math.Abs(bal.UnrealizedPnL-123.45) > 1e-9 {
		t.Fatalf("unrealized pnl: got %v, want 123.45", bal.UnrealizedPnL)
	}
	if math.Abs(bal.Equity-10123.45) > 1e-9 {
		t.Fatalf("equity: got %v, want 10123.45 (balance 10000 + uPnL 123.45)", bal.Equity)
	}
	if bal.Equity == 10000.0 {
		t.Fatalf("equity collapsed to the settled balance — uPnL is not being read")
	}
	// usedMargin scaled by the POSITION's moneyDigits (V-E3): 100000/100 = 1000.
	if want := 10123.45 - 1000.0; math.Abs(bal.Available-want) > 1e-9 {
		t.Fatalf("available: got %v, want %v", bal.Available, want)
	}
}

// A flat account must not spend a request on 2187.
func TestCTraderGetBalance_NoPositionsSkipsUnrealizedRequest(t *testing.T) {
	opts := &ctraderBalanceOpts{positions: []map[string]any{}}
	c := newCTraderBalanceServer(t, opts)

	bal, err := c.GetBalance(context.Background())
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if bal.Equity != 10000.0 {
		t.Fatalf("equity: got %v, want 10000", bal.Equity)
	}
	if opts.seen[ctraderPayloadUnrealizedPnLReq] != 0 {
		t.Fatalf("2187 sent %d times for a flat account, want 0", opts.seen[ctraderPayloadUnrealizedPnLReq])
	}
}

// A broker that refuses 2187 must not sink the snapshot; the gap is reported
// as a capability warning instead (it rides to sync_statuses).
func TestCTraderGetBalance_UnrealizedFailureWarnsInsteadOfFailing(t *testing.T) {
	c := newCTraderBalanceServer(t, &ctraderBalanceOpts{
		positions: []map[string]any{{
			"positionId": 1,
			"tradeData":  map[string]any{"symbolId": 1, "volume": 100000, "tradeSide": 1},
		}},
		unrealized: nil,
	})

	bal, err := c.GetBalance(context.Background())
	if err != nil {
		t.Fatalf("GetBalance must degrade, not fail: %v", err)
	}
	if bal.Equity != 10000.0 {
		t.Fatalf("equity: got %v, want the settled balance 10000", bal.Equity)
	}
	if !hasWarning(c.CapabilityWarnings(), "unrealized_pnl_unavailable") {
		t.Fatalf("expected unrealized_pnl_unavailable warning, got %v", c.CapabilityWarnings())
	}
}

// E-C2: a EUR account must be labelled EUR and flagged, not stamped USD.
func TestCTraderGetBalance_NonUSDAccountCurrency(t *testing.T) {
	c := newCTraderBalanceServer(t, &ctraderBalanceOpts{
		positions:      []map[string]any{},
		depositAssetID: 3,
		assets: []map[string]any{
			{"assetId": 1, "name": "USD", "displayName": "US Dollar"},
			{"assetId": 3, "name": "EUR", "displayName": "Euro"},
		},
	})

	bal, err := c.GetBalance(context.Background())
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if bal.Currency != "EUR" {
		t.Fatalf("currency: got %q, want EUR", bal.Currency)
	}
	if !hasWarning(c.CapabilityWarnings(), "account_currency_eur") {
		t.Fatalf("expected account_currency_eur warning, got %v", c.CapabilityWarnings())
	}
}

// A USD account resolves its currency without raising a warning.
func TestCTraderGetBalance_USDAccountHasNoCurrencyWarning(t *testing.T) {
	c := newCTraderBalanceServer(t, &ctraderBalanceOpts{
		positions:      []map[string]any{},
		depositAssetID: 1,
		assets:         []map[string]any{{"assetId": 1, "name": "USD"}},
	})

	bal, err := c.GetBalance(context.Background())
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if bal.Currency != "USD" {
		t.Fatalf("currency: got %q, want USD", bal.Currency)
	}
	if len(c.CapabilityWarnings()) != 0 {
		t.Fatalf("expected no warnings, got %v", c.CapabilityWarnings())
	}
}

func hasWarning(warnings []string, want string) bool {
	for _, w := range warnings {
		if w == want {
			return true
		}
	}
	return false
}
