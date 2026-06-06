package connector

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

type wsTestMessage struct {
	ClientMsgID string                 `json:"clientMsgId"`
	PayloadType int                    `json:"payloadType"`
	Payload     map[string]interface{} `json:"payload"`
}

func TestCTraderGetAccounts_RefreshesTokenOnAccessTokenInvalid(t *testing.T) {
	var getAccountsCalls atomic.Int32
	var seenTokensMu sync.Mutex
	seenTokens := make([]string, 0, 2)

	wsServer := newCTraderWSServer(t, func(conn *websocket.Conn, msg wsTestMessage) {
		switch msg.PayloadType {
		case ctraderPayloadAppAuthReq:
			sendWSResponse(t, conn, msg.ClientMsgID, ctraderPayloadAppAuthRes, map[string]any{})
		case ctraderPayloadGetAccountsReq:
			token, _ := msg.Payload["accessToken"].(string)
			seenTokensMu.Lock()
			seenTokens = append(seenTokens, token)
			seenTokensMu.Unlock()

			if getAccountsCalls.Add(1) == 1 {
				sendWSError(t, conn, msg.ClientMsgID, "CH_ACCESS_TOKEN_INVALID", "Access token expired")
				return
			}
			sendWSResponse(t, conn, msg.ClientMsgID, ctraderPayloadGetAccountsRes, map[string]any{
				"ctidTraderAccount": []map[string]any{{
					"ctidTraderAccountId": 12345,
					"isLive":              true,
					"brokerName":          "TestBroker",
				}},
			})
		default:
			t.Fatalf("unexpected payloadType: %d", msg.PayloadType)
		}
	})
	defer wsServer.Close()

	var tokenCalls atomic.Int32
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/token" {
			t.Fatalf("unexpected token path: %s", r.URL.Path)
		}
		if r.Method != http.MethodPost {
			t.Fatalf("unexpected token method: %s", r.Method)
		}
		tokenCalls.Add(1)

		body, _ := io.ReadAll(r.Body)
		vals, err := url.ParseQuery(string(body))
		if err != nil {
			t.Fatalf("parse form: %v", err)
		}
		if got := vals.Get("refresh_token"); got != "refresh-token" {
			t.Fatalf("refresh token mismatch: %s", got)
		}

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_token":"new-token","expires_in":3600}`))
	}))
	defer tokenServer.Close()

	c := &CTrader{
		clientID:     "client-id",
		clientSecret: "client-secret",
		accessToken:  "old-token",
		refreshToken: "refresh-token",
		isLive:       true,
		wsLiveURL:    toWSURL(wsServer.URL),
		authURL:      tokenServer.URL,
		httpClient:   &http.Client{Timeout: 5 * time.Second},
	}

	accounts, err := c.getAccounts(context.Background())
	if err != nil {
		t.Fatalf("getAccounts returned error: %v", err)
	}
	if len(accounts) != 1 {
		t.Fatalf("expected 1 account, got %d", len(accounts))
	}
	if tokenCalls.Load() != 1 {
		t.Fatalf("expected 1 token refresh call, got %d", tokenCalls.Load())
	}

	seenTokensMu.Lock()
	defer seenTokensMu.Unlock()
	if len(seenTokens) != 2 {
		t.Fatalf("expected 2 getAccounts calls, got %d", len(seenTokens))
	}
	if seenTokens[0] != "old-token" || seenTokens[1] != "new-token" {
		t.Fatalf("unexpected access tokens in requests: %+v", seenTokens)
	}
}

func TestCTraderAuthenticateAccount_RefreshesTokenOnAccessTokenInvalid(t *testing.T) {
	var accountAuthCalls atomic.Int32
	var seenTokensMu sync.Mutex
	seenTokens := make([]string, 0, 2)

	wsServer := newCTraderWSServer(t, func(conn *websocket.Conn, msg wsTestMessage) {
		switch msg.PayloadType {
		case ctraderPayloadAppAuthReq:
			sendWSResponse(t, conn, msg.ClientMsgID, ctraderPayloadAppAuthRes, map[string]any{})
		case ctraderPayloadAccountAuthReq:
			token, _ := msg.Payload["accessToken"].(string)
			seenTokensMu.Lock()
			seenTokens = append(seenTokens, token)
			seenTokensMu.Unlock()

			if accountAuthCalls.Add(1) == 1 {
				sendWSError(t, conn, msg.ClientMsgID, "CH_ACCESS_TOKEN_INVALID", "Access token expired")
				return
			}
			sendWSResponse(t, conn, msg.ClientMsgID, ctraderPayloadAccountAuthRes, map[string]any{})
		case ctraderPayloadTraderReq:
			sendWSResponse(t, conn, msg.ClientMsgID, ctraderPayloadTraderRes, map[string]any{
				"trader": map[string]any{
					"ctidTraderAccountId": 12345,
					"balance":             1000000,
					"moneyDigits":         2,
				},
			})
		default:
			t.Fatalf("unexpected payloadType: %d", msg.PayloadType)
		}
	})
	defer wsServer.Close()

	var tokenCalls atomic.Int32
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tokenCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_token":"new-token","expires_in":3600}`))
	}))
	defer tokenServer.Close()

	c := &CTrader{
		clientID:     "client-id",
		clientSecret: "client-secret",
		accessToken:  "old-token",
		refreshToken: "refresh-token",
		isLive:       true,
		wsLiveURL:    toWSURL(wsServer.URL),
		authURL:      tokenServer.URL,
		httpClient:   &http.Client{Timeout: 5 * time.Second},
	}

	trader, err := c.getTraderInfo(context.Background(), 12345)
	if err != nil {
		t.Fatalf("getTraderInfo returned error: %v", err)
	}
	if trader == nil || trader.Balance != 1000000 {
		t.Fatalf("unexpected trader response: %+v", trader)
	}
	if tokenCalls.Load() != 1 {
		t.Fatalf("expected 1 token refresh call, got %d", tokenCalls.Load())
	}

	seenTokensMu.Lock()
	defer seenTokensMu.Unlock()
	if len(seenTokens) != 2 {
		t.Fatalf("expected 2 account auth calls, got %d", len(seenTokens))
	}
	if seenTokens[0] != "old-token" || seenTokens[1] != "new-token" {
		t.Fatalf("unexpected access tokens in account auth: %+v", seenTokens)
	}
}

func TestCTraderGetAccounts_TokenInvalidWithoutRefreshToken(t *testing.T) {
	wsServer := newCTraderWSServer(t, func(conn *websocket.Conn, msg wsTestMessage) {
		switch msg.PayloadType {
		case ctraderPayloadAppAuthReq:
			sendWSResponse(t, conn, msg.ClientMsgID, ctraderPayloadAppAuthRes, map[string]any{})
		case ctraderPayloadGetAccountsReq:
			sendWSError(t, conn, msg.ClientMsgID, "CH_ACCESS_TOKEN_INVALID", "Access token expired")
		default:
			t.Fatalf("unexpected payloadType: %d", msg.PayloadType)
		}
	})
	defer wsServer.Close()

	c := &CTrader{
		clientID:     "client-id",
		clientSecret: "client-secret",
		accessToken:  "old-token",
		refreshToken: "",
		isLive:       true,
		wsLiveURL:    toWSURL(wsServer.URL),
		httpClient:   &http.Client{Timeout: 5 * time.Second},
	}

	_, err := c.getAccounts(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "CH_ACCESS_TOKEN_INVALID") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func toWSURL(httpURL string) string {
	return "ws" + strings.TrimPrefix(httpURL, "http")
}

func newCTraderWSServer(t *testing.T, onMessage func(conn *websocket.Conn, msg wsTestMessage)) *httptest.Server {
	t.Helper()

	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade websocket: %v", err)
		}

		for {
			_, raw, err := conn.ReadMessage()
			if err != nil {
				return
			}

			var msg wsTestMessage
			if err := json.Unmarshal(raw, &msg); err != nil {
				t.Fatalf("unmarshal ws message: %v", err)
			}
			onMessage(conn, msg)
		}
	}))

	return server
}

func sendWSResponse(t *testing.T, conn *websocket.Conn, clientMsgID string, payloadType int, payload any) {
	t.Helper()
	msg := map[string]any{
		"clientMsgId": clientMsgID,
		"payloadType": payloadType,
		"payload":     payload,
	}
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal ws response: %v", err)
	}
	if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
		t.Fatalf("write ws response: %v", err)
	}
}

func sendWSError(t *testing.T, conn *websocket.Conn, clientMsgID, code, description string) {
	t.Helper()
	sendWSResponse(t, conn, clientMsgID, ctraderPayloadErrorRes, map[string]any{
		"errorCode":   code,
		"description": description,
	})
}

func TestTradeSideUnmarshal_StringAndEnum(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want tradeSide
	}{
		{"string buy", `"BUY"`, "BUY"},
		{"string sell", `"SELL"`, "SELL"},
		{"enum buy", `1`, "BUY"},
		{"enum sell", `2`, "SELL"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var got tradeSide
			if err := json.Unmarshal([]byte(tc.raw), &got); err != nil {
				t.Fatalf("unmarshal %s: %v", tc.raw, err)
			}
			if got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestCTraderReconcileUnmarshal_RealPayload(t *testing.T) {
	// Exact reconcile payload captured from a live cTrader demo account holding
	// one open EURUSD position. Reproduces two prod decode failures at once:
	//   - tradeSide arrives as the ProtoOATradeSide enum integer (1 = BUY)
	//   - price arrives as a double (1.15229), not a scaled integer
	raw := `{"ctidTraderAccountId":46043853,"position":[{"positionId":264207985,"tradeData":{"symbolId":1,"volume":10000000,"tradeSide":1,"openTimestamp":1780688563637,"guaranteedStopLoss":false,"comment":"","measurementUnits":"EUR"},"positionStatus":1,"swap":0,"price":1.15229,"utcLastUpdateTimestamp":1780688563637,"commission":-450,"marginRate":1.0,"mirroringCommission":0,"guaranteedStopLoss":false,"usedMargin":100000,"moneyDigits":2}]}`
	var resp struct {
		Position []cTraderPosition `json:"position"`
	}
	if err := json.Unmarshal([]byte(raw), &resp); err != nil {
		t.Fatalf("unmarshal reconcile: %v", err)
	}
	if len(resp.Position) != 1 {
		t.Fatalf("got %d positions, want 1", len(resp.Position))
	}
	p := resp.Position[0]
	if p.TradeData.TradeSide != "BUY" {
		t.Fatalf("tradeSide: got %q, want BUY", p.TradeData.TradeSide)
	}
	if p.Price != 1.15229 {
		t.Fatalf("price: got %v, want 1.15229", p.Price)
	}
}

func TestParseCTraderCashflows_RealPayload(t *testing.T) {
	// Real ProtoOACashFlowHistoryListRes from a demo account: one $1000 deposit
	// (operationType 0 = BALANCE_DEPOSIT, delta 100000, moneyDigits 2).
	raw := []byte(`{"ctidTraderAccountId":46043853,"depositWithdraw":[{"operationType":0,"balanceHistoryId":144969818,"balance":100889,"delta":100000,"changeBalanceTimestamp":1780688558274,"moneyDigits":2}]}`)
	cfs, err := parseCTraderCashflows(raw)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(cfs) != 1 {
		t.Fatalf("got %d cashflows, want 1", len(cfs))
	}
	if cfs[0].Amount != 1000.0 {
		t.Fatalf("deposit amount: got %v, want 1000", cfs[0].Amount)
	}
}

func TestParseCTraderCashflows_WithdrawAndNonCashflowFiltered(t *testing.T) {
	// A withdrawal (op 1) -> negative amount; a swap (op 21) -> excluded.
	raw := []byte(`{"depositWithdraw":[{"operationType":1,"delta":50000,"moneyDigits":2},{"operationType":21,"delta":-12,"moneyDigits":2}]}`)
	cfs, err := parseCTraderCashflows(raw)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(cfs) != 1 {
		t.Fatalf("got %d cashflows, want 1 (swap must be excluded)", len(cfs))
	}
	if cfs[0].Amount != -500.0 {
		t.Fatalf("withdraw amount: got %v, want -500", cfs[0].Amount)
	}
}

func TestCTraderDealUnmarshal_RealPayload(t *testing.T) {
	// Real deal from the deal list: dealStatus as enum int (2 = FILLED) and
	// executionPrice as a double (1.15229). Both previously broke GetTrades.
	raw := `{"deal":[{"dealId":320455222,"positionId":264207985,"volume":10000000,"filledVolume":10000000,"symbolId":1,"executionTimestamp":1780688563637,"executionPrice":1.15229,"tradeSide":1,"dealStatus":2,"commission":-450,"moneyDigits":2}]}`
	var resp struct {
		Deal []cTraderDeal `json:"deal"`
	}
	if err := json.Unmarshal([]byte(raw), &resp); err != nil {
		t.Fatalf("unmarshal deal list: %v", err)
	}
	if len(resp.Deal) != 1 {
		t.Fatalf("got %d deals, want 1", len(resp.Deal))
	}
	d := resp.Deal[0]
	if d.DealStatus != "FILLED" {
		t.Fatalf("dealStatus: got %q, want FILLED", d.DealStatus)
	}
	if d.TradeSide != "BUY" {
		t.Fatalf("tradeSide: got %q, want BUY", d.TradeSide)
	}
	if d.ExecutionPrice != 1.15229 {
		t.Fatalf("executionPrice: got %v, want 1.15229", d.ExecutionPrice)
	}
}

func TestBuildCTraderHistoricalSnapshots_RealRoundTrip(t *testing.T) {
	// Real captured payloads: a $1000 deposit (balance 8.89 -> 1008.89) then a
	// 1-lot EURUSD round-trip closing at balance 996.42, all on 2026-06-05 UTC.
	dealJSON := `{"deal":[{"dealId":320460360,"positionId":264207985,"volume":10000000,"filledVolume":10000000,"symbolId":1,"executionTimestamp":1780689978964,"executionPrice":1.15225,"tradeSide":2,"dealStatus":2,"commission":-450,"closePositionDetail":{"grossProfit":-347,"swap":0,"commission":-900,"balance":99642,"moneyDigits":2},"moneyDigits":2},{"dealId":320455222,"positionId":264207985,"volume":10000000,"filledVolume":10000000,"symbolId":1,"executionTimestamp":1780688563637,"executionPrice":1.15229,"tradeSide":1,"dealStatus":2,"commission":-450,"moneyDigits":2}]}`
	var dr struct {
		Deal []cTraderDeal `json:"deal"`
	}
	if err := json.Unmarshal([]byte(dealJSON), &dr); err != nil {
		t.Fatalf("unmarshal deals: %v", err)
	}
	cfs, err := parseCTraderDepositWithdraws([]byte(`{"depositWithdraw":[{"operationType":0,"balance":100889,"delta":100000,"changeBalanceTimestamp":1780688558274,"moneyDigits":2}]}`))
	if err != nil {
		t.Fatalf("parse cashflows: %v", err)
	}

	now := time.Date(2026, 6, 6, 10, 0, 0, 0, time.UTC)
	snaps := buildCTraderHistoricalSnapshots(dr.Deal, cfs, now)

	if len(snaps) != 1 {
		t.Fatalf("got %d snapshots, want 1", len(snaps))
	}
	s := snaps[0]
	if s.Date.Format("20060102") != "20260605" {
		t.Fatalf("date: got %s, want 20260605", s.Date.Format("20060102"))
	}
	if !floatNear(s.TotalEquity, 996.42, 0.01) {
		t.Fatalf("equity: got %v, want 996.42", s.TotalEquity)
	}
	if s.Deposits != 1000 {
		t.Fatalf("deposits: got %v, want 1000", s.Deposits)
	}
	if s.Withdrawals != 0 {
		t.Fatalf("withdrawals: got %v, want 0", s.Withdrawals)
	}
	if s.TotalTrades != 2 {
		t.Fatalf("trades: got %d, want 2", s.TotalTrades)
	}
	if !floatNear(s.TotalVolume, 230454, 0.5) {
		t.Fatalf("volume: got %v, want ~230454", s.TotalVolume)
	}
}

func TestBuildCTraderHistoricalSnapshots_CarryForwardAndWithdraw(t *testing.T) {
	day1 := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	day3 := time.Date(2026, 6, 3, 12, 0, 0, 0, time.UTC)
	now := time.Date(2026, 6, 4, 9, 0, 0, 0, time.UTC)

	cashflows := []ctraderDepositWithdraw{
		{OperationType: 0, Balance: 100000, Delta: 100000, Timestamp: day1.UnixMilli(), MoneyDigits: 2}, // +$1000 -> 1000
		{OperationType: 1, Balance: 80000, Delta: -20000, Timestamp: day3.UnixMilli(), MoneyDigits: 2},  // -$200  -> 800
	}
	snaps := buildCTraderHistoricalSnapshots(nil, cashflows, now)

	if len(snaps) != 3 {
		t.Fatalf("got %d snapshots, want 3 (06-01..06-03)", len(snaps))
	}
	// 06-01: deposit day, equity 1000
	if snaps[0].Date.Format("20060102") != "20260601" || snaps[0].TotalEquity != 1000 || snaps[0].Deposits != 1000 {
		t.Fatalf("day1: %+v", snaps[0])
	}
	// 06-02: no activity, equity carried forward
	if snaps[1].Date.Format("20060102") != "20260602" || snaps[1].TotalEquity != 1000 || snaps[1].Deposits != 0 || snaps[1].Withdrawals != 0 {
		t.Fatalf("day2 (carry-forward): %+v", snaps[1])
	}
	// 06-03: withdrawal, equity 800
	if snaps[2].Date.Format("20060102") != "20260603" || snaps[2].TotalEquity != 800 || snaps[2].Withdrawals != 200 {
		t.Fatalf("day3: %+v", snaps[2])
	}
}

func floatNear(a, b, tol float64) bool {
	d := a - b
	if d < 0 {
		d = -d
	}
	return d <= tol
}

// compile-time guard: cTrader must keep implementing the in-enclave
// reconstruction interface that gates every-sync reconstruction (sync.go
// reconstructsEverySync). If this breaks, cTrader silently stops backfilling.
var _ HistoricalSnapshotProvider = (*CTrader)(nil)

func TestBuildCTraderHistoricalSnapshots_BoundaryHeal(t *testing.T) {
	// Real round-trip + deposit on 2026-06-05, reconstructed on a LATER heal day
	// (now=2026-06-07). The recurring re-run must emit the boundary day 06-06
	// with Deposits=0 (overwriting the live path's spurious 24h-window deposit)
	// while keeping 06-05 as the single real deposit, equity carried forward.
	dealJSON := `{"deal":[{"dealId":320460360,"positionId":264207985,"volume":10000000,"filledVolume":10000000,"symbolId":1,"executionTimestamp":1780689978964,"executionPrice":1.15225,"tradeSide":2,"dealStatus":2,"commission":-450,"closePositionDetail":{"grossProfit":-347,"swap":0,"commission":-900,"balance":99642,"moneyDigits":2},"moneyDigits":2},{"dealId":320455222,"positionId":264207985,"volume":10000000,"filledVolume":10000000,"symbolId":1,"executionTimestamp":1780688563637,"executionPrice":1.15229,"tradeSide":1,"dealStatus":2,"commission":-450,"moneyDigits":2}]}`
	var dr struct {
		Deal []cTraderDeal `json:"deal"`
	}
	if err := json.Unmarshal([]byte(dealJSON), &dr); err != nil {
		t.Fatalf("unmarshal deals: %v", err)
	}
	cfs, err := parseCTraderDepositWithdraws([]byte(`{"depositWithdraw":[{"operationType":0,"balance":100889,"delta":100000,"changeBalanceTimestamp":1780688558274,"moneyDigits":2}]}`))
	if err != nil {
		t.Fatalf("parse cashflows: %v", err)
	}

	now := time.Date(2026, 6, 7, 10, 0, 0, 0, time.UTC)
	snaps := buildCTraderHistoricalSnapshots(dr.Deal, cfs, now)

	byDay := map[string]*HistoricalSnapshot{}
	var total float64
	for _, s := range snaps {
		byDay[s.Date.Format("20060102")] = s
		total += s.Deposits
	}

	if d := byDay["20260605"]; d == nil || d.Deposits != 1000 {
		t.Fatalf("06-05: want deposits=1000, got %+v", d)
	}
	d06 := byDay["20260606"]
	if d06 == nil {
		t.Fatalf("06-06 boundary-heal row missing (cannot overwrite the live spurious deposit)")
	}
	if d06.Deposits != 0 {
		t.Fatalf("06-06 deposits: want 0 (heal), got %v", d06.Deposits)
	}
	if !floatNear(d06.TotalEquity, 996.42, 0.01) {
		t.Fatalf("06-06 equity: want 996.42 carry-forward, got %v", d06.TotalEquity)
	}
	if total != 1000 {
		t.Fatalf("total deposits across series: want 1000 (counted exactly once), got %v", total)
	}
}
