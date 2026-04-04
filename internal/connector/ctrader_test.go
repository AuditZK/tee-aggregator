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
