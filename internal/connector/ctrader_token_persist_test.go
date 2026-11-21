package connector

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func ctraderRefreshServer(t *testing.T, body string) *httptest.Server {
	t.Helper()
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(s.Close)
	return s
}

// E-H3: cTrader rotates single-use refresh tokens, so the rotated pair must
// reach the DB. A single failed write used to be the whole story.
func TestCTraderRefresh_RetriesThePersist(t *testing.T) {
	server := ctraderRefreshServer(t, `{"access_token":"new-access","refresh_token":"new-refresh","expires_in":3600}`)

	var calls atomic.Int32
	var gotAccess, gotRefresh string
	c := &CTrader{
		clientID:     "id",
		clientSecret: "secret",
		accessToken:  "old-access",
		refreshToken: "old-refresh",
		authURL:      server.URL,
		httpClient:   &http.Client{Timeout: 5 * time.Second},
	}
	c.SetTokenPersister(func(ctx context.Context, access, refresh string) error {
		if calls.Add(1) < 3 {
			return errors.New("pool exhausted")
		}
		gotAccess, gotRefresh = access, refresh
		return nil
	})

	if err := c.refreshAccessToken(context.Background()); err != nil {
		t.Fatalf("refresh should have survived two failed writes: %v", err)
	}
	if calls.Load() != 3 {
		t.Fatalf("persister called %d times, want 3", calls.Load())
	}
	if gotAccess != "new-access" || gotRefresh != "new-refresh" {
		t.Fatalf("persisted %q/%q, want new-access/new-refresh", gotAccess, gotRefresh)
	}
	if c.currentRefreshToken() != "new-refresh" {
		t.Fatalf("in-memory refresh token: got %q, want new-refresh", c.currentRefreshToken())
	}
}

// When the write never lands the caller must hear about it: the stored token
// is now the consumed one, so the connection is dead at the next restart.
func TestCTraderRefresh_ReportsAPersistThatNeverLands(t *testing.T) {
	server := ctraderRefreshServer(t, `{"access_token":"new-access","refresh_token":"new-refresh","expires_in":3600}`)

	var calls atomic.Int32
	c := &CTrader{
		clientID:     "id",
		clientSecret: "secret",
		accessToken:  "old-access",
		refreshToken: "old-refresh",
		authURL:      server.URL,
		httpClient:   &http.Client{Timeout: 5 * time.Second},
	}
	c.SetTokenPersister(func(ctx context.Context, access, refresh string) error {
		calls.Add(1)
		return errors.New("update oauth tokens matched 0 active rows, want 1")
	})

	err := c.refreshAccessToken(context.Background())
	if err == nil {
		t.Fatal("a lost rotated token must not be reported as success")
	}
	if !strings.Contains(err.Error(), "re-authorized") {
		t.Fatalf("error should name the consequence, got: %v", err)
	}
	if calls.Load() != ctraderPersistAttempts {
		t.Fatalf("persister called %d times, want %d", calls.Load(), ctraderPersistAttempts)
	}
	// The rotated pair still lives in RAM: the old refresh token is dead at
	// the broker, so keeping it would only guarantee the next call fails too.
	if c.currentRefreshToken() != "new-refresh" {
		t.Fatalf("in-memory refresh token: got %q, want new-refresh", c.currentRefreshToken())
	}
}

// E-B: expires_in is recorded so the next call refreshes ahead of expiry
// instead of after a rejection.
func TestCTraderRefresh_ArmsProactiveRefresh(t *testing.T) {
	server := ctraderRefreshServer(t, `{"access_token":"a","refresh_token":"r","expires_in":60}`)

	c := &CTrader{
		clientID:     "id",
		clientSecret: "secret",
		accessToken:  "old",
		refreshToken: "old-refresh",
		authURL:      server.URL,
		httpClient:   &http.Client{Timeout: 5 * time.Second},
	}
	if c.needsProactiveRefresh() {
		t.Fatal("no expiry known yet — proactive refresh must stay disarmed")
	}
	if err := c.refreshAccessToken(context.Background()); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	// 60 s of life, 5 min window → due immediately.
	if !c.needsProactiveRefresh() {
		t.Fatal("a token expiring in 60 s must be due for a proactive refresh")
	}
}

// cTrader requires the application auth first on every connection. The
// proactive refresh dropped the socket and let the next request open a new one
// without it, so the call right after the refresh was refused.
func TestCTraderProactiveRefresh_ReauthenticatesTheNewSession(t *testing.T) {
	var mu sync.Mutex
	appAuthed := map[*websocket.Conn]bool{}
	var seenToken string

	wsServer := newCTraderWSServer(t, func(conn *websocket.Conn, msg wsTestMessage) {
		mu.Lock()
		defer mu.Unlock()
		switch msg.PayloadType {
		case ctraderPayloadAppAuthReq:
			appAuthed[conn] = true
			sendWSResponse(t, conn, msg.ClientMsgID, ctraderPayloadAppAuthRes, map[string]any{})
		case ctraderPayloadGetAccountsReq:
			if !appAuthed[conn] {
				sendWSError(t, conn, msg.ClientMsgID, "CH_CLIENT_NOT_AUTHENTICATED", "application not authorized on this connection")
				return
			}
			seenToken, _ = msg.Payload["accessToken"].(string)
			sendWSResponse(t, conn, msg.ClientMsgID, ctraderPayloadGetAccountsRes, map[string]any{
				"ctidTraderAccount": []map[string]any{{"ctidTraderAccountId": 12345, "isLive": true}},
			})
		default:
			t.Errorf("unexpected payloadType: %d", msg.PayloadType)
		}
	})
	defer wsServer.Close()
	tokenServer := ctraderRefreshServer(t, `{"access_token":"new-access","refresh_token":"new-refresh","expires_in":3600}`)

	c := &CTrader{
		clientID:          "id",
		clientSecret:      "secret",
		accessToken:       "old-access",
		refreshToken:      "old-refresh",
		accessTokenExpiry: time.Now().Add(time.Minute),
		isLive:            true,
		wsLiveURL:         toWSURL(wsServer.URL),
		authURL:           tokenServer.URL,
		httpClient:        &http.Client{Timeout: 5 * time.Second},
	}

	if _, err := c.getAccounts(context.Background()); err != nil {
		t.Fatalf("the call after a proactive refresh failed: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if seenToken != "new-access" {
		t.Fatalf("request carried %q, want the refreshed token", seenToken)
	}
}

// E-H4: after a refresh, CurrentCredentials must report the ROTATED pair.
// The connect path stores what this returns; storing the request's pair wrote
// a refresh token the broker had already consumed.
func TestCTraderCurrentCredentials_ReportsTheRotatedPair(t *testing.T) {
	server := ctraderRefreshServer(t, `{"access_token":"rotated-access","refresh_token":"rotated-refresh","expires_in":3600}`)

	c := &CTrader{
		clientID:     "id",
		clientSecret: "secret",
		accessToken:  "connect-time-access",
		refreshToken: "connect-time-refresh",
		authURL:      server.URL,
		httpClient:   &http.Client{Timeout: 5 * time.Second},
	}

	access, refresh := c.CurrentCredentials()
	if access != "connect-time-access" || refresh != "connect-time-refresh" {
		t.Fatalf("before refresh: got %q/%q", access, refresh)
	}

	if err := c.refreshAccessToken(context.Background()); err != nil {
		t.Fatalf("refresh: %v", err)
	}

	access, refresh = c.CurrentCredentials()
	if access != "rotated-access" || refresh != "rotated-refresh" {
		t.Fatalf("after refresh: got %q/%q, want the rotated pair", access, refresh)
	}
}
