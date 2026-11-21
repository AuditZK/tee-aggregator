package connector

import (
	"context"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// V-E1: a BLOCKED_PAYLOAD_TYPE in the middle of a deal walk used to abort the
// whole reconstruction, which then restarted from zero on the next sync with
// the same request profile. The page must be retried instead, honouring the
// retryAfter the broker supplies.
func TestCTraderGetAllDeals_RetriesAfterRateLimit(t *testing.T) {
	var dealCalls atomic.Int32

	server := newCTraderWSServer(t, func(conn *websocket.Conn, msg wsTestMessage) {
		switch msg.PayloadType {
		case ctraderPayloadAppAuthReq:
			sendWSResponse(t, conn, msg.ClientMsgID, ctraderPayloadAppAuthRes, map[string]any{})
		case ctraderPayloadAccountAuthReq:
			sendWSResponse(t, conn, msg.ClientMsgID, ctraderPayloadAccountAuthRes, map[string]any{})
		case ctraderPayloadDealListReq:
			if dealCalls.Add(1) == 1 {
				// Rate-limited once, with a retryAfter 50 ms out.
				sendWSResponse(t, conn, msg.ClientMsgID, ctraderPayloadErrorRes, map[string]any{
					"errorCode":   "BLOCKED_PAYLOAD_TYPE",
					"description": "You are being rate limited",
					"retryAfter":  time.Now().Add(50 * time.Millisecond).UnixMilli(),
				})
				return
			}
			sendWSResponse(t, conn, msg.ClientMsgID, ctraderPayloadDealListRes, map[string]any{
				"deal": []map[string]any{{
					"dealId":             1,
					"executionTimestamp": 1780688563637,
					"dealStatus":         2,
					"moneyDigits":        2,
				}},
				"hasMore": false,
			})
		default:
			t.Errorf("unexpected payloadType: %d", msg.PayloadType)
		}
	})
	defer server.Close()

	c := &CTrader{
		clientID:         "client-id",
		clientSecret:     "client-secret",
		accessToken:      "token",
		isLive:           true,
		wsLiveURL:        toWSURL(server.URL),
		httpClient:       &http.Client{Timeout: 5 * time.Second},
		rateLimitBackoff: 10 * time.Millisecond,
	}

	deals, err := c.getAllDeals(context.Background(), 12345, time.Now().Add(-24*time.Hour), time.Now())
	if err != nil {
		t.Fatalf("getAllDeals must survive one rate limit: %v", err)
	}
	if len(deals) != 1 {
		t.Fatalf("got %d deals, want 1", len(deals))
	}
	if dealCalls.Load() != 2 {
		t.Fatalf("deal list called %d times, want 2 (one blocked, one retried)", dealCalls.Load())
	}
}

// A page that stays blocked must surface an error rather than silently
// returning a short history.
func TestCTraderGetAllDeals_GivesUpAfterRepeatedRateLimits(t *testing.T) {
	var dealCalls atomic.Int32

	server := newCTraderWSServer(t, func(conn *websocket.Conn, msg wsTestMessage) {
		switch msg.PayloadType {
		case ctraderPayloadAppAuthReq:
			sendWSResponse(t, conn, msg.ClientMsgID, ctraderPayloadAppAuthRes, map[string]any{})
		case ctraderPayloadAccountAuthReq:
			sendWSResponse(t, conn, msg.ClientMsgID, ctraderPayloadAccountAuthRes, map[string]any{})
		case ctraderPayloadDealListReq:
			dealCalls.Add(1)
			sendWSError(t, conn, msg.ClientMsgID, "BLOCKED_PAYLOAD_TYPE", "You are being rate limited")
		default:
			t.Errorf("unexpected payloadType: %d", msg.PayloadType)
		}
	})
	defer server.Close()

	c := &CTrader{
		clientID:         "client-id",
		clientSecret:     "client-secret",
		accessToken:      "token",
		isLive:           true,
		wsLiveURL:        toWSURL(server.URL),
		httpClient:       &http.Client{Timeout: 5 * time.Second},
		rateLimitBackoff: time.Millisecond,
	}

	_, err := c.getAllDeals(context.Background(), 12345, time.Now().Add(-24*time.Hour), time.Now())
	if err == nil {
		t.Fatal("expected an error when every attempt is rate limited")
	}
	if !strings.Contains(err.Error(), "rate limited") {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := dealCalls.Load(); got != ctraderRateLimitAttempts {
		t.Fatalf("deal list called %d times, want %d", got, ctraderRateLimitAttempts)
	}
}

// E-H1: pagination that runs out of pages must refuse, not return a truncated
// history. cTrader pages forward in time, so the lost deals are the recent
// ones and the carry-forward builder would draw a flat line to today.
func TestCTraderGetAllDeals_RefusesTruncatedHistory(t *testing.T) {
	var dealCalls atomic.Int32

	server := newCTraderWSServer(t, func(conn *websocket.Conn, msg wsTestMessage) {
		switch msg.PayloadType {
		case ctraderPayloadAppAuthReq:
			sendWSResponse(t, conn, msg.ClientMsgID, ctraderPayloadAppAuthRes, map[string]any{})
		case ctraderPayloadAccountAuthReq:
			sendWSResponse(t, conn, msg.ClientMsgID, ctraderPayloadAccountAuthRes, map[string]any{})
		case ctraderPayloadDealListReq:
			// Always one more page, each advancing the cursor by a second.
			n := int64(dealCalls.Add(1))
			sendWSResponse(t, conn, msg.ClientMsgID, ctraderPayloadDealListRes, map[string]any{
				"deal": []map[string]any{{
					"dealId":             n,
					"executionTimestamp": 1780688563637 + n*1000,
					"dealStatus":         2,
					"moneyDigits":        2,
				}},
				"hasMore": true,
			})
		default:
			t.Errorf("unexpected payloadType: %d", msg.PayloadType)
		}
	})
	defer server.Close()

	c := &CTrader{
		clientID:     "client-id",
		clientSecret: "client-secret",
		accessToken:  "token",
		isLive:       true,
		wsLiveURL:    toWSURL(server.URL),
		httpClient:   &http.Client{Timeout: 5 * time.Second},
		// Real cap is 200 pages at 600 ms apart; the mechanism is identical
		// at 3 pages and the test finishes in milliseconds.
		maxDealPages:     3,
		histRequestDelay: time.Millisecond,
	}

	deals, err := c.getAllDeals(context.Background(), 12345, time.Now().Add(-24*time.Hour), time.Now())
	if err == nil {
		t.Fatalf("expected a refusal, got %d deals from an endless pager", len(deals))
	}
	if !strings.Contains(err.Error(), "refusing a truncated history") {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := dealCalls.Load(); got != 3 {
		t.Fatalf("walked %d pages, want the cap of 3", got)
	}
}
