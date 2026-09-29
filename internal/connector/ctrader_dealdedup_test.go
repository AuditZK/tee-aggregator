package connector

import (
	"context"
	"net/http"
	"reflect"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// Spotware documents no order within a deal-list page. The walk advanced from
// the LAST listed deal: on a page listed newest-first that is the oldest, so
// the next request re-read the same page, added nothing, and the walk ended as
// if the ledger were exhausted.
func TestCTraderGetAllDeals_PageOrderDoesNotEndTheWalk(t *testing.T) {
	base := int64(1780688563637)
	ledger := make([]map[string]any, 0, 5)
	for i := int64(1); i <= 5; i++ {
		ledger = append(ledger, map[string]any{"dealId": i, "executionTimestamp": base + i*1000, "dealStatus": 2, "moneyDigits": 2})
	}
	var calls atomic.Int32

	server := newCTraderWSServer(t, func(conn *websocket.Conn, msg wsTestMessage) {
		switch msg.PayloadType {
		case ctraderPayloadAppAuthReq:
			sendWSResponse(t, conn, msg.ClientMsgID, ctraderPayloadAppAuthRes, map[string]any{})
		case ctraderPayloadAccountAuthReq:
			sendWSResponse(t, conn, msg.ClientMsgID, ctraderPayloadAccountAuthRes, map[string]any{})
		case ctraderPayloadDealListReq:
			calls.Add(1)
			from := int64(msg.Payload["fromTimestamp"].(float64))
			// The two oldest deals at or after `from`, listed newest first.
			var page []map[string]any
			for _, d := range ledger {
				if d["executionTimestamp"].(int64) >= from && len(page) < 2 {
					page = append([]map[string]any{d}, page...)
				}
			}
			more := false
			for _, d := range ledger {
				if d["executionTimestamp"].(int64) > page[0]["executionTimestamp"].(int64) {
					more = true
				}
			}
			sendWSResponse(t, conn, msg.ClientMsgID, ctraderPayloadDealListRes, map[string]any{"deal": page, "hasMore": more})
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
		histRequestDelay: time.Millisecond,
	}

	deals, err := c.getAllDeals(context.Background(), 12345, time.UnixMilli(base), time.UnixMilli(base+10000))
	if err != nil {
		t.Fatalf("getAllDeals: %v", err)
	}
	got := make([]int64, 0, len(deals))
	for _, d := range deals {
		got = append(got, d.DealID)
	}
	sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
	if !reflect.DeepEqual(got, []int64{1, 2, 3, 4, 5}) {
		t.Fatalf("deals %v after %d pages, want all five", got, calls.Load())
	}
}

// CONN-05: pagination re-reads the boundary millisecond (from = last), so
// dedup-by-dealId must keep each deal exactly once while still capturing a
// second deal that shares the boundary timestamp — the case the old
// from=last+1 silently skipped.
func TestAppendUnseenDeals_BoundaryDedup(t *testing.T) {
	seen := make(map[int64]struct{})
	var all []cTraderDeal

	page1 := []cTraderDeal{
		{DealID: 1, ExecutionTimestamp: 10},
		{DealID: 2, ExecutionTimestamp: 20}, // last deal of page 1 (ts = boundary)
	}
	page2 := []cTraderDeal{
		{DealID: 2, ExecutionTimestamp: 20}, // re-read because from = last = 20
		{DealID: 3, ExecutionTimestamp: 20}, // shares the boundary ms — old code skipped it
		{DealID: 4, ExecutionTimestamp: 30},
	}

	var added1, added2 int
	all, added1 = appendUnseenDeals(all, seen, page1)
	all, added2 = appendUnseenDeals(all, seen, page2)

	if added1 != 2 {
		t.Fatalf("page1 added = %d, want 2", added1)
	}
	if added2 != 2 {
		t.Fatalf("page2 added = %d, want 2 (id=2 deduped; id=3 & id=4 new)", added2)
	}

	got := make([]int64, len(all))
	for i, d := range all {
		got[i] = d.DealID
	}
	want := []int64{1, 2, 3, 4}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("deals = %v, want %v (boundary deal kept once, same-ms deal not skipped)", got, want)
	}
}
