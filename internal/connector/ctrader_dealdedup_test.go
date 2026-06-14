package connector

import (
	"reflect"
	"testing"
)

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
