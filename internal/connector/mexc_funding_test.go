package connector

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"
)

// mexcFundingServer pages records newest first, at most page_size per page,
// as the contract API documents. Only the documented path answers.
func mexcFundingServer(t *testing.T, records []map[string]any, success bool) (*MEXC, *int) {
	t.Helper()
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != mexcPathFundingRecords {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		calls++
		w.Header().Set("Content-Type", "application/json")
		if !success {
			_ = json.NewEncoder(w).Encode(map[string]any{"success": false, "code": 401})
			return
		}
		page, _ := strconv.Atoi(r.URL.Query().Get("page_num"))
		size, _ := strconv.Atoi(r.URL.Query().Get("page_size"))
		if size > mexcFundingPageSize {
			http.Error(w, "page_size", http.StatusBadRequest)
			return
		}
		lo, hi := (page-1)*size, page*size
		if lo > len(records) {
			lo = len(records)
		}
		if hi > len(records) {
			hi = len(records)
		}
		totalPage := (len(records) + size - 1) / size
		_ = json.NewEncoder(w).Encode(map[string]any{
			"success": true, "code": 0,
			"data": map[string]any{
				"pageSize": size, "totalCount": len(records), "totalPage": totalPage,
				"currentPage": page, "resultList": records[lo:hi],
			},
		})
	}))
	t.Cleanup(srv.Close)
	m := NewMEXC(&Credentials{APIKey: "k", APISecret: "s"})
	m.futuresURL = srv.URL
	return m, &calls
}

// mexcRecords settles three symbols every eight hours, newest first.
func mexcRecords(newest time.Time, settlements int) []map[string]any {
	var out []map[string]any
	id := int64(1_000_000)
	for i := 0; i < settlements; i++ {
		ts := newest.Add(-time.Duration(i) * 8 * time.Hour).UnixMilli()
		for _, sym := range []string{"BTC_USDT", "ETH_USDT", "SOL_USDT"} {
			out = append(out, map[string]any{
				"id": id, "symbol": sym, "positionType": 1, "positionValue": 1000.0,
				"funding": -0.5, "rate": 0.0005, "settleTime": ts,
			})
			id--
		}
	}
	return out
}

// The reader called /account/funding_records, which MEXC does not have, and
// turned the failure into "no funding": every MEXC day stored zero funding.
// It also read only the newest 100 records.
func TestMEXCFundingReadsTheDocumentedPathAndEveryPage(t *testing.T) {
	newest := time.Date(2026, 11, 26, 8, 0, 0, 0, time.UTC)
	records := mexcRecords(newest, 120) // 360 records, 40 days
	m, calls := mexcFundingServer(t, records, true)

	since := newest.Add(-30 * 24 * time.Hour) // 91 settlements x 3 symbols
	fees, err := m.GetFundingFees(context.Background(), nil, since)
	if err != nil {
		t.Fatalf("GetFundingFees: %v", err)
	}
	if len(fees) != 273 {
		t.Fatalf("read %d settlements, want 273", len(fees))
	}
	for _, f := range fees {
		if f.Timestamp.Before(since) {
			t.Fatalf("settlement %s is before since", f.Timestamp)
		}
		if f.Amount != -0.5 {
			t.Fatalf("amount %v, want -0.5 (paid)", f.Amount)
		}
	}
	// 273 records sit on pages 1 to 3, and page 3 already reaches past since.
	if *calls != 3 {
		t.Errorf("%d pages read, want 3", *calls)
	}
}

func TestMEXCFundingStopsAtTheLastPage(t *testing.T) {
	newest := time.Date(2026, 11, 26, 8, 0, 0, 0, time.UTC)
	m, calls := mexcFundingServer(t, mexcRecords(newest, 10), true)

	fees, err := m.GetFundingFees(context.Background(), nil, newest.Add(-365*24*time.Hour))
	if err != nil {
		t.Fatalf("GetFundingFees: %v", err)
	}
	if len(fees) != 30 || *calls != 1 {
		t.Errorf("read %d settlements in %d pages, want 30 in 1", len(fees), *calls)
	}
}

// A key without futures access is not an account that paid no funding. A
// recount must keep what it stored rather than overwrite it with zero.
func TestMEXCFundingWithoutFuturesAccessIsNotRead(t *testing.T) {
	m, _ := mexcFundingServer(t, nil, false)

	fees, err := m.GetFundingFees(context.Background(), nil, time.Now().Add(-24*time.Hour))
	if !errors.Is(err, ErrFundingNotRead) {
		t.Fatalf("want ErrFundingNotRead, got fees=%v err=%v", fees, err)
	}
}

func TestMEXCFundingTransportFailureIsAnError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusForbidden)
	}))
	t.Cleanup(srv.Close)
	m := NewMEXC(&Credentials{APIKey: "k", APISecret: "s"})
	m.futuresURL = srv.URL

	_, err := m.GetFundingFees(context.Background(), nil, time.Now().Add(-24*time.Hour))
	if err == nil || errors.Is(err, ErrFundingNotRead) {
		t.Fatalf("want a plain error, got %v", err)
	}
}
