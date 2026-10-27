package connector

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

type flexStubTransport struct {
	mu   sync.Mutex
	urls []string
	doc  string
}

func (f *flexStubTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	f.mu.Lock()
	f.urls = append(f.urls, r.URL.String())
	f.mu.Unlock()
	body := `<FlexStatementResponse><Status>Success</Status><ReferenceCode>REF42</ReferenceCode></FlexStatementResponse>`
	if strings.Contains(r.URL.Path, "GetStatement") {
		body = f.doc
	}
	return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(body)), Header: http.Header{}}, nil
}

func TestGetRawStatementForPeriodSendsDatesAndSkipsSharedCache(t *testing.T) {
	const token = "SYNTHETICPERIODTOKEN0001"
	stub := &flexStubTransport{doc: `<FlexQueryResponse queryName="q" echo="` + token + `"><FlexStatements count="1"/></FlexQueryResponse>`}
	i := &IBKR{token: token, queryID: "123456", client: &http.Client{Transport: stub}}

	from := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2023, 12, 31, 0, 0, 0, 0, time.UTC)
	doc, ct, err := i.GetRawStatementForPeriod(context.Background(), from, to)
	if err != nil {
		t.Fatalf("GetRawStatementForPeriod: %v", err)
	}
	if ct != "application/xml" {
		t.Fatalf("content type = %q", ct)
	}
	if strings.Contains(string(doc), token) {
		t.Fatal("token echoed by the venue reached the caller")
	}

	if len(stub.urls) < 1 || !strings.Contains(stub.urls[0], "fd=20230101") || !strings.Contains(stub.urls[0], "td=20231231") {
		t.Fatalf("SendRequest did not carry the period: %v", stub.urls)
	}
	// Only the Flex Web Service honours a chosen period.
	for _, u := range stub.urls {
		if !strings.HasPrefix(u, "https://ndcdyn.interactivebrokers.com/AccountManagement/FlexWebService/") {
			t.Fatalf("chosen-period request went to %s", u)
		}
	}

	flexReportCacheMu.Lock()
	_, cached := flexReportCache[token+":123456"]
	flexReportCacheMu.Unlock()
	if cached {
		t.Fatal("a chosen-period statement landed in the cache the daily sync reads")
	}
}

func TestGetRawStatementForPeriodRespectsTokenGate(t *testing.T) {
	const token = "SYNTHETICPERIODTOKEN0002"
	stub := &flexStubTransport{doc: `<FlexQueryResponse><FlexStatements count="1"/></FlexQueryResponse>`}
	i := &IBKR{token: token, queryID: "123456", client: &http.Client{Transport: stub}}
	from := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2023, 12, 31, 0, 0, 0, 0, time.UTC)

	if _, _, err := i.GetRawStatementForPeriod(context.Background(), from, to); err != nil {
		t.Fatalf("first call: %v", err)
	}
	sent := len(stub.urls)
	if _, _, err := i.GetRawStatementForPeriod(context.Background(), from, to); err == nil {
		t.Fatal("second call inside the cooldown went through")
	}
	if len(stub.urls) != sent {
		t.Fatal("the gated call still reached IBKR")
	}
}

func TestGetHistoricalSnapshotsForPeriodKeepsOnlyThePeriod(t *testing.T) {
	const token = "SYNTHETICPERIODTOKEN0003"
	stub := &flexStubTransport{doc: string(flexReport(`
      <EquitySummaryInBase>
        <EquitySummaryByReportDateInBase reportDate="20221231" total="900" cash="900"/>
        <EquitySummaryByReportDateInBase reportDate="20230102" total="1000" cash="1000"/>
        <EquitySummaryByReportDateInBase reportDate="20231229" total="1200" cash="1200"/>
        <EquitySummaryByReportDateInBase reportDate="20240102" total="1300" cash="1300"/>
      </EquitySummaryInBase>`))}
	i := &IBKR{token: token, queryID: "123456", client: &http.Client{Transport: stub}}

	got, err := i.GetHistoricalSnapshotsForPeriod(context.Background(),
		time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(2023, 12, 31, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("GetHistoricalSnapshotsForPeriod: %v", err)
	}
	if len(got) != 2 || got[0].Date.Format("20060102") != "20230102" || got[1].Date.Format("20060102") != "20231229" {
		t.Fatalf("days outside the period leaked in: %+v", got)
	}
	if !strings.Contains(stub.urls[0], "fd=20230101") || !strings.Contains(stub.urls[0], "td=20231231") {
		t.Fatalf("period not sent: %v", stub.urls)
	}
}
