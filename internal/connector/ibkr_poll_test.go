package connector

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type flexInProgressTransport struct{ polls atomic.Int32 }

func (f *flexInProgressTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	f.polls.Add(1)
	body := `<FlexStatementResponse><Status>Warn</Status><ErrorCode>1019</ErrorCode><ErrorMessage>Statement generation in progress. Please try again shortly.</ErrorMessage></FlexStatementResponse>`
	return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(body)), Header: http.Header{}}, nil
}

func TestPollFlexReport_StatementStillGeneratingIsTransient(t *testing.T) {
	saved := flexPollDelays
	flexPollDelays = []time.Duration{time.Millisecond, time.Millisecond, time.Millisecond}
	t.Cleanup(func() { flexPollDelays = saved })

	stub := &flexInProgressTransport{}
	i := &IBKR{token: "SYNTHETICPOLLTOKEN0001", queryID: "123456", client: &http.Client{Transport: stub}}

	_, err := i.pollFlexReport(context.Background(), flexURL(ibkrFlexGetURL, i.token, "REF42"))
	if err == nil {
		t.Fatal("a statement never ready must fail the poll")
	}
	if !errors.Is(err, ErrTransient) {
		t.Fatalf("still generating is IBKR's own retry-later, it must be transient: %v", err)
	}
	if got := stub.polls.Load(); got != 3 {
		t.Fatalf("polled %d times, want one per delay (3)", got)
	}
}

func TestFlexPollBudget_OutlastsTheOvernightBatch(t *testing.T) {
	if got := FlexPollBudget(); got < 10*time.Minute {
		t.Fatalf("poll budget %v: four minutes was too short at the 6h retry", got)
	}
}
