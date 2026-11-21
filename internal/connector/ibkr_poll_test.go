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

	_, err := i.pollFlexReport(context.Background(), flexURL(ibkrFlexWSGetURL, i.token, "REF42"))
	if err == nil {
		t.Fatal("a statement never ready must fail the poll")
	}
	if !errors.Is(err, ErrTransient) {
		t.Fatalf("still generating is IBKR's own retry-later, it must be transient: %v", err)
	}
	if !errors.Is(err, ErrStatementStillGenerating) {
		t.Fatalf("the backfill halves the period on this error, it must be told apart: %v", err)
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

// A backfill waits longer than a sync for a statement: the poll goes on at
// its last interval until the budget the caller gave.
func TestPollFlexReport_WaitsOutTheCallersBudget(t *testing.T) {
	saved := flexPollDelays
	flexPollDelays = []time.Duration{time.Millisecond, time.Millisecond, 2 * time.Millisecond}
	t.Cleanup(func() { flexPollDelays = saved })

	stub := &flexInProgressTransport{}
	i := &IBKR{token: "SYNTHETICPOLLTOKEN0002", queryID: "123456", client: &http.Client{Transport: stub}}

	ctx := WithFlexPollBudget(context.Background(), 10*time.Millisecond)
	_, err := i.pollFlexReport(ctx, flexURL(ibkrFlexWSGetURL, i.token, "REF43"))
	if !errors.Is(err, ErrStatementStillGenerating) {
		t.Fatalf("a statement never ready must still fail the poll: %v", err)
	}
	if got := stub.polls.Load(); got != 6 {
		t.Fatalf("polled %d times, want the 3 usual and 3 more to reach the budget", got)
	}
}

func TestFlexPollBudgetOf_NeverShortensThePoll(t *testing.T) {
	if got := FlexPollBudgetOf(context.Background()); got != FlexPollBudget() {
		t.Fatalf("no budget asked: %v, want the default %v", got, FlexPollBudget())
	}
	if got := FlexPollBudgetOf(WithFlexPollBudget(context.Background(), time.Second)); got != FlexPollBudget() {
		t.Fatalf("a shorter budget: %v, want the default %v", got, FlexPollBudget())
	}
	if got := FlexPollBudgetOf(WithFlexPollBudget(context.Background(), time.Hour)); got != time.Hour {
		t.Fatalf("a longer budget: %v, want it kept", got)
	}
}
