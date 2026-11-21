package connector

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"
)

// SEC-11: the Flex endpoint accepts no auth header, so flexURL puts the token
// in the query string — and *url.Error prints the full URL. Every transport
// failure must therefore be re-rendered before it reaches SyncResult.Error,
// sync_statuses.errorMessage and the REST/gRPC response.
const auditFlexToken = "SYNTHETIC1234567890TOKEN"

type failingRoundTripper struct{}

func (failingRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, errors.New("dial tcp 198.51.100.1:443: connect: connection refused")
}

func newLeakProbeIBKR() *IBKR {
	return &IBKR{
		token:   auditFlexToken,
		queryID: "999999",
		client:  &http.Client{Transport: failingRoundTripper{}},
	}
}

func TestFlexTransportErrorHidesToken(t *testing.T) {
	i := newLeakProbeIBKR()

	day := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	if _, err := i.requestStatement(context.Background(), day, day); err == nil {
		t.Fatal("requestStatement: want transport error, got nil")
	} else if strings.Contains(err.Error(), auditFlexToken) {
		t.Fatalf("flex token leaked through requestStatement: %s", err)
	}

	if _, err := i.pollFlexReport(context.Background(), flexURL(ibkrFlexWSGetURL, i.token, "REF123")); err == nil {
		t.Fatal("pollFlexReport: want transport error, got nil")
	} else if strings.Contains(err.Error(), auditFlexToken) {
		t.Fatalf("flex token leaked through pollFlexReport: %s", err)
	}
}

// The unscrubbed error is what the fix removes — assert the probe is real and
// not passing because the URL never carried the token.
func TestFlexTransportErrorWouldLeakUnscrubbed(t *testing.T) {
	i := newLeakProbeIBKR()
	req, err := http.NewRequest("GET", flexURL(ibkrFlexWSSendURL, i.token, i.queryID), nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	_, rawErr := i.client.Do(req)
	if rawErr == nil {
		t.Fatal("want transport error from the stub, got nil")
	}
	if !strings.Contains(rawErr.Error(), auditFlexToken) {
		t.Fatalf("probe is inert — the raw error no longer carries the token: %s", rawErr)
	}
}
