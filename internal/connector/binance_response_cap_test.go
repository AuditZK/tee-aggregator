package connector

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

// TestBinance_DoRequest_BoundsHostileUpstream is the regression for
// HIGH-001: a Binance upstream that streams more than DefaultMaxResponseBytes
// must yield ErrResponseTooLarge instead of an unbounded heap allocation.
//
// We rewrite both the spot and futures hosts to a local httptest.Server so
// the production code path (Binance.doRequest → ReadCappedBody) is exercised
// end-to-end without touching the real network.
func TestBinance_DoRequest_BoundsHostileUpstream(t *testing.T) {
	hostile := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		// 16 MiB > DefaultMaxResponseBytes (8 MiB). Written once: the
		// LimitReader inside ReadCappedBody short-circuits on the read,
		// so the test runs in well under a second.
		_, _ = w.Write(bytes.Repeat([]byte("X"), 16<<20))
	}))
	defer hostile.Close()

	hostileURL, err := url.Parse(hostile.URL)
	if err != nil {
		t.Fatalf("parse hostile URL: %v", err)
	}

	rewriting := &http.Client{
		Transport: hostRewriter{base: http.DefaultTransport, target: hostileURL},
	}
	b := NewBinanceWithClient(&Credentials{APIKey: "k", APISecret: "s"}, rewriting)

	err = b.TestConnection(context.Background())
	if err == nil {
		t.Fatal("expected ErrResponseTooLarge from oversized upstream, got nil")
	}
	if !errors.Is(err, ErrResponseTooLarge) {
		t.Fatalf("expected ErrResponseTooLarge, got %v", err)
	}
}

// hostRewriter is a tiny RoundTripper that redirects every outbound request
// to a local test server while keeping the original path/query/headers.
type hostRewriter struct {
	base   http.RoundTripper
	target *url.URL
}

func (h hostRewriter) RoundTrip(req *http.Request) (*http.Response, error) {
	clone := req.Clone(req.Context())
	clone.URL.Scheme = h.target.Scheme
	clone.URL.Host = h.target.Host
	clone.Host = h.target.Host
	// Strip the binance.com host header to avoid TLS SNI confusion on
	// the local httptest.Server (plain HTTP in this test).
	clone.Header = req.Header.Clone()
	if strings.HasPrefix(clone.Header.Get("Host"), "api.binance") {
		clone.Header.Del("Host")
	}
	return h.base.RoundTrip(clone)
}
