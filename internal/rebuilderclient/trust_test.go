package rebuilderclient

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"
)

func TestRebuildRefusesACertificateFromAnotherAuthority(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("credentials reached a server the pinned roots do not vouch for")
	}))
	defer srv.Close()

	_, err := New(srv.URL, "token", zap.NewNop()).Rebuild(context.Background(), RebuildRequest{
		UserUID: "u", Exchange: "binance", Label: "main",
		Credentials: Credentials{APIKey: "synthetic-key", APISecret: "synthetic-secret"},
	})
	if err == nil || !strings.Contains(err.Error(), "certificate") {
		t.Fatalf("err = %v, want a certificate verification failure", err)
	}
}

// The chain rebuilder.auditzk.com served in September 2026: leaf, YE1, Root YE
// cross-signed by X2, X2 cross-signed by X1.
func servedChain(t *testing.T) []*x509.Certificate {
	t.Helper()
	raw, err := os.ReadFile("testdata/rebuilder_chain_2026-09.pem")
	if err != nil {
		t.Fatal(err)
	}
	var certs []*x509.Certificate
	for block, rest := pem.Decode(raw); block != nil; block, rest = pem.Decode(rest) {
		c, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			t.Fatal(err)
		}
		certs = append(certs, c)
	}
	if len(certs) < 2 {
		t.Fatalf("fixture holds %d certificates", len(certs))
	}
	return certs
}

func TestPinnedRootsVerifyTheRebuildersChain(t *testing.T) {
	chain := servedChain(t)
	roots := pinnedTransport().TLSClientConfig.RootCAs

	for name, intermediates := range map[string][]*x509.Certificate{
		"as served":                 chain[1:],
		"without the cross-signing": chain[1:2],
	} {
		pool := x509.NewCertPool()
		for _, c := range intermediates {
			pool.AddCert(c)
		}
		_, err := chain[0].Verify(x509.VerifyOptions{
			DNSName:       "rebuilder.auditzk.com",
			Roots:         roots,
			Intermediates: pool,
			CurrentTime:   time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC),
		})
		if err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
}
