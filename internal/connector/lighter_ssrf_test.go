package connector

import (
	"context"
	"strings"
	"testing"
)

// CONN-15c: data_url is upstream-controlled. Following it unchecked turned a
// compromised Lighter API into a GET against the deployment's own network.
func TestAssertPublicHTTPSURLRejectsInternalTargets(t *testing.T) {
	cases := []struct {
		name string
		url  string
	}{
		{"cloud metadata", "https://169.254.169.254/computeMetadata/v1/"},
		{"loopback", "https://127.0.0.1:8080/export.csv"},
		{"rfc1918", "https://10.0.0.5/export.csv"},
		{"ipv6 loopback", "https://[::1]/export.csv"},
		{"ipv6 unique local", "https://[fd00::1]/export.csv"},
		{"docker service name", "https://mt-bridge:8090/export.csv"},
		{"plain http", "http://example.com/export.csv"},
		{"credentials in url", "https://user:pw@example.com/export.csv"},
		{"no host", "https:///export.csv"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := assertPublicHTTPSURL(context.Background(), tc.url); err == nil {
				t.Fatalf("accepted %s", tc.url)
			}
		})
	}
}

func TestAssertPublicHTTPSURLAcceptsPublicAddress(t *testing.T) {
	if err := assertPublicHTTPSURL(context.Background(), "https://93.184.216.34/export.csv"); err != nil {
		t.Fatalf("public address rejected: %v", err)
	}
}

// downloadCSV must refuse before any request is issued.
func TestDownloadCSVRefusesInternalURL(t *testing.T) {
	l := NewLighter(&Credentials{APIKey: "1"})
	_, err := l.downloadCSV(context.Background(), "https://169.254.169.254/latest/meta-data/")
	if err == nil {
		t.Fatal("downloadCSV followed a link-local URL")
	}
	if !strings.Contains(err.Error(), "non-public address") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// A data_url on the API origin is what the export actually returns against a
// local harness; it must not be blocked by the public-address rule.
func TestAssertExportURLAllowsAPIOrigin(t *testing.T) {
	l := NewLighter(&Credentials{APIKey: "1"})
	l.baseURL = "http://127.0.0.1:8080"
	if err := l.assertExportURL(context.Background(), "http://127.0.0.1:8080/csv"); err != nil {
		t.Fatalf("same-origin export url rejected: %v", err)
	}
	if err := l.assertExportURL(context.Background(), "http://127.0.0.1:9999/csv"); err == nil {
		t.Fatal("a different loopback port is not the API origin")
	}
}
