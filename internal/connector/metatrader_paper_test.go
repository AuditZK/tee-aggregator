package connector

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

// fakeMTBridge answers /connect and /account-info like mt-bridge does, with the
// broker-reported server and account type given. It counts account-info calls.
func fakeMTBridge(t *testing.T, reportedServer, accountType string, accountInfoCalls *atomic.Int32) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		var data any
		switch {
		case r.URL.Path == "/api/v1/connect":
			data = map[string]any{"session_id": "s1"}
		case strings.HasSuffix(r.URL.Path, "/account-info"):
			accountInfoCalls.Add(1)
			data = map[string]any{
				"balance":      1000.0,
				"equity":       1000.0,
				"currency":     "USD",
				"server":       reportedServer,
				"account_type": accountType,
				"company":      "Broker Ltd",
			}
		default:
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"success": true, "data": data})
	}))
}

func newTestMetaTrader(t *testing.T, bridgeURL, typedServer string) *MetaTrader {
	t.Helper()
	t.Setenv("NODE_ENV", "development")
	t.Setenv("ENV", "")
	t.Setenv("MT_BRIDGE_URL", bridgeURL)
	mt, err := NewMetaTrader(&Credentials{Exchange: "mt5", APIKey: "12345", APISecret: "pw", Passphrase: typedServer})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	return mt
}

func TestMetaTraderDetectIsPaper(t *testing.T) {
	cases := []struct {
		name           string
		typedServer    string
		reportedServer string
		accountType    string
		want           bool
	}{
		{"typed name says demo", "ICMarketsSC-Demo", "ICMarketsSC-Demo", "", true},
		{"typed IP, broker reports a demo server", "10.0.0.5", "ICMarketsSC-Demo", "", true},
		{"clean names, account type demo", "OANDATMS-MT5", "OANDATMS-MT5", "demo", true},
		{"clean names, account type contest", "Broker-Live", "Broker-Live", "contest", true},
		{"clean names, account type preliminary", "Broker-Live", "Broker-Live", "preliminary", true},
		{"account type real does not clear a demo name", "X-Demo", "X-Live", "real", true},
		{"account type real does not clear a reported demo name", "10.0.0.5", "X-Trial", "real", true},
		{"account type unknown, clean names", "Broker-Live", "Broker-Live", "unknown", false},
		{"older bridge, no new fields, clean name", "Broker-Live", "", "", false},
		{"account type real, clean names", "Broker-Live", "Broker-Live", "real", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			bridge := fakeMTBridge(t, tc.reportedServer, tc.accountType, &calls)
			defer bridge.Close()

			mt := newTestMetaTrader(t, bridge.URL, tc.typedServer)
			got, err := mt.DetectIsPaper(context.Background())
			if err != nil {
				t.Fatalf("DetectIsPaper: %v", err)
			}
			if got != tc.want {
				t.Fatalf("DetectIsPaper = %v, want %v", got, tc.want)
			}
		})
	}
}

// A typed demo name decides without the bridge. A clean name with a bridge
// that cannot be reached is an error, so the caller keeps the stored verdict.
func TestMetaTraderDetectIsPaperFallsBackToTypedName(t *testing.T) {
	bridge := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer bridge.Close()

	for _, tc := range []struct {
		server  string
		want    bool
		wantErr bool
	}{{"Broker-Demo", true, false}, {"Broker-Live", false, true}} {
		mt := newTestMetaTrader(t, bridge.URL, tc.server)
		got, err := mt.DetectIsPaper(context.Background())
		if (err != nil) != tc.wantErr {
			t.Fatalf("%s: DetectIsPaper error = %v, wantErr %v", tc.server, err, tc.wantErr)
		}
		if got != tc.want {
			t.Fatalf("%s: DetectIsPaper = %v, want %v", tc.server, got, tc.want)
		}
	}
}

// GetBalance already reads account-info; DetectIsPaper afterwards reuses it.
func TestMetaTraderDetectIsPaperReusesAccountInfo(t *testing.T) {
	var calls atomic.Int32
	bridge := fakeMTBridge(t, "Broker-Live", "demo", &calls)
	defer bridge.Close()

	mt := newTestMetaTrader(t, bridge.URL, "Broker-Live")
	if _, err := mt.GetBalance(context.Background()); err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	got, err := mt.DetectIsPaper(context.Background())
	if err != nil || !got {
		t.Fatalf("DetectIsPaper = %v, %v; want true", got, err)
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("account-info called %d times, want 1", n)
	}
	if mt.company != "Broker Ltd" || mt.reportedServer != "Broker-Live" {
		t.Fatalf("broker fields not kept: company=%q server=%q", mt.company, mt.reportedServer)
	}
}
