package connector

import (
	"strings"
	"testing"
)

// SEC-13: the bridge connect call ships the MT investor password in cleartext
// JSON and signs the request with MT_BRIDGE_HMAC_SECRET. Production must
// refuse to build the connector rather than send either over an unprotected
// link — the same fail-closed stance the rebuilder gets from CFG-002/CFG-003.
func TestNewMetaTraderRefusesUnprotectedBridgeInProduction(t *testing.T) {
	creds := &Credentials{Exchange: "mt5", APIKey: "12345", APISecret: "pw", Passphrase: "Broker-Demo"}

	cases := []struct {
		name   string
		url    string
		secret string
		want   string
	}{
		{"unset url", "", "0123456789012345678901234", "https://"},
		{"plain http", "http://mt-bridge:8090", "0123456789012345678901234", "https://"},
		{"empty secret", "https://mt-bridge:8090", "", "MT_BRIDGE_HMAC_SECRET"},
		{"short secret", "https://mt-bridge:8090", "tooshort", "MT_BRIDGE_HMAC_SECRET"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("NODE_ENV", "")
			t.Setenv("ENV", "production")
			t.Setenv("MT_BRIDGE_URL", tc.url)
			t.Setenv("MT_BRIDGE_HMAC_SECRET", tc.secret)

			mt, err := NewMetaTrader(creds)
			if err == nil {
				t.Fatalf("connector built with bridge %q and a %d-char secret", mt.bridgeURL, len(mt.hmacSecret))
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error does not name the missing control: %v", err)
			}
		})
	}
}

func TestNewMetaTraderAcceptsProtectedBridgeInProduction(t *testing.T) {
	t.Setenv("NODE_ENV", "")
	t.Setenv("ENV", "production")
	t.Setenv("MT_BRIDGE_URL", "https://mt-bridge.internal:8090/")
	t.Setenv("MT_BRIDGE_HMAC_SECRET", "0123456789012345678901234")

	mt, err := NewMetaTrader(&Credentials{Exchange: "mt4", APIKey: "1", APISecret: "pw", Passphrase: "S"})
	if err != nil {
		t.Fatalf("protected bridge rejected: %v", err)
	}
	if mt.bridgeURL != "https://mt-bridge.internal:8090" {
		t.Fatalf("trailing slash not trimmed: %q", mt.bridgeURL)
	}
}

// Development keeps the loopback default so local stacks still work.
func TestNewMetaTraderDevKeepsLoopbackDefault(t *testing.T) {
	t.Setenv("NODE_ENV", "")
	t.Setenv("ENV", "development")
	t.Setenv("MT_BRIDGE_URL", "")
	t.Setenv("MT_BRIDGE_HMAC_SECRET", "")

	mt, err := NewMetaTrader(&Credentials{Exchange: "mt5", APIKey: "1", APISecret: "pw", Passphrase: "S"})
	if err != nil {
		t.Fatalf("dev build rejected: %v", err)
	}
	if mt.bridgeURL != "http://mt-bridge:8090" {
		t.Fatalf("dev default lost: %q", mt.bridgeURL)
	}
}
