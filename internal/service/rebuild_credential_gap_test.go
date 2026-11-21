package service

import "testing"

// The rebuilder refuses a Lighter wallet address with a 400, every connect
// and every midnight after that. Asking anyway is plaintext egress for an
// answer already known, so the gate lives here, before the request.
func TestRebuildCredentialGap(t *testing.T) {
	cases := []struct {
		exchange, apiKey, want string
	}{
		{"lighter", "0xabc", "lighter_history_needs_ro_token"},
		{"lighter", "", "lighter_history_needs_ro_token"},
		{"lighter", "ro:12:single:1:sig", ""},
		{" Lighter ", "  RO:12:single:1:sig ", ""},
		// Only Lighter draws this line. A Hyperliquid wallet rebuilds from the
		// address alone; a Binance key has no ro: prefix and is not a wallet.
		{"hyperliquid", "0xabc", ""},
		{"binance", "AKIA", ""},
		{"", "0xabc", ""},
	}
	for _, c := range cases {
		if got := rebuildCredentialGap(c.exchange, c.apiKey); got != c.want {
			t.Errorf("rebuildCredentialGap(%q, %q) = %q, want %q", c.exchange, c.apiKey, got, c.want)
		}
	}
}
