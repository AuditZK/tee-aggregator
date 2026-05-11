package service

import "testing"

func TestClassifySyncError(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"decrypt aead", "get credentials: decrypt api key: decryption failed: authentication error", "sync: credential decrypt failure"},
		{"connector create", "create connector: unsupported exchange foobar", "sync: connector creation failed"},
		{"mexc invalid key", `get balance: spot balance: HTTP 400: {"code":10072,"msg":"Api key info invalid"}`, "sync: credential rejected by exchange"},
		{"binance invalid key", "get balance: Invalid API-key, IP, or permissions for action", "sync: credential rejected by exchange"},
		{"oauth refresh missing token", "get balance: token refresh response missing access_token", "sync: OAuth refresh failed"},
		{"oauth invalid grant", "get balance: oauth: invalid_grant", "sync: OAuth refresh failed"},
		{"bitget suspended", `get balance: spot balance: HTTP 400: {"code":"40013","msg":"User status is abnormal"}`, "sync: exchange account suspended"},
		{"binance geo", "get balance: spot balance: binance API error: Service unavailable from a restricted location", "sync: exchange geo-block"},
		{"ibkr rate limit", "get balance: flex request failed: 1018 - Too many requests have been made from this token", "sync: rate limited"},
		{"ibkr flex pending", "get balance: flex request failed: 1001 - Statement could not be generated at this time", "sync: IBKR flex unavailable"},
		{"mt bridge protocol", `get balance: mt-bridge status 502: {"success":false,"error":{"code":"PROTOCOL_ERROR","message":"login failed"}}`, "sync: MT bridge error"},
		{"timeout", "get balance: context deadline exceeded", "sync: timeout"},
		{"dns failure", "get balance: dial tcp: lookup api.example.com: no such host", "sync: exchange unreachable"},
		{"unknown falls through", "get balance: weird new exchange error nobody has seen", "sync: snapshot build failed"},
		{"empty", "", "sync: snapshot build failed"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := classifySyncError(tc.in)
			if got != tc.want {
				t.Errorf("classifySyncError(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestClassifySyncError_StableAcrossWraps(t *testing.T) {
	// Same root cause wrapped through different layers must still classify
	// the same — that's the whole point of fingerprint stability.
	root := `Api key info invalid`
	wraps := []string{
		root,
		"spot balance: " + root,
		"get balance: spot balance: " + root,
	}
	want := "sync: credential rejected by exchange"
	for _, w := range wraps {
		if got := classifySyncError(w); got != want {
			t.Errorf("wrap %q classified as %q, want %q", w, got, want)
		}
	}
}
