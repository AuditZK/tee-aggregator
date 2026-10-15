package connector

import (
	"errors"
	"testing"

	"github.com/trackrecord/enclave/internal/errsanitize"
)

const unknownServerBridgeErr = `mt-bridge status 422: {"success":false,"error":{"code":"UNKNOWN_SERVER","message":"unknown server: cannot resolve MT5 server \"Broker-Timeout 3\": no discovery, hardcoded mapping, or hostname found"}}`

// G-H4: every one of these used to reach the user as a 400
// "invalid credentials" — for cTrader, right after a successful Spotware
// login, about a secret the user does not possess.
func TestClassifyConnectFailure(t *testing.T) {
	cases := []struct {
		err  string
		want ConnectFailure
	}{
		// Enclave misconfiguration (G-M7).
		{"cTrader requires CTRADER_CLIENT_ID and CTRADER_CLIENT_SECRET environment variables", ConnectFailureNotConfigured},
		{"missing cTrader client credentials (set CTRADER_CLIENT_ID/CTRADER_CLIENT_SECRET)", ConnectFailureNotConfigured},

		// A valid login with nothing tradable behind it.
		{"no cTrader accounts found", ConnectFailureNoAccount},

		// Broker outage / throttling — no verdict on the credentials at all.
		{"dial tcp 1.2.3.4:5036: connect: connection refused", ConnectFailureUpstream},
		{"websocket: bad handshake", ConnectFailureUpstream},
		{"cTrader request timeout for payloadType 2149", ConnectFailureUpstream},
		{"cTrader error BLOCKED_PAYLOAD_TYPE: You are being rate limited", ConnectFailureUpstream},
		{"cTrader token refresh rate-limited (429), retry later", ConnectFailureUpstream},
		{"token refresh failed (HTTP 503): upstream", ConnectFailureUpstream},

		// The broker did look, and said no.
		{"cTrader error ACCESS_DENIED: token is not valid", ConnectFailureCredentials},
		{"token refresh rejected: invalid_grant - already used", ConnectFailureCredentials},
		{"binance: invalid api key", ConnectFailureCredentials},

		// The bridge could not place the server name. The quoted name carries
		// "timeout", which must not drag it into the upstream class.
		{unknownServerBridgeErr, ConnectFailureUnknownServer},

		// Unknown stays conservative: refuse the connection.
		{"something nobody has characterised", ConnectFailureCredentials},
	}

	for _, tc := range cases {
		if got := ClassifyConnectFailure(errors.New(tc.err)); got != tc.want {
			t.Errorf("ClassifyConnectFailure(%q) = %d, want %d", tc.err, got, tc.want)
		}
	}
}

// The categories the connect path relies on must exist in errsanitize, and
// must not be shadowed by the "invalid credentials" entry that used to win
// every match.
func TestConnectFailureCategoriesReachTheClient(t *testing.T) {
	cases := []struct{ err, want string }{
		{"cTrader requires CTRADER_CLIENT_ID and CTRADER_CLIENT_SECRET environment variables", errsanitize.MsgExchangeNotConfigured},
		{"no cTrader accounts found", errsanitize.MsgNoTradingAccount},
		{"cTrader error ACCESS_DENIED: token is not valid", errsanitize.MsgBrokerReauthRequired},
		{"cTrader error BLOCKED_PAYLOAD_TYPE: You are being rate limited", errsanitize.MsgRateLimited},
		// Even with the historical prefix still attached, the specific
		// category must win — it is placed ahead of "invalid credentials".
		{"invalid credentials: cTrader error ACCESS_DENIED: token is not valid", errsanitize.MsgBrokerReauthRequired},
		{unknownServerBridgeErr, errsanitize.MsgUnknownServer},
		{ErrUnknownServer.Error() + ": " + unknownServerBridgeErr, errsanitize.MsgUnknownServer},
	}
	for _, tc := range cases {
		if got := errsanitize.Category(tc.err); got != tc.want {
			t.Errorf("Category(%q) = %q, want %q", tc.err, got, tc.want)
		}
	}
}
