package connector

import (
	"errors"
	"strings"

	"github.com/trackrecord/enclave/internal/errsanitize"
)

// ErrUnknownServer marks a connect refused because the MT bridge could not
// place the broker server name. Its text is what the frontend matches on to
// ask for the exact server name.
var ErrUnknownServer = errors.New(errsanitize.MsgUnknownServer)

// ConnectFailure is why a credential test failed at connection time.
//
// G-H4: the connection service used to wrap EVERY non-transient failure as
// "invalid credentials: …", and that string is the first entry of the
// errsanitize table, so it won. A Spotware outage, a missing
// CTRADER_CLIENT_ID, a cTID with no trading account and a 429 all reached the
// user as a 400 "invalid credentials" — advice that is not merely unhelpful
// for an OAuth broker, it is impossible to act on: the user has just
// completed a successful Spotware login and holds no credential to correct.
type ConnectFailure int

const (
	// ConnectFailureCredentials — the venue looked at the secret and refused
	// it. The only class where telling the user to check their credentials is
	// the right answer.
	ConnectFailureCredentials ConnectFailure = iota
	// ConnectFailureUpstream — we never got a verdict: the broker was
	// unreachable, timed out, or throttled us. Nothing about the credentials
	// is known.
	ConnectFailureUpstream
	// ConnectFailureNotConfigured — the enclave itself is missing the
	// app-level configuration the connector needs (cTrader's client id and
	// secret). An operator problem wearing a user's error message.
	ConnectFailureNotConfigured
	// ConnectFailureNoAccount — the login is valid and owns nothing tradable.
	ConnectFailureNoAccount
	// ConnectFailureUnknownServer — the MT bridge could not place the server
	// name, so no broker ever saw the credentials.
	ConnectFailureUnknownServer
)

// connectFailureMarkers maps a lowercase substring to its class. First match
// wins, so the specific markers come before the generic ones.
var connectFailureMarkers = []struct {
	substr string
	kind   ConnectFailure
}{
	// The bridge's code, first: its message quotes the server name as typed,
	// which can contain any marker below.
	{"unknown_server", ConnectFailureUnknownServer},

	// Enclave-side configuration, not the user's secret.
	{"ctrader_client_id", ConnectFailureNotConfigured},
	{"ctrader client credentials", ConnectFailureNotConfigured},

	// A valid login with nothing behind it.
	{"no ctrader accounts found", ConnectFailureNoAccount},
	{"no trading account", ConnectFailureNoAccount},

	// The broker did answer, and the answer was "no".
	{"access_denied", ConnectFailureCredentials},
	{"invalid_grant", ConnectFailureCredentials},
	{"token refresh rejected", ConnectFailureCredentials},
	{"access_token_invalid", ConnectFailureCredentials},
	{"cant_route_request", ConnectFailureCredentials},
	{"invalid credentials", ConnectFailureCredentials},
	{"invalid api key", ConnectFailureCredentials},
	{"invalid signature", ConnectFailureCredentials},
	{"unauthorized", ConnectFailureCredentials},
	{"permission denied", ConnectFailureCredentials},

	// We never reached a verdict.
	{"blocked_payload_type", ConnectFailureUpstream},
	{"rate limit", ConnectFailureUpstream},
	{"rate-limited", ConnectFailureUpstream},
	{"too many requests", ConnectFailureUpstream},
	{"http 429", ConnectFailureUpstream},
	{"(429)", ConnectFailureUpstream},
	{"no such host", ConnectFailureUpstream},
	{"connection refused", ConnectFailureUpstream},
	{"connection reset", ConnectFailureUpstream},
	{"i/o timeout", ConnectFailureUpstream},
	{"timeout", ConnectFailureUpstream},
	{"deadline exceeded", ConnectFailureUpstream},
	{"tls handshake", ConnectFailureUpstream},
	{"eof", ConnectFailureUpstream},
	{"websocket: bad handshake", ConnectFailureUpstream},
	{"dial tcp", ConnectFailureUpstream},
	{"bad gateway", ConnectFailureUpstream},
	{"service unavailable", ConnectFailureUpstream},
	{"http 502", ConnectFailureUpstream},
	{"http 503", ConnectFailureUpstream},
	{"http 504", ConnectFailureUpstream},
	{"maintenance", ConnectFailureUpstream},
}

// ClassifyConnectFailure buckets a TestConnection error. Unrecognised errors
// stay ConnectFailureCredentials: that is the historical behaviour, and it is
// the conservative direction — a connection is refused rather than saved on a
// failure nobody has characterised.
func ClassifyConnectFailure(err error) ConnectFailure {
	if err == nil {
		return ConnectFailureCredentials
	}
	msg := strings.ToLower(err.Error())
	for _, m := range connectFailureMarkers {
		if strings.Contains(msg, m.substr) {
			return m.kind
		}
	}
	return ConnectFailureCredentials
}
