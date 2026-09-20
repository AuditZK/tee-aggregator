// Package errsanitize maps an internal error message to a fixed, user-facing
// category message. It is shared by every egress that renders an error for a
// client so the categories cannot drift apart: the gRPC sanitizer, and
// SyncResult.Error, which is persisted in sync_statuses and serialised to
// REST/gRPC without passing through any output sanitizer (SEC-12).
package errsanitize

import "strings"

// Category messages shared by several match substrings — kept as constants so
// the same user-facing text isn't duplicated across rows.
const (
	msgExchangeUnreachable = "could not reach the exchange endpoint"
	msgExchangeTimeout     = "the exchange request timed out"
	msgIPNotWhitelisted    = "server IP not whitelisted on the exchange"
	msgServiceUnavailable  = "service temporarily unavailable"

	// MsgBrokerReauthRequired is the OAuth-death category: the broker no
	// longer accepts the stored authorization and no credential the user can
	// retype will fix it — only re-running the OAuth flow will. Exported
	// because the sync layer keys the machine-readable "reauth_required:"
	// marker off it (E-H5): before, an expired cTrader token and a dropped
	// TCP connection both persisted as "get balance: sync failed", so
	// nothing downstream could tell "reconnect your account" from "we will
	// retry tonight".
	MsgBrokerReauthRequired = "broker authorization expired, please reconnect the account"

	// MsgRateLimited is the broker-is-throttling-us category. Not a user
	// problem and not a credential problem: the next pass succeeds.
	MsgRateLimited = "rate limited by the broker, retrying later"

	// MsgExchangeNotConfigured covers a connector the enclave cannot even
	// attempt because its app-level configuration is missing. Telling the
	// user their credentials are invalid here sends them to regenerate keys
	// that were never the problem (G-H4/G-M7).
	MsgExchangeNotConfigured = "this broker is not configured on the enclave"

	// MsgNoTradingAccount is a successful login that owns no tradable
	// account — again, nothing the user can fix by re-entering credentials.
	MsgNoTradingAccount = "no trading account found for this broker login"
)

type category struct {
	substr  string
	message string
}

// categories maps a substring that marks an error as safe to surface to the
// end-user (a category of failure they can act on — wrong credentials, IP not
// whitelisted, etc.) to a fixed, infrastructure-free message. SEC-07:
// returning the RAW error text on a match leaked internal detail (e.g. "dial
// tcp internal-db:5432: connect: connection refused" matches "connection
// refused"). First match wins, so more specific substrings come first.
var categories = []category{
	// Rate limiting first: "cTrader token refresh rate-limited (429)" is a
	// throttle, not a dead authorization, and the reauth patterns below would
	// otherwise claim it.
	{"blocked_payload_type", MsgRateLimited},
	// IBKR Flex pacing, in IBKR's words (1001, 1019) and in ours (the
	// token gate). Unlisted, the loser of a shared-token race reached the
	// dashboard as a plain "sync failed", which reads as broken.
	{"shared flex token cooling down", MsgRateLimited},
	{"could not be generated at this time", MsgRateLimited},
	{"statement generation in progress", MsgRateLimited},
	{"rate limited", MsgRateLimited},
	{"rate-limited", MsgRateLimited},
	{"too many requests", MsgRateLimited},
	{"http 429", MsgRateLimited},
	{"(429)", MsgRateLimited},

	// OAuth death. These MUST precede "invalid credentials": that entry is
	// first-match-wins and the connect path prefixes "invalid credentials: "
	// onto everything, so it used to swallow every one of these.
	{"access_denied", MsgBrokerReauthRequired},
	{"invalid_grant", MsgBrokerReauthRequired},
	{"token refresh rejected", MsgBrokerReauthRequired},
	{"access_token_invalid", MsgBrokerReauthRequired},
	{"refresh_token expired", MsgBrokerReauthRequired},
	{"missing access_token", MsgBrokerReauthRequired},
	{"missing refresh token", MsgBrokerReauthRequired},
	{"token refresh response missing", MsgBrokerReauthRequired},
	{"token refresh failed (http 400", MsgBrokerReauthRequired},
	{"token refresh failed (http 401", MsgBrokerReauthRequired},
	{"must be re-authorized", MsgBrokerReauthRequired},
	{"oauth token", MsgBrokerReauthRequired},

	// Configuration and account-shape refusals, also ahead of "invalid
	// credentials" for the same reason (G-H4).
	{"ctrader_client_id", MsgExchangeNotConfigured},
	{"ctrader client credentials", MsgExchangeNotConfigured},
	{"no ctrader accounts found", MsgNoTradingAccount},
	{"no trading account", MsgNoTradingAccount},

	{"invalid credentials", "invalid credentials"},
	{"invalid api key", "invalid API key"},
	{"invalid signature", "invalid API signature"},
	{"auth_failed", "authentication failed"},
	{"unauthorized", "unauthorized"},
	{"insufficient permission", "insufficient API key permissions"},
	{"permission denied", "permission denied"},
	{"ip not whitelist", msgIPNotWhitelisted},
	{"ip restricted", msgIPNotWhitelisted},
	{"whitelist", msgIPNotWhitelisted},
	{"no such host", msgExchangeUnreachable},
	{"connection refused", msgExchangeUnreachable},
	{"deadline exceeded", msgExchangeTimeout},
	{"timeout", msgExchangeTimeout},
	{"validation failed", "validation failed"},
	{"protocol_error", "protocol error"},
	{"failed to create connection", "failed to create connection"},
	{"failed to create user", "failed to create user"},
	{"already exists", "resource already exists"},
	{"not found", "resource not found"},
	{"database not configured", msgServiceUnavailable},
	{"sync service not available", msgServiceUnavailable},
	{"report service not available", msgServiceUnavailable},
	{"service not available", msgServiceUnavailable},
}

// Category returns the canonical message for the failure class msg belongs
// to, or "" when msg matches none — leaving the caller to apply its own
// generic fallback rather than echoing any part of msg.
func Category(msg string) string {
	lower := strings.ToLower(msg)
	for _, c := range categories {
		if strings.Contains(lower, c.substr) {
			return c.message
		}
	}
	return ""
}
