package service

import (
	"strings"

	"github.com/trackrecord/enclave/internal/errsanitize"
)

// classifySyncError maps a wrapped sync-failure string to a stable log
// message. Each return value becomes a distinct errtrack fingerprint so
// the observability dashboard separates "credential rejected by
// exchange" from "OAuth refresh failed" instead of collapsing every
// snapshot failure under one generic group.
//
// It reads the RAW error text, which may carry a credential a vendor put
// in a URL query string (SEC-11) — safe only because every return value is
// a constant. Never surface its input; use egressSyncError for that.
func classifySyncError(errStr string) string {
	s := errStr
	switch {
	case strings.Contains(s, "decryption failed"),
		strings.Contains(s, "decrypt api key"),
		strings.Contains(s, "decrypt api secret"),
		strings.Contains(s, "decrypt passphrase"):
		return "sync: credential decrypt failure"
	case strings.HasPrefix(s, "create connector"),
		strings.Contains(s, "unsupported exchange"):
		return "sync: connector creation failed"
	case strings.Contains(s, "Api key info invalid"),
		strings.Contains(s, "API-key format invalid"),
		strings.Contains(s, "Signature for this request is not valid"),
		strings.Contains(s, "api key invalid"),
		strings.Contains(s, "Invalid API-key"),
		strings.Contains(s, "Invalid api key"):
		return "sync: credential rejected by exchange"
	case strings.Contains(s, "missing access_token"),
		strings.Contains(s, "invalid_grant"),
		strings.Contains(s, "refresh_token expired"),
		strings.Contains(s, "token refresh rejected"),
		strings.Contains(s, "ACCESS_DENIED"),
		strings.Contains(s, "OAuth token"),
		strings.Contains(s, "oauth token"):
		return "sync: OAuth refresh failed"
	case strings.Contains(s, "User status is abnormal"),
		strings.Contains(s, "account is suspended"),
		strings.Contains(s, "account suspended"),
		strings.Contains(s, "account disabled"):
		return "sync: exchange account suspended"
	case strings.Contains(s, "restricted location"),
		strings.Contains(s, "geo-block"),
		strings.Contains(s, "not available in your region"):
		return "sync: exchange geo-block"
	case strings.Contains(s, "Too many requests"),
		strings.Contains(s, "rate limit"),
		strings.Contains(s, "rate-limit"),
		strings.Contains(s, "HTTP 429"):
		return "sync: rate limited"
	case strings.Contains(s, "Statement could not be generated"),
		strings.Contains(s, "flex request failed"):
		return "sync: IBKR flex unavailable"
	case strings.Contains(s, "mt-bridge"),
		strings.Contains(s, "PROTOCOL_ERROR"),
		strings.Contains(s, "PROTOCOLERROR"):
		return "sync: MT bridge error"
	case strings.Contains(s, "context deadline exceeded"),
		strings.Contains(s, "context canceled"):
		return "sync: timeout"
	case strings.Contains(s, "no such host"),
		strings.Contains(s, "connection refused"),
		strings.Contains(s, "TLS handshake"):
		return "sync: exchange unreachable"
	}
	return "sync: snapshot build failed"
}

// genericSyncFailure is what a client sees when the failure matches no
// actionable category — never the raw text.
const genericSyncFailure = "sync failed"

// egressSyncError renders err for SyncResult.Error. That field is persisted
// in sync_statuses and serialised to REST and gRPC clients without passing
// through either output sanitizer (SEC-12), so raw error text — a vendor URL
// carrying the credential in its query string (SEC-11), pgx detail, an
// internal host:port — must never reach it. The full error stays in the log
// at the call site.
func egressSyncError(op string, err error) string {
	msg := errsanitize.Category(err.Error())
	if msg == "" {
		msg = genericSyncFailure
	}
	if op == "" {
		return msg
	}
	return op + ": " + msg
}

// syncStatusMarker returns the machine-readable prefix for a SANITIZED sync
// error, or "" when the failure belongs to no actionable class.
//
// It keys off the sanitized text rather than the raw one on purpose: the
// prefix and the message the user eventually reads then come from a single
// classification (errsanitize.Category), and cannot drift apart. The raw text
// still drives classifySyncError, which owns the LOG fingerprint.
//
//   - reauth_required — the broker no longer honours the stored authorization
//     (ACCESS_DENIED, invalid_grant, a rejected refresh, a 400/401 at the
//     token endpoint, a rotated token we failed to persist). No credential the
//     user can retype fixes this; only re-running the OAuth flow does.
//   - rate_limited — the broker is throttling us (BLOCKED_PAYLOAD_TYPE, 429).
//     Nobody needs to do anything; the next pass succeeds.
func syncStatusMarker(sanitized string) string {
	switch {
	case sanitized == "":
		return ""
	case strings.Contains(sanitized, errsanitize.MsgBrokerReauthRequired):
		return "reauth_required"
	case strings.Contains(sanitized, errsanitize.MsgRateLimited):
		return "rate_limited"
	}
	return ""
}
