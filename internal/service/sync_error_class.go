package service

import "strings"

// classifySyncError maps a wrapped sync-failure string to a stable log
// message. Each return value becomes a distinct errtrack fingerprint so
// the observability dashboard separates "credential rejected by
// exchange" from "OAuth refresh failed" instead of collapsing every
// snapshot failure under one generic group.
//
// The classifier reads only the error text — values fed to it are
// already the result of `fmt.Sprintf("...: %v", err)` and have flowed
// through the connector/decrypt layers, none of which interpolate
// secrets (LOG-001 audited paths). Returned messages are constants.
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
		strings.Contains(s, "refresh_token expired"):
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
