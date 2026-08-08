package connector

import (
	"errors"
	"strings"
)

// ErrIPRestricted marks a rejection the venue blames on the caller's source IP:
// the key is valid, it is simply not whitelisted for the address we reached the
// venue from (the enclave egress, or the proxy for geo-restricted exchanges).
//
// Worth separating from a credential failure because the remediation is the
// opposite one. Told "invalid credentials", a key holder regenerates the key —
// which cannot help, and produces a fresh key that fails identically. Two
// users spent weeks in that loop (support, July 2026) on Binance sub-accounts
// where enabling Futures forces an IP restriction.
var ErrIPRestricted = errors.New("ip not whitelisted")

// ipRestrictionMarkers are the venue signatures for a source-IP rejection,
// matched against the lowercased error text — which carries the vendor body,
// since the connectors wrap non-2xx responses as "HTTP %d: <body>".
//
// Text, not codes: every venue below reuses ONE code for "bad key" OR "bad IP"
// OR "missing permission", so the numeric code cannot discriminate and the
// message is all we have. Matching bare code digits would also collide with
// timestamps and balances elsewhere in the body.
//
//	binance  -2015   "Invalid API-key, IP, or permissions for action."
//	okx      50110   "Invalid IP" (see the rebuilder egress note in service/sync.go)
//	bybit    10010   "request ip mismatch" / "unmatched ip"
//	kucoin   400006  "invalid ip"
//	mexc     700006  "IP not in whitelist"
//	bitget   40018   "invalid ip"
var ipRestrictionMarkers = []string{
	"ip, or permissions", // binance -2015, the whole-phrase anchor
	"invalid ip",
	"ip mismatch",
	"ip not in whitelist",
	"ip is not in",
	"not in the whitelist",
	"unmatched ip",
	"ip address is not",
}

// IsIPRestriction reports whether err is a source-IP rejection rather than a
// bad-credential one. Callers use it to pick the remediation they show the
// user; it never changes whether the error is fatal.
func IsIPRestriction(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrIPRestricted) {
		return true
	}
	msg := strings.ToLower(err.Error())
	for _, marker := range ipRestrictionMarkers {
		if strings.Contains(msg, marker) {
			return true
		}
	}
	return false
}
