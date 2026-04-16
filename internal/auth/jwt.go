// Package auth provides JWT verification for gRPC requests.
//
// Only HMAC-SHA256 (HS256) is supported to keep the verification
// entirely in the standard library — no external dependencies inside
// the TEE that could introduce supply-chain risk.
package auth

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

// Sentinel errors returned by VerifyHS256.
var (
	ErrMalformedToken   = errors.New("malformed JWT")
	ErrInvalidSignature = errors.New("invalid JWT signature")
	ErrTokenExpired     = errors.New("JWT expired")
	ErrInvalidAudience  = errors.New("JWT audience mismatch")
	ErrMissingSub       = errors.New("JWT missing sub claim")
)

// RequiredAudience is the aud claim the enclave accepts.
const RequiredAudience = "go-enclave"

// Claims holds the subset of JWT claims we care about.
type Claims struct {
	Sub string `json:"sub"`
	Aud string `json:"aud"`
	Exp int64  `json:"exp"`
	Iat int64  `json:"iat"`
}

type contextKey string

const ctxKeyUserUID contextKey = "auth_user_uid"

// VerifyHS256 parses and validates a compact JWS token signed with HS256.
// It verifies the signature, expiry, and audience claim.
// Uses constant-time comparison to prevent timing attacks on the signature.
func VerifyHS256(tokenStr string, secret []byte) (*Claims, error) {
	parts := strings.Split(tokenStr, ".")
	if len(parts) != 3 {
		return nil, ErrMalformedToken
	}

	// Verify HMAC-SHA256 signature over "header.payload"
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(parts[0] + "." + parts[1]))
	expected := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(expected), []byte(parts[2])) {
		return nil, ErrInvalidSignature
	}

	// Decode payload (no padding, URL-safe base64)
	payloadBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, ErrMalformedToken
	}

	var claims Claims
	if err := json.Unmarshal(payloadBytes, &claims); err != nil {
		return nil, ErrMalformedToken
	}

	if time.Now().Unix() > claims.Exp {
		return nil, ErrTokenExpired
	}
	if claims.Aud != RequiredAudience {
		return nil, ErrInvalidAudience
	}
	if claims.Sub == "" {
		return nil, ErrMissingSub
	}

	return &claims, nil
}

// WithUserUID stores the authenticated user UID in ctx.
func WithUserUID(ctx context.Context, uid string) context.Context {
	return context.WithValue(ctx, ctxKeyUserUID, uid)
}

// UserUIDFromContext retrieves the authenticated user UID from ctx.
// Returns ("", false) if not present.
func UserUIDFromContext(ctx context.Context) (string, bool) {
	uid, ok := ctx.Value(ctxKeyUserUID).(string)
	return uid, ok && uid != ""
}
