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

// Sentinel errors returned by VerifyHS256 / VerifyHS256WithOptions.
var (
	ErrMalformedToken   = errors.New("malformed JWT")
	ErrInvalidSignature = errors.New("invalid JWT signature")
	ErrTokenExpired     = errors.New("JWT expired")
	ErrTokenNotYetValid = errors.New("JWT not yet valid (nbf)")
	ErrInvalidAudience  = errors.New("JWT audience mismatch")
	ErrInvalidIssuer    = errors.New("JWT issuer mismatch")
	ErrInvalidAlgorithm = errors.New("JWT alg header must be HS256")
	ErrInvalidType      = errors.New("JWT typ header must be JWT")
	ErrMissingSub       = errors.New("JWT missing sub claim")
)

// RequiredAudience is the aud claim the enclave accepts.
const RequiredAudience = "go-enclave"

// defaultClockSkew is the tolerance applied to exp / nbf checks to absorb
// modest clock drift between the issuer and this enclave (SEC-006).
const defaultClockSkew = 30 * time.Second

// Claims holds the subset of JWT claims we care about.
type Claims struct {
	Sub string `json:"sub"`
	Aud string `json:"aud"`
	Iss string `json:"iss"`
	Exp int64  `json:"exp"`
	Nbf int64  `json:"nbf"`
	Iat int64  `json:"iat"`
}

// header is the minimal JWS header we inspect.
type header struct {
	Alg string `json:"alg"`
	Typ string `json:"typ"`
}

// VerifyOptions tunes optional JWT checks. Zero-value = same behaviour as
// the legacy VerifyHS256 (exp + aud + HS256 algorithm).
type VerifyOptions struct {
	// ExpectedIssuer, when non-empty, requires the iss claim to match exactly.
	ExpectedIssuer string
	// ClockSkew is the tolerance window applied to both exp and nbf.
	// Zero uses defaultClockSkew (30 s). Pass a negative value to disable.
	ClockSkew time.Duration
}

type contextKey string

const ctxKeyUserUID contextKey = "auth_user_uid"

// VerifyHS256 parses and validates a compact JWS token signed with HS256.
// It verifies the alg/typ header, signature, exp (with 30 s skew), aud,
// and sub. For iss enforcement, use VerifyHS256WithOptions.
func VerifyHS256(tokenStr string, secret []byte) (*Claims, error) {
	return VerifyHS256WithOptions(tokenStr, secret, VerifyOptions{})
}

// VerifyHS256WithOptions is like VerifyHS256 but exposes the additional
// checks enabled by VerifyOptions (issuer, clock-skew tuning). SEC-006.
func VerifyHS256WithOptions(tokenStr string, secret []byte, opts VerifyOptions) (*Claims, error) {
	parts := strings.Split(tokenStr, ".")
	if len(parts) != 3 {
		return nil, ErrMalformedToken
	}

	// Decode and validate the header before touching the signature or claims.
	// SEC-006: rejecting unexpected alg/typ up front prevents surprises if an
	// additional verifier path is later added to this package.
	headerBytes, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return nil, ErrMalformedToken
	}
	var hdr header
	if err := json.Unmarshal(headerBytes, &hdr); err != nil {
		return nil, ErrMalformedToken
	}
	if hdr.Alg != "HS256" {
		return nil, ErrInvalidAlgorithm
	}
	if hdr.Typ != "" && hdr.Typ != "JWT" {
		return nil, ErrInvalidType
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

	skew := opts.ClockSkew
	if skew == 0 {
		skew = defaultClockSkew
	}
	if skew < 0 {
		skew = 0
	}
	now := time.Now()
	if claims.Exp > 0 && now.Add(-skew).Unix() > claims.Exp {
		return nil, ErrTokenExpired
	}
	if claims.Nbf > 0 && now.Add(skew).Unix() < claims.Nbf {
		return nil, ErrTokenNotYetValid
	}
	if claims.Aud != RequiredAudience {
		return nil, ErrInvalidAudience
	}
	if opts.ExpectedIssuer != "" && claims.Iss != opts.ExpectedIssuer {
		return nil, ErrInvalidIssuer
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
