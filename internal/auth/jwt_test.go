package auth_test

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/trackrecord/enclave/internal/auth"
)

var testSecret = []byte("test-secret-32-bytes-exactly!!!!")

func makeToken(sub, aud string, exp int64) string {
	header := base64.RawURLEncoding.EncodeToString(mustJSON(map[string]string{"alg": "HS256", "typ": "JWT"}))
	payload := base64.RawURLEncoding.EncodeToString(mustJSON(map[string]interface{}{
		"sub": sub,
		"aud": aud,
		"iat": time.Now().Unix(),
		"exp": exp,
	}))
	mac := hmac.New(sha256.New, testSecret)
	mac.Write([]byte(header + "." + payload))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return header + "." + payload + "." + sig
}

func mustJSON(v interface{}) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

func TestVerifyHS256_Valid(t *testing.T) {
	token := makeToken("user_abc123", auth.RequiredAudience, time.Now().Add(60*time.Second).Unix())
	claims, err := auth.VerifyHS256(token, testSecret)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if claims.Sub != "user_abc123" {
		t.Errorf("expected sub=user_abc123, got %s", claims.Sub)
	}
	if claims.Aud != auth.RequiredAudience {
		t.Errorf("expected aud=%s, got %s", auth.RequiredAudience, claims.Aud)
	}
}

func TestVerifyHS256_Expired(t *testing.T) {
	token := makeToken("user_abc123", auth.RequiredAudience, time.Now().Add(-1*time.Second).Unix())
	_, err := auth.VerifyHS256(token, testSecret)
	if err != auth.ErrTokenExpired {
		t.Errorf("expected ErrTokenExpired, got %v", err)
	}
}

func TestVerifyHS256_WrongAudience(t *testing.T) {
	token := makeToken("user_abc123", "wrong-audience", time.Now().Add(60*time.Second).Unix())
	_, err := auth.VerifyHS256(token, testSecret)
	if err != auth.ErrInvalidAudience {
		t.Errorf("expected ErrInvalidAudience, got %v", err)
	}
}

func TestVerifyHS256_WrongSecret(t *testing.T) {
	token := makeToken("user_abc123", auth.RequiredAudience, time.Now().Add(60*time.Second).Unix())
	_, err := auth.VerifyHS256(token, []byte("wrong-secret"))
	if err != auth.ErrInvalidSignature {
		t.Errorf("expected ErrInvalidSignature, got %v", err)
	}
}

func TestVerifyHS256_Malformed_TwoParts(t *testing.T) {
	_, err := auth.VerifyHS256("only.two", testSecret)
	if err != auth.ErrMalformedToken {
		t.Errorf("expected ErrMalformedToken for two parts, got %v", err)
	}
}

func TestVerifyHS256_Malformed_FourParts(t *testing.T) {
	_, err := auth.VerifyHS256("a.b.c.d", testSecret)
	if err != auth.ErrMalformedToken {
		t.Errorf("expected ErrMalformedToken for four parts, got %v", err)
	}
}

func TestVerifyHS256_TamperedPayload(t *testing.T) {
	token := makeToken("user_abc123", auth.RequiredAudience, time.Now().Add(60*time.Second).Unix())
	parts := strings.Split(token, ".")
	// Tamper with payload to claim a different sub
	tampered := base64.RawURLEncoding.EncodeToString(mustJSON(map[string]interface{}{
		"sub": "user_evil",
		"aud": auth.RequiredAudience,
		"iat": time.Now().Unix(),
		"exp": time.Now().Add(60 * time.Second).Unix(),
	}))
	tamperedToken := parts[0] + "." + tampered + "." + parts[2]
	_, err := auth.VerifyHS256(tamperedToken, testSecret)
	if err != auth.ErrInvalidSignature {
		t.Errorf("expected ErrInvalidSignature for tampered payload, got %v", err)
	}
}

func TestVerifyHS256_MissingSub(t *testing.T) {
	header := base64.RawURLEncoding.EncodeToString(mustJSON(map[string]string{"alg": "HS256", "typ": "JWT"}))
	payload := base64.RawURLEncoding.EncodeToString(mustJSON(map[string]interface{}{
		"sub": "",
		"aud": auth.RequiredAudience,
		"iat": time.Now().Unix(),
		"exp": time.Now().Add(60 * time.Second).Unix(),
	}))
	mac := hmac.New(sha256.New, testSecret)
	mac.Write([]byte(header + "." + payload))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	token := header + "." + payload + "." + sig

	_, err := auth.VerifyHS256(token, testSecret)
	if err != auth.ErrMissingSub {
		t.Errorf("expected ErrMissingSub, got %v", err)
	}
}

func TestContextRoundtrip(t *testing.T) {
	ctx := context.Background()
	ctx = auth.WithUserUID(ctx, "user_test123")
	uid, ok := auth.UserUIDFromContext(ctx)
	if !ok {
		t.Fatal("expected ok=true from context")
	}
	if uid != "user_test123" {
		t.Errorf("expected uid=user_test123, got %s", uid)
	}
}

func TestContextEmpty(t *testing.T) {
	ctx := context.Background()
	_, ok := auth.UserUIDFromContext(ctx)
	if ok {
		t.Error("expected ok=false for empty context")
	}
}
