package server

import (
	"bytes"
	"crypto/ecdh"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/trackrecord/enclave/internal/encryption"
	"go.uber.org/zap"
)

func monthsAgo(n int) time.Time {
	now := time.Now().UTC()
	return time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC).AddDate(0, -n, 0)
}

type rebuildFromCase struct {
	name        string
	rebuild     bool
	from        any
	wantStatus  int
	wantFrom    time.Time
	wantErrPart string
}

func rebuildFromCases() []rebuildFromCase {
	asked := monthsAgo(3)
	return []rebuildFromCase{
		{name: "month asked", rebuild: true, from: asked.Format("2006-01"), wantStatus: http.StatusOK, wantFrom: asked},
		{name: "no month", rebuild: true, from: nil, wantStatus: http.StatusOK},
		{name: "bad format", rebuild: true, from: asked.Format("2006-01-02"), wantStatus: http.StatusBadRequest, wantErrPart: "rebuild_from"},
		{name: "future month", rebuild: true, from: monthsAgo(-1).Format("2006-01"), wantStatus: http.StatusBadRequest, wantErrPart: "rebuild_from"},
		{name: "too far back", rebuild: true, from: monthsAgo(12*10 + 1).Format("2006-01"), wantStatus: http.StatusBadRequest, wantErrPart: "rebuild_from"},
		{name: "ignored without rebuild", rebuild: false, from: asked.Format("2006-01"), wantStatus: http.StatusOK},
		{name: "invalid ignored without rebuild", rebuild: false, from: "not-a-month", wantStatus: http.StatusOK},
	}
}

func (tc rebuildFromCase) check(t *testing.T, fake *fakeHandlerConnectionService, rr *httptest.ResponseRecorder) {
	t.Helper()
	if rr.Code != tc.wantStatus {
		t.Fatalf("status = %d, want %d (body %s)", rr.Code, tc.wantStatus, rr.Body.String())
	}
	if tc.wantStatus != http.StatusOK {
		if fake.lastReq != nil {
			t.Fatal("an invalid rebuild_from still reached the connection service")
		}
		if msg, _ := decodeJSONBody(t, rr.Body.Bytes())["error"].(string); !strings.Contains(msg, tc.wantErrPart) {
			t.Fatalf("error = %q, want it to name %q", msg, tc.wantErrPart)
		}
		return
	}
	if fake.lastReq == nil {
		t.Fatal("connection service not called")
	}
	if !fake.lastReq.RebuildFrom.Equal(tc.wantFrom) {
		t.Fatalf("RebuildFrom = %v, want %v", fake.lastReq.RebuildFrom, tc.wantFrom)
	}
	if fake.lastReq.RebuildHistory != tc.rebuild {
		t.Fatalf("RebuildHistory = %v, want %v", fake.lastReq.RebuildHistory, tc.rebuild)
	}
}

func TestCreateUserConnectionHandler_RebuildFrom(t *testing.T) {
	for _, tc := range rebuildFromCases() {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeHandlerConnectionService{}
			h := &Handler{logger: zap.NewNop(), connSvc: fake}
			body := map[string]any{
				"user_uid":        "user_abc1234567890",
				"exchange":        "binance",
				"label":           "main",
				"api_key":         "synthetic-key",
				"api_secret":      "synthetic-secret",
				"rebuild_history": tc.rebuild,
			}
			if tc.from != nil {
				body["rebuild_from"] = tc.from
			}
			tc.check(t, fake, callCreateUserConnectionHandler(t, h, body))
		})
	}
}

func TestConnectCredentialsHandler_RebuildFrom(t *testing.T) {
	ecies, err := encryption.NewECIES()
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range rebuildFromCases() {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeHandlerConnectionService{}
			h := &Handler{logger: zap.NewNop(), connSvc: fake, eciesSvc: ecies}
			body := map[string]any{
				"user_uid":        "user_abc1234567890",
				"exchange":        "binance",
				"label":           "main",
				"encrypted":       sealCredentials(t, ecies, `{"api_key":"synthetic-key","api_secret":"synthetic-secret"}`),
				"rebuild_history": tc.rebuild,
			}
			if tc.from != nil {
				body["rebuild_from"] = tc.from
			}
			raw, err := json.Marshal(body)
			if err != nil {
				t.Fatal(err)
			}
			rr := httptest.NewRecorder()
			h.ConnectCredentials(rr, httptest.NewRequest(http.MethodPost, "/api/v1/credentials/connect", bytes.NewReader(raw)))
			tc.check(t, fake, rr)
		})
	}
}

// sealCredentials encrypts a credential payload the way the browser does.
func sealCredentials(t *testing.T, ecies *encryption.ECIESService, payload string) map[string]string {
	t.Helper()
	pub, err := ecies.PublicKeyPEM()
	if err != nil {
		t.Fatal(err)
	}
	ephRaw, iv, sealed, err := encryption.EncryptToPubkey([]byte(pub), []byte(payload))
	if err != nil {
		t.Fatal(err)
	}
	eph, err := ecdh.P256().NewPublicKey(ephRaw)
	if err != nil {
		t.Fatal(err)
	}
	spki, err := x509.MarshalPKIXPublicKey(eph)
	if err != nil {
		t.Fatal(err)
	}
	tagAt := len(sealed) - 16
	return map[string]string{
		"ephemeralPublicKey": string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: spki})),
		"iv":                 hex.EncodeToString(iv),
		"ciphertext":         hex.EncodeToString(sealed[:tagAt]),
		"tag":                hex.EncodeToString(sealed[tagAt:]),
	}
}
