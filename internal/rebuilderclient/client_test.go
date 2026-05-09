package rebuilderclient

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.uber.org/zap"
)

func TestClient_NotConfigured(t *testing.T) {
	c := New("", "", zap.NewNop())
	if c.Configured() {
		t.Fatal("expected Configured()=false when baseURL+token empty")
	}
	_, err := c.QueueRebuild(context.Background(), QueueRebuildRequest{})
	if err == nil {
		t.Fatal("expected error when not configured")
	}
}

func TestClient_QueueRebuild_SendsAuthHeaderAndPayload(t *testing.T) {
	var gotToken string
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotToken = r.Header.Get("X-Internal-Token")
		body, _ := io.ReadAll(r.Body)
		gotBody = body
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jobId":"j-123","status":"queued"}`))
	}))
	defer srv.Close()

	c := New(srv.URL, "secret-token", zap.NewNop())
	resp, err := c.QueueRebuild(context.Background(), QueueRebuildRequest{
		UserUID:     "u1",
		Exchange:    "hyperliquid",
		Label:       "main",
		Credentials: Credentials{WalletAddress: "0xabc"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.JobID != "j-123" || resp.Status != "queued" {
		t.Fatalf("unexpected resp: %+v", resp)
	}
	if gotToken != "secret-token" {
		t.Errorf("X-Internal-Token: got %q want %q", gotToken, "secret-token")
	}
	var sent QueueRebuildRequest
	if err := json.Unmarshal(gotBody, &sent); err != nil {
		t.Fatalf("body not valid JSON: %v", err)
	}
	if sent.Credentials.WalletAddress != "0xabc" {
		t.Errorf("wallet not propagated: %+v", sent)
	}
	// Request body should NOT contain unrelated empty fields beyond what we set.
	if !strings.Contains(string(gotBody), `"hyperliquid"`) {
		t.Errorf("body missing exchange: %s", gotBody)
	}
}

func TestClient_QueueRebuild_PropagatesHTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":"unauthorized"}`))
	}))
	defer srv.Close()

	c := New(srv.URL, "wrong-token", zap.NewNop())
	_, err := c.QueueRebuild(context.Background(), QueueRebuildRequest{UserUID: "u1", Exchange: "hyperliquid"})
	if err == nil {
		t.Fatal("expected error on 401")
	}
	if !strings.Contains(err.Error(), "401") {
		t.Errorf("error should mention HTTP 401: %v", err)
	}
}

// LOG-CREDS-001: errors returned from this client must never embed plaintext
// credentials, even when the upstream rebuilder echoes them in its response
// body. Regression guard against future "include the body in the error
// message for debugging" temptations.
func TestClient_QueueRebuild_ErrorDoesNotLeakCredentials(t *testing.T) {
	const sentinelKey = "REAL-API-KEY-00000000000000000"
	const sentinelSecret = "REAL-API-SECRET-1111111111111"
	const sentinelPass = "REAL-PASSPHRASE-22222222"
	const sentinelWallet = "0xREALWALLET3333333333333333333333"

	// Hostile rebuilder that echoes the entire request body back in the error
	// payload. We want our client to drop that body, not propagate it.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		w.WriteHeader(http.StatusInternalServerError)
		// Embed the raw body in the response — what the client sees if it
		// (incorrectly) decides to include the response body in its error.
		_, _ = w.Write([]byte(`{"error":"internal","echo":` + string(mustJSON(string(body))) + `}`))
	}))
	defer srv.Close()

	c := New(srv.URL, "secret-token", zap.NewNop())
	_, err := c.QueueRebuild(context.Background(), QueueRebuildRequest{
		UserUID:  "u1",
		Exchange: "hyperliquid",
		Credentials: Credentials{
			WalletAddress: sentinelWallet,
			APIKey:        sentinelKey,
			APISecret:     sentinelSecret,
			Passphrase:    sentinelPass,
		},
	})
	if err == nil {
		t.Fatal("expected error on 500")
	}
	msg := err.Error()
	for _, leak := range []string{sentinelKey, sentinelSecret, sentinelPass, sentinelWallet} {
		if strings.Contains(msg, leak) {
			t.Errorf("error message leaks credential %q: %s", leak, msg)
		}
	}
}

func mustJSON(v string) []byte {
	out, _ := jsonMarshal(v)
	return out
}

// Local alias to keep the test file's import block tidy. encoding/json is
// already imported above; redirect through this helper so the leak-test
// assertion logic reads cleanly.
var jsonMarshal = func(v any) ([]byte, error) {
	return json.Marshal(v)
}
