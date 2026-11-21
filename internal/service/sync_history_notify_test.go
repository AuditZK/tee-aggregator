package service

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.uber.org/zap"
)

// C6: the post-rebuild ping targets a per-user sync trigger that nginx
// exposes. Without a shared secret, anyone who can reach that host can fire
// it. The header only goes out when a token is configured, so an enclave
// upgraded before its receiver keeps working.
func TestNotifyHistoryRebuilt_SendsInternalTokenWhenConfigured(t *testing.T) {
	type call struct {
		path  string
		token string
	}
	calls := make(chan call, 4)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls <- call{path: r.URL.Path, token: r.Header.Get("X-Internal-Token")}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	t.Run("with a token", func(t *testing.T) {
		s := &SyncService{logger: zap.NewNop()}
		s.SetHistoryNotify(server.URL+"/api/v1/sync/user", "shared-secret-value")
		s.notifyHistoryRebuilt(context.Background(), "user-uid-1")

		got := <-calls
		if got.path != "/api/v1/sync/user/user-uid-1" {
			t.Fatalf("path = %q", got.path)
		}
		if got.token != "shared-secret-value" {
			t.Fatalf("X-Internal-Token = %q, want the configured secret", got.token)
		}
	})

	t.Run("without a token the ping still goes out bare", func(t *testing.T) {
		s := &SyncService{logger: zap.NewNop()}
		s.SetHistoryNotify(server.URL+"/api/v1/sync/user", "")
		s.notifyHistoryRebuilt(context.Background(), "user-uid-2")

		got := <-calls
		if got.token != "" {
			t.Fatalf("X-Internal-Token = %q, want no header", got.token)
		}
	})

	t.Run("no URL, no request", func(t *testing.T) {
		s := &SyncService{logger: zap.NewNop()}
		s.SetHistoryNotify("", "shared-secret-value")
		s.notifyHistoryRebuilt(context.Background(), "user-uid-3")

		select {
		case got := <-calls:
			t.Fatalf("unexpected ping: %+v", got)
		default:
		}
	})
}
