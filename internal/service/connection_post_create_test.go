package service

import (
	"context"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/trackrecord/enclave/internal/repository"
)

type hookRecorder struct {
	order chan string
}

func newHookRecorder() *hookRecorder { return &hookRecorder{order: make(chan string, 4)} }

func (h *hookRecorder) hook(name string) func(context.Context, string, string, string) {
	return func(_ context.Context, userUID, exchange, label string) {
		h.order <- name + ":" + userUID + "/" + exchange + "/" + label
	}
}

func (h *hookRecorder) next(t *testing.T, want string) {
	t.Helper()
	select {
	case got := <-h.order:
		if got != want {
			t.Fatalf("hook fired %q, want %q", got, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("hook %q never fired", want)
	}
}

func (h *hookRecorder) expectNothingMore(t *testing.T) {
	t.Helper()
	select {
	case got := <-h.order:
		t.Fatalf("unexpected extra hook: %q", got)
	case <-time.After(150 * time.Millisecond):
	}
}

// G-H7 / C7: the first live snapshot must run for every new connection, not
// only for callers that asked for a history rebuild. The cTrader OAuth
// callback sends no rebuild_history field, the gateway defaults it to false,
// and the whole hook was skipped — so a new cTrader account had no snapshot,
// no sync_statuses row and no status in the admin until 00:00 UTC.
func TestDispatchPostCreateHooks_SyncRunsWithoutRebuildOptIn(t *testing.T) {
	rec := newHookRecorder()
	svc := &ConnectionService{
		postCreateSyncHook:    rec.hook("sync"),
		postCreateRebuildHook: rec.hook("rebuild"),
	}

	svc.dispatchPostCreateHooks("user-1", "ctrader", "cTrader Account", false)

	rec.next(t, "sync:user-1/ctrader/cTrader Account")
	rec.expectNothingMore(t)
}

// SEC-ZK-001 / SEC-08: the rebuild — which ships decrypted credentials to a
// service outside the enclave for non-IBKR exchanges — stays behind the
// explicit opt-in.
func TestDispatchPostCreateHooks_RebuildStaysOptIn(t *testing.T) {
	rec := newHookRecorder()
	svc := &ConnectionService{
		postCreateSyncHook:    rec.hook("sync"),
		postCreateRebuildHook: rec.hook("rebuild"),
	}

	svc.dispatchPostCreateHooks("user-1", "ctrader", "cTrader Account", true)

	// Order matters: the snapshot the sync writes is the equity anchor the
	// rebuild dispatch reads (EndEquityOverride, 2026-08-04).
	rec.next(t, "sync:user-1/ctrader/cTrader Account")
	rec.next(t, "rebuild:user-1/ctrader/cTrader Account")
	rec.expectNothingMore(t)
}

// A deployment with no rebuild hook wired must still take the first snapshot.
func TestDispatchPostCreateHooks_SyncOnlyDeployment(t *testing.T) {
	rec := newHookRecorder()
	svc := &ConnectionService{postCreateSyncHook: rec.hook("sync")}

	svc.dispatchPostCreateHooks("user-1", "mt5", "Exness", true)

	rec.next(t, "sync:user-1/mt5/Exness")
	rec.expectNothingMore(t)
}

// Nothing wired: no goroutine, no panic.
func TestDispatchPostCreateHooks_NoHooks(t *testing.T) {
	(&ConnectionService{}).dispatchPostCreateHooks("user-1", "ctrader", "x", true)
}

// The rebuild request is what tells "the user said no" from "the rebuild
// failed": it reaches the log, and nothing of the credentials does.
func TestLogConnectionCreatedRecordsTheRebuildRequest(t *testing.T) {
	for _, requested := range []bool{true, false} {
		core, logs := observer.New(zapcore.InfoLevel)
		svc := &ConnectionService{logger: zap.New(core)}
		svc.logConnectionCreated(&repository.ExchangeConnection{
			UserUID: "user-1", Exchange: "binance", Label: "main",
			EncryptedAPIKey: "ciphertext-key", EncryptedAPISecret: "ciphertext-secret",
		}, requested)

		entries := logs.FilterMessage("connection created").All()
		if len(entries) != 1 {
			t.Fatalf("requested=%v: %d entries, want 1", requested, len(entries))
		}
		fields := entries[0].ContextMap()
		if got, ok := fields["rebuild_history"].(bool); !ok || got != requested {
			t.Fatalf("rebuild_history = %v, want %v", fields["rebuild_history"], requested)
		}
		for k, v := range fields {
			if s, ok := v.(string); ok && strings.Contains(s, "ciphertext") {
				t.Fatalf("field %s carries credential material: %q", k, s)
			}
		}
	}
}
