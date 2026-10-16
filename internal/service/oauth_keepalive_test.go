package service

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/trackrecord/enclave/internal/connector"
	"github.com/trackrecord/enclave/internal/repository"
	"go.uber.org/zap"
)

// keepAliveFakeConnector refreshes through whatever guard it is handed, against
// the fake broker, the way the Saxo connector does.
type keepAliveFakeConnector struct {
	broker *fakeRotatingBroker
	guard  connector.TokenGuard
}

func (c *keepAliveFakeConnector) GetBalance(context.Context) (*connector.Balance, error) {
	return nil, nil
}
func (c *keepAliveFakeConnector) GetPositions(context.Context) ([]*connector.Position, error) {
	return nil, nil
}
func (c *keepAliveFakeConnector) GetTrades(context.Context, time.Time, time.Time) ([]*connector.Trade, error) {
	return nil, nil
}
func (c *keepAliveFakeConnector) TestConnection(context.Context) error { return nil }
func (c *keepAliveFakeConnector) Exchange() string                     { return "saxo" }
func (c *keepAliveFakeConnector) SetRefreshGuard(g connector.TokenGuard) {
	c.guard = g
}
func (c *keepAliveFakeConnector) KeepAlive(ctx context.Context, minAge time.Duration) error {
	_, err := c.guard.Rotate(ctx, keepAliveRotation(c.broker, minAge))
	return err
}

type keepAliveHarness struct {
	broker *fakeRotatingBroker
	locks  *oauthTokenLocks
	conns  []*repository.ExchangeConnection
	stores map[string]*memTokenStore

	mu     sync.Mutex
	marked []string
}

func newKeepAliveHarness(ids ...string) *keepAliveHarness {
	h := &keepAliveHarness{
		broker: newFakeRotatingBroker(),
		locks:  testLocks(),
		stores: map[string]*memTokenStore{},
	}
	for _, id := range ids {
		h.conns = append(h.conns, &repository.ExchangeConnection{ID: id, UserUID: "user-" + id, Exchange: "saxo", Label: "Saxo Account"})
		h.stores[id] = &memTokenStore{pair: h.broker.grant()}
	}
	return h
}

func (h *keepAliveHarness) keepAlive() *OAuthKeepAlive {
	return &OAuthKeepAlive{
		logger:   zap.NewNop(),
		interval: time.Minute,
		minAge:   0,
		workers:  2,
		list: func(context.Context) ([]*repository.ExchangeConnection, error) {
			return h.conns, nil
		},
		build: func(*repository.ExchangeConnection) (connector.Connector, error) {
			return &keepAliveFakeConnector{broker: h.broker}, nil
		},
		guardFor: func(conn *repository.ExchangeConnection) connector.TokenGuard {
			return h.stores[conn.ID].guard(h.locks, oauthSlotKey(conn.UserUID, conn.Exchange, conn.Label))
		},
		markReauth: func(_ context.Context, conn *repository.ExchangeConnection, _ error) {
			h.mu.Lock()
			h.marked = append(h.marked, conn.ID)
			h.mu.Unlock()
		},
	}
}

func (h *keepAliveHarness) markedIDs() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.marked...)
}

func TestKeepAliveRotatesAndStoresEveryConnection(t *testing.T) {
	h := newKeepAliveHarness("a", "b", "c")
	before := map[string]string{}
	for id, s := range h.stores {
		before[id] = s.get().RefreshToken
	}

	counts := h.keepAlive().runOnce(context.Background())
	if counts[keepAliveOK] != 3 {
		t.Fatalf("outcomes = %v, want 3 ok", counts)
	}
	for id, s := range h.stores {
		now := s.get().RefreshToken
		if now == before[id] || !h.broker.alive(now) {
			t.Fatalf("%s: stored refresh token %q not rotated to a live one", id, now)
		}
	}
	if h.broker.refused.Load() != 0 || len(h.markedIDs()) != 0 {
		t.Fatal("a healthy pass refused or marked something")
	}
}

// A refused refresh token marks the connection for reconnecting once; the
// next passes leave it alone instead of calling Saxo with the dead token.
func TestKeepAliveMarksARefusedConnectionOnce(t *testing.T) {
	h := newKeepAliveHarness("a", "b")
	h.stores["b"].set(connector.OAuthTokens{AccessToken: "synthetic-access-b", RefreshToken: "synthetic-refresh-revoked"})
	k := h.keepAlive()

	counts := k.runOnce(context.Background())
	if counts[keepAliveRefused] != 1 || counts[keepAliveOK] != 1 {
		t.Fatalf("outcomes = %v", counts)
	}
	if got := h.markedIDs(); len(got) != 1 || got[0] != "b" {
		t.Fatalf("marked = %v, want [b]", got)
	}

	calls := h.broker.calls.Load()
	counts = k.runOnce(context.Background())
	if counts[keepAliveAlreadyDead] != 1 {
		t.Fatalf("second pass outcomes = %v", counts)
	}
	if h.broker.calls.Load() != calls+1 {
		t.Fatal("the dead token was presented to the broker again")
	}
	if len(h.markedIDs()) != 1 {
		t.Fatal("the connection was marked twice")
	}
}

// An outage or a throttle at the token endpoint says nothing about the
// authorization: no mark, and the next pass tries again.
func TestKeepAliveRetriesATransientFailure(t *testing.T) {
	h := newKeepAliveHarness("a")
	h.broker.failOn[h.stores["a"].get().RefreshToken] = fmt.Errorf("%w: saxo token endpoint unavailable (HTTP 503)", connector.ErrTransient)
	k := h.keepAlive()

	if counts := k.runOnce(context.Background()); counts[keepAliveFailed] != 1 {
		t.Fatalf("outcomes = %v", counts)
	}
	delete(h.broker.failOn, h.stores["a"].get().RefreshToken)
	if counts := k.runOnce(context.Background()); counts[keepAliveOK] != 1 {
		t.Fatalf("retry outcomes = %v", counts)
	}
	if len(h.markedIDs()) != 0 {
		t.Fatal("a transient failure marked the connection")
	}
}

// Missing app credentials are the operator's to fix; no connection is marked.
func TestKeepAliveNotConfiguredMarksNothing(t *testing.T) {
	h := newKeepAliveHarness("a")
	h.broker.failOn[h.stores["a"].get().RefreshToken] = connector.ErrSaxoNotConfigured
	if counts := h.keepAlive().runOnce(context.Background()); counts[keepAliveNotConfigured] != 1 {
		t.Fatalf("outcomes = %v", counts)
	}
	if len(h.markedIDs()) != 0 {
		t.Fatal("a configuration problem marked the connection")
	}
}

func TestKeepAliveSurvivesAListingFailure(t *testing.T) {
	k := newKeepAliveHarness().keepAlive()
	k.list = func(context.Context) ([]*repository.ExchangeConnection, error) {
		return nil, errors.New("synthetic database outage")
	}
	if counts := k.runOnce(context.Background()); counts != nil {
		t.Fatalf("outcomes = %v, want none", counts)
	}
}

func TestKeepAliveStartStop(t *testing.T) {
	h := newKeepAliveHarness("a")
	k := h.keepAlive()
	k.interval = time.Hour
	k.jitter = time.Minute
	k.Start()
	deadline := time.Now().Add(2 * time.Second)
	for h.broker.calls.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	k.Stop()
	k.Stop()
	if h.broker.calls.Load() == 0 {
		t.Fatal("no pass ran at start")
	}
}

func TestKeepAliveJitterStaysInBounds(t *testing.T) {
	k := &OAuthKeepAlive{interval: 15 * time.Minute, jitter: 2 * time.Minute}
	for range 200 {
		if d := k.nextDelay(); d < 13*time.Minute || d >= 17*time.Minute {
			t.Fatalf("delay %v outside 13m..17m", d)
		}
	}
}
