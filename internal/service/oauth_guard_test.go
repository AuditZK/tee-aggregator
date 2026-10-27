package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/trackrecord/enclave/internal/connector"
	"github.com/trackrecord/enclave/internal/errsanitize"
)

// fakeRotatingBroker behaves like Saxo's token endpoint where it matters:
// every refresh token works once. It records how many refreshes ran at the
// same time and every spent token presented to it.
type fakeRotatingBroker struct {
	mu     sync.Mutex
	valid  map[string]bool
	failOn map[string]error
	issued int
	hold   time.Duration

	calls       atomic.Int32
	refused     atomic.Int32
	inFlight    atomic.Int32
	maxInFlight atomic.Int32
}

func newFakeRotatingBroker() *fakeRotatingBroker {
	return &fakeRotatingBroker{valid: map[string]bool{}, failOn: map[string]error{}}
}

// grant issues a pair the way the authorization-code flow does.
func (b *fakeRotatingBroker) grant() connector.OAuthTokens {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.issued++
	t := connector.OAuthTokens{
		AccessToken:  fmt.Sprintf("synthetic-access-%d", b.issued),
		RefreshToken: fmt.Sprintf("synthetic-refresh-%d", b.issued),
	}
	b.valid[t.RefreshToken] = true
	return t
}

func (b *fakeRotatingBroker) alive(refreshToken string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.valid[refreshToken]
}

func (b *fakeRotatingBroker) refresh(refreshToken string) (*connector.OAuthTokens, error) {
	n := b.inFlight.Add(1)
	defer b.inFlight.Add(-1)
	for {
		m := b.maxInFlight.Load()
		if n <= m || b.maxInFlight.CompareAndSwap(m, n) {
			break
		}
	}
	b.calls.Add(1)
	time.Sleep(b.hold)

	b.mu.Lock()
	if err := b.failOn[refreshToken]; err != nil {
		b.mu.Unlock()
		return nil, err
	}
	if !b.valid[refreshToken] {
		b.mu.Unlock()
		b.refused.Add(1)
		return nil, fmt.Errorf("fake %w (HTTP 400): invalid_grant", connector.ErrRefreshRefused)
	}
	delete(b.valid, refreshToken)
	b.mu.Unlock()
	t := b.grant()
	return &t, nil
}

// memTokenStore is one connection row's stored pair.
type memTokenStore struct {
	mu        sync.Mutex
	pair      connector.OAuthTokens
	failStore atomic.Bool
	loads     atomic.Int32
}

func (m *memTokenStore) get() connector.OAuthTokens {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.pair
}

func (m *memTokenStore) set(t connector.OAuthTokens) {
	m.mu.Lock()
	m.pair = t
	m.mu.Unlock()
}

func (m *memTokenStore) guard(locks *oauthTokenLocks, key string) *tokenGuard {
	return &tokenGuard{
		locks: locks,
		key:   key,
		load: func(context.Context) (connector.OAuthTokens, error) {
			m.loads.Add(1)
			return m.get(), nil
		},
		store: func(_ context.Context, t connector.OAuthTokens) error {
			if m.failStore.Load() {
				return errors.New("synthetic store outage")
			}
			m.set(t)
			return nil
		},
	}
}

func testLocks() *oauthTokenLocks {
	return newOAuthTokenLocks(oauthSlotCapacity, oauthSlotBurst, oauthSlotRefillPerSec)
}

// keepAliveRotation is what the keep-alive asks of the guard.
func keepAliveRotation(b *fakeRotatingBroker, minAge time.Duration) func(connector.OAuthTokens, time.Time) (*connector.OAuthTokens, error) {
	return func(current connector.OAuthTokens, rotatedAt time.Time) (*connector.OAuthTokens, error) {
		if !rotatedAt.IsZero() && time.Since(rotatedAt) < minAge {
			return nil, nil
		}
		return b.refresh(current.RefreshToken)
	}
}

// rejectedRotation is what a sync asks when the gateway refused the access
// token it was holding.
func rejectedRotation(b *fakeRotatingBroker, rejected string) func(connector.OAuthTokens, time.Time) (*connector.OAuthTokens, error) {
	return func(current connector.OAuthTokens, _ time.Time) (*connector.OAuthTokens, error) {
		if current.AccessToken != rejected {
			return nil, nil
		}
		return b.refresh(current.RefreshToken)
	}
}

// The invariant the whole design rests on. The keep-alive, the daily sync, a
// manual sync, a reconstruction and a re-authorization all hit the same
// connection at once. Saxo must never see two refreshes in flight, never see a
// spent refresh token, and the row must end on a pair that still works.
func TestTokenGuardOneRefreshInFlightAcrossEveryPath(t *testing.T) {
	broker := newFakeRotatingBroker()
	broker.hold = 2 * time.Millisecond
	store := &memTokenStore{pair: broker.grant()}
	locks := testLocks()
	key := oauthSlotKey("user-1", "saxo", "Saxo Account")

	var wg sync.WaitGroup
	errs := make(chan error, 256)
	run := func(n int, fn func() error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range n {
				if err := fn(); err != nil {
					errs <- err
				}
			}
		}()
	}

	keepAlive := func() error {
		_, err := store.guard(locks, key).Rotate(context.Background(), keepAliveRotation(broker, 0))
		return err
	}
	syncPath := func() error {
		held := store.get().AccessToken
		_, err := store.guard(locks, key).Rotate(context.Background(), rejectedRotation(broker, held))
		return err
	}
	reauth := func() error {
		slot, release, err := locks.acquire(context.Background(), key)
		if err != nil {
			return err
		}
		defer release()
		store.set(broker.grant())
		slot.freshAuthorization(time.Now())
		return nil
	}

	run(10, keepAlive)
	run(10, syncPath) // daily sync
	run(10, syncPath) // manual sync-now
	run(10, syncPath) // reconstruction
	run(3, reauth)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("path failed: %v", err)
	}

	if m := broker.maxInFlight.Load(); m != 1 {
		t.Fatalf("%d refreshes in flight at once, want exactly 1", m)
	}
	if n := broker.refused.Load(); n != 0 {
		t.Fatalf("%d spent refresh tokens were presented to the broker", n)
	}
	if !broker.alive(store.get().RefreshToken) {
		t.Fatal("the stored refresh token is dead: the connection would not survive its next refresh")
	}
}

// A rotated pair is used only once stored. When the store fails, Rotate fails,
// keeps the pair, and the next holder stores it first and refreshes from it,
// never from the token the broker already spent.
func TestTokenGuardStoresTheRotatedPairBeforeUse(t *testing.T) {
	broker := newFakeRotatingBroker()
	store := &memTokenStore{pair: broker.grant()}
	locks := testLocks()
	key := oauthSlotKey("user-1", "saxo", "Saxo Account")

	store.failStore.Store(true)
	got, err := store.guard(locks, key).Rotate(context.Background(), keepAliveRotation(broker, 0))
	if err == nil || !errors.Is(err, connector.ErrTransient) {
		t.Fatalf("err = %v, want a transient failure", err)
	}
	if got.AccessToken != "" {
		t.Fatal("Rotate handed out a pair it could not store")
	}

	store.failStore.Store(false)
	var seen connector.OAuthTokens
	if _, err := store.guard(locks, key).Rotate(context.Background(), func(current connector.OAuthTokens, _ time.Time) (*connector.OAuthTokens, error) {
		seen = current
		return broker.refresh(current.RefreshToken)
	}); err != nil {
		t.Fatalf("second Rotate: %v", err)
	}
	if seen.RefreshToken != "synthetic-refresh-2" {
		t.Fatalf("refreshed from %q, want the pair kept from the failed store", seen.RefreshToken)
	}
	if broker.refused.Load() != 0 {
		t.Fatal("a spent refresh token was presented")
	}
	if !broker.alive(store.get().RefreshToken) {
		t.Fatal("the stored refresh token is dead")
	}
}

// A refused token is refused once: later attempts answer without calling the
// broker, until a re-authorization replaces the pair.
func TestTokenGuardRemembersARefusedToken(t *testing.T) {
	broker := newFakeRotatingBroker()
	store := &memTokenStore{pair: connector.OAuthTokens{AccessToken: "synthetic-access-x", RefreshToken: "synthetic-refresh-dead"}}
	locks := testLocks()
	key := oauthSlotKey("user-1", "saxo", "Saxo Account")

	_, err := store.guard(locks, key).Rotate(context.Background(), keepAliveRotation(broker, 0))
	if !errors.Is(err, connector.ErrRefreshRefused) || errors.Is(err, errRefreshTokenKnownDead) {
		t.Fatalf("first refusal: err = %v", err)
	}
	if got := errsanitize.Category(err.Error()); got != errsanitize.MsgBrokerReauthRequired {
		t.Fatalf("category = %q, want the reauth category", got)
	}

	_, err = store.guard(locks, key).Rotate(context.Background(), keepAliveRotation(broker, 0))
	if !errors.Is(err, errRefreshTokenKnownDead) {
		t.Fatalf("second attempt: err = %v, want the remembered refusal", err)
	}
	if n := broker.calls.Load(); n != 1 {
		t.Fatalf("broker called %d times, want 1", n)
	}

	slot, release, err := locks.acquire(context.Background(), key)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	store.set(broker.grant())
	slot.freshAuthorization(time.Now())
	release()

	if _, err := store.guard(locks, key).Rotate(context.Background(), keepAliveRotation(broker, 0)); err != nil {
		t.Fatalf("after reconnecting: %v", err)
	}
}

// Another process sharing the database (an enclave mid-upgrade) can rotate the
// row between our read and our refresh. Its refusal must not mark the
// connection dead.
func TestTokenGuardAdoptsAPairAnotherProcessRotated(t *testing.T) {
	first := connector.OAuthTokens{AccessToken: "synthetic-access-a", RefreshToken: "synthetic-refresh-a"}
	second := connector.OAuthTokens{AccessToken: "synthetic-access-b", RefreshToken: "synthetic-refresh-b"}
	var loads atomic.Int32
	g := &tokenGuard{
		locks: testLocks(),
		key:   oauthSlotKey("user-1", "saxo", "Saxo Account"),
		load: func(context.Context) (connector.OAuthTokens, error) {
			if loads.Add(1) == 1 {
				return first, nil
			}
			return second, nil
		},
		store: func(context.Context, connector.OAuthTokens) error { return nil },
	}

	got, err := g.Rotate(context.Background(), func(connector.OAuthTokens, time.Time) (*connector.OAuthTokens, error) {
		return nil, fmt.Errorf("fake %w", connector.ErrRefreshRefused)
	})
	if err != nil || got != second {
		t.Fatalf("got %+v, %v; want the pair the other process stored", got, err)
	}
}

func TestOAuthTokenLocksEvictOnlyIdleSlots(t *testing.T) {
	locks := newOAuthTokenLocks(2, 100, 100)
	ctx := context.Background()

	_, releaseA, err := locks.acquire(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	slotB, releaseB, _ := locks.acquire(ctx, "b")
	slotB.unsaved = &connector.OAuthTokens{AccessToken: "x", RefreshToken: "y"}
	releaseB()

	// "a" is held and "b" carries a pair not yet stored: nothing may go.
	if _, _, err := locks.acquire(ctx, "c"); !errors.Is(err, errOAuthSlotsFull) {
		t.Fatalf("err = %v, want errOAuthSlotsFull", err)
	}

	slotB.unsaved = nil
	_, releaseC, err := locks.acquire(ctx, "c")
	if err != nil {
		t.Fatalf("idle slot not evicted: %v", err)
	}
	releaseC()
	releaseA()
	if n := locks.size(); n != 2 {
		t.Fatalf("size = %d, want the cap of 2", n)
	}
}

func TestOAuthTokenLocksThrottleNewSlots(t *testing.T) {
	locks := newOAuthTokenLocks(100, 1, 0)
	ctx := context.Background()
	_, release, err := locks.acquire(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	release()
	if _, _, err := locks.acquire(ctx, "b"); !errors.Is(err, errOAuthSlotsThrottled) {
		t.Fatalf("err = %v, want errOAuthSlotsThrottled", err)
	}
	// An existing slot costs nothing.
	if _, release, err := locks.acquire(ctx, "a"); err != nil {
		t.Fatalf("existing slot refused: %v", err)
	} else {
		release()
	}
}

// A waiter that gives up leaves no trace: the slot becomes evictable again.
func TestOAuthTokenLocksWaiterGivesUp(t *testing.T) {
	locks := newOAuthTokenLocks(1, 100, 100)
	_, release, err := locks.acquire(context.Background(), "a")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, _, err := locks.acquire(ctx, "a"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want the deadline", err)
	}
	release()
	release()
	if _, release, err := locks.acquire(context.Background(), "b"); err != nil {
		t.Fatalf("slot a still pinned after its waiter left: %v", err)
	} else {
		release()
	}
}

// Connect and re-authorization hold the connection's slot, so the keep-alive
// cannot refresh the old pair while the new one is being stored.
func TestCreateHoldsTheSlotForTokenGuardedExchanges(t *testing.T) {
	svc := &ConnectionService{}
	key := oauthSlotKey("user_abc1234567890", "saxo", "Saxo Account")
	_, release, err := svc.oauthLocks().acquire(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	req := &CreateConnectionRequest{UserUID: "user_abc1234567890", Exchange: "saxo", Label: " Saxo Account "}
	if err := svc.Create(ctx, req); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Create while the slot is held: err = %v, want it to wait", err)
	}
	if err := svc.Create(context.Background(), &CreateConnectionRequest{UserUID: "user_abc1234567890", Exchange: "binance", Label: "main"}); err == nil || !strings.Contains(err.Error(), "not configured") {
		t.Fatalf("a key-based exchange waited on an OAuth slot: %v", err)
	}

	release()
	if err := svc.Create(context.Background(), req); err == nil || !strings.Contains(err.Error(), "not configured") {
		t.Fatalf("after release: err = %v", err)
	}
}

// Every sync path builds its connector here: a Saxo connector must leave with
// the connection's guard, keyed exactly like the slot connect takes.
func TestGetOrCreateConnectorWiresTheTokenGuard(t *testing.T) {
	s := &SyncService{connSvc: &ConnectionService{}, factory: connector.NewFactory()}
	conn, err := s.getOrCreateConnector("saxo_sim", "user-1", " Saxo Account ", &Credentials{APIKey: "a", APISecret: "r"})
	if err != nil {
		t.Fatal(err)
	}
	saxo, ok := conn.(*connector.Saxo)
	if !ok {
		t.Fatalf("got %T", conn)
	}
	g, ok := saxo.RefreshGuard().(*tokenGuard)
	if !ok {
		t.Fatalf("guard = %T, want the service guard", saxo.RefreshGuard())
	}
	if want := oauthSlotKey("user-1", "saxo_sim", "Saxo Account"); g.key != want {
		t.Fatalf("guard key = %q, want %q", g.key, want)
	}
	if g.locks != s.connSvc.oauthLocks() {
		t.Fatal("the guard does not share the connection service's slots")
	}
}

// Where Saxo sits in each per-venue list, and why.
func TestSaxoVenueLists(t *testing.T) {
	for _, ex := range []string{"saxo", "saxo_sim"} {
		if !isOAuthExchange(ex) || !isTokenGuarded(ex) {
			t.Errorf("%s: reconnecting must re-authorize and every refresh must take the slot", ex)
		}
		if !convertsExchange(ex) {
			t.Errorf("%s: an account held in EUR or CHF must be stored in USD", ex)
		}
		// The rebuilder would refresh the pair outside the enclave and burn
		// the stored refresh token.
		if externalRebuilderSupports(ex) {
			t.Errorf("%s must never be sent to the external rebuilder", ex)
		}
		if reconstructsEverySync(ex) || historyReconstructedInEnclave(ex) {
			t.Errorf("%s has no history provider", ex)
		}
		if primaryMarketType(ex) != connector.MarketStocks || fundingMarketType(ex) != connector.MarketStocks {
			t.Errorf("%s: equity and funding must share one bucket", ex)
		}
	}
	if isTokenGuarded("ctrader") {
		t.Error("cTrader refreshes through its own persister, not the slot")
	}
}

// A keep-alive refusal is written under the marker the frontend turns into
// "reconnect".
func TestKeepAliveRefusalCarriesTheReauthMarker(t *testing.T) {
	err := fmt.Errorf("saxo %w (HTTP 400): invalid_grant", connector.ErrRefreshRefused)
	if got := syncStatusMarker(egressSyncError("refresh token", err)); got != "reauth_required" {
		t.Fatalf("marker = %q, want reauth_required", got)
	}
}
