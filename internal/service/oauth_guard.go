package service

import (
	"container/list"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/trackrecord/enclave/internal/connector"
)

// tokenGuardedExchanges rotate the refresh token on every use. Every path that
// can refresh one of their connections takes the connection's slot, and the
// keep-alive refreshes them between syncs.
var tokenGuardedExchanges = []string{"saxo", "saxo_sim"}

func isTokenGuarded(exchange string) bool {
	return slices.Contains(tokenGuardedExchanges, normalizeExchange(exchange))
}

const (
	oauthSlotCapacity     = 10000
	oauthSlotBurst        = 200
	oauthSlotRefillPerSec = 20

	oauthPersistAttempts = 3
	oauthPersistBackoff  = 250 * time.Millisecond
	oauthPersistTimeout  = 10 * time.Second
)

var (
	errOAuthSlotsThrottled = fmt.Errorf("%w: too many new token refresh slots, retry later", connector.ErrTransient)
	errOAuthSlotsFull      = fmt.Errorf("%w: every token refresh slot is busy, retry later", connector.ErrTransient)

	// errRefreshTokenKnownDead answers without calling the broker for a token
	// it already refused: the stored pair stays dead until a reconnect.
	errRefreshTokenKnownDead = fmt.Errorf("%w: this refresh token was already refused, the account must be reconnected", connector.ErrRefreshRefused)
)

// oauthSlot is one connection's refresh slot. sem is the lock; the fields
// below it belong to whoever holds it.
type oauthSlot struct {
	key   string
	sem   chan struct{}
	users int // holders and waiters, guarded by oauthTokenLocks.mu
	elem  *list.Element

	rotatedAt time.Time
	unsaved   *connector.OAuthTokens
	dead      [sha256.Size]byte
	hasDead   bool
}

func (s *oauthSlot) isDead(refreshToken string) bool {
	return s.hasDead && s.dead == sha256.Sum256([]byte(refreshToken))
}

func (s *oauthSlot) markDead(refreshToken string) {
	s.dead = sha256.Sum256([]byte(refreshToken))
	s.hasDead = true
}

// freshAuthorization records a pair the user has just obtained.
func (s *oauthSlot) freshAuthorization(at time.Time) {
	s.rotatedAt = at
	s.unsaved = nil
	s.hasDead = false
}

// oauthTokenLocks holds the refresh slots, bounded like errtrack's store: a
// hard cap, LRU eviction of idle slots, a token bucket on new ones. A slot in
// use or holding a pair not yet stored is never evicted: dropping it would
// hand a second holder a fresh lock, or lose the only live pair.
type oauthTokenLocks struct {
	mu       sync.Mutex
	slots    map[string]*oauthSlot
	lru      *list.List
	capacity int

	tokens     float64
	maxTokens  float64
	refillRate float64
	lastRefill time.Time
}

func newOAuthTokenLocks(capacity, burst int, refillPerSec float64) *oauthTokenLocks {
	return &oauthTokenLocks{
		slots:      make(map[string]*oauthSlot),
		lru:        list.New(),
		capacity:   capacity,
		tokens:     float64(burst),
		maxTokens:  float64(burst),
		refillRate: refillPerSec,
		lastRefill: time.Now(),
	}
}

func oauthSlotKey(userUID, exchange, label string) string {
	return userUID + "|" + normalizeExchange(exchange) + "|" + strings.TrimSpace(label)
}

// acquire blocks until the caller holds key's slot, or ctx ends. release is
// idempotent.
func (l *oauthTokenLocks) acquire(ctx context.Context, key string) (*oauthSlot, func(), error) {
	l.mu.Lock()
	slot, ok := l.slots[key]
	if ok {
		l.lru.MoveToFront(slot.elem)
	} else {
		if !l.takeTokenLocked() {
			l.mu.Unlock()
			return nil, nil, errOAuthSlotsThrottled
		}
		if len(l.slots) >= l.capacity && !l.evictIdleLocked() {
			l.mu.Unlock()
			return nil, nil, errOAuthSlotsFull
		}
		slot = &oauthSlot{key: key, sem: make(chan struct{}, 1)}
		slot.elem = l.lru.PushFront(slot)
		l.slots[key] = slot
	}
	slot.users++
	l.mu.Unlock()

	select {
	case slot.sem <- struct{}{}:
	case <-ctx.Done():
		l.leave(slot)
		return nil, nil, ctx.Err()
	}

	var once sync.Once
	return slot, func() {
		once.Do(func() {
			<-slot.sem
			l.leave(slot)
		})
	}, nil
}

func (l *oauthTokenLocks) leave(slot *oauthSlot) {
	l.mu.Lock()
	slot.users--
	l.mu.Unlock()
}

func (l *oauthTokenLocks) evictIdleLocked() bool {
	for e := l.lru.Back(); e != nil; e = e.Prev() {
		slot := e.Value.(*oauthSlot)
		if slot.users == 0 && slot.unsaved == nil {
			l.lru.Remove(e)
			delete(l.slots, slot.key)
			return true
		}
	}
	return false
}

func (l *oauthTokenLocks) takeTokenLocked() bool {
	now := time.Now()
	if elapsed := now.Sub(l.lastRefill).Seconds(); elapsed > 0 {
		l.tokens = min(l.maxTokens, l.tokens+elapsed*l.refillRate)
		l.lastRefill = now
	}
	if l.tokens < 1 {
		return false
	}
	l.tokens--
	return true
}

func (l *oauthTokenLocks) size() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.slots)
}

// tokenGuard is a connection's connector.TokenGuard: its slot in the shared
// registry, and the stored pair as the source of truth.
type tokenGuard struct {
	locks *oauthTokenLocks
	key   string
	load  func(ctx context.Context) (connector.OAuthTokens, error)
	store func(ctx context.Context, t connector.OAuthTokens) error
}

func (g *tokenGuard) Rotate(ctx context.Context, fn func(connector.OAuthTokens, time.Time) (*connector.OAuthTokens, error)) (connector.OAuthTokens, error) {
	slot, release, err := g.locks.acquire(ctx, g.key)
	if err != nil {
		return connector.OAuthTokens{}, err
	}
	defer release()

	if slot.unsaved != nil {
		if err := g.persist(ctx, *slot.unsaved); err != nil {
			return connector.OAuthTokens{}, fmt.Errorf("%w: store the rotated tokens kept from the last refresh: %w", connector.ErrTransient, err)
		}
		slot.unsaved = nil
	}

	current, err := g.load(ctx)
	if err != nil {
		return connector.OAuthTokens{}, fmt.Errorf("read stored tokens: %w", err)
	}
	if slot.isDead(current.RefreshToken) {
		return current, errRefreshTokenKnownDead
	}

	next, err := fn(current, slot.rotatedAt)
	if errors.Is(err, connector.ErrRefreshRefused) {
		return g.afterRefusal(ctx, slot, current, err)
	}
	if err != nil || next == nil {
		return current, err
	}

	// The broker has rotated: from here the stored refresh token is dead, and
	// the new pair is used only once it is stored.
	slot.rotatedAt = time.Now()
	if err := g.persist(ctx, *next); err != nil {
		slot.unsaved = next
		return connector.OAuthTokens{}, fmt.Errorf("%w: store rotated tokens, kept in memory for the next attempt: %w", connector.ErrTransient, err)
	}
	return *next, nil
}

// afterRefusal re-reads the row before declaring the stored pair dead: another
// process sharing the database (an enclave being upgraded) may have rotated it.
func (g *tokenGuard) afterRefusal(ctx context.Context, slot *oauthSlot, refused connector.OAuthTokens, cause error) (connector.OAuthTokens, error) {
	if again, err := g.load(ctx); err == nil && again.RefreshToken != refused.RefreshToken {
		return again, nil
	}
	slot.markDead(refused.RefreshToken)
	return refused, cause
}

func (g *tokenGuard) persist(ctx context.Context, t connector.OAuthTokens) error {
	var err error
	for attempt := range oauthPersistAttempts {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt) * oauthPersistBackoff)
		}
		// Detached: the pair is already rotated at the broker, so a caller that
		// gave up must not cancel the only write that keeps it.
		storeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), oauthPersistTimeout)
		err = g.store(storeCtx, t)
		cancel()
		if err == nil {
			return nil
		}
	}
	return err
}

func (s *ConnectionService) oauthLocks() *oauthTokenLocks {
	s.oauthOnce.Do(func() {
		if s.oauth == nil {
			s.oauth = newOAuthTokenLocks(oauthSlotCapacity, oauthSlotBurst, oauthSlotRefillPerSec)
		}
	})
	return s.oauth
}

// TokenGuard returns the refresh guard of one connection, shared by every
// connector instance and code path that refreshes it.
func (s *ConnectionService) TokenGuard(userUID, exchange, label string) connector.TokenGuard {
	exchange = normalizeExchange(exchange)
	label = strings.TrimSpace(label)
	return &tokenGuard{
		locks: s.oauthLocks(),
		key:   oauthSlotKey(userUID, exchange, label),
		load: func(ctx context.Context) (connector.OAuthTokens, error) {
			creds, err := s.GetDecryptedCredentialsByLabel(ctx, userUID, exchange, label)
			if err != nil {
				return connector.OAuthTokens{}, err
			}
			return connector.OAuthTokens{AccessToken: creds.APIKey, RefreshToken: creds.APISecret}, nil
		},
		store: func(ctx context.Context, t connector.OAuthTokens) error {
			return s.PersistOAuthTokens(ctx, userUID, exchange, label, t.AccessToken, t.RefreshToken)
		},
	}
}
