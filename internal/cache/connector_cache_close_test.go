package cache

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/trackrecord/enclave/internal/connector"
)

// closableConnector stands in for a connector holding a socket, a read loop
// and a heartbeat. done closes when Close lands, which is what "the goroutines
// are gone" means for the cache's purposes — counting runtime goroutines would
// measure the whole test binary, not this instance.
type closableConnector struct {
	name   string
	closes atomic.Int32
	done   chan struct{}
}

func newClosable(name string) *closableConnector {
	return &closableConnector{name: name, done: make(chan struct{})}
}

func (c *closableConnector) GetBalance(context.Context) (*connector.Balance, error) { return nil, nil }
func (c *closableConnector) GetPositions(context.Context) ([]*connector.Position, error) {
	return nil, nil
}
func (c *closableConnector) GetTrades(context.Context, time.Time, time.Time) ([]*connector.Trade, error) {
	return nil, nil
}
func (c *closableConnector) TestConnection(context.Context) error { return nil }
func (c *closableConnector) Exchange() string                     { return c.name }
func (c *closableConnector) Close() error {
	if c.closes.Add(1) == 1 {
		close(c.done)
	}
	return nil
}

func (c *closableConnector) waitClosed(t *testing.T, why string) {
	t.Helper()
	select {
	case <-c.done:
	case <-time.After(2 * time.Second):
		t.Fatalf("connector %s was not closed on %s — its socket and heartbeat outlive the cache entry", c.name, why)
	}
}

func newTestCache(maxSize int, ttl time.Duration) *ConnectorCache {
	c := NewConnectorCache()
	c.maxSize = maxSize
	c.ttl = ttl
	return c
}

// E-H6: an LRU eviction must release the instance. The cache key is a hash of
// the credentials, so a cTrader token rotation builds a new instance on every
// refresh and the previous one used to heartbeat Spotware forever.
func TestConnectorCache_ClosesOnLRUEviction(t *testing.T) {
	c := newTestCache(1, time.Hour)
	defer c.Stop()

	first := newClosable("first")
	second := newClosable("second")

	c.Put("ctrader", "user-1", HashCredentials("k1", "s1", ""), first)
	c.Put("ctrader", "user-1", HashCredentials("k2", "s2", ""), second)

	first.waitClosed(t, "LRU eviction")
	if second.closes.Load() != 0 {
		t.Fatal("the live entry must not be closed")
	}
}

// Replacing the same key (same credentials, rebuilt connector) must release
// the instance being displaced.
func TestConnectorCache_ClosesTheReplacedEntry(t *testing.T) {
	c := newTestCache(10, time.Hour)
	defer c.Stop()

	hash := HashCredentials("k", "s", "")
	old := newClosable("old")
	fresh := newClosable("fresh")

	c.Put("ctrader", "user-1", hash, old)
	c.Put("ctrader", "user-1", hash, fresh)

	old.waitClosed(t, "being replaced under the same key")
	if got := c.Get("ctrader", "user-1", hash); got != fresh {
		t.Fatal("cache should hold the fresh connector")
	}
	if fresh.closes.Load() != 0 {
		t.Fatal("the replacement must not be closed")
	}
}

// An entry found expired on read is dropped — and must be released too.
func TestConnectorCache_ClosesOnTTLExpiry(t *testing.T) {
	c := newTestCache(10, 10*time.Millisecond)
	defer c.Stop()

	hash := HashCredentials("k", "s", "")
	conn := newClosable("expiring")
	c.Put("ctrader", "user-1", hash, conn)

	time.Sleep(30 * time.Millisecond)
	if got := c.Get("ctrader", "user-1", hash); got != nil {
		t.Fatal("expired entry should not be served")
	}
	conn.waitClosed(t, "TTL expiry")
}

// The periodic cleanup pass releases what it drops.
func TestConnectorCache_ClosesOnCleanupSweep(t *testing.T) {
	c := newTestCache(10, 10*time.Millisecond)
	defer c.Stop()

	conn := newClosable("swept")
	c.Put("ctrader", "user-1", HashCredentials("k", "s", ""), conn)

	time.Sleep(30 * time.Millisecond)
	c.cleanup()
	conn.waitClosed(t, "the TTL cleanup sweep")
}
