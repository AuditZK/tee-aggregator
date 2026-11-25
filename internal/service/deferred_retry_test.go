package service

import (
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/trackrecord/enclave/internal/repository"
)

// Two connections on one Flex token were both deferred at midnight and both
// retried in the same second: the token's pacing turned the second away and it
// never retried. The retries one pass arms must fire apart.
func TestDeferredRetriesOfOnePassFireApart(t *testing.T) {
	var delays []time.Duration
	s := &SyncService{
		logger:     zap.NewNop(),
		deferAfter: func(d time.Duration, _ func()) { delays = append(delays, d) },
	}
	cto := &repository.ExchangeConnection{ID: "conn-cto", Exchange: "ibkr", Label: "cto"}
	pea := &repository.ExchangeConnection{ID: "conn-pea", Exchange: "ibkr", Label: "pea"}

	s.scheduleDeferredRetry(cto, rateLimitRetryDelay)
	s.scheduleDeferredRetry(pea, rateLimitRetryDelay)
	s.scheduleDeferredRetry(cto, rateLimitRetryDelay)

	if len(delays) != 2 {
		t.Fatalf("armed %d retries, want 2 (one per connection)", len(delays))
	}
	if gap := delays[1] - delays[0]; gap < deferredRetrySpacing {
		t.Fatalf("retries %v and %v fire %v apart, want at least %v", delays[0], delays[1], gap, deferredRetrySpacing)
	}
	if delays[0] != rateLimitRetryDelay {
		t.Fatalf("first retry after %v, want %v", delays[0], rateLimitRetryDelay)
	}
}

// A retry that already fired frees its slot: the next pass's first retry is
// not pushed back by yesterday's.
func TestFiredDeferredRetryFreesItsSlot(t *testing.T) {
	var delays []time.Duration
	s := &SyncService{
		logger:     zap.NewNop(),
		deferAfter: func(d time.Duration, _ func()) { delays = append(delays, d) },
	}
	s.scheduleDeferredRetry(&repository.ExchangeConnection{ID: "conn-a"}, rateLimitRetryDelay)

	// What the timer's own cleanup does once the retry has run.
	s.deferMu.Lock()
	delete(s.deferredRetries, "conn-a")
	s.deferMu.Unlock()

	s.scheduleDeferredRetry(&repository.ExchangeConnection{ID: "conn-b"}, rateLimitRetryDelay)
	if delays[1] != rateLimitRetryDelay {
		t.Fatalf("retry after a fired one delayed %v, want %v", delays[1], rateLimitRetryDelay)
	}
}
