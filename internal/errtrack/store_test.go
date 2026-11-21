package errtrack

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap/zapcore"
)

func mkEvent(msg string) SanitizedEvent {
	return SanitizedEvent{Level: "error", Message: msg}
}

func TestStore_RecordIncrementsExistingGroup(t *testing.T) {
	s := NewStore(10, 100)
	id := "abcdef0123456789"
	s.Record(id, zapcore.ErrorLevel, mkEvent("first"))
	s.Record(id, zapcore.ErrorLevel, mkEvent("second"))
	s.Record(id, zapcore.ErrorLevel, mkEvent("third"))

	g, ok := s.Get(id)
	if !ok {
		t.Fatal("group not found")
	}
	if g.Count != 3 {
		t.Fatalf("count = %d, want 3", g.Count)
	}
}

func TestStore_HardCapacityEvictsLRU(t *testing.T) {
	s := NewStore(3, 1000) // generous rate so the cap is the only limit
	for i := 0; i < 10; i++ {
		id := fmt.Sprintf("g%015d", i) // 16 chars, all unique
		s.Record(id, zapcore.ErrorLevel, mkEvent("e"))
	}
	stats := s.Stats()
	if stats.ActiveGroups > 3 {
		t.Fatalf("capacity exceeded: %d active", stats.ActiveGroups)
	}
	if stats.TotalEvicted == 0 {
		t.Fatal("expected eviction counter to advance")
	}
}

func TestStore_RateLimitDropsExcessNewGroups(t *testing.T) {
	// Bucket size of 5: only the first 5 distinct fingerprints in a
	// burst should be admitted. Existing groups continue to count.
	s := NewStore(1000, 5)
	admitted := 0
	for i := 0; i < 100; i++ {
		id := fmt.Sprintf("g%015d", i)
		if got := s.Record(id, zapcore.ErrorLevel, mkEvent("x")); got != "" {
			admitted++
		}
	}
	if admitted > 6 { // allow a tiny refill margin
		t.Fatalf("rate limiter let too many through: %d", admitted)
	}
	if s.Stats().TotalDropped == 0 {
		t.Fatal("expected drop counter to advance")
	}
}

func TestStore_RateLimitRefills(t *testing.T) {
	s := NewStore(1000, 10)
	// Drain
	for i := 0; i < 10; i++ {
		s.Record(fmt.Sprintf("g%015d", i), zapcore.ErrorLevel, mkEvent("x"))
	}
	// Should be empty now
	if s.Record("after_drain_xxxxx", zapcore.ErrorLevel, mkEvent("x")) != "" {
		t.Fatal("expected drained bucket to drop")
	}
	// Sleep enough for refill (>= 1 token)
	time.Sleep(150 * time.Millisecond)
	if s.Record("after_sleep_xxxx", zapcore.ErrorLevel, mkEvent("x")) == "" {
		t.Fatal("expected refilled bucket to admit")
	}
}

func TestStore_ListGroupsSortedByLastSeen(t *testing.T) {
	s := NewStore(100, 1000)
	s.Record("aaaaaaaaaaaaaaaa", zapcore.ErrorLevel, mkEvent("a"))
	time.Sleep(1100 * time.Millisecond) // LastSeen has 1s precision
	s.Record("bbbbbbbbbbbbbbbb", zapcore.ErrorLevel, mkEvent("b"))

	got := s.ListGroups(0)
	if len(got) != 2 {
		t.Fatalf("want 2 groups, got %d", len(got))
	}
	if got[0].ID != "bbbbbbbbbbbbbbbb" {
		t.Fatalf("expected most recent first, got %s", got[0].ID)
	}
}

func TestStore_OutputReSanitizes(t *testing.T) {
	// Simulate a regression where a secret slipped past the capture
	// path: we directly inject an unsafe sample into the store. The
	// public reader (ListGroups / Get) MUST scrub it on the way out.
	s := NewStore(10, 100)
	id := "regression000000"
	s.Record(id, zapcore.ErrorLevel, mkEvent("clean"))

	// Reach in and corrupt the stored sample (defense-in-depth test).
	s.mu.Lock()
	s.groups[id].Sample = &SanitizedEvent{
		Level:   "error",
		Message: "leaked api_key=AKIAIOSFODNN7EXAMPLE",
		Fields:  map[string]string{"random": "password=topsecret123"},
	}
	s.mu.Unlock()

	g, ok := s.Get(id)
	if !ok {
		t.Fatal("group missing")
	}
	if strings.Contains(g.Sample.Message, "AKIAIOSFODNN7EXAMPLE") {
		t.Fatalf("output re-sanitize failed on message: %q", g.Sample.Message)
	}
	if v := g.Sample.Fields["random"]; strings.Contains(v, "topsecret123") {
		t.Fatalf("output re-sanitize failed on field: %q", v)
	}
}

func TestStore_ConcurrentRecordIsRaceFree(t *testing.T) {
	s := NewStore(64, 10000)
	var wg sync.WaitGroup
	const goroutines = 32
	const perG = 200
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			for j := 0; j < perG; j++ {
				// Mix of new and existing groups across goroutines.
				id := fmt.Sprintf("g%015d", j%50)
				s.Record(id, zapcore.ErrorLevel, mkEvent("x"))
			}
		}(i)
	}
	wg.Wait()
	stats := s.Stats()
	if stats.TotalEvents != uint64(goroutines*perG) {
		t.Fatalf("event counter wrong: %d", stats.TotalEvents)
	}
}

func TestStore_SubscribeReceivesEvents(t *testing.T) {
	s := NewStore(10, 100)
	ch, cancel := s.Subscribe(8)
	defer cancel()

	go func() {
		s.Record("subtest000000000", zapcore.ErrorLevel, mkEvent("hi"))
	}()

	select {
	case ev := <-ch:
		if ev.GroupID != "subtest000000000" {
			t.Fatalf("unexpected group %q", ev.GroupID)
		}
	case <-time.After(time.Second):
		t.Fatal("subscriber did not receive")
	}
}

func TestStore_SlowSubscriberDoesNotBlock(t *testing.T) {
	s := NewStore(10, 100000)
	ch, cancel := s.Subscribe(2) // tiny buffer
	defer cancel()
	_ = ch // never read

	done := make(chan struct{})
	go func() {
		for i := 0; i < 1000; i++ {
			s.Record(fmt.Sprintf("g%015d", i), zapcore.ErrorLevel, mkEvent("x"))
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Record blocked on slow subscriber")
	}
}

func TestStore_CancelSubscriberReleases(t *testing.T) {
	s := NewStore(10, 100)
	_, cancel := s.Subscribe(0)
	cancel()
	cancel() // double cancel must not panic
}
