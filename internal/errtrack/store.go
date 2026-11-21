package errtrack

import (
	"container/list"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap/zapcore"
)

// Store defaults. Tuning rationale:
//
//   - DefaultCapacity = 1024: a single enclave is unlikely to produce
//     more than a few hundred *distinct* error groups in practice.
//     Capping at 1k bounds memory at roughly 1k * (~2 KB sample) = ~2 MB
//     in the worst case, well within enclave budgets.
//   - DefaultNewGroupRate = 50/s: lets a real burst (e.g. exchange-side
//     outage producing diverse 5xx) through, but stops a runaway
//     fingerprint generator from filling the LRU in milliseconds.
const (
	DefaultCapacity     = 1024
	DefaultNewGroupRate = 50
)

// Group is the in-memory record of one error fingerprint.
//
// Concurrency model: Group.Count and Group.LastSeenUnix are bumped on
// the hot path (every captured event) using atomic ops, with no mutex.
// All other fields are written ONCE at insertion and never mutated
// (Sample is replaced via copy-on-update under the Store mutex). This
// keeps the increment path lock-free.
type Group struct {
	ID            string
	FirstSeenUnix int64 // immutable after insert
	LastSeenUnix  atomic.Int64
	Count         atomic.Uint64
	Level         zapcore.Level

	// Sample is a redacted snapshot of one occurrence. Read under the
	// Store's RLock; written by replaceSample under WLock. The pointer
	// itself is updated atomically via the mutex; readers should copy
	// the dereferenced value before releasing the lock if they intend
	// to mutate.
	Sample *SanitizedEvent

	// elem points back to the LRU list for O(1) move-to-front on hit.
	// nil for groups that have been evicted.
	elem *list.Element
}

// Store is a bounded, LRU-evicted, rate-limited table of error groups.
//
// The store is the only place where SanitizedEvent values live in
// memory. It is safe for concurrent use from any number of goroutines.
type Store struct {
	mu       sync.RWMutex
	groups   map[string]*Group
	lru      *list.List // front = most recently seen
	capacity int

	// rate limit on NEW group creation (token bucket)
	rateMu     sync.Mutex
	tokens     float64
	maxTokens  float64
	refillRate float64 // tokens per second
	lastRefill time.Time

	// counters for the metrics endpoint and debugging
	totalEvents   atomic.Uint64
	totalDropped  atomic.Uint64 // dropped due to rate limit
	totalEvicted  atomic.Uint64 // dropped due to capacity
	totalGroupsEv atomic.Uint64 // total distinct groups ever seen

	// subscribers receive each accepted event for streaming. We hold
	// channels weakly: writes are non-blocking; slow subscribers miss
	// events rather than backpressure the capture path.
	subMu       sync.Mutex
	subscribers map[chan StreamEvent]struct{}
}

// StreamEvent is what subscribers receive on the SSE stream. It carries
// the redacted sample directly so subscribers don't need to look the
// group up in the store.
type StreamEvent struct {
	GroupID string         `json:"group_id"`
	Count   uint64         `json:"count"`
	Sample  SanitizedEvent `json:"sample"`
}

// NewStore returns a fresh Store. Pass capacity<=0 to use
// DefaultCapacity, newGroupRate<=0 to use DefaultNewGroupRate.
func NewStore(capacity int, newGroupRate int) *Store {
	if capacity <= 0 {
		capacity = DefaultCapacity
	}
	if newGroupRate <= 0 {
		newGroupRate = DefaultNewGroupRate
	}
	return &Store{
		groups:      make(map[string]*Group, capacity),
		lru:         list.New(),
		capacity:    capacity,
		tokens:      float64(newGroupRate),
		maxTokens:   float64(newGroupRate),
		refillRate:  float64(newGroupRate),
		lastRefill:  time.Now(),
		subscribers: make(map[chan StreamEvent]struct{}),
	}
}

// Record ingests an already-sanitized event. The caller MUST guarantee
// that ev came out of the sanitize.go pipeline — Record never inspects
// or scrubs ev itself.
//
// Record is safe to call from many goroutines. It returns the
// fingerprint id that the event was attributed to (useful for tests and
// debugging).
func (s *Store) Record(id string, level zapcore.Level, ev SanitizedEvent) string {
	s.totalEvents.Add(1)
	now := time.Now().Unix()

	// Fast path: existing group. Only takes RLock, no allocation, no
	// LRU mutation when we don't need it.
	s.mu.RLock()
	g, ok := s.groups[id]
	s.mu.RUnlock()

	if ok {
		g.Count.Add(1)
		g.LastSeenUnix.Store(now)
		// Move-to-front is a write op on the LRU list, so we need the
		// write lock. Skip it on the fast path; the LRU only needs to
		// be approximately accurate for eviction purposes. We take it
		// once every 64 hits per group via a simple modulo to keep
		// list ordering reasonable without contention.
		if g.Count.Load()%64 == 0 {
			s.mu.Lock()
			if g.elem != nil {
				s.lru.MoveToFront(g.elem)
			}
			s.mu.Unlock()
		}
		s.broadcast(StreamEvent{GroupID: id, Count: g.Count.Load(), Sample: ev})
		return id
	}

	// Slow path: new group. First check the rate limiter to avoid an
	// adversary creating 1M unique fingerprints in a tight loop.
	if !s.takeToken() {
		s.totalDropped.Add(1)
		return ""
	}

	s.mu.Lock()
	// Re-check under the write lock: another goroutine may have
	// inserted while we were waiting.
	if g, ok = s.groups[id]; ok {
		g.Count.Add(1)
		g.LastSeenUnix.Store(now)
		s.mu.Unlock()
		s.broadcast(StreamEvent{GroupID: id, Count: g.Count.Load(), Sample: ev})
		return id
	}

	// Evict if at capacity. We evict from the back of the LRU.
	for len(s.groups) >= s.capacity {
		back := s.lru.Back()
		if back == nil {
			break
		}
		evID := back.Value.(string)
		s.lru.Remove(back)
		delete(s.groups, evID)
		s.totalEvicted.Add(1)
	}

	sampleCopy := ev // copy by value so subsequent caller mutations cannot reach the store
	g = &Group{
		ID:            id,
		FirstSeenUnix: now,
		Level:         level,
		Sample:        &sampleCopy,
	}
	g.LastSeenUnix.Store(now)
	g.Count.Store(1)
	g.elem = s.lru.PushFront(id)
	s.groups[id] = g
	s.totalGroupsEv.Add(1)
	s.mu.Unlock()

	s.broadcast(StreamEvent{GroupID: id, Count: 1, Sample: ev})
	return id
}

// takeToken atomically removes 1 token from the rate-limit bucket.
// Returns false if the bucket is empty (caller should drop the new
// group). This is only exercised on the slow path (new group creation),
// so a plain mutex is fine.
func (s *Store) takeToken() bool {
	s.rateMu.Lock()
	defer s.rateMu.Unlock()
	now := time.Now()
	elapsed := now.Sub(s.lastRefill).Seconds()
	if elapsed > 0 {
		s.tokens += elapsed * s.refillRate
		if s.tokens > s.maxTokens {
			s.tokens = s.maxTokens
		}
		s.lastRefill = now
	}
	if s.tokens < 1 {
		return false
	}
	s.tokens -= 1
	return true
}

// GroupSummary is the public, output-safe projection of a Group. We
// return value types (not *Group) so callers cannot mutate the stored
// state, and so the JSON serializer never sees the LRU pointer.
type GroupSummary struct {
	ID        string         `json:"id"`
	Level     string         `json:"level"`
	Count     uint64         `json:"count"`
	FirstSeen time.Time      `json:"first_seen"`
	LastSeen  time.Time      `json:"last_seen"`
	Sample    SanitizedEvent `json:"sample"`
}

// ListGroups returns up to `limit` groups, sorted by LastSeen
// descending. limit<=0 or limit>capacity is clamped to capacity.
//
// SECURITY: every returned summary is re-sanitized at the field level
// before leaving the store. This is defense in depth — Record() already
// expects a sanitized event, but a regression in the capture path
// would otherwise propagate to all readers.
func (s *Store) ListGroups(limit int) []GroupSummary {
	s.mu.RLock()
	out := make([]GroupSummary, 0, len(s.groups))
	for _, g := range s.groups {
		out = append(out, summarize(g))
	}
	s.mu.RUnlock()

	sort.Slice(out, func(i, j int) bool {
		return out[i].LastSeen.After(out[j].LastSeen)
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out
}

// Get returns a single group summary, or ok=false if not found.
func (s *Store) Get(id string) (GroupSummary, bool) {
	s.mu.RLock()
	g, ok := s.groups[id]
	if !ok {
		s.mu.RUnlock()
		return GroupSummary{}, false
	}
	out := summarize(g)
	s.mu.RUnlock()
	return out, true
}

// Stats returns lifetime counters for the metrics endpoint.
func (s *Store) Stats() Stats {
	s.mu.RLock()
	active := len(s.groups)
	s.mu.RUnlock()
	return Stats{
		ActiveGroups:    active,
		TotalEvents:     s.totalEvents.Load(),
		TotalDropped:    s.totalDropped.Load(),
		TotalEvicted:    s.totalEvicted.Load(),
		TotalGroupsEver: s.totalGroupsEv.Load(),
	}
}

// Stats is the lifetime metrics snapshot of the store.
type Stats struct {
	ActiveGroups    int    `json:"active_groups"`
	TotalEvents     uint64 `json:"total_events"`
	TotalDropped    uint64 `json:"total_dropped"`
	TotalEvicted    uint64 `json:"total_evicted"`
	TotalGroupsEver uint64 `json:"total_groups_ever"`
}

func summarize(g *Group) GroupSummary {
	sample := SanitizedEvent{}
	if g.Sample != nil {
		sample = *g.Sample
	}
	return GroupSummary{
		ID:        g.ID,
		Level:     g.Level.String(),
		Count:     g.Count.Load(),
		FirstSeen: time.Unix(g.FirstSeenUnix, 0).UTC(),
		LastSeen:  time.Unix(g.LastSeenUnix.Load(), 0).UTC(),
		Sample:    reSanitizeForOutput(sample),
	}
}

// reSanitizeForOutput runs the stored sample through the message and
// field scrubbers a SECOND time before returning it on an exposed
// endpoint. This is intentional defense-in-depth: even if a regression
// or refactor in the capture path lets something slip through, output
// re-scrubbing keeps the leak contained.
func reSanitizeForOutput(s SanitizedEvent) SanitizedEvent {
	s.Message = SanitizeMessage(s.Message)
	if len(s.Fields) > 0 {
		out := make(map[string]string, len(s.Fields))
		for k, v := range s.Fields {
			out[k] = SanitizeMessage(v)
		}
		s.Fields = out
	}
	// frames carry function names + relative file paths only; they
	// are normalized at capture and contain no scrubable secret
	// shapes. Skip the regex pass to keep output cheap.
	return s
}

// Subscribe registers a channel to receive every accepted event. The
// returned cancel func MUST be called to release resources.
//
// The channel is buffered: events that overflow are dropped silently
// (the subscriber is "slow"). This guarantees the capture path never
// blocks on a stalled HTTP client.
func (s *Store) Subscribe(buffer int) (<-chan StreamEvent, func()) {
	if buffer <= 0 {
		buffer = 64
	}
	ch := make(chan StreamEvent, buffer)
	s.subMu.Lock()
	s.subscribers[ch] = struct{}{}
	s.subMu.Unlock()
	cancel := func() {
		s.subMu.Lock()
		if _, ok := s.subscribers[ch]; ok {
			delete(s.subscribers, ch)
			close(ch)
		}
		s.subMu.Unlock()
	}
	return ch, cancel
}

// PoisonForTest replaces the stored sample of an existing group with the
// supplied event WITHOUT running it through sanitize. It exists solely
// so the defense-in-depth re-sanitization layer (output side) can be
// exercised in tests by injecting a known-bad payload that the capture
// path would normally reject. Production code MUST NOT call this.
//
// No-op when the group does not exist.
func (s *Store) PoisonForTest(id string, ev SanitizedEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if g, ok := s.groups[id]; ok {
		cp := ev
		g.Sample = &cp
	}
}

func (s *Store) broadcast(ev StreamEvent) {
	s.subMu.Lock()
	subs := make([]chan StreamEvent, 0, len(s.subscribers))
	for ch := range s.subscribers {
		subs = append(subs, ch)
	}
	s.subMu.Unlock()
	for _, ch := range subs {
		select {
		case ch <- ev:
		default:
			// slow subscriber, drop
		}
	}
}
