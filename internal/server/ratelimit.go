package server

import (
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// maxRateLimiterEntries caps the per-IP tracking map so a client rotating
// X-Forwarded-For or spraying RemoteAddr cannot grow the map unboundedly
// (SEC-004). 10 000 entries is more than 5x the legitimate load we expect,
// and each entry is cheap (~hundreds of bytes).
const maxRateLimiterEntries = 10_000

// IPRateLimiter enforces per-IP rate limiting with a sliding window.
type IPRateLimiter struct {
	requests map[string][]time.Time
	// insertionOrder records the IPs in FIFO order so we can evict the
	// oldest entry when the map hits maxRateLimiterEntries.
	insertionOrder []string
	trustedProxies []*net.IPNet
	mu             sync.Mutex
	limit          int
	window         time.Duration
}

// NewIPRateLimiter creates a new per-IP rate limiter. trustedProxyCIDRs is a
// list of CIDR blocks (or bare IPs) whose X-Forwarded-For header the limiter
// will trust. Any other peer's X-Forwarded-For is ignored — the real TCP
// RemoteAddr is used instead (SEC-004). Pass nil/empty to ignore the header
// entirely.
func NewIPRateLimiter(limit int, window time.Duration, trustedProxyCIDRs ...string) *IPRateLimiter {
	rl := &IPRateLimiter{
		requests:       make(map[string][]time.Time),
		limit:          limit,
		window:         window,
		trustedProxies: parseCIDRs(trustedProxyCIDRs),
	}
	// Periodic cleanup
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			rl.cleanup()
		}
	}()
	return rl
}

func parseCIDRs(raw []string) []*net.IPNet {
	out := make([]*net.IPNet, 0, len(raw))
	for _, s := range raw {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		// Accept bare IPs by converting to /32 or /128.
		if !strings.Contains(s, "/") {
			if ip := net.ParseIP(s); ip != nil {
				if ip.To4() != nil {
					s += "/32"
				} else {
					s += "/128"
				}
			}
		}
		if _, ipNet, err := net.ParseCIDR(s); err == nil {
			out = append(out, ipNet)
		}
	}
	return out
}

func (rl *IPRateLimiter) isTrustedPeer(peerAddr string) bool {
	if len(rl.trustedProxies) == 0 {
		return false
	}
	host, _, err := net.SplitHostPort(peerAddr)
	if err != nil {
		host = peerAddr
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return false
	}
	for _, c := range rl.trustedProxies {
		if c.Contains(ip) {
			return true
		}
	}
	return false
}

// Allow returns true if the request is within rate limits.
func (rl *IPRateLimiter) Allow(ip string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	cutoff := now.Add(-rl.window)

	// Filter expired entries
	times := rl.requests[ip]
	isNewKey := len(times) == 0
	var valid []time.Time
	for _, t := range times {
		if t.After(cutoff) {
			valid = append(valid, t)
		}
	}

	if len(valid) >= rl.limit {
		rl.requests[ip] = valid
		return false
	}

	// SEC-004: cap the map. When we're about to create a new key beyond the
	// limit, evict the oldest tracked IP before inserting.
	if isNewKey && len(rl.requests) >= maxRateLimiterEntries {
		if len(rl.insertionOrder) > 0 {
			victim := rl.insertionOrder[0]
			rl.insertionOrder = rl.insertionOrder[1:]
			delete(rl.requests, victim)
		}
	}

	rl.requests[ip] = append(valid, now)
	if isNewKey {
		rl.insertionOrder = append(rl.insertionOrder, ip)
	}
	return true
}

// Middleware wraps an http.HandlerFunc with rate limiting.
func (rl *IPRateLimiter) Middleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ip := rl.extractIP(r)
		if !rl.Allow(ip) {
			writeJSON(w, http.StatusTooManyRequests, map[string]any{
				"error": "rate limit exceeded, try again later",
			})
			return
		}
		next(w, r)
	}
}

func (rl *IPRateLimiter) cleanup() {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	cutoff := time.Now().Add(-rl.window)
	for ip, times := range rl.requests {
		var valid []time.Time
		for _, t := range times {
			if t.After(cutoff) {
				valid = append(valid, t)
			}
		}
		if len(valid) == 0 {
			delete(rl.requests, ip)
		} else {
			rl.requests[ip] = valid
		}
	}
	// Rebuild insertionOrder to drop deleted entries (keep it O(N) once every
	// 5 min — cheap with maxRateLimiterEntries bounded to 10k).
	if len(rl.insertionOrder) == 0 {
		return
	}
	compacted := rl.insertionOrder[:0]
	for _, ip := range rl.insertionOrder {
		if _, ok := rl.requests[ip]; ok {
			compacted = append(compacted, ip)
		}
	}
	rl.insertionOrder = compacted
}

// extractIP resolves the client IP for rate-limiting. X-Forwarded-For and
// X-Real-IP are trusted ONLY when the request's TCP peer (r.RemoteAddr) is in
// the configured trustedProxies list (SEC-004). Any other peer: the raw
// RemoteAddr wins.
func (rl *IPRateLimiter) extractIP(r *http.Request) string {
	if rl.isTrustedPeer(r.RemoteAddr) {
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			parts := strings.Split(xff, ",")
			return strings.TrimSpace(parts[0])
		}
		if xri := r.Header.Get("X-Real-IP"); xri != "" {
			return strings.TrimSpace(xri)
		}
	}
	// Fall back to RemoteAddr (strip port).
	ip := r.RemoteAddr
	if host, _, err := net.SplitHostPort(ip); err == nil {
		ip = host
	}
	return ip
}
