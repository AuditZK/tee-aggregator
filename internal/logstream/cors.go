package logstream

import (
	"net/http"
	"strings"
)

// corsMiddleware adds CORS headers for the log-stream listener. Mirrors
// internal/server.CORSMiddleware but allows the extra request headers the
// dashboard uses for SSE (Cache-Control, Accept) and emits Vary: Origin so
// caches don't pollute responses across origins.
//
// CORS-002: previously injected by the Caddy sidecar in front of the
// log-stream port. Folded back into the enclave so a single Let's Encrypt
// cert (REST + log-stream) is sufficient and the proxy hop disappears.
func corsMiddleware(allowedOrigins string, next http.Handler) http.Handler {
	origins := parseOrigins(allowedOrigins)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")

		if origin != "" && isOriginAllowed(origin, origins) {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Api-Key, Cache-Control, Accept")
			w.Header().Set("Access-Control-Max-Age", "86400")
			w.Header().Add("Vary", "Origin")
		}

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func parseOrigins(raw string) []string {
	if raw == "" || raw == "*" {
		return []string{"*"}
	}
	var origins []string
	for _, o := range strings.Split(raw, ",") {
		o = strings.TrimSpace(o)
		if o != "" {
			origins = append(origins, o)
		}
	}
	return origins
}

func isOriginAllowed(origin string, allowed []string) bool {
	for _, a := range allowed {
		if a == "*" || a == origin {
			return true
		}
	}
	return false
}
