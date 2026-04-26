package server

import (
	"fmt"
	"net/http"
	"strings"
)

// CORSMiddleware adds CORS headers based on allowed origins.
func CORSMiddleware(allowedOrigins string, next http.Handler) http.Handler {
	origins := parseOrigins(allowedOrigins)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")

		if origin != "" && isOriginAllowed(origin, origins) {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Api-Key")
			w.Header().Set("Access-Control-Max-Age", "86400")
		}

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// ValidateCORSConfig fails closed if production is configured with a
// wildcard or empty CORS_ORIGIN (CORS-001). The historical default
// reflected `*` against any Origin and combined with a JWT bearer would
// expose authenticated endpoints to any cross-origin script. Production
// must explicitly enumerate the trusted origins.
func ValidateCORSConfig(env, raw string) error {
	if !strings.EqualFold(env, "production") {
		return nil
	}
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return fmt.Errorf("CORS_ORIGIN must be an explicit comma-separated allowlist in production (got empty)")
	}
	for _, o := range strings.Split(trimmed, ",") {
		if strings.TrimSpace(o) == "*" {
			return fmt.Errorf("CORS_ORIGIN must not contain '*' in production")
		}
	}
	return nil
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
