package logstream

import (
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/trackrecord/enclave/internal/errtrack"
)

// errorIDPattern enforces the shape of a valid fingerprint id on the
// path. Only lowercase hex of the configured length is accepted; this
// blocks any attempt to use a path-traversal or injection payload as a
// group id, even though the store lookup uses a map (no SQL).
var errorIDPattern = regexp.MustCompile(`^[a-f0-9]{16}$`)

// maxListLimit caps the number of groups one request can fetch. Bounds
// memory and serialisation time per request and keeps the response
// shape predictable for callers.
const maxListLimit = 200

// SetErrorStore wires an errtrack.Store into the logstream server so
// the /errors endpoints can serve from it. Pass nil to disable.
//
// SetErrorStore is safe to call before or after Start.
func (s *Server) SetErrorStore(store *errtrack.Store) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.errStore = store
}

// registerErrorRoutes wires the /errors handlers on the supplied mux.
// Called from Start so that all routes share the same auth middleware
// and TLS config as the rest of logstream. Endpoints are no-ops (404)
// when no store has been set, matching the existing pattern for
// uninitialised attestation.
func (s *Server) registerErrorRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/errors/groups", s.authMiddleware(s.handleErrorGroups))
	mux.HandleFunc("/errors/groups/", s.authMiddleware(s.handleErrorGroupByID))
	mux.HandleFunc("/errors/stream", s.authMiddleware(s.handleErrorStream))
	mux.HandleFunc("/errors/stats", s.authMiddleware(s.handleErrorStats))
}

func (s *Server) errorStore() *errtrack.Store {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.errStore
}

func (s *Server) handleErrorGroups(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, msgMethodNotAllowed, http.StatusMethodNotAllowed)
		return
	}
	store := s.errorStore()
	if store == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"error": "error tracking disabled",
		})
		return
	}

	limit := parseLimit(r.URL.Query().Get("limit"))
	groups := store.ListGroups(limit)
	writeJSON(w, http.StatusOK, map[string]any{
		"groups": groups,
		"count":  len(groups),
	})
}

func (s *Server) handleErrorGroupByID(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, msgMethodNotAllowed, http.StatusMethodNotAllowed)
		return
	}
	store := s.errorStore()
	if store == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"error": "error tracking disabled",
		})
		return
	}

	id := strings.TrimPrefix(r.URL.Path, "/errors/groups/")
	id = strings.Trim(id, "/")
	if !errorIDPattern.MatchString(id) {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"error": "invalid group id",
		})
		return
	}

	g, ok := store.Get(id)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]any{
			"error": "group not found",
		})
		return
	}
	writeJSON(w, http.StatusOK, g)
}

func (s *Server) handleErrorStream(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, msgMethodNotAllowed, http.StatusMethodNotAllowed)
		return
	}
	store := s.errorStore()
	if store == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"error": "error tracking disabled",
		})
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	ch, cancel := store.Subscribe(64)
	defer cancel()

	ctx := r.Context()
	for {
		select {
		case ev, ok := <-ch:
			if !ok {
				return
			}
			// Re-marshal through json.Marshal so that the same
			// re-sanitization that ListGroups applies on the
			// summary path also runs here on the streamed sample.
			ev.Sample = reSanitizeStreamEvent(ev.Sample)
			data, err := json.Marshal(ev)
			if err != nil {
				continue
			}
			if _, err := fmt.Fprintf(w, "data: %s\n\n", data); err != nil {
				return
			}
			flusher.Flush()
		case <-ctx.Done():
			return
		}
	}
}

func (s *Server) handleErrorStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, msgMethodNotAllowed, http.StatusMethodNotAllowed)
		return
	}
	store := s.errorStore()
	if store == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"error": "error tracking disabled",
		})
		return
	}
	writeJSON(w, http.StatusOK, store.Stats())
}

// parseLimit reads the optional ?limit= query parameter, clamping to
// [1, maxListLimit]. Empty or invalid values default to maxListLimit.
func parseLimit(raw string) int {
	if raw == "" {
		return maxListLimit
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return maxListLimit
	}
	if n > maxListLimit {
		return maxListLimit
	}
	return n
}

// reSanitizeStreamEvent runs the SSE-bound sample through one more
// scrub pass before emission. This mirrors the policy on the polling
// endpoints (Store.summarize) so a regression in the capture path
// cannot leak through the stream channel.
func reSanitizeStreamEvent(s errtrack.SanitizedEvent) errtrack.SanitizedEvent {
	s.Message = errtrack.SanitizeMessage(s.Message)
	if len(s.Fields) > 0 {
		out := make(map[string]string, len(s.Fields))
		for k, v := range s.Fields {
			out[k] = errtrack.SanitizeMessage(v)
		}
		s.Fields = out
	}
	return s
}
