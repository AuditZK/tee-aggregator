package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"regexp"
	"strings"

	"github.com/trackrecord/enclave/internal/auth"
	"github.com/trackrecord/enclave/internal/repository"
	"github.com/trackrecord/enclave/internal/service"
	"go.uber.org/zap"
)

const (
	portfoliosPath      = "/api/v1/portfolios"
	maxPortfolioBodyLen = 16 << 10
)

var portfolioIDRegex = regexp.MustCompile(`^[0-9a-f]{32}$`)

type portfolioService interface {
	Create(ctx context.Context, userUID, name string, connections []repository.ConnectionRef) (*repository.Portfolio, error)
	List(ctx context.Context, userUID string) ([]*repository.Portfolio, error)
	Get(ctx context.Context, userUID, id string) (*repository.Portfolio, error)
	Rename(ctx context.Context, userUID, id, name string) (*repository.Portfolio, error)
	Archive(ctx context.Context, userUID, id string) error
	AddMember(ctx context.Context, userUID, id string, c repository.ConnectionRef) (*repository.Portfolio, error)
	RemoveMember(ctx context.Context, userUID, id string, c repository.ConnectionRef) (*repository.Portfolio, error)
}

// Portfolios serves /api/v1/portfolios and everything under it:
//
//	GET    /api/v1/portfolios                list the user's portfolios
//	POST   /api/v1/portfolios                create one
//	GET    /api/v1/portfolios/{id}           read one
//	PATCH  /api/v1/portfolios/{id}           rename it
//	DELETE /api/v1/portfolios/{id}           archive it
//	POST   /api/v1/portfolios/{id}/members   bring a connection in
//	DELETE /api/v1/portfolios/{id}/members   end a connection's stay
//
// The user is the JWT's and nothing else: unlike the legacy routes there is no
// request-supplied uid to fall back on.
func (h *Handler) Portfolios(w http.ResponseWriter, r *http.Request) {
	userUID, ok := auth.UserUIDFromContext(r.Context())
	if !ok || userUID == "" {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"success": false, "error": "authentication required"})
		return
	}
	if h.portfolioSvc == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"success": false, "error": "portfolios not configured"})
		return
	}

	parts := strings.Split(strings.Trim(strings.TrimPrefix(r.URL.Path, portfoliosPath), "/"), "/")
	switch {
	case parts[0] == "":
		h.portfolioCollection(w, r, userUID)
	case !portfolioIDRegex.MatchString(parts[0]):
		writeJSON(w, http.StatusNotFound, map[string]any{"success": false, "error": "portfolio not found"})
	case len(parts) == 1:
		h.portfolioItem(w, r, userUID, parts[0])
	case len(parts) == 2 && parts[1] == "members":
		h.portfolioMembers(w, r, userUID, parts[0])
	default:
		writeJSON(w, http.StatusNotFound, map[string]any{"success": false, "error": "not found"})
	}
}

func (h *Handler) portfolioCollection(w http.ResponseWriter, r *http.Request, userUID string) {
	switch r.Method {
	case http.MethodGet:
		portfolios, err := h.portfolioSvc.List(r.Context(), userUID)
		if err != nil {
			h.writePortfolioErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"success": true, "portfolios": portfolios})
	case http.MethodPost:
		var body struct {
			Name        string                     `json:"name"`
			Connections []repository.ConnectionRef `json:"connections"`
		}
		if !readPortfolioBody(w, r, &body) {
			return
		}
		portfolio, err := h.portfolioSvc.Create(r.Context(), userUID, body.Name, body.Connections)
		if err != nil {
			h.writePortfolioErr(w, err)
			return
		}
		writeJSON(w, http.StatusCreated, map[string]any{"success": true, "portfolio": portfolio})
	default:
		http.Error(w, msgMethodNotAllowed, http.StatusMethodNotAllowed)
	}
}

func (h *Handler) portfolioItem(w http.ResponseWriter, r *http.Request, userUID, id string) {
	switch r.Method {
	case http.MethodGet:
		portfolio, err := h.portfolioSvc.Get(r.Context(), userUID, id)
		if err != nil {
			h.writePortfolioErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"success": true, "portfolio": portfolio})
	case http.MethodPatch:
		var body struct {
			Name string `json:"name"`
		}
		if !readPortfolioBody(w, r, &body) {
			return
		}
		portfolio, err := h.portfolioSvc.Rename(r.Context(), userUID, id, body.Name)
		if err != nil {
			h.writePortfolioErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"success": true, "portfolio": portfolio})
	case http.MethodDelete:
		if err := h.portfolioSvc.Archive(r.Context(), userUID, id); err != nil {
			h.writePortfolioErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"success": true})
	default:
		http.Error(w, msgMethodNotAllowed, http.StatusMethodNotAllowed)
	}
}

func (h *Handler) portfolioMembers(w http.ResponseWriter, r *http.Request, userUID, id string) {
	var change func(context.Context, string, string, repository.ConnectionRef) (*repository.Portfolio, error)
	status := http.StatusOK
	switch r.Method {
	case http.MethodPost:
		change = h.portfolioSvc.AddMember
		status = http.StatusCreated
	case http.MethodDelete:
		change = h.portfolioSvc.RemoveMember
	default:
		http.Error(w, msgMethodNotAllowed, http.StatusMethodNotAllowed)
		return
	}

	var body repository.ConnectionRef
	if !readPortfolioBody(w, r, &body) {
		return
	}
	portfolio, err := change(r.Context(), userUID, id, body)
	if err != nil {
		h.writePortfolioErr(w, err)
		return
	}
	writeJSON(w, status, map[string]any{"success": true, "portfolio": portfolio})
}

func readPortfolioBody(w http.ResponseWriter, r *http.Request, into any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxPortfolioBodyLen)
	if err := json.NewDecoder(r.Body).Decode(into); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "error": "invalid request body"})
		return false
	}
	return true
}

// writePortfolioErr answers with a fixed message: nothing from the database
// reaches the caller.
func (h *Handler) writePortfolioErr(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, repository.ErrNotFound):
		writeJSON(w, http.StatusNotFound, map[string]any{"success": false, "error": "portfolio not found"})
	case errors.Is(err, service.ErrPortfolioInvalid):
		writeJSON(w, http.StatusBadRequest, map[string]any{"success": false, "error": err.Error()})
	case errors.Is(err, service.ErrPortfolioConnectionRefused),
		errors.Is(err, repository.ErrPortfolioNameTaken),
		errors.Is(err, repository.ErrPortfolioMemberExists):
		writeJSON(w, http.StatusConflict, map[string]any{"success": false, "error": err.Error()})
	case errors.Is(err, repository.ErrPortfoliosUnavailable):
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"success": false, "error": err.Error()})
	default:
		h.logger.Error("portfolio request failed", zap.Error(err))
		writeJSON(w, http.StatusInternalServerError, map[string]any{"success": false, "error": "internal error"})
	}
}
