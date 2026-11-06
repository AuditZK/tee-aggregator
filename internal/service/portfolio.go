package service

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/trackrecord/enclave/internal/repository"
)

const maxPortfolioNameLen = 80

var (
	// ErrPortfolioInvalid is returned for a name or a connection list the
	// request got wrong. Its message is fixed and safe to show.
	ErrPortfolioInvalid = errors.New("invalid portfolio")
	// ErrPortfolioConnectionRefused is returned for a connection that cannot
	// enter a portfolio: unknown, paused, or kept out of the verified report.
	ErrPortfolioConnectionRefused = errors.New("this connection cannot join a portfolio")
)

type portfolioStore interface {
	Create(ctx context.Context, userUID, name string, connections []repository.ConnectionRef) (*repository.Portfolio, error)
	List(ctx context.Context, userUID string) ([]*repository.Portfolio, error)
	Get(ctx context.Context, userUID, id string) (*repository.Portfolio, error)
	Rename(ctx context.Context, userUID, id, name string) error
	Archive(ctx context.Context, userUID, id string) error
	AddMember(ctx context.Context, userUID, id string, c repository.ConnectionRef) error
	RemoveMember(ctx context.Context, userUID, id string, c repository.ConnectionRef) error
}

type portfolioConnections interface {
	GetActiveConnections(ctx context.Context, userUID string) ([]*repository.ExchangeConnection, error)
}

// PortfolioService applies what a portfolio may contain. The plan's limit on how
// many a user holds is the frontend's, which knows the plan.
type PortfolioService struct {
	store       portfolioStore
	connections portfolioConnections
}

// NewPortfolioService creates a portfolio service.
func NewPortfolioService(store portfolioStore, connections portfolioConnections) *PortfolioService {
	return &PortfolioService{store: store, connections: connections}
}

func cleanPortfolioName(name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" || len(name) > maxPortfolioNameLen {
		return "", fmt.Errorf("%w: name must be 1 to %d characters", ErrPortfolioInvalid, maxPortfolioNameLen)
	}
	return name, nil
}

// resolve maps requested connections onto the user's own rows, so a member
// carries the spelling the snapshots do. An excluded connection is refused: it
// was promised to stay out of every verified report.
func (s *PortfolioService) resolve(ctx context.Context, userUID string, requested []repository.ConnectionRef) ([]repository.ConnectionRef, error) {
	active, err := s.connections.GetActiveConnections(ctx, userUID)
	if err != nil {
		return nil, err
	}
	byKey := make(map[string]*repository.ExchangeConnection, len(active))
	for _, c := range active {
		byKey[portfolioConnectionKey(c.Exchange, c.Label)] = c
	}

	seen := make(map[string]struct{}, len(requested))
	resolved := make([]repository.ConnectionRef, 0, len(requested))
	for _, r := range requested {
		key := portfolioConnectionKey(r.Exchange, r.Label)
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		c, ok := byKey[key]
		if !ok || c.ExcludeFromReport {
			return nil, ErrPortfolioConnectionRefused
		}
		resolved = append(resolved, repository.ConnectionRef{Exchange: c.Exchange, Label: c.Label})
	}
	return resolved, nil
}

func portfolioConnectionKey(exchange, label string) string {
	return strings.ToLower(strings.TrimSpace(exchange)) + "|" + strings.ToLower(strings.TrimSpace(label))
}

// Create makes a portfolio from at least one of the user's connections.
func (s *PortfolioService) Create(ctx context.Context, userUID, name string, connections []repository.ConnectionRef) (*repository.Portfolio, error) {
	name, err := cleanPortfolioName(name)
	if err != nil {
		return nil, err
	}
	if len(connections) == 0 {
		return nil, fmt.Errorf("%w: at least one connection is required", ErrPortfolioInvalid)
	}
	resolved, err := s.resolve(ctx, userUID, connections)
	if err != nil {
		return nil, err
	}
	return s.store.Create(ctx, userUID, name, resolved)
}

// List returns the user's live portfolios.
func (s *PortfolioService) List(ctx context.Context, userUID string) ([]*repository.Portfolio, error) {
	return s.store.List(ctx, userUID)
}

// Get returns one of the user's portfolios.
func (s *PortfolioService) Get(ctx context.Context, userUID, id string) (*repository.Portfolio, error) {
	return s.store.Get(ctx, userUID, id)
}

// Rename changes a portfolio's name.
func (s *PortfolioService) Rename(ctx context.Context, userUID, id, name string) (*repository.Portfolio, error) {
	name, err := cleanPortfolioName(name)
	if err != nil {
		return nil, err
	}
	if err := s.store.Rename(ctx, userUID, id, name); err != nil {
		return nil, err
	}
	return s.store.Get(ctx, userUID, id)
}

// Archive takes a portfolio out of the user's list.
func (s *PortfolioService) Archive(ctx context.Context, userUID, id string) error {
	return s.store.Archive(ctx, userUID, id)
}

// AddMember brings a connection into a portfolio.
func (s *PortfolioService) AddMember(ctx context.Context, userUID, id string, c repository.ConnectionRef) (*repository.Portfolio, error) {
	resolved, err := s.resolve(ctx, userUID, []repository.ConnectionRef{c})
	if err != nil {
		return nil, err
	}
	if err := s.store.AddMember(ctx, userUID, id, resolved[0]); err != nil {
		return nil, err
	}
	return s.store.Get(ctx, userUID, id)
}

// RemoveMember ends a connection's stay. A connection deleted since is removed
// the same way, so its own row is not consulted here.
func (s *PortfolioService) RemoveMember(ctx context.Context, userUID, id string, c repository.ConnectionRef) (*repository.Portfolio, error) {
	if err := s.store.RemoveMember(ctx, userUID, id, c); err != nil {
		return nil, err
	}
	return s.store.Get(ctx, userUID, id)
}
