package repository

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	pgUniqueViolation  = "23505"
	pgUndefinedTable   = "42P01"
	portfolioIndexName = "portfolios_user_live_name"
	memberIndexName    = "portfolio_members_live"
)

var (
	// ErrPortfoliosUnavailable is returned on a database the portfolio tables
	// were not applied to (migration 020, by hand on the TS schema).
	ErrPortfoliosUnavailable = errors.New("portfolios are not available on this database")
	// ErrPortfolioNameTaken is returned when a live portfolio already has the name.
	ErrPortfolioNameTaken = errors.New("a portfolio already has this name")
	// ErrPortfolioMemberExists is returned when the connection is already in the portfolio.
	ErrPortfolioMemberExists = errors.New("this connection is already in the portfolio")
)

// ConnectionRef names a connection the way every store keys it.
type ConnectionRef struct {
	Exchange string `json:"exchange"`
	Label    string `json:"label"`
}

// PortfolioMember is one stay of a connection in a portfolio.
type PortfolioMember struct {
	Exchange  string     `json:"exchange"`
	Label     string     `json:"label"`
	AddedAt   time.Time  `json:"added_at"`
	RemovedAt *time.Time `json:"removed_at,omitempty"`
}

// Portfolio is a named set of connections reported together.
type Portfolio struct {
	ID         string            `json:"id"`
	UserUID    string            `json:"-"`
	Name       string            `json:"name"`
	CreatedAt  time.Time         `json:"created_at"`
	ArchivedAt *time.Time        `json:"archived_at,omitempty"`
	Members    []PortfolioMember `json:"members"`
}

// PortfolioRepo stores portfolios and the journal of their members. Every
// statement carries the user: a portfolio id alone reaches nothing.
type PortfolioRepo struct {
	pool *pgxpool.Pool
}

// NewPortfolioRepo creates a portfolio repository.
func NewPortfolioRepo(pool *pgxpool.Pool) *PortfolioRepo {
	return &PortfolioRepo{pool: pool}
}

func newPortfolioID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("portfolio id: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

// portfolioErr turns the database's answer into the repository's vocabulary.
func portfolioErr(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch {
		case pgErr.Code == pgUndefinedTable:
			return ErrPortfoliosUnavailable
		case pgErr.Code == pgUniqueViolation && pgErr.ConstraintName == portfolioIndexName:
			return ErrPortfolioNameTaken
		case pgErr.Code == pgUniqueViolation && pgErr.ConstraintName == memberIndexName:
			return ErrPortfolioMemberExists
		}
	}
	return err
}

// Create stores a portfolio and opens a stay for each of its connections.
func (r *PortfolioRepo) Create(ctx context.Context, userUID, name string, connections []ConnectionRef) (*Portfolio, error) {
	id, err := newPortfolioID()
	if err != nil {
		return nil, err
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx,
		`INSERT INTO portfolios (id, user_uid, name) VALUES ($1, $2, $3)`,
		id, userUID, name,
	); err != nil {
		return nil, portfolioErr(err)
	}
	for _, c := range connections {
		if _, err := tx.Exec(ctx,
			`INSERT INTO portfolio_members (portfolio_id, exchange, label) VALUES ($1, $2, $3)`,
			id, c.Exchange, c.Label,
		); err != nil {
			return nil, portfolioErr(err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return r.Get(ctx, userUID, id)
}

// List returns the user's live portfolios, oldest first, with their journal.
func (r *PortfolioRepo) List(ctx context.Context, userUID string) ([]*Portfolio, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT p.id, p.user_uid, p.name, p.created_at, p.archived_at
		FROM portfolios p
		WHERE p.user_uid = $1 AND p.archived_at IS NULL
		ORDER BY p.created_at, p.id`, userUID)
	if err != nil {
		return nil, portfolioErr(err)
	}
	defer rows.Close()

	portfolios := []*Portfolio{}
	byID := map[string]*Portfolio{}
	ids := []string{}
	for rows.Next() {
		p := &Portfolio{Members: []PortfolioMember{}}
		if err := rows.Scan(&p.ID, &p.UserUID, &p.Name, &p.CreatedAt, &p.ArchivedAt); err != nil {
			return nil, err
		}
		portfolios = append(portfolios, p)
		byID[p.ID] = p
		ids = append(ids, p.ID)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return portfolios, nil
	}

	if err := r.loadMembers(ctx, ids, byID); err != nil {
		return nil, err
	}
	return portfolios, nil
}

// Get returns one of the user's portfolios, archived ones included: a report
// published from one still has to read it.
func (r *PortfolioRepo) Get(ctx context.Context, userUID, id string) (*Portfolio, error) {
	p := &Portfolio{Members: []PortfolioMember{}}
	err := r.pool.QueryRow(ctx, `
		SELECT id, user_uid, name, created_at, archived_at
		FROM portfolios WHERE id = $1 AND user_uid = $2`, id, userUID,
	).Scan(&p.ID, &p.UserUID, &p.Name, &p.CreatedAt, &p.ArchivedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, portfolioErr(err)
	}
	if err := r.loadMembers(ctx, []string{id}, map[string]*Portfolio{id: p}); err != nil {
		return nil, err
	}
	return p, nil
}

func (r *PortfolioRepo) loadMembers(ctx context.Context, ids []string, byID map[string]*Portfolio) error {
	rows, err := r.pool.Query(ctx, `
		SELECT portfolio_id, exchange, label, added_at, removed_at
		FROM portfolio_members
		WHERE portfolio_id = ANY($1)
		ORDER BY added_at, id`, ids)
	if err != nil {
		return portfolioErr(err)
	}
	defer rows.Close()

	for rows.Next() {
		var portfolioID string
		var m PortfolioMember
		if err := rows.Scan(&portfolioID, &m.Exchange, &m.Label, &m.AddedAt, &m.RemovedAt); err != nil {
			return err
		}
		if p, ok := byID[portfolioID]; ok {
			p.Members = append(p.Members, m)
		}
	}
	return rows.Err()
}

// Rename changes a live portfolio's name.
func (r *PortfolioRepo) Rename(ctx context.Context, userUID, id, name string) error {
	tag, err := r.pool.Exec(ctx, `
		UPDATE portfolios SET name = $1
		WHERE id = $2 AND user_uid = $3 AND archived_at IS NULL`, name, id, userUID)
	if err != nil {
		return portfolioErr(err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// Archive takes a portfolio out of the user's list. Its rows stay, for the
// reports already published from it.
func (r *PortfolioRepo) Archive(ctx context.Context, userUID, id string) error {
	tag, err := r.pool.Exec(ctx, `
		UPDATE portfolios SET archived_at = now()
		WHERE id = $1 AND user_uid = $2 AND archived_at IS NULL`, id, userUID)
	if err != nil {
		return portfolioErr(err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// AddMember opens a new stay for a connection.
func (r *PortfolioRepo) AddMember(ctx context.Context, userUID, id string, c ConnectionRef) error {
	tag, err := r.pool.Exec(ctx, `
		INSERT INTO portfolio_members (portfolio_id, exchange, label)
		SELECT p.id, $3, $4 FROM portfolios p
		WHERE p.id = $1 AND p.user_uid = $2 AND p.archived_at IS NULL`,
		id, userUID, c.Exchange, c.Label)
	if err != nil {
		return portfolioErr(err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// RemoveMember closes a connection's current stay. Its row stays, and so does
// what the portfolio consolidated from it until now.
func (r *PortfolioRepo) RemoveMember(ctx context.Context, userUID, id string, c ConnectionRef) error {
	tag, err := r.pool.Exec(ctx, `
		UPDATE portfolio_members m SET removed_at = now()
		FROM portfolios p
		WHERE m.portfolio_id = p.id
		  AND p.id = $1 AND p.user_uid = $2 AND p.archived_at IS NULL
		  AND m.removed_at IS NULL
		  AND lower(m.exchange) = lower($3)
		  AND lower(btrim(m.label)) = lower(btrim($4))`,
		id, userUID, c.Exchange, strings.TrimSpace(c.Label))
	if err != nil {
		return portfolioErr(err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}
