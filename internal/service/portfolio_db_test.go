package service

// The portfolio rules against a real Postgres carrying the production schema.
// Runs only under scripts/test-db.sh (TEST_DATABASE_URL).

import (
	"context"
	"errors"
	"testing"

	"github.com/trackrecord/enclave/internal/repository"
	"github.com/trackrecord/enclave/internal/testdb"
)

type stubConnections struct {
	rows []*repository.ExchangeConnection
}

func (s stubConnections) GetActiveConnections(context.Context, string) ([]*repository.ExchangeConnection, error) {
	return s.rows, nil
}

func (s stubConnections) CountConnections(context.Context, string) (int, error) {
	return len(s.rows), nil
}

func portfolioFixture(t *testing.T) (*PortfolioService, context.Context) {
	t.Helper()
	pool := testdb.New(t)
	for _, uid := range []string{"user_a", "user_b"} {
		if _, err := pool.Exec(context.Background(),
			`INSERT INTO users (id, uid, "updatedAt") VALUES ($1, $1, now())`, uid); err != nil {
			t.Fatal(err)
		}
	}
	svc := NewPortfolioService(repository.NewPortfolioRepo(pool), stubConnections{rows: []*repository.ExchangeConnection{
		{Exchange: "binance", Label: "Main"},
		{Exchange: "okx", Label: "Main"},
		{Exchange: "ibkr", Label: "CTO"},
		{Exchange: "bybit", Label: "Private", ExcludeFromReport: true},
	}})
	return svc, context.Background()
}

func refs(pairs ...string) []repository.ConnectionRef {
	out := []repository.ConnectionRef{}
	for i := 0; i < len(pairs); i += 2 {
		out = append(out, repository.ConnectionRef{Exchange: pairs[i], Label: pairs[i+1]})
	}
	return out
}

func TestPortfolio_CreateKeepsTheConnectionsSpelling_DB(t *testing.T) {
	svc, ctx := portfolioFixture(t)
	p, err := svc.Create(ctx, "user_a", "  Book A  ", refs("BINANCE", " main ", "okx", "Main", "binance", "Main"))
	if err != nil {
		t.Fatal(err)
	}
	if p.Name != "Book A" {
		t.Errorf("name = %q", p.Name)
	}
	if len(p.Members) != 2 {
		t.Fatalf("members = %+v", p.Members)
	}
	if p.Members[0].Exchange != "binance" || p.Members[0].Label != "Main" {
		t.Errorf("member = %+v", p.Members[0])
	}
	if p.Members[0].AddedAt.IsZero() || p.Members[0].RemovedAt != nil {
		t.Errorf("stay = %+v", p.Members[0])
	}
}

func TestPortfolio_RefusesWhatCannotEnter_DB(t *testing.T) {
	svc, ctx := portfolioFixture(t)
	cases := map[string][]repository.ConnectionRef{
		"excluded from the verified report": refs("bybit", "Private"),
		"not the user's":                    refs("kraken", "Main"),
	}
	for name, connections := range cases {
		if _, err := svc.Create(ctx, "user_a", name, connections); !errors.Is(err, ErrPortfolioConnectionRefused) {
			t.Errorf("%s: err = %v", name, err)
		}
	}
	if _, err := svc.Create(ctx, "user_a", "empty", nil); !errors.Is(err, ErrPortfolioInvalid) {
		t.Errorf("empty: err = %v", err)
	}
	if _, err := svc.Create(ctx, "user_a", "  ", refs("okx", "Main")); !errors.Is(err, ErrPortfolioInvalid) {
		t.Errorf("no name: err = %v", err)
	}
	if list, _ := svc.List(ctx, "user_a"); len(list) != 0 {
		t.Errorf("something was stored: %+v", list)
	}
}

// The rule: removing a connection ends its stay and erases nothing.
func TestPortfolio_RemovalKeepsTheJournal_DB(t *testing.T) {
	svc, ctx := portfolioFixture(t)
	p, err := svc.Create(ctx, "user_a", "A", refs("binance", "Main", "okx", "Main"))
	if err != nil {
		t.Fatal(err)
	}

	p, err = svc.RemoveMember(ctx, "user_a", p.ID, repository.ConnectionRef{Exchange: "OKX", Label: " main"})
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Members) != 2 {
		t.Fatalf("the row went away: %+v", p.Members)
	}
	var okx repository.PortfolioMember
	for _, m := range p.Members {
		if m.Exchange == "okx" {
			okx = m
		}
	}
	if okx.RemovedAt == nil {
		t.Fatalf("stay not closed: %+v", okx)
	}

	// Coming back opens a second stay rather than reopening the first.
	p, err = svc.AddMember(ctx, "user_a", p.ID, repository.ConnectionRef{Exchange: "okx", Label: "Main"})
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Members) != 3 {
		t.Fatalf("members = %+v", p.Members)
	}

	if _, err := svc.RemoveMember(ctx, "user_a", p.ID, repository.ConnectionRef{Exchange: "kraken", Label: "Main"}); !errors.Is(err, repository.ErrNotFound) {
		t.Errorf("removing a stranger: err = %v", err)
	}
}

func TestPortfolio_RefusesDuplicates_DB(t *testing.T) {
	svc, ctx := portfolioFixture(t)
	p, err := svc.Create(ctx, "user_a", "A", refs("binance", "Main"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.AddMember(ctx, "user_a", p.ID, repository.ConnectionRef{Exchange: "binance", Label: "Main"}); !errors.Is(err, repository.ErrPortfolioMemberExists) {
		t.Errorf("same member: err = %v", err)
	}
	if _, err := svc.Create(ctx, "user_a", "a", refs("okx", "Main")); !errors.Is(err, repository.ErrPortfolioNameTaken) {
		t.Errorf("same name: err = %v", err)
	}
	// Another user is free to use the name.
	if _, err := svc.Create(ctx, "user_b", "A", refs("okx", "Main")); err != nil {
		t.Errorf("another user: err = %v", err)
	}
}

func TestPortfolio_AnotherUserReachesNothing_DB(t *testing.T) {
	svc, ctx := portfolioFixture(t)
	p, err := svc.Create(ctx, "user_a", "A", refs("binance", "Main"))
	if err != nil {
		t.Fatal(err)
	}
	const other = "user_b"
	ref := repository.ConnectionRef{Exchange: "okx", Label: "Main"}
	attempts := map[string]func() error{
		"read":   func() error { _, err := svc.Get(ctx, other, p.ID); return err },
		"rename": func() error { _, err := svc.Rename(ctx, other, p.ID, "mine"); return err },
		"add":    func() error { _, err := svc.AddMember(ctx, other, p.ID, ref); return err },
		"remove": func() error {
			_, err := svc.RemoveMember(ctx, other, p.ID, repository.ConnectionRef{Exchange: "binance", Label: "Main"})
			return err
		},
		"archive": func() error { return svc.Archive(ctx, other, p.ID) },
	}
	for name, attempt := range attempts {
		if err := attempt(); !errors.Is(err, repository.ErrNotFound) {
			t.Errorf("%s: err = %v", name, err)
		}
	}
	got, err := svc.Get(ctx, "user_a", p.ID)
	if err != nil || got.Name != "A" || len(got.Members) != 1 || got.Members[0].RemovedAt != nil || got.ArchivedAt != nil {
		t.Errorf("portfolio changed: %+v, %v", got, err)
	}
}

// An archived portfolio leaves the list and frees its name, but a report
// published from it can still read it.
func TestPortfolio_ArchiveKeepsTheRows_DB(t *testing.T) {
	svc, ctx := portfolioFixture(t)
	p, err := svc.Create(ctx, "user_a", "A", refs("binance", "Main"))
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.Archive(ctx, "user_a", p.ID); err != nil {
		t.Fatal(err)
	}
	if list, _ := svc.List(ctx, "user_a"); len(list) != 0 {
		t.Errorf("still listed: %+v", list)
	}
	got, err := svc.Get(ctx, "user_a", p.ID)
	if err != nil || got.ArchivedAt == nil || len(got.Members) != 1 {
		t.Errorf("archived read: %+v, %v", got, err)
	}
	if _, err := svc.AddMember(ctx, "user_a", p.ID, repository.ConnectionRef{Exchange: "okx", Label: "Main"}); !errors.Is(err, repository.ErrNotFound) {
		t.Errorf("adding to an archived one: err = %v", err)
	}
	if _, err := svc.Create(ctx, "user_a", "A", refs("okx", "Main")); err != nil {
		t.Errorf("name not freed: %v", err)
	}
}

// Erasing a user takes the portfolios with the snapshots: the gateway deletes the
// users row and the foreign keys do the rest.
func TestPortfolio_ErasingTheUserRemovesEverything_DB(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `INSERT INTO users (id, uid, "updatedAt") VALUES ('user_a', 'user_a', now())`); err != nil {
		t.Fatal(err)
	}
	svc := NewPortfolioService(repository.NewPortfolioRepo(pool), stubConnections{rows: []*repository.ExchangeConnection{
		{Exchange: "okx", Label: "Main"},
	}})
	if _, err := svc.Create(ctx, "user_a", "A", refs("okx", "Main")); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM users WHERE uid = 'user_a'`); err != nil {
		t.Fatal(err)
	}
	var portfolios, members int
	if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM portfolios), (SELECT count(*) FROM portfolio_members)`).Scan(&portfolios, &members); err != nil {
		t.Fatal(err)
	}
	if portfolios != 0 || members != 0 {
		t.Errorf("left behind: %d portfolios, %d members", portfolios, members)
	}
}
