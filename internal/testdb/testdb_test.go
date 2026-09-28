package testdb_test

import (
	"context"
	"testing"

	"github.com/trackrecord/enclave/internal/repository"
	"github.com/trackrecord/enclave/internal/testdb"
)

func TestNewCarriesProductionSchema(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()

	var tables int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM information_schema.tables
		WHERE table_schema = 'public' AND table_type = 'BASE TABLE'`).Scan(&tables); err != nil {
		t.Fatalf("count tables: %v", err)
	}
	if tables != 9 {
		t.Fatalf("tables = %d, want the 9 of production", tables)
	}

	if !repository.NewConnectionRepo(pool).IsTSSchema(ctx) {
		t.Fatal("repository does not recognise the production (TS/Prisma) schema")
	}
}

func TestNewIsolatesDatabases(t *testing.T) {
	a := testdb.New(t)
	b := testdb.New(t)
	ctx := context.Background()

	if _, err := a.Exec(ctx, `INSERT INTO users (id, uid, "updatedAt") VALUES ('u1', 'uid-1', now())`); err != nil {
		t.Fatalf("insert: %v", err)
	}
	var n int
	if err := b.QueryRow(ctx, `SELECT count(*) FROM users`).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 0 {
		t.Fatalf("a row written in one test database is visible in another (%d rows)", n)
	}
}
