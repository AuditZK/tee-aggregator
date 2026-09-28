// Package testdb hands tests a throwaway Postgres database carrying the
// production schema, so the repositories and the services built on them run
// against the columns they meet in production rather than against fakes.
//
// Tests using it are skipped unless TEST_DATABASE_URL names a server the test
// may create and drop databases on; scripts/test-db.sh provides one in Docker.
package testdb

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// prodSchema is the production enclave database's tables, types and indexes:
// `pg_dump --schema-only --no-owner --no-privileges`, kept to what the
// application creates. Refresh it after any out-of-band schema change.
//
//go:embed prod_schema.sql
var prodSchema string

// cloneLock serialises template builds and clones across test processes:
// `go test ./...` runs packages in parallel, and Postgres refuses to clone a
// template another session is still writing.
const cloneLock = 72_411_903

// New returns a pool on a fresh database cloned from the production schema.
// The database is dropped when the test ends.
func New(t testing.TB) *pgxpool.Pool {
	t.Helper()
	adminURL := os.Getenv("TEST_DATABASE_URL")
	if adminURL == "" {
		t.Skip("TEST_DATABASE_URL not set; run scripts/test-db.sh")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	name := "zka_t_" + randomSuffix(t)
	if err := cloneTemplate(ctx, adminURL, name); err != nil {
		t.Fatalf("create test database: %v", err)
	}

	cfg, err := pgxpool.ParseConfig(adminURL)
	if err != nil {
		t.Fatalf("parse TEST_DATABASE_URL: %v", err)
	}
	cfg.ConnConfig.Database = name
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("open test database: %v", err)
	}
	t.Cleanup(func() {
		pool.Close()
		dropCtx, dropCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer dropCancel()
		if err := dropDatabase(dropCtx, adminURL, name); err != nil {
			t.Logf("drop test database %s: %v", name, err)
		}
	})
	return pool
}

func templateName() string {
	sum := sha256.Sum256([]byte(prodSchema))
	return "zka_tmpl_" + hex.EncodeToString(sum[:6])
}

func cloneTemplate(ctx context.Context, adminURL, name string) error {
	admin, err := pgx.Connect(ctx, adminURL)
	if err != nil {
		return fmt.Errorf("connect admin: %w", err)
	}
	defer func() { _ = admin.Close(context.Background()) }()

	if _, err := admin.Exec(ctx, "SELECT pg_advisory_lock($1)", cloneLock); err != nil {
		return fmt.Errorf("take clone lock: %w", err)
	}
	defer func() { _, _ = admin.Exec(context.Background(), "SELECT pg_advisory_unlock($1)", cloneLock) }()

	tmpl := templateName()
	var exists bool
	if err := admin.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)", tmpl).Scan(&exists); err != nil {
		return fmt.Errorf("look up template: %w", err)
	}
	if !exists {
		if err := buildTemplate(ctx, admin, adminURL, tmpl); err != nil {
			return err
		}
	}

	if _, err := admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()+" TEMPLATE "+pgx.Identifier{tmpl}.Sanitize()); err != nil {
		return fmt.Errorf("clone template: %w", err)
	}
	return nil
}

func buildTemplate(ctx context.Context, admin *pgx.Conn, adminURL, tmpl string) error {
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{tmpl}.Sanitize()); err != nil {
		return fmt.Errorf("create template: %w", err)
	}

	cfg, err := pgx.ParseConfig(adminURL)
	if err != nil {
		return fmt.Errorf("parse TEST_DATABASE_URL: %w", err)
	}
	cfg.Database = tmpl
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return fmt.Errorf("connect template: %w", err)
	}
	_, execErr := conn.Exec(ctx, prodSchema)
	_ = conn.Close(context.Background())
	if execErr != nil {
		_, _ = admin.Exec(ctx, "DROP DATABASE IF EXISTS "+pgx.Identifier{tmpl}.Sanitize())
		return fmt.Errorf("apply production schema: %w", execErr)
	}
	return nil
}

func dropDatabase(ctx context.Context, adminURL, name string) error {
	admin, err := pgx.Connect(ctx, adminURL)
	if err != nil {
		return err
	}
	defer func() { _ = admin.Close(context.Background()) }()
	_, err = admin.Exec(ctx, "DROP DATABASE IF EXISTS "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)")
	return err
}

func randomSuffix(t testing.TB) string {
	b := make([]byte, 6)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("random database name: %v", err)
	}
	return hex.EncodeToString(b)
}
