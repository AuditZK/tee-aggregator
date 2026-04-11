package db

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

type migrationFile struct {
	version int
	name    string
	path    string
}

// ApplyMigrations executes SQL migration files from dir in ascending numeric order.
// File name format must start with a numeric prefix, e.g. "001_init.sql".
//
// Schema detection: If the target database was previously managed by the
// TypeScript enclave (Prisma/camelCase), this function SKIPS all migrations
// and logs a warning. The TS schema uses "userUid" / "isActive" columns while
// Go migrations create "user_uid" / "is_active" — applying Go migrations on a
// TS schema would fail partway through. The Go repositories include dual-schema
// query support (see connection.go) so the enclave works fine against a TS DB
// as long as migrations are skipped.
func ApplyMigrations(ctx context.Context, pool *pgxpool.Pool, dir string, logger *zap.Logger) error {
	if pool == nil {
		return fmt.Errorf("pool is nil")
	}
	if strings.TrimSpace(dir) == "" {
		return fmt.Errorf("migrations dir is empty")
	}

	// Detect legacy TS/Prisma schema before touching anything.
	tsSchema, err := detectTSPrismaSchema(ctx, pool)
	if err != nil {
		return fmt.Errorf("detect schema: %w", err)
	}
	if tsSchema {
		logger.Warn("detected TS/Prisma schema (camelCase columns) — skipping Go migrations",
			zap.String("reason", "exchange_connections.\"userUid\" column exists"),
			zap.String("hint", "Go repositories use dual-schema query support; no DDL changes applied"),
		)
		return nil
	}

	files, err := loadMigrationFiles(dir)
	if err != nil {
		return err
	}
	if len(files) == 0 {
		logger.Warn("no migration files found", zap.String("dir", dir))
		return nil
	}

	if err := ensureSchemaMigrationsTable(ctx, pool); err != nil {
		return fmt.Errorf("ensure schema_migrations table: %w", err)
	}

	applied, err := getAppliedVersions(ctx, pool)
	if err != nil {
		return fmt.Errorf("load applied migrations: %w", err)
	}

	for _, m := range files {
		if _, ok := applied[m.version]; ok {
			continue
		}

		sqlBytes, err := os.ReadFile(m.path)
		if err != nil {
			return fmt.Errorf("read migration %s: %w", m.path, err)
		}

		tx, err := pool.Begin(ctx)
		if err != nil {
			return fmt.Errorf("begin tx for migration %d: %w", m.version, err)
		}

		if _, err := tx.Exec(ctx, string(sqlBytes)); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("execute migration %d (%s): %w", m.version, m.name, err)
		}

		if _, err := tx.Exec(ctx,
			`INSERT INTO schema_migrations (version, name, applied_at) VALUES ($1, $2, $3)`,
			m.version, m.name, time.Now().UTC(),
		); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("record migration %d (%s): %w", m.version, m.name, err)
		}

		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit migration %d (%s): %w", m.version, m.name, err)
		}

		logger.Info("migration applied",
			zap.Int("version", m.version),
			zap.String("name", m.name),
		)
	}

	return nil
}

func loadMigrationFiles(dir string) ([]migrationFile, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read migrations dir %s: %w", dir, err)
	}

	files := make([]migrationFile, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.ToLower(filepath.Ext(name)) != ".sql" {
			continue
		}

		version, err := parseMigrationVersion(name)
		if err != nil {
			return nil, fmt.Errorf("invalid migration filename %s: %w", name, err)
		}

		files = append(files, migrationFile{
			version: version,
			name:    name,
			path:    filepath.Join(dir, name),
		})
	}

	sort.Slice(files, func(i, j int) bool {
		if files[i].version == files[j].version {
			return files[i].name < files[j].name
		}
		return files[i].version < files[j].version
	})

	for i := 1; i < len(files); i++ {
		if files[i].version == files[i-1].version {
			return nil, fmt.Errorf("duplicate migration version %d (%s and %s)",
				files[i].version, files[i-1].name, files[i].name)
		}
	}

	return files, nil
}

func parseMigrationVersion(filename string) (int, error) {
	base := strings.TrimSuffix(filename, filepath.Ext(filename))
	parts := strings.SplitN(base, "_", 2)
	if len(parts) == 0 || strings.TrimSpace(parts[0]) == "" {
		return 0, fmt.Errorf("missing numeric prefix")
	}
	version, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, err
	}
	return version, nil
}

func ensureSchemaMigrationsTable(ctx context.Context, pool *pgxpool.Pool) error {
	const q = `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version INT PRIMARY KEY,
			name TEXT NOT NULL,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`
	_, err := pool.Exec(ctx, q)
	return err
}

// detectTSPrismaSchema returns true if the target DB has the legacy
// TypeScript/Prisma schema, identified by the presence of a "userUid"
// (camelCase) column on exchange_connections. This is the marker used by
// repositories/connection.go for dual-schema query routing.
//
// Returns (false, nil) if exchange_connections does not exist yet — that's a
// fresh DB and Go migrations should proceed normally.
func detectTSPrismaSchema(ctx context.Context, pool *pgxpool.Pool) (bool, error) {
	const q = `
		SELECT EXISTS (
			SELECT 1
			FROM information_schema.columns
			WHERE table_schema = 'public'
			  AND table_name = 'exchange_connections'
			  AND column_name = 'userUid'
		)`
	var exists bool
	if err := pool.QueryRow(ctx, q).Scan(&exists); err != nil {
		return false, err
	}
	return exists, nil
}

func getAppliedVersions(ctx context.Context, pool *pgxpool.Pool) (map[int]struct{}, error) {
	rows, err := pool.Query(ctx, `SELECT version FROM schema_migrations`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[int]struct{})
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		out[v] = struct{}{}
	}
	return out, rows.Err()
}
