package db

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

// Connect establishes a connection pool to PostgreSQL
func Connect(ctx context.Context, databaseURL string, logger *zap.Logger) (*pgxpool.Pool, error) {
	if databaseURL == "" {
		return nil, fmt.Errorf("DATABASE_URL is required")
	}

	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}

	config.MaxConns = 10
	config.MinConns = 2
	config.MaxConnLifetime = time.Hour
	config.MaxConnIdleTime = 30 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}

	// Test connection
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}

	logger.Info("database connected",
		zap.Int32("max_conns", config.MaxConns),
	)

	// Audit critical parity columns at startup.
	auditSchemaColumns(ctx, pool, logger)

	return pool, nil
}

func auditSchemaColumns(ctx context.Context, pool *pgxpool.Pool, logger *zap.Logger) {
	// Check both Go (snake_case) and TS Prisma (camelCase) column names.
	// If either exists, the column is present.
	required := []struct {
		table    string
		snakeCol string
		camelCol string
	}{
		{"users", "platform_hash", "platformHash"},
		{"exchange_connections", "credentials_hash", "credentialsHash"},
		{"exchange_connections", "sync_interval_minutes", "syncIntervalMinutes"},
		{"exchange_connections", "exclude_from_report", "excludeFromReport"},
		{"exchange_connections", "kyc_level", "kycLevel"},
		{"exchange_connections", "is_paper", "isPaper"},
		{"snapshot_data", "label", "label"},
	}

	for _, rc := range required {
		snakeExists, _ := columnExists(ctx, pool, rc.table, rc.snakeCol)
		camelExists, _ := columnExists(ctx, pool, rc.table, rc.camelCol)

		if !snakeExists && !camelExists {
			logger.Warn("schema column missing",
				zap.String("table", rc.table),
				zap.String("column", rc.snakeCol),
				zap.String("hint", "apply latest SQL migrations"),
			)
		}
	}
}

func columnExists(ctx context.Context, pool *pgxpool.Pool, tableName, columnName string) (bool, error) {
	const query = `
		SELECT EXISTS (
			SELECT 1
			FROM information_schema.columns
			WHERE table_schema = 'public'
			  AND table_name = $1
			  AND column_name = $2
		)`

	var exists bool
	if err := pool.QueryRow(ctx, query, tableName, columnName).Scan(&exists); err != nil {
		return false, err
	}
	return exists, nil
}
