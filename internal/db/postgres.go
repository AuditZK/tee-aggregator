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
	required := []struct {
		table  string
		column string
	}{
		{table: "users", column: "platform_hash"},
		{table: "exchange_connections", column: "credentials_hash"},
		{table: "exchange_connections", column: "sync_interval_minutes"},
		{table: "exchange_connections", column: "exclude_from_report"},
		{table: "exchange_connections", column: "kyc_level"},
		{table: "exchange_connections", column: "is_paper"},
		{table: "snapshot_data", column: "label"},
	}

	for _, rc := range required {
		exists, err := columnExists(ctx, pool, rc.table, rc.column)
		if err != nil {
			logger.Warn("schema audit failed",
				zap.String("table", rc.table),
				zap.String("column", rc.column),
				zap.Error(err),
			)
			continue
		}
		if !exists {
			logger.Warn("schema column missing",
				zap.String("table", rc.table),
				zap.String("column", rc.column),
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
