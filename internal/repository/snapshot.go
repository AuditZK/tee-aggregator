package repository

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// generateCUID generates a CUID-like identifier compatible with Prisma's @id @default(cuid()).
func generateCUID() string {
	b := make([]byte, 12)
	rand.Read(b)
	return fmt.Sprintf("c%x%010x", time.Now().UnixMilli(), b)
}

// Snapshot represents a daily equity snapshot
type Snapshot struct {
	ID              string           `json:"id"`
	UserUID         string           `json:"user_uid"`
	Exchange        string           `json:"exchange"`
	Label           string           `json:"label,omitempty"`
	Timestamp       time.Time        `json:"timestamp"` // 00:00 UTC
	TotalEquity     float64          `json:"total_equity"`
	RealizedBalance float64          `json:"realized_balance"`
	UnrealizedPnL   float64          `json:"unrealized_pnl"`
	Deposits        float64          `json:"deposits"`
	Withdrawals     float64          `json:"withdrawals"`
	TotalTrades     int              `json:"total_trades"`
	TotalVolume     float64          `json:"total_volume"`
	TotalFees       float64          `json:"total_fees"`
	Breakdown       *MarketBreakdown `json:"breakdown,omitempty"`
	CreatedAt       time.Time        `json:"created_at"`
}

// MarketBreakdown holds metrics per market type
type MarketBreakdown struct {
	Stocks      *MarketMetrics `json:"stocks,omitempty"`
	Spot        *MarketMetrics `json:"spot,omitempty"`
	Swap        *MarketMetrics `json:"swap,omitempty"`
	Futures     *MarketMetrics `json:"futures,omitempty"`
	Options     *MarketMetrics `json:"options,omitempty"`
	Margin      *MarketMetrics `json:"margin,omitempty"`
	Earn        *MarketMetrics `json:"earn,omitempty"`
	CFD         *MarketMetrics `json:"cfd,omitempty"`
	Forex       *MarketMetrics `json:"forex,omitempty"`
	Commodities *MarketMetrics `json:"commodities,omitempty"`
}

// MarketMetrics holds trading metrics for a market type
type MarketMetrics struct {
	Equity          float64 `json:"equity,omitempty"`
	AvailableMargin float64 `json:"available_margin,omitempty"`
	Volume          float64 `json:"volume"`
	Trades          int     `json:"trades"`
	TradingFees     float64 `json:"trading_fees"`
	FundingFees     float64 `json:"funding_fees"`
	LongTrades      int     `json:"long_trades,omitempty"`
	ShortTrades     int     `json:"short_trades,omitempty"`
	LongVolume      float64 `json:"long_volume,omitempty"`
	ShortVolume     float64 `json:"short_volume,omitempty"`
}

// SnapshotRepo handles snapshot persistence.
// Supports both TS (Prisma camelCase) and Go (snake_case) column naming.
type SnapshotRepo struct {
	pool *pgxpool.Pool

	capMu              sync.Mutex
	capabilitiesLoaded bool
	hasLabelCol        bool
	isTSSchema         bool // true = TS Prisma camelCase columns
}

// NewSnapshotRepo creates a new snapshot repository
func NewSnapshotRepo(pool *pgxpool.Pool) *SnapshotRepo {
	return &SnapshotRepo{pool: pool}
}

// Upsert creates or updates a snapshot for a user/exchange/date
func (r *SnapshotRepo) Upsert(ctx context.Context, s *Snapshot) error {
	breakdownJSON, _ := json.Marshal(s.Breakdown)
	hasLabel := r.hasLabelColumn(ctx)

	if r.isTSSchema {
		return r.upsertTS(ctx, s, breakdownJSON)
	}

	if hasLabel {
		query := `
			INSERT INTO snapshot_data (
				user_uid, exchange, label, timestamp,
				total_equity, realized_balance, unrealized_pnl,
				deposits, withdrawals, total_trades, total_volume, total_fees,
				breakdown_by_market, created_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
			ON CONFLICT (user_uid, exchange, label, timestamp)
			DO UPDATE SET
				total_equity = EXCLUDED.total_equity,
				realized_balance = EXCLUDED.realized_balance,
				unrealized_pnl = EXCLUDED.unrealized_pnl,
				deposits = EXCLUDED.deposits,
				withdrawals = EXCLUDED.withdrawals,
				total_trades = EXCLUDED.total_trades,
				total_volume = EXCLUDED.total_volume,
				total_fees = EXCLUDED.total_fees,
				breakdown_by_market = EXCLUDED.breakdown_by_market
			RETURNING id`

		return r.pool.QueryRow(ctx, query,
			s.UserUID, s.Exchange, s.Label, s.Timestamp,
			s.TotalEquity, s.RealizedBalance, s.UnrealizedPnL,
			s.Deposits, s.Withdrawals, s.TotalTrades, s.TotalVolume, s.TotalFees,
			breakdownJSON, time.Now().UTC(),
		).Scan(&s.ID)
	}

	query := `
		INSERT INTO snapshot_data (
			user_uid, exchange, timestamp,
			total_equity, realized_balance, unrealized_pnl,
			deposits, withdrawals, total_trades, total_volume, total_fees,
			breakdown_by_market, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
		ON CONFLICT (user_uid, exchange, timestamp)
		DO UPDATE SET
			total_equity = EXCLUDED.total_equity,
			realized_balance = EXCLUDED.realized_balance,
			unrealized_pnl = EXCLUDED.unrealized_pnl,
			deposits = EXCLUDED.deposits,
			withdrawals = EXCLUDED.withdrawals,
			total_trades = EXCLUDED.total_trades,
			total_volume = EXCLUDED.total_volume,
			total_fees = EXCLUDED.total_fees,
			breakdown_by_market = EXCLUDED.breakdown_by_market
		RETURNING id`

	return r.pool.QueryRow(ctx, query,
		s.UserUID, s.Exchange, s.Timestamp,
		s.TotalEquity, s.RealizedBalance, s.UnrealizedPnL,
		s.Deposits, s.Withdrawals, s.TotalTrades, s.TotalVolume, s.TotalFees,
		breakdownJSON, time.Now().UTC(),
	).Scan(&s.ID)
}

// upsertTS writes to TS Prisma schema (camelCase columns, no total_trades/total_volume/total_fees).
// TS always has the label column. Generates a CUID-like id (Prisma doesn't use UUID defaults).
func (r *SnapshotRepo) upsertTS(ctx context.Context, s *Snapshot, breakdownJSON []byte) error {
	now := time.Now().UTC()
	generatedID := generateCUID()
	query := `
		INSERT INTO snapshot_data (
			id, "userUid", exchange, label, timestamp,
			"totalEquity", "realizedBalance", "unrealizedPnL",
			deposits, withdrawals,
			breakdown_by_market, "createdAt", "updatedAt"
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
		ON CONFLICT ("userUid", exchange, label, timestamp)
		DO UPDATE SET
			"totalEquity" = EXCLUDED."totalEquity",
			"realizedBalance" = EXCLUDED."realizedBalance",
			"unrealizedPnL" = EXCLUDED."unrealizedPnL",
			deposits = EXCLUDED.deposits,
			withdrawals = EXCLUDED.withdrawals,
			breakdown_by_market = EXCLUDED.breakdown_by_market,
			"updatedAt" = EXCLUDED."updatedAt"
		RETURNING id`

	return r.pool.QueryRow(ctx, query, generatedID,
		s.UserUID, s.Exchange, s.Label, s.Timestamp,
		s.TotalEquity, s.RealizedBalance, s.UnrealizedPnL,
		s.Deposits, s.Withdrawals,
		breakdownJSON, now, now,
	).Scan(&s.ID)
}

// GetByUserAndDateRange returns snapshots for a user within a date range
func (r *SnapshotRepo) GetByUserAndDateRange(ctx context.Context, userUID string, start, end time.Time) ([]*Snapshot, error) {
	hasLabel := r.hasLabelColumn(ctx)

	if r.isTSSchema {
		return r.getByUserAndDateRangeTS(ctx, userUID, start, end)
	}

	selectCols := "id, user_uid, exchange, timestamp"
	if hasLabel {
		selectCols = "id, user_uid, exchange, label, timestamp"
	}
	query := fmt.Sprintf(`
		SELECT %s,
			total_equity, realized_balance, unrealized_pnl,
			deposits, withdrawals, total_trades, total_volume, total_fees,
			breakdown_by_market, created_at
		FROM snapshot_data
		WHERE user_uid = $1 AND timestamp >= $2 AND timestamp <= $3
		ORDER BY timestamp`,
		selectCols,
	)

	rows, err := r.pool.Query(ctx, query, userUID, start, end)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return r.scanSnapshots(rows, hasLabel)
}

func (r *SnapshotRepo) getByUserAndDateRangeTS(ctx context.Context, userUID string, start, end time.Time) ([]*Snapshot, error) {
	query := `
		SELECT id, "userUid", exchange, label, timestamp,
			"totalEquity", "realizedBalance", "unrealizedPnL",
			deposits, withdrawals,
			breakdown_by_market, "createdAt"
		FROM snapshot_data
		WHERE "userUid" = $1 AND timestamp >= $2 AND timestamp <= $3
		ORDER BY timestamp`

	rows, err := r.pool.Query(ctx, query, userUID, start, end)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return r.scanSnapshotsTS(rows)
}

// GetLatestByUser returns the most recent snapshot for a user
func (r *SnapshotRepo) GetLatestByUser(ctx context.Context, userUID string) (*Snapshot, error) {
	hasLabel := r.hasLabelColumn(ctx)

	if r.isTSSchema {
		return r.getLatestByUserTS(ctx, userUID)
	}

	selectCols := "id, user_uid, exchange, timestamp"
	if hasLabel {
		selectCols = "id, user_uid, exchange, label, timestamp"
	}
	query := fmt.Sprintf(`
		SELECT %s,
			total_equity, realized_balance, unrealized_pnl,
			deposits, withdrawals, total_trades, total_volume, total_fees,
			breakdown_by_market, created_at
		FROM snapshot_data
		WHERE user_uid = $1
		ORDER BY timestamp DESC
		LIMIT 1`,
		selectCols,
	)

	rows, err := r.pool.Query(ctx, query, userUID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	snapshots, err := r.scanSnapshots(rows, hasLabel)
	if err != nil {
		return nil, err
	}

	if len(snapshots) == 0 {
		return nil, ErrNotFound
	}

	return snapshots[0], nil
}

func (r *SnapshotRepo) getLatestByUserTS(ctx context.Context, userUID string) (*Snapshot, error) {
	query := `
		SELECT id, "userUid", exchange, label, timestamp,
			"totalEquity", "realizedBalance", "unrealizedPnL",
			deposits, withdrawals,
			breakdown_by_market, "createdAt"
		FROM snapshot_data
		WHERE "userUid" = $1
		ORDER BY timestamp DESC
		LIMIT 1`

	rows, err := r.pool.Query(ctx, query, userUID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	snapshots, err := r.scanSnapshotsTS(rows)
	if err != nil {
		return nil, err
	}

	if len(snapshots) == 0 {
		return nil, ErrNotFound
	}

	return snapshots[0], nil
}

// GetByUserExchangeAndDate returns a specific snapshot
func (r *SnapshotRepo) GetByUserExchangeAndDate(ctx context.Context, userUID, exchange string, date time.Time) (*Snapshot, error) {
	hasLabel := r.hasLabelColumn(ctx)

	if r.isTSSchema {
		return r.getByUserExchangeAndDateTS(ctx, userUID, exchange, date)
	}

	selectCols := "id, user_uid, exchange, timestamp"
	if hasLabel {
		selectCols = "id, user_uid, exchange, label, timestamp"
	}
	whereClause := "WHERE user_uid = $1 AND exchange = $2 AND timestamp = $3"
	if hasLabel {
		whereClause = "WHERE user_uid = $1 AND exchange = $2 AND label = '' AND timestamp = $3"
	}
	query := fmt.Sprintf(`
		SELECT %s,
			total_equity, realized_balance, unrealized_pnl,
			deposits, withdrawals, total_trades, total_volume, total_fees,
			breakdown_by_market, created_at
		FROM snapshot_data
		%s`,
		selectCols, whereClause,
	)

	rows, err := r.pool.Query(ctx, query, userUID, exchange, date)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	snapshots, err := r.scanSnapshots(rows, hasLabel)
	if err != nil {
		return nil, err
	}

	if len(snapshots) == 0 {
		return nil, ErrNotFound
	}

	return snapshots[0], nil
}

func (r *SnapshotRepo) getByUserExchangeAndDateTS(ctx context.Context, userUID, exchange string, date time.Time) (*Snapshot, error) {
	query := `
		SELECT id, "userUid", exchange, label, timestamp,
			"totalEquity", "realizedBalance", "unrealizedPnL",
			deposits, withdrawals,
			breakdown_by_market, "createdAt"
		FROM snapshot_data
		WHERE "userUid" = $1 AND exchange = $2 AND label = '' AND timestamp = $3`

	rows, err := r.pool.Query(ctx, query, userUID, exchange, date)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	snapshots, err := r.scanSnapshotsTS(rows)
	if err != nil {
		return nil, err
	}

	if len(snapshots) == 0 {
		return nil, ErrNotFound
	}

	return snapshots[0], nil
}

// GetByUserExchangeLabelAndDate returns a specific snapshot for a user/exchange/label/date.
func (r *SnapshotRepo) GetByUserExchangeLabelAndDate(ctx context.Context, userUID, exchange, label string, date time.Time) (*Snapshot, error) {
	hasLabel := r.hasLabelColumn(ctx)

	if r.isTSSchema {
		return r.getByUserExchangeLabelAndDateTS(ctx, userUID, exchange, label, date)
	}

	if !hasLabel {
		return r.GetByUserExchangeAndDate(ctx, userUID, exchange, date)
	}

	query := `
		SELECT id, user_uid, exchange, label, timestamp,
			total_equity, realized_balance, unrealized_pnl,
			deposits, withdrawals, total_trades, total_volume, total_fees,
			breakdown_by_market, created_at
		FROM snapshot_data
		WHERE user_uid = $1 AND exchange = $2 AND label = $3 AND timestamp = $4`

	rows, err := r.pool.Query(ctx, query, userUID, exchange, label, date)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	snapshots, err := r.scanSnapshots(rows, true)
	if err != nil {
		return nil, err
	}
	if len(snapshots) == 0 {
		return nil, ErrNotFound
	}
	return snapshots[0], nil
}

func (r *SnapshotRepo) getByUserExchangeLabelAndDateTS(ctx context.Context, userUID, exchange, label string, date time.Time) (*Snapshot, error) {
	query := `
		SELECT id, "userUid", exchange, label, timestamp,
			"totalEquity", "realizedBalance", "unrealizedPnL",
			deposits, withdrawals,
			breakdown_by_market, "createdAt"
		FROM snapshot_data
		WHERE "userUid" = $1 AND exchange = $2 AND label = $3 AND timestamp = $4`

	rows, err := r.pool.Query(ctx, query, userUID, exchange, label, date)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	snapshots, err := r.scanSnapshotsTS(rows)
	if err != nil {
		return nil, err
	}
	if len(snapshots) == 0 {
		return nil, ErrNotFound
	}
	return snapshots[0], nil
}

// GetLatestByUserExchangeLabel returns the most recent snapshot for a user/exchange/label.
func (r *SnapshotRepo) GetLatestByUserExchangeLabel(ctx context.Context, userUID, exchange, label string) (*Snapshot, error) {
	hasLabel := r.hasLabelColumn(ctx)

	if r.isTSSchema {
		return r.getLatestByUserExchangeLabelTS(ctx, userUID, exchange, label)
	}

	if !hasLabel {
		query := `
			SELECT id, user_uid, exchange, timestamp,
				total_equity, realized_balance, unrealized_pnl,
				deposits, withdrawals, total_trades, total_volume, total_fees,
				breakdown_by_market, created_at
			FROM snapshot_data
			WHERE user_uid = $1 AND exchange = $2
			ORDER BY timestamp DESC
			LIMIT 1`

		rows, err := r.pool.Query(ctx, query, userUID, exchange)
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		snapshots, err := r.scanSnapshots(rows, false)
		if err != nil {
			return nil, err
		}
		if len(snapshots) == 0 {
			return nil, ErrNotFound
		}
		return snapshots[0], nil
	}

	query := `
		SELECT id, user_uid, exchange, label, timestamp,
			total_equity, realized_balance, unrealized_pnl,
			deposits, withdrawals, total_trades, total_volume, total_fees,
			breakdown_by_market, created_at
		FROM snapshot_data
		WHERE user_uid = $1 AND exchange = $2 AND label = $3
		ORDER BY timestamp DESC
		LIMIT 1`

	rows, err := r.pool.Query(ctx, query, userUID, exchange, label)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	snapshots, err := r.scanSnapshots(rows, true)
	if err != nil {
		return nil, err
	}
	if len(snapshots) == 0 {
		return nil, ErrNotFound
	}
	return snapshots[0], nil
}

func (r *SnapshotRepo) getLatestByUserExchangeLabelTS(ctx context.Context, userUID, exchange, label string) (*Snapshot, error) {
	query := `
		SELECT id, "userUid", exchange, label, timestamp,
			"totalEquity", "realizedBalance", "unrealizedPnL",
			deposits, withdrawals,
			breakdown_by_market, "createdAt"
		FROM snapshot_data
		WHERE "userUid" = $1 AND exchange = $2 AND label = $3
		ORDER BY timestamp DESC
		LIMIT 1`

	rows, err := r.pool.Query(ctx, query, userUID, exchange, label)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	snapshots, err := r.scanSnapshotsTS(rows)
	if err != nil {
		return nil, err
	}
	if len(snapshots) == 0 {
		return nil, ErrNotFound
	}
	return snapshots[0], nil
}

// UpsertBatch atomically upserts multiple snapshots in a single transaction.
// If any snapshot fails, the entire batch is rolled back (TS parity: atomic daily sync).
func (r *SnapshotRepo) UpsertBatch(ctx context.Context, snapshots []*Snapshot) error {
	if len(snapshots) == 0 {
		return nil
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	hasLabel := r.hasLabelColumn(ctx)

	for _, s := range snapshots {
		breakdownJSON, _ := json.Marshal(s.Breakdown)

		if r.isTSSchema {
			now := time.Now().UTC()
			_, err = tx.Exec(ctx, `
				INSERT INTO snapshot_data (
					id, "userUid", exchange, label, timestamp,
					"totalEquity", "realizedBalance", "unrealizedPnL",
					deposits, withdrawals,
					breakdown_by_market, "createdAt", "updatedAt"
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
				ON CONFLICT ("userUid", exchange, label, timestamp)
				DO UPDATE SET
					"totalEquity" = EXCLUDED."totalEquity",
					"realizedBalance" = EXCLUDED."realizedBalance",
					"unrealizedPnL" = EXCLUDED."unrealizedPnL",
					deposits = EXCLUDED.deposits,
					withdrawals = EXCLUDED.withdrawals,
					breakdown_by_market = EXCLUDED.breakdown_by_market,
					"updatedAt" = EXCLUDED."updatedAt"`,
				generateCUID(),
				s.UserUID, s.Exchange, s.Label, s.Timestamp,
				s.TotalEquity, s.RealizedBalance, s.UnrealizedPnL,
				s.Deposits, s.Withdrawals,
				breakdownJSON, now, now,
			)
		} else if hasLabel {
			_, err = tx.Exec(ctx, `
				INSERT INTO snapshot_data (
					user_uid, exchange, label, timestamp,
					total_equity, realized_balance, unrealized_pnl,
					deposits, withdrawals, total_trades, total_volume, total_fees,
					breakdown_by_market, created_at
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
				ON CONFLICT (user_uid, exchange, label, timestamp)
				DO UPDATE SET
					total_equity = EXCLUDED.total_equity,
					realized_balance = EXCLUDED.realized_balance,
					unrealized_pnl = EXCLUDED.unrealized_pnl,
					deposits = EXCLUDED.deposits,
					withdrawals = EXCLUDED.withdrawals,
					total_trades = EXCLUDED.total_trades,
					total_volume = EXCLUDED.total_volume,
					total_fees = EXCLUDED.total_fees,
					breakdown_by_market = EXCLUDED.breakdown_by_market`,
				s.UserUID, s.Exchange, s.Label, s.Timestamp,
				s.TotalEquity, s.RealizedBalance, s.UnrealizedPnL,
				s.Deposits, s.Withdrawals, s.TotalTrades, s.TotalVolume, s.TotalFees,
				breakdownJSON, s.CreatedAt,
			)
		} else {
			_, err = tx.Exec(ctx, `
				INSERT INTO snapshot_data (
					user_uid, exchange, timestamp,
					total_equity, realized_balance, unrealized_pnl,
					deposits, withdrawals, total_trades, total_volume, total_fees,
					breakdown_by_market, created_at
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
				ON CONFLICT (user_uid, exchange, timestamp)
				DO UPDATE SET
					total_equity = EXCLUDED.total_equity,
					realized_balance = EXCLUDED.realized_balance,
					unrealized_pnl = EXCLUDED.unrealized_pnl,
					deposits = EXCLUDED.deposits,
					withdrawals = EXCLUDED.withdrawals,
					total_trades = EXCLUDED.total_trades,
					total_volume = EXCLUDED.total_volume,
					total_fees = EXCLUDED.total_fees,
					breakdown_by_market = EXCLUDED.breakdown_by_market`,
				s.UserUID, s.Exchange, s.Timestamp,
				s.TotalEquity, s.RealizedBalance, s.UnrealizedPnL,
				s.Deposits, s.Withdrawals, s.TotalTrades, s.TotalVolume, s.TotalFees,
				breakdownJSON, s.CreatedAt,
			)
		}

		if err != nil {
			return fmt.Errorf("upsert snapshot %s/%s: %w", s.Exchange, s.Label, err)
		}
	}

	return tx.Commit(ctx)
}

func (r *SnapshotRepo) scanSnapshots(rows pgx.Rows, hasLabel bool) ([]*Snapshot, error) {
	var snapshots []*Snapshot

	for rows.Next() {
		var s Snapshot
		var breakdownJSON []byte

		scanArgs := []any{&s.ID, &s.UserUID, &s.Exchange}
		if hasLabel {
			scanArgs = append(scanArgs, &s.Label)
		}
		scanArgs = append(scanArgs,
			&s.Timestamp,
			&s.TotalEquity, &s.RealizedBalance, &s.UnrealizedPnL,
			&s.Deposits, &s.Withdrawals, &s.TotalTrades, &s.TotalVolume, &s.TotalFees,
			&breakdownJSON, &s.CreatedAt,
		)

		err := rows.Scan(scanArgs...)
		if err != nil {
			return nil, err
		}

		if len(breakdownJSON) > 0 {
			json.Unmarshal(breakdownJSON, &s.Breakdown)
		}

		snapshots = append(snapshots, &s)
	}

	return snapshots, rows.Err()
}

// scanSnapshotsTS scans rows from TS Prisma schema (camelCase columns, no total_trades/total_volume/total_fees).
// TS always has the label column. Go-only fields default to zero.
func (r *SnapshotRepo) scanSnapshotsTS(rows pgx.Rows) ([]*Snapshot, error) {
	var snapshots []*Snapshot

	for rows.Next() {
		var s Snapshot
		var breakdownJSON []byte

		err := rows.Scan(
			&s.ID, &s.UserUID, &s.Exchange, &s.Label, &s.Timestamp,
			&s.TotalEquity, &s.RealizedBalance, &s.UnrealizedPnL,
			&s.Deposits, &s.Withdrawals,
			&breakdownJSON, &s.CreatedAt,
		)
		if err != nil {
			return nil, err
		}

		// Go-only columns not present in TS schema — default to zero
		s.TotalTrades = 0
		s.TotalVolume = 0
		s.TotalFees = 0

		if len(breakdownJSON) > 0 {
			json.Unmarshal(breakdownJSON, &s.Breakdown)
		}

		snapshots = append(snapshots, &s)
	}

	return snapshots, rows.Err()
}

func (r *SnapshotRepo) hasLabelColumn(ctx context.Context) bool {
	r.capMu.Lock()
	defer r.capMu.Unlock()

	if r.capabilitiesLoaded {
		return r.hasLabelCol
	}

	// Detect TS Prisma schema (camelCase) vs Go schema (snake_case).
	// If "userUid" column exists in snapshot_data → TS schema.
	tsSchema, _ := r.columnExists(ctx, "snapshot_data", "userUid")
	r.isTSSchema = tsSchema

	if tsSchema {
		// TS Prisma always has the label column
		r.hasLabelCol = true
	} else {
		exists, err := r.columnExists(ctx, "snapshot_data", "label")
		if err != nil {
			r.hasLabelCol = false
		} else {
			r.hasLabelCol = exists
		}
	}

	r.capabilitiesLoaded = true
	return r.hasLabelCol
}

func (r *SnapshotRepo) columnExists(ctx context.Context, tableName, columnName string) (bool, error) {
	const query = `
		SELECT EXISTS (
			SELECT 1
			FROM information_schema.columns
			WHERE table_schema = 'public'
			  AND table_name = $1
			  AND column_name = $2
		)`

	var exists bool
	if err := r.pool.QueryRow(ctx, query, tableName, columnName).Scan(&exists); err != nil {
		return false, fmt.Errorf("check column %s.%s: %w", tableName, columnName, err)
	}
	return exists, nil
}
