package repository

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// QUAL-001: snapshot SELECT column lists, extracted to remove the 3-way
// duplications across GetByUserAndDateRange / GetLatestByUser /
// GetByUserExchangeAndDate. A typo in either constant now flips every
// query at once, instead of producing silent column-mismatch bugs.
const (
	snapshotColsBase      = "id, user_uid, exchange, timestamp"
	snapshotColsWithLabel = "id, user_uid, exchange, label, timestamp"

	// Suffix appended to SELECT/INSERT column lists when migration 013
	// (is_historical) has been applied. Centralized to avoid drift across
	// the dozen query builders in this file.
	snapshotIsHistoricalCol = ", is_historical"

	// Suffix appended to INSERT column lists when migration 015
	// (from_external_rebuilder) has been applied.
	snapshotFromExternalRebuilderCol = ", from_external_rebuilder"
)

// snapshotOptionalCols builds the trailing column / placeholder / ON CONFLICT
// fragments for the Go-only optional snapshot columns — is_historical
// (migration 013) and from_external_rebuilder (migration 015) — together with
// the matching args. baseArgCount is the number of positional args already
// bound; the optional columns take $(baseArgCount+1) onward. Keeping the
// placeholder numbers dynamic avoids the per-call-site hardcoding that made
// adding a second optional column error-prone.
func snapshotOptionalCols(s *Snapshot, hasHist, hasOrigin bool, baseArgCount int) (cols, placeholders, excluded string, extra []any) {
	n := baseArgCount
	if hasHist {
		n++
		cols += snapshotIsHistoricalCol
		placeholders += fmt.Sprintf(", $%d", n)
		excluded += ", is_historical = EXCLUDED.is_historical"
		extra = append(extra, s.IsHistorical)
	}
	if hasOrigin {
		n++
		cols += snapshotFromExternalRebuilderCol
		placeholders += fmt.Sprintf(", $%d", n)
		excluded += ", from_external_rebuilder = EXCLUDED.from_external_rebuilder"
		extra = append(extra, s.FromExternalRebuilder)
	}
	return
}

// generateCUID generates a CUID-like identifier compatible with Prisma's @id @default(cuid()).
func generateCUID() string {
	b := make([]byte, 12)
	rand.Read(b)
	return fmt.Sprintf("c%x%010x", time.Now().UnixMilli(), b)
}

// Snapshot represents a daily equity snapshot.
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

	// IsHistorical marks snapshots reconstructed from broker history (e.g.
	// IBKR Flex daily summaries: equity only, no per-trade detail). Live
	// snapshots from the realtime sync window are false. Persisted only on
	// the Go schema (TS Prisma schema does not have this column).
	IsHistorical bool `json:"is_historical,omitempty"`

	// FromExternalRebuilder marks a snapshot reconstructed by the out-of-enclave
	// history-rebuilder service (SEC-001). Such data is NOT covered by the
	// signed report — GetVerifiableByUserAndDateRange filters it out. False for
	// live snapshots and for IBKR Flex history rebuilt inside the enclave.
	// Persisted only on the Go schema (migration 015).
	FromExternalRebuilder bool `json:"from_external_rebuilder,omitempty"`
}

// MarketBreakdown holds metrics per market type.
//
// The "global" field is a TS-compat aggregate written by the TS enclave:
// it contains the totals across all markets (trades, volume, fees). When
// loading snapshots from a TS Prisma DB where top-level totals columns
// don't exist (total_trades, total_volume, total_fees), we recover them
// from breakdown.global — see scanSnapshotsTS.
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
	Global      *MarketMetrics `json:"global,omitempty"`
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

	capMu                       sync.Mutex
	capabilitiesLoaded          bool
	hasLabelCol                 bool
	hasIsHistoricalCol          bool // Go schema only; TS Prisma never has it
	hasFromExternalRebuilderCol bool // Go schema only (migration 015)
	isTSSchema                  bool // true = TS Prisma camelCase columns
}

// NewSnapshotRepo creates a new snapshot repository
func NewSnapshotRepo(pool *pgxpool.Pool) *SnapshotRepo {
	return &SnapshotRepo{pool: pool}
}

// Upsert creates or updates a snapshot for a user/exchange/date
func (r *SnapshotRepo) Upsert(ctx context.Context, s *Snapshot) error {
	breakdownJSON, _ := json.Marshal(s.Breakdown)
	hasLabel := r.hasLabelColumn(ctx)
	hasHist := r.hasIsHistoricalColumn(ctx)
	hasOrigin := r.hasFromExternalRebuilderColumn(ctx)

	if r.isTSSchema {
		return r.upsertTS(ctx, s, breakdownJSON)
	}

	if hasLabel {
		args := []any{
			s.UserUID, s.Exchange, s.Label, s.Timestamp,
			s.TotalEquity, s.RealizedBalance, s.UnrealizedPnL,
			s.Deposits, s.Withdrawals, s.TotalTrades, s.TotalVolume, s.TotalFees,
			breakdownJSON, time.Now().UTC(),
		}
		// Optional columns keep the path compatible with a DB where
		// migration 013 / 015 has not yet run.
		optCols, optPlaceholders, optExcluded, optArgs := snapshotOptionalCols(s, hasHist, hasOrigin, len(args))
		args = append(args, optArgs...)
		query := fmt.Sprintf(`
			INSERT INTO snapshot_data (
				user_uid, exchange, label, timestamp,
				total_equity, realized_balance, unrealized_pnl,
				deposits, withdrawals, total_trades, total_volume, total_fees,
				breakdown_by_market, created_at%s
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14%s)
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
				breakdown_by_market = EXCLUDED.breakdown_by_market%s
			RETURNING id`, optCols, optPlaceholders, optExcluded)

		return r.pool.QueryRow(ctx, query, args...).Scan(&s.ID)
	}

	args := []any{
		s.UserUID, s.Exchange, s.Timestamp,
		s.TotalEquity, s.RealizedBalance, s.UnrealizedPnL,
		s.Deposits, s.Withdrawals, s.TotalTrades, s.TotalVolume, s.TotalFees,
		breakdownJSON, time.Now().UTC(),
	}
	optCols, optPlaceholders, optExcluded, optArgs := snapshotOptionalCols(s, hasHist, hasOrigin, len(args))
	args = append(args, optArgs...)
	query := fmt.Sprintf(`
		INSERT INTO snapshot_data (
			user_uid, exchange, timestamp,
			total_equity, realized_balance, unrealized_pnl,
			deposits, withdrawals, total_trades, total_volume, total_fees,
			breakdown_by_market, created_at%s
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13%s)
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
			breakdown_by_market = EXCLUDED.breakdown_by_market%s
		RETURNING id`, optCols, optPlaceholders, optExcluded)

	return r.pool.QueryRow(ctx, query, args...).Scan(&s.ID)
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

// GetByUserAndDateRange returns snapshots for a user within a date range.
func (r *SnapshotRepo) GetByUserAndDateRange(ctx context.Context, userUID string, start, end time.Time) ([]*Snapshot, error) {
	return r.getByUserAndDateRange(ctx, userUID, start, end, false)
}

// GetVerifiableByUserAndDateRange is GetByUserAndDateRange restricted to
// snapshots produced inside the SEV-SNP perimeter — it excludes history
// reconstructed by the external rebuilder (SEC-001). The signed report is
// built from this set so its signature only covers enclave-verified data. On a
// TS/Prisma schema (no from_external_rebuilder column) it behaves exactly like
// GetByUserAndDateRange; the rebuilt-history feature targets the Go schema.
func (r *SnapshotRepo) GetVerifiableByUserAndDateRange(ctx context.Context, userUID string, start, end time.Time) ([]*Snapshot, error) {
	return r.getByUserAndDateRange(ctx, userUID, start, end, true)
}

func (r *SnapshotRepo) getByUserAndDateRange(ctx context.Context, userUID string, start, end time.Time, verifiableOnly bool) ([]*Snapshot, error) {
	hasLabel := r.hasLabelColumn(ctx)
	hasHist := r.hasIsHistoricalColumn(ctx)

	if r.isTSSchema {
		return r.getByUserAndDateRangeTS(ctx, userUID, start, end)
	}

	selectCols := snapshotColsBase
	if hasLabel {
		selectCols = snapshotColsWithLabel
	}
	histCol := ""
	if hasHist {
		histCol = snapshotIsHistoricalCol
	}
	whereExtra := ""
	if verifiableOnly && r.hasFromExternalRebuilderColumn(ctx) {
		whereExtra = " AND from_external_rebuilder = FALSE"
	}
	query := fmt.Sprintf(`
		SELECT %s,
			total_equity, realized_balance, unrealized_pnl,
			deposits, withdrawals, total_trades, total_volume, total_fees,
			breakdown_by_market, created_at%s
		FROM snapshot_data
		WHERE user_uid = $1 AND timestamp >= $2 AND timestamp <= $3%s
		ORDER BY timestamp`,
		selectCols, histCol, whereExtra,
	)

	rows, err := r.pool.Query(ctx, query, userUID, start, end)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return r.scanSnapshots(rows, hasLabel, hasHist)
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
	hasHist := r.hasIsHistoricalColumn(ctx)

	if r.isTSSchema {
		return r.getLatestByUserTS(ctx, userUID)
	}

	selectCols := snapshotColsBase
	if hasLabel {
		selectCols = snapshotColsWithLabel
	}
	histCol := ""
	if hasHist {
		histCol = snapshotIsHistoricalCol
	}
	query := fmt.Sprintf(`
		SELECT %s,
			total_equity, realized_balance, unrealized_pnl,
			deposits, withdrawals, total_trades, total_volume, total_fees,
			breakdown_by_market, created_at%s
		FROM snapshot_data
		WHERE user_uid = $1
		ORDER BY timestamp DESC
		LIMIT 1`,
		selectCols, histCol,
	)

	rows, err := r.pool.Query(ctx, query, userUID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	snapshots, err := r.scanSnapshots(rows, hasLabel, hasHist)
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
	hasHist := r.hasIsHistoricalColumn(ctx)

	if r.isTSSchema {
		return r.getByUserExchangeAndDateTS(ctx, userUID, exchange, date)
	}

	selectCols := snapshotColsBase
	if hasLabel {
		selectCols = snapshotColsWithLabel
	}
	whereClause := "WHERE user_uid = $1 AND exchange = $2 AND timestamp = $3"
	if hasLabel {
		whereClause = "WHERE user_uid = $1 AND exchange = $2 AND label = '' AND timestamp = $3"
	}
	histCol := ""
	if hasHist {
		histCol = snapshotIsHistoricalCol
	}
	query := fmt.Sprintf(`
		SELECT %s,
			total_equity, realized_balance, unrealized_pnl,
			deposits, withdrawals, total_trades, total_volume, total_fees,
			breakdown_by_market, created_at%s
		FROM snapshot_data
		%s`,
		selectCols, histCol, whereClause,
	)

	rows, err := r.pool.Query(ctx, query, userUID, exchange, date)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	snapshots, err := r.scanSnapshots(rows, hasLabel, hasHist)
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
	hasHist := r.hasIsHistoricalColumn(ctx)

	if r.isTSSchema {
		return r.getByUserExchangeLabelAndDateTS(ctx, userUID, exchange, label, date)
	}

	if !hasLabel {
		return r.GetByUserExchangeAndDate(ctx, userUID, exchange, date)
	}

	histCol := ""
	if hasHist {
		histCol = snapshotIsHistoricalCol
	}
	query := fmt.Sprintf(`
		SELECT id, user_uid, exchange, label, timestamp,
			total_equity, realized_balance, unrealized_pnl,
			deposits, withdrawals, total_trades, total_volume, total_fees,
			breakdown_by_market, created_at%s
		FROM snapshot_data
		WHERE user_uid = $1 AND exchange = $2 AND label = $3 AND timestamp = $4`, histCol)

	rows, err := r.pool.Query(ctx, query, userUID, exchange, label, date)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	snapshots, err := r.scanSnapshots(rows, true, hasHist)
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
	hasHist := r.hasIsHistoricalColumn(ctx)

	if r.isTSSchema {
		return r.getLatestByUserExchangeLabelTS(ctx, userUID, exchange, label)
	}

	histCol := ""
	if hasHist {
		histCol = snapshotIsHistoricalCol
	}

	if !hasLabel {
		query := fmt.Sprintf(`
			SELECT id, user_uid, exchange, timestamp,
				total_equity, realized_balance, unrealized_pnl,
				deposits, withdrawals, total_trades, total_volume, total_fees,
				breakdown_by_market, created_at%s
			FROM snapshot_data
			WHERE user_uid = $1 AND exchange = $2
			ORDER BY timestamp DESC
			LIMIT 1`, histCol)

		rows, err := r.pool.Query(ctx, query, userUID, exchange)
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		snapshots, err := r.scanSnapshots(rows, false, hasHist)
		if err != nil {
			return nil, err
		}
		if len(snapshots) == 0 {
			return nil, ErrNotFound
		}
		return snapshots[0], nil
	}

	query := fmt.Sprintf(`
		SELECT id, user_uid, exchange, label, timestamp,
			total_equity, realized_balance, unrealized_pnl,
			deposits, withdrawals, total_trades, total_volume, total_fees,
			breakdown_by_market, created_at%s
		FROM snapshot_data
		WHERE user_uid = $1 AND exchange = $2 AND label = $3
		ORDER BY timestamp DESC
		LIMIT 1`, histCol)

	rows, err := r.pool.Query(ctx, query, userUID, exchange, label)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	snapshots, err := r.scanSnapshots(rows, true, hasHist)
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

// ExistsForUserExchangeLabel returns true if any snapshot already exists for
// the given (user_uid, exchange, label) tuple. Used by the anti-cherry-pick
// guard (ENG-001) — replaces the old full-range scan that was O(all user
// snapshots) and fail-open on DB errors. Returns an error on DB failure so
// the caller can fail closed.
func (r *SnapshotRepo) ExistsForUserExchangeLabel(ctx context.Context, userUID, exchange, label string) (bool, error) {
	hasLabel := r.hasLabelColumn(ctx)

	var query string
	var args []any
	switch {
	case r.isTSSchema:
		query = `SELECT 1 FROM snapshot_data WHERE "userUid" = $1 AND exchange = $2 AND label = $3 LIMIT 1`
		args = []any{userUID, exchange, label}
	case !hasLabel:
		query = `SELECT 1 FROM snapshot_data WHERE user_uid = $1 AND exchange = $2 LIMIT 1`
		args = []any{userUID, exchange}
	default:
		query = `SELECT 1 FROM snapshot_data WHERE user_uid = $1 AND exchange = $2 AND label = $3 LIMIT 1`
		args = []any{userUID, exchange, label}
	}

	var one int
	err := r.pool.QueryRow(ctx, query, args...).Scan(&one)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// GetEarliestTimestamp returns the oldest snapshot timestamp for a
// (user, exchange, label) tuple. Used by the IBKR Flex sync to detect when
// the broker's history has been extended retroactively (e.g. user widened
// their Flex query window) so we can log/flag the reconstruction. Returns
// (zero time, ErrNotFound) when no snapshot exists for the tuple yet —
// callers treat this as "first sync, no prior data".
func (r *SnapshotRepo) GetEarliestTimestamp(ctx context.Context, userUID, exchange, label string) (time.Time, error) {
	hasLabel := r.hasLabelColumn(ctx)

	var query string
	var args []any
	switch {
	case r.isTSSchema:
		query = `SELECT MIN(timestamp) FROM snapshot_data WHERE "userUid" = $1 AND exchange = $2 AND label = $3`
		args = []any{userUID, exchange, label}
	case !hasLabel:
		query = `SELECT MIN(timestamp) FROM snapshot_data WHERE user_uid = $1 AND exchange = $2`
		args = []any{userUID, exchange}
	default:
		query = `SELECT MIN(timestamp) FROM snapshot_data WHERE user_uid = $1 AND exchange = $2 AND label = $3`
		args = []any{userUID, exchange, label}
	}

	var earliest *time.Time
	if err := r.pool.QueryRow(ctx, query, args...).Scan(&earliest); err != nil {
		return time.Time{}, err
	}
	if earliest == nil {
		return time.Time{}, ErrNotFound
	}
	return earliest.UTC(), nil
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
	hasHist := r.hasIsHistoricalColumn(ctx)
	hasOrigin := r.hasFromExternalRebuilderColumn(ctx)

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
			args := []any{
				s.UserUID, s.Exchange, s.Label, s.Timestamp,
				s.TotalEquity, s.RealizedBalance, s.UnrealizedPnL,
				s.Deposits, s.Withdrawals, s.TotalTrades, s.TotalVolume, s.TotalFees,
				breakdownJSON, time.Now().UTC(),
			}
			optCols, optPlaceholders, optExcluded, optArgs := snapshotOptionalCols(s, hasHist, hasOrigin, len(args))
			args = append(args, optArgs...)
			_, err = tx.Exec(ctx, fmt.Sprintf(`
				INSERT INTO snapshot_data (
					user_uid, exchange, label, timestamp,
					total_equity, realized_balance, unrealized_pnl,
					deposits, withdrawals, total_trades, total_volume, total_fees,
					breakdown_by_market, created_at%s
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14%s)
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
					breakdown_by_market = EXCLUDED.breakdown_by_market%s`,
				optCols, optPlaceholders, optExcluded), args...,
			)
		} else {
			args := []any{
				s.UserUID, s.Exchange, s.Timestamp,
				s.TotalEquity, s.RealizedBalance, s.UnrealizedPnL,
				s.Deposits, s.Withdrawals, s.TotalTrades, s.TotalVolume, s.TotalFees,
				breakdownJSON, time.Now().UTC(),
			}
			optCols, optPlaceholders, optExcluded, optArgs := snapshotOptionalCols(s, hasHist, hasOrigin, len(args))
			args = append(args, optArgs...)
			_, err = tx.Exec(ctx, fmt.Sprintf(`
				INSERT INTO snapshot_data (
					user_uid, exchange, timestamp,
					total_equity, realized_balance, unrealized_pnl,
					deposits, withdrawals, total_trades, total_volume, total_fees,
					breakdown_by_market, created_at%s
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13%s)
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
					breakdown_by_market = EXCLUDED.breakdown_by_market%s`,
				optCols, optPlaceholders, optExcluded), args...,
			)
		}

		if err != nil {
			return fmt.Errorf("upsert snapshot %s/%s: %w", s.Exchange, s.Label, err)
		}
	}

	return tx.Commit(ctx)
}

func (r *SnapshotRepo) scanSnapshots(rows pgx.Rows, hasLabel, hasHist bool) ([]*Snapshot, error) {
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
		if hasHist {
			scanArgs = append(scanArgs, &s.IsHistorical)
		}

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

// scanSnapshotsTS scans rows from TS Prisma schema (camelCase columns).
//
// The TS schema does not have top-level total_trades/total_volume/total_fees
// columns — those aggregates live inside the breakdown_by_market JSONB under
// the "global" key, which is how the TS enclave writes them. To keep parity
// with the TS GetAggregatedMetrics response, we unmarshal the breakdown and
// lift breakdown.global.* into the top-level Snapshot fields.
//
// TS always has the label column. If breakdown.global is missing (very old
// rows predating the global aggregate), the totals fall back to zero, which
// matches TS behaviour for those same rows.
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

		// Default to zero; will be overwritten from breakdown.global below
		// if present.
		s.TotalTrades = 0
		s.TotalVolume = 0
		s.TotalFees = 0

		if len(breakdownJSON) > 0 {
			if err := json.Unmarshal(breakdownJSON, &s.Breakdown); err == nil && s.Breakdown != nil && s.Breakdown.Global != nil {
				g := s.Breakdown.Global
				s.TotalTrades = g.Trades
				s.TotalVolume = g.Volume
				s.TotalFees = g.TradingFees + g.FundingFees
			}
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
		// TS Prisma never has is_historical / from_external_rebuilder
		// (Go-only columns from migrations 013 / 015).
		r.hasIsHistoricalCol = false
		r.hasFromExternalRebuilderCol = false
	} else {
		exists, err := r.columnExists(ctx, "snapshot_data", "label")
		if err != nil {
			r.hasLabelCol = false
		} else {
			r.hasLabelCol = exists
		}
		histExists, err := r.columnExists(ctx, "snapshot_data", "is_historical")
		if err != nil {
			r.hasIsHistoricalCol = false
		} else {
			r.hasIsHistoricalCol = histExists
		}
		originExists, err := r.columnExists(ctx, "snapshot_data", "from_external_rebuilder")
		if err != nil {
			r.hasFromExternalRebuilderCol = false
		} else {
			r.hasFromExternalRebuilderCol = originExists
		}
	}

	r.capabilitiesLoaded = true
	return r.hasLabelCol
}

// hasIsHistoricalColumn reports whether migration 013 has been applied. The
// detection is gated through hasLabelColumn() to share the capability cache.
func (r *SnapshotRepo) hasIsHistoricalColumn(ctx context.Context) bool {
	r.hasLabelColumn(ctx) // primes capabilitiesLoaded
	r.capMu.Lock()
	defer r.capMu.Unlock()
	return r.hasIsHistoricalCol
}

// hasFromExternalRebuilderColumn reports whether migration 015 has been
// applied. Detection is gated through hasLabelColumn() to share the cache.
func (r *SnapshotRepo) hasFromExternalRebuilderColumn(ctx context.Context) bool {
	r.hasLabelColumn(ctx) // primes capabilitiesLoaded
	r.capMu.Lock()
	defer r.capMu.Unlock()
	return r.hasFromExternalRebuilderCol
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
