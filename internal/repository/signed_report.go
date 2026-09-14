package repository

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// SignedReportRecord represents a cached signed report
type SignedReportRecord struct {
	ID             string          `json:"id"`
	ReportID       string          `json:"report_id"`
	UserUID        string          `json:"user_uid"`
	StartDate      time.Time       `json:"start_date"`
	EndDate        time.Time       `json:"end_date"`
	Benchmark      string          `json:"benchmark"`
	ReportData     json.RawMessage `json:"report_data"`
	Signature      string          `json:"signature"`
	ReportHash     string          `json:"report_hash"`
	EnclaveVersion string          `json:"enclave_version"`
	CreatedAt      time.Time       `json:"created_at"`
}

// SignedReportRepo handles signed report persistence. It speaks both column
// flavours of signed_reports — see signedReportSchema for why two exist.
type SignedReportRepo struct {
	pool       *pgxpool.Pool
	schemaOnce sync.Once
	schema     signedReportSchema
}

// NewSignedReportRepo creates a new signed report repository
func NewSignedReportRepo(pool *pgxpool.Pool) *SignedReportRepo {
	return &SignedReportRepo{pool: pool}
}

func (r *SignedReportRepo) cols(ctx context.Context) signedReportSchema {
	return detectSignedReportSchema(ctx, r.pool, &r.schemaOnce, &r.schema)
}

// scanReport keeps every read's Scan in step with selectList's column order.
func scanReport(row pgx.Row, report *SignedReportRecord) error {
	return row.Scan(
		&report.ID, &report.ReportID, &report.UserUID, &report.StartDate, &report.EndDate,
		&report.Benchmark, &report.ReportData, &report.Signature, &report.ReportHash,
		&report.EnclaveVersion, &report.CreatedAt,
	)
}

// Create inserts a new signed report
func (r *SignedReportRepo) Create(ctx context.Context, report *SignedReportRecord) error {
	schema := r.cols(ctx)

	args := []any{
		report.ReportID, report.UserUID, report.StartDate, report.EndDate,
		report.Benchmark, report.ReportData, report.Signature, report.ReportHash,
		report.EnclaveVersion,
	}
	if schema.generateID {
		// Prisma's @default(cuid()) is not a database default, so an insert
		// that leaves id out fails the NOT NULL constraint.
		args = append([]any{generateCUID()}, args...)
	}

	return r.pool.QueryRow(ctx, schema.insertQuery(), args...).Scan(&report.ID)
}

// GetCached retrieves a cached report by user + period + benchmark, produced
// by the given enclave version. Reports signed by an older engine stay in
// the table (they remain verifiable) but are never re-served: a metric fix
// must not keep echoing the pre-fix numbers for a cached period.
func (r *SignedReportRepo) GetCached(ctx context.Context, userUID string, startDate, endDate time.Time, benchmark, enclaveVersion string) (*SignedReportRecord, error) {
	var report SignedReportRecord
	err := scanReport(
		r.pool.QueryRow(ctx, r.cols(ctx).cachedQuery(), userUID, startDate, endDate, benchmark, enclaveVersion),
		&report,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &report, nil
}

// GetByReportID retrieves a report by its report ID
func (r *SignedReportRepo) GetByReportID(ctx context.Context, reportID string) (*SignedReportRecord, error) {
	var report SignedReportRecord
	err := scanReport(r.pool.QueryRow(ctx, r.cols(ctx).byReportIDQuery(), reportID), &report)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &report, nil
}

// ListByUser returns all reports for a user, newest first
func (r *SignedReportRepo) ListByUser(ctx context.Context, userUID string) ([]*SignedReportRecord, error) {
	rows, err := r.pool.Query(ctx, r.cols(ctx).listByUserQuery(), userUID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var reports []*SignedReportRecord
	for rows.Next() {
		var report SignedReportRecord
		if err := scanReport(rows, &report); err != nil {
			return nil, err
		}
		reports = append(reports, &report)
	}
	return reports, rows.Err()
}
