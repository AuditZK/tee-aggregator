package repository

import (
	"context"
	"encoding/json"
	"errors"
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

// SignedReportRepo handles signed report persistence
type SignedReportRepo struct {
	pool *pgxpool.Pool
}

// NewSignedReportRepo creates a new signed report repository
func NewSignedReportRepo(pool *pgxpool.Pool) *SignedReportRepo {
	return &SignedReportRepo{pool: pool}
}

// Create inserts a new signed report
func (r *SignedReportRepo) Create(ctx context.Context, report *SignedReportRecord) error {
	query := `
		INSERT INTO signed_reports (report_id, user_uid, start_date, end_date, benchmark, report_data, signature, report_hash, enclave_version, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW())
		ON CONFLICT (user_uid, start_date, end_date, benchmark)
		DO UPDATE SET
			report_id = EXCLUDED.report_id,
			report_data = EXCLUDED.report_data,
			signature = EXCLUDED.signature,
			report_hash = EXCLUDED.report_hash,
			enclave_version = EXCLUDED.enclave_version,
			created_at = NOW()
		RETURNING id`

	return r.pool.QueryRow(ctx, query,
		report.ReportID, report.UserUID, report.StartDate, report.EndDate,
		report.Benchmark, report.ReportData, report.Signature, report.ReportHash,
		report.EnclaveVersion,
	).Scan(&report.ID)
}

// GetCached retrieves a cached report by user + period + benchmark, produced
// by the given enclave version. Reports signed by an older engine stay in
// the table (they remain verifiable) but are never re-served: a metric fix
// must not keep echoing the pre-fix numbers for a cached period.
func (r *SignedReportRepo) GetCached(ctx context.Context, userUID string, startDate, endDate time.Time, benchmark, enclaveVersion string) (*SignedReportRecord, error) {
	query := `
		SELECT id, report_id, user_uid, start_date, end_date, benchmark, report_data, signature, report_hash, enclave_version, created_at
		FROM signed_reports
		WHERE user_uid = $1 AND start_date = $2 AND end_date = $3 AND benchmark = $4 AND enclave_version = $5`

	var report SignedReportRecord
	err := r.pool.QueryRow(ctx, query, userUID, startDate, endDate, benchmark, enclaveVersion).Scan(
		&report.ID, &report.ReportID, &report.UserUID, &report.StartDate, &report.EndDate,
		&report.Benchmark, &report.ReportData, &report.Signature, &report.ReportHash,
		&report.EnclaveVersion, &report.CreatedAt,
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
	query := `
		SELECT id, report_id, user_uid, start_date, end_date, benchmark, report_data, signature, report_hash, enclave_version, created_at
		FROM signed_reports
		WHERE report_id = $1`

	var report SignedReportRecord
	err := r.pool.QueryRow(ctx, query, reportID).Scan(
		&report.ID, &report.ReportID, &report.UserUID, &report.StartDate, &report.EndDate,
		&report.Benchmark, &report.ReportData, &report.Signature, &report.ReportHash,
		&report.EnclaveVersion, &report.CreatedAt,
	)
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
	query := `
		SELECT id, report_id, user_uid, start_date, end_date, benchmark, report_data, signature, report_hash, enclave_version, created_at
		FROM signed_reports
		WHERE user_uid = $1
		ORDER BY created_at DESC`

	rows, err := r.pool.Query(ctx, query, userUID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var reports []*SignedReportRecord
	for rows.Next() {
		var report SignedReportRecord
		if err := rows.Scan(
			&report.ID, &report.ReportID, &report.UserUID, &report.StartDate, &report.EndDate,
			&report.Benchmark, &report.ReportData, &report.Signature, &report.ReportHash,
			&report.EnclaveVersion, &report.CreatedAt,
		); err != nil {
			return nil, err
		}
		reports = append(reports, &report)
	}
	return reports, rows.Err()
}
