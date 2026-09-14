package repository

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Two shapes of signed_reports exist in the wild, the same split that already
// forced dual paths in SnapshotRepo, ConnectionRepo and SyncRateLimitLogRepo:
//
//   - Go (migration 006): snake_case columns, id UUID with a database default,
//     unique index on (user_uid, start_date, end_date, benchmark).
//   - TS/Prisma (production): camelCase columns, id TEXT with NO database
//     default (Prisma mints a cuid client-side), unique index on
//     ("userUid", "startDate", "endDate", benchmark).
//
// The repository spoke only snake_case, so on production every INSERT and
// every SELECT failed with 42703 and the caller swallowed the error as a
// cache miss: reports still came out correct, each one re-signed from
// scratch, and the table has not gained a row since 2026-04-16. The
// freshness gate added in #16 could never fire either, having nothing to
// gate. Nothing in the output was wrong, which is exactly why it went
// unnoticed for five months.
type signedReportSchema struct {
	// name is what the flavour is called in logs and test failures.
	name string
	// generateID is true when the table's id has no database default and the
	// application must supply one (Prisma's @default(cuid())).
	generateID bool

	id             string
	reportID       string
	userUID        string
	startDate      string
	endDate        string
	benchmark      string
	reportData     string
	signature      string
	reportHash     string
	enclaveVersion string
	createdAt      string
}

var signedReportGoSchema = signedReportSchema{
	name:           "go",
	generateID:     false,
	id:             "id",
	reportID:       "report_id",
	userUID:        "user_uid",
	startDate:      "start_date",
	endDate:        "end_date",
	benchmark:      "benchmark",
	reportData:     "report_data",
	signature:      "signature",
	reportHash:     "report_hash",
	enclaveVersion: "enclave_version",
	createdAt:      "created_at",
}

var signedReportTSSchema = signedReportSchema{
	name:           "ts",
	generateID:     true,
	id:             "id",
	reportID:       `"reportId"`,
	userUID:        `"userUid"`,
	startDate:      `"startDate"`,
	endDate:        `"endDate"`,
	benchmark:      "benchmark",
	reportData:     `"reportData"`,
	signature:      "signature",
	reportHash:     `"reportHash"`,
	enclaveVersion: `"enclaveVersion"`,
	createdAt:      `"createdAt"`,
}

// selectList is the column order every Scan in this file depends on. Reads and
// scans must move together, so both sides come from here.
func (s signedReportSchema) selectList() string {
	return strings.Join([]string{
		s.id, s.reportID, s.userUID, s.startDate, s.endDate,
		s.benchmark, s.reportData, s.signature, s.reportHash,
		s.enclaveVersion, s.createdAt,
	}, ", ")
}

// insertQuery upserts on the period key. The conflict target is the four
// columns both flavours index — the enclave version is deliberately not part
// of it, so a report re-signed by a newer engine REPLACES the stale row
// instead of accumulating one row per version; GetCached then filters on the
// version to avoid serving what an older engine produced.
func (s signedReportSchema) insertQuery() string {
	cols := []string{
		s.reportID, s.userUID, s.startDate, s.endDate, s.benchmark,
		s.reportData, s.signature, s.reportHash, s.enclaveVersion,
	}
	if s.generateID {
		cols = append([]string{s.id}, cols...)
	}
	placeholders := make([]string, len(cols))
	for i := range cols {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}

	return fmt.Sprintf(`
		INSERT INTO signed_reports (%s, %s)
		VALUES (%s, NOW())
		ON CONFLICT (%s, %s, %s, %s)
		DO UPDATE SET
			%s = EXCLUDED.%s,
			%s = EXCLUDED.%s,
			%s = EXCLUDED.%s,
			%s = EXCLUDED.%s,
			%s = EXCLUDED.%s,
			%s = NOW()
		RETURNING id`,
		strings.Join(cols, ", "), s.createdAt,
		strings.Join(placeholders, ", "),
		s.userUID, s.startDate, s.endDate, s.benchmark,
		s.reportID, s.reportID,
		s.reportData, s.reportData,
		s.signature, s.signature,
		s.reportHash, s.reportHash,
		s.enclaveVersion, s.enclaveVersion,
		s.createdAt,
	)
}

func (s signedReportSchema) cachedQuery() string {
	return fmt.Sprintf(`
		SELECT %s
		FROM signed_reports
		WHERE %s = $1 AND %s = $2 AND %s = $3 AND %s = $4 AND %s = $5`,
		s.selectList(), s.userUID, s.startDate, s.endDate, s.benchmark, s.enclaveVersion)
}

func (s signedReportSchema) byReportIDQuery() string {
	return fmt.Sprintf(`
		SELECT %s
		FROM signed_reports
		WHERE %s = $1`, s.selectList(), s.reportID)
}

func (s signedReportSchema) listByUserQuery() string {
	return fmt.Sprintf(`
		SELECT %s
		FROM signed_reports
		WHERE %s = $1
		ORDER BY %s DESC`, s.selectList(), s.userUID, s.createdAt)
}

// detectSignedReportSchema asks the database which flavour it is. The probe
// runs once per process; a probe that errors (no table yet, no connection)
// answers "go", which is what a freshly migrated database is — and a wrong
// answer costs a swallowed cache miss, never a wrong report.
func detectSignedReportSchema(ctx context.Context, pool *pgxpool.Pool, once *sync.Once, dst *signedReportSchema) signedReportSchema {
	once.Do(func() {
		*dst = signedReportGoSchema
		if pool == nil {
			return
		}
		var isTS bool
		err := pool.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM information_schema.columns
				WHERE table_schema = 'public'
				  AND table_name = 'signed_reports'
				  AND column_name = 'userUid'
			)`).Scan(&isTS)
		if err == nil && isTS {
			*dst = signedReportTSSchema
		}
	})
	return *dst
}
