package repository

import (
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// Production runs the Prisma-shaped table. Every query below used to be
// written in snake_case only, so each one raised 42703 there and the caller
// read the error as "nothing cached" — a silent, permanent cache miss. These
// tests pin the column names per flavour, because nothing downstream fails
// loudly when they are wrong.

// snakeCaseColumns are the names that exist ONLY on the Go schema. Finding any
// of them in a TS query is the bug this file exists to catch.
var snakeCaseColumns = []string{
	"report_id", "user_uid", "start_date", "end_date",
	"report_data", "report_hash", "enclave_version", "created_at",
}

func TestSignedReportTSQueriesNeverUseSnakeCase(t *testing.T) {
	queries := map[string]string{
		"insert":     signedReportTSSchema.insertQuery(),
		"cached":     signedReportTSSchema.cachedQuery(),
		"byReportID": signedReportTSSchema.byReportIDQuery(),
		"listByUser": signedReportTSSchema.listByUserQuery(),
	}
	for name, q := range queries {
		for _, col := range snakeCaseColumns {
			if strings.Contains(q, col) {
				t.Errorf("%s query names the Go column %q on the TS schema:\n%s", name, col, q)
			}
		}
	}
}

func TestSignedReportTSQueriesQuoteCamelCase(t *testing.T) {
	// Unquoted camelCase folds to lower case in Postgres and misses the column,
	// so quoting is not cosmetic here.
	for _, col := range []string{`"reportId"`, `"userUid"`, `"startDate"`, `"endDate"`, `"reportData"`, `"reportHash"`, `"enclaveVersion"`, `"createdAt"`} {
		if !strings.Contains(signedReportTSSchema.insertQuery(), col) {
			t.Errorf("TS insert does not quote %s:\n%s", col, signedReportTSSchema.insertQuery())
		}
	}
}

func TestSignedReportSelectListMatchesScanOrder(t *testing.T) {
	// scanReport binds eleven destinations in this exact order; a column added
	// to one side and not the other is a runtime scan error on every read.
	if got, want := signedReportGoSchema.selectList(),
		"id, report_id, user_uid, start_date, end_date, benchmark, report_data, signature, report_hash, enclave_version, created_at"; got != want {
		t.Errorf("go select list = %q", got)
	}
	if got, want := signedReportTSSchema.selectList(),
		`id, "reportId", "userUid", "startDate", "endDate", benchmark, "reportData", signature, "reportHash", "enclaveVersion", "createdAt"`; got != want {
		t.Errorf("ts select list = %q", got)
	}
}

func TestSignedReportInsertSuppliesIDOnlyWhereTheDatabaseHasNoDefault(t *testing.T) {
	// Go schema: id UUID DEFAULT gen_random_uuid(), so the insert must leave
	// it out. TS schema: id TEXT with no default, so the insert must pass one
	// or hit a NOT NULL violation.
	goQuery := signedReportGoSchema.insertQuery()
	if strings.Contains(goQuery, "INSERT INTO signed_reports (id,") {
		t.Errorf("go insert supplies id although the column defaults:\n%s", goQuery)
	}
	if signedReportGoSchema.generateID {
		t.Error("go schema must not generate ids")
	}

	tsQuery := signedReportTSSchema.insertQuery()
	if !strings.Contains(tsQuery, "INSERT INTO signed_reports (id,") {
		t.Errorf("ts insert omits id although Prisma leaves the column without a default:\n%s", tsQuery)
	}
	if !signedReportTSSchema.generateID {
		t.Error("ts schema must generate ids")
	}
}

func TestSignedReportInsertPlaceholdersMatchArgCount(t *testing.T) {
	// Create binds nine values, ten on the TS schema once the client-side id is
	// prepended. A placeholder the caller never binds is a runtime error.
	for _, tc := range []struct {
		schema signedReportSchema
		want   int
	}{
		{signedReportGoSchema, 9},
		{signedReportTSSchema, 10},
	} {
		q := tc.schema.insertQuery()
		found := regexp.MustCompile(`\$\d+`).FindAllString(q, -1)
		seen := map[string]bool{}
		for _, p := range found {
			seen[p] = true
		}
		if len(seen) != tc.want {
			t.Errorf("%s insert uses %d distinct placeholders, want %d:\n%s", tc.schema.name, len(seen), tc.want, q)
		}
		if !strings.Contains(q, "$"+strconv.Itoa(tc.want)) {
			t.Errorf("%s insert never reaches $%d:\n%s", tc.schema.name, tc.want, q)
		}
	}
}

func TestSignedReportConflictTargetIsThePeriodKey(t *testing.T) {
	// Both flavours index (user, start, end, benchmark) and neither indexes the
	// enclave version, so the version must stay out of the conflict target: a
	// report re-signed by a newer engine replaces the stale row rather than
	// failing on a target no index backs.
	if got, want := conflictTarget(signedReportGoSchema.insertQuery()), "user_uid, start_date, end_date, benchmark"; got != want {
		t.Errorf("go conflict target = %q", got)
	}
	if got, want := conflictTarget(signedReportTSSchema.insertQuery()), `"userUid", "startDate", "endDate", benchmark`; got != want {
		t.Errorf("ts conflict target = %q", got)
	}
	for _, s := range []signedReportSchema{signedReportGoSchema, signedReportTSSchema} {
		if strings.Contains(conflictTarget(s.insertQuery()), "nclaveVersion") || strings.Contains(conflictTarget(s.insertQuery()), "enclave_version") {
			t.Errorf("%s conflict target includes the enclave version", s.name)
		}
	}
}

func conflictTarget(query string) string {
	m := regexp.MustCompile(`ON CONFLICT \(([^)]*)\)`).FindStringSubmatch(query)
	if len(m) != 2 {
		return ""
	}
	return m[1]
}
