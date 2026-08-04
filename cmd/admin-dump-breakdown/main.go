// Admin tool: dump the stored per-asset-class breakdown_by_market for one
// user's snapshots over a date range. Read-only — a plain SELECT on
// snapshot_data, no broker call, no credential decryption, no writes.
//
// Use case: a daily equity swing looks implausible and we need to know which
// asset class (stocks / options / futures / cash) carried it, without pulling
// a fresh Flex statement (which would consume the connection's rate-limited
// daily slot).
//
// Usage (inside the enclave container, which has DATABASE_URL in its env):
//
//	docker exec enclave_go_prod /tmp/admin-dump-breakdown \
//	    -user-uid="e8c9c56a-d9fd-5ec2-973e-f217beca81fe" \
//	    -exchange="ibkr" \
//	    -from="2026-07-28" -to="2026-08-01"
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// databaseURL returns DATABASE_URL from the environment, falling back to the
// host env files when run outside the container. The value is used in-process
// only and never printed.
func databaseURL() string {
	if v := os.Getenv("DATABASE_URL"); v != "" {
		return v
	}
	for _, p := range []string{
		"/home/mouchlachjimmy/prod.env",
		"/home/mouchlachjimmy/.env-enclave",
	} {
		f, err := os.Open(p)
		if err != nil {
			continue
		}
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			line := strings.TrimSpace(sc.Text())
			if v, ok := strings.CutPrefix(line, "DATABASE_URL="); ok {
				f.Close()
				return strings.Trim(strings.TrimSpace(v), `"'`)
			}
		}
		f.Close()
	}
	return ""
}

// weekendCandidatesSQL selects the weekend-dated snapshot rows the backfill
// can never correct (statements only contain trading days, so a row dated
// Saturday/Sunday keeps whatever the stale midnight sync wrote — the permanent
// "phantom" population from the 2026-08-01 audit) together with the value they
// should carry: the last statement-confirmed (backfilled) trading-day close
// before them. Rows already equal to that close are excluded (idempotent).
const weekendCandidatesSQL = `
	SELECT s."userUid", s.label, s.timestamp::date,
	       s."totalEquity", prev."totalEquity", prev.day
	FROM snapshot_data s
	JOIN LATERAL (
		SELECT p.timestamp::date AS day, p."totalEquity", p."realizedBalance", p."unrealizedPnL"
		FROM snapshot_data p
		WHERE p."userUid" = s."userUid" AND p.exchange = s.exchange AND p.label = s.label
		  AND p.timestamp < s.timestamp
		  AND extract(dow FROM p.timestamp) NOT IN (0, 6)
		  AND p."createdAt" <> p."updatedAt"
		  AND s.timestamp - p.timestamp <= interval '3 days'
		ORDER BY p.timestamp DESC LIMIT 1
	) prev ON true
	WHERE s.exchange = $1
	  AND extract(dow FROM s.timestamp) IN (0, 6)
	  AND s."createdAt" = s."updatedAt"
	  AND s."totalEquity" <> prev."totalEquity"
	ORDER BY s."userUid", s.label, s.timestamp`

// repairWeekendsSQL applies the same selection as weekendCandidatesSQL and
// overwrites equity, realized balance and unrealized PnL with the previous
// confirmed trading-day values — the true weekend state is a carry-forward of
// that close. updatedAt is bumped so the row no longer reads as "never
// backfilled". Single statement, ctid-matched, so selection and write cannot
// drift apart.
const repairWeekendsSQL = `
	WITH cand AS (
		SELECT s.ctid AS row_ctid,
		       prev."totalEquity" AS new_eq,
		       prev."realizedBalance" AS new_rb,
		       prev."unrealizedPnL" AS new_upnl
		FROM snapshot_data s
		JOIN LATERAL (
			SELECT p."totalEquity", p."realizedBalance", p."unrealizedPnL"
			FROM snapshot_data p
			WHERE p."userUid" = s."userUid" AND p.exchange = s.exchange AND p.label = s.label
			  AND p.timestamp < s.timestamp
			  AND extract(dow FROM p.timestamp) NOT IN (0, 6)
			  AND p."createdAt" <> p."updatedAt"
			  AND s.timestamp - p.timestamp <= interval '3 days'
			ORDER BY p.timestamp DESC LIMIT 1
		) prev ON true
		WHERE s.exchange = $1
		  AND extract(dow FROM s.timestamp) IN (0, 6)
		  AND s."createdAt" = s."updatedAt"
		  AND s."totalEquity" <> prev."totalEquity"
	)
	UPDATE snapshot_data t
	SET "totalEquity" = c.new_eq,
	    "realizedBalance" = c.new_rb,
	    "unrealizedPnL" = c.new_upnl,
	    "updatedAt" = NOW()
	FROM cand c
	WHERE t.ctid = c.row_ctid`

// repairWeekends lists (dry-run) or rewrites (apply) the permanent weekend
// phantoms for one exchange. The dry-run output doubles as the undo record:
// it prints every old value before anything is written.
func repairWeekends(exchange string, apply bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, databaseURL())
	if err != nil {
		fmt.Fprintf(os.Stderr, "db connect: %v\n", err)
		os.Exit(1)
	}
	defer pool.Close()

	rows, err := pool.Query(ctx, weekendCandidatesSQL, exchange)
	if err != nil {
		fmt.Fprintf(os.Stderr, "query candidates: %v\n", err)
		os.Exit(1)
	}
	count := 0
	for rows.Next() {
		var user, label string
		var day, srcDay time.Time
		var oldEq, newEq float64
		if err := rows.Scan(&user, &label, &day, &oldEq, &newEq, &srcDay); err != nil {
			rows.Close()
			fmt.Fprintf(os.Stderr, "scan: %v\n", err)
			os.Exit(1)
		}
		count++
		fmt.Printf("%s\t%s|%s\t%s\told=%.2f\tnew=%.2f\t(source %s)\n",
			day.Format("2006-01-02"), exchange, label, user, oldEq, newEq, srcDay.Format("2006-01-02"))
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "rows: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("candidates: %d\n", count)

	if !apply {
		fmt.Println("dry-run only — re-run with -apply to write")
		return
	}
	tag, err := pool.Exec(ctx, repairWeekendsSQL, exchange)
	if err != nil {
		fmt.Fprintf(os.Stderr, "repair update: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("updated: %d rows\n", tag.RowsAffected())
}

// dumpSyncStatusEnum prints the members of the legacy Postgres enum backing
// sync_statuses.status (SyncStatusEnum — absent from every current Prisma
// schema, a leftover DB type) plus the actual column type. Read-only; used to
// decide how to persist the "skipped_stale" status without breaking readers.
func dumpSyncStatusEnum() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, databaseURL())
	if err != nil {
		fmt.Fprintf(os.Stderr, "db connect: %v\n", err)
		os.Exit(1)
	}
	defer pool.Close()

	rows, err := pool.Query(ctx, `
		SELECT t.typname, e.enumlabel
		FROM pg_type t JOIN pg_enum e ON e.enumtypid = t.oid
		WHERE t.typname ILIKE '%syncstatus%'
		ORDER BY t.typname, e.enumsortorder`)
	if err != nil {
		fmt.Fprintf(os.Stderr, "enum query: %v\n", err)
		os.Exit(1)
	}
	for rows.Next() {
		var typ, label string
		if err := rows.Scan(&typ, &label); err != nil {
			fmt.Fprintf(os.Stderr, "scan: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("enum\t%s\t%s\n", typ, label)
	}
	rows.Close()

	rows, err = pool.Query(ctx, `
		SELECT column_name, data_type, udt_name
		FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = 'sync_statuses'
		ORDER BY ordinal_position`)
	if err != nil {
		fmt.Fprintf(os.Stderr, "columns query: %v\n", err)
		os.Exit(1)
	}
	defer rows.Close()
	for rows.Next() {
		var col, dt, udt string
		if err := rows.Scan(&col, &dt, &udt); err != nil {
			fmt.Fprintf(os.Stderr, "scan: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("col\t%s\t%s\t%s\n", col, dt, udt)
	}
}

// listTables prints the public tables of the connected database — read-only,
// used to locate where (if anywhere) per-trade rows with symbols live.
func listTables() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, databaseURL())
	if err != nil {
		fmt.Fprintf(os.Stderr, "db connect: %v\n", err)
		os.Exit(1)
	}
	defer pool.Close()
	rows, err := pool.Query(ctx, `
		SELECT table_name FROM information_schema.tables
		WHERE table_schema = 'public' ORDER BY table_name`)
	if err != nil {
		fmt.Fprintf(os.Stderr, "query: %v\n", err)
		os.Exit(1)
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			fmt.Fprintf(os.Stderr, "scan: %v\n", err)
			os.Exit(1)
		}
		fmt.Println(name)
	}
}

func main() {
	userUID := flag.String("user-uid", "", "user UID (required)")
	exchange := flag.String("exchange", "ibkr", "exchange filter")
	from := flag.String("from", "", "inclusive start date YYYY-MM-DD (required)")
	to := flag.String("to", "", "exclusive end date YYYY-MM-DD (required)")
	mode := flag.String("mode", "breakdown", "breakdown | tables | repair-weekends")
	apply := flag.Bool("apply", false, "repair-weekends only: actually write; default is dry-run")
	flag.Parse()

	if *mode == "tables" {
		listTables()
		return
	}
	if *mode == "repair-weekends" {
		repairWeekends(*exchange, *apply)
		return
	}
	if *mode == "sync-status-enum" {
		dumpSyncStatusEnum()
		return
	}

	if *userUID == "" || *from == "" || *to == "" {
		flag.Usage()
		os.Exit(2)
	}
	if _, err := time.Parse("2006-01-02", *from); err != nil {
		fmt.Fprintf(os.Stderr, "bad -from: %v\n", err)
		os.Exit(2)
	}
	if _, err := time.Parse("2006-01-02", *to); err != nil {
		fmt.Fprintf(os.Stderr, "bad -to: %v\n", err)
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, databaseURL())
	if err != nil {
		fmt.Fprintf(os.Stderr, "db connect: %v\n", err)
		os.Exit(1)
	}
	defer pool.Close()

	rows, err := pool.Query(ctx, `
		SELECT timestamp::date, label, "totalEquity", "realizedBalance",
		       COALESCE(breakdown_by_market::text, '{}')
		FROM snapshot_data
		WHERE "userUid" = $1 AND exchange = $2
		  AND timestamp >= $3::date AND timestamp < $4::date
		ORDER BY timestamp, label`,
		*userUID, *exchange, *from, *to)
	if err != nil {
		fmt.Fprintf(os.Stderr, "query: %v\n", err)
		os.Exit(1)
	}
	defer rows.Close()

	for rows.Next() {
		var day time.Time
		var label, breakdown string
		var equity, realized float64
		if err := rows.Scan(&day, &label, &equity, &realized, &breakdown); err != nil {
			fmt.Fprintf(os.Stderr, "scan: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("%s\t%s\tequity=%.2f\trealized=%.2f\t%s\n",
			day.Format("2006-01-02"), label, equity, realized, breakdown)
	}
	if err := rows.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "rows: %v\n", err)
		os.Exit(1)
	}
}
