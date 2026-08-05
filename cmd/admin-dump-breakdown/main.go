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

// clearRebuildOptin nulls the durable rebuild consent (rebuild_requested_at /
// "rebuildRequestedAt", migration 018) on one connection, taking it out of the
// midnight recalibration pass. Built for the 2026-08-05 case: the binance
// rebuilder is non-deterministic and wrong for this account (two runs, two
// incompatible histories, neither near the live value), its rebuild never
// finalizes, so the nightly pass rewrote the purged history — including
// overwriting the live row — every night. Clearing consent stops the loop
// until the rebuilder is fixed; re-opting in later = re-set the timestamp or
// reconnect with rebuild_history=true.
func clearRebuildOptin(userUID, exchange, label string, apply bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, databaseURL())
	if err != nil {
		fmt.Fprintf(os.Stderr, "db connect: %v\n", err)
		os.Exit(1)
	}
	defer pool.Close()

	col := ""
	for _, cand := range []string{"rebuildRequestedAt", "rebuild_requested_at"} {
		var exists bool
		pool.QueryRow(ctx, `
			SELECT EXISTS (SELECT 1 FROM information_schema.columns
			WHERE table_schema='public' AND table_name='exchange_connections' AND column_name=$1)`,
			cand).Scan(&exists)
		if exists {
			col = cand
			break
		}
	}
	if col == "" {
		fmt.Fprintln(os.Stderr, "colonne rebuild_requested_at absente — rien à débrancher")
		os.Exit(1)
	}

	var id string
	var val *time.Time
	err = pool.QueryRow(ctx,
		`SELECT id, "`+col+`" FROM exchange_connections WHERE "userUid"=$1 AND exchange=$2 AND label=$3`,
		userUID, exchange, label).Scan(&id, &val)
	if err != nil {
		fmt.Fprintf(os.Stderr, "lookup: %v\n", err)
		os.Exit(1)
	}
	cur := "NULL"
	if val != nil {
		cur = val.Format("2006-01-02 15:04:05")
	}
	fmt.Printf("connexion %s|%s|%s (id %s) : %s = %s\n", exchange, label, userUID, id, col, cur)
	if val == nil {
		fmt.Println("déjà débranchée — rien à faire")
		return
	}
	if !apply {
		fmt.Println("dry-run only — re-run with -apply to clear")
		return
	}
	tag, err := pool.Exec(ctx,
		`UPDATE exchange_connections SET "`+col+`" = NULL WHERE id = $1`, id)
	if err != nil {
		fmt.Fprintf(os.Stderr, "update: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("cleared: %d row\n", tag.RowsAffected())
}

// purgeHistory deletes every snapshot row of one connection strictly BEFORE
// the cutoff date. Built for the 2026-08-04 case: a binance rebuild produced a
// 90-day history whose level is ~29x below the live account value (partial
// wallet coverage in the external rebuilder), fabricating a +2774% "day" the
// moment the first live snapshot landed. Purging the reconstructed rows lets
// the track record start honestly at the first live figure; the history can
// be rebuilt after the rebuilder is fixed. Dry-run by default.
func purgeHistory(userUID, exchange, label, before string, apply bool) {
	if _, err := time.Parse("2006-01-02", before); err != nil {
		fmt.Fprintf(os.Stderr, "bad -to: %v\n", err)
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, databaseURL())
	if err != nil {
		fmt.Fprintf(os.Stderr, "db connect: %v\n", err)
		os.Exit(1)
	}
	defer pool.Close()

	var n int
	var minDay, maxDay *time.Time
	err = pool.QueryRow(ctx, `
		SELECT count(*), min(timestamp), max(timestamp)
		FROM snapshot_data
		WHERE "userUid" = $1 AND exchange = $2 AND label = $3 AND timestamp < $4::date`,
		userUID, exchange, label, before).Scan(&n, &minDay, &maxDay)
	if err != nil {
		fmt.Fprintf(os.Stderr, "count query: %v\n", err)
		os.Exit(1)
	}
	if n == 0 {
		fmt.Println("rien à purger (0 ligne avant la date de coupure)")
		return
	}
	fmt.Printf("purge %s|%s|%s : %d lignes, du %s au %s (coupure exclusive %s)\n",
		exchange, label, userUID, n, minDay.Format("2006-01-02"), maxDay.Format("2006-01-02"), before)

	if !apply {
		fmt.Println("dry-run only — re-run with -apply to delete")
		return
	}
	tag, err := pool.Exec(ctx, `
		DELETE FROM snapshot_data
		WHERE "userUid" = $1 AND exchange = $2 AND label = $3 AND timestamp < $4::date`,
		userUID, exchange, label, before)
	if err != nil {
		fmt.Fprintf(os.Stderr, "delete: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("deleted: %d rows\n", tag.RowsAffected())
}

// dumpLatestUsers prints the three most recently created users with their
// broker connections, sync status and snapshot count — read-only, answers
// "did the newest signup actually connect a broker and did it sync".
func dumpLatestUsers() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, databaseURL())
	if err != nil {
		fmt.Fprintf(os.Stderr, "db connect: %v\n", err)
		os.Exit(1)
	}
	defer pool.Close()

	rows, err := pool.Query(ctx, `
		SELECT uid, "createdAt",
		       CASE WHEN "platformHash" IS NULL THEN 'platformHash=NULL'
		            ELSE 'platformHash=set' END
		FROM users ORDER BY "createdAt" DESC LIMIT 3`)
	if err != nil {
		fmt.Fprintf(os.Stderr, "users query: %v\n", err)
		os.Exit(1)
	}
	type u struct {
		uid     string
		created time.Time
		ph      string
	}
	var users []u
	for rows.Next() {
		var x u
		if err := rows.Scan(&x.uid, &x.created, &x.ph); err != nil {
			rows.Close()
			fmt.Fprintf(os.Stderr, "scan: %v\n", err)
			os.Exit(1)
		}
		users = append(users, x)
	}
	rows.Close()

	for _, x := range users {
		fmt.Printf("user\t%s\tcreated %s\t%s\n", x.uid, x.created.Format("2006-01-02 15:04"), x.ph)
		crows, err := pool.Query(ctx, `
			SELECT c.exchange, c.label, c."isActive", c."createdAt",
			       COALESCE(s.status::text, '-'),
			       COALESCE(s."lastSyncTime"::text, '-'),
			       COALESCE(s."errorMessage", ''),
			       (SELECT count(*) FROM snapshot_data sd
			        WHERE sd."userUid" = c."userUid" AND sd.exchange = c.exchange AND sd.label = c.label)
			FROM exchange_connections c
			LEFT JOIN sync_statuses s
			  ON s."userUid" = c."userUid" AND s.exchange = c.exchange AND s.label = c.label
			WHERE c."userUid" = $1
			ORDER BY c."createdAt"`, x.uid)
		if err != nil {
			fmt.Fprintf(os.Stderr, "connections query: %v\n", err)
			os.Exit(1)
		}
		n := 0
		for crows.Next() {
			var exch, label, status, lastSync, errMsg string
			var active bool
			var created time.Time
			var snaps int
			if err := crows.Scan(&exch, &label, &active, &created, &status, &lastSync, &errMsg, &snaps); err != nil {
				crows.Close()
				fmt.Fprintf(os.Stderr, "scan: %v\n", err)
				os.Exit(1)
			}
			n++
			fmt.Printf("  conn\t%s|%s\tactive=%v\tconnected %s\tstatus=%s\tlastSync=%s\tsnapshots=%d\t%s\n",
				exch, label, active, created.Format("2006-01-02 15:04"), status, lastSync, snaps, errMsg)
		}
		crows.Close()
		if n == 0 {
			fmt.Println("  conn\t(aucune connexion broker)")
		}
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
	mode := flag.String("mode", "breakdown", "breakdown | tables | repair-weekends | sync-status-enum | latest-users | purge-history")
	label := flag.String("label", "", "connection label (purge-history)")
	apply := flag.Bool("apply", false, "repair-weekends/purge-history: actually write; default is dry-run")
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
	if *mode == "latest-users" {
		dumpLatestUsers()
		return
	}
	if *mode == "clear-rebuild-optin" {
		if *userUID == "" {
			fmt.Fprintln(os.Stderr, "clear-rebuild-optin requires -user-uid, -exchange and -label")
			os.Exit(2)
		}
		clearRebuildOptin(*userUID, *exchange, *label, *apply)
		return
	}
	if *mode == "purge-history" {
		if *userUID == "" || *to == "" {
			fmt.Fprintln(os.Stderr, "purge-history requires -user-uid, -exchange, -label and -to (exclusive cutoff date)")
			os.Exit(2)
		}
		purgeHistory(*userUID, *exchange, *label, *to, *apply)
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
		       deposits, withdrawals, "createdAt", "updatedAt",
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
		var day, createdAt, updatedAt time.Time
		var label, breakdown string
		var equity, realized, dep, wd float64
		if err := rows.Scan(&day, &label, &equity, &realized, &dep, &wd, &createdAt, &updatedAt, &breakdown); err != nil {
			fmt.Fprintf(os.Stderr, "scan: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("%s\t%s\tequity=%.2f\trealized=%.2f\tdep=%.2f\twd=%.2f\tcreated=%s\tupdated=%s\t%s\n",
			day.Format("2006-01-02"), label, equity, realized, dep, wd,
			createdAt.Format("01-02 15:04:05"), updatedAt.Format("01-02 15:04:05"), breakdown)
	}
	if err := rows.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "rows: %v\n", err)
		os.Exit(1)
	}
}
