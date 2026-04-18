// Admin probe: decrypt the stored credentials for a user/exchange/label
// combination, instantiate the matching connector via the shared factory,
// and dump what the upstream exchange/broker actually returns right now
// (balance, positions, recent trades).
//
// Use this to diagnose frozen/stale snapshots without going through the sync
// pipeline — we see exactly what the broker hands back.
//
// Usage (inside the enclave container):
//
//	docker exec -it enclave_go_test /app/admin-probe \
//	    -user-uid="a2c493a1-d411-5458-a166-ddf25f50add9" \
//	    -exchange="mt5" \
//	    -label="mt5-vtmarkets account"
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"

	"github.com/trackrecord/enclave/internal/connector"
	"github.com/trackrecord/enclave/internal/encryption"
	"github.com/trackrecord/enclave/internal/repository"
	"github.com/trackrecord/enclave/internal/service"
)

const errFmt = "  ERROR: %v\n"

type probeArgs struct {
	userUID  string
	exchange string
	label    string
	daysBack int
}

func main() {
	args := parseArgs()
	if args.userUID == "" || args.exchange == "" || args.label == "" {
		fmt.Fprintln(os.Stderr, "missing required flag(s): -user-uid, -exchange, -label")
		flag.Usage()
		os.Exit(2)
	}

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		fatalf("DATABASE_URL env var is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	creds, err := loadCredentials(ctx, dbURL, args)
	if err != nil {
		fatalf("%v", err)
	}
	creds.Exchange = strings.ToLower(strings.TrimSpace(args.exchange))

	printHeader(args, creds)

	conn, err := connector.NewFactory().Create(creds)
	if err != nil {
		fatalf("build connector: %v", err)
	}

	probeAll(ctx, conn, args.daysBack)
}

func parseArgs() probeArgs {
	var a probeArgs
	flag.StringVar(&a.userUID, "user-uid", "", "user UID (required)")
	flag.StringVar(&a.exchange, "exchange", "", "exchange name (required)")
	flag.StringVar(&a.label, "label", "", "connection label (required)")
	flag.IntVar(&a.daysBack, "days-back", 7, "how many days of trades to fetch")
	flag.Parse()
	return a
}

func loadCredentials(ctx context.Context, dbURL string, a probeArgs) (*connector.Credentials, error) {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		return nil, fmt.Errorf("connect db: %w", err)
	}
	defer pool.Close()

	derivation, err := encryption.NewKeyDerivationService(logger)
	if err != nil {
		return nil, fmt.Errorf("init key derivation: %w", err)
	}
	kms, err := encryption.NewKeyManagementService(pool, encryption.KeyManagementOptions{
		Derivation:    derivation,
		Logger:        logger,
		AllowAutoInit: false,
	})
	if err != nil {
		return nil, fmt.Errorf("init key management: %w", err)
	}
	enc, err := kms.GetEncryptionService()
	if err != nil {
		return nil, fmt.Errorf("build encryption service: %w", err)
	}

	connRepo := repository.NewConnectionRepo(pool)
	connSvc := service.NewConnectionService(connRepo, enc)
	creds, err := connSvc.GetDecryptedCredentialsByLabel(ctx, a.userUID, a.exchange, a.label)
	if err != nil {
		return nil, fmt.Errorf("decrypt credentials: %w", err)
	}
	return creds, nil
}

func printHeader(a probeArgs, creds *connector.Credentials) {
	fmt.Printf("=== %s / %q (user=%s) ===\n", a.exchange, a.label, a.userUID)
	fmt.Printf("apiKey len=%d · apiSecret len=%d · passphrase len=%d\n",
		len(creds.APIKey), len(creds.APISecret), len(creds.Passphrase))
	if creds.Exchange == "mt5" || creds.Exchange == "mt4" || creds.Exchange == "exness" {
		fmt.Printf("mt login=%q server=%q MT_BRIDGE_URL=%q\n",
			creds.APIKey, creds.Passphrase, os.Getenv("MT_BRIDGE_URL"))
	}
	fmt.Println()
}

func probeAll(ctx context.Context, conn connector.Connector, daysBack int) {
	probeTestConnection(ctx, conn)
	probeBalance(ctx, conn)
	probePositions(ctx, conn)

	end := time.Now().UTC()
	start := end.AddDate(0, 0, -daysBack)
	probeTrades(ctx, conn, start, end, daysBack)
	probeCashflows(ctx, conn, start, daysBack)
}

func probeTestConnection(ctx context.Context, conn connector.Connector) {
	fmt.Println("--- TestConnection ---")
	if err := conn.TestConnection(ctx); err != nil {
		fmt.Fprintf(os.Stderr, errFmt, err)
	} else {
		fmt.Println("  ok")
	}
	fmt.Println()
}

func probeBalance(ctx context.Context, conn connector.Connector) {
	fmt.Println("--- GetBalance ---")
	bal, err := conn.GetBalance(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, errFmt, err)
	} else {
		printJSON(bal)
	}
	fmt.Println()
}

func probePositions(ctx context.Context, conn connector.Connector) {
	fmt.Println("--- GetPositions ---")
	positions, err := conn.GetPositions(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, errFmt, err)
	} else {
		fmt.Printf("  count=%d\n", len(positions))
		printJSON(positions)
	}
	fmt.Println()
}

func probeTrades(ctx context.Context, conn connector.Connector, start, end time.Time, daysBack int) {
	fmt.Printf("--- GetTrades (last %d days) ---\n", daysBack)
	trades, err := conn.GetTrades(ctx, start, end)
	if err != nil {
		fmt.Fprintf(os.Stderr, errFmt, err)
		return
	}
	fmt.Printf("  count=%d · range=[%s .. %s]\n",
		len(trades), start.Format(time.RFC3339), end.Format(time.RFC3339))
	printJSON(trades)
	fmt.Println()
}

func probeCashflows(ctx context.Context, conn connector.Connector, since time.Time, daysBack int) {
	cf, ok := conn.(connector.CashflowFetcher)
	if !ok {
		return
	}
	fmt.Printf("--- GetCashflows (last %d days) ---\n", daysBack)
	flows, err := cf.GetCashflows(ctx, since)
	if err != nil {
		fmt.Fprintf(os.Stderr, errFmt, err)
		return
	}
	fmt.Printf("  count=%d\n", len(flows))
	printJSON(flows)
}

func printJSON(v any) {
	b, err := json.MarshalIndent(v, "  ", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "  json encode: %v\n", err)
		return
	}
	fmt.Println("  " + string(b))
}

func fatalf(format string, a ...any) {
	fmt.Fprintf(os.Stderr, "ERROR: "+format+"\n", a...)
	os.Exit(1)
}
