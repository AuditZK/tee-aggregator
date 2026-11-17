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
	"crypto/sha256"
	"encoding/hex"
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
	// reveal opts into printing cleartext MT login/server to stderr (SEC-011).
	// Off by default; only MT-specific debug sessions should flip this.
	reveal bool
	// acceptPlaintext is the operator's explicit acknowledgement that this
	// tool writes live exchange data (balance, positions, trades) to stdout
	// unredacted — it bypasses the zap log-redaction pipeline that
	// protects the enclave's production logs (ADMIN-001). Required flag:
	// without it, main() refuses to run.
	acceptPlaintext bool
}

func main() {
	args := parseArgs()
	if args.userUID == "" || args.exchange == "" || args.label == "" {
		fmt.Fprintln(os.Stderr, "missing required flag(s): -user-uid, -exchange, -label")
		flag.Usage()
		os.Exit(2)
	}
	if !args.acceptPlaintext {
		fmt.Fprintln(os.Stderr, "admin-probe writes LIVE exchange data to stdout unredacted.")
		fmt.Fprintln(os.Stderr, "Re-run with -i-accept-plaintext-output to acknowledge.")
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
	flag.BoolVar(&a.reveal, "reveal", false, "print cleartext MT login/server to stderr (off by default, SEC-011)")
	flag.BoolVar(&a.acceptPlaintext, "i-accept-plaintext-output", false,
		"required ack that balance/positions/trades will be printed to stdout bypassing log redaction (ADMIN-001)")
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
	svcCreds, err := connSvc.GetDecryptedCredentialsByLabel(ctx, a.userUID, a.exchange, a.label)
	if err != nil {
		return nil, fmt.Errorf("decrypt credentials: %w", err)
	}
	return &connector.Credentials{
		Exchange:   svcCreds.Exchange,
		APIKey:     svcCreds.APIKey,
		APISecret:  svcCreds.APISecret,
		Passphrase: svcCreds.Passphrase,
	}, nil
}

func printHeader(a probeArgs, creds *connector.Credentials) {
	fmt.Printf("=== %s / %q (user=%s) ===\n", a.exchange, a.label, a.userUID)
	// SEC-011: print lengths + 8-char SHA-256 fingerprints instead of the raw
	// credentials. The fingerprint lets an operator correlate two runs without
	// ever exposing the actual secret to stdout / log aggregators.
	fmt.Printf("apiKey   len=%d fp=%s\n", len(creds.APIKey), credFingerprint(creds.APIKey))
	fmt.Printf("apiSecret len=%d fp=%s\n", len(creds.APISecret), credFingerprint(creds.APISecret))
	fmt.Printf("passphrase len=%d fp=%s\n", len(creds.Passphrase), credFingerprint(creds.Passphrase))
	if creds.Exchange == "mt5" || creds.Exchange == "mt4" || creds.Exchange == "exness" {
		if a.reveal {
			// Cleartext only when the operator explicitly asked for it, and
			// only on stderr so piping stdout to a log file does not capture
			// the secret.
			fmt.Fprintf(os.Stderr, "[-reveal] mt login=%q server=%q MT_BRIDGE_URL=%q\n",
				creds.APIKey, creds.Passphrase, os.Getenv("MT_BRIDGE_URL"))
		} else {
			fmt.Printf("mt login=*** server=*** (use -reveal to print on stderr) MT_BRIDGE_URL=%q\n",
				os.Getenv("MT_BRIDGE_URL"))
		}
	}
	fmt.Println()
}

// credFingerprint returns the first 8 hex chars of SHA-256(s) or "-" when s is
// empty. Safe to print: the preimage is not recoverable and two identical
// secrets produce the same fingerprint (useful for correlation).
func credFingerprint(s string) string {
	if s == "" {
		return "-"
	}
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:4])
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
