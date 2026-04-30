// Admin tool to recover a missing snapshot for a single (user, exchange,
// label) connection. Bypasses the manual-sync anti-cherry-pick guard
// (isManualSyncAllowed) by going through SyncService.SyncConnectionScheduledByLabel,
// which is the same code path the daily scheduler uses.
//
// Use case: a transient outage (DNS, broker downtime, mt-bridge timeout)
// caused the 00:00 UTC daily sync to fail for one specific connection
// while the rest of that user's connections succeeded. The next daily
// sync will only cover today's date, leaving a one-day gap. This tool
// fills that gap by re-running the same scheduled-sync pipeline (decrypt
// credentials, fetch balance/trades from broker, build snapshot, upsert
// into snapshot_data) targeted at the failed connection only.
//
// Idempotent — Upsert overwrites today's snapshot if one already exists
// for the (userUid, timestamp, exchange, label) tuple. Safe to retry.
//
// Usage (inside the enclave container, which has DATABASE_URL +
// ENCRYPTION_KEY + MT_BRIDGE_URL etc. in its env):
//
//	docker exec -it enclave_go_staging /app/admin-recover-snapshot \
//	    -user-uid="a2c493a1-d411-5458-a166-ddf25f50add9" \
//	    -exchange="mt5" \
//	    -label="mt5-vtmarkets account" \
//	    -i-accept-prod-write
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"

	"github.com/trackrecord/enclave/internal/cache"
	"github.com/trackrecord/enclave/internal/connector"
	"github.com/trackrecord/enclave/internal/encryption"
	proxyPkg "github.com/trackrecord/enclave/internal/proxy"
	"github.com/trackrecord/enclave/internal/repository"
	"github.com/trackrecord/enclave/internal/service"
)

func main() {
	var (
		userUID         = flag.String("user-uid", "", "user UID (required)")
		exchange        = flag.String("exchange", "", "exchange name, e.g. mt5 (required)")
		label           = flag.String("label", "", "connection label (required)")
		acceptProdWrite = flag.Bool("i-accept-prod-write", false,
			"required ack: this tool fetches live exchange data and writes a snapshot to the prod snapshot_data table")
		timeoutSec = flag.Int("timeout", 120, "operation timeout in seconds (broker calls can be slow)")
	)
	flag.Parse()

	if *userUID == "" || *exchange == "" || *label == "" {
		fmt.Fprintln(os.Stderr, "missing required flag(s): -user-uid, -exchange, -label")
		flag.Usage()
		os.Exit(2)
	}
	if !*acceptProdWrite {
		fmt.Fprintln(os.Stderr, "admin-recover-snapshot writes a NEW snapshot row to snapshot_data using live broker data.")
		fmt.Fprintln(os.Stderr, "Re-run with -i-accept-prod-write to acknowledge.")
		os.Exit(2)
	}

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		fatalf("DATABASE_URL env var is required (run inside the enclave container)")
	}

	logger, _ := zap.NewProduction()
	defer logger.Sync()

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*timeoutSec)*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		fatalf("connect db: %v", err)
	}
	defer pool.Close()

	// Same encryption wiring as cmd/enclave: load the wrapped DEK from
	// data_encryption_keys and unwrap it with the master key.
	derivation, err := encryption.NewKeyDerivationService(logger)
	if err != nil {
		fatalf("init key derivation: %v", err)
	}
	kms, err := encryption.NewKeyManagementService(pool, encryption.KeyManagementOptions{
		Derivation:    derivation,
		Logger:        logger,
		AllowAutoInit: false,
	})
	if err != nil {
		fatalf("init key management: %v", err)
	}
	enc, err := kms.GetEncryptionService()
	if err != nil {
		fatalf("build encryption service: %v", err)
	}

	connRepo := repository.NewConnectionRepo(pool)
	snapshotRepo := repository.NewSnapshotRepo(pool)
	syncStatusRepo := repository.NewSyncStatusRepo(pool)
	connCache := cache.NewConnectorCache()
	defer connCache.Stop()

	connSvc := service.NewConnectionService(connRepo, enc)
	syncSvc := service.NewSyncService(connSvc, snapshotRepo, connCache, logger)
	syncSvc.SetSyncStatusRepo(syncStatusRepo)

	// Match the prod enclave's HTTP-proxy wiring so geo-restricted brokers
	// (Binance EU, etc.) resolve through the same egress as the daily sync.
	if proxyURL := os.Getenv("EXCHANGE_HTTP_PROXY"); proxyURL != "" {
		exchanges := os.Getenv("PROXY_EXCHANGES")
		if exchanges == "" {
			exchanges = "binance"
		}
		proxyCfg := proxyPkg.ParseConfig(proxyURL, exchanges)
		proxyFactory := connector.NewFactoryWithProxy(proxyCfg)
		connSvc.SetFactory(proxyFactory)
		syncSvc.SetFactory(proxyFactory)
	}

	fmt.Printf("recovering snapshot for user=%s exchange=%s label=%q (timeout=%ds)\n",
		*userUID, *exchange, *label, *timeoutSec)

	result := syncSvc.SyncConnectionScheduledByLabel(ctx, *userUID, *exchange, *label)

	out, _ := json.MarshalIndent(result, "", "  ")
	fmt.Println(string(out))

	if result.Error != "" || !result.Success {
		os.Exit(1)
	}
}

func fatalf(format string, a ...any) {
	fmt.Fprintf(os.Stderr, "ERROR: "+format+"\n", a...)
	os.Exit(1)
}
