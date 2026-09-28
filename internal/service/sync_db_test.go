package service

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/trackrecord/enclave/internal/cache"
	"github.com/trackrecord/enclave/internal/connector"
	"github.com/trackrecord/enclave/internal/encryption"
	"github.com/trackrecord/enclave/internal/repository"
	"github.com/trackrecord/enclave/internal/testdb"
)

const (
	dbUser   = "test-user-db"
	dbKey    = "synthetic-api-key"
	dbSecret = "synthetic-api-secret"
)

type dbHarness struct {
	ctx      context.Context
	pool     *pgxpool.Pool
	enc      *encryption.Service
	users    *repository.UserRepo
	conns    *repository.ConnectionRepo
	snaps    *repository.SnapshotRepo
	statuses *repository.SyncStatusRepo
	cache    *cache.ConnectorCache
	connSvc  *ConnectionService
	sync     *SyncService
}

func newDBHarness(t *testing.T) *dbHarness {
	t.Helper()
	pool := testdb.New(t)
	enc, err := encryption.New(bytes.Repeat([]byte{0x42}, 32))
	if err != nil {
		t.Fatalf("encryption: %v", err)
	}
	h := &dbHarness{
		ctx:      context.Background(),
		pool:     pool,
		enc:      enc,
		users:    repository.NewUserRepo(pool),
		conns:    repository.NewConnectionRepo(pool),
		snaps:    repository.NewSnapshotRepo(pool),
		statuses: repository.NewSyncStatusRepo(pool),
		cache:    cache.NewConnectorCache(),
	}
	t.Cleanup(h.cache.Stop)
	h.connSvc = NewConnectionService(h.conns, enc)
	h.sync = NewSyncService(h.connSvc, h.snaps, h.cache, zap.NewNop())
	h.sync.SetSyncStatusRepo(h.statuses)
	return h
}

// seedConnection writes a row the way production rows were written until
// now: TS-format ciphertext bound to nothing but the key.
func (h *dbHarness) seedConnection(t *testing.T, exchange, label, apiKey, apiSecret string) {
	t.Helper()
	if _, err := h.users.GetOrCreate(h.ctx, dbUser); err != nil {
		t.Fatalf("create user: %v", err)
	}
	if !h.conns.IsTSSchema(h.ctx) {
		t.Fatal("test database is not on the production schema")
	}
	encKey, err := h.enc.EncryptTSString(apiKey)
	if err != nil {
		t.Fatalf("encrypt key: %v", err)
	}
	encSecret, err := h.enc.EncryptTSString(apiSecret)
	if err != nil {
		t.Fatalf("encrypt secret: %v", err)
	}
	if err := h.conns.Create(h.ctx, &repository.ExchangeConnection{
		UserUID:            dbUser,
		Exchange:           exchange,
		Label:              label,
		EncryptedAPIKey:    encKey,
		EncryptedAPISecret: encSecret,
		CredentialsHash:    hashCredentials(apiKey, apiSecret, ""),
	}); err != nil {
		t.Fatalf("create connection: %v", err)
	}
}

// plant puts c where the sync looks for a live connector, so no venue is called.
func (h *dbHarness) plant(apiKey, apiSecret string, c connector.Connector) {
	h.cache.Put(c.Exchange(), dbUser, cache.HashCredentials(apiKey, apiSecret, ""), c)
}

func (h *dbHarness) snapshotToday(t *testing.T, exchange, label string) *repository.Snapshot {
	t.Helper()
	snap, err := h.snaps.GetByUserExchangeLabelAndDate(h.ctx, dbUser, exchange, label, startOfTodayUTC())
	if err != nil {
		t.Fatalf("read today's snapshot: %v", err)
	}
	return snap
}

func (h *dbHarness) status(t *testing.T, exchange, label string) *repository.SyncStatus {
	t.Helper()
	st, err := h.statuses.GetByUserExchangeLabel(h.ctx, dbUser, exchange, label)
	if err != nil {
		t.Fatalf("read sync status: %v", err)
	}
	return st
}

func startOfTodayUTC() time.Time {
	now := time.Now().UTC()
	return time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
}

// syncPath is one of the two live pipelines (DUP-001). Every behaviour below
// is asserted on both, so merging them cannot change what either does.
type syncPath struct {
	name string
	run  func(h *dbHarness, exchange, label string) *SyncResult
}

var syncPaths = []syncPath{
	{"scheduled", func(h *dbHarness, exchange, label string) *SyncResult {
		results, err := h.sync.SyncUserScheduledDueAtomic(h.ctx, dbUser, time.Now().UTC())
		if err != nil || len(results) != 1 {
			return &SyncResult{Error: "scheduled pass: unexpected results"}
		}
		return results[0]
	}},
	{"manual", func(h *dbHarness, exchange, label string) *SyncResult {
		return h.sync.SyncConnectionScheduledByLabel(h.ctx, dbUser, exchange, label)
	}},
}

type scriptedConnector struct {
	exchange    string
	balances    []connector.Balance
	balanceErr  error
	cashflows   []*connector.Cashflow
	cashflowErr error
	warnings    []string
	// cashflowWarning is raised only while the cashflows are read, the way
	// Bybit and OKX find their gaps.
	cashflowWarning string

	mu    sync.Mutex
	reads int
}

func (c *scriptedConnector) GetBalance(context.Context) (*connector.Balance, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reads++
	if c.balanceErr != nil {
		return nil, c.balanceErr
	}
	b := c.balances[min(c.reads, len(c.balances))-1]
	return &b, nil
}

func (c *scriptedConnector) balanceReads() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.reads
}

func (c *scriptedConnector) GetPositions(context.Context) ([]*connector.Position, error) {
	return nil, nil
}

func (c *scriptedConnector) GetTrades(context.Context, time.Time, time.Time) ([]*connector.Trade, error) {
	return nil, nil
}

func (c *scriptedConnector) TestConnection(context.Context) error { return nil }
func (c *scriptedConnector) Exchange() string                     { return c.exchange }

func (c *scriptedConnector) GetCashflows(context.Context, time.Time) ([]*connector.Cashflow, error) {
	if c.cashflowWarning != "" {
		c.mu.Lock()
		c.warnings = append(c.warnings, c.cashflowWarning)
		c.mu.Unlock()
	}
	return c.cashflows, c.cashflowErr
}

func (c *scriptedConnector) CapabilityWarnings() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.warnings)
}

func TestDBSyncWritesSnapshotAndStatus(t *testing.T) {
	for _, p := range syncPaths {
		t.Run(p.name, func(t *testing.T) {
			h := newDBHarness(t)
			h.seedConnection(t, "binance", "main", dbKey, dbSecret)
			h.plant(dbKey, dbSecret, &scriptedConnector{
				exchange: "binance",
				balances: []connector.Balance{{Equity: 1000, Available: 800, UnrealizedPnL: 50}},
				cashflows: []*connector.Cashflow{
					{Amount: 200, Currency: "USDT", Timestamp: time.Now().Add(-time.Hour)},
					{Amount: -50, Currency: "USDT", Timestamp: time.Now().Add(-time.Hour)},
				},
				warnings: []string{"futures_permission_missing"},
			})

			res := p.run(h, "binance", "main")
			if !res.Success || res.Error != "" {
				t.Fatalf("sync failed: %+v", res)
			}

			snap := h.snapshotToday(t, "binance", "main")
			if snap.TotalEquity != 1000 || snap.RealizedBalance != 950 || snap.UnrealizedPnL != 50 {
				t.Errorf("equity fields = %v/%v/%v, want 1000/950/50", snap.TotalEquity, snap.RealizedBalance, snap.UnrealizedPnL)
			}
			if snap.Deposits != 200 || snap.Withdrawals != 50 {
				t.Errorf("cashflows = +%v/-%v, want +200/-50", snap.Deposits, snap.Withdrawals)
			}
			if snap.IsHistorical {
				t.Error("live snapshot stored as historical")
			}

			st := h.status(t, "binance", "main")
			if st.Status != "completed" || st.LastSyncTime == nil {
				t.Errorf("status = %q (lastSyncTime %v), want completed and stamped", st.Status, st.LastSyncTime)
			}
			if st.ErrorMessage != "warning: futures_permission_missing" {
				t.Errorf("errorMessage = %q, want the capability warning", st.ErrorMessage)
			}
		})
	}
}

func TestDBWarningsFoundWhileReadingCashflowsReachTheStatus(t *testing.T) {
	for _, p := range syncPaths {
		t.Run(p.name, func(t *testing.T) {
			h := newDBHarness(t)
			h.seedConnection(t, "bybit", "main", dbKey, dbSecret)
			h.plant(dbKey, dbSecret, &scriptedConnector{
				exchange:        "bybit",
				balances:        []connector.Balance{{Equity: 1000, Available: 1000}},
				cashflowWarning: "funding_wallet_unreadable",
			})

			if res := p.run(h, "bybit", "main"); !res.Success {
				t.Fatalf("sync failed: %+v", res)
			}
			if st := h.status(t, "bybit", "main"); st.ErrorMessage != "warning: funding_wallet_unreadable" {
				t.Errorf("errorMessage = %q, want the warning raised during the cashflow read", st.ErrorMessage)
			}
		})
	}
}

func TestDBFirstSyncWithoutCashflowBooksInceptionDeposit(t *testing.T) {
	for _, p := range syncPaths {
		t.Run(p.name, func(t *testing.T) {
			h := newDBHarness(t)
			h.seedConnection(t, "binance", "main", dbKey, dbSecret)
			h.plant(dbKey, dbSecret, &scriptedConnector{
				exchange: "binance",
				balances: []connector.Balance{{Equity: 1000, Available: 1000}},
			})

			if res := p.run(h, "binance", "main"); !res.Success {
				t.Fatalf("sync failed: %+v", res)
			}
			if snap := h.snapshotToday(t, "binance", "main"); snap.Deposits != 1000 {
				t.Errorf("deposits = %v, want the 1000 equity booked as inception deposit", snap.Deposits)
			}
		})
	}
}

func TestDBCashflowFailureStillWritesSnapshot(t *testing.T) {
	for _, p := range syncPaths {
		t.Run(p.name, func(t *testing.T) {
			h := newDBHarness(t)
			h.seedConnection(t, "binance", "main", dbKey, dbSecret)
			yesterday := startOfTodayUTC().Add(-24 * time.Hour)
			if err := h.snaps.Upsert(h.ctx, &repository.Snapshot{
				UserUID: dbUser, Exchange: "binance", Label: "main", Timestamp: yesterday,
				TotalEquity: 900, RealizedBalance: 900,
			}); err != nil {
				t.Fatalf("seed yesterday: %v", err)
			}
			h.plant(dbKey, dbSecret, &scriptedConnector{
				exchange:    "binance",
				balances:    []connector.Balance{{Equity: 1000, Available: 1000}},
				cashflowErr: errors.New("ledger unavailable"),
			})

			if res := p.run(h, "binance", "main"); !res.Success {
				t.Fatalf("sync failed: %+v", res)
			}
			snap := h.snapshotToday(t, "binance", "main")
			if snap.TotalEquity != 1000 || snap.Deposits != 0 || snap.Withdrawals != 0 {
				t.Errorf("snapshot = equity %v +%v/-%v, want 1000 with no flows", snap.TotalEquity, snap.Deposits, snap.Withdrawals)
			}
			if st := h.status(t, "binance", "main"); st.Status != "completed" || st.ErrorMessage != "" {
				t.Errorf("status = %q %q, want completed without message", st.Status, st.ErrorMessage)
			}
		})
	}
}

func TestDBUndecryptableCredentialsFailTheSync(t *testing.T) {
	for _, p := range syncPaths {
		t.Run(p.name, func(t *testing.T) {
			h := newDBHarness(t)
			h.seedConnection(t, "binance", "main", dbKey, dbSecret)
			other, err := encryption.New(bytes.Repeat([]byte{0x17}, 32))
			if err != nil {
				t.Fatal(err)
			}
			foreign, err := other.EncryptTSString(dbKey)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := h.pool.Exec(h.ctx, `UPDATE exchange_connections SET "encryptedApiKey" = $1`, foreign); err != nil {
				t.Fatalf("corrupt row: %v", err)
			}

			res := p.run(h, "binance", "main")
			if res.Success || res.Error == "" {
				t.Fatalf("sync succeeded on a row the key cannot open: %+v", res)
			}
			if _, err := h.snaps.GetByUserExchangeLabelAndDate(h.ctx, dbUser, "binance", "main", startOfTodayUTC()); !errors.Is(err, repository.ErrNotFound) {
				t.Errorf("a snapshot was written (err=%v)", err)
			}
			st := h.status(t, "binance", "main")
			if st.Status != "error" || st.LastSyncTime != nil {
				t.Errorf("status = %q (lastSyncTime %v), want error and unstamped", st.Status, st.LastSyncTime)
			}
			for _, s := range []string{dbKey, dbSecret, foreign} {
				if strings.Contains(st.ErrorMessage, s) || strings.Contains(res.Error, s) {
					t.Errorf("sync error exposes credential material: %q", st.ErrorMessage)
				}
			}
		})
	}
}

func TestDBCollapseGuard(t *testing.T) {
	prev := collapseGuardDelay
	collapseGuardDelay = 0
	t.Cleanup(func() { collapseGuardDelay = prev })

	cases := []struct {
		name          string
		second        float64
		operatorWarns int
	}{
		{"transient zero heals", 950, 0},
		{"confirmed collapse stands", 100, 1},
	}
	for _, p := range syncPaths {
		for _, tc := range cases {
			t.Run(p.name+"/"+tc.name, func(t *testing.T) {
				core, logs := observer.New(zapcore.DebugLevel)
				h := newDBHarness(t)
				h.sync.logger = zap.New(core)
				h.seedConnection(t, "binance", "main", dbKey, dbSecret)
				if err := h.snaps.Upsert(h.ctx, &repository.Snapshot{
					UserUID: dbUser, Exchange: "binance", Label: "main",
					Timestamp:   startOfTodayUTC().Add(-24 * time.Hour),
					TotalEquity: 1000, RealizedBalance: 1000,
				}); err != nil {
					t.Fatalf("seed yesterday: %v", err)
				}
				fake := &scriptedConnector{
					exchange: "binance",
					balances: []connector.Balance{{Equity: 100, Available: 100}, {Equity: tc.second, Available: tc.second}},
				}
				h.plant(dbKey, dbSecret, fake)

				if res := p.run(h, "binance", "main"); !res.Success {
					t.Fatalf("sync failed: %+v", res)
				}
				if got := fake.balanceReads(); got != 2 {
					t.Errorf("balance reads = %d, want a single re-read", got)
				}
				if snap := h.snapshotToday(t, "binance", "main"); snap.TotalEquity != tc.second {
					t.Errorf("persisted equity = %v, want the second reading %v", snap.TotalEquity, tc.second)
				}
				// A marker here would tell the user to recreate their key.
				if st := h.status(t, "binance", "main"); st.ErrorMessage != "" {
					t.Errorf("errorMessage = %q, want none", st.ErrorMessage)
				}
				if got := logs.FilterMessage("balance collapse confirmed on re-read, persisted as measured (SANITY-001)").FilterLevelExact(zapcore.WarnLevel).Len(); got != tc.operatorWarns {
					t.Errorf("operator warnings = %d, want %d", got, tc.operatorWarns)
				}
			})
		}
	}
}

func TestDBConnectionCredentialsRoundTrip(t *testing.T) {
	t.Setenv("ENV", "")
	t.Setenv("NODE_ENV", "")
	h := newDBHarness(t)
	if _, err := h.users.GetOrCreate(h.ctx, dbUser); err != nil {
		t.Fatal(err)
	}

	if err := h.connSvc.Create(h.ctx, &CreateConnectionRequest{
		UserUID: dbUser, Exchange: "mock", Label: "main",
		APIKey: dbKey, APISecret: dbSecret, Passphrase: "synthetic-passphrase",
	}); err != nil {
		t.Fatalf("create: %v", err)
	}

	creds, err := h.connSvc.GetDecryptedCredentialsByLabel(h.ctx, dbUser, "mock", "main")
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	if creds.APIKey != dbKey || creds.APISecret != dbSecret || creds.Passphrase != "synthetic-passphrase" {
		t.Errorf("round trip = %q/%q/%q", creds.APIKey, creds.APISecret, creds.Passphrase)
	}

	var row string
	if err := h.pool.QueryRow(h.ctx, `SELECT "encryptedApiKey" || "encryptedApiSecret" || coalesce("encryptedPassphrase", '') || coalesce("credentialsHash", '') FROM exchange_connections`).Scan(&row); err != nil {
		t.Fatal(err)
	}
	for _, s := range []string{dbKey, dbSecret, "synthetic-passphrase"} {
		if strings.Contains(row, s) {
			t.Errorf("stored row contains plaintext %q", s)
		}
	}
}

// SEC-01: a ciphertext written before binding existed still opens in another
// row, until every row is bound and the legacy read is closed.
func TestDBLegacyCiphertextStillOpensElsewhere(t *testing.T) {
	h := newDBHarness(t)
	h.seedConnection(t, "binance", "a", "key-of-a", "secret-of-a")
	h.seedConnection(t, "binance", "b", "key-of-b", "secret-of-b")
	if _, err := h.pool.Exec(h.ctx, `UPDATE exchange_connections AS b
		SET "encryptedApiKey" = a."encryptedApiKey", "encryptedApiSecret" = a."encryptedApiSecret"
		FROM exchange_connections AS a WHERE a.label = 'a' AND b.label = 'b'`); err != nil {
		t.Fatalf("move ciphertext: %v", err)
	}

	creds, err := h.connSvc.GetDecryptedCredentialsByLabel(h.ctx, dbUser, "binance", "b")
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	if creds.APIKey != "key-of-a" {
		t.Errorf("row b decrypted to %q", creds.APIKey)
	}
}

func TestDBPersistOAuthTokensRoundTrip(t *testing.T) {
	h := newDBHarness(t)
	h.seedConnection(t, "ctrader", "main", "old-access", "old-refresh")

	if err := h.connSvc.PersistOAuthTokens(h.ctx, dbUser, "ctrader", "main", "new-access", "new-refresh"); err != nil {
		t.Fatalf("persist: %v", err)
	}
	creds, err := h.connSvc.GetDecryptedCredentialsByLabel(h.ctx, dbUser, "ctrader", "main")
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	if creds.APIKey != "new-access" || creds.APISecret != "new-refresh" {
		t.Errorf("tokens = %q/%q, want the persisted pair", creds.APIKey, creds.APISecret)
	}
}
