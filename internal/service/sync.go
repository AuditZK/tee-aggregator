package service

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/trackrecord/enclave/internal/cache"
	"github.com/trackrecord/enclave/internal/connector"
	"github.com/trackrecord/enclave/internal/rebuilderclient"
	"github.com/trackrecord/enclave/internal/repository"
	"go.uber.org/zap"
)

// QUAL-001: error format strings duplicated across the sync orchestrator.
const (
	errFmtGetConnections      = "get connections: %w"
	errFmtNoActiveConnections = "no active connections for user %s"

	// History-reconstruction source labels passed to persistHistoricalSnapshots.
	// sourceExternalRebuilder marks snapshots rebuilt OUTSIDE the SEV-SNP
	// perimeter by the history-rebuilder service — they must never enter a
	// signed report (SEC-001). sourceInEnclave covers IBKR Flex history, which
	// is reconstructed inside the enclave and is legitimately verifiable.
	sourceExternalRebuilder = "rebuilder-service"
	sourceInEnclave         = "in-enclave"
)

// SyncService orchestrates exchange synchronization.
type SyncService struct {
	connSvc      *ConnectionService
	snapshotRepo *repository.SnapshotRepo
	syncStatus   *repository.SyncStatusRepo
	factory      *connector.Factory
	connCache    *cache.ConnectorCache
	logger       *zap.Logger
	// rebuilder is the optional non-ZK history-rebuilder client. When nil OR
	// not Configured(), connection-time rebuilds for non-IBKR exchanges are
	// silently skipped — keeps enclave-only dev environments functional.
	rebuilder *rebuilderclient.Client
	// historyNotifyURL, when set, is the base URL pinged after a connection's
	// history backfill completes (<url>/<userUID>). Best-effort, carries no
	// credentials, ignores the response — lets analytics run a per-user sync
	// without waiting for its daily cron. Empty = no ping.
	historyNotifyURL string
}

// NewSyncService creates a new sync service
func NewSyncService(
	connSvc *ConnectionService,
	snapshotRepo *repository.SnapshotRepo,
	connCache *cache.ConnectorCache,
	logger *zap.Logger,
) *SyncService {
	return &SyncService{
		connSvc:      connSvc,
		snapshotRepo: snapshotRepo,
		factory:      connector.NewFactory(),
		connCache:    connCache,
		logger:       logger,
	}
}

// SetSyncStatusRepo configures optional sync-status tracking.
func (s *SyncService) SetSyncStatusRepo(repo *repository.SyncStatusRepo) {
	s.syncStatus = repo
}

// SetRebuilderClient wires the (non-ZK) history-rebuilder-service client.
// Pass nil or an unconfigured client to disable connection-time rebuilds for
// non-IBKR exchanges (the enclave then writes nothing for HL, Lighter, … on
// connect; IBKR's in-enclave Flex rebuild is unaffected).
func (s *SyncService) SetRebuilderClient(c *rebuilderclient.Client) {
	s.rebuilder = c
}

// SetHistoryNotifyURL configures the best-effort "history rebuilt" ping URL.
// Empty disables it (the enclave then stays fully blind — downstream services
// pick up new history on their own schedule).
func (s *SyncService) SetHistoryNotifyURL(rawURL string) {
	s.historyNotifyURL = strings.TrimSpace(rawURL)
}

// notifyHistoryRebuilt sends a best-effort POST to <historyNotifyURL>/<userUID>
// after a connection's historical backfill completes. It carries no payload
// and no credentials, and the response is ignored — the enclave only emits a
// ping, it never reaches into another service's data. On any failure the
// downstream service still catches up via its own cron. No-op when unset.
func (s *SyncService) notifyHistoryRebuilt(ctx context.Context, userUID string) {
	if s.historyNotifyURL == "" {
		return
	}

	endpoint := strings.TrimRight(s.historyNotifyURL, "/") + "/" + url.PathEscape(userUID)
	reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, endpoint, nil)
	if err != nil {
		s.logger.Warn("history-rebuilt notify: build request failed", zap.Error(err))
		return
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		s.logger.Warn("history-rebuilt notify failed",
			zap.String("user_uid", userUID), zap.Error(err))
		return
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 300 {
		s.logger.Warn("history-rebuilt notify rejected",
			zap.String("user_uid", userUID), zap.Int("status", resp.StatusCode))
		return
	}
	s.logger.Info("history-rebuilt notify sent", zap.String("user_uid", userUID))
}

// SetFactory replaces the connector factory. Used to inject a proxy-aware
// factory after construction (e.g. when EXCHANGE_HTTP_PROXY is configured).
func (s *SyncService) SetFactory(f *connector.Factory) {
	s.factory = f
}

// SyncResult holds the result of a sync operation
type SyncResult struct {
	UserUID           string    `json:"user_uid"`
	Exchange          string    `json:"exchange"`
	Label             string    `json:"label,omitempty"`
	Success           bool      `json:"success"`
	TradeCount        int       `json:"trade_count"`
	SnapshotEquity    float64   `json:"snapshot_equity"`
	SnapshotTimestamp time.Time `json:"snapshot_timestamp"`
	Error             string    `json:"error,omitempty"`

	// snapshot is the built snapshot for atomic batch saves (not serialized).
	snapshot *repository.Snapshot
}

// SyncUser synchronizes all exchanges for a user (manual sync).
// Each exchange is individually checked for manual sync blocking.
func (s *SyncService) SyncUser(ctx context.Context, userUID string) ([]*SyncResult, error) {
	connections, err := s.connSvc.GetActiveConnections(ctx, userUID)
	if err != nil {
		return nil, fmt.Errorf(errFmtGetConnections, err)
	}

	if len(connections) == 0 {
		return nil, fmt.Errorf(errFmtNoActiveConnections, userUID)
	}

	var (
		results []*SyncResult
		mu      sync.Mutex
		wg      sync.WaitGroup
	)

	for _, conn := range connections {
		wg.Add(1)
		go func(c *repository.ExchangeConnection) {
			defer wg.Done()

			var result *SyncResult
			allowed, allowErr := s.isManualSyncAllowed(ctx, userUID, c.Exchange, c.Label)
			switch {
			case allowErr != nil:
				// ENG-001: fail closed — surface the DB error instead of
				// silently permitting the sync.
				result = &SyncResult{
					UserUID:  userUID,
					Exchange: c.Exchange,
					Label:    c.Label,
					Error:    fmt.Sprintf("manual sync blocked: %v", allowErr),
				}
			case !allowed:
				result = &SyncResult{
					UserUID:  userUID,
					Exchange: c.Exchange,
					Label:    c.Label,
					Error:    "manual sync blocked: snapshot already exists. Only the hourly scheduler can sync after initial snapshot.",
				}
			default:
				result = s.syncConnection(ctx, c)
			}

			mu.Lock()
			results = append(results, result)
			mu.Unlock()
		}(conn)
	}

	wg.Wait()
	return results, nil
}

// SyncUserScheduled synchronizes all exchanges for a user (scheduler path - bypasses manual block)
func (s *SyncService) SyncUserScheduled(ctx context.Context, userUID string) ([]*SyncResult, error) {
	return s.SyncUserScheduledDue(ctx, userUID, time.Now().UTC())
}

// SyncUserScheduledDue synchronizes only connections that are due based on
// per-connection sync_interval_minutes and last_sync_time (from sync_statuses).
func (s *SyncService) SyncUserScheduledDue(ctx context.Context, userUID string, now time.Time) ([]*SyncResult, error) {
	connections, err := s.connSvc.GetActiveConnections(ctx, userUID)
	if err != nil {
		return nil, fmt.Errorf(errFmtGetConnections, err)
	}

	if len(connections) == 0 {
		return nil, fmt.Errorf(errFmtNoActiveConnections, userUID)
	}

	var (
		results []*SyncResult
		mu      sync.Mutex
		wg      sync.WaitGroup
	)

	for _, conn := range connections {
		if !s.isConnectionDue(ctx, conn, now) {
			continue
		}

		wg.Add(1)
		go func(c *repository.ExchangeConnection) {
			defer wg.Done()

			result := s.syncConnection(ctx, c)

			mu.Lock()
			results = append(results, result)
			mu.Unlock()
		}(conn)
	}

	wg.Wait()
	return results, nil
}

// SyncExchange synchronizes a single exchange for a user (manual sync).
// If multiple labels exist for the same exchange, all matching connections are synced.
// Blocks if a snapshot already exists for this user+exchange+label (anti-cherry-picking).
func (s *SyncService) SyncExchange(ctx context.Context, userUID, exchange string) *SyncResult {
	connections, err := s.getConnectionsByExchange(ctx, userUID, exchange)
	if err != nil {
		return &SyncResult{
			UserUID:  userUID,
			Exchange: exchange,
			Error:    err.Error(),
		}
	}
	if len(connections) == 0 {
		return &SyncResult{
			UserUID:  userUID,
			Exchange: exchange,
			Error:    fmt.Sprintf("no active connection for exchange %s", exchange),
		}
	}

	for _, conn := range connections {
		allowed, allowErr := s.isManualSyncAllowed(ctx, userUID, conn.Exchange, conn.Label)
		if allowErr != nil {
			// ENG-001: fail closed on DB error.
			return &SyncResult{
				UserUID:  userUID,
				Exchange: conn.Exchange,
				Label:    conn.Label,
				Error:    fmt.Sprintf("manual sync blocked: %v", allowErr),
			}
		}
		if !allowed {
			return &SyncResult{
				UserUID:  userUID,
				Exchange: conn.Exchange,
				Label:    conn.Label,
				Error:    "manual sync blocked: snapshot already exists. Only the hourly scheduler can sync after initial snapshot.",
			}
		}
	}

	results := make([]*SyncResult, 0, len(connections))
	for _, conn := range connections {
		results = append(results, s.syncConnection(ctx, conn))
	}
	return aggregateSyncResults(userUID, exchange, results)
}

// SyncConnectionScheduledByLabel re-runs the scheduled sync pipeline for a
// single (user, exchange, label) connection, bypassing the manual-sync
// anti-cherry-pick guard (isManualSyncAllowed). Intended for one-off
// recovery of a missing snapshot when the daily scheduler failed for that
// specific connection (e.g. transient DNS/network/broker outage at 00:00
// UTC). Idempotent — Upsert overwrites today's snapshot if one already
// exists for the (userUid, timestamp, exchange, label) tuple.
//
// Uses connSvc.repo.GetByUserExchangeLabel directly (TRIM-tolerant exact
// lookup) instead of GetActiveConnections + filter, so a fresh capability
// detection on the underlying pool can't mask a real row.
func (s *SyncService) SyncConnectionScheduledByLabel(ctx context.Context, userUID, exchange, label string) *SyncResult {
	conn, err := s.connSvc.GetActiveConnectionByLabel(ctx, userUID, exchange, label)
	if err != nil {
		return &SyncResult{
			UserUID:  userUID,
			Exchange: exchange,
			Label:    label,
			Error:    fmt.Sprintf("lookup connection: %v", err),
		}
	}
	return s.syncConnection(ctx, conn)
}

// SyncExchangeScheduled is used by the hourly scheduler - bypasses manual sync block
func (s *SyncService) SyncExchangeScheduled(ctx context.Context, userUID, exchange string) *SyncResult {
	connections, err := s.getConnectionsByExchange(ctx, userUID, exchange)
	if err != nil {
		return &SyncResult{
			UserUID:  userUID,
			Exchange: exchange,
			Error:    err.Error(),
		}
	}
	if len(connections) == 0 {
		return &SyncResult{
			UserUID:  userUID,
			Exchange: exchange,
			Error:    fmt.Sprintf("no active connection for exchange %s", exchange),
		}
	}

	results := make([]*SyncResult, 0, len(connections))
	for _, conn := range connections {
		results = append(results, s.syncConnection(ctx, conn))
	}
	return aggregateSyncResults(userUID, exchange, results)
}

// isManualSyncAllowed checks if a manual sync is permitted.
//
// Returns (allowed=false, err=nil) when a snapshot already exists for this
// user+exchange+label — the caller must refuse the sync (anti-cherry-pick).
// Returns (allowed=false, err=<db error>) when the DB lookup fails —
// the caller must also refuse and surface the error (ENG-001: fail closed,
// not open).
//
// Replaces a previous full-range scan over every historical snapshot with a
// targeted `SELECT 1 ... LIMIT 1` via ExistsForUserExchangeLabel.
func (s *SyncService) isManualSyncAllowed(ctx context.Context, userUID, exchange, label string) (bool, error) {
	exists, err := s.snapshotRepo.ExistsForUserExchangeLabel(ctx, userUID, exchange, label)
	if err != nil {
		// Fail closed: the caller surfaces the DB error to the operator
		// instead of silently letting a manual sync overwrite an
		// existing committed snapshot.
		return false, fmt.Errorf("anti-cherry-pick check failed: %w", err)
	}
	return !exists, nil
}

func (s *SyncService) getConnectionsByExchange(ctx context.Context, userUID, exchange string) ([]*repository.ExchangeConnection, error) {
	connections, err := s.connSvc.GetActiveConnections(ctx, userUID)
	if err != nil {
		return nil, fmt.Errorf(errFmtGetConnections, err)
	}
	targetExchange := normalizeExchange(exchange)
	matches := make([]*repository.ExchangeConnection, 0)
	for _, c := range connections {
		if normalizeExchange(c.Exchange) == targetExchange {
			matches = append(matches, c)
		}
	}
	return matches, nil
}

func (s *SyncService) syncConnection(ctx context.Context, connMeta *repository.ExchangeConnection) *SyncResult {
	result := &SyncResult{
		UserUID:  connMeta.UserUID,
		Exchange: connMeta.Exchange,
		Label:    connMeta.Label,
	}
	lastAttempt := time.Now().UTC()
	defer s.recordSyncStatus(ctx, connMeta, result, lastAttempt)

	// 1. Get decrypted credentials
	creds, err := s.connSvc.GetDecryptedCredentialsByLabel(ctx, connMeta.UserUID, connMeta.Exchange, connMeta.Label)
	if err != nil {
		result.Error = fmt.Sprintf("get credentials: %v", err)
		s.logger.Error("sync failed: get credentials",
			zap.String("user_uid", connMeta.UserUID),
			zap.String("exchange", connMeta.Exchange),
			zap.String("label", connMeta.Label),
			zap.Error(err),
		)
		return result
	}

	// 2. Get or create connector (cached, TS parity: UniversalConnectorCache)
	conn, err := s.getOrCreateConnector(connMeta.Exchange, connMeta.UserUID, connMeta.Label, creds)
	if err != nil {
		result.Error = fmt.Sprintf("create connector: %v", err)
		return result
	}

	// 2b. History reconstruction for connectors implementing HistoricalSnapshotProvider.
	//     IBKR runs on every sync (Flex returns the full window in a single cheap call
	//     and can carry retroactive corrections). Other connectors only run on first
	//     sync — once the historical backfill is in DB, subsequent syncs only produce
	//     the live (today) snapshot.
	if hsp, ok := conn.(connector.HistoricalSnapshotProvider); ok {
		// IBKR keeps the every-sync Flex refresh because Flex is a single cheap
		// call and can carry retroactive corrections. Other connectors
		// (Hyperliquid, …) reconstruct ONCE at connection time via
		// ReconstructHistoryOnConnect — the sync flow is live-only afterwards.
		if strings.ToLower(connMeta.Exchange) == "ibkr" {
			s.syncFromHistoricalProvider(ctx, connMeta, hsp)
		}
	}

	// 3. Get balance
	balance, err := conn.GetBalance(ctx)
	if err != nil {
		result.Error = fmt.Sprintf("get balance: %v", err)
		s.logger.Error("sync failed: get balance",
			zap.String("user_uid", connMeta.UserUID),
			zap.String("exchange", connMeta.Exchange),
			zap.String("label", connMeta.Label),
			zap.Error(err),
		)
		return result
	}

	// 4. Get trades for the 24h window ending at the snapshot boundary.
	// startOfDay is the snapshot timestamp (today 00:00 UTC). The sync runs
	// at that moment, so we need to look BACK one day to attribute the last
	// 24h of trades/cashflows to this snapshot — otherwise GetTrades runs
	// with a zero-length window and returns nothing.
	now := time.Now().UTC()
	startOfDay := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	activityStart := startOfDay.Add(-24 * time.Hour)

	// 4a. Per-market trade fetching if supported; otherwise fallback to flat GetTrades
	var trades []*connector.Trade
	var swapSymbols []string
	if pmFetcher, ok := conn.(connector.PerMarketTradeFetcher); ok {
		if detector, ok2 := conn.(connector.MarketTypeDetector); ok2 {
			if marketTypes, err := detector.DetectMarketTypes(ctx); err == nil {
				for _, mt := range marketTypes {
					mtTrades, err := pmFetcher.GetTradesByMarket(ctx, mt, activityStart)
					if err != nil {
						continue
					}
					for _, t := range mtTrades {
						if t.MarketType == "" {
							t.MarketType = mt
						}
						trades = append(trades, t)
						if mt == connector.MarketSwap {
							swapSymbols = appendUnique(swapSymbols, t.Symbol)
						}
					}
				}
			}
		}
	}
	if len(trades) == 0 {
		trades, _ = conn.GetTrades(ctx, activityStart, now)
		// Collect swap symbols from fallback trades for funding fee fetch
		for _, t := range trades {
			if t.MarketType == connector.MarketSwap {
				swapSymbols = appendUnique(swapSymbols, t.Symbol)
			}
		}
	}

	// 5. Aggregate trades by market type
	breakdown := s.aggregateTrades(trades)

	// 5a. Fetch funding fees for swap positions (always if supported — funding
	// applies to all open positions, not just those traded today)
	if ffFetcher, ok := conn.(connector.FundingFeesFetcher); ok {
		if fees, err := ffFetcher.GetFundingFees(ctx, swapSymbols, activityStart); err == nil {
			totalFunding := 0.0
			for _, f := range fees {
				totalFunding += f.Amount
			}
			breakdown.swap.fundingFees = totalFunding
		}
	}

	// 5b. Fetch earn/staking balance if supported
	if earnFetcher, ok := conn.(connector.EarnBalanceFetcher); ok {
		if earnEquity, err := earnFetcher.GetEarnBalance(ctx); err == nil && earnEquity > 0 {
			breakdown.earn.equity = earnEquity
			balance.Equity += earnEquity // Add to global equity
		}
	}

	// 6. Fetch deposits/withdrawals for the same 24h window as trades.
	var deposits, withdrawals float64
	if cfFetcher, ok := conn.(connector.CashflowFetcher); ok {
		cashflows, err := cfFetcher.GetCashflows(ctx, activityStart)
		if err == nil {
			for _, cf := range cashflows {
				if cf.Amount > 0 {
					deposits += cf.Amount
				} else {
					withdrawals += -cf.Amount
				}
			}
		} else {
			s.logger.Debug("cashflow fetch failed (non-critical)",
				zap.String("exchange", connMeta.Exchange),
				zap.Error(err),
			)
		}
	}

	// 7. Enrich breakdown with per-market equity if connector supports it
	if bmFetcher, ok := conn.(connector.BalanceByMarketFetcher); ok {
		if marketBalances, err := bmFetcher.GetBalanceByMarket(ctx); err == nil {
			s.enrichBreakdownWithBalances(breakdown, marketBalances)
		} else {
			s.logger.Debug("balance by market fetch failed (non-critical)",
				zap.String("exchange", connMeta.Exchange),
				zap.Error(err),
			)
		}
	}

	// 7b. TS parity: breakdown_by_market must always carry equity so the
	// gRPC mapper can build a non-nil global aggregate. Connectors that
	// implement BalanceByMarketFetcher already populated equity via
	// enrichBreakdownWithBalances; for all others, assign total equity to
	// the exchange's primary market type.
	if !breakdown.hasAnyEquity() {
		m := breakdown.getOrCreateMarket(primaryMarketType(connMeta.Exchange))
		m.equity = balance.Equity
		m.availableMargin = balance.Available
	}

	// 8. Inception-deposit convention (UX-001). When this is the very first
	// snapshot we write for the connection AND the connector didn't already
	// surface a cashflow for the period, treat the existing balance as a
	// deposit. Without this, the dashboard's cumulative-return calc has no
	// base reference for users who connect a broker that already holds
	// funds (Lighter / HL / MEXC / MT5 demos…), and the "Inception deposit"
	// marker on the equity curve is missing. See Notion ticket
	// "Code fix : inception deposit auto sur premier snapshot d'une connexion".
	if deposits == 0 && balance.Equity > 0 && s.isFirstSync(ctx, connMeta) {
		deposits = balance.Equity
	}

	// 9. Create snapshot
	// TS parity: realizedBalance = equity - unrealizedPnL (preserves the
	// invariant equity == realized + unrealized). Using balance.Available
	// (cash) diverges on margin accounts — cash can be deeply negative when
	// positions are bought on margin, even though equity is positive.
	snapshot := &repository.Snapshot{
		UserUID:         connMeta.UserUID,
		Exchange:        connMeta.Exchange,
		Label:           connMeta.Label,
		Timestamp:       startOfDay,
		TotalEquity:     balance.Equity,
		RealizedBalance: balance.Equity - balance.UnrealizedPnL,
		UnrealizedPnL:   balance.UnrealizedPnL,
		Deposits:        deposits,
		Withdrawals:     withdrawals,
		TotalTrades:     len(trades),
		TotalVolume:     breakdown.totalVolume(),
		TotalFees:       breakdown.totalFees(),
		Breakdown:       breakdown.toRepo(balance.Equity, balance.Available, len(trades)),
		// Live snapshot: full balance + 24h trades, never reconstructed.
		// Explicit so an Upsert overwriting a stale historical=true row
		// from a pre-refactor DB flips the flag back to false.
		IsHistorical: false,
	}

	result.snapshot = snapshot
	result.TradeCount = len(trades)
	result.SnapshotEquity = balance.Equity
	result.SnapshotTimestamp = startOfDay

	// Save snapshot individually (non-atomic path, used by manual sync)
	if err := s.snapshotRepo.Upsert(ctx, snapshot); err != nil {
		result.Error = fmt.Sprintf("save snapshot: %v", err)
		s.logger.Error("sync failed: save snapshot",
			zap.String("user_uid", connMeta.UserUID),
			zap.String("exchange", connMeta.Exchange),
			zap.String("label", connMeta.Label),
			zap.Error(err),
		)
		return result
	}

	// Success - trades are now garbage collected (never persisted)
	result.Success = true

	s.logger.Info("sync completed",
		zap.String("user_uid", connMeta.UserUID),
		zap.String("exchange", connMeta.Exchange),
		zap.String("label", connMeta.Label),
		zap.Int("trades", len(trades)),
		zap.Float64("equity", balance.Equity),
	)

	return result
}

// SyncUserScheduledDueAtomic builds all snapshots first, then saves atomically.
// If any snapshot build fails, the successful ones are still saved.
// The save itself is transactional: all-or-nothing (TS parity).
func (s *SyncService) SyncUserScheduledDueAtomic(ctx context.Context, userUID string, now time.Time) ([]*SyncResult, error) {
	connections, err := s.connSvc.GetActiveConnections(ctx, userUID)
	if err != nil {
		return nil, fmt.Errorf(errFmtGetConnections, err)
	}

	if len(connections) == 0 {
		return nil, fmt.Errorf(errFmtNoActiveConnections, userUID)
	}

	// Phase 1: Build snapshots with limited concurrency (max 2 per user).
	// CCXT connectors load markets (~40MB each), so 10 in parallel = OOM on small VMs.
	var (
		results []*SyncResult
		mu      sync.Mutex
		wg      sync.WaitGroup
	)

	// PERF-005: 4 native Go connectors in parallel ≈ 20 MB peak heap
	// (struct + http.Client + JSON parsing). The previous "sequential per
	// user" comment referenced CCXT (Python wrapper, ~150 MB/LoadMarkets)
	// which the Go enclave doesn't use — every connector under
	// internal/connector/ is native Go. Going from 1 → 4 turns a 19×Δ
	// per-connector worst case into roughly ⌈19/4⌉×Δ.
	connSem := make(chan struct{}, 4)
	// 5min matches the IBKR Flex poll budget (~4min for 30-day/YTD reports) with
	// a safety margin; other connectors are sub-second so the ceiling never hits.
	const connTimeout = 5 * time.Minute

	for _, conn := range connections {
		if !s.isConnectionDue(ctx, conn, now) {
			continue
		}

		wg.Add(1)
		go func(c *repository.ExchangeConnection) {
			defer wg.Done()
			connSem <- struct{}{}
			defer func() {
				<-connSem
			}()

			connCtx, cancel := context.WithTimeout(ctx, connTimeout)
			defer cancel()

			result := s.buildConnectionSnapshot(connCtx, c)
			mu.Lock()
			results = append(results, result)
			mu.Unlock()
		}(conn)
	}

	wg.Wait()

	// Phase 2: Collect successful snapshots, log failures
	var snapshots []*repository.Snapshot
	for _, r := range results {
		if r.Error != "" {
			s.logger.Error(classifySyncError(r.Error),
				zap.String("user_uid", userUID),
				zap.String("exchange", r.Exchange),
				zap.String("label", r.Label),
				zap.String("error", r.Error),
			)
			continue
		}
		if r.snapshot != nil {
			snapshots = append(snapshots, r.snapshot)
		}
	}

	// Phase 3: Atomic save
	if len(snapshots) > 0 {
		if err := s.snapshotRepo.UpsertBatch(ctx, snapshots); err != nil {
			s.logger.Error("atomic snapshot save failed - transaction rolled back",
				zap.String("user_uid", userUID),
				zap.Int("snapshots", len(snapshots)),
				zap.Error(err),
			)
			// Mark all as failed
			for _, r := range results {
				if r.snapshot != nil && r.Error == "" {
					r.Success = false
					r.Error = fmt.Sprintf("atomic save failed: %v", err)
				}
			}
		} else {
			// Mark all with snapshots as success
			for _, r := range results {
				if r.snapshot != nil && r.Error == "" {
					r.Success = true
				}
			}
			s.logger.Info("atomic snapshot save completed",
				zap.String("user_uid", userUID),
				zap.Int("snapshots_saved", len(snapshots)),
			)
		}
	}

	// Phase 4: Record sync status for all
	for _, r := range results {
		if r.snapshot != nil {
			conn := findConnection(connections, r.Exchange, r.Label)
			if conn != nil {
				s.recordSyncStatus(ctx, conn, r, now)
			}
		}
	}

	return results, nil
}

// buildConnectionSnapshot builds a snapshot without saving (for atomic batch).
func (s *SyncService) buildConnectionSnapshot(ctx context.Context, connMeta *repository.ExchangeConnection) *SyncResult {
	start := time.Now()
	s.logger.Info("building snapshot",
		zap.String("user_uid", connMeta.UserUID),
		zap.String("exchange", connMeta.Exchange),
		zap.String("label", connMeta.Label),
	)

	result := &SyncResult{
		UserUID:  connMeta.UserUID,
		Exchange: connMeta.Exchange,
		Label:    connMeta.Label,
	}

	creds, err := s.connSvc.GetDecryptedCredentialsByLabel(ctx, connMeta.UserUID, connMeta.Exchange, connMeta.Label)
	if err != nil {
		result.Error = fmt.Sprintf("get credentials: %v", err)
		s.logger.Error(classifySyncError(result.Error), zap.String("exchange", connMeta.Exchange), zap.String("step", "decrypt"), zap.Duration("elapsed", time.Since(start)), zap.Error(err))
		return result
	}

	conn, err := s.getOrCreateConnector(connMeta.Exchange, connMeta.UserUID, connMeta.Label, creds)
	if err != nil {
		result.Error = fmt.Sprintf("create connector: %v", err)
		s.logger.Error(classifySyncError(result.Error), zap.String("exchange", connMeta.Exchange), zap.String("step", "connector"), zap.Duration("elapsed", time.Since(start)), zap.Error(err))
		return result
	}

	// History reconstruction (see syncConnection for the gating rules). The
	// scheduler path goes through buildConnectionSnapshot, not syncConnection,
	// so we duplicate the call here to cover both manual and scheduled syncs.
	if hsp, ok := conn.(connector.HistoricalSnapshotProvider); ok {
		// IBKR keeps the every-sync Flex refresh because Flex is a single cheap
		// call and can carry retroactive corrections. Other connectors
		// (Hyperliquid, …) reconstruct ONCE at connection time via
		// ReconstructHistoryOnConnect — the sync flow is live-only afterwards.
		if strings.ToLower(connMeta.Exchange) == "ibkr" {
			s.syncFromHistoricalProvider(ctx, connMeta, hsp)
		}
	}

	balance, err := conn.GetBalance(ctx)
	if err != nil {
		result.Error = fmt.Sprintf("get balance: %v", err)
		s.logger.Error(classifySyncError(result.Error), zap.String("exchange", connMeta.Exchange), zap.String("label", connMeta.Label), zap.String("step", "get_balance"), zap.Duration("elapsed", time.Since(start)), zap.Error(err))
		return result
	}
	s.logger.Info("balance fetched", zap.String("exchange", connMeta.Exchange), zap.String("label", connMeta.Label), zap.Duration("elapsed", time.Since(start)))

	// Same 24h-window semantics as syncConnection above: the snapshot lands
	// at startOfDay (today 00:00 UTC), so we pull trades/cashflows from the
	// preceding 24h window.
	now := time.Now().UTC()
	startOfDay := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	activityStart := startOfDay.Add(-24 * time.Hour)

	var trades []*connector.Trade
	var swapSymbols []string
	if pmFetcher, ok := conn.(connector.PerMarketTradeFetcher); ok {
		if detector, ok2 := conn.(connector.MarketTypeDetector); ok2 {
			if marketTypes, err := detector.DetectMarketTypes(ctx); err == nil {
				for _, mt := range marketTypes {
					mtTrades, err := pmFetcher.GetTradesByMarket(ctx, mt, activityStart)
					if err != nil {
						continue
					}
					for _, t := range mtTrades {
						if t.MarketType == "" {
							t.MarketType = mt
						}
						trades = append(trades, t)
						if mt == connector.MarketSwap {
							swapSymbols = appendUnique(swapSymbols, t.Symbol)
						}
					}
				}
			}
		}
	}
	if len(trades) == 0 {
		trades, _ = conn.GetTrades(ctx, activityStart, now)
		// Collect swap symbols from fallback trades for funding fee fetch
		for _, t := range trades {
			if t.MarketType == connector.MarketSwap {
				swapSymbols = appendUnique(swapSymbols, t.Symbol)
			}
		}
	}

	breakdown := s.aggregateTrades(trades)

	if ffFetcher, ok := conn.(connector.FundingFeesFetcher); ok {
		if fees, err := ffFetcher.GetFundingFees(ctx, swapSymbols, activityStart); err == nil {
			total := 0.0
			for _, f := range fees {
				total += f.Amount
			}
			breakdown.swap.fundingFees = total
		}
	}

	if earnFetcher, ok := conn.(connector.EarnBalanceFetcher); ok {
		if earnEquity, err := earnFetcher.GetEarnBalance(ctx); err == nil && earnEquity > 0 {
			breakdown.earn.equity = earnEquity
			balance.Equity += earnEquity
		}
	}

	var deposits, withdrawals float64
	if cfFetcher, ok := conn.(connector.CashflowFetcher); ok {
		if cashflows, err := cfFetcher.GetCashflows(ctx, activityStart); err == nil {
			for _, cf := range cashflows {
				if cf.Amount > 0 {
					deposits += cf.Amount
				} else {
					withdrawals += -cf.Amount
				}
			}
		}
	}

	if bmFetcher, ok := conn.(connector.BalanceByMarketFetcher); ok {
		if marketBalances, err := bmFetcher.GetBalanceByMarket(ctx); err == nil {
			s.enrichBreakdownWithBalances(breakdown, marketBalances)
		} else {
			s.logger.Debug("balance by market fetch failed (non-critical)",
				zap.String("exchange", connMeta.Exchange),
				zap.Error(err),
			)
		}
	}

	// TS parity: breakdown_by_market must always carry equity (same as
	// syncConnection step 7b above).
	if !breakdown.hasAnyEquity() {
		m := breakdown.getOrCreateMarket(primaryMarketType(connMeta.Exchange))
		m.equity = balance.Equity
		m.availableMargin = balance.Available
	}

	// Inception-deposit convention (UX-001): see the non-atomic path above
	// for the full rationale. Both call sites need the same fallback,
	// otherwise the atomic-sync flow leaves new connections with deposits=0
	// when the connector lacks CashflowFetcher.
	if deposits == 0 && balance.Equity > 0 && s.isFirstSync(ctx, connMeta) {
		deposits = balance.Equity
	}

	// TS parity: realizedBalance = equity - unrealizedPnL. See the non-atomic
	// path above for the full rationale.
	result.snapshot = &repository.Snapshot{
		UserUID:         connMeta.UserUID,
		Exchange:        connMeta.Exchange,
		Label:           connMeta.Label,
		Timestamp:       startOfDay,
		TotalEquity:     balance.Equity,
		RealizedBalance: balance.Equity - balance.UnrealizedPnL,
		UnrealizedPnL:   balance.UnrealizedPnL,
		Deposits:        deposits,
		Withdrawals:     withdrawals,
		TotalTrades:     len(trades),
		TotalVolume:     breakdown.totalVolume(),
		TotalFees:       breakdown.totalFees(),
		Breakdown:       breakdown.toRepo(balance.Equity, balance.Available, len(trades)),
		// Live snapshot: see syncConnection above for rationale.
		IsHistorical: false,
	}
	result.TradeCount = len(trades)
	result.SnapshotEquity = balance.Equity
	result.SnapshotTimestamp = startOfDay

	return result
}

func findConnection(connections []*repository.ExchangeConnection, exchange, label string) *repository.ExchangeConnection {
	for _, c := range connections {
		if c.Exchange == exchange && c.Label == label {
			return c
		}
	}
	return nil
}

func (s *SyncService) isConnectionDue(ctx context.Context, conn *repository.ExchangeConnection, now time.Time) bool {
	if s.syncStatus == nil {
		return true
	}

	intervalMinutes := conn.SyncIntervalMinutes

	status, err := s.syncStatus.GetByUserExchangeLabel(ctx, conn.UserUID, conn.Exchange, conn.Label)
	if err != nil {
		if err == repository.ErrNotFound {
			return true
		}
		s.logger.Warn("failed to load sync status; treating as due",
			zap.String("user_uid", conn.UserUID),
			zap.String("exchange", conn.Exchange),
			zap.String("label", conn.Label),
			zap.Error(err),
		)
		return true
	}

	if status.LastSyncTime == nil {
		return true
	}

	return isDueByInterval(status.LastSyncTime, intervalMinutes, now)
}

func isDueByInterval(lastSync *time.Time, intervalMinutes int, now time.Time) bool {
	if intervalMinutes <= 0 {
		intervalMinutes = 1440
	}
	if lastSync == nil {
		return true
	}

	last := lastSync.UTC()
	current := now.UTC()
	if current.Before(last) {
		return false
	}

	// Daily sync (1440 min): use calendar-day comparison instead of 24h delta.
	// This prevents drift when sync runs at 00:58 then 01:15 then 01:30 etc.
	// A connection is due if the current UTC date is after the last sync UTC date.
	if intervalMinutes >= 1440 {
		lastDate := last.Truncate(24 * time.Hour)
		currentDate := current.Truncate(24 * time.Hour)
		return currentDate.After(lastDate)
	}

	return current.Sub(last) >= time.Duration(intervalMinutes)*time.Minute
}

func (s *SyncService) recordSyncStatus(ctx context.Context, conn *repository.ExchangeConnection, result *SyncResult, lastAttempt time.Time) {
	if s.syncStatus == nil || conn == nil || result == nil {
		return
	}

	status := "error"
	if result.Success {
		status = "completed"
	}

	record := &repository.SyncStatus{
		UserUID:      conn.UserUID,
		Exchange:     conn.Exchange,
		Label:        conn.Label,
		LastSyncTime: &lastAttempt,
		Status:       status,
		TotalTrades:  result.TradeCount,
		ErrorMessage: result.Error,
	}

	if err := s.syncStatus.Upsert(ctx, record); err != nil {
		s.logger.Warn("failed to persist sync status",
			zap.String("user_uid", conn.UserUID),
			zap.String("exchange", conn.Exchange),
			zap.String("label", conn.Label),
			zap.Error(err),
		)
	}
}

// enrichBreakdownWithBalances populates equity and available_margin per market
// from the connector's BalanceByMarketFetcher, matching TS parity.
func (s *SyncService) enrichBreakdownWithBalances(agg *aggregatedBreakdown, balances []*connector.MarketBalance) {
	for _, mb := range balances {
		if mb.Equity == 0 && mb.AvailableMargin == 0 {
			continue
		}
		ma := agg.getOrCreateMarket(mb.MarketType)
		ma.equity = mb.Equity
		ma.availableMargin = mb.AvailableMargin
	}
}

// ReconstructHistoryOnConnect runs the historical snapshot backfill triggered
// by a freshly created connection (wired via ConnectionService.SetPostCreateHook).
//
// Decrypts the credentials, builds the connector, and — if the connector
// implements HistoricalSnapshotProvider — fetches the daily timeline and
// upserts it as is_historical=true rows. Best-effort: errors are logged, never
// returned (the caller is a goroutine with no surface to surface them on).
//
// Runs once at connection time. Subsequent syncs only produce live snapshots,
// except for IBKR which keeps an every-sync Flex refresh handled in
// syncConnection (Flex returns the full window cheaply and can carry
// retroactive corrections).
func (s *SyncService) ReconstructHistoryOnConnect(ctx context.Context, userUID, exchange, label string) {
	connMeta, err := s.connSvc.GetActiveConnectionByLabel(ctx, userUID, exchange, label)
	if err != nil {
		s.logger.Error("history backfill: connection lookup failed",
			zap.String("user_uid", userUID),
			zap.String("exchange", exchange),
			zap.String("label", label),
			zap.Error(err),
		)
		return
	}
	creds, err := s.connSvc.GetDecryptedCredentialsByLabel(ctx, userUID, exchange, label)
	if err != nil {
		s.logger.Error("history backfill: credential decrypt failed",
			zap.String("user_uid", userUID),
			zap.String("exchange", exchange),
			zap.String("label", label),
			zap.Error(err),
		)
		return
	}
	conn, err := s.getOrCreateConnector(connMeta.Exchange, connMeta.UserUID, label, creds)
	if err != nil {
		s.logger.Error("history backfill: connector create failed",
			zap.String("user_uid", userUID),
			zap.String("exchange", exchange),
			zap.Error(err),
		)
		return
	}
	s.reconstructHistory(ctx, connMeta, conn, creds)
}

// reconstructHistory routes a freshly built connection to its history
// backfill path. Connectors implementing HistoricalSnapshotProvider (IBKR)
// reconstruct in-enclave — signed by the report chain, never leaving the
// SEV-SNP perimeter. Every other connector hands off to the external
// rebuilder, which is where plaintext credentials cross the perimeter
// (SEC-ZK-001). Split from ReconstructHistoryOnConnect so this routing —
// the decision that governs whether credentials leave the enclave — is
// unit-testable without a DB-backed ConnectionService.
func (s *SyncService) reconstructHistory(ctx context.Context, connMeta *repository.ExchangeConnection, conn connector.Connector, creds *Credentials) {
	if hsp, ok := conn.(connector.HistoricalSnapshotProvider); ok {
		// IBKR (and any future ZK-native provider) keeps the in-enclave path:
		// signed by the report chain, stays inside the SEV-SNP perimeter.
		s.syncFromHistoricalProvider(ctx, connMeta, hsp)
		s.notifyHistoryRebuilt(ctx, connMeta.UserUID)
		return
	}

	// SEC-ZK-001: fallback for non-ZK exchanges (Hyperliquid, Lighter, …) —
	// hand off to the external history-rebuilder-go. Plaintext creds leave
	// the enclave here. Explicitly accepted tradeoff: historical data is
	// NOT sold as verifiable, the live snapshot path stays in-enclave.
	//
	// The external rebuilder is request/response: it fetches exchange
	// data, computes the daily timeline, and returns the snapshots in the
	// HTTP response. The aggregator (this code) is the sole writer of
	// user_snapshots — the rebuilder never touches the aggregator's DB.
	if s.rebuilder == nil || !s.rebuilder.Configured() {
		s.logger.Debug("history backfill: rebuilder not configured, skipping",
			zap.String("user_uid", connMeta.UserUID),
			zap.String("exchange", connMeta.Exchange),
		)
		return
	}
	s.logger.Info("history backfill: dispatching to external rebuilder",
		zap.String("user_uid", connMeta.UserUID),
		zap.String("exchange", connMeta.Exchange),
	)
	res, err := s.rebuilder.Rebuild(ctx, rebuilderclient.RebuildRequest{
		UserUID:  connMeta.UserUID,
		Exchange: connMeta.Exchange,
		Label:    connMeta.Label,
		Credentials: rebuilderclient.Credentials{
			// LOG-CREDS-001: this struct holds plaintext credentials. Once
			// Rebuild returns, the local reference (`creds`) is dropped
			// below; never log this struct or fmt.Sprintf it.
			WalletAddress: creds.APIKey, // HL stores wallet in APIKey; harmless for others (rebuilder picks the right field per exchange)
			APIKey:        creds.APIKey,
			APISecret:     creds.APISecret,
			Passphrase:    creds.Passphrase,
		},
	})
	// LOG-CREDS-001: drop the local reference promptly. Best-effort — Go
	// strings are immutable so the original heap allocation may live until
	// GC, but this prevents accidental reuse of `creds` later in this scope.
	creds = nil
	if err != nil {
		s.logger.Error("history backfill: rebuilder request failed",
			zap.String("user_uid", connMeta.UserUID),
			zap.String("exchange", connMeta.Exchange),
			zap.Error(err),
		)
		return
	}
	s.logger.Info("history backfill: rebuilder returned snapshots",
		zap.String("user_uid", connMeta.UserUID),
		zap.String("exchange", connMeta.Exchange),
		zap.Int("snapshot_count", len(res.Snapshots)),
		zap.Int64("rebuild_duration_ms", res.DurationMs),
	)

	firstSync := s.isFirstSync(ctx, connMeta)
	s.persistHistoricalSnapshots(ctx, connMeta, res.Snapshots, firstSync, sourceExternalRebuilder)
	s.notifyHistoryRebuilt(ctx, connMeta.UserUID)
}

// syncIbkrFromFlex upserts every daily snapshot returned by the user's Flex
// Query, EXCEPT today's bucket — that one is built by the live branch
// (GetBalance + 24h trades) right after this call returns, so writing it
// from Flex first would be overwritten and would race the live writer.
//
// Each Flex-derived snapshot is marked is_historical=true so consumers can
// distinguish reconstructed days (daily summary, no per-trade detail) from
// live days (full trade breakdown).
//
// The Query period is configured user-side (LastBusinessWeek, YTD, custom
// range…), so we trust whatever window Flex hands back. A user who widens
// their Flex query (e.g. 30d → 365d) sees the new earlier days flow into
// the DB on the next sync — we log "history reconstruction detected" the
// first time we see Flex go further back than what we already have.
func (s *SyncService) syncFromHistoricalProvider(ctx context.Context, connMeta *repository.ExchangeConnection, provider connector.HistoricalSnapshotProvider) {
	firstSync := s.isFirstSync(ctx, connMeta)
	if firstSync {
		s.logger.Info("history backfill — first sync",
			zap.String("user_uid", connMeta.UserUID),
			zap.String("exchange", connMeta.Exchange),
			zap.String("label", connMeta.Label),
		)
	} else {
		s.logger.Info("history reconstruction sync",
			zap.String("user_uid", connMeta.UserUID),
			zap.String("exchange", connMeta.Exchange),
			zap.String("label", connMeta.Label),
		)
	}

	historicalSnapshots, err := provider.GetHistoricalSnapshots(ctx, time.Time{})
	if err != nil {
		s.logger.Error("historical snapshots fetch failed",
			zap.String("user_uid", connMeta.UserUID),
			zap.String("exchange", connMeta.Exchange),
			zap.Error(err),
		)
		return
	}

	s.persistHistoricalSnapshots(ctx, connMeta, historicalSnapshots, firstSync, sourceInEnclave)
}

// persistHistoricalSnapshots is the upsert loop shared between the
// in-enclave IBKR path and the external rebuilder path. The aggregator
// owns the writes to snapshot_data — neither the connector nor the
// external rebuilder writes user data directly.
func (s *SyncService) persistHistoricalSnapshots(
	ctx context.Context,
	connMeta *repository.ExchangeConnection,
	historicalSnapshots []*connector.HistoricalSnapshot,
	firstSync bool,
	source string,
) {
	// Today is owned by the live branch — never reconstruct it.
	now := time.Now().UTC()
	todayKey := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)

	s.logHistoryExpansion(ctx, connMeta, historicalSnapshots, firstSync)

	snapshots, skippedToday := buildHistoricalSnapshots(connMeta, historicalSnapshots, todayKey, source == sourceExternalRebuilder)
	if len(snapshots) == 0 {
		return
	}

	// ENG-002: write the whole reconstructed series in ONE transaction. The
	// previous per-row Upsert loop left a silent hole in the timeline when a
	// single day failed, and a holed series quietly skews the report's TWR.
	// UpsertBatch is all-or-nothing — and the non-IBKR backfill fires only once
	// at connection time and is never re-attempted — so absorb a transient DB
	// blip with a bounded retry rather than permanently leaving the connection
	// without history. IBKR re-runs every sync and self-heals regardless.
	err := retryWithBackoff(ctx, 3, time.Second, func() error {
		return s.snapshotRepo.UpsertBatch(ctx, snapshots)
	})
	if err != nil {
		s.logger.Error("history reconstruction failed — transaction rolled back, no snapshots written",
			zap.String("user_uid", connMeta.UserUID),
			zap.String("exchange", connMeta.Exchange),
			zap.String("label", connMeta.Label),
			zap.String("source", source),
			zap.Int("total_days", len(snapshots)),
			zap.String("hint", "transient retries exhausted; re-create the connection to re-run the backfill"),
			zap.Error(err),
		)
		return
	}

	s.logger.Info("history reconstruction completed",
		zap.String("user_uid", connMeta.UserUID),
		zap.String("exchange", connMeta.Exchange),
		zap.String("source", source),
		zap.Int("snapshots_upserted", len(snapshots)),
		zap.Int("skipped_today", skippedToday),
		zap.Int("total_days", len(historicalSnapshots)),
	)
}

// retryWithBackoff calls fn up to maxAttempts times, sleeping attempt*base
// between tries and aborting early if ctx is cancelled. Returns nil on the
// first success, otherwise the error from the final attempt.
func retryWithBackoff(ctx context.Context, maxAttempts int, base time.Duration, fn func() error) error {
	var err error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err = fn(); err == nil {
			return nil
		}
		if attempt == maxAttempts {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(attempt) * base):
		}
	}
	return err
}

// buildHistoricalSnapshots maps connector daily summaries to repo Snapshots
// marked is_historical=true, skipping the today bucket (owned by the live
// branch). Pure function — no IO — to keep the upsert loop testable.
func buildHistoricalSnapshots(
	connMeta *repository.ExchangeConnection,
	hs []*connector.HistoricalSnapshot,
	todayKey time.Time,
	fromExternalRebuilder bool,
) (snapshots []*repository.Snapshot, skippedToday int) {
	for _, h := range hs {
		dayKey := time.Date(h.Date.Year(), h.Date.Month(), h.Date.Day(), 0, 0, 0, 0, time.UTC)
		if dayKey.Equal(todayKey) {
			skippedToday++
			continue
		}

		breakdown := &repository.MarketBreakdown{}
		var totalAvailMargin float64
		for mt, mb := range h.Breakdown {
			metrics := &repository.MarketMetrics{
				Equity:          mb.Equity,
				AvailableMargin: mb.AvailableMargin,
			}
			totalAvailMargin += mb.AvailableMargin
			switch mt {
			case connector.MarketStocks:
				breakdown.Stocks = metrics
			case connector.MarketOptions:
				breakdown.Options = metrics
			case connector.MarketFutures:
				breakdown.Futures = metrics
			case connector.MarketCFD:
				breakdown.CFD = metrics
			case connector.MarketForex:
				breakdown.Forex = metrics
			case connector.MarketSwap:
				breakdown.Swap = metrics
			case connector.MarketSpot:
				breakdown.Spot = metrics
			}
		}
		// TS-compat global aggregate: dashboard reads breakdown.global.equity
		// (without it IBKR equity shows as 0 on the frontend) AND
		// breakdown.global.volume / .trades / .trading_fees for the per-day
		// activity widgets. The rebuilder doesn't split volume/trades per
		// market type, so they live only at the global level for historical
		// snapshots; live-sync still populates per-market breakdowns from
		// the actual fill stream.
		breakdown.Global = &repository.MarketMetrics{
			Equity:          h.TotalEquity,
			AvailableMargin: totalAvailMargin,
			Volume:          h.TotalVolume,
			Trades:          h.TotalTrades,
			TradingFees:     h.TotalFees,
		}

		snapshots = append(snapshots, &repository.Snapshot{
			UserUID:         connMeta.UserUID,
			Exchange:        connMeta.Exchange,
			Label:           connMeta.Label,
			Timestamp:       dayKey,
			TotalEquity:     h.TotalEquity,
			RealizedBalance: h.RealizedBalance,
			UnrealizedPnL:   h.TotalEquity - h.RealizedBalance,
			Deposits:        h.Deposits,
			Withdrawals:     h.Withdrawals,
			TotalTrades:     h.TotalTrades,
			TotalVolume:     h.TotalVolume,
			TotalFees:       h.TotalFees,
			Breakdown:       breakdown,
			IsHistorical:    true,
			// PayloadVersion 1.3 surfaces this flag per-day in signed reports;
			// the report builder labels each daily return as
			// "rebuilder-service" or "in-enclave" accordingly so verifiers
			// can apply their own trust policy.
			FromExternalRebuilder: fromExternalRebuilder,
		})
	}

	// Inception-deposit convention (UX-001) for historical reconstructions:
	// the earliest reconstructed day inherits its TotalEquity as an inception
	// deposit unless the connector already reported a non-zero cashflow.
	// Without this the dashboard's cumulative-return curve has no base
	// reference, just like the live-sync first-snapshot case. The connector
	// list is small enough (IBKR Flex, history-rebuilder for HL/Lighter/MEXC)
	// that a single "patch the earliest entry" pass is enough — no need to
	// sort by date since both sources emit a chronologically sorted slice
	// today, but use min() in case that drifts.
	if len(snapshots) > 0 {
		earliest := snapshots[0]
		for _, s := range snapshots[1:] {
			if s.Timestamp.Before(earliest.Timestamp) {
				earliest = s
			}
		}
		if earliest.Deposits == 0 && earliest.TotalEquity > 0 {
			earliest.Deposits = earliest.TotalEquity
		}
	}

	return snapshots, skippedToday
}

// isFirstSync returns true only when this connection has NEVER produced any
// snapshot — neither via a live sync (sync_status row) nor via a historical
// rebuild (snapshot_data rows). Without the second check, a freshly re-added
// connection that just got 156 days of rebuilt history would still appear as
// "first sync" on the next daily run, causing the inception-deposit convention
// (UX-001) to fire a SECOND time and inflate today's deposits to today's full
// equity (visible as a $5K spike at the right edge of the equity chart).
//
// Falls back to "not first" if neither repo is wired or both lookups error —
// better to under-log than to spuriously claim "first".
func (s *SyncService) isFirstSync(ctx context.Context, connMeta *repository.ExchangeConnection) bool {
	if s.syncStatus == nil {
		return false
	}
	// Primary: sync_status row presence (live sync ever completed).
	if _, err := s.syncStatus.GetByUserExchangeLabel(ctx, connMeta.UserUID, connMeta.Exchange, connMeta.Label); err != repository.ErrNotFound {
		return false
	}
	// Fallback: historical snapshots from the rebuilder already exist, so the
	// inception deposit was set in buildHistoricalSnapshots — don't re-apply.
	if s.snapshotRepo != nil {
		if hasAny, err := s.snapshotRepo.ExistsForUserExchangeLabel(ctx, connMeta.UserUID, connMeta.Exchange, connMeta.Label); err == nil && hasAny {
			return false
		}
	}
	return true
}

// logHistoryExpansion compares the earliest day Flex returned to the earliest
// day already in the DB for this connection. If Flex now reaches further
// back, log it — that signals the user widened their Flex query window and
// the aggregator is reconstructing the new days.
func (s *SyncService) logHistoryExpansion(ctx context.Context, connMeta *repository.ExchangeConnection, hs []*connector.HistoricalSnapshot, firstSync bool) {
	if firstSync || len(hs) == 0 {
		return
	}

	var minFlex time.Time
	for _, h := range hs {
		if minFlex.IsZero() || h.Date.Before(minFlex) {
			minFlex = h.Date
		}
	}

	minDB, err := s.snapshotRepo.GetEarliestTimestamp(ctx, connMeta.UserUID, connMeta.Exchange, connMeta.Label)
	if err != nil {
		// ErrNotFound here means firstSync slipped through (e.g. sync_status
		// missing for an existing user). Either way nothing to compare.
		return
	}

	if minFlex.Before(minDB) {
		s.logger.Info("IBKR Flex: history reconstruction detected",
			zap.String("user_uid", connMeta.UserUID),
			zap.String("exchange", connMeta.Exchange),
			zap.String("label", connMeta.Label),
			zap.String("db_min", minDB.Format("2006-01-02")),
			zap.String("flex_min", minFlex.Format("2006-01-02")),
			zap.Int("days_added", int(minDB.Sub(minFlex).Hours()/24)),
		)
	}
}

// aggregatedBreakdown holds aggregated trade data
type aggregatedBreakdown struct {
	stocks      marketAgg
	spot        marketAgg
	swap        marketAgg
	futures     marketAgg
	options     marketAgg
	margin      marketAgg
	earn        marketAgg
	cfd         marketAgg
	forex       marketAgg
	commodities marketAgg
}

type marketAgg struct {
	equity          float64
	availableMargin float64
	volume          float64
	trades          int
	fees            float64
	fundingFees     float64
	longTrades      int
	shortTrades     int
	longVolume      float64
	shortVolume     float64
}

func (s *SyncService) aggregateTrades(trades []*connector.Trade) *aggregatedBreakdown {
	agg := &aggregatedBreakdown{}

	for _, t := range trades {
		volume := t.Price * t.Quantity
		ma := &agg.spot

		switch t.MarketType {
		case connector.MarketStocks:
			ma = &agg.stocks
		case connector.MarketSwap:
			ma = &agg.swap
		case connector.MarketFutures:
			ma = &agg.futures
		case connector.MarketOptions:
			ma = &agg.options
		case connector.MarketMargin:
			ma = &agg.margin
		case connector.MarketEarn:
			ma = &agg.earn
		case connector.MarketCFD:
			ma = &agg.cfd
		case connector.MarketForex:
			ma = &agg.forex
		case connector.MarketCommodities:
			ma = &agg.commodities
		}

		ma.volume += volume
		ma.trades++
		ma.fees += t.Fee

		if t.Side == "buy" || t.Side == "long" {
			ma.longTrades++
			ma.longVolume += volume
		} else if t.Side == "sell" || t.Side == "short" {
			ma.shortTrades++
			ma.shortVolume += volume
		}
	}

	return agg
}

func (m *marketAgg) toRepoMetrics() *repository.MarketMetrics {
	return &repository.MarketMetrics{
		Equity:          m.equity,
		AvailableMargin: m.availableMargin,
		Volume:          m.volume,
		Trades:          m.trades,
		TradingFees:     m.fees,
		FundingFees:     m.fundingFees,
		LongTrades:      m.longTrades,
		ShortTrades:     m.shortTrades,
		LongVolume:      m.longVolume,
		ShortVolume:     m.shortVolume,
	}
}

// getOrCreateConnector returns a cached connector or creates a new one.
// TS parity: UniversalConnectorCache with SHA-256 credentials hash.
func (s *SyncService) getOrCreateConnector(exchange, userUID, label string, creds *Credentials) (connector.Connector, error) {
	credsHash := cache.HashCredentials(creds.APIKey, creds.APISecret, creds.Passphrase)

	// Check cache first
	if s.connCache != nil {
		if cached := s.connCache.Get(exchange, userUID, credsHash); cached != nil {
			return cached, nil
		}
	}

	// Create new connector
	conn, err := s.factory.Create(&connector.Credentials{
		Exchange:   exchange,
		APIKey:     creds.APIKey,
		APISecret:  creds.APISecret,
		Passphrase: creds.Passphrase,
	})
	if err != nil {
		return nil, err
	}

	// Wire token persistence for OAuth connectors so refreshed tokens survive
	// container restarts. Without this, every boot starts from the original
	// (possibly expired) access_token stored in DB.
	if tr, ok := conn.(connector.TokenRefreshable); ok && s.connSvc != nil {
		tr.SetTokenPersister(func(ctx context.Context, accessToken, refreshToken string) error {
			return s.connSvc.PersistOAuthTokens(ctx, userUID, exchange, label, accessToken, refreshToken)
		})
	}

	// Store in cache
	if s.connCache != nil {
		s.connCache.Put(exchange, userUID, credsHash, conn)
	}

	return conn, nil
}

// DumpCashflows fetches BALANCE deals (deposits/withdrawals) from the broker
// for a user over a wider date range than the normal daily sync window.
// Intended for admin backfills after a bug affected cashflow capture.
func (s *SyncService) DumpCashflows(
	ctx context.Context,
	userUID, exchange, label string,
	since time.Time,
) ([]*connector.Cashflow, error) {
	if s.connSvc == nil {
		return nil, fmt.Errorf("connection service not configured")
	}

	creds, err := s.connSvc.GetDecryptedCredentialsByLabel(ctx, userUID, exchange, label)
	if err != nil {
		return nil, fmt.Errorf("decrypt credentials: %w", err)
	}

	conn, err := s.getOrCreateConnector(strings.ToLower(exchange), userUID, label, creds)
	if err != nil {
		return nil, fmt.Errorf("build connector: %w", err)
	}

	cfFetcher, ok := conn.(connector.CashflowFetcher)
	if !ok {
		return nil, fmt.Errorf("connector %s does not support cashflow fetching", exchange)
	}

	return cfFetcher.GetCashflows(ctx, since)
}

func appendUnique(slice []string, s string) []string {
	for _, v := range slice {
		if v == s {
			return slice
		}
	}
	return append(slice, s)
}

// hasAnyEquity reports whether at least one market bucket has equity data.
// Used to decide whether the breakdown needs a fallback equity assignment.
func (a *aggregatedBreakdown) hasAnyEquity() bool {
	return a.stocks.equity > 0 || a.spot.equity > 0 || a.swap.equity > 0 ||
		a.futures.equity > 0 || a.options.equity > 0 || a.margin.equity > 0 ||
		a.earn.equity > 0 || a.cfd.equity > 0 || a.forex.equity > 0 || a.commodities.equity > 0
}

// primaryMarketType returns the canonical market type for a given exchange.
// Used as fallback when BalanceByMarketFetcher is not implemented — equity is
// assigned to this bucket so breakdown_by_market.global is never nil.
func primaryMarketType(exchange string) string {
	switch strings.ToLower(exchange) {
	case "alpaca", "ibkr":
		return connector.MarketStocks
	case "ctrader", "mt4", "mt5":
		return connector.MarketCFD
	default:
		return connector.MarketSpot
	}
}

func (m *marketAgg) hasData() bool {
	return m.trades > 0 || m.equity > 0 || m.availableMargin > 0
}

func (a *aggregatedBreakdown) getOrCreateMarket(marketType string) *marketAgg {
	switch marketType {
	case connector.MarketStocks:
		return &a.stocks
	case connector.MarketSwap:
		return &a.swap
	case connector.MarketFutures:
		return &a.futures
	case connector.MarketOptions:
		return &a.options
	case connector.MarketMargin:
		return &a.margin
	case connector.MarketEarn:
		return &a.earn
	case connector.MarketCFD:
		return &a.cfd
	case connector.MarketForex:
		return &a.forex
	case connector.MarketCommodities:
		return &a.commodities
	default:
		return &a.spot
	}
}

func (a *aggregatedBreakdown) totalVolume() float64 {
	return a.stocks.volume + a.spot.volume + a.swap.volume + a.futures.volume + a.options.volume +
		a.margin.volume + a.earn.volume + a.cfd.volume + a.forex.volume + a.commodities.volume
}

func (a *aggregatedBreakdown) totalFees() float64 {
	return a.stocks.fees + a.spot.fees + a.swap.fees + a.futures.fees + a.options.fees +
		a.margin.fees + a.earn.fees + a.cfd.fees + a.forex.fees + a.commodities.fees
}

// toRepo converts the aggregated breakdown to repository format and, if
// globalEquity > 0, populates the `global` aggregate that TS consumers
// (frontend dashboard, analytics-service) read to get total equity, volume
// and fees. Without global, the breakdown is Go-only and the dashboard
// displays 0 for users synced by the Go enclave.
func (a *aggregatedBreakdown) toRepo(globalEquity, globalAvailableMargin float64, totalTrades int) *repository.MarketBreakdown {
	breakdown := &repository.MarketBreakdown{}

	if a.stocks.hasData() {
		breakdown.Stocks = a.stocks.toRepoMetrics()
	}

	if a.spot.hasData() {
		breakdown.Spot = a.spot.toRepoMetrics()
	}

	if a.swap.hasData() {
		breakdown.Swap = a.swap.toRepoMetrics()
	}

	if a.futures.hasData() {
		breakdown.Futures = a.futures.toRepoMetrics()
	}

	if a.options.hasData() {
		breakdown.Options = a.options.toRepoMetrics()
	}

	if a.margin.hasData() {
		breakdown.Margin = a.margin.toRepoMetrics()
	}

	if a.earn.hasData() {
		breakdown.Earn = a.earn.toRepoMetrics()
	}

	if a.cfd.hasData() {
		breakdown.CFD = a.cfd.toRepoMetrics()
	}

	if a.forex.hasData() {
		breakdown.Forex = a.forex.toRepoMetrics()
	}

	if a.commodities.hasData() {
		breakdown.Commodities = a.commodities.toRepoMetrics()
	}

	// TS-compat global aggregate: dashboard reads breakdown.global.equity
	// (falls back from .totalEquityUsd), so we must always set it when we
	// know the total equity.
	if globalEquity > 0 || totalTrades > 0 {
		breakdown.Global = &repository.MarketMetrics{
			Equity:          globalEquity,
			AvailableMargin: globalAvailableMargin,
			Volume:          a.totalVolume(),
			Trades:          totalTrades,
			TradingFees:     a.totalFees(), // toRepoMetrics splits fees by kind; aggregate only keeps total
		}
	}

	return breakdown
}

func aggregateSyncResults(userUID, exchange string, results []*SyncResult) *SyncResult {
	out := &SyncResult{
		UserUID:  userUID,
		Exchange: exchange,
		Success:  false,
	}
	if len(results) == 0 {
		out.Error = "no sync results"
		return out
	}

	var latest *SyncResult
	var errs []string
	for _, r := range results {
		if r == nil {
			continue
		}
		out.TradeCount += r.TradeCount
		if r.Success {
			out.Success = true
			if latest == nil || r.SnapshotTimestamp.After(latest.SnapshotTimestamp) {
				latest = r
			}
		}
		if r.Error != "" {
			errs = append(errs, fmt.Sprintf("%s/%s: %s", r.Exchange, r.Label, r.Error))
		}
	}

	if latest != nil {
		out.SnapshotEquity = latest.SnapshotEquity
		out.SnapshotTimestamp = latest.SnapshotTimestamp
	}
	if len(errs) > 0 {
		out.Error = strings.Join(errs, " | ")
	}
	if !out.Success && out.Error == "" {
		out.Error = "sync failed for all connections"
	}

	return out
}
