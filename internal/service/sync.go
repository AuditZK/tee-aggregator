package service

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/trackrecord/enclave/internal/cache"
	"github.com/trackrecord/enclave/internal/connector"
	"github.com/trackrecord/enclave/internal/repository"
	"go.uber.org/zap"
)

// SyncService orchestrates exchange synchronization
type SyncService struct {
	connSvc      *ConnectionService
	snapshotRepo *repository.SnapshotRepo
	syncStatus   *repository.SyncStatusRepo
	factory      *connector.Factory
	connCache    *cache.ConnectorCache
	logger       *zap.Logger
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
		return nil, fmt.Errorf("get connections: %w", err)
	}

	if len(connections) == 0 {
		return nil, fmt.Errorf("no active connections for user %s", userUID)
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
			if !s.isManualSyncAllowed(ctx, userUID, c.Exchange, c.Label) {
				result = &SyncResult{
					UserUID:  userUID,
					Exchange: c.Exchange,
					Label:    c.Label,
					Error:    "manual sync blocked: snapshot already exists. Only the hourly scheduler can sync after initial snapshot.",
				}
			} else {
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
		return nil, fmt.Errorf("get connections: %w", err)
	}

	if len(connections) == 0 {
		return nil, fmt.Errorf("no active connections for user %s", userUID)
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
		if !s.isManualSyncAllowed(ctx, userUID, conn.Exchange, conn.Label) {
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
// Returns false if any snapshot already exists for the user+exchange+label.
func (s *SyncService) isManualSyncAllowed(ctx context.Context, userUID, exchange, label string) bool {
	// Check if any snapshot exists for this user+exchange+label.
	snapshots, err := s.snapshotRepo.GetByUserAndDateRange(ctx, userUID,
		time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Now().UTC(),
	)
	if err != nil {
		// On error, allow sync (fail open for first-time users)
		return true
	}

	for _, snap := range snapshots {
		if snap.Exchange == exchange && snap.Label == label {
			return false
		}
	}
	return true
}

func (s *SyncService) getConnectionsByExchange(ctx context.Context, userUID, exchange string) ([]*repository.ExchangeConnection, error) {
	connections, err := s.connSvc.GetActiveConnections(ctx, userUID)
	if err != nil {
		return nil, fmt.Errorf("get connections: %w", err)
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
	conn, err := s.getOrCreateConnector(connMeta.Exchange, connMeta.UserUID, creds)
	if err != nil {
		result.Error = fmt.Sprintf("create connector: %v", err)
		return result
	}

	// 2b. IBKR: Auto-backfill 365 days on first sync (TS parity)
	if strings.ToLower(connMeta.Exchange) == "ibkr" {
		s.backfillIbkrIfNeeded(ctx, connMeta, conn)
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

	// 4. Get trades for today (memory only - will be aggregated then discarded)
	now := time.Now().UTC()
	startOfDay := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)

	// 4a. Per-market trade fetching if supported; otherwise fallback to flat GetTrades
	var trades []*connector.Trade
	var swapSymbols []string
	if pmFetcher, ok := conn.(connector.PerMarketTradeFetcher); ok {
		if detector, ok2 := conn.(connector.MarketTypeDetector); ok2 {
			if marketTypes, err := detector.DetectMarketTypes(ctx); err == nil {
				for _, mt := range marketTypes {
					mtTrades, err := pmFetcher.GetTradesByMarket(ctx, mt, startOfDay)
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
		trades, _ = conn.GetTrades(ctx, startOfDay, now)
	}

	// 5. Aggregate trades by market type
	breakdown := s.aggregateTrades(trades)

	// 5a. Fetch funding fees for swap positions
	if ffFetcher, ok := conn.(connector.FundingFeesFetcher); ok && len(swapSymbols) > 0 {
		if fees, err := ffFetcher.GetFundingFees(ctx, swapSymbols, startOfDay); err == nil {
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

	// 6. Fetch deposits/withdrawals if connector supports it
	var deposits, withdrawals float64
	if cfFetcher, ok := conn.(connector.CashflowFetcher); ok {
		cashflows, err := cfFetcher.GetCashflows(ctx, startOfDay)
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
		}
	}

	// 8. Create snapshot
	snapshot := &repository.Snapshot{
		UserUID:         connMeta.UserUID,
		Exchange:        connMeta.Exchange,
		Label:           connMeta.Label,
		Timestamp:       startOfDay,
		TotalEquity:     balance.Equity,
		RealizedBalance: balance.Available,
		UnrealizedPnL:   balance.UnrealizedPnL,
		Deposits:        deposits,
		Withdrawals:     withdrawals,
		TotalTrades:     len(trades),
		TotalVolume:     breakdown.totalVolume(),
		TotalFees:       breakdown.totalFees(),
		Breakdown:       breakdown.toRepo(),
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
		return nil, fmt.Errorf("get connections: %w", err)
	}

	if len(connections) == 0 {
		return nil, fmt.Errorf("no active connections for user %s", userUID)
	}

	// Phase 1: Build snapshots with limited concurrency (max 2 per user).
	// CCXT connectors load markets (~40MB each), so 10 in parallel = OOM on small VMs.
	var (
		results []*SyncResult
		mu      sync.Mutex
		wg      sync.WaitGroup
	)

	connSem := make(chan struct{}, 2) // Max 2 concurrent connections per user
	const connTimeout = 2 * time.Minute  // Max 2 min per connection (IBKR polls can hang)

	for _, conn := range connections {
		if !s.isConnectionDue(ctx, conn, now) {
			continue
		}

		wg.Add(1)
		go func(c *repository.ExchangeConnection) {
			defer wg.Done()
			connSem <- struct{}{}
			defer func() { <-connSem }()

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
			s.logger.Error("connection sync failed",
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
		s.logger.Error("snapshot build failed", zap.String("exchange", connMeta.Exchange), zap.String("step", "decrypt"), zap.Duration("elapsed", time.Since(start)), zap.Error(err))
		return result
	}

	conn, err := s.getOrCreateConnector(connMeta.Exchange, connMeta.UserUID, creds)
	if err != nil {
		result.Error = fmt.Sprintf("create connector: %v", err)
		s.logger.Error("snapshot build failed", zap.String("exchange", connMeta.Exchange), zap.String("step", "connector"), zap.Duration("elapsed", time.Since(start)), zap.Error(err))
		return result
	}

	balance, err := conn.GetBalance(ctx)
	if err != nil {
		result.Error = fmt.Sprintf("get balance: %v", err)
		s.logger.Error("snapshot build failed", zap.String("exchange", connMeta.Exchange), zap.String("label", connMeta.Label), zap.String("step", "get_balance"), zap.Duration("elapsed", time.Since(start)), zap.Error(err))
		return result
	}
	s.logger.Info("balance fetched", zap.String("exchange", connMeta.Exchange), zap.String("label", connMeta.Label), zap.Duration("elapsed", time.Since(start)))

	now := time.Now().UTC()
	startOfDay := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)

	var trades []*connector.Trade
	var swapSymbols []string
	if pmFetcher, ok := conn.(connector.PerMarketTradeFetcher); ok {
		if detector, ok2 := conn.(connector.MarketTypeDetector); ok2 {
			if marketTypes, err := detector.DetectMarketTypes(ctx); err == nil {
				for _, mt := range marketTypes {
					mtTrades, err := pmFetcher.GetTradesByMarket(ctx, mt, startOfDay)
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
		trades, _ = conn.GetTrades(ctx, startOfDay, now)
	}

	breakdown := s.aggregateTrades(trades)

	if ffFetcher, ok := conn.(connector.FundingFeesFetcher); ok && len(swapSymbols) > 0 {
		if fees, err := ffFetcher.GetFundingFees(ctx, swapSymbols, startOfDay); err == nil {
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
		if cashflows, err := cfFetcher.GetCashflows(ctx, startOfDay); err == nil {
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
		}
	}

	result.snapshot = &repository.Snapshot{
		UserUID:         connMeta.UserUID,
		Exchange:        connMeta.Exchange,
		Label:           connMeta.Label,
		Timestamp:       startOfDay,
		TotalEquity:     balance.Equity,
		RealizedBalance: balance.Available,
		UnrealizedPnL:   balance.UnrealizedPnL,
		Deposits:        deposits,
		Withdrawals:     withdrawals,
		TotalTrades:     len(trades),
		TotalVolume:     breakdown.totalVolume(),
		TotalFees:       breakdown.totalFees(),
		Breakdown:       breakdown.toRepo(),
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

// backfillIbkrIfNeeded checks if this is the first IBKR sync (0 snapshots)
// and if so, fetches up to 365 days of historical equity data from Flex.
func (s *SyncService) backfillIbkrIfNeeded(ctx context.Context, connMeta *repository.ExchangeConnection, conn connector.Connector) {
	// Check if any snapshots exist for this connection
	existing, err := s.snapshotRepo.GetByUserAndDateRange(ctx, connMeta.UserUID,
		time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Now().UTC(),
	)
	if err == nil {
		for _, snap := range existing {
			if snap.Exchange == connMeta.Exchange && snap.Label == connMeta.Label {
				return // Snapshots already exist, skip backfill
			}
		}
	}

	provider, ok := conn.(connector.HistoricalSnapshotProvider)
	if !ok {
		return
	}

	since := time.Now().UTC().AddDate(-1, 0, 0) // 365 days ago
	s.logger.Info("IBKR first sync: running historical backfill",
		zap.String("user_uid", connMeta.UserUID),
		zap.String("exchange", connMeta.Exchange),
		zap.String("label", connMeta.Label),
	)

	historicalSnapshots, err := provider.GetHistoricalSnapshots(ctx, since)
	if err != nil {
		s.logger.Error("IBKR backfill failed",
			zap.String("user_uid", connMeta.UserUID),
			zap.Error(err),
		)
		return
	}

	processed := 0
	for _, hs := range historicalSnapshots {
		// Build market breakdown from historical data (TS parity)
		var breakdown *repository.MarketBreakdown
		if len(hs.Breakdown) > 0 {
			breakdown = &repository.MarketBreakdown{}
			for mt, mb := range hs.Breakdown {
				metrics := &repository.MarketMetrics{
					Equity:          mb.Equity,
					AvailableMargin: mb.AvailableMargin,
				}
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
				}
			}
		}

		snapshot := &repository.Snapshot{
			UserUID:         connMeta.UserUID,
			Exchange:        connMeta.Exchange,
			Label:           connMeta.Label,
			Timestamp:       hs.Date,
			TotalEquity:     hs.TotalEquity,
			RealizedBalance: hs.RealizedBalance,
			UnrealizedPnL:   hs.TotalEquity - hs.RealizedBalance,
			Deposits:        hs.Deposits,
			Withdrawals:     hs.Withdrawals,
			Breakdown:       breakdown,
		}

		if err := s.snapshotRepo.Upsert(ctx, snapshot); err != nil {
			s.logger.Warn("IBKR backfill: failed to save snapshot",
				zap.String("date", hs.Date.Format("2006-01-02")),
				zap.Error(err),
			)
			continue
		}
		processed++
	}

	s.logger.Info("IBKR historical backfill completed",
		zap.String("user_uid", connMeta.UserUID),
		zap.Int("snapshots_created", processed),
		zap.Int("total_days", len(historicalSnapshots)),
	)
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
func (s *SyncService) getOrCreateConnector(exchange, userUID string, creds *Credentials) (connector.Connector, error) {
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

	// Store in cache
	if s.connCache != nil {
		s.connCache.Put(exchange, userUID, credsHash, conn)
	}

	return conn, nil
}

func appendUnique(slice []string, s string) []string {
	for _, v := range slice {
		if v == s {
			return slice
		}
	}
	return append(slice, s)
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

func (a *aggregatedBreakdown) toRepo() *repository.MarketBreakdown {
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
