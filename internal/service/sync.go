package service

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/trackrecord/enclave/internal/cache"
	"github.com/trackrecord/enclave/internal/connector"
	"github.com/trackrecord/enclave/internal/errsanitize"
	"github.com/trackrecord/enclave/internal/rebuilderclient"
	"github.com/trackrecord/enclave/internal/repository"
	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"
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

const (
	// defaultActivityWindow is the classic lookback for trades, cashflows and
	// funding fees: the 24h ending at the snapshot boundary. It stays the floor
	// — activityWindowStart only ever widens past it, never narrows.
	defaultActivityWindow = 24 * time.Hour

	// maxActivityLookback caps how far back a catch-up sync may reach. Several
	// connectors reject or heavily paginate very wide ranges, so a connection
	// dormant for a year must not ask for a year of trades in one call. Days
	// older than this stay unobserved — they are already lost to the gap that
	// created them.
	maxActivityLookback = 30 * 24 * time.Hour
)

// activityWindowStart returns the instant from which this sync pulls trades,
// cashflows and funding fees.
//
// This used to be a fixed `startOfDay - 24h`, which silently assumed the
// previous sync ran yesterday. Whenever a sync is missed — host reboot, outage,
// a rate-limited connector — the intervening days were never queried and their
// cashflows were lost for good. That is not merely missing detail: TWR computes
// adjustedEquity as `equity - deposits + withdrawals`, so a deposit nobody
// fetched is indistinguishable from trading profit, and an unfetched withdrawal
// reads as a loss. The equity curve absorbs the error permanently.
//
// Anchoring on the connection's last snapshot makes the window span exactly the
// unobserved period. Two deliberate bounds:
//
//   - Never NARROWER than defaultActivityWindow. A snapshot written mid-day (a
//     manual re-sync) must not shrink the window and drop activity the previous
//     snapshot never accounted for.
//   - Never WIDER than maxActivityLookback, so a long-dormant connection cannot
//     issue an unbounded history request.
//
// Trade-off worth knowing: after a gap, several days of trades land on the
// catch-up snapshot, inflating its trade count and volume. That is the right
// call — those trades were previously dropped entirely, and the cashflows they
// come with are what keep the return series honest.
func (s *SyncService) activityWindowStart(ctx context.Context, connMeta *repository.ExchangeConnection, startOfDay time.Time) time.Time {
	var last time.Time
	if s.snapshotRepo != nil && connMeta != nil {
		// A failed lookup deliberately degrades to the classic window rather
		// than reaching back blindly, so a flaky DB cannot turn every sync into
		// a month-wide history request.
		if latest, err := s.snapshotRepo.GetLatestByUserExchangeLabel(ctx, connMeta.UserUID, connMeta.Exchange, connMeta.Label); err == nil && latest != nil {
			last = latest.Timestamp.UTC()
		}
	}

	start, widened, clamped := resolveActivityWindow(startOfDay, last)
	switch {
	case clamped:
		s.logger.Warn("activity window clamped — connection unsynced longer than the lookback cap",
			zap.String("user_uid", connMeta.UserUID),
			zap.String("exchange", connMeta.Exchange),
			zap.String("label", connMeta.Label),
			zap.Time("last_snapshot", last),
			zap.Duration("cap", maxActivityLookback),
		)
	case widened:
		s.logger.Info("activity window widened to cover a sync gap",
			zap.String("user_uid", connMeta.UserUID),
			zap.String("exchange", connMeta.Exchange),
			zap.String("label", connMeta.Label),
			zap.Time("last_snapshot", last),
			zap.Duration("window", startOfDay.Sub(start)),
		)
	}
	return start
}

// resolveActivityWindow is the policy behind activityWindowStart, split out as
// a pure function so the clamping rules are testable without a DB. A zero
// lastSnapshot means "no history known".
func resolveActivityWindow(startOfDay, lastSnapshot time.Time) (start time.Time, widened, clamped bool) {
	fallback := startOfDay.Add(-defaultActivityWindow)
	if lastSnapshot.IsZero() || !lastSnapshot.Before(fallback) {
		return fallback, false, false
	}
	if floor := startOfDay.Add(-maxActivityLookback); lastSnapshot.Before(floor) {
		return floor, true, true
	}
	return lastSnapshot, true, false
}

// dayWindow restricts which reconstructed days persistHistoricalSnapshots is
// allowed to write. The zero value means "no restriction" — every day the
// provider or rebuilder returned is upserted, which is what the connect-time
// and midnight-recalibration paths want.
//
// A bounded window exists for gap repair (OPS-004): when a sync interruption
// leaves holes in an otherwise-correct timeline, a full reconstruct would
// upsert the provider's ENTIRE walk — which can span months — over days that
// were already correct. Restricting the write to the missing days repairs the
// hole without rewriting known-good history.
type dayWindow struct {
	from time.Time // inclusive, UTC midnight; zero = unbounded
	to   time.Time // inclusive, UTC midnight; zero = unbounded
}

func (w dayWindow) isSet() bool { return !w.from.IsZero() || !w.to.IsZero() }

// reconstructOpts carries the knobs the gap-repair entrypoints need. The zero
// value is the historical behaviour: full window, real write.
type reconstructOpts struct {
	window dayWindow
	// dryRun runs the whole reconstruct — including the rebuilder round-trip,
	// which for non-ZK exchanges still egresses credentials (SEC-ZK-001) — but
	// logs the resulting days instead of upserting them. Exists because some
	// rebuilder walks are RELATIVE (anchored on current equity), so splicing a
	// few rebuilt days between existing live snapshots can land on a different
	// calibration basis than its neighbours. Inspect, then write.
	dryRun bool
	// collect receives the days a dry run would have written. The log lines
	// cannot carry them: logredact masks every money field, on purpose, so a
	// diagnostic that reports through the log reports nothing. The days go to
	// the loopback caller that asked for them instead, the same channel the
	// raw-statement dump uses and for the same reason.
	collect func([]*repository.Snapshot)
	// coveredFrom is the earliest instant the rebuild that produced these days
	// could see. It is the floor of the prune: a stored day above it that the
	// rebuild no longer produces is stale and goes, one below it was never
	// examined and stays. Zero means the rebuilder stated no horizon, and the
	// prune falls back to owning the whole rebuilt history.
	coveredFrom time.Time
}

// contains reports whether dayKey (a UTC midnight) falls inside the window.
func (w dayWindow) contains(dayKey time.Time) bool {
	if !w.from.IsZero() && dayKey.Before(w.from) {
		return false
	}
	if !w.to.IsZero() && dayKey.After(w.to) {
		return false
	}
	return true
}

// filter keeps only the snapshots whose day falls inside the window.
func (w dayWindow) filter(hs []*connector.HistoricalSnapshot) []*connector.HistoricalSnapshot {
	if !w.isSet() {
		return hs
	}
	out := make([]*connector.HistoricalSnapshot, 0, len(hs))
	for _, h := range hs {
		dayKey := time.Date(h.Date.Year(), h.Date.Month(), h.Date.Day(), 0, 0, 0, 0, time.UTC)
		if w.contains(dayKey) {
			out = append(out, h)
		}
	}
	return out
}

// SANITY-001 collapse guard: exchange backends can answer 200-with-zeros
// during their daily settlement window (observed: Binance fapi at 00:00 UTC
// read both futures-balance endpoints as empty with NO error — invisible to
// the transient-error guard — and two accounts persisted snapshots missing
// their whole USDⓈ-M wallet). When a fresh read collapses versus the last
// persisted snapshot of the SAME connection, re-read once after a delay and
// take the second reading: a transient zero heals, a real crash reads the
// same twice and stands.
const (
	collapseGuardRatio    = 0.5             // re-read when equity < 50% of the last snapshot
	collapseGuardFloorUSD = 100.0           // dust accounts skip the guard (noise)
	collapseGuardDelay    = 2 * time.Minute // long enough to exit a settlement window
	collapseGuardLookback = 5 * 24 * time.Hour
)

// ctraderRecurringReconstructDays bounds cTrader's every-sync reconstruction
// window. The deep historical backfill runs once at connect (since=zero); the
// recurring re-run only needs to re-own the recent boundary day(s) so the live
// path's 24h-window deposit — attributed to the NEXT day — is overwritten by
// the reconstruction's correct calendar-day attribution. Must comfortably
// exceed the max plausible lag between a deposit and the next daily sync.
const ctraderRecurringReconstructDays = 14

// reconstructsEverySync reports whether an exchange's in-enclave history
// provider is re-run on every sync (not just at connect). IBKR's Flex is a
// single cheap call; cTrader re-runs a bounded recent window (see
// everySyncReconstructSince) to self-heal the live/reconstruction boundary
// double-count — its connect-time deposit lands on the reconstruction's
// calendar-day row AND on the next live snapshot's 24h window, so the recurring
// re-run re-emits that boundary day with deposits=0, overwriting the live row.
//
// IMPORTANT: only ever consulted INSIDE a conn.(connector.HistoricalSnapshotProvider)
// type assertion. Non-reconstructed exchanges (Hyperliquid/MEXC/Lighter) never
// satisfy that interface, so their mandatory live 24h cash-flow gap-deposit
// window is untouched. Do NOT promote this to a standalone gate.
func reconstructsEverySync(exchange string) bool {
	e := strings.ToLower(exchange)
	return e == "ibkr" || e == "ctrader"
}

// balanceReconciledExchanges lists exchanges whose live snapshots derive
// capital flows from the settled-balance residual rather than trusting the
// broker's cashflow report alone (see doc/plan-balance-reconciled-cashflow.md).
// An exchange belongs here only once its ledger is proven complete — realized
// P&L + charges reconcile to ~0 residual over an account's life (the
// cmd/ig-probe gate) — otherwise an under-reported fee would surface as a
// phantom withdrawal. IG earns it: its demo top-ups appear in NO transaction
// endpoint, so only the balance reveals them. Add others one at a time, each
// after its own reconciliation check.
var balanceReconciledExchanges = []string{"ig"}

func isBalanceReconciled(exchange string) bool {
	e := strings.ToLower(exchange)
	for _, x := range balanceReconciledExchanges {
		if x == e {
			return true
		}
	}
	return false
}

// everySyncReconstructSince returns the lookback for an every-sync
// reconstruction. IBKR re-emits its full Flex window cheaply (zero = full);
// cTrader's walk is expensive (paginated deals + weekly cash-flow chunks), so
// the recurring re-run is bounded — the deep backfill happens once at connect
// or via the admin reconstruct endpoint (both pass since=zero).
func everySyncReconstructSince(exchange string) time.Time {
	if strings.EqualFold(exchange, "ctrader") {
		return time.Now().UTC().AddDate(0, 0, -ctraderRecurringReconstructDays)
	}
	return time.Time{}
}

// SyncService orchestrates exchange synchronization.
type SyncService struct {
	connSvc      *ConnectionService
	snapshotRepo *repository.SnapshotRepo
	syncStatus   *repository.SyncStatusRepo
	// rateLimitLog optionally journals throttle events (Flex 1018, stale-
	// statement skips) to sync_rate_limit_logs. Nil = log-only, no persistence.
	rateLimitLog *repository.SyncRateLimitLogRepo
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
	// historyNotifyToken, when set, is sent as X-Internal-Token on that ping.
	// The endpoint it targets is a per-user sync trigger nginx exposes; a
	// shared secret is what stops anyone who can reach it from firing it.
	// Empty = no header, which is the pre-existing behaviour and keeps the
	// ping working against a receiver that does not check one yet (C6).
	historyNotifyToken string

	// deferredRetries dedups in-memory 6h rate-limit retries keyed by connection
	// ID, so a connection that 1018s on every daily pass (IBKR CTO+PEA sharing one
	// Flex token) doesn't stack overlapping timers. Guarded by deferMu.
	deferMu         sync.Mutex
	deferredRetries map[string]bool

	// connectorGroup collapses concurrent builds of the SAME connector
	// (E-M1, CONN-11 residue 2). getOrCreateConnector was check-then-create:
	// two syncs racing on a cache miss each built an instance from the same
	// stored credentials, and for cTrader that means two connectors holding
	// the same single-use refresh token — whichever refreshes second is
	// rejected, and the pair the loser then persists is already dead.
	connectorGroup singleflight.Group
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

// SetRateLimitLogRepo configures optional rate-limit journaling. Nil-safe:
// without it, throttle events are logged but not persisted.
func (s *SyncService) SetRateLimitLogRepo(repo *repository.SyncRateLimitLogRepo) {
	s.rateLimitLog = repo
}

// recordRateLimitHit journals one throttle event (Flex 1018 race loss or
// stale-statement skip) to sync_rate_limit_logs. Best-effort: a failed insert
// only warns, it never blocks the sync path.
func (s *SyncService) recordRateLimitHit(ctx context.Context, conn *repository.ExchangeConnection) {
	if s.rateLimitLog == nil || conn == nil {
		return
	}
	if err := s.rateLimitLog.RecordHit(ctx, conn.UserUID, conn.Exchange, conn.Label, time.Now().UTC()); err != nil {
		s.logger.Warn("failed to journal rate-limit hit",
			zap.String("user_uid", conn.UserUID),
			zap.String("exchange", conn.Exchange),
			zap.String("label", conn.Label),
			zap.Error(err),
		)
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

// SetHistoryNotify configures the best-effort "history rebuilt" ping: the base
// URL, and the shared secret sent as X-Internal-Token. An empty URL disables
// the ping (the enclave then stays fully blind — downstream services pick up
// new history on their own schedule). An empty token sends no header, so the
// ping keeps working against a receiver that does not check one yet.
func (s *SyncService) SetHistoryNotify(rawURL, token string) {
	s.historyNotifyURL = strings.TrimSpace(rawURL)
	s.historyNotifyToken = strings.TrimSpace(token)
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
	if s.historyNotifyToken != "" {
		req.Header.Set("X-Internal-Token", s.historyNotifyToken)
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

	// RateLimited is the typed form of "the broker is pacing us, retry
	// later". Error is sanitized before it is stored (egressSyncError), so
	// the text no longer carries the connector's own words and the string
	// predicate that used to arm the deferred retry stopped matching: from
	// 2026-09-09 the loser of a shared Flex token race was recorded as
	// "get balance: sync failed" and never retried, and two accounts on one
	// token took turns having a day. Set from the RAW error at the failure
	// site, where the words are still there.
	RateLimited bool `json:"rate_limited,omitempty"`

	// CapabilityWarnings carries key-scope gaps the connector discovered
	// while fetching the balance (connector.CapabilityWarner) — recorded in
	// sync_statuses.errorMessage under a "warning:" prefix with status still
	// "completed", mirrored to the frontend so the user is asked to widen
	// the key instead of silently publishing a partial equity.
	CapabilityWarnings []string `json:"capability_warnings,omitempty"`

	// RetryArmed marks a rate-limited result whose deferred retry IS
	// scheduled (atomic daily path only). recordSyncStatus keys off it to
	// write status "pending" instead of "error": a planned re-sync a few
	// hours out is a report, not a failure, and painting it red is exactly
	// the "blame the user's credentials" pattern this codebase keeps
	// re-learning (see CTO/PEA, 2026-08-27).
	RetryArmed bool `json:"retry_armed,omitempty"`

	// Skipped marks a sync that intentionally wrote nothing because the
	// broker's freshest statement predates the last trading day — stamping it
	// with today's date would fabricate a phantom snapshot (audit 2026-08-01).
	// The gap is honest: a deferred retry re-syncs once the statement window
	// clears, and the nightly reconstruction fills the day regardless.
	Skipped    bool   `json:"skipped,omitempty"`
	SkipReason string `json:"skip_reason,omitempty"`

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
					Error:    egressSyncError("manual sync blocked", allowErr),
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
			Error:    egressSyncError("", err),
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
				Error:    egressSyncError("manual sync blocked", allowErr),
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
			Error:    egressSyncError("lookup connection", err),
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
			Error:    egressSyncError("", err),
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
		result.Error = egressSyncError("get credentials", err)
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
		result.Error = egressSyncError("create connector", err)
		return result
	}

	// 2b. History reconstruction for connectors implementing HistoricalSnapshotProvider.
	//     IBKR runs on every sync (Flex returns the full window in a single cheap call
	//     and can carry retroactive corrections). Other connectors only run on first
	//     sync — once the historical backfill is in DB, subsequent syncs only produce
	//     the live (today) snapshot.
	if hsp, ok := conn.(connector.HistoricalSnapshotProvider); ok {
		// IBKR and cTrader re-run reconstruction every sync; others reconstruct
		// ONCE at connect via ReconstructHistoryOnConnect. IBKR re-emits its full
		// Flex window cheaply; cTrader re-runs a BOUNDED recent window
		// (everySyncReconstructSince) to self-heal the live/reconstruction
		// boundary deposit double-count. Gated INSIDE the HistoricalSnapshotProvider
		// type assertion, so live-only exchanges (HL/MEXC/Lighter) are unaffected.
		if reconstructsEverySync(connMeta.Exchange) {
			noteReconstructionFailure(result, s.syncFromHistoricalProvider(ctx, connMeta, hsp, everySyncReconstructSince(connMeta.Exchange), reconstructOpts{}))
		}
	}

	// 3. Get balance
	balance, err := s.fetchBalanceWithCollapseGuard(ctx, conn, connMeta)
	if err != nil {
		result.Error = egressSyncError("get balance", err)
		result.RateLimited = isRateLimitError(err.Error())
		s.logger.Error("sync failed: get balance",
			zap.String("user_uid", connMeta.UserUID),
			zap.String("exchange", connMeta.Exchange),
			zap.String("label", connMeta.Label),
			zap.Error(err),
		)
		return result
	}

	// 4. Get trades for the window ending at the snapshot boundary. startOfDay
	// is the snapshot timestamp (today 00:00 UTC) and the sync runs at that
	// moment, so we must look BACK to attribute trades/cashflows to this
	// snapshot — otherwise GetTrades runs with a zero-length window and returns
	// nothing. Normally that is the last 24h; after a missed sync it stretches
	// to the previous snapshot so the gap's cashflows are not lost (see
	// activityWindowStart).
	now := time.Now().UTC()
	startOfDay := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)

	// 4-pre. Freshness guard — same rationale and recovery as the guard in
	// buildConnectionSnapshot (root fix, audit 2026-08-01). The deferred
	// recordSyncStatus above persists the skip as status "skipped_stale".
	if reason := staleBalanceSkipReason(conn, startOfDay); reason != "" {
		result.Skipped = true
		result.SkipReason = reason
		s.logger.Warn("skipping live snapshot: stale statement",
			zap.String("user_uid", connMeta.UserUID),
			zap.String("exchange", connMeta.Exchange),
			zap.String("label", connMeta.Label),
			zap.String("reason", reason),
		)
		s.recordRateLimitHit(ctx, connMeta)
		s.scheduleDeferredRetry(connMeta, rateLimitRetryDelay)
		return result
	}

	activityStart := s.activityWindowStart(ctx, connMeta, startOfDay)

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

	// 5a. Fetch funding fees (always if supported — funding applies to all open
	// positions, not just those traded today)
	var fundingCharges float64
	if ffFetcher, ok := conn.(connector.FundingFeesFetcher); ok {
		if fees, err := ffFetcher.GetFundingFees(ctx, swapSymbols, activityStart); err == nil {
			for _, f := range fees {
				fundingCharges += f.Amount
			}
			breakdown.getOrCreateMarket(fundingMarketType(connMeta.Exchange)).fundingFees = fundingCharges
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

	// 6bis. Key-scope gaps discovered during the balance fetch — forwarded
	// through the sync status so the frontend can ask the user to widen the
	// key (see connector.CapabilityWarner).
	if cw, ok := conn.(connector.CapabilityWarner); ok {
		// APPEND: the reconstruction step above may already have recorded one
		// (E-M4), and overwriting would drop it.
		if warns := cw.CapabilityWarnings(); len(warns) > 0 {
			result.CapabilityWarnings = append(result.CapabilityWarnings, warns...)
			s.logger.Warn("connector reported capability gaps",
				zap.String("user_uid", connMeta.UserUID),
				zap.String("exchange", connMeta.Exchange),
				zap.String("label", connMeta.Label),
				zap.Strings("warnings", warns),
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

	// 6b. Balance-reconciled capital flow: for eligible exchanges the settled
	// balance overrides the broker's cashflow report, catching hidden deposits
	// (doc/plan-balance-reconciled-cashflow.md). No-op for everyone else.
	deposits, withdrawals = s.reconcileCapitalFlow(ctx, connMeta, startOfDay, trades, balance.Equity, balance.UnrealizedPnL, fundingCharges, deposits, withdrawals)

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

	// Safety net (AUDIT_HL_DAILY_GAIN_BUG.md): flag implausible daily moves
	// before they land in metrics. Detection only, the write still happens.
	s.warnOnAberrantDailyMove(ctx, snapshot)

	// Save snapshot individually (non-atomic path, used by manual sync)
	if err := s.snapshotRepo.Upsert(ctx, snapshot); err != nil {
		result.Error = egressSyncError("save snapshot", err)
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

	// Most-stale-first, with a short launch stagger. Correctness against the
	// shared-token 1018 lives in the connector's token gate; this ordering
	// makes the gate's winner deterministic instead of a goroutine race. For
	// two accounts on one Flex token the pattern settles into stable slots:
	// the midnight winner stays 24h-stale at the next midnight and wins
	// again, while the other account keeps its own deferred-retry slot a few
	// hours later — both fresh daily, at predictable times.
	if s.syncStatus != nil {
		lastSync := make(map[string]time.Time, len(connections))
		if statuses, serr := s.syncStatus.GetByUser(ctx, userUID); serr == nil {
			for _, st := range statuses {
				if st.LastSyncTime != nil {
					lastSync[st.Exchange+"|"+st.Label] = *st.LastSyncTime
				}
			}
		}
		sort.SliceStable(connections, func(a, b int) bool {
			return lastSync[connections[a].Exchange+"|"+connections[a].Label].
				Before(lastSync[connections[b].Exchange+"|"+connections[b].Label])
		})
	}

	const launchStagger = 300 * time.Millisecond

	launchIdx := 0
	for _, conn := range connections {
		if !s.isConnectionDue(ctx, conn, now) {
			continue
		}

		wg.Add(1)
		go func(c *repository.ExchangeConnection, headStart time.Duration) {
			defer wg.Done()
			time.Sleep(headStart)
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
		}(conn, time.Duration(launchIdx)*launchStagger)
		launchIdx++
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
					r.Error = egressSyncError("atomic save failed", err)
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

	// Phase 4: Record sync status for every result, failures included.
	//
	// Failures used to be logged here and dropped, which left a connection that
	// has NEVER synced with no row in sync_statuses at all. Nothing to mirror
	// into Neon, so the dashboard showed no error, no banner, no hint — just an
	// empty account, indefinitely. It only hits connections whose FIRST sync
	// fails, which means it only ever hit brand-new users, at the exact moment
	// they were deciding whether the product works (2026-08-23: a cTrader
	// connection created at 15:21 was still silent two days and two daily
	// passes later; the user wrote in to say nothing on the site worked).
	//
	// recordSyncStatus leaves lastSyncTime untouched on a failure, so this
	// stays compatible with the retry paths below, which rely on a failed
	// connection remaining due.
	for _, r := range results {
		// Phase 5 below schedules a deferred retry for exactly this
		// predicate, so the status can honestly say "pending" rather than
		// "error" — same contract as the skipped_stale case.
		if r.rateLimited() {
			r.RetryArmed = true
		}
		conn := findConnection(connections, r.Exchange, r.Label)
		if conn != nil {
			s.recordSyncStatus(ctx, conn, r, now)
		}
	}

	// Phase 5: shared-token rate-limit recovery. IBKR enforces a token-level Flex
	// rate limit (1018). When two connections share one token (e.g. IBKR CTO+PEA)
	// the parallel daily pass races them and the loser gets 1018, leaving that
	// account stale until tomorrow. Retry the loser once after the token's window
	// clears (~6h) so both refresh the same day. A failed sync records its error
	// but not a lastSyncTime, so the connection stays "due" — the deferred retry,
	// or failing that the next daily pass, recovers it.
	for _, r := range results {
		if r.rateLimited() {
			if conn := findConnection(connections, r.Exchange, r.Label); conn != nil {
				s.recordRateLimitHit(ctx, conn)
				s.scheduleDeferredRetry(conn, rateLimitRetryDelay)
			}
		}
	}

	return results, nil
}

// rateLimitRetryDelay is how long to wait before re-syncing a connection that
// lost a shared-token Flex race (IBKR 1018). It must exceed IBKR's ~3h
// token-level window with margin; 6h was chosen after 3h proved too tight.
const rateLimitRetryDelay = 6 * time.Hour

// isRateLimitError reports whether a sync result error is one the same day can
// still recover from: IBKR's token-level rate limit (1018), the local token
// gate, or a statement IBKR declined to generate.
//
// That last one used to be excluded, on the grounds that 1001 and its siblings
// "clear in seconds, not hours". They do, in the middle of a trading day. The
// daily pass runs at 00:00 UTC, which is the middle of IBKR's own end-of-day
// processing, and a refusal there lasts through it. Excluded, the connection
// skipped the 6h retry that recovers every other IBKR account and waited a
// full day for the next pass, which landed in the same window and refused
// again: one account repeated that for three nights while its statements were
// being served normally in the afternoon.
//
// The match is the connector's own judgement, not a second list of codes here:
// it wraps exactly the refusals it considers transient in ErrTransient.
func isRateLimitError(errStr string) bool {
	return strings.Contains(errStr, "1018") || strings.Contains(errStr, "Too many requests") ||
		strings.Contains(errStr, "shared flex token cooling down") ||
		strings.Contains(errStr, connector.ErrTransient.Error()+": flex request failed:") ||
		strings.Contains(errStr, errsanitize.MsgRateLimited)
}

// rateLimited answers for a result whether its failure was pacing rather
// than breakage, from the typed flag first and the text as a fallback for
// results built by paths that only carry the text.
func (r *SyncResult) rateLimited() bool {
	return r.RateLimited || isRateLimitError(r.Error)
}

// lastExpectedStatementDate returns the most recent weekday (UTC) strictly
// before startOfDay — the newest statement date a daily-reporting broker can
// possibly have when the sync runs at startOfDay. Saturday, Sunday and Monday
// all expect Friday: over a weekend nothing trades, so a statement ending
// Friday is fully fresh and its close is the honest carry-forward value.
// Holidays are not modelled; the guard then skips one extra day, which the
// nightly reconstruction fills — a conservative, self-healing miss.
func lastExpectedStatementDate(startOfDay time.Time) time.Time {
	d := startOfDay.AddDate(0, 0, -1)
	for d.Weekday() == time.Saturday || d.Weekday() == time.Sunday {
		d = d.AddDate(0, 0, -1)
	}
	return d
}

// staleBalanceSkipReason returns a non-empty human-readable reason when conn's
// last GetBalance figure is too old to be stamped with startOfDay's date.
// Connectors that don't report freshness (everything except statement-based
// brokers) and statements without a parsable date fail open: empty reason.
//
// This is the root fix for the phantom-snapshot defect (audit 2026-08-01): the
// midnight sync used to write the statement's two-day-old equity under today's
// date; on weekend dates the backfill never revisits the row, so the stale
// value became a permanent fake ±N% zigzag in every derived metric.
func staleBalanceSkipReason(conn connector.Connector, startOfDay time.Time) string {
	fp, ok := conn.(connector.BalanceFreshnessProvider)
	if !ok {
		return ""
	}
	asOf := fp.BalanceAsOf()
	if asOf.IsZero() {
		return ""
	}
	expected := lastExpectedStatementDate(startOfDay)
	if asOf.Before(expected) {
		return fmt.Sprintf("stale statement: balance as of %s, need >= %s to stamp %s",
			asOf.Format("2006-01-02"), expected.Format("2006-01-02"), startOfDay.Format("2006-01-02"))
	}
	return ""
}

// warnOnAberrantDailyMove logs a WARN when a live snapshot implies a
// flow-adjusted daily move beyond ±50% versus the previous stored snapshot —
// the safety net recommended by AUDIT_HL_DAILY_GAIN_BUG.md (May 2026) that
// would have caught the phantom-snapshot defect a year earlier. Detection
// only: the snapshot is still written, an operator investigates.
func (s *SyncService) warnOnAberrantDailyMove(ctx context.Context, snap *repository.Snapshot) {
	if snap == nil || s.snapshotRepo == nil {
		return
	}
	prev, err := s.snapshotRepo.GetLatestByUserExchangeLabelBefore(ctx, snap.UserUID, snap.Exchange, snap.Label, snap.Timestamp)
	if err != nil || prev == nil || prev.TotalEquity <= 0 {
		return
	}
	base := prev.TotalEquity
	if snap.Deposits > 0 {
		base += snap.Deposits
	}
	if base <= 0 {
		return
	}
	move := (snap.TotalEquity - prev.TotalEquity - (snap.Deposits - snap.Withdrawals)) / base
	if move > 0.5 || move < -0.5 {
		s.logger.Warn("aberrant daily move on live snapshot (flow-adjusted > 50%)",
			zap.String("user_uid", snap.UserUID),
			zap.String("exchange", snap.Exchange),
			zap.String("label", snap.Label),
			zap.Time("day", snap.Timestamp),
			zap.Float64("move_pct", move*100),
			zap.Float64("prev_equity", prev.TotalEquity),
			zap.Float64("new_equity", snap.TotalEquity),
			zap.Float64("deposits", snap.Deposits),
			zap.Float64("withdrawals", snap.Withdrawals),
		)
	}
}

// scheduleDeferredRetry re-syncs a single connection once, after delay, to
// recover the loser of a shared-token Flex race so both accounts refresh the
// same day. The timer is in-memory: if the enclave restarts within the window it
// is lost, and the next daily pass retries (a failed sync records no status, so
// the connection stays "due"). Dedup'd per connection ID so repeated 1018s don't
// stack timers.
func (s *SyncService) scheduleDeferredRetry(conn *repository.ExchangeConnection, delay time.Duration) {
	if conn == nil {
		return
	}
	s.deferMu.Lock()
	if s.deferredRetries == nil {
		s.deferredRetries = make(map[string]bool)
	}
	if s.deferredRetries[conn.ID] {
		s.deferMu.Unlock()
		return // a retry is already pending for this connection
	}
	s.deferredRetries[conn.ID] = true
	s.deferMu.Unlock()

	s.logger.Info("scheduling deferred sync retry after rate limit",
		zap.String("user_uid", conn.UserUID),
		zap.String("exchange", conn.Exchange),
		zap.String("label", conn.Label),
		zap.Duration("delay", delay),
	)

	time.AfterFunc(delay, func() {
		defer func() {
			s.deferMu.Lock()
			delete(s.deferredRetries, conn.ID)
			s.deferMu.Unlock()
		}()
		s.retryConnectionDeferred(conn)
	})
}

// retryConnectionDeferred re-syncs one connection out-of-band and saves its
// snapshot independently of the daily batch. It never schedules another deferral:
// one retry per daily cycle bounds the work, and if it still fails the next daily
// pass picks the connection up.
func (s *SyncService) retryConnectionDeferred(conn *repository.ExchangeConnection) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	result := s.buildConnectionSnapshot(ctx, conn)
	if result.Error != "" {
		s.logger.Warn("deferred rate-limit retry still failing",
			zap.String("user_uid", conn.UserUID),
			zap.String("exchange", conn.Exchange),
			zap.String("label", conn.Label),
			zap.String("error", result.Error),
		)
		return
	}
	if result.snapshot == nil {
		return
	}
	if err := s.snapshotRepo.UpsertBatch(ctx, []*repository.Snapshot{result.snapshot}); err != nil {
		s.logger.Error("deferred rate-limit retry save failed",
			zap.String("user_uid", conn.UserUID),
			zap.String("exchange", conn.Exchange),
			zap.String("label", conn.Label),
			zap.Error(err),
		)
		return
	}
	result.Success = true
	s.recordSyncStatus(ctx, conn, result, time.Now().UTC())
	s.logger.Info("deferred rate-limit retry succeeded",
		zap.String("user_uid", conn.UserUID),
		zap.String("exchange", conn.Exchange),
		zap.String("label", conn.Label),
	)
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
		result.Error = egressSyncError("get credentials", err)
		s.logger.Error(classifySyncError("get credentials: "+err.Error()), zap.String("exchange", connMeta.Exchange), zap.String("step", "decrypt"), zap.Duration("elapsed", time.Since(start)), zap.Error(err))
		return result
	}

	conn, err := s.getOrCreateConnector(connMeta.Exchange, connMeta.UserUID, connMeta.Label, creds)
	if err != nil {
		result.Error = egressSyncError("create connector", err)
		s.logger.Error(classifySyncError("create connector: "+err.Error()), zap.String("exchange", connMeta.Exchange), zap.String("step", "connector"), zap.Duration("elapsed", time.Since(start)), zap.Error(err))
		return result
	}

	// History reconstruction (see syncConnection for the gating rules). The
	// scheduler path goes through buildConnectionSnapshot, not syncConnection,
	// so we duplicate the call here to cover both manual and scheduled syncs.
	if hsp, ok := conn.(connector.HistoricalSnapshotProvider); ok {
		// IBKR and cTrader re-run reconstruction every sync; others reconstruct
		// ONCE at connect via ReconstructHistoryOnConnect. IBKR re-emits its full
		// Flex window cheaply; cTrader re-runs a BOUNDED recent window
		// (everySyncReconstructSince) to self-heal the live/reconstruction
		// boundary deposit double-count. Gated INSIDE the HistoricalSnapshotProvider
		// type assertion, so live-only exchanges (HL/MEXC/Lighter) are unaffected.
		if reconstructsEverySync(connMeta.Exchange) {
			noteReconstructionFailure(result, s.syncFromHistoricalProvider(ctx, connMeta, hsp, everySyncReconstructSince(connMeta.Exchange), reconstructOpts{}))
		}
	}

	balance, err := s.fetchBalanceWithCollapseGuard(ctx, conn, connMeta)
	if err != nil {
		result.Error = egressSyncError("get balance", err)
		result.RateLimited = isRateLimitError(err.Error())
		s.logger.Error(classifySyncError("get balance: "+err.Error()), zap.String("exchange", connMeta.Exchange), zap.String("label", connMeta.Label), zap.String("step", "get_balance"), zap.Duration("elapsed", time.Since(start)), zap.Error(err))
		return result
	}
	s.logger.Info("balance fetched", zap.String("exchange", connMeta.Exchange), zap.String("label", connMeta.Label), zap.Duration("elapsed", time.Since(start)))

	// Key-scope gaps discovered during the balance fetch — same hook as
	// syncConnection (the scheduler goes through THIS function, which is
	// where the midnight herd actually detects a key that cannot read part
	// of the account; see connector.CapabilityWarner).
	if cw, ok := conn.(connector.CapabilityWarner); ok {
		// APPEND: the reconstruction step above may already have recorded one
		// (E-M4), and overwriting would drop it.
		if warns := cw.CapabilityWarnings(); len(warns) > 0 {
			result.CapabilityWarnings = append(result.CapabilityWarnings, warns...)
			s.logger.Warn("connector reported capability gaps",
				zap.String("user_uid", connMeta.UserUID),
				zap.String("exchange", connMeta.Exchange),
				zap.String("label", connMeta.Label),
				zap.Strings("warnings", warns),
			)
		}
	}

	// Same window semantics as syncConnection above: the snapshot lands at
	// startOfDay (today 00:00 UTC), so we pull trades/cashflows from the
	// preceding window — 24h normally, stretched back to the last snapshot
	// when a sync was missed (see activityWindowStart).
	now := time.Now().UTC()
	startOfDay := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)

	// Freshness guard (root fix, audit 2026-08-01): refuse to stamp a stale
	// statement figure with today's date — that fabricated the J−2 phantom
	// snapshots. Writing nothing is honest (the reconstruction fills the day),
	// and the deferred retry picks up the fresh statement the same day, the
	// same recovery already used for the shared-token 1018 race.
	if reason := staleBalanceSkipReason(conn, startOfDay); reason != "" {
		result.Skipped = true
		result.SkipReason = reason
		s.logger.Warn("skipping live snapshot: stale statement",
			zap.String("user_uid", connMeta.UserUID),
			zap.String("exchange", connMeta.Exchange),
			zap.String("label", connMeta.Label),
			zap.String("reason", reason),
		)
		s.recordRateLimitHit(ctx, connMeta)
		s.scheduleDeferredRetry(connMeta, rateLimitRetryDelay)
		return result
	}

	activityStart := s.activityWindowStart(ctx, connMeta, startOfDay)

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

	var fundingCharges float64
	if ffFetcher, ok := conn.(connector.FundingFeesFetcher); ok {
		if fees, err := ffFetcher.GetFundingFees(ctx, swapSymbols, activityStart); err == nil {
			for _, f := range fees {
				fundingCharges += f.Amount
			}
			breakdown.getOrCreateMarket(fundingMarketType(connMeta.Exchange)).fundingFees = fundingCharges
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

	// Balance-reconciled capital flow: same override as the non-atomic path
	// (doc/plan-balance-reconciled-cashflow.md). No-op for ineligible exchanges.
	deposits, withdrawals = s.reconcileCapitalFlow(ctx, connMeta, startOfDay, trades, balance.Equity, balance.UnrealizedPnL, fundingCharges, deposits, withdrawals)

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

	// Safety net (AUDIT_HL_DAILY_GAIN_BUG.md): flag implausible daily moves
	// before they land in metrics. Detection only, the write still happens.
	s.warnOnAberrantDailyMove(ctx, result.snapshot)

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

// syncStatusStamp returns the lastSyncTime to persist for a finished attempt.
//
// A failure returns nil, which the repository coalesces so the stored value
// survives. That matters twice over. Stamping an error would make the
// connection look freshly synced and drop it out of the due set the deferred
// rate-limit retry and the next daily pass rely on, and it would let a
// connection that has never once succeeded advertise a "last synced" time it
// never earned.
func syncStatusStamp(status string, attempt time.Time) *time.Time {
	if status == "error" {
		return nil
	}
	return &attempt
}

func (s *SyncService) recordSyncStatus(ctx context.Context, conn *repository.ExchangeConnection, result *SyncResult, lastAttempt time.Time) {
	if s.syncStatus == nil || conn == nil || result == nil {
		return
	}

	status := "error"
	errMsg := result.Error
	switch {
	case result.Skipped:
		// Surfaced as non-"completed" so a stale-statement skip is visible in
		// sync_statuses instead of the silent success that hid the
		// phantom-snapshot defect for months. The column is backed by the
		// legacy Postgres enum SyncStatusEnum (pending|syncing|completed|error
		// — absent from every current Prisma schema but still constraining the
		// column), so a literal 'skipped_stale' is rejected (22P02, observed
		// on the 2026-08-04 first run). 'pending' is the honest member: the
		// day's fresh statement is not available yet and a retry is armed.
		// The machine-readable marker lives in errorMessage.
		status = "pending"
		errMsg = "skipped_stale: " + result.SkipReason
	case result.RetryArmed && result.Error != "":
		// Rate-limited with a deferred retry armed (shared Flex token). Same
		// enum constraint as above: "pending" is the honest member — the
		// re-sync is scheduled, the data is complete up to yesterday, and
		// nothing is wrong with the user's credentials. Machine-readable
		// marker in errorMessage, mirroring the skipped_stale contract.
		status = "pending"
		errMsg = "deferred_rate_limit: " + result.Error
	case syncStatusMarker(result.Error) != "":
		// E-H5 / C4: a machine-readable marker in errorMessage, same shape as
		// skipped_stale:/deferred_rate_limit:/warning: above. Without it, a
		// dead OAuth token and a dropped TCP connection both landed in
		// sync_statuses as "get balance: sync failed", so nothing downstream
		// could tell "reconnect your account" (only the user can fix it) from
		// "we will retry tonight" (nobody needs to do anything). Analytics
		// maps reauth_required: onto exchange_status.status = needs_reauth.
		// The status column stays "error": the sync did fail.
		errMsg = syncStatusMarker(result.Error) + ": " + result.Error
	case result.Success:
		status = "completed"
		// A successful sync can still carry key-scope warnings — recorded in
		// errorMessage under a stable machine-readable prefix ("warning:")
		// so the analytics mirror and the frontend can key off it without a
		// schema change; status stays "completed" (the sync DID succeed on
		// what the key covers).
		if errMsg == "" && len(result.CapabilityWarnings) > 0 {
			errMsg = "warning: " + strings.Join(result.CapabilityWarnings, ",")
		}
	}

	record := &repository.SyncStatus{
		UserUID:      conn.UserUID,
		Exchange:     conn.Exchange,
		Label:        conn.Label,
		LastSyncTime: syncStatusStamp(status, lastAttempt),
		Status:       status,
		TotalTrades:  result.TradeCount,
		ErrorMessage: errMsg,
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

// reconcileCapitalFlow returns the snapshot's deposits/withdrawals. For eligible
// exchanges it replaces the broker-reported figures with balance-reconciled
// ones: the settled balance is truth, and any change the ledger (realized P&L +
// charges) and the reported flow do not explain is a hidden capital movement —
// the failure a demo top-up that appears in no transaction endpoint would
// otherwise cause. See doc/plan-balance-reconciled-cashflow.md. Falls back to
// the reported figures when the exchange is not eligible, no prior snapshot
// exists (first sync — inception-deposit handles that), or the lookup fails.
func (s *SyncService) reconcileCapitalFlow(
	ctx context.Context,
	connMeta *repository.ExchangeConnection,
	before time.Time,
	trades []*connector.Trade,
	equity, unrealizedPnL, fundingCharges, reportedDeposits, reportedWithdrawals float64,
) (deposits, withdrawals float64) {
	if !isBalanceReconciled(connMeta.Exchange) {
		return reportedDeposits, reportedWithdrawals
	}
	prev, err := s.snapshotRepo.GetLatestByUserExchangeLabelBefore(ctx, connMeta.UserUID, connMeta.Exchange, connMeta.Label, before)
	if err != nil || prev == nil {
		return reportedDeposits, reportedWithdrawals
	}

	var realizedPnL float64
	for _, tr := range trades {
		realizedPnL += tr.RealizedPnL
	}
	dep, wd := InferCapitalFlow(
		ReconInputs{SettledBalance: prev.RealizedBalance},
		ReconInputs{
			SettledBalance: equity - unrealizedPnL,
			RealizedPnL:    realizedPnL,
			Charges:        fundingCharges,
			ReportedFlow:   reportedDeposits - reportedWithdrawals,
		},
		reconThreshold(equity),
	)
	if dep != reportedDeposits || wd != reportedWithdrawals {
		s.logger.Info("capital flow reconciled from balance",
			zap.String("exchange", connMeta.Exchange),
			zap.String("label", connMeta.Label),
			zap.Float64("reported_deposits", reportedDeposits),
			zap.Float64("reported_withdrawals", reportedWithdrawals),
			zap.Float64("reconciled_deposits", dep),
			zap.Float64("reconciled_withdrawals", wd),
		)
	}
	return dep, wd
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
	s.reconstructHistoryFor(ctx, userUID, exchange, label, reconstructOpts{})
}

// ReconstructHistoryRange backfills ONLY the days in [from, to] (inclusive,
// UTC midnights). Same routing and same credential handling as
// ReconstructHistoryOnConnect — the only difference is that days outside the
// window are discarded before the upsert, so existing snapshots on either
// side are left untouched.
//
// This is the gap-repair entrypoint (OPS-004): use it when a sync outage left
// holes in an otherwise-correct timeline. A zero from/to degrades to the full
// reconstruct, so callers that mean "only these days" must pass both.
// dryRun performs the full reconstruct but logs the resulting days instead of
// writing them — use it before committing a splice into an existing timeline.
func (s *SyncService) ReconstructHistoryRange(ctx context.Context, userUID, exchange, label string, from, to time.Time, dryRun bool) {
	s.reconstructHistoryFor(ctx, userUID, exchange, label, reconstructOpts{
		window: dayWindow{
			from: time.Date(from.Year(), from.Month(), from.Day(), 0, 0, 0, 0, time.UTC),
			to:   time.Date(to.Year(), to.Month(), to.Day(), 0, 0, 0, 0, time.UTC),
		},
		dryRun: dryRun,
	})
}

// DryRunReconstructRange runs a reconstruction without writing and hands back
// the days it would have persisted, so they can be compared against what is
// already stored. Synchronous, unlike ReconstructHistoryRange: the caller is
// waiting for the answer, not for confirmation that work started.
func (s *SyncService) DryRunReconstructRange(ctx context.Context, userUID, exchange, label string, from, to time.Time) []*repository.Snapshot {
	var out []*repository.Snapshot
	s.reconstructHistoryFor(ctx, userUID, exchange, label, reconstructOpts{
		window: dayWindow{
			from: time.Date(from.Year(), from.Month(), from.Day(), 0, 0, 0, 0, time.UTC),
			to:   time.Date(to.Year(), to.Month(), to.Day(), 0, 0, 0, 0, time.UTC),
		},
		dryRun:  true,
		collect: func(sn []*repository.Snapshot) { out = sn },
	})
	return out
}

func (s *SyncService) reconstructHistoryFor(ctx context.Context, userUID, exchange, label string, opts reconstructOpts) {
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
	s.reconstructHistory(ctx, connMeta, conn, creds, opts)
}

// reconstructHistory routes a freshly built connection to its history
// backfill path. Connectors implementing HistoricalSnapshotProvider (IBKR)
// reconstruct in-enclave — signed by the report chain, never leaving the
// SEV-SNP perimeter. Every other connector hands off to the external
// rebuilder, which is where plaintext credentials cross the perimeter
// (SEC-ZK-001). Split from ReconstructHistoryOnConnect so this routing —
// the decision that governs whether credentials leave the enclave — is
// unit-testable without a DB-backed ConnectionService.
func (s *SyncService) reconstructHistory(ctx context.Context, connMeta *repository.ExchangeConnection, conn connector.Connector, creds *Credentials, opts reconstructOpts) {
	if hsp, ok := conn.(connector.HistoricalSnapshotProvider); ok {
		// IBKR (and any future ZK-native provider) keeps the in-enclave path:
		// signed by the report chain, stays inside the SEV-SNP perimeter.
		// since=zero => full backfill (connect time + admin reconstruct); the
		// every-sync path uses a bounded window.
		// Connect-time reconstruction: the failure is already logged at Error
		// and there is no SyncResult to hang a warning on here — the caller is
		// the post-create hook, not a sync.
		_ = s.syncFromHistoricalProvider(ctx, connMeta, hsp, time.Time{}, opts)
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
	// Dispatch only what the deployed rebuilder registers. Without this gate an
	// unsupported exchange still shipped plaintext credentials out of the
	// enclave for a call that can only answer HTTP 400 — egress with zero
	// payoff (observed live: an okx connect on 2026-07-20, two log lines and
	// nothing else). Warn, not error: the user's connection is fine, only the
	// optional backfill is unavailable.
	if !externalRebuilderSupports(connMeta.Exchange) {
		s.logger.Warn("history backfill: exchange not supported by the deployed rebuilder, skipping",
			zap.String("user_uid", connMeta.UserUID),
			zap.String("exchange", connMeta.Exchange),
		)
		return
	}
	// Anchor: the freshest live row when one exists (the connect hook runs the
	// first live sync before this dispatch). The MTM-walk rebuilders calibrate
	// their curve on it exactly as the midnight pass does; the absolute-
	// statement family (binance, bitget, …) never calibrates on it but uses it
	// as the anchor-gate witness — a rebuilt tail that contradicts it (net of
	// flows) is refused instead of persisted. Zero when no row exists yet:
	// the rebuilder falls back to its own live valuation, as before.
	var endEquity float64
	if s.snapshotRepo != nil {
		if latest, lerr := s.snapshotRepo.GetLatestByUserExchangeLabel(ctx, connMeta.UserUID, connMeta.Exchange, connMeta.Label); lerr == nil && latest != nil {
			endEquity = latest.TotalEquity
		}
	}
	if gap := rebuildCredentialGap(connMeta.Exchange, creds.APIKey); gap != "" {
		// Not an error: the connection is fine, the credential just cannot
		// rebuild, and the connector reports that gap on every sync.
		s.logger.Info("history backfill: credential cannot rebuild, skipping",
			zap.String("user_uid", connMeta.UserUID),
			zap.String("exchange", connMeta.Exchange),
			zap.String("gap", gap),
		)
		creds = nil
		return
	}
	s.logger.Info("history backfill: dispatching to external rebuilder",
		zap.String("user_uid", connMeta.UserUID),
		zap.String("exchange", connMeta.Exchange),
		zap.Float64("end_equity_anchor", endEquity),
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
		EndEquityOverride: endEquity,
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

	opts.coveredFrom = res.CoveredFrom
	firstSync := s.isFirstSync(ctx, connMeta)
	s.persistHistoricalSnapshots(ctx, connMeta, res.Snapshots, firstSync, sourceExternalRebuilder, opts)
	s.notifyHistoryRebuilt(ctx, connMeta.UserUID)
}

// errRebuildCredentialGap marks a connection whose stored credential can
// read the account but not its history, so no rebuild will ever succeed
// until the holder replaces it.
var errRebuildCredentialGap = errors.New("credential cannot rebuild history")

// rebuildCredentialGap names why a credential cannot rebuild, or returns ""
// when it can. Only Lighter draws that line today: a "ro:" token reads the
// fills, a wallet address does not, and the rebuilder refuses the address
// with a 400. Sending it anyway is plaintext egress for an answer already
// known, every connect and every midnight after that.
func rebuildCredentialGap(exchange, apiKey string) string {
	if strings.ToLower(strings.TrimSpace(exchange)) != "lighter" {
		return ""
	}
	if strings.HasPrefix(strings.ToLower(strings.TrimSpace(apiKey)), "ro:") {
		return ""
	}
	return "lighter_history_needs_ro_token"
}

// externalRebuilderExchanges lists the exchanges the deployed rebuilder
// actually supports AND that we've validated end-to-end for midnight
// recalibration. MUST stay a subset of the rebuilder's live registry
// (track_record_site/history-rebuilder-go/cmd/rebuilder/main.go) — listing an
// exchange the deployed rebuilder doesn't register makes it answer HTTP 400
// "unsupported exchange" on every nightly tick (no MarkFinalized on failure →
// retried forever). On 2026-05-28 the prod rebuilder registered only
// hyperliquid, so lighter/mexc/alpaca connections produced 5 failed
// recalibrations per tick. Re-add each exchange here only after it's both
// (a) committed + deployed in the rebuilder registry and (b) confirmed to
// honour EndEquityOverride with an MTM-walk offset that depends on endEquity.
//
// Exchanges NOT in this list (IBKR, MT5, …) use in-enclave history providers
// whose daily-equity summaries come straight from the broker statement — no
// MTM walk, no calibration drift, nothing to recalibrate.
//
// bitget: the rebuilder's walk is ABSOLUTE (account bills carry balance-after
// values, statement-is-truth like IBKR) and deliberately ignores
// EndEquityOverride — the midnight pass is therefore not a re-anchor but an
// idempotent refresh (re-walk with bills that now include the connect day)
// whose real purpose is MarkRebuildFinalized, so bitget connections follow
// the same rebuild→finalize lifecycle as hyperliquid instead of sitting
// unfinalized until they age out of the retry window.
//
// binance: same absolute family as bitget — the rebuilder reads Binance's
// own daily accountSnapshot statements (30-day retention, so the rebuilt
// window is shorter than bitget's 90) and likewise ignores EndEquityOverride.
// The rebuilt rows upsert over the connect-day live snapshot, which also
// retires that day's UX-001 inception deposit (deposits recomputed from the
// on-chain + universal-transfer ledger instead of deposits=equity).
//
// okx: same absolute family — unified-account bills carry balance-after per
// currency (statement-is-truth, EndEquityOverride ignored), so the midnight
// pass is the idempotent re-walk that finalizes, as with bitget. A key
// IP-locked to the enclave egress reaches OKX from the rebuilder host only
// through the rebuilder's OKX_EGRESS_PROXY tunnel; without it the rebuild fails
// 50110 and never finalizes.
//
// alpaca: statement-is-truth like IBKR — the broker's /account/portfolio/history
// is an authoritative end-of-day equity series (no MTM walk, no offset, so
// EndEquityOverride is irrelevant). Cash flows/fees/trades are folded from
// /account/activities on top. The midnight pass is the idempotent re-fetch that
// finalizes.
// bybit: same absolute family — the unified-account transaction log carries
// cashBalance per currency (statement-is-truth, EndEquityOverride ignored), so
// the midnight pass is the idempotent re-walk that finalizes, as with bitget.
// Its reach is 2 years, deeper than any other rebuilder we run, but only 7 days
// per query, so a first rebuild is slow and the module truncates to the span it
// managed rather than failing. Bybit CloudFront country-blocks THIS enclave's
// region, which is why the live connector proxies (see factory.go); the
// rebuilder host reaches api.bybit.com directly and needs no tunnel. A key
// IP-locked to the enclave egress will still be refused from the rebuilder.
//
// mexc, lighter: back in the list on 2026-08-28. Both were dropped on
// 2026-05-28 because the prod rebuilder did not register them yet; it has
// since 2026-07-08, and their modules now honour EndEquityOverride exactly
// like hyperliquid (calibrated MTM walk anchored on the midnight snapshot),
// which is the condition (b) above. The lighter module needs a read-only
// "ro:" token — a bare wallet address cannot read fills — and answers a
// credential error otherwise, which the connect hook reports as a rebuild
// failure without touching the live connection.
var externalRebuilderExchanges = []string{"hyperliquid", "bitget", "binance", "okx", "alpaca", "bybit", "kraken", "mexc", "lighter"}

// externalRebuilderSupports reports whether the deployed rebuilder registers
// this exchange — the gate for every dispatch that carries plaintext
// credentials out of the enclave.
func externalRebuilderSupports(exchange string) bool {
	e := strings.ToLower(strings.TrimSpace(exchange))
	for _, supported := range externalRebuilderExchanges {
		if e == supported {
			return true
		}
	}
	return false
}

// maxRebuildRetryDays bounds how long the midnight recalibration keeps retrying
// a consenting connection that never finalizes (SEC-08): past this many days
// from creation it ages out of ListUnfinalizedExternalRebuilds instead of
// re-egressing credentials to the rebuilder on every nightly tick forever.
const maxRebuildRetryDays = 7

// ErrHistoryNotDeletable is returned for a connection whose history was
// reconstructed inside the perimeter. Those rows are indistinguishable from
// live ones on the production schema, so there is nothing safe to delete.
var ErrHistoryNotDeletable = errors.New("history reconstruction is not deletable for this exchange")

// historyReconstructedInEnclave reports whether an exchange's backfill runs
// through a HistoricalSnapshotProvider rather than the external rebuilder.
// Deliberately separate from reconstructsEverySync, which carries its own
// narrow contract and must not become a general gate.
func historyReconstructedInEnclave(exchange string) bool {
	switch strings.ToLower(exchange) {
	case "ibkr", "ctrader":
		return true
	default:
		return false
	}
}

// DeleteRebuiltHistory drops a connection's out-of-perimeter rebuilt snapshots
// and returns how many were removed. Live snapshots are untouched: the origin
// column separates them, so days synced since the rebuild survive even when
// they fall inside the rebuilt window.
//
// Consent is withdrawn before the rows go, not after. The nightly
// recalibration pass selects on rebuild_requested_at, so a failure between the
// two steps leaves it disarmed and the history intact — the caller retries.
// The reverse order leaves a window in which the pass re-fetches and re-upserts
// everything that was just deleted.
func (s *SyncService) DeleteRebuiltHistory(ctx context.Context, userUID, exchange, label string) (int64, error) {
	if historyReconstructedInEnclave(exchange) {
		return 0, ErrHistoryNotDeletable
	}

	target, err := s.connSvc.GetHistoryTarget(ctx, userUID, exchange, label)
	if err != nil {
		return 0, fmt.Errorf("load connection: %w", err)
	}

	if err := s.connSvc.ClearRebuildConsent(ctx, target.ID); err != nil {
		return 0, fmt.Errorf("withdraw rebuild consent: %w", err)
	}

	// The live branch owns every day from the connection's own day onward, and
	// reconstruction never writes today's bucket, so this cutoff drops nothing
	// the rebuilder produced while protecting days a later pass re-stamped.
	liveFrom := target.CreatedAt.UTC().Truncate(24 * time.Hour)

	deleted, err := s.snapshotRepo.DeleteExternalRebuilderHistory(ctx, userUID, exchange, label, liveFrom)
	if err != nil {
		return 0, err
	}

	s.logger.Info("rebuilt history deleted",
		zap.String("user_uid", userUID),
		zap.String("exchange", exchange),
		zap.Int64("snapshots", deleted))
	return deleted, nil
}

// RecalibrateRebuiltHistories re-runs the external rebuilder for connections
// whose initial rebuild was anchored on the imprecise connect-time live
// equity (= live perp+spot fetched at e.g. 14:32 UTC, when the user clicked
// "Rebuild full history"). Uses the just-written midnight snapshot equity as
// EndEquityOverride so the MTM walk's offset calibration aligns to the exact
// 00:00 UTC ground truth.
//
// Called by SyncScheduler.executeDailySync AFTER all live syncs complete —
// by that point today's snapshots are in DB and provide the anchor.
//
// SECURITY (SEC-ZK-001): identical credential path to reconstructHistory's
// first-time rebuild. Each connection's creds live only inside recalibrateOne's
// scope and are dropped after the rebuilder call returns. No new surface area
// vs the existing connect-time rebuild flow.
//
// Sequential processing (no concurrency) bounds exchange-API pressure: at
// most one rebuilder request in flight across the entire user base. Failures
// per connection log + skip + DO NOT MarkFinalized — that connection naturally
// retries on the next nightly tick.
//
// No-op cases (silent early return):
//   - Rebuilder client not configured (dev environment)
//   - rebuild_finalized_at column missing (migration not applied)
//   - No unfinalized connections matching the filter
func (s *SyncService) RecalibrateRebuiltHistories(ctx context.Context) {
	if s.rebuilder == nil || !s.rebuilder.Configured() {
		return
	}
	// Cutoff = today's midnight. Connections created AFTER today_midnight
	// (i.e. earlier today, post-tick) skip this pass — their first midnight
	// snapshot doesn't exist yet, so there's nothing to anchor on. They'll be
	// picked up on the next nightly tick.
	cutoff := time.Now().UTC().Truncate(24 * time.Hour)
	// SEC-08 retry window: only recalibrate connections created within the last
	// maxRebuildRetryDays so a perpetually-failing connection ages out rather
	// than re-egressing credentials to the rebuilder every night forever.
	createdAfter := cutoff.AddDate(0, 0, -maxRebuildRetryDays)

	conns, err := s.connSvc.ListUnfinalizedExternalRebuilds(ctx, cutoff, createdAfter, externalRebuilderExchanges)
	if err != nil {
		s.logger.Error("midnight recalibration: list unfinalized failed", zap.Error(err))
		return
	}
	if len(conns) == 0 {
		return
	}

	s.logger.Info("midnight recalibration: starting", zap.Int("count", len(conns)))
	start := time.Now()
	var success, failed int

	var skipped int
	for _, conn := range conns {
		if err := s.recalibrateOne(ctx, conn); err != nil {
			if errors.Is(err, errRebuildCredentialGap) {
				// Nothing to retry: the credential cannot rebuild, and the
				// connector already says so on the connection's status.
				skipped++
				continue
			}
			failed++
			// LOG-CREDS-001: identifiers only — recalibrateOne wraps every
			// error so the resulting string never contains plaintext creds.
			s.logger.Warn("midnight recalibration: connection failed",
				zap.String("user_uid", conn.UserUID),
				zap.String("exchange", conn.Exchange),
				zap.String("label", conn.Label),
				zap.Error(err),
			)
			continue
		}
		success++
	}

	s.logger.Info("midnight recalibration: done",
		zap.Int("success", success),
		zap.Int("failed", failed),
		zap.Int("skipped", skipped),
		zap.Duration("duration", time.Since(start)),
	)
}

// recalibrateOne re-runs one connection's external rebuild with the
// EndEquityOverride anchor. Returns an error so the caller can log/count;
// the caller is responsible for the next-connection retry policy.
//
// SEC-ZK-001: the decrypted `creds` value is dropped after the rebuilder
// call. Errors returned from this function never embed `creds` (rebuilder
// errors carry HTTP status only, decryption errors carry the encryption
// library's message which never echoes the ciphertext).
func (s *SyncService) recalibrateOne(ctx context.Context, conn *repository.ExchangeConnection) error {
	// 1. Read the just-written midnight live snapshot as ground-truth anchor.
	latest, err := s.snapshotRepo.GetLatestByUserExchangeLabel(ctx, conn.UserUID, conn.Exchange, conn.Label)
	if err != nil {
		return fmt.Errorf("get latest snapshot: %w", err)
	}
	if latest == nil || latest.TotalEquity <= 0 {
		return fmt.Errorf("no usable midnight snapshot (equity zero or missing)")
	}

	// 2. Decrypt credentials. Identical path to reconstructHistory.
	creds, err := s.connSvc.GetDecryptedCredentialsByLabel(ctx, conn.UserUID, conn.Exchange, conn.Label)
	if err != nil {
		return fmt.Errorf("decrypt credentials: %w", err)
	}

	if gap := rebuildCredentialGap(conn.Exchange, creds.APIKey); gap != "" {
		creds = nil
		return fmt.Errorf("%w: %s", errRebuildCredentialGap, gap)
	}

	// 3. Dispatch to rebuilder with EndEquityOverride. Skips fetchLiveEquity
	// on the rebuilder side — anchors the walk's offset on latest.TotalEquity.
	res, err := s.rebuilder.Rebuild(ctx, rebuilderclient.RebuildRequest{
		UserUID:  conn.UserUID,
		Exchange: conn.Exchange,
		Label:    conn.Label,
		Credentials: rebuilderclient.Credentials{
			// HL stores wallet in APIKey; harmless for other exchanges (the
			// rebuilder picks the right credential field per its registry).
			WalletAddress: creds.APIKey,
			APIKey:        creds.APIKey,
			APISecret:     creds.APISecret,
			Passphrase:    creds.Passphrase,
		},
		EndEquityOverride: latest.TotalEquity,
	})
	// LOG-CREDS-001: drop the local plaintext reference promptly. Best-effort
	// (Go strings are immutable — the original heap allocation may live until
	// GC) but prevents accidental reuse of `creds` later in this scope.
	creds = nil
	if err != nil {
		return fmt.Errorf("rebuilder request: %w", err)
	}

	// 4. Overwrite the previously-rebuilt snapshots with the recalibrated ones.
	// persistHistoricalSnapshots is upsert — re-writing the same date keys
	// just updates equity/breakdown in place. firstSync=false because we
	// know history already exists (we wouldn't be in this list otherwise).
	// SEC-08: only finalize after the snapshots actually persist. A persist
	// failure leaves rebuild_finalized_at NULL so the connection retries on the
	// next tick (bounded by maxRebuildRetryDays) instead of being silently
	// marked done with no recalibrated history written.
	if err := s.persistHistoricalSnapshots(ctx, conn, res.Snapshots, false, sourceExternalRebuilder, reconstructOpts{coveredFrom: res.CoveredFrom}); err != nil {
		return fmt.Errorf("persist recalibrated snapshots: %w", err)
	}

	// 5. Stamp finalized so we don't reprocess this connection.
	if err := s.connSvc.MarkRebuildFinalized(ctx, conn.ID, time.Now().UTC()); err != nil {
		return fmt.Errorf("mark finalized: %w", err)
	}

	// 6. Nudge analytics like every other rebuild path does — without this
	// the finalized (re-anchored) history sits in the enclave DB while Neon
	// serves the pre-recalibration metrics until the analytics cron catches
	// up. Best-effort by design.
	s.notifyHistoryRebuilt(ctx, conn.UserUID)
	return nil
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
func (s *SyncService) syncFromHistoricalProvider(ctx context.Context, connMeta *repository.ExchangeConnection, provider connector.HistoricalSnapshotProvider, since time.Time, opts reconstructOpts) error {
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

	historicalSnapshots, err := provider.GetHistoricalSnapshots(ctx, since)
	if err != nil {
		if errors.Is(err, connector.ErrFlexTokenBusy) {
			// Planned pacing on a shared Flex token, not a failure — the
			// deferred retry owns the recovery. Error-level here would page
			// someone every midnight for a mechanism working as designed.
			s.logger.Info("historical snapshots deferred (shared flex token)",
				zap.String("user_uid", connMeta.UserUID),
				zap.String("exchange", connMeta.Exchange),
				zap.Error(err),
			)
			return nil
		}
		s.logger.Error("historical snapshots fetch failed",
			zap.String("user_uid", connMeta.UserUID),
			zap.String("exchange", connMeta.Exchange),
			zap.Error(err),
		)
		// E-M4: every caller used to discard this. The live snapshot still
		// got written and the sync was recorded "completed", so a
		// reconstruction failing every night — the boundary self-heal for
		// cTrader, the whole Flex window for IBKR — left no trace anywhere a
		// human looks.
		return err
	}

	s.persistHistoricalSnapshots(ctx, connMeta, historicalSnapshots, firstSync, sourceInEnclave, opts)
	return nil
}

// noteReconstructionFailure records a failed history reconstruction on the
// live sync's result. The snapshot itself is fine, so this must not fail the
// sync: it rides in CapabilityWarnings, which recordSyncStatus writes into
// sync_statuses.errorMessage under the "warning:" prefix with the status left
// "completed" — the one channel analytics already propagates (E-M4).
func noteReconstructionFailure(result *SyncResult, err error) {
	if result == nil || err == nil {
		return
	}
	result.CapabilityWarnings = append(result.CapabilityWarnings, "history_reconstruction_failed")
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
	opts reconstructOpts,
) error {
	// Today is owned by the live branch — never reconstruct it.
	now := time.Now().UTC()
	todayKey := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)

	// Gap repair: drop everything outside the requested days BEFORE anything
	// else, so the expansion log, the inception rule and the upsert all see
	// only the days we intend to write.
	if opts.window.isSet() {
		before := len(historicalSnapshots)
		historicalSnapshots = opts.window.filter(historicalSnapshots)
		s.logger.Info("history reconstruction restricted to a day window",
			zap.String("user_uid", connMeta.UserUID),
			zap.String("exchange", connMeta.Exchange),
			zap.String("label", connMeta.Label),
			zap.Time("window_from", opts.window.from),
			zap.Time("window_to", opts.window.to),
			zap.Int("days_returned", before),
			zap.Int("days_kept", len(historicalSnapshots)),
		)
		if len(historicalSnapshots) == 0 {
			s.logger.Warn("history reconstruction: provider returned nothing inside the window — nothing written",
				zap.String("user_uid", connMeta.UserUID),
				zap.String("exchange", connMeta.Exchange),
				zap.String("label", connMeta.Label),
			)
			return nil
		}
	}

	s.logHistoryExpansion(ctx, connMeta, historicalSnapshots, firstSync)

	snapshots, skippedToday := buildHistoricalSnapshots(connMeta, historicalSnapshots, todayKey, source == sourceExternalRebuilder)
	if len(snapshots) == 0 {
		return nil
	}

	// A bounded repair window cannot be the account's inception by
	// construction — it targets days surrounded by existing history. Skipping
	// keeps UX-001 from stamping the window's first day as starting capital,
	// which would book the whole account as a deposit on that day.
	var superseded *repository.Snapshot
	if !opts.window.isSet() {
		superseded = s.applyInceptionDeposit(ctx, connMeta, snapshots)
	}
	// The connect hook runs the live sync BEFORE this reconstruction, on
	// purpose (it writes the equity anchor the rebuild dispatch reads). That
	// live row is the connection's first, so it stamped the inception deposit
	// on TODAY — and the reconstruction then stamps the real inception day
	// underneath it, because applyInceptionDeposit only looks for snapshots
	// EARLIER than its own earliest. Both guards pass, both stamp, and the
	// account reports roughly twice the capital it was ever given. Riding in
	// the same batch keeps the correction atomic with the history that
	// supersedes it.
	if superseded != nil {
		snapshots = append(snapshots, superseded)
		s.logger.Info("clearing the connect-time inception deposit superseded by reconstructed history",
			zap.String("user_uid", connMeta.UserUID),
			zap.String("exchange", connMeta.Exchange),
			zap.String("label", connMeta.Label),
			zap.Time("cleared_day", superseded.Timestamp),
		)
	}

	if existing, rerr := s.snapshotRepo.GetByUserAndDateRange(ctx, connMeta.UserUID, time.Unix(0, 0).UTC(), time.Now().UTC().Add(24*time.Hour)); rerr == nil {
		mine := make([]*repository.Snapshot, 0, len(existing))
		for _, e := range existing {
			if e.Exchange == connMeta.Exchange && e.Label == connMeta.Label {
				mine = append(mine, e)
			}
		}
		if day, measured, bad := contradictedDay(snapshots, mine); bad {
			// A dry run exists to inspect a disagreement, so returning here
			// silenced the dump in exactly the case worth dumping: the
			// operator saw one rejected day and never the series around it.
			if opts.dryRun {
				s.logger.Warn("history reconstruction DRY RUN — contradicts a measured day (series follows)",
					zap.String("user_uid", connMeta.UserUID),
					zap.String("exchange", connMeta.Exchange),
					zap.String("label", connMeta.Label),
					zap.String("day", day.Timestamp.Format("2006-01-02")),
					zap.Float64("rebuilt_equity", day.TotalEquity),
					zap.Float64("measured_equity", measured),
				)
			} else {
				s.logger.Error("history reconstruction rejected — contradicts a measured day",
					zap.String("user_uid", connMeta.UserUID),
					zap.String("exchange", connMeta.Exchange),
					zap.String("label", connMeta.Label),
					zap.String("source", source),
					zap.String("day", day.Timestamp.Format("2006-01-02")),
					zap.Float64("rebuilt_equity", day.TotalEquity),
					zap.Float64("measured_equity", measured),
					zap.Int("days_discarded", len(snapshots)),
					zap.String("hint", "the reconstruction did not reproduce a day the live sync measured; nothing was written"),
				)
				return fmt.Errorf("reconstruction contradicts the measured equity on %s", day.Timestamp.Format("2006-01-02"))
			}
		}
	}

	// Dry run: everything above already happened (including, for non-ZK
	// exchanges, the rebuilder round-trip) — we just stop short of the write
	// and log what WOULD have been persisted so it can be compared against the
	// live snapshots bracketing the gap.
	if opts.dryRun {
		if opts.collect != nil {
			opts.collect(snapshots)
		}
		for _, sn := range snapshots {
			s.logger.Info("history reconstruction DRY RUN — would upsert",
				zap.String("user_uid", connMeta.UserUID),
				zap.String("exchange", connMeta.Exchange),
				zap.String("label", connMeta.Label),
				zap.String("day", sn.Timestamp.Format("2006-01-02")),
				zap.Float64("total_equity", sn.TotalEquity),
				zap.Float64("realized_balance", sn.RealizedBalance),
				zap.Float64("deposits", sn.Deposits),
				zap.Float64("withdrawals", sn.Withdrawals),
				zap.Int("total_trades", sn.TotalTrades),
			)
		}
		s.logger.Info("history reconstruction DRY RUN — nothing written",
			zap.String("user_uid", connMeta.UserUID),
			zap.String("exchange", connMeta.Exchange),
			zap.String("label", connMeta.Label),
			zap.Int("days", len(snapshots)),
		)
		return nil
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
		return err
	}

	// A reconstruction that moves dates leaves its own previous output behind:
	// the upsert rewrites the days it still emits and cannot know about the
	// ones it stopped emitting. Only the external rebuilder's rows can be
	// pruned — an in-enclave rebuild writes the same origin flag as the live
	// branch, so there is nothing to tell them apart by.
	if source == sourceExternalRebuilder {
		keep := make([]time.Time, 0, len(snapshots))
		for _, sn := range snapshots {
			keep = append(keep, sn.Timestamp)
		}
		// A bounded repair owns only its window. A full run owns everything the
		// rebuild could actually see — not the beginning of time: the venue
		// rations its ledger, and a day older than the rebuild's horizon was
		// never examined. Deleting those would truncate a customer's record to
		// the venue's retention on every reconstruction, and nothing could
		// bring them back.
		pruneFrom := opts.coveredFrom
		if opts.window.isSet() {
			pruneFrom = opts.window.from
		}
		switch pruned, perr := s.snapshotRepo.PruneRebuiltDaysOutside(ctx, connMeta.UserUID, connMeta.Exchange, connMeta.Label, pruneFrom, keep); {
		case perr != nil && !errors.Is(perr, repository.ErrOriginUnavailable):
			// The series that matters is written; a stale day left behind is
			// worth a warning, not a failed reconstruction.
			s.logger.Warn("history reconstruction: stale rebuilt days not pruned",
				zap.String("user_uid", connMeta.UserUID),
				zap.String("exchange", connMeta.Exchange),
				zap.String("label", connMeta.Label),
				zap.Error(perr),
			)
		case perr == nil && pruned > 0:
			s.logger.Info("history reconstruction pruned days it no longer produces",
				zap.String("user_uid", connMeta.UserUID),
				zap.String("exchange", connMeta.Exchange),
				zap.String("label", connMeta.Label),
				zap.Int64("days_pruned", pruned),
			)
		}
	}

	s.logger.Info("history reconstruction completed",
		zap.String("user_uid", connMeta.UserUID),
		zap.String("exchange", connMeta.Exchange),
		zap.String("source", source),
		zap.Int("snapshots_upserted", len(snapshots)),
		zap.Int("skipped_today", skippedToday),
		zap.Int("total_days", len(historicalSnapshots)),
	)
	return nil
}

// applyInceptionDeposit stamps the inception-deposit convention (UX-001) on a
// reconstructed series: when the EARLIEST day of the batch carries equity but
// no deposit AND no older snapshot exists for this connection, that equity is
// the account's starting capital — record it as a deposit, or the aggregated
// view books it as pure gain the day the connection joins (observed: an IBKR
// PEA funded by an in-kind transfer predating the Flex window showed +73% the
// day it appeared, inflating the user's TWR from ~83% to 216%). The
// no-older-row check makes the rule stable as the Flex window rolls forward:
// the old first day stays in DB, so later window starts never get a phantom
// mid-series deposit. Best-effort — a failed history lookup changes nothing.
// Returns the pre-existing snapshot whose own inception stamp this series
// supersedes, with its deposit already zeroed, for the caller to persist in the
// same batch. Nil when there is nothing to correct.
func (s *SyncService) applyInceptionDeposit(ctx context.Context, connMeta *repository.ExchangeConnection, snapshots []*repository.Snapshot) *repository.Snapshot {
	earliest := earliestFundedDay(snapshots)
	if earliest == nil {
		return nil
	}
	// One read of the connection's whole existing timeline serves both checks:
	// whether history already reaches further back (then this is not inception)
	// and whether a connect-time stamp is now superseded.
	existing, err := s.snapshotRepo.GetByUserAndDateRange(ctx, connMeta.UserUID, time.Unix(0, 0).UTC(), time.Now().UTC().Add(24*time.Hour))
	if err != nil {
		return nil
	}
	mine := make([]*repository.Snapshot, 0, len(existing))
	for _, p := range existing {
		if p.Exchange == connMeta.Exchange && p.Label == connMeta.Label {
			mine = append(mine, p)
		}
	}
	stamp, superseded := resolveInception(earliest, mine)
	if stamp {
		earliest.Deposits = earliest.TotalEquity
		s.logger.Info("inception deposit stamped on reconstructed series (UX-001)",
			zap.String("user_uid", connMeta.UserUID),
			zap.String("exchange", connMeta.Exchange),
			zap.String("label", connMeta.Label),
			zap.Time("inception_day", earliest.Timestamp),
			zap.Float64("deposit", earliest.Deposits),
		)
	}
	return superseded
}

// reproductionTolerance separates two instruments measuring one account from
// two instruments measuring different accounts. It is not a judgement about how
// wrong a reconstruction may be — a cent-exact match is unreachable, because
// the live path and the rebuilder price the same holdings from different
// sources at different instants.
//
// Read off the first live run of this gate rather than picked: the observed
// divergences fell in two clumps with two decades of empty space between them,
// 0.004% on a $5,301 hyperliquid account against 1.4% on a $125,414 binance one
// and 14% on a dust account. Anything inside the first clump is the instruments
// disagreeing about a price; anything past it is them disagreeing about the
// account. The 50% the rebuilder's own gate carried sat far above both, which
// is why it published a $22,244 error.
const reproductionTolerance = 0.001

// contradictedDay reports the first day a reconstruction disagrees with a day
// we measured directly, and by how much.
//
// The gate this feeds replaces asking "is the gap small enough", which needs a
// threshold somebody has to pick and defend. Reproduction is decidable: where a
// rebuilt day and a live day describe the same date, they describe the same
// account on that date, so they must agree. When they do not, the
// reconstruction measured something else — a wallet the live path cannot see, a
// window that ran out, a walk anchored wrong — and every OTHER day it produced
// is built the same way. Rejecting the batch whole is the only honest read.
//
// Measured 2026-08-31: a binance rebuild wrote $116,040.69 for a day the live
// sync had measured at $94,584.28. The rebuilder's own witness gate passed it,
// tolerating up to 50%.
func contradictedDay(rebuilt, existing []*repository.Snapshot) (*repository.Snapshot, float64, bool) {
	measured := make(map[time.Time]float64, len(existing))
	for _, e := range existing {
		if e.IsHistorical || e.FromExternalRebuilder {
			// Reconstructed too — nothing independent to check against. The
			// external rebuilder's rows carry from_external_rebuilder rather
			// than is_historical; holding a fresh reconstruction to them
			// rejected every rebuild that changed its own method (2026-09-13:
			// the first mark-to-market pass on a bybit history was thrown out
			// against the realized-only pass of the same morning).
			continue
		}
		if e.TotalEquity <= 0 {
			// A live row at zero is far more often a degenerate sync than a
			// funded account measured empty, and the rest of the service
			// already refuses to anchor on one (recalibrateOne). Witnessing
			// against it rejected a whole reconstruction over a day nobody
			// trusts — observed on the first live run of this gate.
			continue
		}
		measured[e.Timestamp.UTC().Truncate(24*time.Hour)] = e.TotalEquity
	}
	for _, r := range rebuilt {
		eq, ok := measured[r.Timestamp.UTC().Truncate(24*time.Hour)]
		if !ok {
			continue
		}
		if math.Abs(r.TotalEquity-eq)/math.Abs(eq) > reproductionTolerance {
			return r, eq, true
		}
	}
	return nil, 0, false
}

// clampAvailableMargin holds the invariant that free margin cannot exceed the
// equity it is drawn from. Venues report the two on different bases: Binance
// derives availableBalance from the wallet while equity is the margin balance,
// and MT5 reports free margin against the account balance — so an account
// sitting on an unrealized loss came out with more margin free than it owned.
// Measured 2026-08-31: 240 MT5 rows and 90 Binance rows, every one of them off
// by exactly the unrealized loss. A venue that genuinely lends beyond equity
// (an MT5 credit line) is flattened to equity here too; representing that would
// take a field of its own rather than a free-margin figure the dashboard reads
// against equity.
func clampAvailableMargin(available, equity float64) float64 {
	if available > equity {
		return equity
	}
	return available
}

// earliestFundedDay returns the batch's first day that carries equity, which
// is the day the account actually starts. Taking the first day outright let a
// zero-padded reconstruction abandon the whole inception rule — including the
// clearing of the connect-time stamp sitting above it, which then survived as
// a phantom deposit worth the entire balance. Nil when nothing is funded.
func earliestFundedDay(snapshots []*repository.Snapshot) *repository.Snapshot {
	var earliest *repository.Snapshot
	for _, sn := range snapshots {
		if sn.TotalEquity <= 0 {
			continue
		}
		if earliest == nil || sn.Timestamp.Before(earliest.Timestamp) {
			earliest = sn
		}
	}
	return earliest
}

// resolveInception decides, for a reconstructed series landing on a
// connection's existing rows, whether its earliest day needs the UX-001 stamp
// and whether a connect-time stamp beneath it is now superseded.
//
// The two are independent. A series whose earliest day already carries its
// deposit (the external rebuilder reads it off the ledger) needs no stamp, but
// the connect-time stamp it lands under is superseded all the same. Coupling
// the clearing to the stamping left such a stamp in place: a whole balance
// booked as a mid-series deposit, read as -31% on the day.
func resolveInception(earliest *repository.Snapshot, mine []*repository.Snapshot) (stamp bool, superseded *repository.Snapshot) {
	for _, p := range mine {
		if p.Timestamp.Before(earliest.Timestamp) {
			return false, nil // history extends further back — this is not the inception day
		}
	}
	return earliest.Deposits == 0, supersededConnectStamp(mine, earliest.Timestamp)
}

// inceptionStampEpsilon is how close deposits must sit to equity to read as a
// stamp rather than a transfer that happens to land near the balance.
const inceptionStampEpsilon = 1e-6

// supersededConnectStamp picks out the inception deposit the connect-time live
// sync wrote moments earlier, now that real history exists beneath it.
//
// Deliberately narrow: EXACTLY ONE pre-existing row. That is the signature of
// the connect flow, where the live sync writes a single day and the
// reconstruction follows. A connection with a longer live history is a
// different situation (IBKR re-emits its whole Flex window every sync, and a
// widened window leaves its own stale stamp behind) and is left alone rather
// than guessed at.
//
// A deposit equal to the day's entire equity is what the stamp looks like; a
// real transfer of exactly the full balance would mean an account emptied to
// zero and refunded in full on the one day it was already being reconstructed.
func supersededConnectStamp(existing []*repository.Snapshot, inception time.Time) *repository.Snapshot {
	if len(existing) != 1 {
		return nil
	}
	live := existing[0]
	if !live.Timestamp.After(inception) {
		return nil
	}
	if live.Deposits <= 0 || math.Abs(live.Deposits-live.TotalEquity) > inceptionStampEpsilon {
		return nil
	}
	cleared := *live
	cleared.Deposits = 0
	return &cleared
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

// withoutLeadingEmptyDays drops the run of empty days a reconstruction emits
// ahead of the account's real history: the external rebuilder pads the whole
// window it was asked for, so a 90-day request against a 9-day-old account
// arrives with 81 zero-equity days in front. Persisted, they draw months of
// flat zero on the dashboard and leave the batch's earliest day carrying no
// equity, which is how applyInceptionDeposit came to skip clearing a
// connect-time stamp. Interior empty days are kept — an account emptied
// mid-history is real data.
func withoutLeadingEmptyDays(hs []*connector.HistoricalSnapshot) []*connector.HistoricalSnapshot {
	ordered := make([]*connector.HistoricalSnapshot, len(hs))
	copy(ordered, hs)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].Date.Before(ordered[j].Date) })
	for i, h := range ordered {
		if !isEmptyReconstructedDay(h) {
			return ordered[i:]
		}
	}
	return nil
}

func isEmptyReconstructedDay(h *connector.HistoricalSnapshot) bool {
	return h.TotalEquity == 0 && h.RealizedBalance == 0 &&
		h.Deposits == 0 && h.Withdrawals == 0 && h.TotalTrades == 0
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
	for _, h := range withoutLeadingEmptyDays(hs) {
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
			AvailableMargin: clampAvailableMargin(totalAvailMargin, h.TotalEquity),
			Volume:          h.TotalVolume,
			Trades:          h.TotalTrades,
			TradingFees:     h.TotalFees,
			FundingFees:     h.FundingFees,
			LongTrades:      h.LongTrades,
			ShortTrades:     h.ShortTrades,
			LongVolume:      h.LongVolume,
			ShortVolume:     h.ShortVolume,
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

	// Inception-deposit convention (UX-001) moved to applyInceptionDeposit in
	// persistHistoricalSnapshots: stamping the BATCH-earliest day here was
	// wrong for rolling reconstruction windows — every re-run whose window
	// start had advanced stamped a NEW day while the previous stamp stayed in
	// DB, compounding phantom deposits day after day (observed: a $253k
	// account carrying $506k of "deposits" across two consecutive days). The
	// replacement checks the DB for older rows and stamps only the true
	// inception day.

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
	// inception deposit was set by applyInceptionDeposit — don't re-apply.
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

// fetchBalanceWithCollapseGuard reads the balance and, when it collapses
// below collapseGuardRatio of this connection's last persisted snapshot,
// re-reads once after collapseGuardDelay and returns the second reading.
// See the SANITY-001 constants for the rationale. Best-effort on the
// history lookup: no prior snapshot (fresh connection) means no guard.
func (s *SyncService) fetchBalanceWithCollapseGuard(ctx context.Context, conn connector.Connector, connMeta *repository.ExchangeConnection) (*connector.Balance, error) {
	balance, err := conn.GetBalance(ctx)
	if err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	startOfDay := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	prior, herr := s.snapshotRepo.GetByUserAndDateRange(ctx, connMeta.UserUID, now.Add(-collapseGuardLookback), now)
	if herr != nil {
		return balance, nil
	}
	var lastEquity float64
	var lastAt time.Time
	for _, snap := range prior {
		// Same connection only, and exclude today's own row — it may itself
		// be the poisoned reading this guard exists to prevent.
		if snap.Exchange != connMeta.Exchange || snap.Label != connMeta.Label || !snap.Timestamp.Before(startOfDay) {
			continue
		}
		if snap.Timestamp.After(lastAt) {
			lastAt = snap.Timestamp
			lastEquity = snap.TotalEquity
		}
	}
	if lastEquity < collapseGuardFloorUSD || balance.Equity >= lastEquity*collapseGuardRatio {
		return balance, nil
	}

	s.logger.Warn("balance collapsed vs last snapshot — re-reading once (SANITY-001)",
		zap.String("user_uid", connMeta.UserUID),
		zap.String("exchange", connMeta.Exchange),
		zap.String("label", connMeta.Label),
		zap.Float64("read_equity", balance.Equity),
		zap.Float64("last_equity", lastEquity),
		zap.Duration("delay", collapseGuardDelay),
	)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(collapseGuardDelay):
	}
	second, err2 := conn.GetBalance(ctx)
	if err2 != nil {
		return nil, fmt.Errorf("collapse-guard re-read: %w", err2)
	}
	s.logger.Info("collapse-guard re-read result (SANITY-001)",
		zap.String("user_uid", connMeta.UserUID),
		zap.String("exchange", connMeta.Exchange),
		zap.String("label", connMeta.Label),
		zap.Float64("first_equity", balance.Equity),
		zap.Float64("second_equity", second.Equity),
	)
	return second, nil
}

func (s *SyncService) aggregateTrades(trades []*connector.Trade) *aggregatedBreakdown {
	agg := &aggregatedBreakdown{}

	for _, t := range trades {
		volume := t.Notional()
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

	// E-M1: collapse concurrent misses on the same connection. The check
	// above and the create below used to be a plain check-then-create, so two
	// syncs racing on a cold cache each built a connector from the same
	// stored credentials. For cTrader that hands the same single-use refresh
	// token to two instances: the second refresh is rejected, and the token
	// the loser persists is already dead.
	key := fmt.Sprintf("%s:%s:%x", exchange, userUID, credsHash[:8])
	built, err, _ := s.connectorGroup.Do(key, func() (any, error) {
		// Re-check under the flight: a winner may have just populated it.
		if s.connCache != nil {
			if cached := s.connCache.Get(exchange, userUID, credsHash); cached != nil {
				return cached, nil
			}
		}

		conn, err := s.factory.Create(&connector.Credentials{
			Exchange:   exchange,
			APIKey:     creds.APIKey,
			APISecret:  creds.APISecret,
			Passphrase: creds.Passphrase,
		})
		if err != nil {
			return nil, err
		}

		// Wire token persistence for OAuth connectors so refreshed tokens
		// survive container restarts. Without this, every boot starts from the
		// original (possibly expired) access_token stored in DB.
		if tr, ok := conn.(connector.TokenRefreshable); ok && s.connSvc != nil {
			tr.SetTokenPersister(func(ctx context.Context, accessToken, refreshToken string) error {
				return s.connSvc.PersistOAuthTokens(ctx, userUID, exchange, label, accessToken, refreshToken)
			})
		}

		if s.connCache != nil {
			s.connCache.Put(exchange, userUID, credsHash, conn)
		}
		return conn, nil
	})
	if err != nil {
		return nil, err
	}
	conn, ok := built.(connector.Connector)
	if !ok || conn == nil {
		return nil, fmt.Errorf("build connector for %s: empty result", exchange)
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
) ([]*connector.Cashflow, []string, error) {
	if s.connSvc == nil {
		return nil, nil, fmt.Errorf("connection service not configured")
	}

	creds, err := s.connSvc.GetDecryptedCredentialsByLabel(ctx, userUID, exchange, label)
	if err != nil {
		return nil, nil, fmt.Errorf("decrypt credentials: %w", err)
	}

	conn, err := s.getOrCreateConnector(strings.ToLower(exchange), userUID, label, creds)
	if err != nil {
		return nil, nil, fmt.Errorf("build connector: %w", err)
	}

	cfFetcher, ok := conn.(connector.CashflowFetcher)
	if !ok {
		return nil, nil, fmt.Errorf("connector %s does not support cashflow fetching", exchange)
	}

	flows, err := cfFetcher.GetCashflows(ctx, since)
	if err != nil {
		return nil, nil, err
	}
	// A degraded fetch (shortened window, unpriceable currency) is success
	// with a caveat; the dump states the caveat instead of hiding it behind
	// either a clean result or a sanitized error.
	var warnings []string
	if cw, ok := conn.(connector.CapabilityWarner); ok {
		warnings = cw.CapabilityWarnings()
	}
	return flows, warnings, nil
}

// ErrBalanceProbeUnsupported is returned when the connection's connector has
// no BalanceProber. The admin endpoint turns it into a 501 so "this venue was
// never wired for probing" cannot be misread as "this account has no balance".
var ErrBalanceProbeUnsupported = errors.New("connector does not implement balance probing")

// ProbeBalance decrypts the connection's credentials the way a sync does,
// asks the venue for its balance once, and returns that payload verbatim
// beside the Balance the connector derived from it.
//
// It exists because a free-margin figure that looks wrong on the dashboard
// cannot be settled from the outside: the raw fields never leave the enclave,
// and re-reading them by hand would mean handling the customer's API key. The
// caller is the loopback-only admin endpoint; nothing here is persisted and
// the amounts are not logged.
func (s *SyncService) ProbeBalance(
	ctx context.Context,
	userUID, exchange, label string,
) (*connector.BalanceProbe, error) {
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

	prober, ok := conn.(connector.BalanceProber)
	if !ok {
		return nil, fmt.Errorf("%s: %w", exchange, ErrBalanceProbeUnsupported)
	}
	return prober.ProbeBalance(ctx)
}

// ErrFundingProbeUnsupported is returned when the connection's connector has
// no FundingProber, which for most venues means there is no second wallet to
// read rather than a missing feature.
var ErrFundingProbeUnsupported = errors.New("connector does not implement funding probing")

// ProbeFunding reports the wallet that sits outside the tracked perimeter.
//
// A transfer out of the trading account is booked as a withdrawal and its
// return as a deposit, so a round trip turns whatever the holding did in
// between into capital. Telling that apart from a real exit needs the other
// wallet's ledger, which only this enclave can read. Nothing is persisted and
// the rows are not logged.
func (s *SyncService) ProbeFunding(
	ctx context.Context,
	userUID, exchange, label string,
	since time.Time,
) (*connector.FundingProbe, error) {
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

	prober, ok := conn.(connector.FundingProber)
	if !ok {
		return nil, fmt.Errorf("%s: %w", exchange, ErrFundingProbeUnsupported)
	}
	return prober.ProbeFunding(ctx, since)
}

// DumpRawCashflows returns the broker's unfiltered balance-operation ledger for
// a user, every operationType preserved. Diagnostic for balance jumps that the
// deposit/withdraw filter (op 0/1) can't explain — a demo reset that surfaces
// as a spurious return has no BALANCE_DEPOSIT entry, so this exposes whichever
// operation (or none) actually moved the balance.
func (s *SyncService) DumpRawCashflows(
	ctx context.Context,
	userUID, exchange, label string,
	since time.Time,
) ([]connector.RawBalanceOp, error) {
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

	rawFetcher, ok := conn.(connector.RawCashflowFetcher)
	if !ok {
		return nil, fmt.Errorf("connector %s does not support raw cashflow fetching", exchange)
	}

	return rawFetcher.GetRawCashflowEntries(ctx, since)
}

// ErrRawStatementUnsupported is returned when the connection's connector has
// no statement document to hand over.
var ErrRawStatementUnsupported = errors.New("connector does not expose a raw statement")

// DumpRawStatement returns the venue's own statement document for one
// connection, whole. On a venue that rations statements (IBKR: roughly one
// Flex request per token per six hours, refusals counted), taking the document
// once is what stops every further question from costing another wait.
func (s *SyncService) DumpRawStatement(ctx context.Context, userUID, exchange, label string) ([]byte, string, error) {
	if s.connSvc == nil {
		return nil, "", fmt.Errorf("connection service not configured")
	}

	creds, err := s.connSvc.GetDecryptedCredentialsByLabel(ctx, userUID, exchange, label)
	if err != nil {
		return nil, "", fmt.Errorf("decrypt credentials: %w", err)
	}

	conn, err := s.getOrCreateConnector(strings.ToLower(exchange), userUID, label, creds)
	if err != nil {
		return nil, "", fmt.Errorf("build connector: %w", err)
	}

	provider, ok := conn.(connector.RawStatementProvider)
	if !ok {
		return nil, "", fmt.Errorf("%s: %w", exchange, ErrRawStatementUnsupported)
	}
	return provider.GetRawStatement(ctx)
}

// ExchangeMetadataRefresh reports what one connection's metadata probe stored.
type ExchangeMetadataRefresh struct {
	Exchange string `json:"exchange"`
	Label    string `json:"label,omitempty"`
	KYCLevel string `json:"kyc_level,omitempty"`
	IsPaper  bool   `json:"is_paper"`
	// PaperProbed false means the broker was never asked — the connector has no
	// paper detector, or the probe failed. IsPaper is then meaningless.
	PaperProbed bool `json:"paper_probed"`
	// Err is raw; the egress layer sanitizes it into Error.
	Err   error  `json:"-"`
	Error string `json:"error,omitempty"`
}

// RefreshExchangeMetadata re-probes paper/live and KYC status for a user's
// active connections and stores the result. Detection otherwise runs only at
// connect time, so a connection created before its connector gained a probe —
// or one whose stored flags never took — keeps whatever its row was created
// with, and false is indistinguishable from never-written.
//
// exchange and label narrow the pass; empty means every active connection.
func (s *SyncService) RefreshExchangeMetadata(ctx context.Context, userUID, exchange, label string) ([]*ExchangeMetadataRefresh, error) {
	if s.connSvc == nil {
		return nil, fmt.Errorf("connection service not configured")
	}

	conns, err := s.connSvc.GetActiveConnections(ctx, userUID)
	if err != nil {
		return nil, fmt.Errorf("list connections: %w", err)
	}

	wantExchange := strings.ToLower(strings.TrimSpace(exchange))
	wantLabel := strings.TrimSpace(label)

	out := make([]*ExchangeMetadataRefresh, 0, len(conns))
	for _, c := range conns {
		if wantExchange != "" && strings.ToLower(c.Exchange) != wantExchange {
			continue
		}
		if wantLabel != "" && c.Label != wantLabel {
			continue
		}

		item := &ExchangeMetadataRefresh{Exchange: c.Exchange, Label: c.Label}
		out = append(out, item)

		creds, err := s.connSvc.GetDecryptedCredentialsByLabel(ctx, userUID, c.Exchange, c.Label)
		if err != nil {
			item.Err = err
			continue
		}

		conn, err := s.getOrCreateConnector(strings.ToLower(c.Exchange), userUID, c.Label, creds)
		if err != nil {
			item.Err = err
			continue
		}

		captured := s.connSvc.CaptureExchangeMetadata(ctx, c.ID, conn)
		item.KYCLevel = captured.KYCLevel
		item.IsPaper = captured.IsPaper
		item.PaperProbed = captured.PaperProbed
		item.Err = captured.Err
	}

	return out, nil
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
	case "ctrader", "mt4", "mt5", "ig", "ig_demo":
		return connector.MarketCFD
	case "okx", "bybit":
		// Both read their SWAP fills in GetTrades, so every trade is typed swap,
		// while the unified-account equity has no per-market split to follow.
		// Left on the spot default, equity filed under spot with the trades
		// under swap: one bucket carried the equity with no trades, the other
		// the trades with no equity, and per-market return divided by that
		// zero. Send equity where the trades already are.
		return connector.MarketSwap
	default:
		return connector.MarketSpot
	}
}

func (m *marketAgg) hasData() bool {
	// fundingFees counts: a market can carry a real cost with no trade and no
	// equity of its own (a position opened last window and still held charges
	// funding every night). Omitting it dropped the whole bucket at
	// persistence, so the cost was fetched and then thrown away.
	return m.trades > 0 || m.equity > 0 || m.availableMargin > 0 || m.fundingFees != 0
}

// fundingMarketType names the bucket an exchange's funding charges belong in —
// the same bucket its trades land in, or the cost sits alone in a market the
// account never traded. Perp venues fund swaps, which is why that is the
// default; CFD brokers charge the same overnight cost on a CFD position.
func fundingMarketType(exchange string) string {
	switch strings.ToLower(exchange) {
	case "ig", "ig_demo", "ctrader", "mt4", "mt5":
		return connector.MarketCFD
	default:
		return connector.MarketSwap
	}
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

func (a *aggregatedBreakdown) totalLongTrades() int {
	return a.stocks.longTrades + a.spot.longTrades + a.swap.longTrades + a.futures.longTrades + a.options.longTrades +
		a.margin.longTrades + a.earn.longTrades + a.cfd.longTrades + a.forex.longTrades + a.commodities.longTrades
}

func (a *aggregatedBreakdown) totalShortTrades() int {
	return a.stocks.shortTrades + a.spot.shortTrades + a.swap.shortTrades + a.futures.shortTrades + a.options.shortTrades +
		a.margin.shortTrades + a.earn.shortTrades + a.cfd.shortTrades + a.forex.shortTrades + a.commodities.shortTrades
}

func (a *aggregatedBreakdown) totalLongVolume() float64 {
	return a.stocks.longVolume + a.spot.longVolume + a.swap.longVolume + a.futures.longVolume + a.options.longVolume +
		a.margin.longVolume + a.earn.longVolume + a.cfd.longVolume + a.forex.longVolume + a.commodities.longVolume
}

func (a *aggregatedBreakdown) totalShortVolume() float64 {
	return a.stocks.shortVolume + a.spot.shortVolume + a.swap.shortVolume + a.futures.shortVolume + a.options.shortVolume +
		a.margin.shortVolume + a.earn.shortVolume + a.cfd.shortVolume + a.forex.shortVolume + a.commodities.shortVolume
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
			AvailableMargin: clampAvailableMargin(globalAvailableMargin, globalEquity),
			Volume:          a.totalVolume(),
			Trades:          totalTrades,
			TradingFees:     a.totalFees(), // toRepoMetrics splits fees by kind; aggregate only keeps total
			LongTrades:      a.totalLongTrades(),
			ShortTrades:     a.totalShortTrades(),
			LongVolume:      a.totalLongVolume(),
			ShortVolume:     a.totalShortVolume(),
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
