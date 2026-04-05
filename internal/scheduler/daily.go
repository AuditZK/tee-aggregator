package scheduler

import (
	"context"
	"sync"
	"time"

	"github.com/trackrecord/enclave/internal/repository"
	"github.com/trackrecord/enclave/internal/service"
	"go.uber.org/zap"
)

// SyncScheduler fires once per day at 00:00 UTC and syncs all users atomically.
// CRITICAL: Forces UTC timezone to prevent snapshot manipulation (TS parity).
type SyncScheduler struct {
	syncSvc  *service.SyncService
	userRepo *repository.UserRepo
	logger   *zap.Logger

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NewSyncScheduler creates a scheduler.
func NewSyncScheduler(
	syncSvc *service.SyncService,
	userRepo *repository.UserRepo,
	logger *zap.Logger,
) *SyncScheduler {
	return &SyncScheduler{
		syncSvc:  syncSvc,
		userRepo: userRepo,
		logger:   logger,
		stopCh:   make(chan struct{}),
	}
}

// Start begins the daily scheduler. Fires at next 00:00 UTC, then every 24h.
func (s *SyncScheduler) Start() {
	s.wg.Add(1)
	go s.run()

	next := timeUntilMidnightUTC()
	s.logger.Info("daily sync scheduler started",
		zap.String("policy", "once per day at 00:00 UTC"),
		zap.Duration("next_sync_in", next),
		zap.String("next_sync_at", time.Now().UTC().Add(next).Format(time.RFC3339)),
	)
}

// Stop gracefully stops the scheduler.
func (s *SyncScheduler) Stop() {
	close(s.stopCh)
	s.wg.Wait()
	s.logger.Info("daily sync scheduler stopped")
}

func (s *SyncScheduler) run() {
	defer s.wg.Done()

	timer := time.NewTimer(timeUntilMidnightUTC())

	for {
		select {
		case <-s.stopCh:
			timer.Stop()
			return

		case <-timer.C:
			s.executeDailySync()
			// Next tick: 24h from now (handles DST-free UTC correctly)
			timer.Reset(timeUntilMidnightUTC())
		}
	}
}

// timeUntilMidnightUTC returns the duration until the next 00:00 UTC.
func timeUntilMidnightUTC() time.Duration {
	now := time.Now().UTC()
	nextMidnight := time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 0, 0, time.UTC)
	return nextMidnight.Sub(now)
}

func (s *SyncScheduler) executeDailySync() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	now := time.Now().UTC()
	s.logger.Info("daily sync started", zap.String("time", now.Format(time.RFC3339)))
	start := time.Now()

	users, err := s.userRepo.GetAllWithConnections(ctx)
	if err != nil {
		s.logger.Error("failed to get users for daily sync", zap.Error(err))
		return
	}

	if len(users) == 0 {
		s.logger.Info("no users to sync")
		return
	}

	s.logger.Info("syncing all users", zap.Int("users", len(users)))

	var (
		userSyncedCount int
		successCount    int
		failCount       int
		mu              sync.Mutex
		wg              sync.WaitGroup
	)

	sem := make(chan struct{}, 3) // Max 3 concurrent user syncs (CCXT loads ~40MB per connector)

	for _, user := range users {
		wg.Add(1)
		go func(u *repository.User) {
			defer wg.Done()

			sem <- struct{}{}
			defer func() { <-sem }()

			results, err := s.syncSvc.SyncUserScheduledDueAtomic(ctx, u.UID, now)

			mu.Lock()
			defer mu.Unlock()

			if err != nil {
				failCount++
				s.logger.Error("user sync failed",
					zap.String("user_uid", u.UID),
					zap.Error(err),
				)
				return
			}

			if len(results) > 0 {
				userSyncedCount++
			}

			for _, r := range results {
				if r.Success {
					successCount++
				} else {
					failCount++
				}
			}
		}(user)
	}

	wg.Wait()

	s.logger.Info("daily sync completed",
		zap.Int("users_synced", userSyncedCount),
		zap.Int("total_users", len(users)),
		zap.Int("snapshots_success", successCount),
		zap.Int("snapshots_failed", failCount),
		zap.Duration("duration", time.Since(start)),
	)
}

// RunNow executes sync immediately for all users (for manual trigger / testing).
func (s *SyncScheduler) RunNow() {
	s.executeDailySync()
}
