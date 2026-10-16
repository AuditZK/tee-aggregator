package service

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/trackrecord/enclave/internal/connector"
	"github.com/trackrecord/enclave/internal/repository"
	"go.uber.org/zap"
)

const (
	keepAliveInterval    = 15 * time.Minute
	keepAliveJitter      = 2 * time.Minute
	keepAliveMinAge      = 5 * time.Minute
	keepAliveWorkers     = 4
	keepAliveConnTimeout = 45 * time.Second
	keepAliveMaxRows     = 10000
)

// OAuthKeepAlive refreshes every active connection of tokenGuardedExchanges on
// a short cycle. A Saxo refresh token dies about 40 minutes after it is
// issued, far sooner than the daily sync comes round, so without this pass
// every such connection is dead by its first midnight.
type OAuthKeepAlive struct {
	logger *zap.Logger

	interval time.Duration
	jitter   time.Duration
	minAge   time.Duration
	workers  int

	list       func(ctx context.Context) ([]*repository.ExchangeConnection, error)
	build      func(conn *repository.ExchangeConnection) (connector.Connector, error)
	guardFor   func(conn *repository.ExchangeConnection) connector.TokenGuard
	markReauth func(ctx context.Context, conn *repository.ExchangeConnection, err error)

	cancel   context.CancelFunc
	stopOnce sync.Once
	wg       sync.WaitGroup
}

// NewOAuthKeepAlive wires the keep-alive to the connection rows, the sync
// service's connector factory and its sync-status writer.
func NewOAuthKeepAlive(connSvc *ConnectionService, syncSvc *SyncService, logger *zap.Logger) *OAuthKeepAlive {
	return &OAuthKeepAlive{
		logger:   logger,
		interval: keepAliveInterval,
		jitter:   keepAliveJitter,
		minAge:   keepAliveMinAge,
		workers:  keepAliveWorkers,
		list:     connSvc.listActiveTokenGuarded,
		// The connector starts empty: the guard hands it the stored pair.
		build: func(conn *repository.ExchangeConnection) (connector.Connector, error) {
			return syncSvc.factory.Create(&connector.Credentials{Exchange: conn.Exchange})
		},
		guardFor: func(conn *repository.ExchangeConnection) connector.TokenGuard {
			return connSvc.TokenGuard(conn.UserUID, conn.Exchange, conn.Label)
		},
		markReauth: func(ctx context.Context, conn *repository.ExchangeConnection, err error) {
			syncSvc.recordSyncStatus(ctx, conn, &SyncResult{
				UserUID:  conn.UserUID,
				Exchange: conn.Exchange,
				Label:    conn.Label,
				Error:    egressSyncError("refresh token", err),
			}, time.Now().UTC())
		},
	}
}

// Start runs a pass at once, then one every interval, give or take the jitter.
func (k *OAuthKeepAlive) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	k.cancel = cancel
	k.wg.Add(1)
	go k.run(ctx)
	k.logger.Info("oauth keep-alive started",
		zap.Duration("interval", k.interval),
		zap.Strings("scope", tokenGuardedExchanges),
	)
}

// Stop cancels the pass in progress and waits for it.
func (k *OAuthKeepAlive) Stop() {
	k.stopOnce.Do(func() {
		if k.cancel != nil {
			k.cancel()
		}
		k.wg.Wait()
		k.logger.Info("oauth keep-alive stopped")
	})
}

func (k *OAuthKeepAlive) run(ctx context.Context) {
	defer k.wg.Done()
	for {
		k.runOnce(ctx)
		timer := time.NewTimer(k.nextDelay())
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (k *OAuthKeepAlive) nextDelay() time.Duration {
	if k.jitter <= 0 {
		return k.interval
	}
	return k.interval - k.jitter + rand.N(2*k.jitter)
}

type keepAliveOutcome int

const (
	keepAliveOK keepAliveOutcome = iota
	keepAliveRefused
	keepAliveAlreadyDead
	keepAliveNotConfigured
	keepAliveFailed
)

type keepAliveTally struct {
	mu     sync.Mutex
	counts map[keepAliveOutcome]int
}

func (t *keepAliveTally) add(o keepAliveOutcome) {
	t.mu.Lock()
	t.counts[o]++
	t.mu.Unlock()
}

// runOnce refreshes every listed connection, a few at a time, within one
// interval.
func (k *OAuthKeepAlive) runOnce(ctx context.Context) map[keepAliveOutcome]int {
	passCtx, cancel := context.WithTimeout(ctx, k.interval)
	defer cancel()

	conns, err := k.list(passCtx)
	if err != nil {
		k.logger.Warn("oauth keep-alive: listing connections failed, retried next cycle", zap.Error(err))
		return nil
	}

	tally := &keepAliveTally{counts: map[keepAliveOutcome]int{}}
	jobs := make(chan *repository.ExchangeConnection)
	var wg sync.WaitGroup
	for range max(1, min(k.workers, len(conns))) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for conn := range jobs {
				tally.add(k.keepAlive(passCtx, conn))
			}
		}()
	}
feed:
	for _, conn := range conns {
		select {
		case jobs <- conn:
		case <-passCtx.Done():
			break feed
		}
	}
	close(jobs)
	wg.Wait()

	if n := tally.counts[keepAliveNotConfigured]; n > 0 {
		k.logger.Error("oauth keep-alive: app credentials missing or refused, connections will expire",
			zap.Int("connections", n),
			zap.String("hint", "set SAXO_APP_KEY and SAXO_APP_SECRET (and SAXO_REDIRECT_URI if the app requires it) in the enclave environment"),
		)
	}
	k.logger.Debug("oauth keep-alive pass done",
		zap.Int("connections", len(conns)),
		zap.Int("ok", tally.counts[keepAliveOK]),
		zap.Int("refused", tally.counts[keepAliveRefused]),
		zap.Int("failed", tally.counts[keepAliveFailed]),
	)
	return tally.counts
}

func (k *OAuthKeepAlive) keepAlive(ctx context.Context, conn *repository.ExchangeConnection) keepAliveOutcome {
	connCtx, cancel := context.WithTimeout(ctx, keepAliveConnTimeout)
	defer cancel()

	c, err := k.build(conn)
	if err != nil {
		k.logger.Warn("oauth keep-alive: connector not built", zap.String("connection_id", conn.ID), zap.Error(err))
		return keepAliveFailed
	}
	ka, canKeep := c.(connector.OAuthKeepAliver)
	rg, canGuard := c.(connector.RefreshGuarded)
	if !canKeep || !canGuard {
		k.logger.Error("oauth keep-alive: connector cannot be kept alive", zap.String("connection_id", conn.ID))
		return keepAliveFailed
	}
	rg.SetRefreshGuard(k.guardFor(conn))

	err = ka.KeepAlive(connCtx, k.minAge)
	switch {
	case err == nil:
		return keepAliveOK
	case errors.Is(err, errRefreshTokenKnownDead):
		return keepAliveAlreadyDead
	case errors.Is(err, connector.ErrRefreshRefused):
		k.logger.Warn("oauth keep-alive: the broker refused the refresh token, the account must be reconnected",
			zap.String("connection_id", conn.ID),
			zap.Error(err),
		)
		markCtx, markCancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer markCancel()
		k.markReauth(markCtx, conn, err)
		return keepAliveRefused
	case connector.ClassifyConnectFailure(err) == connector.ConnectFailureNotConfigured:
		return keepAliveNotConfigured
	default:
		k.logger.Warn("oauth keep-alive: refresh failed, retried next cycle",
			zap.String("connection_id", conn.ID),
			zap.Error(err),
		)
		return keepAliveFailed
	}
}
