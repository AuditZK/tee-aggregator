package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/trackrecord/enclave/internal/connector"
)

// ErrBrokerAccountsChanged refuses a statement that measures other broker
// accounts than the ones the connection's stored history was built from.
var ErrBrokerAccountsChanged = errors.New("broker_accounts_changed: the statement covers other accounts than this connection's history")

// ErrBrokerAccountMeasured refuses a connection, or its first statement, when
// one of its accounts is already measured by another connection of the user.
var ErrBrokerAccountMeasured = errors.New("broker_account_already_connected: an account in this statement is already measured by another connection")

// brokerAccountGuardTimeout bounds the database reads behind one guard check.
const brokerAccountGuardTimeout = 10 * time.Second

// pinBrokerAccounts states accounts as the value a connection is pinned to:
// per-user fingerprints, sorted, never the account ids themselves.
func pinBrokerAccounts(userUID string, accounts []string) string {
	fps := make([]string, 0, len(accounts))
	for _, a := range accounts {
		sum := sha256.Sum256([]byte("broker-account\x00" + userUID + "\x00" + strings.ToUpper(strings.TrimSpace(a))))
		fps = append(fps, hex.EncodeToString(sum[:16]))
	}
	sort.Strings(fps)
	return strings.Join(fps, ",")
}

// pinCovers reports whether every account of current is in pinned.
func pinCovers(pinned, current string) bool {
	set := map[string]bool{}
	for _, fp := range strings.Split(pinned, ",") {
		set[fp] = true
	}
	for _, fp := range strings.Split(current, ",") {
		if !set[fp] {
			return false
		}
	}
	return true
}

// pinsOverlap reports whether two pinned values share an account.
func pinsOverlap(a, b string) bool {
	if a == "" || b == "" {
		return false
	}
	set := map[string]bool{}
	for _, fp := range strings.Split(a, ",") {
		set[fp] = true
	}
	for _, fp := range strings.Split(b, ",") {
		if set[fp] {
			return true
		}
	}
	return false
}

// GuardBrokerAccounts vets the accounts a statement measures against the
// connection's pin. An unpinned connection is pinned to today's, unless
// another connection already measures one of them: summed in the consolidated
// view, that account would count twice. A pinned one accepts only its own
// accounts, so a query edited to tick others cannot splice their balance into
// the stored history; repinning is an operator's decision
// (UnpinBrokerAccounts), taken with a rebuild of the history. A statement of
// a past period may lack an account opened since, and pins nothing.
func (s *ConnectionService) GuardBrokerAccounts(ctx context.Context, userUID, exchange, label string, accounts []string, past bool) error {
	if s.repo == nil || len(accounts) == 0 || !s.repo.HasBrokerAccountsCol(ctx) {
		return nil
	}
	conn, err := s.repo.GetByUserExchangeLabel(ctx, userUID, exchange, label)
	if err != nil {
		return fmt.Errorf("look up connection: %w", err)
	}
	current := pinBrokerAccounts(userUID, accounts)
	stored, err := s.repo.BrokerAccounts(ctx, conn.ID)
	if err != nil {
		return fmt.Errorf("read pinned accounts: %w", err)
	}
	switch {
	case stored == current:
		return nil
	case past && (stored == "" || pinCovers(stored, current)):
		return nil
	case stored == "":
		if err := s.checkBrokerAccountsFree(ctx, userUID, exchange, label, current); err != nil {
			return err
		}
		if err := s.repo.SetBrokerAccounts(ctx, conn.ID, current); err != nil {
			return fmt.Errorf("pin accounts: %w", err)
		}
		return nil
	}
	return ErrBrokerAccountsChanged
}

// checkBrokerAccountsFree refuses accounts another active connection of the
// user on the same exchange already measures.
func (s *ConnectionService) checkBrokerAccountsFree(ctx context.Context, userUID, exchange, ownLabel, pinned string) error {
	others, err := s.repo.BrokerAccountsByUser(ctx, userUID, exchange)
	if err != nil {
		return fmt.Errorf("read other pinned accounts: %w", err)
	}
	for label, theirs := range others {
		if label != strings.TrimSpace(ownLabel) && pinsOverlap(pinned, theirs) {
			return ErrBrokerAccountMeasured
		}
	}
	return nil
}

// brokerAccountsAtConnect returns the pin a new connection takes from the
// statement its test read, refusing accounts another connection measures.
// Empty when the connector cannot tell or the test read no statement.
func (s *ConnectionService) brokerAccountsAtConnect(ctx context.Context, conn connector.Connector, userUID, exchange, label string) (string, error) {
	scoped, ok := conn.(connector.AccountScoped)
	if !ok || s.repo == nil || !s.repo.HasBrokerAccountsCol(ctx) {
		return "", nil
	}
	accounts := scoped.MeasuredAccounts()
	if len(accounts) == 0 {
		return "", nil
	}
	pinned := pinBrokerAccounts(userUID, accounts)
	if err := s.checkBrokerAccountsFree(ctx, userUID, exchange, label, pinned); err != nil {
		return "", err
	}
	return pinned, nil
}

// UnpinBrokerAccounts clears a connection's pin: its next statement pins the
// accounts it then measures. For an operator adding a segment to a holder's
// query, before rebuilding the whole history from the new statements.
func (s *ConnectionService) UnpinBrokerAccounts(ctx context.Context, userUID, exchange, label string) error {
	if s.repo == nil {
		return fmt.Errorf("connection service not configured")
	}
	conn, err := s.repo.GetByUserExchangeLabel(ctx, userUID, exchange, label)
	if err != nil {
		return fmt.Errorf("look up connection: %w", err)
	}
	return s.repo.SetBrokerAccounts(ctx, conn.ID, "")
}

// awaitsRepin reports whether the connection's pin is cleared, as
// UnpinBrokerAccounts leaves it until the next daily statement pins the new
// account set.
func (s *ConnectionService) awaitsRepin(ctx context.Context, connID string) (bool, error) {
	if s == nil || s.repo == nil || !s.repo.HasBrokerAccountsCol(ctx) {
		return false, nil
	}
	pinned, err := s.repo.BrokerAccounts(ctx, connID)
	if err != nil {
		return false, fmt.Errorf("read pinned accounts: %w", err)
	}
	return pinned == "", nil
}

// UnpinBrokerAccounts is ConnectionService.UnpinBrokerAccounts for the admin
// routes, which reach connections through the sync service.
func (s *SyncService) UnpinBrokerAccounts(ctx context.Context, userUID, exchange, label string) error {
	if s.connSvc == nil {
		return fmt.Errorf("connection service not configured")
	}
	return s.connSvc.UnpinBrokerAccounts(ctx, userUID, exchange, label)
}
