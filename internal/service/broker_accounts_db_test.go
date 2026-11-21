package service

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/trackrecord/enclave/internal/connector"
	"github.com/trackrecord/enclave/internal/repository"
)

func (h *dbHarness) pinnedOf(t *testing.T, label string) string {
	t.Helper()
	conn, err := h.conns.GetByUserExchangeLabel(h.ctx, dbUser, "ibkr", label)
	if err != nil {
		t.Fatalf("look up %s: %v", label, err)
	}
	pinned, err := h.conns.BrokerAccounts(h.ctx, conn.ID)
	if err != nil {
		t.Fatalf("read pin of %s: %v", label, err)
	}
	return pinned
}

func TestDBGuardBrokerAccounts_PinnedConnectionKeepsItsAccounts(t *testing.T) {
	h := newDBHarness(t)
	h.seedConnection(t, "ibkr", "main", dbKey, dbSecret)
	guard := func(accounts []string, past bool) error {
		return h.connSvc.GuardBrokerAccounts(h.ctx, dbUser, "ibkr", "main", accounts, past)
	}

	if err := guard([]string{"U0000001"}, false); err != nil {
		t.Fatalf("first statement: %v", err)
	}
	pinned := h.pinnedOf(t, "main")
	if pinned == "" {
		t.Fatal("the first statement must pin the connection")
	}
	if strings.Contains(strings.ToUpper(pinned), "U0000001") {
		t.Fatalf("the pin stores the account id itself: %q", pinned)
	}
	if err := guard([]string{"U0000001"}, false); err != nil {
		t.Fatalf("same account: %v", err)
	}
	for _, accounts := range [][]string{{"U0000001", "U0000001F"}, {"U0000002"}, {"U0000001F"}} {
		if err := guard(accounts, false); !errors.Is(err, ErrBrokerAccountsChanged) {
			t.Fatalf("accounts %v: error %v, want ErrBrokerAccountsChanged", accounts, err)
		}
	}
	if h.pinnedOf(t, "main") != pinned {
		t.Fatal("a refused statement must leave the pin as it was")
	}
}

func TestDBGuardBrokerAccounts_PastPeriodMayLackAnAccount(t *testing.T) {
	h := newDBHarness(t)
	h.seedConnection(t, "ibkr", "main", dbKey, dbSecret)
	guard := func(accounts []string, past bool) error {
		return h.connSvc.GuardBrokerAccounts(h.ctx, dbUser, "ibkr", "main", accounts, past)
	}
	if err := guard([]string{"U0000001F"}, true); err != nil {
		t.Fatalf("past statement of an unpinned connection: %v", err)
	}
	if h.pinnedOf(t, "main") != "" {
		t.Fatal("a past statement must not pin the connection")
	}
	if err := guard([]string{"U0000001", "U0000001F"}, false); err != nil {
		t.Fatalf("daily statement: %v", err)
	}
	if err := guard([]string{"U0000001"}, true); err != nil {
		t.Fatalf("a month before the segment opened: %v", err)
	}
	if err := guard([]string{"U0000002"}, true); !errors.Is(err, ErrBrokerAccountsChanged) {
		t.Fatalf("past statement of another account: error %v, want ErrBrokerAccountsChanged", err)
	}
}

func TestDBGuardBrokerAccounts_AccountMeasuredOnce(t *testing.T) {
	h := newDBHarness(t)
	h.seedConnection(t, "ibkr", "main", dbKey, dbSecret)
	h.seedConnection(t, "ibkr", "second", "synthetic-api-key-2", "synthetic-api-secret-2")
	if err := h.connSvc.GuardBrokerAccounts(h.ctx, dbUser, "ibkr", "main", []string{"U0000001"}, false); err != nil {
		t.Fatalf("pin main: %v", err)
	}

	err := h.connSvc.GuardBrokerAccounts(h.ctx, dbUser, "ibkr", "second", []string{"U0000001", "U0000001F"}, false)
	if !errors.Is(err, ErrBrokerAccountMeasured) {
		t.Fatalf("a second connection over the same account: error %v, want ErrBrokerAccountMeasured", err)
	}
	if h.pinnedOf(t, "second") != "" {
		t.Fatal("a refused connection must stay unpinned")
	}
	if err := h.connSvc.GuardBrokerAccounts(h.ctx, dbUser, "ibkr", "second", []string{"U0000001F"}, false); err != nil {
		t.Fatalf("the segment alone on a second connection: %v", err)
	}
}

type scopedStub struct {
	connector.Connector
	accounts []string
}

func (s *scopedStub) SetAccountGuard(func([]string, bool) error) {}
func (s *scopedStub) MeasuredAccounts() []string                 { return s.accounts }

func TestDBBrokerAccountsAtConnect_RefusesAnAccountAlreadyConnected(t *testing.T) {
	h := newDBHarness(t)
	h.seedConnection(t, "ibkr", "main", dbKey, dbSecret)
	if err := h.connSvc.GuardBrokerAccounts(h.ctx, dbUser, "ibkr", "main", []string{"U0000001"}, false); err != nil {
		t.Fatalf("pin main: %v", err)
	}
	if _, err := h.connSvc.brokerAccountsAtConnect(h.ctx, &scopedStub{accounts: []string{"U0000001"}}, dbUser, "ibkr", "copy"); !errors.Is(err, ErrBrokerAccountMeasured) {
		t.Fatalf("error %v, want ErrBrokerAccountMeasured", err)
	}
	pinned, err := h.connSvc.brokerAccountsAtConnect(h.ctx, &scopedStub{accounts: []string{"U0000001F"}}, dbUser, "ibkr", "segment")
	if err != nil || pinned == "" {
		t.Fatalf("pin %q, error %v: the segment alone is free", pinned, err)
	}
}

func TestDBUnpinBrokerAccounts_NextStatementRepins(t *testing.T) {
	h := newDBHarness(t)
	h.seedConnection(t, "ibkr", "main", dbKey, dbSecret)
	if err := h.connSvc.GuardBrokerAccounts(h.ctx, dbUser, "ibkr", "main", []string{"U0000001"}, false); err != nil {
		t.Fatalf("pin: %v", err)
	}
	if err := h.sync.UnpinBrokerAccounts(h.ctx, dbUser, "ibkr", "main"); err != nil {
		t.Fatalf("unpin: %v", err)
	}
	if err := h.connSvc.GuardBrokerAccounts(h.ctx, dbUser, "ibkr", "main", []string{"U0000001", "U0000001F"}, false); err != nil {
		t.Fatalf("statement with the segment after unpin: %v", err)
	}
	if err := h.connSvc.GuardBrokerAccounts(h.ctx, dbUser, "ibkr", "main", []string{"U0000001"}, false); !errors.Is(err, ErrBrokerAccountsChanged) {
		t.Fatalf("error %v, want the new pin to hold", err)
	}
}

// A segment added to the query lifts every day from the one it first held
// anything: the stored days, measured without it, cannot witness the rewrite
// once the operator has cleared the pin, and still refuse any other rebuild.
func TestDBRewriteAfterUnpinIsNotWitnessedByTheOldAccountSet(t *testing.T) {
	h := newDBHarness(t)
	h.seedConnection(t, "ibkr", "main", dbKey, dbSecret)
	if err := h.connSvc.GuardBrokerAccounts(h.ctx, dbUser, "ibkr", "main", []string{"U0000001"}, false); err != nil {
		t.Fatalf("pin: %v", err)
	}
	connMeta := h.connection(t, "ibkr", "main")

	today := startOfTodayUTC()
	d := func(n int) time.Time { return today.AddDate(0, 0, -10+n) }
	var stored []*repository.Snapshot
	var rebuilt []*connector.HistoricalSnapshot
	for n := 1; n <= 5; n++ {
		stored = append(stored, &repository.Snapshot{
			UserUID: dbUser, Exchange: "ibkr", Label: "main", Timestamp: d(n),
			TotalEquity: 1000, RealizedBalance: 1000,
			Breakdown: &repository.MarketBreakdown{Global: &repository.MarketMetrics{Equity: 1000}},
		})
		rebuilt = append(rebuilt, &connector.HistoricalSnapshot{Date: d(n), TotalEquity: 1500, RealizedBalance: 1500})
	}
	if err := h.snaps.UpsertBatch(h.ctx, stored); err != nil {
		t.Fatalf("seed history: %v", err)
	}

	persist := func(opts reconstructOpts) error {
		return h.sync.persistHistoricalSnapshots(h.ctx, connMeta, rebuilt, false, sourceInEnclave, opts)
	}
	if err := persist(reconstructOpts{rewrite: true}); err == nil {
		t.Fatal("a rewrite of a pinned connection must stay witnessed by its stored days")
	}
	if err := h.sync.UnpinBrokerAccounts(h.ctx, dbUser, "ibkr", "main"); err != nil {
		t.Fatalf("unpin: %v", err)
	}
	if err := persist(reconstructOpts{}); err == nil {
		t.Fatal("a rebuild that is not a rewrite must stay witnessed after unpin")
	}
	if err := persist(reconstructOpts{rewrite: true}); err != nil {
		t.Fatalf("rewrite after unpin: %v", err)
	}
	for n := 1; n <= 5; n++ {
		got, err := h.snaps.GetByUserExchangeLabelAndDate(h.ctx, dbUser, "ibkr", "main", d(n))
		if err != nil || got.TotalEquity != 1500 {
			t.Fatalf("%s = %+v (err %v), want the new account set's 1500", d(n).Format(time.DateOnly), got, err)
		}
	}
}
