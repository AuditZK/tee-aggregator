package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/trackrecord/enclave/internal/connector"
	"github.com/trackrecord/enclave/internal/repository"
)

// statementConnector is a Flex-like venue: one statement, closed on asOf,
// answers the live balance, the trades, the cash flows and the day-by-day
// history, each flow and trade filed under its own date.
type statementConnector struct {
	asOf       time.Time
	closes     map[time.Time]float64
	flows      []*connector.Cashflow
	trades     []*connector.Trade
	rebuildErr error
}

func (c *statementConnector) GetBalance(context.Context) (*connector.Balance, error) {
	eq := c.closes[c.asOf]
	return &connector.Balance{Equity: eq, Available: eq, Currency: "USD"}, nil
}
func (c *statementConnector) GetPositions(context.Context) ([]*connector.Position, error) {
	return nil, nil
}
func (c *statementConnector) GetTrades(_ context.Context, start, end time.Time) ([]*connector.Trade, error) {
	var out []*connector.Trade
	for _, t := range c.trades {
		if !t.Timestamp.Before(start) && !t.Timestamp.After(end) {
			out = append(out, t)
		}
	}
	return out, nil
}
func (c *statementConnector) TestConnection(context.Context) error { return nil }
func (c *statementConnector) Exchange() string                     { return "ibkr" }
func (c *statementConnector) BalanceAsOf() time.Time               { return c.asOf }

func (c *statementConnector) GetCashflows(_ context.Context, since time.Time) ([]*connector.Cashflow, error) {
	var out []*connector.Cashflow
	for _, cf := range c.flows {
		if !cf.Timestamp.Before(since) {
			out = append(out, cf)
		}
	}
	return out, nil
}

func (c *statementConnector) GetHistoricalSnapshots(context.Context, time.Time) ([]*connector.HistoricalSnapshot, error) {
	if c.rebuildErr != nil {
		return nil, c.rebuildErr
	}
	var out []*connector.HistoricalSnapshot
	for day, eq := range c.closes {
		h := &connector.HistoricalSnapshot{Date: day, TotalEquity: eq, RealizedBalance: eq, Currency: "USD"}
		for _, cf := range c.flows {
			if sameDay(cf.Timestamp, day) {
				h.Deposits += cf.Amount
				h.Cashflows = append(h.Cashflows, cf)
			}
		}
		for _, t := range c.trades {
			if sameDay(t.Timestamp, day) {
				h.TotalTrades++
			}
		}
		out = append(out, h)
	}
	return out, nil
}

func sameDay(a, b time.Time) bool {
	return a.Year() == b.Year() && a.Month() == b.Month() && a.Day() == b.Day()
}

// A synthetic account funded on the statement's last day: the deposit and a
// trade of that day sit on the day the reconstruction writes and the stored
// row before it, so the live sync writes today the same close with an empty
// window. When the reconstruction fails, the live row is the only place the
// deposit can be booked.
func newStatementAccount(asOf time.Time) *statementConnector {
	first, second := asOf.AddDate(0, 0, -2), asOf.AddDate(0, 0, -1)
	return &statementConnector{
		asOf:   asOf,
		closes: map[time.Time]float64{first: 1000, second: 1010, asOf: 4010},
		flows: []*connector.Cashflow{
			{Amount: 1000, Currency: "USD", Timestamp: first},
			{Amount: 3000, Currency: "USD", Timestamp: asOf},
		},
		trades: []*connector.Trade{
			{ID: "t1", Symbol: "SYN", Side: "buy", Price: 10, Quantity: 1, Timestamp: asOf.Add(14*time.Hour + 30*time.Minute), MarketType: connector.MarketStocks},
		},
	}
}

func TestDBIBKRLiveRowLeavesTheRebuiltDaysTheirFlows(t *testing.T) {
	for _, p := range syncPaths {
		t.Run(p.name, func(t *testing.T) {
			h := newDBHarness(t)
			h.seedConnection(t, "ibkr", "main", dbKey, dbSecret)
			h.sync.SetFXSource(stubFX{})
			asOf := lastExpectedStatementDate(startOfTodayUTC())
			h.plant(dbKey, dbSecret, newStatementAccount(asOf))

			if res := p.run(h, "ibkr", "main"); res.Error != "" || res.Skipped {
				t.Fatalf("sync: %+v", res)
			}

			rebuilt, err := h.snaps.GetByUserExchangeLabelAndDate(h.ctx, dbUser, "ibkr", "main", asOf)
			if err != nil {
				t.Fatalf("read the statement's last day: %v", err)
			}
			if rebuilt.Deposits != 3000 || rebuilt.TotalTrades != 1 {
				t.Fatalf("statement day: deposits %v, trades %d, want 3000 and 1", rebuilt.Deposits, rebuilt.TotalTrades)
			}
			live := h.snapshotToday(t, "ibkr", "main")
			if live.TotalEquity != 4010 || live.Deposits != 0 || live.Withdrawals != 0 || live.TotalTrades != 0 {
				t.Fatalf("live row: equity %v, deposits %v, withdrawals %v, trades %d, want 4010 with nothing booked again",
					live.TotalEquity, live.Deposits, live.Withdrawals, live.TotalTrades)
			}
		})
	}
}

// A reconstruction that writes nothing, because the statement could not be
// fetched or because a stored day contradicts it, leaves the statement day
// unbooked: the live row books its flows and trades as before.
func TestDBIBKRLiveRowKeepsTheFlowsOfAnUnwrittenReconstruction(t *testing.T) {
	for _, tc := range []struct {
		name       string
		rebuildErr error
		storedEq   float64
	}{
		{"statement unavailable", errors.New("flex request failed: 1001"), 1000},
		{"refused by a stored day", nil, 5000},
	} {
		for _, p := range syncPaths {
			t.Run(tc.name+"/"+p.name, func(t *testing.T) {
				h := newDBHarness(t)
				h.seedConnection(t, "ibkr", "main", dbKey, dbSecret)
				h.sync.SetFXSource(stubFX{})
				asOf := lastExpectedStatementDate(startOfTodayUTC())
				account := newStatementAccount(asOf)
				account.rebuildErr = tc.rebuildErr
				h.plant(dbKey, dbSecret, account)

				prev := asOf.AddDate(0, 0, -1)
				if err := h.snaps.Upsert(h.ctx, &repository.Snapshot{
					UserUID: dbUser, Exchange: "ibkr", Label: "main", Timestamp: prev,
					TotalEquity: tc.storedEq, RealizedBalance: tc.storedEq,
					Breakdown: &repository.MarketBreakdown{Global: &repository.MarketMetrics{Equity: tc.storedEq}},
				}); err != nil {
					t.Fatalf("seed the previous live row: %v", err)
				}

				if res := p.run(h, "ibkr", "main"); res.Error != "" || res.Skipped {
					t.Fatalf("sync: %+v", res)
				}
				stored, err := h.snaps.GetByUserExchangeLabelAndDate(h.ctx, dbUser, "ibkr", "main", prev)
				if err != nil || stored.TotalEquity != tc.storedEq {
					t.Fatalf("previous row rewritten (%v, err %v): the reconstruction was meant to write nothing", stored, err)
				}
				live := h.snapshotToday(t, "ibkr", "main")
				if live.Deposits != 3000 || live.TotalTrades != 1 {
					t.Fatalf("live row: deposits %v, trades %d, want the statement day's 3000 and 1 booked here", live.Deposits, live.TotalTrades)
				}
			})
		}
	}
}

// A synthetic IBKR account whose live rows each hold the statement of the day
// before, with a day missing. The reconstruction states every day at its own
// date: it must be accepted, fill the missing day, and leave every rewritten
// day holding its own statement.
func TestDBIBKRReconstructionFillsTheDaysTheLiveSyncMissed(t *testing.T) {
	h := newDBHarness(t)
	h.seedConnection(t, "ibkr", "main", dbKey, dbSecret)
	connMeta, err := h.conns.GetByUserExchangeLabel(h.ctx, dbUser, "ibkr", "main")
	if err != nil {
		t.Fatalf("read connection: %v", err)
	}

	today := startOfTodayUTC()
	d := func(n int) time.Time { return today.AddDate(0, 0, -10+n) }
	closeOf := func(n int) float64 { return 1000 + 10*float64(n) }
	live := func(n int) *repository.Snapshot {
		eq := closeOf(n - 1)
		return &repository.Snapshot{
			UserUID: dbUser, Exchange: "ibkr", Label: "main", Timestamp: d(n),
			TotalEquity: eq, RealizedBalance: eq,
			Breakdown: &repository.MarketBreakdown{Global: &repository.MarketMetrics{Equity: eq}},
		}
	}
	if err := h.snaps.UpsertBatch(h.ctx, []*repository.Snapshot{live(2), live(3), live(4), live(6)}); err != nil {
		t.Fatalf("seed history: %v", err)
	}

	var rebuilt []*connector.HistoricalSnapshot
	for n := 1; n <= 6; n++ {
		rebuilt = append(rebuilt, &connector.HistoricalSnapshot{Date: d(n), TotalEquity: closeOf(n), RealizedBalance: closeOf(n)})
	}
	if err := h.sync.persistHistoricalSnapshots(h.ctx, connMeta, rebuilt, false, sourceInEnclave, reconstructOpts{}); err != nil {
		t.Fatalf("reconstruction rejected: %v", err)
	}

	rows, err := h.snaps.GetByUserAndDateRange(h.ctx, dbUser, d(0), today)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	byDay := map[time.Time]float64{}
	for _, r := range rows {
		byDay[r.Timestamp.UTC()] = r.TotalEquity
	}
	for n := 1; n <= 6; n++ {
		if got, ok := byDay[d(n)]; !ok || got != closeOf(n) {
			t.Fatalf("%s = %v (present %v), want its own close %v", d(n).Format(time.DateOnly), got, ok, closeOf(n))
		}
	}
}
