package service

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/trackrecord/enclave/internal/connector"
	"github.com/trackrecord/enclave/internal/repository"
)

// postedFunding is a settlement stamped on the hour and posted by the venue
// some time later.
type postedFunding struct {
	at, posted time.Time
	amount     float64
}

// lateFundingConnector serves funding the way Binance and Bybit do: rows
// stamped at or after since, among those posted by the time of the read.
// ignoreSince makes it return every posted row, the way an overlapping
// window would see them again.
type lateFundingConnector struct {
	partialTradesConnector
	rows        []postedFunding
	clock       time.Time
	ignoreSince bool
	asked       []time.Time
}

func (c *lateFundingConnector) GetTrades(context.Context, time.Time, time.Time) ([]*connector.Trade, error) {
	return nil, nil
}

func (c *lateFundingConnector) GetFundingFees(_ context.Context, _ []string, since time.Time) ([]*connector.FundingFee, error) {
	c.asked = append(c.asked, since)
	var out []*connector.FundingFee
	for _, r := range c.rows {
		if r.posted.After(c.clock) || (!c.ignoreSince && r.at.Before(since)) {
			continue
		}
		out = append(out, &connector.FundingFee{Amount: r.amount, Symbol: "BTCUSDT", Timestamp: r.at})
	}
	return out, nil
}

// Three settlements a day, each posted 40 s after its hour; the midnight pass
// reads at 00:00:25 and the next one resumes at that pass's start stamp.
func settlementsPostedLate(first time.Time, days int) []postedFunding {
	var rows []postedFunding
	for i := 0; i < days*3; i++ {
		at := first.Add(time.Duration(i) * 8 * time.Hour)
		rows = append(rows, postedFunding{at: at, posted: at.Add(40 * time.Second), amount: -float64(i + 1)})
	}
	return rows
}

func TestReadActivity_MidnightSettlementPostedAfterTheReadCountsNextDayOnce(t *testing.T) {
	s := NewSyncService(nil, nil, nil, zap.NewNop())
	meta := &repository.ExchangeConnection{UserUID: "u", Exchange: "binance", Label: "main"}
	d := time.Date(2026, 11, 10, 0, 0, 0, 0, time.UTC)
	// Settlements -1..-9: D-1 00:00, 08:00, 16:00, D 00:00, 08:00, 16:00,
	// D+1 00:00, 08:00, 16:00.
	conn := &lateFundingConnector{rows: settlementsPostedLate(d.Add(-24*time.Hour), 3)}

	read := func(from, now time.Time) float64 {
		conn.clock = now
		return s.readActivity(context.Background(), conn, meta, from, now, nil).fundingCharges
	}

	// The pass of D reads at D 00:00:25, before D 00:00 (-4) is posted. Its
	// row carries day D-1: -1 -2 -3.
	if got := read(d.Add(-24*time.Hour+20*time.Second), d.Add(25*time.Second)); got != -6 {
		t.Fatalf("row D funding = %v, want -6 (day D-1's three settlements)", got)
	}
	// The pass of D+1 resumes at the D pass's start stamp, 00:00:20, after
	// the D 00:00 stamp. It still counts -4, with -5 and -6, and leaves D+1
	// 00:00 (-7) to the next row even though it is not posted yet anyway.
	if got := read(d.Add(20*time.Second), d.Add(24*time.Hour+25*time.Second)); got != -15 {
		t.Fatalf("row D+1 funding = %v, want -15 (-4 -5 -6: the late midnight row once)", got)
	}
	if asked := conn.asked[1]; !asked.Equal(d) {
		t.Fatalf("funding asked since %v, want the closed day's midnight %v", asked, d)
	}

	// A pass rewriting row D+1 later that day, with D+1 00:00 now posted,
	// reads the same day: same funding, the new midnight row not added.
	if got := read(d, d.Add(34*time.Hour)); got != -15 {
		t.Fatalf("rewritten row D+1 funding = %v, want -15 again", got)
	}

	// The pass of D+2 counts -7 -8 -9 and nothing from before.
	if got := read(d.Add(24*time.Hour+20*time.Second), d.Add(48*time.Hour+25*time.Second)); got != -24 {
		t.Fatalf("row D+2 funding = %v, want -24", got)
	}
}

// A venue that hands back rows the previous window already counted (an
// overlapping read) does not get them counted twice.
func TestReadActivity_FundingSeenByTwoWindowsCountsOnce(t *testing.T) {
	s := NewSyncService(nil, nil, nil, zap.NewNop())
	meta := &repository.ExchangeConnection{UserUID: "u", Exchange: "bybit", Label: "main"}
	d := time.Date(2026, 11, 10, 0, 0, 0, 0, time.UTC)
	conn := &lateFundingConnector{rows: settlementsPostedLate(d.Add(-24*time.Hour), 2), ignoreSince: true}

	total := 0.0
	for _, pass := range []struct{ from, now time.Time }{
		{d.Add(-24*time.Hour + 20*time.Second), d.Add(25 * time.Second)},
		{d.Add(20 * time.Second), d.Add(24*time.Hour + 25*time.Second)},
	} {
		conn.clock = pass.now
		total += s.readActivity(context.Background(), conn, meta, pass.from, pass.now, nil).fundingCharges
	}
	// -1..-6, each once.
	if total != -21 {
		t.Fatalf("funding over two rows = %v, want -21", total)
	}
}

// IG reconciles its flows from the balance, which moves the instant a charge
// is made. The reconciliation subtracts the charges of the instant window it
// compares; the row stores the closed days'. Given the closed days instead, a
// rewrite of row D at 14:00 read a 09:00 commission as a withdrawal and row
// D+1 read it back as a deposit.
func TestReadActivity_BalanceReconciliationUsesTheInstantWindow(t *testing.T) {
	s := NewSyncService(nil, nil, nil, zap.NewNop())
	meta := &repository.ExchangeConnection{UserUID: "u", Exchange: "ig", Label: "main"}
	d := time.Date(2026, 11, 10, 0, 0, 0, 0, time.UTC)
	charge := d.Add(9 * time.Hour)
	conn := &lateFundingConnector{rows: []postedFunding{{at: charge, posted: charge, amount: -12}}}

	flow := func(from, now time.Time, prevSettled, settled float64) (act liveActivity, dep, wd float64) {
		conn.clock = now
		act = s.readActivity(context.Background(), conn, meta, from, now, nil)
		dep, wd = InferCapitalFlow(
			ReconInputs{SettledBalance: prevSettled},
			ReconInputs{SettledBalance: settled, Charges: act.reconciliationCharges()},
			reconThreshold(settled),
		)
		return act, dep, wd
	}

	// Row D rewritten at D 14:00 (a manual sync): the window runs from the
	// fallback D-1 00:00, the balance from row D-1's 1000 to 988.
	act, dep, wd := flow(d.Add(-24*time.Hour), d.Add(14*time.Hour), 1000, 988)
	if dep != 0 || wd != 0 {
		t.Fatalf("row D books +%v/-%v, want no flow: the drop is the commission", dep, wd)
	}
	if act.fundingCharges != 0 {
		t.Fatalf("row D stores funding %v, want 0: the commission belongs to day D", act.fundingCharges)
	}

	// Row D+1 resumes at the rewrite's start stamp, the balance unmoved.
	act, dep, wd = flow(d.Add(14*time.Hour), d.Add(24*time.Hour+25*time.Second), 988, 988)
	if dep != 0 || wd != 0 {
		t.Fatalf("row D+1 books +%v/-%v, want no flow: the balance did not move", dep, wd)
	}
	if act.fundingCharges != -12 {
		t.Fatalf("row D+1 stores funding %v, want -12 (day D, closed)", act.fundingCharges)
	}
}
