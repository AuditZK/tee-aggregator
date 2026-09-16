package connector

import (
	"testing"
	"time"
)

// An IBKR contract is quoted per underlying unit and traded in contracts, so
// Price * Quantity is the value divided by the multiplier. A customer's
// options round trip of 2,030,051 reached the dashboard as 20,300, which read
// as 72 trades producing 578,062 of profit out of nothing.
func TestParseTradesFromReport_ContractMultiplier(t *testing.T) {
	report := []byte(`<FlexQueryResponse>
  <FlexStatements>
    <FlexStatement>
      <Trades>
        <Trade tradeID="1" symbol="TESTX 260731C00100000" buySell="SELL"
               tradePrice="29.42" quantity="-69" multiplier="100"
               ibCommission="-84.6" currency="USD" dateTime="20260730;143000"
               assetCategory="OPT" fifoPnlRealized="5000" />
        <Trade tradeID="2" symbol="TESTX" buySell="BUY"
               tradePrice="150" quantity="10"
               ibCommission="-1" currency="USD" dateTime="20260730;150000"
               assetCategory="STK" fifoPnlRealized="0" />
      </Trades>
    </FlexStatement>
  </FlexStatements>
</FlexQueryResponse>`)

	i := &IBKR{}
	trades, err := i.parseTradesFromReport(report,
		time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(trades) != 2 {
		t.Fatalf("got %d trades, want 2", len(trades))
	}

	opt := trades[0]
	if opt.Multiplier != 100 {
		t.Fatalf("option multiplier: got %v, want 100", opt.Multiplier)
	}
	if got, want := opt.Notional(), 29.42*69*100; got != want {
		t.Fatalf("option notional: got %v, want %v", got, want)
	}
	if opt.MarketType != MarketOptions {
		t.Fatalf("option market type: got %q, want %q", opt.MarketType, MarketOptions)
	}

	// A cash instrument carries no multiplier attribute at all, and must come
	// out exactly as it did before the field existed.
	stk := trades[1]
	if stk.Multiplier != 0 {
		t.Fatalf("stock multiplier: got %v, want 0", stk.Multiplier)
	}
	if got, want := stk.Notional(), 1500.0; got != want {
		t.Fatalf("stock notional: got %v, want %v", got, want)
	}
}

// The Flex query only carries the attribute when it selects the Multiplier
// field. Without it the value is the pre-fix one rather than a zero that would
// erase the day's volume outright.
func TestTradeNotional_WithoutAMultiplier(t *testing.T) {
	tr := &Trade{Price: 29.42, Quantity: 69}
	if got, want := tr.Notional(), 29.42*69; got != want {
		t.Fatalf("notional: got %v, want %v", got, want)
	}
}

func TestAggregateFlexTradesByDate_CountsContractValue(t *testing.T) {
	day := time.Date(2026, 7, 30, 14, 30, 0, 0, time.UTC)
	byDate := aggregateFlexTradesByDate([]*Trade{
		{Side: "sell", Price: 29.42, Quantity: 69, Multiplier: 100, Timestamp: day},
		{Side: "buy", Price: 150, Quantity: 10, Timestamp: day.Add(time.Hour)},
	})

	d := byDate["20260730"]
	if got, want := d.shortVolume, 29.42*69*100; got != want {
		t.Fatalf("short volume: got %v, want %v", got, want)
	}
	if got, want := d.longVolume, 1500.0; got != want {
		t.Fatalf("long volume: got %v, want %v", got, want)
	}
	if got, want := d.volume, 29.42*69*100+1500; got != want {
		t.Fatalf("total volume: got %v, want %v", got, want)
	}
}

// Every Flex query built from our setup guide before 2026-09-16 omits the
// Multiplier field, so the statement of an existing customer carries no
// attribute at all. Waiting for each of them to edit their query would leave
// the figure wrong for as long as they never do.
func TestParseTradesFromReport_OptionWithoutTheAttribute(t *testing.T) {
	report := []byte(`<FlexQueryResponse>
  <FlexStatements>
    <FlexStatement>
      <Trades>
        <Trade tradeID="1" symbol="TESTX 260731C00255000" buySell="SELL"
               tradePrice="2" quantity="-498"
               ibCommission="-269.27" currency="USD" dateTime="20260730;095413"
               assetCategory="OPT" fifoPnlRealized="48692.44" />
        <Trade tradeID="2" symbol="TESTZ" buySell="BUY"
               tradePrice="4200" quantity="3"
               ibCommission="-6" currency="USD" dateTime="20260730;100000"
               assetCategory="FUT" fifoPnlRealized="0" />
      </Trades>
    </FlexStatement>
  </FlexStatements>
</FlexQueryResponse>`)

	i := &IBKR{}
	trades, err := i.parseTradesFromReport(report,
		time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	// 498 contracts of premium at 2, which is 99,600 of cash and not 996.
	if got, want := trades[0].Notional(), 99600.0; got != want {
		t.Fatalf("option without the attribute: got %v, want %v", got, want)
	}

	// A future is anything from 20 to 1000 and there is nothing to read it
	// off, so it keeps the value it had rather than inheriting the option's.
	if trades[1].Multiplier != 0 {
		t.Fatalf("future must not be given a guessed multiplier, got %v", trades[1].Multiplier)
	}
	if got, want := trades[1].Notional(), 12600.0; got != want {
		t.Fatalf("future notional: got %v, want %v", got, want)
	}
}

// A statement that does carry the attribute wins over the default, which is
// what an option adjusted by a corporate action depends on.
func TestParseTradesFromReport_AdjustedOptionKeepsItsOwnMultiplier(t *testing.T) {
	report := []byte(`<FlexQueryResponse>
  <FlexStatements>
    <FlexStatement>
      <Trades>
        <Trade tradeID="1" symbol="TESTX1 260731C00100000" buySell="BUY"
               tradePrice="3" quantity="10" multiplier="110"
               ibCommission="-1" currency="USD" dateTime="20260730;095413"
               assetCategory="OPT" fifoPnlRealized="0" />
      </Trades>
    </FlexStatement>
  </FlexStatements>
</FlexQueryResponse>`)

	i := &IBKR{}
	trades, err := i.parseTradesFromReport(report,
		time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got, want := trades[0].Notional(), 3300.0; got != want {
		t.Fatalf("adjusted option: got %v, want %v", got, want)
	}
}
