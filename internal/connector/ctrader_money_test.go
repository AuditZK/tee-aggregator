package connector

import (
	"encoding/json"
	"testing"
)

// realRoundTripJSON is the captured 1-lot EURUSD round trip the history tests
// also use: the account went from 1008.89 to 996.42 across it.
const realRoundTripJSON = `{"deal":[{"dealId":320460360,"positionId":264207985,"volume":10000000,"filledVolume":10000000,"symbolId":1,"executionTimestamp":1780689978964,"executionPrice":1.15225,"tradeSide":2,"dealStatus":2,"commission":-450,"closePositionDetail":{"grossProfit":-347,"swap":0,"commission":-900,"balance":99642,"moneyDigits":2},"moneyDigits":2},{"dealId":320455222,"positionId":264207985,"volume":10000000,"filledVolume":10000000,"symbolId":1,"executionTimestamp":1780688563637,"executionPrice":1.15229,"tradeSide":1,"dealStatus":2,"commission":-450,"moneyDigits":2}]}`

func realRoundTrip(t *testing.T) (closing, opening cTraderDeal) {
	t.Helper()
	var resp struct {
		Deal []cTraderDeal `json:"deal"`
	}
	if err := json.Unmarshal([]byte(realRoundTripJSON), &resp); err != nil {
		t.Fatalf("unmarshal deals: %v", err)
	}
	return resp.Deal[0], resp.Deal[1]
}

// Gross profit, swap and commission arrive signed from the account's side. The
// realized PnL used to subtract the (already negative) costs and reported this
// losing round trip as a gain.
func TestCTraderDeal_RealizedPnLIsTheBalanceMove(t *testing.T) {
	closing, opening := realRoundTrip(t)

	if want := 996.42 - 1008.89; !floatNear(closing.realizedPnL(), want, 1e-9) {
		t.Fatalf("realized PnL = %v, want %v: the move of the closing balance", closing.realizedPnL(), want)
	}
	if opening.realizedPnL() != 0 {
		t.Fatalf("an opening deal realizes nothing, got %v", opening.realizedPnL())
	}
}

// Every other venue reports the fee paid as a positive figure and
// trading_fees sums them; cTrader's negative commission subtracted from that
// total instead.
func TestCTraderDeal_FeeIsThePositiveCost(t *testing.T) {
	closing, opening := realRoundTrip(t)

	if got := closing.fee() + opening.fee(); !floatNear(got, 9, 1e-9) {
		t.Fatalf("fees = %v, want 9 paid over the round trip", got)
	}
}

// Summing Price*Quantity across a CFD book added quote currencies together: a
// yen pair's notional counted at the yen price.
func TestCTraderDeal_NotionalIsInDollarsWhenTheDealSaysHow(t *testing.T) {
	// Synthetic 1-lot USDJPY: base USD, so a base-to-USD rate of 1.
	usdjpy := cTraderDeal{FilledVolume: 10000000, ExecutionPrice: 147.5, BaseToUsdConversionRate: 1}
	if got := usdjpy.notional(); !floatNear(got, 100000, 1e-6) {
		t.Fatalf("USDJPY notional = %v, want 100000 dollars", got)
	}
	trade := &Trade{Price: usdjpy.ExecutionPrice, Quantity: usdjpy.units(), USDNotional: usdjpy.usdNotional()}
	if got := trade.Notional(); !floatNear(got, 100000, 1e-6) {
		t.Fatalf("Trade.Notional = %v, want the dollar notional", got)
	}

	// A deal without the rate keeps the quote-currency notional.
	eurusd := cTraderDeal{FilledVolume: 10000000, ExecutionPrice: 1.1}
	if got := eurusd.notional(); !floatNear(got, 110000, 1e-6) {
		t.Fatalf("notional without a rate = %v, want 110000", got)
	}
	if eurusd.usdNotional() != 0 {
		t.Fatal("a deal without a rate must not claim a dollar notional")
	}
}
