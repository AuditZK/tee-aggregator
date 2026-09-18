package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
)

// ProbeBalance returns Alpaca's own account payload next to the Balance
// GetBalance derives from it, plus the open positions the unrealized figure is
// summed from. Alpaca publishes no aggregate unrealized field, so that sum is
// the one number a reader cannot check against /v2/account alone, and a flat
// equity has two very different explanations: an account holding nothing, or a
// sync that stopped asking.
func (a *Alpaca) ProbeBalance(ctx context.Context) (*BalanceProbe, error) {
	body, err := a.doRequest(ctx, a.baseURL, "/v2/account")
	if err != nil {
		return nil, err
	}

	var raw map[string]any
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("parse alpaca account: %w", err)
	}

	probe := &BalanceProbe{
		Exchange:    a.Exchange(),
		MarginBasis: "account.cash (available), account.equity (equity), sum(positions.unrealized_pl)",
		Account:     map[string]string{},
		Currencies:  []map[string]string{},
	}

	// Verbatim and as strings: Alpaca answers "" for a field that does not
	// apply to the account, and that is an answer, not a zero.
	for _, k := range []string{
		"account_number", "status", "currency", "cash", "equity", "last_equity",
		"portfolio_value", "buying_power", "long_market_value", "short_market_value",
		"position_market_value", "accrued_fees", "pattern_day_trader", "trading_blocked",
		"account_blocked", "trade_suspended_by_user", "created_at",
	} {
		if v, ok := raw[k]; ok {
			probe.Account[k] = fmt.Sprint(v)
		}
	}
	if s, ok := probe.Account["status"]; ok {
		probe.AccountMode = "account status " + s
	}

	equity, _ := strconv.ParseFloat(probe.Account["equity"], 64)
	cash, _ := strconv.ParseFloat(probe.Account["cash"], 64)

	positions, posErr := a.GetPositions(ctx)
	if posErr != nil {
		probe.Notes = append(probe.Notes, "positions unavailable: "+vendorErrorDetail(posErr.Error()))
	}
	unrealized := 0.0
	for _, p := range positions {
		unrealized += p.UnrealizedPnL
		probe.Currencies = append(probe.Currencies, map[string]string{
			"symbol":        p.Symbol,
			"size":          strconv.FormatFloat(p.Size, 'f', -1, 64),
			"entry_price":   strconv.FormatFloat(p.EntryPrice, 'f', -1, 64),
			"mark_price":    strconv.FormatFloat(p.MarkPrice, 'f', -1, 64),
			"unrealized_pl": strconv.FormatFloat(p.UnrealizedPnL, 'f', -1, 64),
			"market_type":   p.MarketType,
		})
	}
	if posErr == nil && len(positions) == 0 {
		probe.Notes = append(probe.Notes, "no open position: the account is entirely in cash")
	}

	probe.Derived = &Balance{
		Available:     cash,
		Equity:        equity,
		UnrealizedPnL: unrealized,
		Currency:      "USD",
	}
	return probe, nil
}
