package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
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
		otherURL, otherName := alpacaOtherEnvironment(a.baseURL)
		return nil, a.explainAuthFailure(ctx, err, otherURL, otherName)
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

// explainAuthFailure turns Alpaca's answer to a rejected key into something an
// operator can act on. Alpaca replies 401 {"message": "unauthorized."} and
// nothing else, the same body whether the key was revoked, belongs to the
// other environment, or the account is closed.
//
// One of those is worth separating, because the fix differs: the environment
// is chosen from the key's prefix, so a key that no longer matches its prefix
// is sent to the wrong host and refused for a reason unrelated to its
// validity. Asking the other host settles it. The probe does this; a sync
// never does, because reading an environment the holder did not configure is
// a worse failure than a refused read.
func (a *Alpaca) explainAuthFailure(ctx context.Context, err error, otherURL, otherName string) error {
	if !strings.Contains(err.Error(), "401") {
		return err
	}
	if _, otherErr := a.doRequest(ctx, otherURL, "/v2/account"); otherErr == nil {
		return fmt.Errorf("alpaca credentials refused by the %s API and accepted by the %s one: the key belongs to the %s environment and this connection points at the other",
			alpacaEnvironmentName(a.baseURL), otherName, otherName)
	}
	return fmt.Errorf("alpaca credentials refused by both the live and paper APIs: the key is revoked or the account closed, not pointed at the wrong environment")
}

// alpacaOtherEnvironment names the host a key would reach if its prefix had
// been read the other way round.
func alpacaOtherEnvironment(baseURL string) (url, name string) {
	if baseURL == alpacaPaperAPI {
		return alpacaLiveAPI, "live"
	}
	return alpacaPaperAPI, "paper"
}

func alpacaEnvironmentName(baseURL string) string {
	if baseURL == alpacaPaperAPI {
		return "paper"
	}
	return "live"
}
