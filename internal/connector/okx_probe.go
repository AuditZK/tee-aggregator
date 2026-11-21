package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
)

// okxAcctLvNames maps GET /api/v5/account/config's acctLv onto the mode names
// the v5 docs use in the "distribution of applicable fields" table.
var okxAcctLvNames = map[string]string{
	"1": "spot mode",
	"2": "futures mode (single-currency margin)",
	"3": "multi-currency margin mode",
	"4": "portfolio margin mode",
}

// accountLevel returns the account's acctLv, fetched at most once per
// connector and cached. It is deliberately NOT on the balance path: OKX blanks
// every field that does not apply to the current mode, so GetBalance reads the
// mode off the payload it already has and spends no extra request on it (see
// the mapping table above okxReadMargin). The probe asks anyway, so an
// operator can compare the mode the venue reports against the one this code
// inferred instead of taking the inference on trust.
//
// Best effort: an account whose key cannot read the config endpoint still gets
// a probe, with a note saying why the mode is missing.
func (o *OKX) accountLevel(ctx context.Context) (string, error) {
	o.mu.Lock()
	if o.acctLvFetched {
		lv := o.acctLv
		o.mu.Unlock()
		return lv, nil
	}
	o.mu.Unlock()

	body, err := o.doRequest(ctx, "GET", "/api/v5/account/config")
	if err != nil {
		return "", err
	}

	var resp struct {
		Data []struct {
			AcctLv string `json:"acctLv"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return "", err
	}
	var lv string
	if len(resp.Data) > 0 {
		lv = resp.Data[0].AcctLv
	}

	o.mu.Lock()
	o.acctLv, o.acctLvFetched = lv, true
	o.mu.Unlock()
	return lv, nil
}

// ProbeBalance returns the raw balance payload next to the Balance GetBalance
// derives from it. Same credentials, same endpoint, same decoding as a sync —
// the point is that the two sides of the answer come from one round trip, so
// a disagreement between the venue's figures and the dashboard's cannot be
// blamed on them having looked at different moments.
func (o *OKX) ProbeBalance(ctx context.Context) (*BalanceProbe, error) {
	account, err := o.fetchAccountBalance(ctx)
	if err != nil {
		return nil, err
	}

	probe := &BalanceProbe{
		Exchange:   o.Exchange(),
		Account:    map[string]string{},
		Currencies: []map[string]string{},
		Derived:    &Balance{Currency: "USDT"},
	}

	if acctLv, err := o.accountLevel(ctx); err != nil {
		probe.Notes = append(probe.Notes, "account mode unavailable: "+vendorErrorDetail(err.Error()))
	} else if acctLv != "" {
		name := okxAcctLvNames[acctLv]
		if name == "" {
			name = "unknown"
		}
		probe.AccountMode = fmt.Sprintf("acctLv %s (%s)", acctLv, name)
		probe.Account["acctLv"] = acctLv
	}

	if account == nil {
		probe.Notes = append(probe.Notes, "balance response carried no account entry")
		return probe, nil
	}

	// Verbatim, in the venue's own field names. An empty value here is the
	// venue saying "not applicable in this account mode" and is as much of an
	// answer as a number would be, so the keys are always present.
	probe.Account["totalEq"] = account.TotalEq
	probe.Account["isoEq"] = account.IsoEq
	probe.Account["adjEq"] = account.AdjEq
	probe.Account["availEq"] = account.AvailEq
	probe.Account["ordFroz"] = account.OrdFroz
	probe.Account["imr"] = account.IMR
	probe.Account["mmr"] = account.MMR
	probe.Account["mgnRatio"] = account.MgnRatio
	probe.Account["upl"] = account.UPL
	probe.Account["uTime"] = account.UTime

	for _, d := range account.Details {
		probe.Currencies = append(probe.Currencies, map[string]string{
			"ccy":       d.Ccy,
			"eq":        d.Eq,
			"eqUsd":     d.EqUsd,
			"cashBal":   d.CashBal,
			"availBal":  d.AvailBal,
			"availEq":   d.AvailEq,
			"frozenBal": d.FrozenBal,
			"isoEq":     d.IsoEq,
			"upl":       d.UPL,
			"imr":       d.IMR,
			"mmr":       d.MMR,
		})
	}

	reading := okxReadMargin(*account)
	equity, _ := strconv.ParseFloat(account.TotalEq, 64)
	probe.MarginBasis = reading.Basis
	probe.Derived = &Balance{
		Available:     reading.Available,
		Equity:        equity,
		UnrealizedPnL: reading.Unrealized,
		Currency:      "USDT",
	}
	if probe.AccountMode == "" {
		probe.AccountMode = "inferred: " + reading.Mode
	} else {
		probe.Notes = append(probe.Notes, "inferred from field presence: "+reading.Mode)
	}

	return probe, nil
}
