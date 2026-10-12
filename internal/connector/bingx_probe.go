package connector

import (
	"context"
	"encoding/json"
	"strconv"
)

// bingxVSTAPI serves the demo perpetual account (VST), reachable with the same
// key as the live account.
const bingxVSTAPI = "https://open-api-vst.bingx.com"

// bingxProbeWallets are the BingX accounts a key can hold money in. GetBalance
// reads only the first two, so money parked anywhere else syncs as 0. An empty
// host means the connector's own API.
var bingxProbeWallets = []struct{ name, host, path string }{
	{"swap_v2", "", "/openApi/swap/v2/user/balance"},
	{"spot", "", "/openApi/spot/v1/account/balance"},
	{"swap_v3", "", "/openApi/swap/v3/user/balance"},
	{"overview", "", "/openApi/account/v1/allAccountBalance"},
	{"fund", "", "/openApi/fund/v1/account/balance"},
	{"standard_futures", "", "/openApi/contract/v1/balance"},
	{"coin_m", "", "/openApi/cswap/v1/user/balance"},
	{"api_permissions", "", "/openApi/v1/account/apiPermissions"},
	{"demo_swap_v2", bingxVSTAPI, "/openApi/swap/v2/user/balance"},
	{"demo_positions", bingxVSTAPI, "/openApi/swap/v2/user/positions"},
	{"demo_income", bingxVSTAPI, "/openApi/swap/v2/user/income"},
}

// bingxProbeDataCap bounds one wallet's payload: a spot account lists every
// asset it ever touched, zero or not.
const bingxProbeDataCap = 8000

// ProbeBalance returns every BingX wallet payload verbatim, with the venue's
// code and message, beside the Balance GetBalance derives. An unreachable
// wallet becomes a note, never a zero.
func (b *BingX) ProbeBalance(ctx context.Context) (*BalanceProbe, error) {
	probe := &BalanceProbe{
		Exchange:    b.Exchange(),
		MarginBasis: "swap/v2 balance.equity + spot balances valued at spot tickers",
		Account:     map[string]string{},
		Currencies:  []map[string]string{},
	}

	for _, w := range bingxProbeWallets {
		base := b.base
		if w.host != "" && b.base.BaseURL == bingxAPI {
			base.BaseURL = w.host
		}
		body, err := base.signedQueryGET(ctx, "X-BX-APIKEY", w.path, "")
		if err != nil {
			probe.Notes = append(probe.Notes, w.name+" unavailable: "+vendorErrorDetail(err.Error()))
			continue
		}
		var env struct {
			Code json.Number     `json:"code"`
			Msg  string          `json:"msg"`
			Data json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal(body, &env); err != nil {
			probe.Notes = append(probe.Notes, w.name+" unreadable")
			continue
		}
		probe.Account[w.name+".code"] = env.Code.String()
		if env.Msg != "" {
			probe.Account[w.name+".msg"] = env.Msg
		}
		data := string(env.Data)
		if len(data) > bingxProbeDataCap {
			data = data[:bingxProbeDataCap] + "…(truncated, " + strconv.Itoa(len(env.Data)) + " bytes)"
		}
		probe.Currencies = append(probe.Currencies, map[string]string{
			"wallet": w.name,
			"host":   w.host,
			"path":   w.path,
			"data":   data,
		})
	}

	derived, err := b.GetBalance(ctx)
	if err != nil {
		probe.Notes = append(probe.Notes, "balance unavailable: "+vendorErrorDetail(err.Error()))
	} else {
		probe.Derived = derived
	}
	return probe, nil
}
