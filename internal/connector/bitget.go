package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const bitgetAPI = "https://api.bitget.com"

// Bitget implements Connector for Bitget exchange using native HTTP.
// Uses HMAC-SHA256 + passphrase signing (similar to OKX).
type Bitget struct {
	base       CryptoBase
	passphrase string
}

// NewBitget creates a new Bitget connector.
func NewBitget(creds *Credentials) *Bitget {
	return &Bitget{
		base:       NewCryptoBase(creds.APIKey, creds.APISecret, bitgetAPI),
		passphrase: creds.Passphrase,
	}
}

func (b *Bitget) Exchange() string { return "bitget" }

func (b *Bitget) sign(timestamp, method, path, body string) string {
	return signHMACBase64(b.base.APISecret, timestamp+method+path+body)
}

func (b *Bitget) doRequest(ctx context.Context, method, path string) ([]byte, error) {
	body, err := retryHTTP(b.base.Client, func() (*http.Request, error) {
		timestamp := strconv.FormatInt(time.Now().UnixMilli(), 10)
		signature := b.sign(timestamp, method, path, "")

		req, err := http.NewRequestWithContext(ctx, method, b.base.BaseURL+path, nil)
		if err != nil {
			return nil, err
		}

		req.Header.Set("ACCESS-KEY", b.base.APIKey)
		req.Header.Set("ACCESS-SIGN", signature)
		req.Header.Set("ACCESS-TIMESTAMP", timestamp)
		req.Header.Set("ACCESS-PASSPHRASE", b.passphrase)
		req.Header.Set("Content-Type", "application/json")
		return req, nil
	})
	if err != nil {
		return nil, err
	}

	var result struct {
		Code string `json:"code"`
		Msg  string `json:"msg"`
	}
	json.Unmarshal(body, &result)
	if result.Code != "00000" {
		return nil, fmt.Errorf("bitget API error: %s (code %s)", vendorErrorDetail(result.Msg), result.Code)
	}

	return body, nil
}

func (b *Bitget) TestConnection(ctx context.Context) error {
	_, err := b.doRequest(ctx, "GET", "/api/v2/spot/account/assets")
	return err
}

func (b *Bitget) GetBalance(ctx context.Context) (*Balance, error) {
	// Spot balance
	spotBody, err := b.doRequest(ctx, "GET", "/api/v2/spot/account/assets")
	if err != nil {
		return nil, fmt.Errorf("spot balance: %w", err)
	}

	var spotResp struct {
		Data []struct {
			Coin      string `json:"coin"`
			Available string `json:"available"`
			Frozen    string `json:"frozen"`
		} `json:"data"`
	}
	if err := json.Unmarshal(spotBody, &spotResp); err != nil {
		return nil, fmt.Errorf("parse spot balance: %w", err)
	}

	// Value EVERY spot holding, not just stablecoins (CONN-VALUE-001 — the
	// stablecoin-only sum reported ~0 equity for crypto holders and diverged
	// from the rebuilder's reconstructed history, which prices all coins).
	var holdings []SpotHolding
	spotAvailable := 0.0
	hasNonStable := false
	for _, a := range spotResp.Data {
		avail, _ := strconv.ParseFloat(a.Available, 64)
		frozen, _ := strconv.ParseFloat(a.Frozen, 64)
		total := avail + frozen
		if total <= 0 {
			continue
		}
		holdings = append(holdings, SpotHolding{Asset: a.Coin, Amount: total})
		if IsStablecoinUSD(a.Coin) {
			spotAvailable += avail
		} else {
			hasNonStable = true
		}
	}

	// One public all-tickers call prices every non-stable coin; skipped for
	// pure-stable accounts, and a fetch failure degrades to stables-only
	// rather than failing the sync.
	priceMap := map[string]float64{}
	if hasNonStable {
		if pm, perr := b.fetchPriceMap(ctx); perr == nil {
			priceMap = pm
		}
	}
	spotEquity := ValueSpotHoldingsUSD(holdings, priceMap)

	// Futures balance over both stable-margined products (ignore errors —
	// account may not have futures enabled).
	futuresEquity := 0.0
	futuresUnrealized := 0.0
	for _, pt := range bitgetMixProductTypes {
		futBody, ferr := b.doRequest(ctx, "GET", "/api/v2/mix/account/accounts?productType="+pt)
		if ferr != nil {
			continue
		}
		var futResp struct {
			Data []struct {
				MarginCoin    string `json:"marginCoin"`
				AccountEquity string `json:"accountEquity"`
				UnrealizedPL  string `json:"unrealizedPL"`
				Available     string `json:"available"`
			} `json:"data"`
		}
		if json.Unmarshal(futBody, &futResp) != nil {
			continue
		}
		for _, a := range futResp.Data {
			if IsStablecoinUSD(a.MarginCoin) {
				eq, _ := strconv.ParseFloat(a.AccountEquity, 64)
				upl, _ := strconv.ParseFloat(a.UnrealizedPL, 64)
				futuresEquity += eq
				futuresUnrealized += upl
			}
		}
	}

	return &Balance{
		Equity:        spotEquity + futuresEquity,
		Available:     spotAvailable,
		UnrealizedPnL: futuresUnrealized,
		Currency:      "USDT",
	}, nil
}

func (b *Bitget) GetPositions(ctx context.Context) ([]*Position, error) {
	body, err := b.doRequest(ctx, "GET", "/api/v2/mix/position/all-position?productType=USDT-FUTURES")
	if err != nil {
		return nil, err
	}

	var resp struct {
		Data []struct {
			Symbol       string `json:"symbol"`
			HoldSide     string `json:"holdSide"` // "long" or "short"
			Total        string `json:"total"`
			OpenPriceAvg string `json:"openPriceAvg"`
			MarkPrice    string `json:"markPrice"`
			UnrealizedPL string `json:"unrealizedPL"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}

	positions := make([]*Position, 0, len(resp.Data))
	for _, p := range resp.Data {
		size, _ := strconv.ParseFloat(p.Total, 64)
		if size == 0 {
			continue
		}
		entry, _ := strconv.ParseFloat(p.OpenPriceAvg, 64)
		mark, _ := strconv.ParseFloat(p.MarkPrice, 64)
		upl, _ := strconv.ParseFloat(p.UnrealizedPL, 64)

		positions = append(positions, &Position{
			Symbol:        p.Symbol,
			Side:          p.HoldSide,
			Size:          size,
			EntryPrice:    entry,
			MarkPrice:     mark,
			UnrealizedPnL: upl,
			MarketType:    MarketSwap,
		})
	}

	return positions, nil
}

func (b *Bitget) GetTrades(ctx context.Context, start, end time.Time) ([]*Trade, error) {
	path := fmt.Sprintf("/api/v2/spot/trade/fills?startTime=%d&endTime=%d&limit=100",
		start.UnixMilli(), end.UnixMilli())

	body, err := b.doRequest(ctx, "GET", path)
	if err != nil {
		return nil, err
	}

	var resp struct {
		Data []struct {
			TradeID   string `json:"tradeId"`
			Symbol    string `json:"symbol"`
			Side      string `json:"side"`
			Price     string `json:"priceAvg"`
			Size      string `json:"size"`
			Fee       string `json:"fees"`
			FeeCcy    string `json:"feeCurrency"`
			Timestamp string `json:"cTime"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}

	trades := make([]*Trade, 0, len(resp.Data))
	for _, t := range resp.Data {
		price, _ := strconv.ParseFloat(t.Price, 64)
		qty, _ := strconv.ParseFloat(t.Size, 64)
		fee, _ := strconv.ParseFloat(t.Fee, 64)
		ts, _ := strconv.ParseInt(t.Timestamp, 10, 64)

		trades = append(trades, &Trade{
			ID:          t.TradeID,
			Symbol:      t.Symbol,
			Side:        strings.ToLower(t.Side),
			Price:       price,
			Quantity:    qty,
			Fee:         fee,
			FeeCurrency: t.FeeCcy,
			Timestamp:   time.UnixMilli(ts),
			MarketType:  MarketSpot,
		})
	}

	return trades, nil
}

// ---- Cashflows (perimeter model, mirrors the external rebuilder) ----------
//
// The tracked perimeter is spot + stable-margined futures — exactly what
// GetBalance reports. A cashflow is money CROSSING that boundary: on-chain
// deposits/withdrawals, Earn/Savings flows ("financial"), platform grants,
// copy-trading/strategy and margin/OTC transfers, and any spot↔futures
// transfer leg whose counterpart inside the perimeter is missing. Matched
// spot↔futures pairs cancel inside the combined equity and are NOT cashflow.
//
// Without this, every deposit lands as a fabricated return in the daily TWR
// (the account's equity jumps with deposits=0). Field-validated against the
// history-rebuilder's identical classification (bitget rebuild, 2026-07-08).

// bitgetMixProductTypes are the stable-margined futures products in the
// perimeter. COIN-FUTURES is excluded, matching GetBalance.
var bitgetMixProductTypes = []string{"USDT-FUTURES", "USDC-FUTURES"}

// bitgetFutExternalTransfers are mix-bill businessTypes whose counterpart
// wallet is outside the perimeter. trans_from/to_exchange (spot side tracked)
// and trans_from/to_contract (both products tracked) are handled by pairing.
// "risk_captital" is Bitget's actual spelling (observed in prod bills); the
// documented spelling is kept alongside.
var bitgetFutExternalTransfers = map[string]bool{
	"trans_from_cross": true, "trans_to_cross": true,
	"trans_from_isolated": true, "trans_to_isolated": true,
	"trans_from_otc": true, "trans_to_otc": true,
	"trans_from_strategy": true, "trans_to_strategy": true,
	"bonus_issue": true, "bonus_recycle": true, "bonus_expired": true,
	"cash_gift_issue": true, "cash_gift_recycle": true,
	"user_grants_issue": true, "user_grants_recycle": true,
	"risk_capital_user_transfer": true, "risk_captital_user_transfer": true,
}

const (
	// bitgetTransferMatchWindow is how far apart the two legs of one
	// spot↔futures transfer may be booked and still pair up.
	bitgetTransferMatchWindow = 10 * time.Minute
	// bitgetMaxBillPages bounds idLessThan pagination per ledger — the daily
	// sync window is 24h, so a handful of pages is already generous.
	bitgetMaxBillPages = 40
)

type bitgetSpotBill struct {
	t         time.Time
	coin      string
	size      float64 // signed coin delta
	groupType string
}

type bitgetMixBill struct {
	t            time.Time
	delta        float64 // signed margin-coin (USD) delta
	businessType string
}

// GetCashflows classifies the ledger entries in [since, now] into external
// deposits/withdrawals. Amounts are SIGNED and valued in USD (stables 1:1,
// other coins at the current public ticker) because the sync layer sums
// cf.Amount directly into the snapshot's deposits/withdrawals.
func (b *Bitget) GetCashflows(ctx context.Context, since time.Time) ([]*Cashflow, error) {
	now := time.Now().UTC()

	spotBills, err := b.fetchSpotBills(ctx, since, now)
	if err != nil {
		return nil, fmt.Errorf("spot bills: %w", err)
	}
	mixBills := b.fetchMixBills(ctx, since, now) // best-effort per product

	// Price map only when a non-stable coin shows up in the window.
	priceMap := map[string]float64{}
	for _, sb := range spotBills {
		if !IsStablecoinUSD(sb.coin) {
			if pm, perr := b.fetchPriceMap(ctx); perr == nil {
				priceMap = pm
			}
			break
		}
	}

	return classifyBitgetCashflows(spotBills, mixBills, priceMap), nil
}

// fetchSpotBills pages the whole spot ledger over [since, now].
func (b *Bitget) fetchSpotBills(ctx context.Context, since, now time.Time) ([]bitgetSpotBill, error) {
	var bills []bitgetSpotBill
	idLessThan := ""
	for page := 0; page < bitgetMaxBillPages; page++ {
		path := fmt.Sprintf("/api/v2/spot/account/bills?startTime=%d&endTime=%d&limit=500",
			since.UnixMilli(), now.UnixMilli())
		if idLessThan != "" {
			path += "&idLessThan=" + idLessThan
		}
		body, err := b.doRequest(ctx, "GET", path)
		if err != nil {
			return nil, err
		}
		var resp struct {
			Data []struct {
				BillID    string `json:"billId"`
				CTime     string `json:"cTime"`
				Coin      string `json:"coin"`
				GroupType string `json:"groupType"`
				Size      string `json:"size"`
			} `json:"data"`
		}
		if err := json.Unmarshal(body, &resp); err != nil {
			return nil, fmt.Errorf("parse spot bills: %w", err)
		}
		lastID := ""
		for _, r := range resp.Data {
			ms, _ := strconv.ParseInt(r.CTime, 10, 64)
			size, _ := strconv.ParseFloat(r.Size, 64)
			bills = append(bills, bitgetSpotBill{
				t:         time.UnixMilli(ms).UTC(),
				coin:      strings.ToUpper(r.Coin),
				size:      size,
				groupType: strings.ToLower(r.GroupType),
			})
			lastID = r.BillID
		}
		if len(resp.Data) < 500 || lastID == "" || lastID == idLessThan {
			break
		}
		idLessThan = lastID
	}
	return bills, nil
}

// fetchMixBills pages the futures ledgers of both stable-margined products.
// Best-effort: a product the account never enabled just contributes nothing.
func (b *Bitget) fetchMixBills(ctx context.Context, since, now time.Time) []bitgetMixBill {
	var bills []bitgetMixBill
	for _, pt := range bitgetMixProductTypes {
		idLessThan := ""
		for page := 0; page < bitgetMaxBillPages; page++ {
			path := fmt.Sprintf("/api/v2/mix/account/bill?productType=%s&startTime=%d&endTime=%d&limit=100",
				pt, since.UnixMilli(), now.UnixMilli())
			if idLessThan != "" {
				path += "&idLessThan=" + idLessThan
			}
			body, err := b.doRequest(ctx, "GET", path)
			if err != nil {
				break
			}
			var resp struct {
				Data struct {
					Bills []struct {
						CTime        string `json:"cTime"`
						Amount       string `json:"amount"`
						BusinessType string `json:"businessType"`
					} `json:"bills"`
					EndID string `json:"endId"`
				} `json:"data"`
			}
			if json.Unmarshal(body, &resp) != nil {
				break
			}
			for _, r := range resp.Data.Bills {
				ms, _ := strconv.ParseInt(r.CTime, 10, 64)
				amount, _ := strconv.ParseFloat(r.Amount, 64)
				bills = append(bills, bitgetMixBill{
					t:            time.UnixMilli(ms).UTC(),
					delta:        amount,
					businessType: strings.ToLower(r.BusinessType),
				})
			}
			if len(resp.Data.Bills) < 100 || resp.Data.EndID == "" || resp.Data.EndID == idLessThan {
				break
			}
			idLessThan = resp.Data.EndID
		}
	}
	return bills
}

// fetchPriceMap loads every spot pair's last price in one public call.
func (b *Bitget) fetchPriceMap(ctx context.Context) (map[string]float64, error) {
	body, err := retryHTTP(b.base.Client, func() (*http.Request, error) {
		return http.NewRequestWithContext(ctx, "GET", b.base.BaseURL+"/api/v2/spot/market/tickers", nil)
	})
	if err != nil {
		return nil, err
	}
	var resp struct {
		Data []struct {
			Symbol string `json:"symbol"`
			LastPr string `json:"lastPr"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}
	prices := make(map[string]float64, len(resp.Data))
	for _, t := range resp.Data {
		if p, perr := strconv.ParseFloat(t.LastPr, 64); perr == nil && p > 0 {
			prices[strings.ToUpper(t.Symbol)] = p
		}
	}
	return prices, nil
}

// pairBitgetTransfers matches spot "transfer" legs against futures
// trans_from/to_exchange legs (opposite-signed amount within the match
// window). Unmatched legs crossed the perimeter — e.g. funding→spot→futures
// books two spot legs but only one futures counterpart, and the unmatched
// spot leg is the real external deposit.
func pairBitgetTransfers(spotBills []bitgetSpotBill, mixBills []bitgetMixBill) (map[int]bool, map[int]bool) {
	spotMatched := map[int]bool{}
	futMatched := map[int]bool{}
	for fi, fb := range mixBills {
		if fb.businessType != "trans_from_exchange" && fb.businessType != "trans_to_exchange" {
			continue
		}
		for si, sb := range spotBills {
			if spotMatched[si] || sb.groupType != "transfer" || !IsStablecoinUSD(sb.coin) {
				continue
			}
			if math.Abs(sb.size+fb.delta) > 0.01 {
				continue
			}
			dt := sb.t.Sub(fb.t)
			if dt < -bitgetTransferMatchWindow || dt > bitgetTransferMatchWindow {
				continue
			}
			spotMatched[si] = true
			futMatched[fi] = true
			break
		}
	}
	return spotMatched, futMatched
}

// classifyBitgetCashflows turns the raw ledgers into signed USD cashflows.
// Pure (no IO) — the ticker map is injected — so the perimeter rules stay
// unit-testable.
func classifyBitgetCashflows(spotBills []bitgetSpotBill, mixBills []bitgetMixBill, priceMap map[string]float64) []*Cashflow {
	spotMatched, futMatched := pairBitgetTransfers(spotBills, mixBills)

	var flows []*Cashflow
	add := func(t time.Time, usd float64) {
		if usd != 0 {
			flows = append(flows, &Cashflow{Amount: usd, Currency: "USDT", Timestamp: t})
		}
	}
	usdValue := func(coin string, qty float64) float64 {
		if IsStablecoinUSD(coin) {
			return qty
		}
		if p := priceMap[strings.ToUpper(coin)+"USDT"]; p > 0 {
			return qty * p
		}
		return 0 // unpriceable dust — never fabricate a flow from it
	}

	for si, sb := range spotBills {
		switch sb.groupType {
		case "deposit", "on_chain", "financial":
			// Signed size carries the direction (financial: Earn subscribe is
			// an outflow, redeem/interest an inflow).
			add(sb.t, usdValue(sb.coin, sb.size))
		case "withdraw":
			add(sb.t, -usdValue(sb.coin, math.Abs(sb.size)))
		case "transfer":
			if !spotMatched[si] {
				add(sb.t, usdValue(sb.coin, sb.size))
			}
		}
	}
	for fi, fb := range mixBills {
		external := bitgetFutExternalTransfers[fb.businessType]
		if !external {
			isExchangeTransfer := fb.businessType == "trans_from_exchange" || fb.businessType == "trans_to_exchange"
			external = isExchangeTransfer && !futMatched[fi]
		}
		if external {
			add(fb.t, fb.delta) // stable-margined: already USD
		}
	}
	return flows
}
