package connector

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const hyperliquidAPI = "https://api.hyperliquid.xyz"

// Hyperliquid is a read-only DEX connector that uses wallet address only.
type Hyperliquid struct {
	walletAddress string
	client        *http.Client
}

// NewHyperliquid creates a new Hyperliquid connector.
func NewHyperliquid(creds *Credentials) *Hyperliquid {
	addr := creds.WalletAddress
	if addr == "" {
		addr = creds.APIKey // Fallback: wallet address stored in APIKey field
	}
	return &Hyperliquid{
		walletAddress: addr,
		client:        &http.Client{Timeout: 30 * time.Second},
	}
}

func (h *Hyperliquid) Exchange() string { return "hyperliquid" }

// DetectIsPaper mirrors TS behavior: Hyperliquid connector targets mainnet.
func (h *Hyperliquid) DetectIsPaper(_ context.Context) (bool, error) {
	return false, nil
}

func (h *Hyperliquid) TestConnection(ctx context.Context) error {
	_, err := h.GetBalance(ctx)
	return err
}

func (h *Hyperliquid) GetBalance(ctx context.Context) (*Balance, error) {
	// Perps clearinghouse state
	resp, err := h.postInfo(ctx, map[string]interface{}{
		"type": "clearinghouseState",
		"user": h.walletAddress,
	})
	if err != nil {
		return nil, fmt.Errorf("get clearinghouse state: %w", err)
	}

	var state struct {
		MarginSummary struct {
			AccountValue    string `json:"accountValue"`
			TotalMarginUsed string `json:"totalMarginUsed"`
		} `json:"marginSummary"`
		AssetPositions []struct {
			Position struct {
				UnrealizedPnl string `json:"unrealizedPnl"`
			} `json:"position"`
		} `json:"assetPositions"`
	}
	if err := json.Unmarshal(resp, &state); err != nil {
		return nil, fmt.Errorf("parse clearinghouse state: %w", err)
	}

	equity, _ := strconv.ParseFloat(state.MarginSummary.AccountValue, 64)
	// HL exposes the actual margin tied up by open positions directly. Use it
	// to derive the live free margin (= equity − margin_used) — what the user
	// can actually deploy into new positions. This is the same number the HL
	// frontend shows as "Available". Previously this was conflated with
	// realized cash (equity − unrealizedPnL), which is a different concept.
	totalMarginUsed, _ := strconv.ParseFloat(state.MarginSummary.TotalMarginUsed, 64)

	// Sum unrealized PnL from open positions (used to derive realized cash
	// in the snapshot pipeline; not the same as free margin).
	var unrealizedPnL float64
	for _, ap := range state.AssetPositions {
		v, _ := strconv.ParseFloat(ap.Position.UnrealizedPnl, 64)
		unrealizedPnL += v
	}

	// Also check spot balances.
	//
	// Post the 2026-05-23 HL network upgrade, USDC that collateralizes perp
	// positions is exposed on BOTH endpoints: it sits in
	// spotClearinghouseState.balances[].total AND is already counted inside
	// clearinghouseState.marginSummary.accountValue. Adding spot.total raw
	// double-counts the collateral, inflating reported equity by exactly
	// totalMarginUsed (an idle account showed a phantom overnight gain).
	//
	// Fix: add only the spot portion NOT reserved as perp margin
	// (= total − hold). For pure-spot users (no perp positions, hold=0)
	// this is identical to the prior behavior.
	// A spot fetch/parse failure now FAILS the sync instead of silently
	// reporting perps-only equity — the same class of understatement CONN-12
	// documents, just triggered by a network blip instead of a filter.
	spotResp, err := h.postInfo(ctx, map[string]interface{}{
		"type": "spotClearinghouseState",
		"user": h.walletAddress,
	})
	if err != nil {
		return nil, fmt.Errorf("get spot clearinghouse state: %w", err)
	}
	var spotState struct {
		Balances []hlSpotBalance `json:"balances"`
	}
	if err := json.Unmarshal(spotResp, &spotState); err != nil {
		return nil, fmt.Errorf("parse spot clearinghouse state: %w", err)
	}
	spotVal, err := h.valueSpotBalancesUSD(ctx, spotState.Balances)
	if err != nil {
		return nil, err
	}
	equity += spotVal

	return &Balance{
		Equity:        equity,
		Available:     equity - totalMarginUsed,
		UnrealizedPnL: unrealizedPnL,
		Currency:      "USD",
	}, nil
}

func (h *Hyperliquid) GetPositions(ctx context.Context) ([]*Position, error) {
	resp, err := h.postInfo(ctx, map[string]interface{}{
		"type": "clearinghouseState",
		"user": h.walletAddress,
	})
	if err != nil {
		return nil, err
	}

	var state struct {
		AssetPositions []struct {
			Position struct {
				Coin          string `json:"coin"`
				Szi           string `json:"szi"`
				EntryPx       string `json:"entryPx"`
				PositionValue string `json:"positionValue"`
				UnrealizedPnl string `json:"unrealizedPnl"`
				LiquidationPx string `json:"liquidationPx"`
				Leverage      struct {
					Value int `json:"value"`
				} `json:"leverage"`
			} `json:"position"`
		} `json:"assetPositions"`
	}
	if err := json.Unmarshal(resp, &state); err != nil {
		return nil, err
	}

	var positions []*Position
	for _, ap := range state.AssetPositions {
		p := ap.Position
		size, _ := strconv.ParseFloat(p.Szi, 64)
		if size == 0 {
			continue
		}

		side := "long"
		if size < 0 {
			side = "short"
			size = -size
		}

		entryPx, _ := strconv.ParseFloat(p.EntryPx, 64)
		pnl, _ := strconv.ParseFloat(p.UnrealizedPnl, 64)

		positions = append(positions, &Position{
			Symbol:        p.Coin + "-PERP",
			Side:          side,
			Size:          size,
			EntryPrice:    entryPx,
			UnrealizedPnL: pnl,
			MarketType:    MarketSwap,
		})
	}

	return positions, nil
}

func (h *Hyperliquid) GetTrades(ctx context.Context, start, end time.Time) ([]*Trade, error) {
	resp, err := h.postInfo(ctx, map[string]interface{}{
		"type":      "userFillsByTime",
		"user":      h.walletAddress,
		"startTime": start.UnixMilli(),
		"endTime":   end.UnixMilli(),
	})
	if err != nil {
		// Fallback to userFills (no time filter)
		resp, err = h.postInfo(ctx, map[string]interface{}{
			"type": "userFills",
			"user": h.walletAddress,
		})
		if err != nil {
			return nil, err
		}
	}

	var fills []struct {
		Coin      string `json:"coin"`
		Px        string `json:"px"`
		Sz        string `json:"sz"`
		Side      string `json:"side"` // "A" (buy) or "B" (sell)
		Time      int64  `json:"time"`
		Fee       string `json:"fee"`
		Tid       int64  `json:"tid"`
		ClosedPnl string `json:"closedPnl"`
	}
	if err := json.Unmarshal(resp, &fills); err != nil {
		return nil, err
	}

	var trades []*Trade
	for _, f := range fills {
		ts := time.UnixMilli(f.Time)
		if ts.Before(start) || ts.After(end) {
			continue
		}

		price, _ := strconv.ParseFloat(f.Px, 64)
		qty, _ := strconv.ParseFloat(f.Sz, 64)
		fee, _ := strconv.ParseFloat(f.Fee, 64)
		pnl, _ := strconv.ParseFloat(f.ClosedPnl, 64)

		side := "buy"
		if f.Side == "B" {
			side = "sell"
		}

		trades = append(trades, &Trade{
			ID:          fmt.Sprintf("%d", f.Tid),
			Symbol:      f.Coin + "-PERP",
			Side:        side,
			Price:       price,
			Quantity:    qty,
			Fee:         fee,
			FeeCurrency: "USDC",
			RealizedPnL: pnl,
			Timestamp:   ts,
			MarketType:  MarketSwap,
		})
	}

	return trades, nil
}

// hlSpotBalance is one spot balance line as spotClearinghouseState returns it.
type hlSpotBalance struct {
	Coin  string `json:"coin"`
	Total string `json:"total"`
	Hold  string `json:"hold"`
}

// fetchSpotPriceMap returns token name → USD price for every spot pair quoted
// in USDC, from a single spotMetaAndAssetCtxs call. The response is a 2-part
// array: [0] spotMeta (tokens with indices, universe of pairs whose `tokens`
// field is [baseIdx, quoteIdx]) and [1] one asset context per universe entry.
// midPx, falling back to markPx for pairs with an empty book.
func (h *Hyperliquid) fetchSpotPriceMap(ctx context.Context) (map[string]float64, error) {
	resp, err := h.postInfo(ctx, map[string]interface{}{"type": "spotMetaAndAssetCtxs"})
	if err != nil {
		return nil, err
	}
	var parts []json.RawMessage
	if err := json.Unmarshal(resp, &parts); err != nil {
		return nil, fmt.Errorf("parse spotMetaAndAssetCtxs: %w", err)
	}
	if len(parts) != 2 {
		return nil, fmt.Errorf("spotMetaAndAssetCtxs: expected 2 parts, got %d", len(parts))
	}
	var meta struct {
		Tokens []struct {
			Name  string `json:"name"`
			Index int    `json:"index"`
		} `json:"tokens"`
		Universe []struct {
			Tokens []int `json:"tokens"`
		} `json:"universe"`
	}
	if err := json.Unmarshal(parts[0], &meta); err != nil {
		return nil, fmt.Errorf("parse spot meta: %w", err)
	}
	var ctxs []struct {
		MidPx  string `json:"midPx"`
		MarkPx string `json:"markPx"`
	}
	if err := json.Unmarshal(parts[1], &ctxs); err != nil {
		return nil, fmt.Errorf("parse spot asset ctxs: %w", err)
	}

	nameByIndex := make(map[int]string, len(meta.Tokens))
	usdcIndex := -1
	for _, t := range meta.Tokens {
		nameByIndex[t.Index] = t.Name
		if t.Name == "USDC" {
			usdcIndex = t.Index
		}
	}
	prices := make(map[string]float64)
	for i, u := range meta.Universe {
		if i >= len(ctxs) || len(u.Tokens) != 2 || u.Tokens[1] != usdcIndex {
			continue
		}
		base := nameByIndex[u.Tokens[0]]
		if base == "" {
			continue
		}
		px, _ := strconv.ParseFloat(ctxs[i].MidPx, 64)
		if px <= 0 {
			px, _ = strconv.ParseFloat(ctxs[i].MarkPx, 64)
		}
		if px > 0 {
			prices[strings.ToUpper(base)] = px
		}
	}
	return prices, nil
}

// valueSpotBalancesUSD converts spot balances to their USD contribution to
// equity — the ONE valuation both GetBalance and GetBalanceByMarket use, so
// the two reads of the same wallet can no longer disagree (CONN-12: one
// filtered USDC/USDT and dropped every token, the other summed raw token
// QUANTITIES with no price at all).
//
// Stablecoins contribute total − hold: USDC reserved as perp collateral is
// already inside marginSummary.accountValue (incident 2026-05-24), so only
// the unreserved portion may be added. Every other token contributes its FULL
// amount at its USDC mid price — token holdings are never part of
// accountValue, and a token sitting in an open spot order is still owned.
// A token with no resolvable USDC pair contributes 0 (dust and delisted
// tolerance), but when the price map itself cannot be fetched while tokens
// are held, the sync fails as transient instead of silently writing a
// stables-only equity.
func (h *Hyperliquid) valueSpotBalancesUSD(ctx context.Context, balances []hlSpotBalance) (float64, error) {
	var stableFree float64
	var tokens []hlSpotBalance
	for _, b := range balances {
		total, _ := strconv.ParseFloat(b.Total, 64)
		if total <= 0 {
			continue
		}
		if IsStablecoinUSD(b.Coin) {
			hold, _ := strconv.ParseFloat(b.Hold, 64)
			if free := total - hold; free > 0 {
				stableFree += free
			}
			continue
		}
		tokens = append(tokens, b)
	}
	if len(tokens) == 0 {
		return stableFree, nil
	}

	prices, err := h.fetchSpotPriceMap(ctx)
	if err != nil {
		return 0, fmt.Errorf("%w: hyperliquid spot price map: %v", ErrSpotPricingUnavailable, err)
	}
	value := stableFree
	for _, b := range tokens {
		total, _ := strconv.ParseFloat(b.Total, 64)
		if px := prices[strings.ToUpper(strings.TrimSpace(b.Coin))]; px > 0 {
			value += total * px
		}
	}
	return value, nil
}

func (h *Hyperliquid) postInfo(ctx context.Context, body interface{}) (json.RawMessage, error) {
	jsonBody, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", hyperliquidAPI+"/info", bytes.NewReader(jsonBody))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := h.client.Do(req)
	if err != nil {
		return nil, err
	}

	// CONN-AUDIT-001 + 002: bounded read + truncated body in errors.
	data, err := ReadCappedBody(resp.Body, DefaultMaxResponseBytes)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("hyperliquid API error %d: %s", resp.StatusCode, TruncatedBody(data))
	}

	return data, nil
}

// GetCashflows returns deposits/withdrawals from Hyperliquid ledger.
func (h *Hyperliquid) GetCashflows(ctx context.Context, since time.Time) ([]*Cashflow, error) {
	respBody, err := h.postInfo(ctx, map[string]interface{}{
		"type":      "userNonFundingLedgerUpdates",
		"user":      h.walletAddress,
		"startTime": since.UnixMilli(),
	})
	if err != nil {
		return nil, err
	}

	var updates []struct {
		Time  int64 `json:"time"`
		Delta struct {
			Type string `json:"type"`
			Usdc string `json:"usdc"`
		} `json:"delta"`
	}

	if err := json.Unmarshal(respBody, &updates); err != nil {
		return nil, err
	}

	var cashflows []*Cashflow
	for _, u := range updates {
		ts := time.UnixMilli(u.Time).UTC()
		amount, _ := strconv.ParseFloat(u.Delta.Usdc, 64)
		if amount == 0 {
			continue
		}

		switch u.Delta.Type {
		case "deposit":
			cashflows = append(cashflows, &Cashflow{
				Amount:    math.Abs(amount),
				Currency:  "USDC",
				Timestamp: ts,
			})
		case "withdraw":
			cashflows = append(cashflows, &Cashflow{
				Amount:    -math.Abs(amount),
				Currency:  "USDC",
				Timestamp: ts,
			})
		}
	}

	return cashflows, nil
}

// GetBalanceByMarket returns per-market equity (swap for perps, spot if available).
func (h *Hyperliquid) GetBalanceByMarket(ctx context.Context) ([]*MarketBalance, error) {
	respBody, err := h.postInfo(ctx, map[string]interface{}{
		"type": "clearinghouseState",
		"user": h.walletAddress,
	})
	if err != nil {
		return nil, err
	}

	var state struct {
		MarginSummary struct {
			AccountValue    string `json:"accountValue"`
			TotalMarginUsed string `json:"totalMarginUsed"`
		} `json:"marginSummary"`
	}
	if err := json.Unmarshal(respBody, &state); err != nil {
		return nil, err
	}

	var balances []*MarketBalance
	perpEquity, _ := strconv.ParseFloat(state.MarginSummary.AccountValue, 64)
	perpMarginUsed, _ := strconv.ParseFloat(state.MarginSummary.TotalMarginUsed, 64)
	if perpEquity > 0 {
		// Free margin on the perp account = accountValue − totalMarginUsed.
		// Direct from HL's marginSummary; no leverage-table approximation needed.
		balances = append(balances, &MarketBalance{
			MarketType:      MarketSwap,
			Equity:          perpEquity,
			AvailableMargin: perpEquity - perpMarginUsed,
		})
	}

	// Spot balance — through the same valuation as GetBalance, so the spot
	// bucket in breakdown_by_market equals the spot leg of the equity. The
	// old inline sum here added raw token QUANTITIES (1 HYPE counted as 1
	// USD) and included the USDC hold that accountValue already carries.
	spotResp, err := h.postInfo(ctx, map[string]interface{}{
		"type": "spotClearinghouseState",
		"user": h.walletAddress,
	})
	if err != nil {
		return nil, fmt.Errorf("get spot clearinghouse state: %w", err)
	}
	var spotState struct {
		Balances []hlSpotBalance `json:"balances"`
	}
	if err := json.Unmarshal(spotResp, &spotState); err != nil {
		return nil, fmt.Errorf("parse spot clearinghouse state: %w", err)
	}
	spotVal, err := h.valueSpotBalancesUSD(ctx, spotState.Balances)
	if err != nil {
		return nil, err
	}
	if spotVal > 0 {
		balances = append(balances, &MarketBalance{MarketType: MarketSpot, Equity: spotVal})
	}

	return balances, nil
}


// GetFundingFees returns funding fee history from Hyperliquid.
// Uses the userFunding endpoint (read-only, no signing needed).
func (h *Hyperliquid) GetFundingFees(ctx context.Context, symbols []string, since time.Time) ([]*FundingFee, error) {
	respBody, err := h.postInfo(ctx, map[string]interface{}{
		"type":      "userFunding",
		"user":      h.walletAddress,
		"startTime": since.UnixMilli(),
	})
	if err != nil {
		return nil, err
	}

	var entries []struct {
		Time  int64 `json:"time"`
		Delta struct {
			Type        string `json:"type"`
			Coin        string `json:"coin"`
			Usdc        string `json:"usdc"`
			FundingRate string `json:"fundingRate"`
		} `json:"delta"`
	}

	if err := json.Unmarshal(respBody, &entries); err != nil {
		return nil, err
	}

	var fees []*FundingFee
	for _, e := range entries {
		amount, _ := strconv.ParseFloat(e.Delta.Usdc, 64)
		if amount == 0 {
			continue
		}
		fees = append(fees, &FundingFee{
			Amount:    amount,
			Symbol:    e.Delta.Coin,
			Timestamp: time.UnixMilli(e.Time).UTC(),
		})
	}

	return fees, nil
}
