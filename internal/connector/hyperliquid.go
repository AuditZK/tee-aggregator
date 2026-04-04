package connector

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
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
		CrossMaintenanceMarginUsed string `json:"crossMaintenanceMarginUsed"`
	}
	if err := json.Unmarshal(resp, &state); err != nil {
		return nil, fmt.Errorf("parse clearinghouse state: %w", err)
	}

	equity, _ := strconv.ParseFloat(state.MarginSummary.AccountValue, 64)
	marginUsed, _ := strconv.ParseFloat(state.MarginSummary.TotalMarginUsed, 64)

	// Also check spot balances
	spotResp, err := h.postInfo(ctx, map[string]interface{}{
		"type": "spotClearinghouseState",
		"user": h.walletAddress,
	})
	if err == nil {
		var spotState struct {
			Balances []struct {
				Coin  string `json:"coin"`
				Total string `json:"total"`
			} `json:"balances"`
		}
		if json.Unmarshal(spotResp, &spotState) == nil {
			for _, b := range spotState.Balances {
				val, _ := strconv.ParseFloat(b.Total, 64)
				if b.Coin == "USDC" || b.Coin == "USDT" {
					equity += val
				}
			}
		}
	}

	return &Balance{
		Equity:    equity,
		Available: equity - marginUsed,
		Currency:  "USD",
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
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("hyperliquid API error %d: %s", resp.StatusCode, string(data))
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
			AccountValue string `json:"accountValue"`
		} `json:"marginSummary"`
	}
	if err := json.Unmarshal(respBody, &state); err != nil {
		return nil, err
	}

	var balances []*MarketBalance
	perpEquity, _ := strconv.ParseFloat(state.MarginSummary.AccountValue, 64)
	if perpEquity > 0 {
		balances = append(balances, &MarketBalance{MarketType: MarketSwap, Equity: perpEquity})
	}

	// Spot balance
	spotResp, err := h.postInfo(ctx, map[string]interface{}{
		"type": "spotClearinghouseState",
		"user": h.walletAddress,
	})
	if err == nil {
		var spotState struct {
			Balances []struct {
				Token string `json:"token"`
				Total string `json:"total"`
			} `json:"balances"`
		}
		if json.Unmarshal(spotResp, &spotState) == nil {
			spotTotal := 0.0
			for _, b := range spotState.Balances {
				val, _ := strconv.ParseFloat(b.Total, 64)
				spotTotal += val
			}
			if spotTotal > 0 {
				balances = append(balances, &MarketBalance{MarketType: MarketSpot, Equity: spotTotal})
			}
		}
	}

	return balances, nil
}

// GetFundingFees returns funding fee history from Hyperliquid.
// Uses the userFunding endpoint (read-only, no signing needed).
func (h *Hyperliquid) GetFundingFees(ctx context.Context, symbols []string, since time.Time) ([]*FundingFee, error) {
	body, _ := json.Marshal(map[string]interface{}{
		"type":      "userFunding",
		"user":      h.walletAddress,
		"startTime": since.UnixMilli(),
	})

	respBody, err := h.postInfo(ctx, body)
	if err != nil {
		return nil, err
	}

	var entries []struct {
		Time    int64  `json:"time"`
		Coin    string `json:"coin"`
		Usdc    string `json:"usdc"`
		Szi     string `json:"szi"`
		FundingRate string `json:"fundingRate"`
	}

	if err := json.Unmarshal(respBody, &entries); err != nil {
		return nil, err
	}

	var fees []*FundingFee
	for _, e := range entries {
		amount, _ := strconv.ParseFloat(e.Usdc, 64)
		if amount == 0 {
			continue
		}
		fees = append(fees, &FundingFee{
			Amount:    amount,
			Symbol:    e.Coin,
			Timestamp: time.UnixMilli(e.Time).UTC(),
		})
	}

	return fees, nil
}
