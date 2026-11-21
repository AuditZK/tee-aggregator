package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5"
)

// UpdateActivity rewrites the activity fields of one snapshot's breakdown
// (volume, trades, fees, funding, buy/sell split) market by market, only if
// its global entry still holds the trades and volume the caller read. Every
// other key, equity and margin included, stays as stored. Reports whether the
// row changed.
func (r *SnapshotRepo) UpdateActivity(ctx context.Context, id string, oldTrades int, oldVolume float64, next *MarketBreakdown) (bool, error) {
	r.hasLabelColumn(ctx)
	if !r.isTSSchema {
		return false, errors.New("update snapshot activity: production schema only")
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	var raw []byte
	err = tx.QueryRow(ctx, `SELECT breakdown_by_market FROM snapshot_data WHERE id = $1 FOR UPDATE`, id).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read snapshot breakdown: %w", err)
	}

	patched, ok, err := patchActivity(raw, oldTrades, oldVolume, next)
	if err != nil || !ok {
		return false, err
	}
	tag, err := tx.Exec(ctx, `UPDATE snapshot_data SET breakdown_by_market = $1, "updatedAt" = $2 WHERE id = $3`,
		patched, time.Now().UTC(), id)
	if err != nil {
		return false, fmt.Errorf("update snapshot activity: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit snapshot activity: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// patchActivity sets next's activity fields into the stored breakdown JSON,
// or reports false when its global entry no longer holds what the caller read.
func patchActivity(raw []byte, oldTrades int, oldVolume float64, next *MarketBreakdown) ([]byte, bool, error) {
	doc := map[string]map[string]any{}
	if len(raw) > 0 && string(raw) != "null" {
		if err := json.Unmarshal(raw, &doc); err != nil {
			return nil, false, fmt.Errorf("decode snapshot breakdown: %w", err)
		}
	}
	g := doc["global"]
	if jsonNumber(g["trades"]) != float64(oldTrades) || math.Abs(jsonNumber(g["volume"])-oldVolume) > 1e-6 {
		return nil, false, nil
	}

	for key, m := range next.byKey() {
		if m == nil {
			continue
		}
		entry := doc[key]
		if entry == nil {
			entry = map[string]any{}
			doc[key] = entry
		}
		entry["volume"] = m.Volume
		entry["trades"] = m.Trades
		entry["trading_fees"] = m.TradingFees
		entry["funding_fees"] = m.FundingFees
		entry["long_trades"] = m.LongTrades
		entry["short_trades"] = m.ShortTrades
		entry["long_volume"] = m.LongVolume
		entry["short_volume"] = m.ShortVolume
	}
	out, err := json.Marshal(doc)
	if err != nil {
		return nil, false, fmt.Errorf("encode snapshot breakdown: %w", err)
	}
	return out, true, nil
}

func (b *MarketBreakdown) byKey() map[string]*MarketMetrics {
	return map[string]*MarketMetrics{
		"stocks": b.Stocks, "spot": b.Spot, "swap": b.Swap, "futures": b.Futures,
		"options": b.Options, "margin": b.Margin, "earn": b.Earn, "cfd": b.CFD,
		"forex": b.Forex, "commodities": b.Commodities, "global": b.Global,
	}
}

func jsonNumber(v any) float64 {
	f, _ := v.(float64)
	return f
}
