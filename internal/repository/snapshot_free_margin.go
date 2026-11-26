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

// MarketMargin is one breakdown entry's equity and free margin, as read or as
// to be written.
type MarketMargin struct {
	Equity          float64 `json:"equity"`
	AvailableMargin float64 `json:"available_margin"`
}

// marginMatchTolerance absorbs the jsonb numeric round trip of a stored float.
const marginMatchTolerance = 1e-6

// UpdateRebuiltAvailableMargin rewrites the available_margin of the named
// breakdown entries ("spot", "swap", "global", …) of one rebuilt snapshot, and
// nothing else: equity, flows, activity, the origin flag and every other key
// of the breakdown stay byte-for-byte as stored. It writes only when the row
// is still a rebuilt one (from_external_rebuilder) and each named entry still
// holds the equity and free margin the caller read (expect); a row rewritten
// since is left alone and reported false.
func (r *SnapshotRepo) UpdateRebuiltAvailableMargin(ctx context.Context, id string, expect map[string]MarketMargin, next map[string]float64) (bool, error) {
	r.hasLabelColumn(ctx)
	if !r.isTSSchema {
		return false, errors.New("update rebuilt free margin: production schema only")
	}
	if !r.hasFromExternalRebuilderColumn(ctx) {
		return false, errors.New("update rebuilt free margin: from_external_rebuilder column missing, cannot tell a rebuilt row from a live one")
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	var raw []byte
	var rebuilt bool
	err = tx.QueryRow(ctx, `SELECT breakdown_by_market, from_external_rebuilder FROM snapshot_data WHERE id = $1 FOR UPDATE`, id).Scan(&raw, &rebuilt)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read snapshot breakdown: %w", err)
	}
	if !rebuilt {
		// A live-measured day is never rewritten here.
		return false, nil
	}

	patched, ok, err := patchAvailableMargin(raw, expect, next)
	if err != nil || !ok {
		return false, err
	}
	tag, err := tx.Exec(ctx, `UPDATE snapshot_data SET breakdown_by_market = $1, "updatedAt" = $2 WHERE id = $3 AND from_external_rebuilder = true`,
		patched, time.Now().UTC(), id)
	if err != nil {
		return false, fmt.Errorf("update rebuilt free margin: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit rebuilt free margin: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// patchAvailableMargin sets next's free margins into the stored breakdown
// JSON. Only the available_margin key of the named entries changes: every
// other key keeps its stored bytes (the entries are decoded as raw JSON, so
// a number is never re-encoded). It reports false, writing nothing, when an
// entry named in next is absent or no longer holds what expect says.
func patchAvailableMargin(raw []byte, expect map[string]MarketMargin, next map[string]float64) ([]byte, bool, error) {
	if len(next) == 0 {
		return nil, false, nil
	}
	doc := map[string]json.RawMessage{}
	if len(raw) > 0 && string(raw) != "null" {
		if err := json.Unmarshal(raw, &doc); err != nil {
			return nil, false, fmt.Errorf("decode snapshot breakdown: %w", err)
		}
	}
	for key, free := range next {
		if math.IsNaN(free) || math.IsInf(free, 0) {
			return nil, false, fmt.Errorf("free margin for %s is not a number", key)
		}
		want, ok := expect[key]
		if !ok {
			return nil, false, fmt.Errorf("no expected value for %s", key)
		}
		entryRaw, ok := doc[key]
		if !ok {
			return nil, false, nil
		}
		entry := map[string]json.RawMessage{}
		if err := json.Unmarshal(entryRaw, &entry); err != nil {
			return nil, false, fmt.Errorf("decode breakdown %s: %w", key, err)
		}
		if !rawNumberIs(entry["equity"], want.Equity) || !rawNumberIs(entry["available_margin"], want.AvailableMargin) {
			return nil, false, nil
		}
		v, err := json.Marshal(free)
		if err != nil {
			return nil, false, err
		}
		entry["available_margin"] = v
		out, err := json.Marshal(entry)
		if err != nil {
			return nil, false, fmt.Errorf("encode breakdown %s: %w", key, err)
		}
		doc[key] = out
	}
	out, err := json.Marshal(doc)
	if err != nil {
		return nil, false, fmt.Errorf("encode snapshot breakdown: %w", err)
	}
	return out, true, nil
}

// rawNumberIs reports whether a stored JSON number equals want; an absent key
// is 0, as omitempty writes it.
func rawNumberIs(v json.RawMessage, want float64) bool {
	got := 0.0
	if len(v) > 0 && string(v) != "null" {
		if err := json.Unmarshal(v, &got); err != nil {
			return false
		}
	}
	return math.Abs(got-want) <= marginMatchTolerance*math.Max(1, math.Abs(want))
}
