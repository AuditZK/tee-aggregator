package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"sort"
	"strings"
	"time"
)

const (
	igHistoryLookback = 2 * 365 * 24 * time.Hour

	igActivityPageSize = 500
	igMaxActivityPages = 100

	// IG grants 10,000 historical price points a week to a key and account,
	// shared with anything else the client runs on that key.
	igHistoryPriceBudget = 5000

	igPriceTimeFormat = "2006/01/02 15:04:05"

	// A close this near the open cannot fix a deal's point value from its P&L.
	igMinPointMove = 1e-9
)

// igLedgerLine is one ledger row reduced to its effect on cash and to the
// daily figures a snapshot reports.
type igLedgerLine struct {
	at         time.Time
	amount     float64
	deposit    float64
	withdrawal float64
	charge     float64
	deal       bool
	trade      *Trade
	raw        IGRawTransaction
}

// igHeld is a position slice: a closed deal from the ledger, or the part of a
// position still open. Marked at a midnight, it is worth
// point × (mark − openLevel) × size in the account's currency.
type igHeld struct {
	name      string
	epic      string
	opened    time.Time
	closed    time.Time
	size      float64
	openLevel float64
	point     float64
}

func (h igHeld) heldAt(m time.Time) bool {
	return h.opened.Before(m) && (h.closed.IsZero() || m.Before(h.closed))
}

// long picks the side of the book the position would exit on.
func (h igHeld) long() bool {
	return h.point*h.size > 0
}

// midnights returns the first and last row midnight inside [firstRow, lastRow]
// at which the slice is held, and false when it is held at none.
func (h igHeld) midnights(firstRow, lastRow time.Time) (time.Time, time.Time, bool) {
	first := truncUTCDay(h.opened).Add(24 * time.Hour)
	if first.Before(firstRow) {
		first = firstRow
	}
	last := lastRow
	if !h.closed.IsZero() {
		c := truncUTCDay(h.closed)
		if c.Equal(h.closed) {
			c = c.Add(-24 * time.Hour)
		}
		if c.Before(last) {
			last = c
		}
	}
	if last.Before(first) || !h.heldAt(first) {
		return time.Time{}, time.Time{}, false
	}
	return first, last, true
}

type igBar struct {
	day      time.Time
	bid, ask float64
}

// markAt is the close of the last trading day finished before m, on the side
// the position would exit at.
func markAt(bars []igBar, m time.Time, long bool) (float64, bool) {
	idx := sort.Search(len(bars), func(k int) bool { return !bars[k].day.Before(m) })
	for k := idx - 1; k >= 0; k-- {
		b := bars[k]
		price := b.bid
		if !long {
			price = b.ask
		}
		if price == 0 {
			price = b.bid + b.ask
		}
		if price != 0 {
			return price, true
		}
	}
	return 0, false
}

type igOpenPosition struct {
	epic         string
	name         string
	long         bool
	size         float64
	level        float64
	contractSize float64
	mark         float64
	opened       time.Time
}

func igLedgerLines(txs []IGRawTransaction) ([]igLedgerLine, error) {
	lines := make([]igLedgerLine, 0, len(txs))
	for _, t := range txs {
		at, err := parseIGTime(t.DateUTC)
		if err != nil {
			return nil, fmt.Errorf("ig %s ledger line has an unreadable date: %w", t.TransactionType, err)
		}
		amount, err := ParseIGDecimal(t.ProfitAndLoss)
		if err != nil {
			return nil, fmt.Errorf("ig %s ledger line has an unreadable amount: %w", t.TransactionType, err)
		}
		l := igLedgerLine{at: at, raw: t}
		switch {
		case igDepositCodes[strings.ToUpper(strings.TrimSpace(t.TransactionType))]:
			l.amount = math.Abs(amount)
			l.deposit = l.amount
		case isIGWithdrawal(t):
			l.amount = -math.Abs(amount)
			l.withdrawal = math.Abs(amount)
		case isIGTradeLine(t):
			l.amount = amount
			l.deal = true
			if tr, ok := igTradeFrom(t, at, at); ok {
				l.trade = tr
			}
		default:
			l.amount = amount
			l.charge = amount
		}
		lines = append(lines, l)
	}
	sort.SliceStable(lines, func(a, b int) bool { return lines[a].at.Before(lines[b].at) })
	trades := make([]*Trade, 0, len(lines))
	for _, l := range lines {
		if l.trade != nil {
			trades = append(trades, l.trade)
		}
	}
	igSetPointValues(trades, igPointValues(txs))
	return lines, nil
}

// igClosedSlices turns the ledger's deals into position slices. A deal's
// point value comes from its own P&L; one that closed at its open level takes
// the median of its instrument's other deals.
func igClosedSlices(lines []igLedgerLine) ([]igHeld, map[string]float64, error) {
	var held []igHeld
	perName := map[string][]float64{}
	for _, l := range lines {
		if !l.deal {
			continue
		}
		opened, err := parseIGTime(l.raw.OpenDateUTC)
		if err != nil {
			return nil, nil, fmt.Errorf("ig deal line has no readable open date: %w", err)
		}
		size, err := ParseIGDecimal(l.raw.Size)
		if err != nil {
			return nil, nil, fmt.Errorf("ig deal line has an unreadable size: %w", err)
		}
		open, err := ParseIGDecimal(l.raw.OpenLevel)
		if err != nil {
			return nil, nil, fmt.Errorf("ig deal line has an unreadable open level: %w", err)
		}
		h := igHeld{name: l.raw.InstrumentName, opened: opened, closed: l.at, size: size, openLevel: open}
		if closeLevel, cerr := ParseIGDecimal(l.raw.CloseLevel); cerr == nil {
			if move := (closeLevel - open) * size; math.Abs(move) > igMinPointMove {
				h.point = l.amount / move
				perName[h.name] = append(perName[h.name], h.point)
			}
		}
		held = append(held, h)
	}
	points := make(map[string]float64, len(perName))
	for name, ps := range perName {
		points[name] = median(ps)
	}
	for k := range held {
		if held[k].point == 0 {
			held[k].point = points[held[k].name]
		}
	}
	return held, points, nil
}

// igOpenSlices values the positions still open. Where no closed deal of the
// instrument gives a point value, one shared ratio over contract size makes
// the open positions add up to the unrealised P&L the account reports.
func igOpenSlices(open []igOpenPosition, points map[string]float64, profitLoss float64) []igHeld {
	held := make([]igHeld, 0, len(open))
	known, raw := 0.0, 0.0
	var unknown []int
	for _, p := range open {
		size := p.size
		if !p.long {
			size = -size
		}
		h := igHeld{name: p.name, epic: p.epic, opened: p.opened, size: size, openLevel: p.level}
		if pt := points[p.name]; pt != 0 {
			h.point = math.Abs(pt)
			known += h.point * (p.mark - p.level) * size
		} else {
			unknown = append(unknown, len(held))
			raw += (p.mark - p.level) * size * igContractSize(p.contractSize)
		}
		held = append(held, h)
	}
	ratio := 1.0
	if math.Abs(raw) > igMinPointMove {
		ratio = (profitLoss - known) / raw
	}
	if ratio <= 0 {
		ratio = 1
	}
	for _, k := range unknown {
		held[k].point = ratio * igContractSize(open[k].contractSize)
	}
	return held
}

func igContractSize(c float64) float64 {
	if c == 0 {
		return 1
	}
	return c
}

func median(xs []float64) float64 {
	s := append([]float64(nil), xs...)
	sort.Float64s(s)
	n := len(s)
	if n == 0 {
		return 0
	}
	if n%2 == 1 {
		return s[n/2]
	}
	return (s[n/2-1] + s[n/2]) / 2
}

// igHistoryRows is the row range a ledger supports: from the day after its
// first line to yesterday, today's row being the live sync's.
func igHistoryRows(lines []igLedgerLine, now time.Time) (time.Time, time.Time, bool) {
	if len(lines) == 0 {
		return time.Time{}, time.Time{}, false
	}
	first := truncUTCDay(lines[0].at).Add(24 * time.Hour)
	last := truncUTCDay(now).Add(-24 * time.Hour)
	return first, last, !last.Before(first)
}

// buildIGHistoricalSnapshots walks cash back from today's balance through the
// ledger and marks every position held at each midnight to that day's close.
//
// A row holds the equity at the midnight opening its date, with the flows and
// trades of the day before, the shape cTrader's rows and the live sync share.
func buildIGHistoricalSnapshots(lines []igLedgerLine, held []igHeld, bars map[string][]igBar, cashNow float64, now time.Time) ([]*HistoricalSnapshot, error) {
	firstRow, lastRow, ok := igHistoryRows(lines, now)
	if !ok {
		return nil, nil
	}

	type day struct {
		deposits, withdrawals, charges float64
		trades, long, short            int
		volume, longVol, shortVol      float64
	}
	byDay := map[time.Time]*day{}
	total := 0.0
	for _, l := range lines {
		total += l.amount
		key := truncUTCDay(l.at)
		d := byDay[key]
		if d == nil {
			d = &day{}
			byDay[key] = d
		}
		d.deposits += l.deposit
		d.withdrawals += l.withdrawal
		d.charges += l.charge
		if l.trade != nil {
			n := l.trade.Notional()
			d.trades++
			d.volume += n
			if l.trade.Side == "sell" {
				d.short++
				d.shortVol += n
			} else {
				d.long++
				d.longVol += n
			}
		}
	}

	var out []*HistoricalSnapshot
	before, next := 0.0, 0
	for row := firstRow; !row.After(lastRow); row = row.Add(24 * time.Hour) {
		for next < len(lines) && lines[next].at.Before(row) {
			before += lines[next].amount
			next++
		}
		cash := cashNow - (total - before)

		latent := 0.0
		for _, h := range held {
			if !h.heldAt(row) {
				continue
			}
			mark, ok := markAt(bars[h.epic], row, h.long())
			if !ok {
				return nil, fmt.Errorf("no ig close before %s for a position held at that midnight: refusing an unvalued history", row.Format(time.DateOnly))
			}
			latent += h.point * (mark - h.openLevel) * h.size
		}

		snap := &HistoricalSnapshot{Date: row, TotalEquity: cash + latent, RealizedBalance: cash}
		if d := byDay[row.Add(-24*time.Hour)]; d != nil {
			snap.Deposits = d.deposits
			snap.Withdrawals = d.withdrawals
			snap.FundingFees = -d.charges
			snap.TotalTrades = d.trades
			snap.TotalVolume = d.volume
			snap.LongTrades = d.long
			snap.ShortTrades = d.short
			snap.LongVolume = d.longVol
			snap.ShortVolume = d.shortVol
		}
		out = append(out, snap)
	}
	return out, nil
}

// igPriceRange is the span of daily closes one instrument needs.
type igPriceRange struct {
	from, to time.Time
}

func (r igPriceRange) days() int {
	return int(r.to.Sub(r.from)/(24*time.Hour)) + 1
}

// igPriceRanges groups the slices held at a row midnight by instrument. A
// few days ahead of the first midnight covers a weekend or holiday before it.
func igPriceRanges(held []igHeld, firstRow, lastRow time.Time) map[string]igPriceRange {
	ranges := map[string]igPriceRange{}
	for _, h := range held {
		first, last, ok := h.midnights(firstRow, lastRow)
		if !ok {
			continue
		}
		from, to := first.Add(-5*24*time.Hour), last.Add(-24*time.Hour)
		r, seen := ranges[h.epic]
		if !seen || from.Before(r.from) {
			r.from = from
		}
		if !seen || to.After(r.to) {
			r.to = to
		}
		ranges[h.epic] = r
	}
	return ranges
}

// GetHistoricalSnapshots reconstructs the account's daily equity in-enclave:
// the IG password never leaves the SEV-SNP perimeter, and the encrypted login
// some regions require is the live connector's own.
//
// Cash is walked back from today's balance through the transaction ledger.
// Every position held at a midnight is marked to the close of the trading day
// before it, at a point value read from the deal's own P&L, or, for a
// position still open with no closed deal of its instrument, calibrated on the
// unrealised P&L the account reports today. A position that cannot be priced
// fails the whole history rather than leave a day valued at cash.
func (i *IG) GetHistoricalSnapshots(ctx context.Context, since time.Time) ([]*HistoricalSnapshot, error) {
	now := time.Now().UTC()
	start := since.UTC()
	if start.IsZero() || start.Before(now.Add(-igHistoryLookback)) {
		start = now.Add(-igHistoryLookback)
	}
	start = truncUTCDay(start)

	acct, err := i.fetchSelectedAccount(ctx)
	if err != nil {
		return nil, err
	}
	txs, err := i.fetchTransactions(ctx, start, now)
	if err != nil {
		return nil, err
	}
	lines, err := igLedgerLines(txs)
	if err != nil {
		return nil, err
	}
	coverage := HistoryCoverage{LedgerLines: len(lines), PriceAllowanceLeft: -1}
	if len(lines) > 0 {
		coverage.OldestLine = lines[0].at
	}
	firstRow, lastRow, ok := igHistoryRows(lines, now)
	if !ok {
		i.setHistoryCoverage(coverage)
		return nil, nil
	}

	closed, points, err := igClosedSlices(lines)
	if err != nil {
		return nil, err
	}
	open, err := i.fetchOpenPositions(ctx)
	if err != nil {
		return nil, err
	}
	held := append(closed, igOpenSlices(open, points, acct.Balance.ProfitLoss)...)

	var overnight []int
	for k, h := range held {
		if _, _, atMidnight := h.midnights(firstRow, lastRow); atMidnight {
			overnight = append(overnight, k)
		}
	}
	if err := i.resolveEpics(ctx, held, overnight); err != nil {
		return nil, err
	}
	for _, k := range overnight {
		if held[k].point == 0 {
			return nil, fmt.Errorf("an ig position held overnight has no point value: refusing an unvalued history")
		}
	}

	ranges := igPriceRanges(held, firstRow, lastRow)
	budget := 0
	for _, r := range ranges {
		budget += r.days()
	}
	if budget > igHistoryPriceBudget {
		return nil, fmt.Errorf("ig history needs about %d daily closes, above the %d kept for it out of the weekly allowance", budget, igHistoryPriceBudget)
	}

	bars := make(map[string][]igBar, len(ranges))
	for epic, r := range ranges {
		b, left, err := i.fetchDailyBars(ctx, epic, r.from, r.to)
		if err != nil {
			return nil, err
		}
		bars[epic] = b
		coverage.PricePoints += len(b)
		coverage.PriceAllowanceLeft = left
	}
	coverage.HeldPositions = len(overnight)
	coverage.Instruments = len(ranges)

	out, err := buildIGHistoricalSnapshots(lines, held, bars, acct.Balance.Balance, now)
	if err != nil {
		return nil, err
	}
	for _, snap := range out {
		snap.Currency = acct.Currency
		snap.MeasuredOn = snap.Date.Add(-24 * time.Hour)
	}
	i.setHistoryCoverage(coverage)
	return out, nil
}

func (i *IG) setHistoryCoverage(c HistoryCoverage) {
	i.historyMu.Lock()
	i.coverage = c
	i.historyMu.Unlock()
}

// HistoryCoverage reports what the last reconstruction could read.
func (i *IG) HistoryCoverage() HistoryCoverage {
	i.historyMu.Lock()
	defer i.historyMu.Unlock()
	return i.coverage
}

// resolveEpics finds the instrument code of every listed slice: the ledger
// names an instrument, the price history wants its epic. Open positions carry
// theirs; closed deals take the one the activity history pairs with the same
// market name, and a name pairing with several epics is refused rather than
// priced off the wrong one.
func (i *IG) resolveEpics(ctx context.Context, held []igHeld, idx []int) error {
	byName := map[string]map[string]bool{}
	var from, to time.Time
	for _, h := range held {
		if h.epic != "" {
			addEpic(byName, h.name, h.epic)
		}
	}
	for _, k := range idx {
		h := held[k]
		if h.epic != "" || len(byName[h.name]) > 0 {
			continue
		}
		if from.IsZero() || h.closed.Before(from) {
			from = h.closed
		}
		if h.closed.After(to) {
			to = h.closed
		}
	}
	if !from.IsZero() {
		names, err := i.fetchActivityEpics(ctx, truncUTCDay(from), truncUTCDay(to).Add(24*time.Hour-time.Second))
		if err != nil {
			return err
		}
		for name, epics := range names {
			for e := range epics {
				addEpic(byName, name, e)
			}
		}
	}

	missing, ambiguous := 0, 0
	for _, k := range idx {
		if held[k].epic != "" {
			continue
		}
		switch epics := byName[held[k].name]; len(epics) {
		case 1:
			for e := range epics {
				held[k].epic = e
			}
		case 0:
			missing++
		default:
			ambiguous++
		}
	}
	if missing > 0 || ambiguous > 0 {
		return fmt.Errorf("ig history cannot price %d positions held overnight (%d with no instrument code, %d with several): refusing an unvalued history", missing+ambiguous, missing, ambiguous)
	}
	return nil
}

func addEpic(m map[string]map[string]bool, name, epic string) {
	if name == "" || epic == "" {
		return
	}
	if m[name] == nil {
		m[name] = map[string]bool{}
	}
	m[name][epic] = true
}

func (i *IG) fetchOpenPositions(ctx context.Context) ([]igOpenPosition, error) {
	accountID, err := i.ensureAccountID(ctx)
	if err != nil {
		return nil, err
	}
	body, err := i.doAuthed(ctx, "2", "/positions", nil, accountID)
	if err != nil {
		return nil, fmt.Errorf("fetch ig positions: %w", err)
	}
	var resp struct {
		Positions []struct {
			Market struct {
				Epic           string  `json:"epic"`
				InstrumentName string  `json:"instrumentName"`
				Bid            float64 `json:"bid"`
				Offer          float64 `json:"offer"`
			} `json:"market"`
			Position struct {
				Direction      string  `json:"direction"`
				Size           float64 `json:"size"`
				Level          float64 `json:"level"`
				ContractSize   float64 `json:"contractSize"`
				CreatedDateUTC string  `json:"createdDateUTC"`
			} `json:"position"`
		} `json:"positions"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("decode ig positions: %w", err)
	}
	out := make([]igOpenPosition, 0, len(resp.Positions))
	for _, p := range resp.Positions {
		if p.Position.Size == 0 {
			continue
		}
		opened, err := parseIGTime(p.Position.CreatedDateUTC)
		if err != nil {
			return nil, fmt.Errorf("ig open position has no readable open date: %w", err)
		}
		long := !strings.EqualFold(p.Position.Direction, "SELL")
		mark := p.Market.Bid
		if !long {
			mark = p.Market.Offer
		}
		out = append(out, igOpenPosition{
			epic:         p.Market.Epic,
			name:         p.Market.InstrumentName,
			long:         long,
			size:         p.Position.Size,
			level:        p.Position.Level,
			contractSize: p.Position.ContractSize,
			mark:         mark,
			opened:       opened,
		})
	}
	return out, nil
}

// fetchActivityEpics maps market names to the epics the activity history
// pairs them with over [from, to].
func (i *IG) fetchActivityEpics(ctx context.Context, from, to time.Time) (map[string]map[string]bool, error) {
	accountID, err := i.ensureAccountID(ctx)
	if err != nil {
		return nil, err
	}
	path := "/history/activity"
	q := url.Values{}
	q.Set("from", from.UTC().Format(igTimeFormat))
	q.Set("to", to.UTC().Format(igTimeFormat))
	q.Set("detailed", "true")
	q.Set("pageSize", fmt.Sprint(igActivityPageSize))

	out := map[string]map[string]bool{}
	for page := 0; page < igMaxActivityPages; page++ {
		body, err := i.doAuthed(ctx, "3", path, q, accountID)
		if err != nil {
			return nil, fmt.Errorf("fetch ig activity: %w", err)
		}
		var resp struct {
			Activities []struct {
				Epic    string `json:"epic"`
				Details *struct {
					MarketName string `json:"marketName"`
				} `json:"details"`
			} `json:"activities"`
			Metadata struct {
				Paging struct {
					Next string `json:"next"`
				} `json:"paging"`
			} `json:"metadata"`
		}
		if err := json.Unmarshal(body, &resp); err != nil {
			return nil, fmt.Errorf("decode ig activity: %w", err)
		}
		for _, a := range resp.Activities {
			if a.Details != nil {
				addEpic(out, a.Details.MarketName, a.Epic)
			}
		}
		if resp.Metadata.Paging.Next == "" {
			return out, nil
		}
		next, err := url.Parse(resp.Metadata.Paging.Next)
		if err != nil {
			return nil, fmt.Errorf("parse ig activity paging: %w", err)
		}
		path = next.Path
		if at := strings.Index(path, "/history/"); at >= 0 {
			path = path[at:]
		}
		q = next.Query()
		q.Del("version")
	}
	return nil, fmt.Errorf("ig activity exceeds %d pages of %d: refusing a truncated instrument map", igMaxActivityPages, igActivityPageSize)
}

// fetchDailyBars reads one instrument's daily closes over [from, to] and the
// price allowance left after the call.
func (i *IG) fetchDailyBars(ctx context.Context, epic string, from, to time.Time) ([]igBar, int, error) {
	accountID, err := i.ensureAccountID(ctx)
	if err != nil {
		return nil, 0, err
	}
	q := url.Values{}
	q.Set("resolution", "DAY")
	q.Set("from", truncUTCDay(from).Format(igTimeFormat))
	q.Set("to", truncUTCDay(to).Add(24*time.Hour-time.Second).Format(igTimeFormat))
	q.Set("pageSize", "0")

	body, err := i.doAuthed(ctx, "3", "/prices/"+url.PathEscape(epic), q, accountID)
	if err != nil {
		switch {
		case strings.Contains(string(body), "exceeded-account-historical-data-allowance"):
			return nil, 0, fmt.Errorf("ig historical price allowance used up, the history waits for it to reset: %w", err)
		case strings.Contains(string(body), "error.unsupported.epic"):
			return nil, 0, fmt.Errorf("ig serves no price history for an instrument held overnight: refusing an unvalued history: %w", err)
		}
		return nil, 0, fmt.Errorf("fetch ig prices: %w", err)
	}

	type side struct {
		Bid        *float64 `json:"bid"`
		Ask        *float64 `json:"ask"`
		LastTraded *float64 `json:"lastTraded"`
	}
	var resp struct {
		Prices []struct {
			SnapshotTime    string `json:"snapshotTime"`
			SnapshotTimeUTC string `json:"snapshotTimeUTC"`
			ClosePrice      side   `json:"closePrice"`
		} `json:"prices"`
		Metadata struct {
			Allowance struct {
				RemainingAllowance int `json:"remainingAllowance"`
			} `json:"allowance"`
		} `json:"metadata"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, 0, fmt.Errorf("decode ig prices: %w", err)
	}

	val := func(p *float64) float64 {
		if p == nil {
			return 0
		}
		return *p
	}
	bars := make([]igBar, 0, len(resp.Prices))
	for _, p := range resp.Prices {
		day, err := igBarDay(p.SnapshotTime, p.SnapshotTimeUTC)
		if err != nil {
			return nil, 0, err
		}
		b := igBar{day: day, bid: val(p.ClosePrice.Bid), ask: val(p.ClosePrice.Ask)}
		if b.bid == 0 && b.ask == 0 {
			b.bid = val(p.ClosePrice.LastTraded)
			b.ask = b.bid
		}
		bars = append(bars, b)
	}
	sort.Slice(bars, func(a, b int) bool { return bars[a].day.Before(bars[b].day) })
	return bars, resp.Metadata.Allowance.RemainingAllowance, nil
}

// igBarDay is the trading date a daily bar closes. snapshotTime is in the
// account's own zone, where the date is the trading day itself; its UTC twin
// falls on the day before for an account east of London.
func igBarDay(local, utc string) (time.Time, error) {
	if ts, err := time.Parse(igPriceTimeFormat, local); err == nil {
		return time.Date(ts.Year(), ts.Month(), ts.Day(), 0, 0, 0, 0, time.UTC), nil
	}
	ts, err := parseIGTime(utc)
	if err != nil {
		return time.Time{}, fmt.Errorf("ig price bar has no readable date: %w", err)
	}
	return truncUTCDay(ts), nil
}
