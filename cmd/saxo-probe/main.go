// Read-only probe that runs the Saxo connector against a real account, SIM by
// default, and says which of its assumptions hold. It writes nothing to any
// enclave or database.
//
// What it settles that Saxo's documentation does not:
//
//  1. Refresh and rotation: each refresh returns a new refresh token, the one
//     spent is refused afterwards, and the lifetimes Saxo states.
//  2. Every connector call answers: client, balance, positions, closed
//     positions, the transactions ledger, funding.
//  3. Capital classification: the TransactionType / FundingSubType values the
//     ledger actually carries, and whether the transfers the connector books
//     match Saxo's own CashTransfer series over the same window.
//
// Usage:
//
//	SAXO_APP_KEY=... SAXO_APP_SECRET=... [SAXO_REDIRECT_URI=...] \
//	SAXO_ACCESS_TOKEN=... SAXO_REFRESH_TOKEN=... \
//	go run ./cmd/saxo-probe -days 30 -save saxo-pair.json
//
// SAXO_LIVE=1 targets the live gateway. A run spends the refresh token it
// starts from: -save writes the newest pair to a file (mode 0600) after every
// rotation, and -load starts the next run from that file. Tokens are never
// printed.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/trackrecord/enclave/internal/connector"
)

const (
	checkSingleUse      = "single use"
	checkTransferSeries = "transfer series"
)

type pairFile struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
}

type check struct {
	name   string
	status string
	detail string
}

type probe struct {
	saxo   *connector.Saxo
	sim    bool
	save   string
	from   time.Time
	to     time.Time
	checks []check
}

func (p *probe) record(name, status, format string, args ...any) {
	detail := fmt.Sprintf(format, args...)
	p.checks = append(p.checks, check{name: name, status: status, detail: detail})
	fmt.Printf("[%-4s] %-22s %s\n", status, name, detail)
}

func main() {
	days := flag.Int("days", 30, "window, in days, for trades, cashflows and funding")
	rotations := flag.Int("rotations", 2, "refreshes to run, each spending the previous refresh token")
	save := flag.String("save", "", "file to write the newest token pair to (mode 0600)")
	load := flag.String("load", "", "file to read the starting token pair from instead of the environment")
	flag.Parse()

	pair, err := startingPair(*load)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	sim := os.Getenv("SAXO_LIVE") != "1"
	if *save == "" && *rotations > 0 {
		fmt.Println("note: no -save given; the refresh token passed in is spent by the first rotation and the new one is not kept")
	}

	now := time.Now().UTC()
	p := &probe{
		saxo: connector.NewSaxo(&connector.Credentials{APIKey: pair.AccessToken, APISecret: pair.RefreshToken}, sim),
		sim:  sim,
		save: *save,
		to:   now,
		from: now.AddDate(0, 0, -*days),
	}

	env := "SIM"
	if !sim {
		env = "LIVE"
	}
	fmt.Printf("Saxo probe, %s gateway, %d-day window\n\n", env, *days)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	p.checkConfig()
	p.checkRotation(ctx, pair.RefreshToken, *rotations)
	p.checkAccount(ctx)
	flows := p.checkLedger(ctx)
	p.checkTransferSeries(ctx, flows)
	p.summary()
}

func startingPair(load string) (pairFile, error) {
	if load != "" {
		raw, err := os.ReadFile(load)
		if err != nil {
			return pairFile{}, fmt.Errorf("read %s: %w", load, err)
		}
		var p pairFile
		if err := json.Unmarshal(raw, &p); err != nil {
			return pairFile{}, fmt.Errorf("decode %s: %w", load, err)
		}
		return p, nil
	}
	p := pairFile{AccessToken: os.Getenv("SAXO_ACCESS_TOKEN"), RefreshToken: os.Getenv("SAXO_REFRESH_TOKEN")}
	if p.AccessToken == "" || p.RefreshToken == "" {
		return pairFile{}, errors.New("set SAXO_ACCESS_TOKEN and SAXO_REFRESH_TOKEN, or pass -load")
	}
	return p, nil
}

func (p *probe) persist() {
	if p.save == "" {
		return
	}
	access, refresh := p.saxo.CurrentCredentials()
	raw, _ := json.Marshal(pairFile{AccessToken: access, RefreshToken: refresh})
	if err := os.WriteFile(p.save, raw, 0o600); err != nil {
		fmt.Fprintf(os.Stderr, "could not save the token pair to %s: %v\n", p.save, err)
	}
}

func (p *probe) checkConfig() {
	key, secret := os.Getenv("SAXO_APP_KEY") != "", os.Getenv("SAXO_APP_SECRET") != ""
	status := "OK"
	if !key || !secret {
		status = "FAIL"
	}
	p.record("app credentials", status, "SAXO_APP_KEY set=%v, SAXO_APP_SECRET set=%v, SAXO_REDIRECT_URI sent=%v",
		key, secret, os.Getenv("SAXO_REDIRECT_URI") != "")
}

func (p *probe) checkRotation(ctx context.Context, firstRefresh string, rotations int) {
	for i := 1; i <= rotations; i++ {
		_, before := p.saxo.CurrentCredentials()
		err := p.saxo.KeepAlive(ctx, 0)
		_, after := p.saxo.CurrentCredentials()
		if err != nil {
			p.record(fmt.Sprintf("refresh %d", i), "FAIL", "%v", err)
			return
		}
		p.persist()
		if after == before {
			p.record(fmt.Sprintf("refresh %d", i), "WARN", "refresh succeeded but the refresh token did not change")
			continue
		}
		accessTTL, refreshTTL := p.saxo.TokenLifetimes()
		p.record(fmt.Sprintf("refresh %d", i), "OK", "rotated; access token lives %v, refresh token %v", accessTTL, refreshTTL)
		if refreshTTL > 0 && refreshTTL < 25*time.Minute {
			p.record("refresh lifetime", "WARN", "%v is too short for the 15-minute keep-alive cycle", refreshTTL)
		}
	}
	if rotations == 0 {
		return
	}

	spent := connector.NewSaxo(&connector.Credentials{APIKey: "spent", APISecret: firstRefresh}, p.sim)
	err := spent.KeepAlive(ctx, 0)
	switch {
	case errors.Is(err, connector.ErrRefreshRefused):
		p.record(checkSingleUse, "OK", "the spent refresh token is refused: %v", err)
	case err == nil:
		p.record(checkSingleUse, "WARN", "Saxo accepted a spent refresh token; rotation has a grace period or is not single-use")
	default:
		p.record(checkSingleUse, "WARN", "could not tell: %v", err)
	}
}

func (p *probe) checkAccount(ctx context.Context) {
	if err := p.saxo.TestConnection(ctx); err != nil {
		p.record("client", "FAIL", "%v", err)
		return
	}
	p.record("client", "OK", "ClientKey resolved")
	p.persist()

	if paper, _ := p.saxo.DetectIsPaper(ctx); paper != p.sim {
		p.record("paper flag", "FAIL", "DetectIsPaper=%v on the %v gateway", paper, map[bool]string{true: "SIM", false: "live"}[p.sim])
	} else {
		p.record("paper flag", "OK", "DetectIsPaper=%v", paper)
	}

	if bal, err := p.saxo.GetBalance(ctx); err != nil {
		p.record("balance", "FAIL", "%v", err)
	} else {
		p.record("balance", "OK", "equity=%.2f realized=%.2f unrealized=%.2f available=%.2f %s",
			bal.Equity, bal.Equity-bal.UnrealizedPnL, bal.UnrealizedPnL, bal.Available, bal.Currency)
	}

	if positions, err := p.saxo.GetPositions(ctx); err != nil {
		p.record("positions", "FAIL", "%v", err)
	} else {
		byMarket := map[string]int{}
		for _, pos := range positions {
			byMarket[pos.MarketType]++
		}
		p.record("positions", "OK", "%d open %v", len(positions), byMarket)
	}

	if trades, err := p.saxo.GetTrades(ctx, p.from, p.to); err != nil {
		p.record("closed positions", "FAIL", "%v", err)
	} else {
		var fees float64
		byMarket := map[string]int{}
		for _, t := range trades {
			fees += t.Fee
			byMarket[t.MarketType]++
		}
		p.record("closed positions", "OK", "%d closed in the window, fees=%.2f %v (recent closes only, by design)", len(trades), fees, byMarket)
	}
}

// checkLedger prints what the ledger actually carries and what the connector
// books from it, and returns the net capital the connector booked.
func (p *probe) checkLedger(ctx context.Context) float64 {
	raw, err := p.saxo.RawTransactions(ctx, p.from, p.to)
	if err != nil {
		p.record("transactions", "FAIL", "%v", err)
		return math.NaN()
	}
	p.record("transactions", "OK", "%d rows over the window", len(raw))
	printLedgerShape(raw)

	flows, err := p.saxo.GetCashflows(ctx, p.from)
	if err != nil {
		p.record("cashflows", "FAIL", "%v", err)
		return math.NaN()
	}
	var deposits, withdrawals float64
	for _, f := range flows {
		if f.Amount > 0 {
			deposits += f.Amount
		} else {
			withdrawals -= f.Amount
		}
	}
	p.record("cashflows", "OK", "%d booked: deposits=%.2f withdrawals=%.2f (closed days only, today waits for tomorrow)", len(flows), deposits, withdrawals)

	if fees, err := p.saxo.GetFundingFees(ctx, nil, p.from); err != nil {
		p.record("funding", "FAIL", "%v", err)
	} else {
		var sum float64
		for _, f := range fees {
			sum += f.Amount
		}
		p.record("funding", "OK", "%d cost/income rows, net %.2f", len(fees), sum)
	}

	if w := p.saxo.CapabilityWarnings(); len(w) > 0 {
		p.record("warnings", "WARN", "%s", strings.Join(w, ", "))
	}
	return deposits - withdrawals
}

func printLedgerShape(raw []connector.SaxoRawTransaction) {
	types := map[string]int{}
	funding := map[string]int{}
	amountTypes := map[string]int{}
	events := map[string]int{}
	intraday := 0
	for _, t := range raw {
		types[t.TransactionType]++
		events[t.TransactionType+" / "+t.Event]++
		switch t.TransactionType {
		case "CashTransfer":
			funding[t.FundingSubType]++
		case "CashAmount", "CorporateAction":
			amountTypes[t.AmountType]++
		}
		if t.Intraday {
			intraday++
		}
	}
	printCounts("TransactionType", types)
	printCounts("CashTransfer FundingSubType (IntraClientTransfer is skipped, the rest is capital)", funding)
	printCounts("CashAmount/CorporateAction amount types (funding, never capital)", amountTypes)
	printCounts("TransactionType / Event (a securities transfer is capital the connector cannot book)", events)
	fmt.Printf("  intraday rows (not booked yet): %d\n\n", intraday)
}

func printCounts(title string, counts map[string]int) {
	fmt.Printf("  %s:\n", title)
	if len(counts) == 0 {
		fmt.Println("    (none)")
		return
	}
	keys := make([]string, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Printf("    %-48q %d\n", k, counts[k])
	}
}

// checkTransferSeries compares the capital the connector booked with Saxo's
// own CashTransfer series. The series' shape (cumulative or daily) is not
// documented, so both readings are tried.
func (p *probe) checkTransferSeries(ctx context.Context, booked float64) {
	cash, securities, err := p.saxo.TransferSeries(ctx, p.from, p.to.AddDate(0, 0, -1))
	if err != nil {
		p.record(checkTransferSeries, "WARN", "performance series unreadable: %v", err)
		return
	}
	if math.IsNaN(booked) {
		p.record(checkTransferSeries, "WARN", "no booked capital to compare with")
		return
	}

	var daily, first, last float64
	for _, pt := range cash {
		daily += pt.Value
	}
	if len(cash) > 0 {
		first, last = cash[0].Value, cash[len(cash)-1].Value
	}
	near := func(a, b float64) bool { return math.Abs(a-b) <= math.Max(1, 0.001*math.Abs(b)) }
	switch {
	case near(booked, last):
		p.record(checkTransferSeries, "OK", "booked %.2f matches the series read as cumulative over the window (%d points)", booked, len(cash))
	case near(booked, daily):
		p.record(checkTransferSeries, "OK", "booked %.2f matches the series read as daily (%d points)", booked, len(cash))
	case near(booked, last-first):
		p.record(checkTransferSeries, "OK", "booked %.2f matches the series read as cumulative since inception, first day excluded (%d points)", booked, len(cash))
	default:
		p.record(checkTransferSeries, "FAIL", "booked %.2f; series first=%.2f last=%.2f daily-sum=%.2f: a capital movement is classified differently, read the ledger shape above",
			booked, first, last, daily)
	}

	moved := false
	for i := 1; i < len(securities); i++ {
		if securities[i].Value != securities[i-1].Value {
			moved = true
		}
	}
	if moved {
		p.record("securities transfer", "WARN", "securities moved in or out during the window; the live sync books them as return")
	}
}

func (p *probe) summary() {
	// Any call above may have refreshed on a 401.
	p.persist()
	var failed, warned int
	for _, c := range p.checks {
		switch c.status {
		case "FAIL":
			failed++
		case "WARN":
			warned++
		}
	}
	fmt.Printf("\n%d checks, %d failed, %d warnings\n", len(p.checks), failed, warned)
	if p.save != "" {
		fmt.Printf("newest token pair saved to %s\n", p.save)
	}
	if failed > 0 {
		os.Exit(1)
	}
}
