// Read-only probe that answers the questions IG's published docs do not, by
// reading a real account's ledger. It writes no data and touches no enclave.
//
// Three facts gate the IG history reconstruction, and none can be settled from
// the documentation (labs.ig.com refuses non-browser clients):
//
//  1. Does the transaction `size` carry a sign? The connector derives buy/sell
//     from it. If sizes come back unsigned, every trade reads as a buy and the
//     long/short split is wrong.
//  2. What shape is `profitAndLoss`? The parser strips non-numeric characters
//     to tolerate the account's currency symbol; this confirms it holds.
//  3. Is the ledger complete? Unlike cTrader, IG carries no balance-after on a
//     transaction, so a reconstruction can only walk back from today's balance
//     — which is sound only if every line that moves cash is in the ledger.
//     The probe walks it and reports the residual: over an account's whole
//     life the implied opening balance must land on ~0. Anything else means a
//     balance-moving line is missing and a walk-back would drift silently.
//
// Usage:
//
//	IG_API_KEY=... IG_IDENTIFIER=... IG_PASSWORD=... go run ./cmd/ig-probe -days 365
//
// Defaults to IG's demo host. Set IG_DEMO=0 to point it at a live account.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/trackrecord/enclave/internal/connector"
)

func main() {
	days := flag.Int("days", 365, "how far back to read the ledger")
	samples := flag.Int("samples", 3, "raw samples to print per transaction type")
	flag.Parse()

	apiKey := os.Getenv("IG_API_KEY")
	identifier := os.Getenv("IG_IDENTIFIER")
	password := os.Getenv("IG_PASSWORD")
	if apiKey == "" || identifier == "" || password == "" {
		fmt.Fprintln(os.Stderr, "set IG_API_KEY, IG_IDENTIFIER and IG_PASSWORD")
		os.Exit(2)
	}
	demo := os.Getenv("IG_DEMO") != "0"

	ig := connector.NewIG(&connector.Credentials{
		APIKey:     apiKey,
		APISecret:  password,
		Passphrase: identifier,
	}, demo)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	env := "demo"
	if !demo {
		env = "LIVE"
	}
	fmt.Printf("IG probe — %s, %d day window\n\n", env, *days)

	if err := run(ctx, ig, *days, *samples); err != nil {
		fmt.Fprintf(os.Stderr, "\nprobe failed: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, ig *connector.IG, days, samples int) error {
	bal, err := ig.GetBalance(ctx)
	if err != nil {
		return fmt.Errorf("get balance: %w", err)
	}
	// IG reports settled cash and open P&L separately; the connector sums them
	// into Equity, and the walk-back reconciles against the settled figure.
	settled := bal.Equity - bal.UnrealizedPnL
	fmt.Printf("balance    equity=%.2f settled=%.2f unrealized=%.2f available=%.2f %s\n\n",
		bal.Equity, settled, bal.UnrealizedPnL, bal.Available, bal.Currency)

	until := time.Now().UTC()
	since := until.AddDate(0, 0, -days)
	txs, err := ig.RawTransactions(ctx, since, until)
	if err != nil {
		return fmt.Errorf("read ledger: %w", err)
	}
	if len(txs) == 0 {
		fmt.Println("ledger is empty over this window — trade the demo account, or widen -days")
		return nil
	}

	reportLedgerSpan(txs, since)
	reportTypes(txs, samples)
	reportSizeSigns(txs)
	reportReconciliation(txs, settled)
	return reportConnectorView(ctx, ig, since, until)
}

func reportLedgerSpan(txs []connector.IGRawTransaction, since time.Time) {
	earliest, latest := "", ""
	for _, t := range txs {
		if earliest == "" || t.DateUTC < earliest {
			earliest = t.DateUTC
		}
		if t.DateUTC > latest {
			latest = t.DateUTC
		}
	}
	fmt.Printf("ledger     %d transactions, %s → %s\n", len(txs), earliest, latest)

	// If the oldest line sits well inside the window, the window very likely
	// reaches back past the account's first ever transaction — which is what
	// makes the reconciliation residual below meaningful.
	if earliest > since.Add(48*time.Hour).Format("2006-01-02T15:04:05") {
		fmt.Printf("           window starts %s, before the oldest line — it likely covers the account's whole life\n\n",
			since.Format("2006-01-02"))
	} else {
		fmt.Printf("           oldest line sits at the window edge — widen -days or the residual below means nothing\n\n")
	}
}

type typeStat struct {
	name    string
	count   int
	cash    int
	samples []connector.IGRawTransaction
}

func reportTypes(txs []connector.IGRawTransaction, samples int) {
	stats := map[string]*typeStat{}
	for _, t := range txs {
		key := t.TransactionType
		s := stats[key]
		if s == nil {
			s = &typeStat{name: key}
			stats[key] = s
		}
		s.count++
		if t.CashTransaction {
			s.cash++
		}
		if len(s.samples) < samples {
			s.samples = append(s.samples, t)
		}
	}

	ordered := make([]*typeStat, 0, len(stats))
	for _, s := range stats {
		ordered = append(ordered, s)
	}
	sort.Slice(ordered, func(a, b int) bool { return ordered[a].count > ordered[b].count })

	fmt.Println("transactionType values actually present:")
	for _, s := range ordered {
		known := ""
		if _, ok := map[string]bool{"DEPO": true, "CASHIN": true, "WITH": true}[strings.ToUpper(s.name)]; ok {
			known = "  [classified as a capital flow]"
		} else if s.cash > 0 {
			known = "  [cash line the connector does NOT classify — check if it moves capital]"
		}
		fmt.Printf("  %-24s %4d  (cashTransaction=true on %d)%s\n", s.name, s.count, s.cash, known)
		for _, x := range s.samples {
			fmt.Printf("       size=%-8q pnl=%-12q open=%-10q close=%-10q %s\n",
				x.Size, x.ProfitAndLoss, x.OpenLevel, x.CloseLevel, x.InstrumentName)
		}
	}
	fmt.Println()
}

// The connector reads buy/sell off the sign of size. If IG returns sizes
// unsigned, that inference is wrong for every trade.
func reportSizeSigns(txs []connector.IGRawTransaction) {
	var signed, unsigned, empty int
	for _, t := range txs {
		if t.CashTransaction {
			continue
		}
		s := strings.TrimSpace(t.Size)
		switch {
		case s == "":
			empty++
		case strings.HasPrefix(s, "+"), strings.HasPrefix(s, "-"):
			signed++
		default:
			unsigned++
		}
	}

	fmt.Printf("deal size signs   signed=%d unsigned=%d empty=%d\n", signed, unsigned, empty)
	switch {
	case signed > 0 && unsigned == 0:
		fmt.Println("                  → sign is present: the buy/sell inference holds")
	case signed == 0 && unsigned > 0:
		fmt.Println("                  → NO sign anywhere: the buy/sell inference is WRONG, direction must come from /history/activity")
	case signed > 0 && unsigned > 0:
		fmt.Println("                  → mixed: unsigned lines are silently read as buys, needs a closer look")
	}
	fmt.Println()
}

// A walk-back is only sound if the ledger accounts for every move of the cash
// balance. Subtracting each line's reported P&L from today's settled balance
// must land on ~0 at the account's inception; a non-zero residual is the size
// of what the ledger does not explain.
func reportReconciliation(txs []connector.IGRawTransaction, settled float64) {
	var sum float64
	var parsed, failed int
	for _, t := range txs {
		v, err := connector.ParseIGDecimal(t.ProfitAndLoss)
		if err != nil {
			failed++
			continue
		}
		sum += v
		parsed++
	}

	residual := settled - sum
	fmt.Println("ledger completeness (the reconstruction gate):")
	fmt.Printf("  settled balance now        %12.2f\n", settled)
	fmt.Printf("  sum of every line's P&L    %12.2f   (%d parsed, %d unparseable)\n", sum, parsed, failed)
	fmt.Printf("  implied opening balance    %12.2f\n", residual)
	if failed > 0 {
		fmt.Printf("  → %d lines would not parse; fix that before trusting the residual\n", failed)
	}
	fmt.Println("  → over an account's whole life this should be ~0.")
	fmt.Println("    A material residual means some balance-moving line is absent from the")
	fmt.Println("    ledger, and a walk-back reconstruction would drift by that much.")
	fmt.Println()
}

func reportConnectorView(ctx context.Context, ig *connector.IG, since, until time.Time) error {
	trades, err := ig.GetTrades(ctx, since, until)
	if err != nil {
		return fmt.Errorf("get trades: %w", err)
	}
	var buys, sells int
	for _, t := range trades {
		if t.Side == "buy" {
			buys++
		} else {
			sells++
		}
	}

	flows, err := ig.GetCashflows(ctx, since)
	if err != nil {
		return fmt.Errorf("get cashflows: %w", err)
	}
	var deposits, withdrawals float64
	for _, f := range flows {
		if f.Amount > 0 {
			deposits += f.Amount
		} else {
			withdrawals += f.Amount
		}
	}

	positions, err := ig.GetPositions(ctx)
	if err != nil {
		return fmt.Errorf("get positions: %w", err)
	}

	fmt.Println("connector view of the same window:")
	fmt.Printf("  trades     %d  (buy=%d sell=%d)\n", len(trades), buys, sells)
	fmt.Printf("  cashflows  %d  (deposits=%.2f withdrawals=%.2f)\n", len(flows), deposits, withdrawals)
	fmt.Printf("  positions  %d open\n", len(positions))
	return nil
}
