package connector

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// flexStatementBlock is one <FlexStatement> kept byte for byte, with just
// enough parsed to choose between several.
type flexStatementBlock struct {
	Attrs               []xml.Attr `xml:",any,attr"`
	Inner               []byte     `xml:",innerxml"`
	EquitySummaryInBase struct {
		Rows []struct {
			ReportDate string `xml:"reportDate,attr"`
			Total      string `xml:"total,attr"`
		} `xml:"EquitySummaryByReportDateInBase"`
	} `xml:"EquitySummaryInBase"`
}

func (b *flexStatementBlock) attr(name string) string {
	for _, a := range b.Attrs {
		if a.Name.Local == name {
			return a.Value
		}
	}
	return ""
}

// latest is the most recent report date of the statement and its total.
func (b *flexStatementBlock) latest() (string, float64) {
	date, total := "", 0.0
	for _, r := range b.EquitySummaryInBase.Rows {
		if r.ReportDate >= date {
			date = r.ReportDate
			total, _ = strconv.ParseFloat(r.Total, 64)
		}
	}
	return date, total
}

// accountEquity is the total on the latest report date across an account's
// statements, so an account split by day is weighed on its last day.
func accountEquity(blocks []*flexStatementBlock) float64 {
	date, total := "", 0.0
	for _, b := range blocks {
		if d, t := b.latest(); d >= date {
			date, total = d, t
		}
	}
	return total
}

// singleFlexStatement narrows a Flex report to one account and returns how
// many accounts it carried.
//
// A query run over several accounts (an advisor or organization master with
// its sub-accounts, or two accounts ticked together) returns one
// <FlexStatement> per account, and every parser here reads a single one:
// encoding/xml folds the blocks together, appending each account's rows. The
// master, which holds no equity, saw its days skipped while its copy of every
// funding transfer was summed with the sub-account's, so each deposit landed
// twice. One connection measures one account, so the account holding the
// equity is kept: the largest balance on its latest report date, the first
// one on a tie.
//
// "Breakout by Day" also returns one <FlexStatement> per day, all for the
// same account. Narrowing those to one would keep a single day of history, so
// an account's daily statements are merged back into one (mergeDailyStatements).
// A report that cannot be read is returned as it came, for the parser to
// reject with its own error.
func singleFlexStatement(report []byte) ([]byte, int, error) {
	var flex struct {
		XMLName        xml.Name `xml:"FlexQueryResponse"`
		FlexStatements struct {
			FlexStatement []flexStatementBlock `xml:"FlexStatement"`
		} `xml:"FlexStatements"`
	}
	if err := xml.Unmarshal(report, &flex); err != nil {
		return report, 0, nil
	}
	blocks := flex.FlexStatements.FlexStatement
	if len(blocks) < 2 {
		return report, len(blocks), nil
	}

	var order []string
	byAccount := map[string][]*flexStatementBlock{}
	for n := range blocks {
		id := blocks[n].attr("accountId")
		if _, seen := byAccount[id]; !seen {
			order = append(order, id)
		}
		byAccount[id] = append(byAccount[id], &blocks[n])
	}
	keep := order[0]
	for _, id := range order[1:] {
		if accountEquity(byAccount[id]) > accountEquity(byAccount[keep]) {
			keep = id
		}
	}

	kept := byAccount[keep]
	if len(kept) == 1 {
		var b bytes.Buffer
		writeFlexOpen(&b, "FlexStatement", kept[0].Attrs)
		b.Write(kept[0].Inner)
		b.WriteString("</FlexStatement>")
		return wrapFlexStatement(b.Bytes()), len(order), nil
	}
	merged, err := mergeDailyStatements(kept)
	if err != nil {
		return nil, len(order), err
	}
	return wrapFlexStatement(merged), len(order), nil
}

func wrapFlexStatement(stmt []byte) []byte {
	var b bytes.Buffer
	b.WriteString(`<FlexQueryResponse><FlexStatements count="1">`)
	b.Write(stmt)
	b.WriteString(`</FlexStatements></FlexQueryResponse>`)
	return b.Bytes()
}

func writeFlexOpen(b *bytes.Buffer, name string, attrs []xml.Attr) {
	b.WriteString("<" + name)
	for _, a := range attrs {
		b.WriteString(" " + a.Name.Local + `="`)
		_ = xml.EscapeText(b, []byte(a.Value))
		b.WriteString(`"`)
	}
	b.WriteString(">")
}

type flexRow struct {
	XMLName xml.Name
	Attrs   []xml.Attr `xml:",any,attr"`
	Inner   []byte     `xml:",innerxml"`
}

func (r *flexRow) attr(name string) string {
	for _, a := range r.Attrs {
		if a.Name.Local == name {
			return a.Value
		}
	}
	return ""
}

// key identifies a row by its name and every attribute, in order.
func (r *flexRow) key() string {
	var b strings.Builder
	b.WriteString(r.XMLName.Local)
	for _, a := range r.Attrs {
		b.WriteString("\x00" + a.Name.Local + "=" + a.Value)
	}
	b.WriteString("\x00")
	b.Write(r.Inner)
	return b.String()
}

type flexSection struct {
	XMLName xml.Name
	Attrs   []xml.Attr `xml:",any,attr"`
	Rows    []flexRow  `xml:",any"`
}

// flexDay normalises a statement date (yyyyMMdd or yyyy-MM-dd) so dates
// compare as strings; ok is false for anything else.
func flexDay(v string) (string, bool) {
	d := strings.ReplaceAll(v, "-", "")
	if len(d) != 8 {
		return "", false
	}
	if _, err := strconv.Atoi(d); err != nil {
		return "", false
	}
	return d, true
}

// mergeDailyStatements folds one account's per-day statements into one.
//
// Each day carries its own rows, plus the previous day's NAV as an opening
// row, so NAV rows are keyed by report date and Open Positions come from the
// last day only: summed, every day's book would count once per day. A Cash
// Report row is a total over its statement's period, so it is stamped with
// the statement's dates when it does not carry them. A row repeated verbatim
// in two daily statements is the same row, kept once.
//
// The merge refuses a split it cannot read as consecutive days: statements
// without dates, or periods that overlap. Failing the sync keeps the stored
// history; guessing would rewrite it from the wrong rows.
func mergeDailyStatements(blocks []*flexStatementBlock) ([]byte, error) {
	days, err := consecutiveDays(blocks)
	if err != nil {
		return nil, err
	}

	m := flexMerge{sections: map[string]*flexSection{}, nav: map[string]flexRow{}, seen: map[string]bool{}}
	for _, d := range days {
		var body struct {
			Sections []flexSection `xml:",any"`
		}
		wrapped := append(append([]byte("<s>"), d.block.Inner...), "</s>"...)
		if err := xml.Unmarshal(wrapped, &body); err != nil {
			return nil, fmt.Errorf("merge daily flex statements: read %s: %w", d.from, err)
		}
		m.addDay(d, body.Sections)
	}
	m.settleNAV()

	last := days[len(days)-1]
	attrs := make([]xml.Attr, 0, len(last.block.Attrs))
	for _, a := range last.block.Attrs {
		if a.Name.Local == "fromDate" {
			a.Value = days[0].block.attr("fromDate")
		}
		attrs = append(attrs, a)
	}

	var b bytes.Buffer
	writeFlexOpen(&b, "FlexStatement", attrs)
	m.write(&b)
	b.WriteString("</FlexStatement>")
	return b.Bytes(), nil
}

type datedStatement struct {
	block    *flexStatementBlock
	from, to string
}

// consecutiveDays orders the statements by period and refuses any without a
// readable one, or two whose periods overlap.
func consecutiveDays(blocks []*flexStatementBlock) ([]datedStatement, error) {
	days := make([]datedStatement, 0, len(blocks))
	for _, b := range blocks {
		from, okFrom := flexDay(b.attr("fromDate"))
		to, okTo := flexDay(b.attr("toDate"))
		if !okFrom || !okTo || from > to {
			return nil, fmt.Errorf("merge daily flex statements: a statement has no readable period")
		}
		days = append(days, datedStatement{b, from, to})
	}
	sort.SliceStable(days, func(a, b int) bool { return days[a].from < days[b].from })
	for n := 1; n < len(days); n++ {
		if days[n].from <= days[n-1].to {
			return nil, fmt.Errorf("merge daily flex statements: periods %s-%s and %s-%s overlap",
				days[n-1].from, days[n-1].to, days[n].from, days[n].to)
		}
	}
	return days, nil
}

type flexMerge struct {
	order    []string
	sections map[string]*flexSection
	nav      map[string]flexRow
	seen     map[string]bool
}

func (m *flexMerge) addDay(d datedStatement, sections []flexSection) {
	dayKeys := map[string]bool{}
	for _, sec := range sections {
		name := sec.XMLName.Local
		out, ok := m.sections[name]
		if !ok {
			out = &flexSection{XMLName: sec.XMLName, Attrs: sec.Attrs}
			m.sections[name] = out
			m.order = append(m.order, name)
		}
		switch name {
		case "EquitySummaryInBase":
			for _, r := range sec.Rows {
				m.nav[r.attr("reportDate")] = r
			}
		case "OpenPositions":
			out.Rows = sec.Rows
		default:
			m.addRows(d, out, sec.Rows, dayKeys)
		}
	}
}

func (m *flexMerge) addRows(d datedStatement, out *flexSection, rows []flexRow, dayKeys map[string]bool) {
	name := out.XMLName.Local
	for _, r := range rows {
		if name == "CashReport" && r.attr("fromDate") == "" && r.attr("toDate") == "" {
			r.Attrs = append(r.Attrs,
				xml.Attr{Name: xml.Name{Local: "fromDate"}, Value: d.from},
				xml.Attr{Name: xml.Name{Local: "toDate"}, Value: d.to})
		}
		k := name + "\x01" + r.key()
		if m.seen[k] && !dayKeys[k] {
			continue
		}
		m.seen[k], dayKeys[k] = true, true
		out.Rows = append(out.Rows, r)
	}
}

// settleNAV writes the NAV rows back once each, in report-date order: the
// live balance reads the last row as the latest.
func (m *flexMerge) settleNAV() {
	nav, ok := m.sections["EquitySummaryInBase"]
	if !ok {
		return
	}
	dates := make([]string, 0, len(m.nav))
	for date := range m.nav {
		dates = append(dates, date)
	}
	sort.Strings(dates)
	for _, date := range dates {
		nav.Rows = append(nav.Rows, m.nav[date])
	}
}

func (m *flexMerge) write(b *bytes.Buffer) {
	for _, name := range m.order {
		sec := m.sections[name]
		writeFlexOpen(b, name, sec.Attrs)
		for _, r := range sec.Rows {
			writeFlexOpen(b, r.XMLName.Local, r.Attrs)
			b.Write(r.Inner)
			b.WriteString("</" + r.XMLName.Local + ">")
		}
		b.WriteString("</" + name + ">")
	}
}

// flexCashReportRow is one Cash Report line: a currency's totals over the
// period between its dates.
type flexCashReportRow struct {
	Currency          string `xml:"currency,attr"`
	FromDate          string `xml:"fromDate,attr"`
	ToDate            string `xml:"toDate,attr"`
	InternalTransfers string `xml:"internalTransfers,attr"`
	AccountTransfers  string `xml:"accountTransfers,attr"`
}

// cashReportTransfers is, per day, the cash a Cash Report broken out by day
// shows moving in (+) or out (-) by transfer.
type cashReportTransfers map[string]float64

// dailyCashReportTransfers reads the base-currency summary rows that cover a
// single day. Internal transfers include the sweeps between an account's own
// segments (an IB-UKL "F" segment), which reach neither Cash Transactions nor
// the Transfers section: without them the cash a sweep brings in reads as
// performance. A row over several days cannot be placed on one, and is left
// out rather than guessed.
func dailyCashReportTransfers(rows []flexCashReportRow) cashReportTransfers {
	out := cashReportTransfers{}
	for _, r := range rows {
		if r.Currency != "BASE_SUMMARY" {
			continue
		}
		from, okFrom := flexDay(r.FromDate)
		to, okTo := flexDay(r.ToDate)
		if !okFrom || !okTo || from != to {
			continue
		}
		internal, _ := strconv.ParseFloat(r.InternalTransfers, 64)
		account, _ := strconv.ParseFloat(r.AccountTransfers, 64)
		out[from] += internal + account
	}
	return out
}

// flows states the transfers as cash flows in the account's base currency.
func (c cashReportTransfers) flows(since time.Time) []*Cashflow {
	days := make([]string, 0, len(c))
	for day, amount := range c {
		if amount != 0 {
			days = append(days, day)
		}
	}
	sort.Strings(days)
	var out []*Cashflow
	for _, day := range days {
		ts, err := time.Parse("20060102", day)
		if err != nil || ts.Before(since) {
			continue
		}
		out = append(out, &Cashflow{Amount: c[day], Currency: "BASE_SUMMARY", Timestamp: ts})
	}
	return out
}

// statementReport is the Flex report every figure is read from: the cached
// document narrowed to the one account this connection measures. The raw
// views (GetRawStatement, GetRawCashflowEntries) keep the whole document.
func (i *IBKR) statementReport(ctx context.Context) ([]byte, error) {
	report, err := i.fetchFlexReport(ctx)
	if err != nil {
		return nil, err
	}
	single, n, err := singleFlexStatement(report)
	if err != nil {
		return nil, err
	}
	i.statementWarning = ""
	if n > 1 {
		i.statementWarning = fmt.Sprintf("ibkr_multi_account_statement(x%d)", n)
	}
	return single, nil
}
