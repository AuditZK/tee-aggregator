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

// narrowedStatement is a Flex report cut down to what one connection
// measures: accounts names the accounts it covers, carried how many the
// report held.
type narrowedStatement struct {
	report   []byte
	accounts []string
	carried  int
}

// singleFlexStatement narrows a Flex report to one account and returns how
// many accounts it carried.
func singleFlexStatement(report []byte) ([]byte, int, error) {
	n, err := narrowFlexStatement(report)
	return n.report, n.carried, err
}

// narrowFlexStatement narrows a Flex report to the one account a connection
// measures.
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
// An account and its IB-UKL segment (U1234567 and U1234567F) are one account
// to the holder and to PortfolioAnalyst: money sweeps between them daily. Kept
// alone, either one reads every sweep as a deposit or a withdrawal and misses
// what the other half earned, so the pair is consolidated
// (consolidateSegments) and weighed as one.
//
// "Breakout by Day" also returns one <FlexStatement> per day, all for the
// same account. Narrowing those to one would keep a single day of history, so
// an account's daily statements are merged back into one (mergeDailyStatements).
// A report that cannot be read is returned as it came, for the parser to
// reject with its own error.
func narrowFlexStatement(report []byte) (narrowedStatement, error) {
	var flex struct {
		XMLName        xml.Name `xml:"FlexQueryResponse"`
		FlexStatements struct {
			FlexStatement []flexStatementBlock `xml:"FlexStatement"`
		} `xml:"FlexStatements"`
	}
	if err := xml.Unmarshal(report, &flex); err != nil {
		return narrowedStatement{report: report}, nil
	}
	blocks := flex.FlexStatements.FlexStatement
	if len(blocks) == 0 {
		return narrowedStatement{report: report}, nil
	}
	if len(blocks) == 1 {
		return narrowedStatement{report: report, accounts: namedAccounts([]string{blocks[0].attr("accountId")}), carried: 1}, nil
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
	groups := segmentGroups(order)
	groupEquity := func(g []string) float64 {
		total := 0.0
		for _, id := range g {
			total += accountEquity(byAccount[id])
		}
		return total
	}
	keep := groups[0]
	for _, g := range groups[1:] {
		if groupEquity(g) > groupEquity(keep) {
			keep = g
		}
	}

	stmts := make([][]byte, 0, len(keep))
	for _, id := range keep {
		stmt, err := accountStatement(byAccount[id])
		if err != nil {
			return narrowedStatement{carried: len(order)}, err
		}
		stmts = append(stmts, stmt)
	}
	stmt := stmts[0]
	if len(stmts) > 1 {
		var err error
		if stmt, err = consolidateSegments(stmts); err != nil {
			return narrowedStatement{carried: len(order)}, err
		}
	}
	return narrowedStatement{report: wrapFlexStatement(stmt), accounts: namedAccounts(keep), carried: len(order)}, nil
}

// namedAccounts drops statements that name no account: nothing can be pinned
// to them.
func namedAccounts(ids []string) []string {
	var out []string
	for _, id := range ids {
		if strings.TrimSpace(id) != "" {
			out = append(out, id)
		}
	}
	return out
}

// segmentGroups groups the report's accounts into what a connection can
// measure: an account with its IB-UKL segment (the same id plus "F"), every
// other account alone. Each group lists its base account first.
func segmentGroups(order []string) [][]string {
	present := make(map[string]bool, len(order))
	for _, id := range order {
		present[strings.ToUpper(id)] = true
	}
	segmentOf := func(id string) (string, bool) {
		up := strings.ToUpper(id)
		if len(up) < 2 || !strings.HasSuffix(up, "F") {
			return "", false
		}
		base := up[:len(up)-1]
		return base, present[base]
	}
	var groups [][]string
	index := map[string]int{}
	for _, id := range order {
		if _, ok := segmentOf(id); ok {
			continue
		}
		index[strings.ToUpper(id)] = len(groups)
		groups = append(groups, []string{id})
	}
	for _, id := range order {
		if base, ok := segmentOf(id); ok {
			groups[index[base]] = append(groups[index[base]], id)
		}
	}
	return groups
}

// accountStatement is one account's statement, its daily ones merged.
func accountStatement(blocks []*flexStatementBlock) ([]byte, error) {
	if len(blocks) > 1 {
		return mergeDailyStatements(blocks)
	}
	var b bytes.Buffer
	writeFlexOpen(&b, "FlexStatement", blocks[0].Attrs)
	b.Write(blocks[0].Inner)
	b.WriteString("</FlexStatement>")
	return b.Bytes(), nil
}

// segmentRowSections hold one row per event: the consolidated statement
// carries both segments' rows. A sweep between the segments shows in each
// one's Cash Report, out of one and into the other on the same day, so the
// pair's sum nets it to nothing.
var segmentRowSections = map[string]bool{
	"Trades":           true,
	"OpenPositions":    true,
	"CashTransactions": true,
	"Transfers":        true,
	"CashReport":       true,
}

type flexStatementDoc struct {
	Attrs    []xml.Attr    `xml:",any,attr"`
	Sections []flexSection `xml:",any"`
}

func (d *flexStatementDoc) attr(name string) string {
	for _, a := range d.Attrs {
		if a.Name.Local == name {
			return a.Value
		}
	}
	return ""
}

// consolidateSegments folds an account's statement and its segment's into one
// statement under the base account: NAV summed day by day, every event row
// kept, every other section (rates, account information) taken from the base.
func consolidateSegments(stmts [][]byte) ([]byte, error) {
	docs := make([]flexStatementDoc, len(stmts))
	for n, s := range stmts {
		if err := xml.Unmarshal(s, &docs[n]); err != nil {
			return nil, fmt.Errorf("consolidate segment statements: read statement: %w", err)
		}
	}

	var order []string
	byName := map[string][]*flexSection{}
	for n := range docs {
		for k := range docs[n].Sections {
			name := docs[n].Sections[k].XMLName.Local
			if _, seen := byName[name]; !seen {
				order = append(order, name)
			}
			byName[name] = append(byName[name], &docs[n].Sections[k])
		}
	}

	from, to := "", ""
	for n := range docs {
		if f, ok := flexDay(docs[n].attr("fromDate")); ok && (from == "" || f < from) {
			from = f
		}
		if t, ok := flexDay(docs[n].attr("toDate")); ok && t > to {
			to = t
		}
	}
	attrs := make([]xml.Attr, 0, len(docs[0].Attrs))
	for _, a := range docs[0].Attrs {
		switch {
		case a.Name.Local == "fromDate" && from != "":
			a.Value = from
		case a.Name.Local == "toDate" && to != "":
			a.Value = to
		}
		attrs = append(attrs, a)
	}

	var b bytes.Buffer
	writeFlexOpen(&b, "FlexStatement", attrs)
	for _, name := range order {
		secs := byName[name]
		out := flexSection{XMLName: secs[0].XMLName, Attrs: secs[0].Attrs}
		switch {
		case name == "EquitySummaryInBase":
			rows, err := sumSegmentNAV(secs)
			if err != nil {
				return nil, err
			}
			out.Rows = rows
		case segmentRowSections[name]:
			for _, s := range secs {
				out.Rows = append(out.Rows, s.Rows...)
			}
		default:
			out.Rows = secs[0].Rows
		}
		writeFlexOpen(&b, name, out.Attrs)
		for _, r := range out.Rows {
			writeFlexOpen(&b, r.XMLName.Local, r.Attrs)
			b.Write(r.Inner)
			b.WriteString("</" + r.XMLName.Local + ">")
		}
		b.WriteString("</" + name + ">")
	}
	b.WriteString("</FlexStatement>")
	return b.Bytes(), nil
}

// navTextAttrs are the NAV row attributes that are not amounts.
var navTextAttrs = map[string]bool{
	"reportDate": true, "currency": true, "accountId": true, "acctAlias": true, "model": true,
}

// sumSegmentNAV adds the segments' NAV rows date by date. A segment without a
// row on a date counts its last known one, so a day it skips does not read as
// its balance leaving; before its first row it counts nothing.
func sumSegmentNAV(secs []*flexSection) ([]flexRow, error) {
	sides := make([][]flexRow, len(secs))
	dateSet := map[string]bool{}
	for n, s := range secs {
		for _, r := range s.Rows {
			if d := r.attr("reportDate"); d != "" {
				sides[n] = append(sides[n], r)
				dateSet[d] = true
			}
		}
		sort.SliceStable(sides[n], func(a, b int) bool {
			return sides[n][a].attr("reportDate") < sides[n][b].attr("reportDate")
		})
	}
	dates := make([]string, 0, len(dateSet))
	for d := range dateSet {
		dates = append(dates, d)
	}
	sort.Strings(dates)

	next := make([]int, len(sides))
	carried := make([]*flexRow, len(sides))
	out := make([]flexRow, 0, len(dates))
	for _, date := range dates {
		var rows []*flexRow
		for n := range sides {
			for next[n] < len(sides[n]) && sides[n][next[n]].attr("reportDate") <= date {
				carried[n] = &sides[n][next[n]]
				next[n]++
			}
			if carried[n] != nil {
				rows = append(rows, carried[n])
			}
		}
		row, err := sumNAVRows(date, rows)
		if err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, nil
}

// sumNAVRows adds the amounts of one date's rows and keeps the first row's
// text. Segments in two base currencies cannot be added.
func sumNAVRows(date string, rows []*flexRow) (flexRow, error) {
	var names []string
	first := map[string]string{}
	sums := map[string]float64{}
	text := map[string]bool{}
	for _, r := range rows {
		for _, a := range r.Attrs {
			name := a.Name.Local
			if _, seen := first[name]; !seen {
				names = append(names, name)
				first[name] = a.Value
				text[name] = navTextAttrs[name]
			}
			if name == "currency" && a.Value != "" && first[name] != "" && a.Value != first[name] {
				return flexRow{}, fmt.Errorf("consolidate segment statements: NAV in %s and %s on %s", first[name], a.Value, date)
			}
			if text[name] || a.Value == "" {
				continue
			}
			v, err := strconv.ParseFloat(a.Value, 64)
			if err != nil {
				text[name] = true
				continue
			}
			sums[name] += v
		}
	}
	row := flexRow{XMLName: rows[0].XMLName}
	for _, name := range names {
		value := first[name]
		switch {
		case name == "reportDate":
			value = date
		case !text[name]:
			value = strconv.FormatFloat(sums[name], 'f', -1, 64)
		}
		row.Attrs = append(row.Attrs, xml.Attr{Name: xml.Name{Local: name}, Value: value})
	}
	return row, nil
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
	// nil when the query has no such column: the day's deposits then come
	// from Cash Transactions.
	DepositWithdrawals *string `xml:"depositWithdrawals,attr"`
}

// cashReportTransfers is, per day, the cash a Cash Report broken out by day
// shows moving in (+) or out (-) by transfer, and by deposit when
// datesDeposits.
type cashReportTransfers struct {
	days          map[string]float64
	datesDeposits bool
}

// dailyCashReportTransfers reads the base-currency summary rows that cover a
// single day. Internal transfers include the sweeps between an account's own
// segments (an IB-UKL "F" segment), which reach neither Cash Transactions nor
// the Transfers section: without them the cash a sweep brings in reads as
// performance. A row over several days cannot be placed on one, and is left
// out rather than guessed.
//
// Deposits are read here too when the rows carry them. A Cash Transaction is
// dated when the wire was initiated, up to a week before the cash reaches the
// account, and a deposit advance cancelled on arrival is backdated the same
// way; the day's Cash Report books each one on the day it moved the equity.
func dailyCashReportTransfers(rows []flexCashReportRow) cashReportTransfers {
	out := cashReportTransfers{days: map[string]float64{}}
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
		out.days[from] += internal + account
		if r.DepositWithdrawals != nil {
			deposits, _ := strconv.ParseFloat(*r.DepositWithdrawals, 64)
			out.days[from] += deposits
			out.datesDeposits = true
		}
	}
	return out
}

func (c cashReportTransfers) covers(day string) bool {
	_, ok := c.days[day]
	return ok
}

// flows states the transfers as cash flows in the account's base currency.
func (c cashReportTransfers) flows(since time.Time) []*Cashflow {
	days := make([]string, 0, len(c.days))
	for day, amount := range c.days {
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
		out = append(out, &Cashflow{Amount: c.days[day], Currency: "BASE_SUMMARY", Timestamp: ts})
	}
	return out
}

// statementReport is the Flex report every figure is read from: the daily
// statement reaching back to from, narrowed to the one account this connection
// measures. The raw views (GetRawStatement, GetRawCashflowEntries) keep the
// whole document.
func (i *IBKR) statementReport(ctx context.Context, from time.Time) ([]byte, error) {
	report, err := i.dailyStatement(ctx, from)
	if err != nil {
		return nil, err
	}
	n, err := i.narrow(report, false)
	if err != nil {
		return nil, err
	}
	i.measuredAccounts = n.accounts
	i.statementWarning = ""
	if n.carried > len(n.accounts) {
		i.statementWarning = fmt.Sprintf("ibkr_multi_account_statement(x%d)", n.carried)
	}
	return n.report, nil
}

// narrow cuts a report down to the accounts this connection measures and
// hands them to the account guard, which refuses a report that measures
// other accounts than the stored history.
func (i *IBKR) narrow(report []byte, past bool) (narrowedStatement, error) {
	n, err := narrowFlexStatement(report)
	if err != nil {
		return narrowedStatement{}, err
	}
	if i.accountGuard != nil && len(n.accounts) > 0 {
		if err := i.accountGuard(n.accounts, past); err != nil {
			return narrowedStatement{}, err
		}
	}
	return n, nil
}

// SetAccountGuard implements AccountScoped.
func (i *IBKR) SetAccountGuard(guard func(accounts []string, past bool) error) {
	i.accountGuard = guard
}

// MeasuredAccounts implements AccountScoped: the accounts the last daily
// statement was narrowed to.
func (i *IBKR) MeasuredAccounts() []string {
	return append([]string(nil), i.measuredAccounts...)
}
