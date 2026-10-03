package connector

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"strconv"
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

// latestEquity is the total on the statement's most recent report date.
func (b *flexStatementBlock) latestEquity() float64 {
	date, total := "", 0.0
	for _, r := range b.EquitySummaryInBase.Rows {
		if r.ReportDate >= date {
			date = r.ReportDate
			total, _ = strconv.ParseFloat(r.Total, 64)
		}
	}
	return total
}

// singleFlexStatement narrows a Flex report to one account and returns how
// many statements it carried.
//
// A query run over several accounts (an advisor or organization master with
// its sub-accounts, or two accounts ticked together) returns one
// <FlexStatement> per account, and every parser here reads a single one:
// encoding/xml folds the blocks together, appending each account's rows. The
// master, which holds no equity, saw its days skipped while its copy of every
// funding transfer was summed with the sub-account's, so each deposit landed
// twice. One connection measures one account, so the statement holding the
// equity is kept: the largest balance on its latest report date, the first
// one on a tie. A report that cannot be read is returned as it came, for the
// parser to reject with its own error.
func singleFlexStatement(report []byte) ([]byte, int) {
	var flex struct {
		XMLName        xml.Name `xml:"FlexQueryResponse"`
		FlexStatements struct {
			FlexStatement []flexStatementBlock `xml:"FlexStatement"`
		} `xml:"FlexStatements"`
	}
	if err := xml.Unmarshal(report, &flex); err != nil {
		return report, 0
	}
	blocks := flex.FlexStatements.FlexStatement
	if len(blocks) < 2 {
		return report, len(blocks)
	}

	keep := 0
	for n := 1; n < len(blocks); n++ {
		if blocks[n].latestEquity() > blocks[keep].latestEquity() {
			keep = n
		}
	}

	var b bytes.Buffer
	b.WriteString(`<FlexQueryResponse><FlexStatements count="1"><FlexStatement`)
	for _, a := range blocks[keep].Attrs {
		b.WriteString(" " + a.Name.Local + `="`)
		_ = xml.EscapeText(&b, []byte(a.Value))
		b.WriteString(`"`)
	}
	b.WriteString(">")
	b.Write(blocks[keep].Inner)
	b.WriteString(`</FlexStatement></FlexStatements></FlexQueryResponse>`)
	return b.Bytes(), len(blocks)
}

// statementReport is the Flex report every figure is read from: the cached
// document narrowed to the one account this connection measures. The raw
// views (GetRawStatement, GetRawCashflowEntries) keep the whole document.
func (i *IBKR) statementReport(ctx context.Context) ([]byte, error) {
	report, err := i.fetchFlexReport(ctx)
	if err != nil {
		return nil, err
	}
	single, n := singleFlexStatement(report)
	i.statementWarning = ""
	if n > 1 {
		i.statementWarning = fmt.Sprintf("ibkr_multi_account_statement(x%d)", n)
	}
	return single, nil
}
