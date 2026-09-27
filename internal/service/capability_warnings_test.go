package service

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
	"time"

	"github.com/trackrecord/enclave/internal/connector"
	"github.com/trackrecord/enclave/internal/repository"
	"go.uber.org/zap"
)

type warningConnector struct{ warns []string }

func (w *warningConnector) GetBalance(context.Context) (*connector.Balance, error) {
	return &connector.Balance{}, nil
}
func (w *warningConnector) GetPositions(context.Context) ([]*connector.Position, error) {
	return nil, nil
}
func (w *warningConnector) GetTrades(context.Context, time.Time, time.Time) ([]*connector.Trade, error) {
	return nil, nil
}
func (w *warningConnector) TestConnection(context.Context) error { return nil }
func (w *warningConnector) Exchange() string                     { return "okx" }
func (w *warningConnector) CapabilityWarnings() []string         { return w.warns }

// Collected before an early return and again after the last read, the same
// warning must land once, and a marker the reconstruction already recorded
// must survive.
func TestCollectCapabilityWarnings_AddsOnlyWhatIsNew(t *testing.T) {
	s := &SyncService{logger: zap.NewNop()}
	conn := &warningConnector{warns: []string{"okx_funding_balance_unreadable"}}
	meta := &repository.ExchangeConnection{UserUID: "user_synthetic", Exchange: "okx", Label: "main"}
	result := &SyncResult{CapabilityWarnings: []string{"history_reconstruction_failed"}}

	s.collectCapabilityWarnings(conn, meta, result)
	conn.warns = append(conn.warns, "okx_bills_truncated")
	s.collectCapabilityWarnings(conn, meta, result)

	want := []string{"history_reconstruction_failed", "okx_funding_balance_unreadable", "okx_bills_truncated"}
	if len(result.CapabilityWarnings) != len(want) {
		t.Fatalf("warnings = %v, want %v", result.CapabilityWarnings, want)
	}
	for i := range want {
		if result.CapabilityWarnings[i] != want[i] {
			t.Fatalf("warnings = %v, want %v", result.CapabilityWarnings, want)
		}
	}
}

// Bybit and OKX find their gaps while reading cashflows. The midnight pass used
// to collect warnings before that read, so those gaps never reached the sync
// status. Neither pipeline can be run without a database, so the order is
// pinned on the source: in both, the last collection follows GetCashflows.
func TestCapabilityWarningsAreCollectedAfterTheCashflowRead(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "sync.go", nil, 0)
	if err != nil {
		t.Fatalf("parse sync.go: %v", err)
	}
	for _, name := range []string{"syncConnection", "buildConnectionSnapshot"} {
		var cashflowsAt, lastCollectAt token.Pos
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Name.Name != name {
				continue
			}
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
					switch sel.Sel.Name {
					case "GetCashflows":
						cashflowsAt = call.Pos()
					case "collectCapabilityWarnings":
						if call.Pos() > lastCollectAt {
							lastCollectAt = call.Pos()
						}
					}
				}
				return true
			})
		}
		if cashflowsAt == token.NoPos || lastCollectAt == token.NoPos {
			t.Fatalf("%s: GetCashflows or collectCapabilityWarnings not found", name)
		}
		if lastCollectAt < cashflowsAt {
			t.Errorf("%s collects capability warnings before GetCashflows (%s): gaps found by the cashflow read are lost",
				name, fset.Position(cashflowsAt))
		}
	}
}
