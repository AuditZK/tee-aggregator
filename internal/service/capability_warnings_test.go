package service

import (
	"context"
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
