package service

import (
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/trackrecord/enclave/internal/connector"
	"github.com/trackrecord/enclave/internal/repository"
)

type crossCheckConnector struct {
	partialTradesConnector
	check connector.BalanceCrossCheck
	ok    bool
}

func (c *crossCheckConnector) BalanceCrossCheck() (connector.BalanceCrossCheck, bool) {
	return c.check, c.ok
}

func TestLogBalanceCrossCheck(t *testing.T) {
	core, logs := observer.New(zapcore.InfoLevel)
	s := NewSyncService(nil, nil, nil, zap.New(core))
	meta := &repository.ExchangeConnection{UserUID: "u", Exchange: "binance", Label: "main"}

	s.logBalanceCrossCheck(&crossCheckConnector{}, meta)
	s.logBalanceCrossCheck(&partialTradesConnector{}, meta)
	if logs.Len() != 0 {
		t.Fatalf("%d log lines without a cross-check, want 0", logs.Len())
	}

	s.logBalanceCrossCheck(&crossCheckConnector{
		check: connector.BalanceCrossCheck{Name: "binance_pm_pro_actual_equity", Measured: 240700, Reference: 240600},
		ok:    true,
	}, meta)
	entries := logs.FilterMessage("balance cross-check").All()
	if len(entries) != 1 {
		t.Fatalf("%d cross-check lines, want 1", len(entries))
	}
	fields := entries[0].ContextMap()
	if fields["residual"] != 100.0 || fields["check"] != "binance_pm_pro_actual_equity" {
		t.Fatalf("fields = %v", fields)
	}
}
