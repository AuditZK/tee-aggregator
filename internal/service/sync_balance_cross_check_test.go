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

type degradedConnector struct {
	partialTradesConnector
	causes []string
}

func (c *degradedConnector) BalanceDegradations() []string { return c.causes }

func TestLogBalanceDegradations(t *testing.T) {
	core, logs := observer.New(zapcore.WarnLevel)
	s := NewSyncService(nil, nil, nil, zap.New(core))
	meta := &repository.ExchangeConnection{UserUID: "u", Exchange: "binance", Label: "main"}
	s.logBalanceDegradations(&degradedConnector{causes: []string{"COIN-M positions unreadable (HTTP 429), classic split kept"}}, meta)
	entries := logs.FilterMessage("balance read degraded").All()
	if len(entries) != 1 {
		t.Fatalf("%d degraded lines, want 1", len(entries))
	}
	fields := entries[0].ContextMap()
	if fields["exchange"] != "binance" || fields["cause"] == nil {
		t.Fatalf("fields = %v", fields)
	}
	if _, ok := fields["user_uid"]; ok {
		t.Fatal("the account is named in the line")
	}
	if _, ok := fields["label"]; ok {
		t.Fatal("the account label is named in the line")
	}
}
