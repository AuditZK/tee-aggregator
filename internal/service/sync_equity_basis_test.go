package service

import (
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/trackrecord/enclave/internal/connector"
	"github.com/trackrecord/enclave/internal/repository"
)

type equityBasisConnector struct {
	partialTradesConnector
	note connector.EquityBasisNote
	ok   bool
}

func (c *equityBasisConnector) EquityBasisNote() (connector.EquityBasisNote, bool) {
	return c.note, c.ok
}

func TestLogEquityBasis(t *testing.T) {
	core, logs := observer.New(zapcore.InfoLevel)
	s := NewSyncService(nil, nil, nil, zap.New(core))
	meta := &repository.ExchangeConnection{UserUID: "u", Exchange: "binance", Label: "main"}

	s.logEquityBasis(&equityBasisConnector{}, meta)
	s.logEquityBasis(&partialTradesConnector{}, meta)
	if logs.Len() != 0 {
		t.Fatalf("%d log lines without a note, want 0", logs.Len())
	}

	s.logEquityBasis(&equityBasisConnector{
		note: connector.EquityBasisNote{Name: "binance_pm_market_value", Basis: connector.EquityBasisMarketValue, Residual: 3.2, ResidualShare: 0.00064},
		ok:   true,
	}, meta)
	s.logEquityBasis(&equityBasisConnector{
		note: connector.EquityBasisNote{Name: "binance_pm_market_value", Basis: connector.EquityBasisVenue, Fallback: "2 assets without a spot price"},
		ok:   true,
	}, meta)
	entries := logs.FilterMessage("equity basis").All()
	if len(entries) != 2 {
		t.Fatalf("%d equity basis lines, want 2", len(entries))
	}
	for _, e := range entries {
		if e.Level != zapcore.InfoLevel {
			t.Fatalf("level = %v, want info", e.Level)
		}
		fields := e.ContextMap()
		for _, k := range []string{"user_uid", "label", "measured", "reference"} {
			if _, ok := fields[k]; ok {
				t.Fatalf("field %s in the line: %v", k, fields)
			}
		}
	}
	first, second := entries[0].ContextMap(), entries[1].ContextMap()
	if first["residual_usd"] != 3.2 || first["residual_share"] != 0.00064 || first["basis"] != "market_value" || first["exchange"] != "binance" {
		t.Fatalf("fields = %v", first)
	}
	if second["basis"] != "venue_equity" || second["fallback"] != "2 assets without a spot price" {
		t.Fatalf("fields = %v", second)
	}
}
