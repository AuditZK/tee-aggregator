package connector

import (
	"context"
	"testing"
	"time"
)

const (
	okxAssetBalancesPath = "/api/v5/asset/balances"
	okxAssetBillsPath    = "/api/v5/asset/bills"
	okxAssetHistoryPath  = "/api/v5/asset/bills-history"
)

// The probe exists to learn a ledger whose published type tables disagree, so
// a field this code has never heard of must still reach the operator. A struct
// with known fields would drop exactly the column the question turns on.
func TestOKXProbeFunding_KeepsFieldsThisCodeDoesNotKnow(t *testing.T) {
	s := newOKXBillsServer(t, map[string]string{
		okxAssetBalancesPath: `{"code":"0","data":[{"ccy":"BTC","bal":"0.0454","frozenBal":"0","availBal":"0.0454"}]}`,
		okxAssetBillsPath: `{"code":"0","data":[` +
			`{"billId":"f1","ts":"1788349784777","ccy":"BTC","balChg":"0.0454","type":"130","subType":"","fee":"0","clientId":"","notes":"from trading"}` +
			`]}`,
	}, nil)

	probe, err := s.connector().ProbeFunding(context.Background(), time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("ProbeFunding: %v", err)
	}
	if len(probe.Bills) != 1 {
		t.Fatalf("got %d bills, want 1: %+v", len(probe.Bills), probe.Bills)
	}

	bill := probe.Bills[0]
	for _, key := range []string{"billId", "ccy", "balChg", "type", "notes", "clientId"} {
		if _, ok := bill[key]; !ok {
			t.Errorf("field %q dropped — the unknown columns are the point: %+v", key, bill)
		}
	}
	if bill["type"] != "130" {
		t.Errorf("type = %q, want the venue's own code 130", bill["type"])
	}
	// A raw millisecond stamp is unreadable next to a snapshot date, and the
	// whole question is which day a row belongs to.
	if bill["iso_time"] != "2026-09-02T11:49:44Z" {
		t.Errorf("iso_time = %q, want the row's UTC instant", bill["iso_time"])
	}
	if len(probe.Balances) != 1 || probe.Balances[0]["bal"] != "0.0454" {
		t.Errorf("balances = %+v, want the BTC line verbatim", probe.Balances)
	}
}

// A key without the funding permission fails the ledger call. That is an
// answer — the account may still hold the parked funds — so it is reported
// rather than turned into an empty ledger that reads as an idle wallet.
func TestOKXProbeFunding_UnreadableLedgerIsReportedNotEmptied(t *testing.T) {
	s := newOKXBillsServer(t, map[string]string{
		okxAssetBalancesPath: `{"code":"0","data":[{"ccy":"BTC","bal":"0.0454"}]}`,
	}, map[string]int{
		okxAssetBillsPath:   403,
		okxAssetHistoryPath: 403,
	})

	probe, err := s.connector().ProbeFunding(context.Background(), time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("a readable balance must still answer: %v", err)
	}
	if len(probe.Bills) != 0 {
		t.Fatalf("got bills from a refused endpoint: %+v", probe.Bills)
	}
	if len(probe.Notes) != 2 {
		t.Fatalf("notes = %v, want one per refused endpoint", probe.Notes)
	}
}

func TestOKXProbeFunding_WholeWalletUnreadableIsAnError(t *testing.T) {
	s := newOKXBillsServer(t, nil, map[string]int{
		okxAssetBalancesPath: 403,
		okxAssetBillsPath:    403,
		okxAssetHistoryPath:  403,
	})

	if _, err := s.connector().ProbeFunding(context.Background(), time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)); err == nil {
		t.Fatal("a wallet that answered nothing must not read as an empty wallet")
	}
}

// Both endpoints are asked: the recent window alone would hide a transfer
// older than it, and a missing transfer is the defect this probe investigates.
func TestOKXProbeFunding_ReadsBothLedgerWindows(t *testing.T) {
	s := newOKXBillsServer(t, map[string]string{
		okxAssetBillsPath:   `{"code":"0","data":[{"billId":"recent","ts":"1788349784777","ccy":"BTC","balChg":"1"}]}`,
		okxAssetHistoryPath: `{"code":"0","data":[{"billId":"old","ts":"1786349784777","ccy":"BTC","balChg":"-1"}]}`,
	}, nil)

	probe, err := s.connector().ProbeFunding(context.Background(), time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("ProbeFunding: %v", err)
	}
	if len(probe.Bills) != 2 {
		t.Fatalf("got %d bills, want both windows merged: %+v", len(probe.Bills), probe.Bills)
	}
	if probe.Bills[0]["billId"] != "old" {
		t.Errorf("bills are not chronological: %+v", probe.Bills)
	}

	var sawRecent, sawHistory bool
	for _, p := range s.pathsHit() {
		switch p {
		case okxAssetBillsPath:
			sawRecent = true
		case okxAssetHistoryPath:
			sawHistory = true
		}
	}
	if !sawRecent || !sawHistory {
		t.Errorf("paths hit = %v, want both ledger windows", s.pathsHit())
	}
}
