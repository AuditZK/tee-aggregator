package connector

import (
	"context"
	"slices"
	"testing"
	"time"
)

// GetCashflows resets its own markers when it starts. A gap GetBalance found
// (the funding wallet it could not read) used to live in that same list, so
// the cashflow read that follows it in every sync erased it before the sync
// status was written.
func TestOKXBalanceWarningSurvivesTheCashflowRead(t *testing.T) {
	router := newOKXFakeRouter(t, map[string]string{
		"/api/v5/account/balance":       okxProbePortfolioBalance,
		"/api/v5/account/bills":         `{"code":"0","data":[]}`,
		"/api/v5/account/bills-archive": `{"code":"0","data":[]}`,
		"/api/v5/asset/bills":           `{"code":"0","data":[]}`,
		"/api/v5/asset/bills-history":   `{"code":"0","data":[]}`,
	})
	okx := router.connector()

	if _, err := okx.GetBalance(context.Background()); err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if !slices.Contains(okx.CapabilityWarnings(), "okx_funding_balance_unreadable") {
		t.Fatalf("an unreadable funding wallet must be reported, got %v", okx.CapabilityWarnings())
	}

	_, _ = okx.GetCashflows(context.Background(), time.Now().Add(-24*time.Hour))

	if !slices.Contains(okx.CapabilityWarnings(), "okx_funding_balance_unreadable") {
		t.Fatalf("the cashflow read erased the balance gap, got %v", okx.CapabilityWarnings())
	}
}
