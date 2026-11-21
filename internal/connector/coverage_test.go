package connector

import (
	"reflect"
	"testing"
)

// The distinction the whole rule rests on. A wallet read as empty is measured;
// a wallet we could not reach is not. The breakdown alone cannot tell them
// apart — an empty market carries no bucket — which is why coverage is stated
// rather than inferred.
func TestCoveredWallets_EmptyWalletIsStillCovered(t *testing.T) {
	coverage := []WalletCoverage{
		{Wallet: "spot", Status: WalletRead},
		{Wallet: "um_futures", Status: WalletRead}, // read, held nothing
		{Wallet: "cross_margin", Status: WalletUnreadable, Reason: "-2015"},
	}

	got := CoveredWallets(coverage)

	if want := []string{"spot", "um_futures"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("covered = %v, want %v — an empty wallet must stay in the set", got, want)
	}
	if missed := UnreadableWallets(coverage); !reflect.DeepEqual(missed, []string{"cross_margin"}) {
		t.Fatalf("unreadable = %v, want [cross_margin]", missed)
	}
}

// A connector that does not report yet is unknown, never complete. Treating
// silence as full coverage would let every unmigrated connector publish as if
// it had been checked.
func TestCoveredWallets_NoReportIsUnknownNotComplete(t *testing.T) {
	if got := CoveredWallets(nil); got != nil {
		t.Fatalf("covered = %v, want nil for a connector that reports nothing", got)
	}
	if got := UnreadableWallets(nil); got != nil {
		t.Fatalf("unreadable = %v, want nil", got)
	}
}

// The set is compared between days, so its order must not depend on the order
// the connector happened to fetch its wallets in.
func TestCoveredWallets_OrderIndependent(t *testing.T) {
	a := CoveredWallets([]WalletCoverage{
		{Wallet: "um_futures", Status: WalletRead},
		{Wallet: "spot", Status: WalletRead},
	})
	b := CoveredWallets([]WalletCoverage{
		{Wallet: "spot", Status: WalletRead},
		{Wallet: "um_futures", Status: WalletRead},
	})
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("fetch order changed the set: %v vs %v", a, b)
	}
}

// A product the account never opened is not a gap in our measurement.
func TestCoveredWallets_NotOpenedIsNeitherCoveredNorMissed(t *testing.T) {
	coverage := []WalletCoverage{
		{Wallet: "spot", Status: WalletRead},
		{Wallet: "coinm_futures", Status: WalletNotOpened},
	}
	if got := CoveredWallets(coverage); !reflect.DeepEqual(got, []string{"spot"}) {
		t.Fatalf("covered = %v, want [spot]", got)
	}
	if got := UnreadableWallets(coverage); got != nil {
		t.Fatalf("unreadable = %v, want nil — a product never opened is not a gap", got)
	}
}
