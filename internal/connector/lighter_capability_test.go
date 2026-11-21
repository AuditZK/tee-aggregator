package connector

import "testing"

// A wallet address reads a Lighter account's balance and positions but not
// its fills, so its history can never be rebuilt. The gap has to travel on
// the connection's status, where the holder sees it; it used to be a 400 in
// the enclave's log and a connection recorded as completed.
func TestLighterCapabilityWarnings_WalletAddressCannotRebuild(t *testing.T) {
	l := NewLighter(&Credentials{APIKey: "0x00000000000000000000000000000000000000aa"})
	got := l.CapabilityWarnings()
	if len(got) != 1 || got[0] != "lighter_history_needs_ro_token" {
		t.Fatalf("warnings = %v, want [lighter_history_needs_ro_token]", got)
	}
}

func TestLighterCapabilityWarnings_TokenHasNothingToSay(t *testing.T) {
	l := NewLighter(&Credentials{APIKey: "ro:12:single:1:sig"})
	if got := l.CapabilityWarnings(); len(got) != 0 {
		t.Fatalf("warnings = %v, want none for a ro: token", got)
	}
}

func TestLighter_ImplementsCapabilityWarner(t *testing.T) {
	var _ CapabilityWarner = (*Lighter)(nil)
}
