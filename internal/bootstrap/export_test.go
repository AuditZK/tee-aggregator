package bootstrap

// Test-only helpers. Compiled only when `go test` is invoked for this
// package, so production builds cannot reach SetOperatorPubkeyForTest.

// SetOperatorPubkeyForTest swaps the operator pubkey (ssh-ed25519
// wire-format base64) for the duration of a test. Returns a restore
// function the caller MUST defer to put the original value back —
// otherwise a later test in the same process inherits the override
// and may falsely pass.
func SetOperatorPubkeyForTest(pubkey string) (restore func()) {
	prev := operatorPubkey
	operatorPubkey = pubkey
	return func() { operatorPubkey = prev }
}
