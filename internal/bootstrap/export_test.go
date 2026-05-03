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

// CanonicalPayloadJSONForTest exposes the unexported canonical JSON
// helper so cross-file tests can build their own signatures. Same
// signature/behaviour as the internal one.
func CanonicalPayloadJSONForTest(p *SignedAllowlistPayload) ([]byte, error) {
	return canonicalPayloadJSON(p)
}

// ComputeExpectedReportDataForTest exposes the report-data computation
// so a test can drive the HandoffServer with a hand-built attestation.
// (The companion of computeExpectedReportData in handoff_server.go.)
func ComputeExpectedReportDataForTest(report interface{}, nonce []byte) string {
	// Forward to the real implementation. We accept interface{} to
	// avoid leaking attestation types into the test surface; tests
	// that need this MUST pass *attestation.AttestationReport.
	if r, ok := report.(*reportLikeForTest); ok {
		return r.compute(nonce)
	}
	panic("ComputeExpectedReportDataForTest: pass a *attestation.AttestationReport, see test helper")
}

// reportLikeForTest is a thin shim so the public test helper above can
// stay generic. Tests build their attestation directly and call
// computeExpectedReportData; this stub exists for symmetry.
type reportLikeForTest struct {
	compute func(nonce []byte) string
}
