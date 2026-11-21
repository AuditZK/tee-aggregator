package main

import (
	"testing"

	"github.com/trackrecord/enclave/internal/attestation"
	"github.com/trackrecord/enclave/internal/config"
	"go.uber.org/zap"
)

// CFG-004: in production a benchmark URL must be https AND carry an internal
// token (the series feeds the signed report). An unset URL and development are
// exempt (dev http + tokenless test stacks are allowed).
func TestCheckBenchmarkConfig(t *testing.T) {
	const token = "tok-abcdefghijklmnopqrstuvwx" // ≥24 chars

	cases := []struct {
		name    string
		url     string
		token   string
		isDev   bool
		wantErr bool
	}{
		{"unset url ok", "", "", false, false},
		{"dev http no token ok", "http://api-test.auditzk.com", "", true, false},
		{"dev https no token ok", "https://benchmark.auditzk.com", "", true, false},
		{"prod http rejected", "http://203.0.113.10:8080", token, false, true},
		{"prod https no token rejected", "https://benchmark.auditzk.com", "", false, true},
		{"prod https with token ok", "https://benchmark.auditzk.com", token, false, false},
		{"prod scheme is case-insensitive", "HTTPS://benchmark.auditzk.com", token, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := checkBenchmarkConfig(tc.url, tc.token, tc.isDev)
			if (err != nil) != tc.wantErr {
				t.Fatalf("checkBenchmarkConfig(%q, hasToken=%v, dev=%v) err=%v, wantErr=%v",
					tc.url, tc.token != "", tc.isDev, err, tc.wantErr)
			}
		})
	}
}

// CFG-005: the mt-bridge connect call carries the MT investor password in
// cleartext and its signature is only as strong as MT_BRIDGE_HMAC_SECRET, so
// production refuses to boot on an http link or a weak key. An unset URL
// (MetaTrader disabled) and development are exempt.
func TestCheckMTBridgeConfig(t *testing.T) {
	const secret = "mt-abcdefghijklmnopqrstuvwx" // ≥24 chars

	cases := []struct {
		name    string
		url     string
		secret  string
		isDev   bool
		wantErr bool
	}{
		{"unset url ok", "", "", false, false},
		{"dev http no secret ok", "http://mt-bridge:8090", "", true, false},
		{"prod http rejected", "http://mt-bridge:8090", secret, false, true},
		{"prod https no secret rejected", "https://mt-bridge.internal", "", false, true},
		{"prod https short secret rejected", "https://mt-bridge.internal", "tooshort", false, true},
		{"prod https with secret ok", "https://mt-bridge.internal", secret, false, false},
		{"prod scheme is case-insensitive", "HTTPS://mt-bridge.internal", secret, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := checkMTBridgeConfig(tc.url, tc.secret, tc.isDev)
			if (err != nil) != tc.wantErr {
				t.Fatalf("checkMTBridgeConfig(%q, secretLen=%d, dev=%v) err=%v, wantErr=%v",
					tc.url, len(tc.secret), tc.isDev, err, tc.wantErr)
			}
		})
	}
}

// The re-attestation downgrade guard. enforceProductionAttestation used to
// return nothing and refreshSignerAttestation called signer.SetAttestation
// right after it regardless, so the "keeping previous binding" it logs was a
// lie: the downgraded report overwrote the good binding with attested=false
// and every report signed from then on carried it. The claim only became true
// on the next restart — which is exactly where the startup Fatal lives.
//
// The boolean is what the caller now returns on. false = do not bind.
func TestEnforceProductionAttestation_RefusesToBindADowngrade(t *testing.T) {
	good := &attestation.AttestationReport{
		Platform: attestation.PlatformSevSnp,
		Attestation: &attestation.SevSnpReport{
			Verified:                 true,
			VcekVerified:             true,
			ReportDataBoundToRequest: true,
		},
	}

	downgrades := map[string]*attestation.AttestationReport{
		"no attestation block": {Platform: attestation.PlatformSevSnp},
		"unattested-dev platform": {
			Platform:    "unattested-dev",
			Attestation: &attestation.SevSnpReport{Verified: true, VcekVerified: true, ReportDataBoundToRequest: true},
		},
		"snpguest report unverified": {
			Platform:    attestation.PlatformSevSnp,
			Attestation: &attestation.SevSnpReport{Verified: false, VcekVerified: true, ReportDataBoundToRequest: true},
		},
		"vcek chain unverified": {
			Platform:    attestation.PlatformSevSnp,
			Attestation: &attestation.SevSnpReport{Verified: true, VcekVerified: false, ReportDataBoundToRequest: true},
		},
		"report data not bound to our keys": {
			Platform:    attestation.PlatformSevSnp,
			Attestation: &attestation.SevSnpReport{Verified: true, VcekVerified: true, ReportDataBoundToRequest: false},
		},
	}

	prod := &config.Config{Env: "production"}
	dev := &config.Config{Env: "development"}
	logger := zap.NewNop()

	if !enforceProductionAttestation(prod, good, logger, false) {
		t.Fatal("a fully verified report must be bound")
	}
	if !enforceProductionAttestation(prod, good, logger, true) {
		t.Fatal("a fully verified report must be bound at startup too")
	}

	for name, report := range downgrades {
		t.Run(name, func(t *testing.T) {
			// initial=false only: initial=true reaches logger.Fatal, which
			// exits the process.
			if enforceProductionAttestation(prod, report, logger, false) {
				t.Fatalf("%s was accepted; it would overwrite the previous binding with attested=false", name)
			}
			// Outside production nothing is enforced — dev enclaves sign as
			// unattested-dev on purpose.
			if !enforceProductionAttestation(dev, report, logger, false) {
				t.Fatalf("%s must not be refused outside production", name)
			}
		})
	}
}
