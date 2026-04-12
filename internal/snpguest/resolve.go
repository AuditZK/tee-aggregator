// Package snpguest resolves the path to the AMD SEV-SNP `snpguest` CLI.
//
// The binary can live in several places depending on how the image was
// built: Dockerfile.production installs it from a Rust build stage into
// /usr/local/bin, some bases place it in /usr/bin, and a developer may
// expose it via $PATH. Hardcoding /usr/bin/snpguest — as the original
// attestation and key-derivation code did — silently breaks SEV-SNP
// support on images where the binary landed elsewhere, which in turn
// causes the Go enclave to fall back to a weaker key path and produce
// DEKs that cannot decrypt credentials written by the TypeScript
// enclave on the same hardware.
package snpguest

import (
	"os"
	"os/exec"

	"go.uber.org/zap"
)

// ResolvePath returns the first snpguest binary found on disk, checking
// a list of well-known locations before falling back to $PATH. Returns
// an empty string if none is available — callers must treat that as
// "SEV-SNP unavailable" and fall back accordingly.
//
// The logger parameter is optional; if non-nil, it records a single
// Warn when no binary is found so that operators can diagnose a silent
// dev-mode fallback on a hardware-capable host.
func ResolvePath(logger *zap.Logger) string {
	candidates := []string{
		"/usr/local/bin/snpguest",
		"/usr/bin/snpguest",
	}
	for _, p := range candidates {
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	if p, err := exec.LookPath("snpguest"); err == nil {
		return p
	}
	if logger != nil {
		logger.Warn("snpguest binary not found — SEV-SNP-derived keys unavailable",
			zap.Strings("searched", candidates),
		)
	}
	return ""
}
