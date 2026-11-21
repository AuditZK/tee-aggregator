package tls

import (
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"
)

// NewKeyGeneratorFromFiles loads certificate/key PEM files and prepares a TLS key holder.
// This is used for TS parity where startup must fail if cert files are unavailable.
func NewKeyGeneratorFromFiles(certPath, keyPath string) (*KeyGenerator, error) {
	certPEM, err := os.ReadFile(certPath)
	if err != nil {
		return nil, fmt.Errorf("read TLS cert file %s: %w", certPath, err)
	}
	keyPEM, err := os.ReadFile(keyPath)
	if err != nil {
		return nil, fmt.Errorf("read TLS key file %s: %w", keyPath, err)
	}

	keyPair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, fmt.Errorf("parse TLS keypair (cert=%s key=%s): %w", certPath, keyPath, err)
	}
	if len(keyPair.Certificate) == 0 {
		return nil, fmt.Errorf("TLS certificate chain is empty (cert=%s)", certPath)
	}

	leaf, err := x509.ParseCertificate(keyPair.Certificate[0])
	if err != nil {
		return nil, fmt.Errorf("parse leaf certificate (cert=%s): %w", certPath, err)
	}

	return &KeyGenerator{
		certificate: leaf,
		certPEM:     certPEM,
		keyPEM:      keyPEM,
		fingerprint: sha256FingerprintColonUpper(leaf.Raw),
	}, nil
}

func sha256FingerprintColonUpper(der []byte) string {
	hash := sha256.Sum256(der)
	parts := make([]string, len(hash))
	for i, b := range hash {
		parts[i] = fmt.Sprintf("%02X", b)
	}
	return strings.Join(parts, ":")
}
