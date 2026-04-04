package tls

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"strings"
	"time"
)

// KeyGenerator manages TLS credentials inside the enclave.
// The private key never leaves enclave RAM.
type KeyGenerator struct {
	privateKey  *ecdsa.PrivateKey
	certificate *x509.Certificate
	certPEM     []byte
	keyPEM      []byte
	fingerprint string
}

// NewKeyGenerator generates an ECDSA P-256 keypair and self-signed X.509 certificate.
func NewKeyGenerator() (*KeyGenerator, error) {
	// Generate ECDSA P-256 key pair
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate ecdsa key: %w", err)
	}

	// Serial number
	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, fmt.Errorf("generate serial number: %w", err)
	}

	// Certificate template
	template := &x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			CommonName:   "Track Record Enclave",
			Organization: []string{"Track Record Platform"},
			Country:      []string{"FR"},
		},
		NotBefore:             time.Now().Add(-1 * time.Hour),
		NotAfter:              time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}

	// Self-sign
	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &privateKey.PublicKey, privateKey)
	if err != nil {
		return nil, fmt.Errorf("create certificate: %w", err)
	}

	// Parse back for storage
	cert, err := x509.ParseCertificate(certDER)
	if err != nil {
		return nil, fmt.Errorf("parse certificate: %w", err)
	}

	// PEM encode certificate
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})

	// PEM encode private key
	keyDER, err := x509.MarshalECPrivateKey(privateKey)
	if err != nil {
		return nil, fmt.Errorf("marshal private key: %w", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	// SHA-256 fingerprint of DER-encoded certificate
	hash := sha256.Sum256(certDER)
	parts := make([]string, len(hash))
	for i, b := range hash {
		parts[i] = fmt.Sprintf("%02X", b)
	}
	fingerprint := strings.Join(parts, ":")

	return &KeyGenerator{
		privateKey:  privateKey,
		certificate: cert,
		certPEM:     certPEM,
		keyPEM:      keyPEM,
		fingerprint: fingerprint,
	}, nil
}

// Fingerprint returns the SHA-256 fingerprint of the certificate.
func (k *KeyGenerator) Fingerprint() string {
	return k.fingerprint
}

// CertPEM returns the PEM-encoded certificate.
func (k *KeyGenerator) CertPEM() []byte {
	return k.certPEM
}

// KeyPEM returns the PEM-encoded private key.
func (k *KeyGenerator) KeyPEM() []byte {
	return k.keyPEM
}

// FingerprintBytes returns the raw SHA-256 hash of the certificate DER.
func (k *KeyGenerator) FingerprintBytes() []byte {
	hash := sha256.Sum256(k.certificate.Raw)
	return hash[:]
}

// Cleanup wipes the private key from memory.
func (k *KeyGenerator) Cleanup() {
	// Zero out key PEM bytes regardless of key source (generated or file-loaded).
	for i := range k.keyPEM {
		k.keyPEM[i] = 0
	}
	k.privateKey = nil
}
