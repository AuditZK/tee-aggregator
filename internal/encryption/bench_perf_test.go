package encryption

import (
	"crypto/rand"
	"testing"
)

// PERF-AUDIT: probe AES-GCM allocation cost. Service.Encrypt/Decrypt rebuild
// the cipher.Block + cipher.AEAD on every call (aes.go:54-62, 108-116) even
// though Service.key is immutable. The benchmarks below quantify the per-call
// overhead so the audit can size the win from caching the AEAD on Service.

func makeServiceForBench(b *testing.B) *Service {
	b.Helper()
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		b.Fatalf("rand: %v", err)
	}
	s, err := New(key)
	if err != nil {
		b.Fatalf("New: %v", err)
	}
	return s
}

func BenchmarkEncryptWithAAD_64B(b *testing.B) {
	s := makeServiceForBench(b)
	plain := make([]byte, 64)
	if _, err := rand.Read(plain); err != nil {
		b.Fatalf("rand: %v", err)
	}
	aad := []byte("user|conn|api_key")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := s.EncryptWithAAD(plain, aad); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecryptWithAAD_64B(b *testing.B) {
	s := makeServiceForBench(b)
	plain := make([]byte, 64)
	if _, err := rand.Read(plain); err != nil {
		b.Fatalf("rand: %v", err)
	}
	aad := []byte("user|conn|api_key")
	enc, err := s.EncryptWithAAD(plain, aad)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := s.DecryptWithAAD(enc, aad); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecryptTSFormat_64B(b *testing.B) {
	s := makeServiceForBench(b)
	plain := make([]byte, 64)
	if _, err := rand.Read(plain); err != nil {
		b.Fatalf("rand: %v", err)
	}
	hexBlob, err := s.EncryptTSFormat(plain)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := s.DecryptTSFormat(hexBlob); err != nil {
			b.Fatal(err)
		}
	}
}
