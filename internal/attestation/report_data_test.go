package attestation

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"hash"
	"strings"
	"testing"
)

// SEC-102 documents that buildReportData length-prefixes each input so
// (tls_fp, e2e_pk) pairs whose concatenation happens to collide can never
// produce identical report_data.
func TestBuildReportData_DomainSeparation(t *testing.T) {
	a := &Service{
		tlsFingerprint: "ABCD",
		e2ePublicKey:   "EFGH",
		signingPubKey:  "IJKL",
	}
	b := &Service{
		tlsFingerprint: "ABC",
		e2ePublicKey:   "DEFGH",
		signingPubKey:  "IJKL",
	}
	if a.buildReportData(nil) == b.buildReportData(nil) {
		t.Fatal("len-prefixed inputs must not collide on different splits")
	}
}

func TestBuildReportData_DeterministicOnSameInput(t *testing.T) {
	s := &Service{tlsFingerprint: "F", e2ePublicKey: "P", signingPubKey: "K"}
	first := s.buildReportData([]byte("nonce"))
	second := s.buildReportData([]byte("nonce"))
	if first != second {
		t.Fatalf("non-deterministic: %s vs %s", first, second)
	}
}

func TestBuildReportData_NonceChangesOutput(t *testing.T) {
	s := &Service{tlsFingerprint: "F", e2ePublicKey: "P", signingPubKey: "K"}
	if s.buildReportData(nil) == s.buildReportData([]byte{0x01}) {
		t.Fatal("different nonces must yield different report_data")
	}
}

func TestBuildReportData_NilNonceDomainSeparated(t *testing.T) {
	// Per the comment: "nil nonce ⇒ writes 8 zero bytes, still
	// domain-separated." Verify by hand.
	s := &Service{tlsFingerprint: "X", e2ePublicKey: "Y", signingPubKey: "Z"}
	got := s.buildReportData(nil)

	expected := sha256.New()
	writeLenPrefixed(expected, []byte("X"))
	writeLenPrefixed(expected, []byte("Y"))
	writeLenPrefixed(expected, []byte("Z"))
	writeLenPrefixed(expected, nil)
	want := hex.EncodeToString(expected.Sum(nil))

	if got != want {
		t.Fatalf("got %s, want %s", got, want)
	}
}

func TestWriteLenPrefixed_LayoutMatchesSpec(t *testing.T) {
	h := newCapturingHash()
	writeLenPrefixed(h, []byte("hello"))

	got := h.captured.Bytes()
	if len(got) != 8+5 {
		t.Fatalf("len=%d, want 8+5", len(got))
	}
	gotLen := binary.BigEndian.Uint64(got[:8])
	if gotLen != 5 {
		t.Fatalf("prefix=%d, want 5", gotLen)
	}
	if !bytes.Equal(got[8:], []byte("hello")) {
		t.Fatalf("payload=%q, want hello", got[8:])
	}
}

func TestWriteLenPrefixed_NilEmitsZeroLength(t *testing.T) {
	h := newCapturingHash()
	writeLenPrefixed(h, nil)
	got := h.captured.Bytes()
	if len(got) != 8 {
		t.Fatalf("len=%d, want 8 (zero-length prefix only)", len(got))
	}
	if binary.BigEndian.Uint64(got) != 0 {
		t.Fatal("nil → uint64 prefix must be 0")
	}
}

// capturingHash is a hash.Hash whose Write captures bytes for inspection;
// it lets us verify the exact byte layout writeLenPrefixed produces.
type capturingHash struct {
	hash.Hash
	captured *bytes.Buffer
}

func newCapturingHash() *capturingHash {
	return &capturingHash{Hash: sha256.New(), captured: &bytes.Buffer{}}
}
func (c *capturingHash) Write(p []byte) (int, error) {
	c.captured.Write(p)
	return c.Hash.Write(p)
}

// SEC-115: PlatformSevSnp / PlatformUnattestedDev constants must keep their
// exact string values. Anything else breaks the wire contract with verifiers.
func TestPlatformConstants(t *testing.T) {
	if PlatformSevSnp != "sev-snp" {
		t.Fatalf("PlatformSevSnp=%q (must be sev-snp)", PlatformSevSnp)
	}
	if PlatformUnattestedDev != "unattested-dev" {
		t.Fatalf("PlatformUnattestedDev=%q (must be unattested-dev)", PlatformUnattestedDev)
	}
}

// Extra cases for parseSnpguestReport that the existing parse_test.go
// might not exercise: unknown fields between known sections.
func TestParseSnpguestReport_IgnoresUnknownFields(t *testing.T) {
	out := strings.Join([]string{
		"Version: 2",
		"Guest SVN: 7",
		"Some Other Field: foo",
		"Measurement:",
		"12 06 83 61",
		"Platform Version:",
		"00 00 00 00",
		"Trailing: bar",
	}, "\n")
	r := &SevSnpReport{}
	parseSnpguestReport(out, r)
	if r.Measurement != "12068361" {
		t.Fatalf("Measurement=%q", r.Measurement)
	}
	if r.PlatformVersion != "00000000" {
		t.Fatalf("PlatformVersion=%q", r.PlatformVersion)
	}
}
