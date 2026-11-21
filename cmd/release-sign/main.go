// Tool offline pour générer et signer la SignedAllowlist d'une release.
//
// Usage:
//
//	# 1. (One time) generate the operator keypair
//	./release-sign keygen --out ~/.tee/operator
//	# → ~/.tee/operator      (privkey, prompts for passphrase)
//	# → ~/.tee/operator.pub  (pubkey, embed in internal/bootstrap.OperatorPubkey)
//
//	# 2. Show the pubkey to copy into the source code
//	./release-sign pubkey ~/.tee/operator.pub
//
//	# 3. Sign a new allowlist for release v1.2.0
//	./release-sign sign \
//	    --key ~/.tee/operator \
//	    --measurement 1206abcdef... \
//	    --label "go-enclave-v1.2.0" \
//	    --previous signed_allowlist.v1.1.0.json \
//	    --out signed_allowlist.v1.2.0.json
//	# → reads previous allowlist (or starts empty), adds the new
//	#   measurement + label, signs, writes the new file.
//
// Threat model: the privkey file is encrypted with a passphrase via
// scrypt-AES-GCM. ssh-keygen-style format. NEVER commit the encrypted
// privkey to git — keep it on operator's laptop + 2 USB backups.
//
// The corresponding pubkey is hardcoded in internal/bootstrap/signed_allowlist.go
// so a compromised runtime config cannot substitute a different signer.
package main

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/trackrecord/enclave/internal/bootstrap"
	"golang.org/x/crypto/scrypt"
	"golang.org/x/term"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "keygen":
		runKeygen(os.Args[2:])
	case "sign":
		runSign(os.Args[2:])
	case "pubkey":
		runShowPubkey(os.Args[2:])
	case "verify":
		runVerify(os.Args[2:])
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n", os.Args[1])
		printUsage()
		os.Exit(2)
	}
}

func printUsage() {
	fmt.Fprintln(os.Stderr, `release-sign — operator tool for B2 signed-binary-identity releases.

Subcommands:
  keygen   --out PATH                    Generate a fresh Ed25519 keypair (prompts for passphrase).
  pubkey   PATH.pub                      Print the pubkey in the format the Go const expects.
  sign     --key PATH --measurement HEX  Add a measurement to the allowlist and sign it.
           --label TEXT [--previous PATH] [--out PATH]
  verify   --pubkey PATH --in PATH       Verify a signed allowlist against a pubkey file.

Run any subcommand with -h for the full flag set.`)
}

// ─── keygen ─────────────────────────────────────────────────────────────────

func runKeygen(args []string) {
	fs := flag.NewFlagSet("keygen", flag.ExitOnError)
	out := fs.String("out", "", "output path for the encrypted private key (REQUIRED)")
	_ = fs.Parse(args)
	if *out == "" {
		die("keygen: --out is required")
	}
	if _, err := os.Stat(*out); err == nil {
		die(fmt.Sprintf("keygen: %s already exists — refusing to overwrite", *out))
	}

	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		die(fmt.Sprintf("keygen: %v", err))
	}
	pass := readPassphrase("Passphrase (>= 14 chars or 6 dicewared words): ", true)

	encrypted, err := encryptPrivkey(priv, pass)
	if err != nil {
		die(fmt.Sprintf("keygen: encrypt: %v", err))
	}
	if err := os.WriteFile(*out, encrypted, 0600); err != nil {
		die(fmt.Sprintf("keygen: write privkey: %v", err))
	}
	if err := os.WriteFile(*out+".pub", encodePubkey(pub), 0644); err != nil {
		die(fmt.Sprintf("keygen: write pubkey: %v", err))
	}
	fmt.Printf("✓ Wrote %s (encrypted privkey, mode 0600)\n", *out)
	fmt.Printf("✓ Wrote %s.pub (pubkey, paste into internal/bootstrap/signed_allowlist.go)\n", *out)
	fmt.Println()
	fmt.Println("Next steps:")
	fmt.Printf("  1. Copy the OperatorPubkey constant: ./release-sign pubkey %s.pub\n", *out)
	fmt.Println("  2. Update internal/bootstrap/signed_allowlist.go: const OperatorPubkey = \"<paste>\"")
	fmt.Println("  3. Rebuild + redeploy enclave with the new constant.")
	fmt.Println("  4. Backup", *out, "to 2+ USB drives stored offline.")
}

// ─── pubkey ─────────────────────────────────────────────────────────────────

func runShowPubkey(args []string) {
	if len(args) != 1 {
		die("pubkey: pass the path to .pub file")
	}
	raw, err := os.ReadFile(args[0])
	if err != nil {
		die(fmt.Sprintf("pubkey: read: %v", err))
	}
	// File format: "ssh-ed25519 BASE64 comment\n"
	parts := strings.Fields(string(raw))
	if len(parts) < 2 || parts[0] != "ssh-ed25519" {
		die("pubkey: file is not a ssh-ed25519 pubkey")
	}
	fmt.Println("Constant value to paste in internal/bootstrap/signed_allowlist.go:")
	fmt.Println()
	fmt.Printf("const OperatorPubkey = %q\n", parts[1])
}

// ─── sign ───────────────────────────────────────────────────────────────────

func runSign(args []string) {
	fs := flag.NewFlagSet("sign", flag.ExitOnError)
	keyPath := fs.String("key", "", "path to the encrypted Ed25519 privkey (REQUIRED)")
	measurement := fs.String("measurement", "", "SEV-SNP measurement of the new release (hex, REQUIRED)")
	label := fs.String("label", "", "human-readable label for the new entry (REQUIRED)")
	previous := fs.String("previous", "", "path to the previous SignedAllowlist (optional — first release leaves this empty)")
	out := fs.String("out", "signed_allowlist.json", "output path")
	_ = fs.Parse(args)

	if *keyPath == "" || *measurement == "" || *label == "" {
		die("sign: --key, --measurement, --label are required")
	}

	encryptedPriv, err := os.ReadFile(*keyPath)
	if err != nil {
		die(fmt.Sprintf("sign: read privkey: %v", err))
	}
	pass := readPassphrase("Passphrase: ", false)
	priv, err := decryptPrivkey(encryptedPriv, pass)
	if err != nil {
		die(fmt.Sprintf("sign: decrypt privkey: %v (wrong passphrase?)", err))
	}
	defer wipe(priv)

	var entries []bootstrap.AllowlistEntry
	if *previous != "" {
		entries = readPreviousEntries(*previous)
	}
	entries = upsertEntry(entries, bootstrap.AllowlistEntry{
		Measurement: strings.ToLower(strings.TrimSpace(*measurement)),
		Label:       *label,
		AddedAt:     time.Now().UTC(),
	})

	now := time.Now().UTC()
	payload := bootstrap.SignedAllowlistPayload{
		Version:  1,
		IssuedAt: now,
		Entries:  entries,
	}
	canonical, err := canonicalPayloadJSON(&payload)
	if err != nil {
		die(fmt.Sprintf("sign: canonicalise: %v", err))
	}
	sig := ed25519.Sign(priv, canonical)

	signed := bootstrap.SignedAllowlist{
		Payload:   payload,
		Signature: base64.StdEncoding.EncodeToString(sig),
	}
	raw, err := json.MarshalIndent(signed, "", "  ")
	if err != nil {
		die(fmt.Sprintf("sign: marshal: %v", err))
	}
	if err := os.WriteFile(*out, raw, 0644); err != nil {
		die(fmt.Sprintf("sign: write: %v", err))
	}
	fmt.Printf("✓ Wrote %s (%d entries, signed at %s)\n", *out, len(entries), now.Format(time.RFC3339))
	fmt.Println("Next steps:")
	fmt.Println("  1. Ship", *out, "to the new enclave (via GCP metadata `signed-allowlist`)")
	fmt.Println("  2. Set HANDOFF_PEER_URL on the new enclave to the running v1's handoff endpoint")
	fmt.Println("  3. Boot the new enclave; it will fetch the master key via attested handoff")
}

// ─── verify ─────────────────────────────────────────────────────────────────

func runVerify(args []string) {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	pubPath := fs.String("pubkey", "", "path to operator pubkey (.pub file)")
	in := fs.String("in", "", "path to signed allowlist JSON")
	_ = fs.Parse(args)
	if *pubPath == "" || *in == "" {
		die("verify: --pubkey and --in are required")
	}
	pubRaw, err := os.ReadFile(*pubPath)
	if err != nil {
		die(fmt.Sprintf("verify: read pubkey: %v", err))
	}
	parts := strings.Fields(string(pubRaw))
	if len(parts) < 2 || parts[0] != "ssh-ed25519" {
		die("verify: pubkey file is not ssh-ed25519")
	}
	pubBytes, err := decodePubkey(parts[1])
	if err != nil {
		die(fmt.Sprintf("verify: decode pubkey: %v", err))
	}

	allowlistRaw, err := os.ReadFile(*in)
	if err != nil {
		die(fmt.Sprintf("verify: read allowlist: %v", err))
	}
	var sa bootstrap.SignedAllowlist
	if err := json.Unmarshal(allowlistRaw, &sa); err != nil {
		die(fmt.Sprintf("verify: unmarshal: %v", err))
	}
	canonical, err := canonicalPayloadJSON(&sa.Payload)
	if err != nil {
		die(fmt.Sprintf("verify: canonicalise: %v", err))
	}
	sig, err := base64.StdEncoding.DecodeString(sa.Signature)
	if err != nil {
		die(fmt.Sprintf("verify: decode sig: %v", err))
	}
	if !ed25519.Verify(pubBytes, canonical, sig) {
		die("verify: SIGNATURE INVALID")
	}
	fmt.Printf("✓ Signature valid — %d entries, issued %s\n",
		len(sa.Payload.Entries), sa.Payload.IssuedAt.Format(time.RFC3339))
	for _, e := range sa.Payload.Entries {
		fmt.Printf("  - %s  (%s, added %s)\n", e.Measurement, e.Label, e.AddedAt.Format("2006-01-02"))
	}
}

// ─── helpers ────────────────────────────────────────────────────────────────

// canonicalPayloadJSON mirrors bootstrap.canonicalPayloadJSON. Pulled
// inline because that function is unexported. Keep them aligned!
func canonicalPayloadJSON(p *bootstrap.SignedAllowlistPayload) ([]byte, error) {
	entries := make([]bootstrap.AllowlistEntry, len(p.Entries))
	copy(entries, p.Entries)
	for i := range entries {
		entries[i].Measurement = strings.ToLower(strings.TrimSpace(entries[i].Measurement))
	}
	// sort by measurement
	for i := 1; i < len(entries); i++ {
		j := i
		for j > 0 && entries[j-1].Measurement > entries[j].Measurement {
			entries[j-1], entries[j] = entries[j], entries[j-1]
			j--
		}
	}
	canonical := bootstrap.SignedAllowlistPayload{
		Version:  p.Version,
		IssuedAt: p.IssuedAt.UTC(),
		Entries:  entries,
	}
	return json.Marshal(canonical)
}

func readPreviousEntries(path string) []bootstrap.AllowlistEntry {
	raw, err := os.ReadFile(path)
	if err != nil {
		die(fmt.Sprintf("sign: read previous: %v", err))
	}
	var sa bootstrap.SignedAllowlist
	if err := json.Unmarshal(raw, &sa); err != nil {
		die(fmt.Sprintf("sign: unmarshal previous: %v", err))
	}
	return sa.Payload.Entries
}

func upsertEntry(entries []bootstrap.AllowlistEntry, e bootstrap.AllowlistEntry) []bootstrap.AllowlistEntry {
	for i, existing := range entries {
		if existing.Measurement == e.Measurement {
			entries[i] = e // refresh label/AddedAt
			return entries
		}
	}
	return append(entries, e)
}

// encodePubkey returns the ssh-ed25519 wire-format file body for a pubkey.
// File format: "ssh-ed25519 BASE64 release-sign-generated\n".
func encodePubkey(pub ed25519.PublicKey) []byte {
	algName := "ssh-ed25519"
	body := make([]byte, 0, 4+len(algName)+4+ed25519.PublicKeySize)
	body = append(body, byte(len(algName)>>24), byte(len(algName)>>16), byte(len(algName)>>8), byte(len(algName)))
	body = append(body, []byte(algName)...)
	body = append(body, byte(ed25519.PublicKeySize>>24), byte(ed25519.PublicKeySize>>16),
		byte(ed25519.PublicKeySize>>8), byte(ed25519.PublicKeySize))
	body = append(body, pub...)
	b64 := base64.StdEncoding.EncodeToString(body)
	return []byte(fmt.Sprintf("ssh-ed25519 %s release-sign-generated\n", b64))
}

func decodePubkey(b64 string) (ed25519.PublicKey, error) {
	raw, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return nil, err
	}
	algName := "ssh-ed25519"
	if len(raw) < 4+len(algName)+4+ed25519.PublicKeySize {
		return nil, errors.New("pubkey too short")
	}
	if string(raw[4:4+len(algName)]) != algName {
		return nil, errors.New("not ssh-ed25519")
	}
	rest := raw[4+len(algName)+4:]
	if len(rest) < ed25519.PublicKeySize {
		return nil, errors.New("pubkey body too short")
	}
	pk := make(ed25519.PublicKey, ed25519.PublicKeySize)
	copy(pk, rest[:ed25519.PublicKeySize])
	return pk, nil
}

// File format for the encrypted privkey:
//
//	"TEE-OPERATOR-PRIVKEY-V1\n"
//	[ 32 bytes ] scrypt salt
//	[ 12 bytes ] AES-GCM nonce
//	[ N  bytes ] AES-GCM ciphertext (privkey)
//
// scrypt parameters: N=2^17, r=8, p=1 → ~200 ms / 64 MiB on modern CPU.
const privkeyMagic = "TEE-OPERATOR-PRIVKEY-V1\n"

func encryptPrivkey(priv ed25519.PrivateKey, pass []byte) ([]byte, error) {
	salt := make([]byte, 32)
	if _, err := rand.Read(salt); err != nil {
		return nil, err
	}
	nonce := make([]byte, 12)
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	kek, err := scrypt.Key(pass, salt, 1<<17, 8, 1, 32)
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(kek)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	ct := gcm.Seal(nil, nonce, priv, nil)
	out := make([]byte, 0, len(privkeyMagic)+len(salt)+len(nonce)+len(ct))
	out = append(out, []byte(privkeyMagic)...)
	out = append(out, salt...)
	out = append(out, nonce...)
	out = append(out, ct...)
	return out, nil
}

func decryptPrivkey(blob, pass []byte) (ed25519.PrivateKey, error) {
	if len(blob) < len(privkeyMagic)+32+12 {
		return nil, errors.New("privkey blob too short")
	}
	if string(blob[:len(privkeyMagic)]) != privkeyMagic {
		return nil, errors.New("not a TEE-OPERATOR-PRIVKEY-V1 file")
	}
	rest := blob[len(privkeyMagic):]
	salt := rest[:32]
	nonce := rest[32 : 32+12]
	ct := rest[32+12:]

	kek, err := scrypt.Key(pass, salt, 1<<17, 8, 1, 32)
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(kek)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	pt, err := gcm.Open(nil, nonce, ct, nil)
	if err != nil {
		return nil, err
	}
	if len(pt) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("decrypted privkey size=%d, want %d", len(pt), ed25519.PrivateKeySize)
	}
	pk := make(ed25519.PrivateKey, ed25519.PrivateKeySize)
	copy(pk, pt)
	wipe(pt)
	return pk, nil
}

func readPassphrase(prompt string, confirm bool) []byte {
	fmt.Fprint(os.Stderr, prompt)
	pass, err := term.ReadPassword(int(os.Stdin.Fd()))
	fmt.Fprintln(os.Stderr)
	if err != nil {
		die(fmt.Sprintf("read passphrase: %v", err))
	}
	if len(pass) < 8 {
		die("passphrase too short (< 8 chars)")
	}
	if confirm {
		fmt.Fprint(os.Stderr, "Confirm passphrase: ")
		again, err := term.ReadPassword(int(os.Stdin.Fd()))
		fmt.Fprintln(os.Stderr)
		if err != nil {
			die(fmt.Sprintf("read passphrase: %v", err))
		}
		if string(pass) != string(again) {
			die("passphrases do not match")
		}
	}
	return pass
}

func wipe(b []byte) {
	for i := range b {
		b[i] = 0
	}
}

func die(msg string) {
	fmt.Fprintln(os.Stderr, "FATAL:", msg)
	os.Exit(1)
}
