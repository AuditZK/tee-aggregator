package server

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"sync"
)

var executableHash struct {
	once sync.Once
	hex  string
}

// executableSHA256 is what an auditor compares their own build of main
// against. The SEV-SNP measurement covers the VM image and not this file,
// which sits on the VM's disk, so without it the running build cannot be
// checked from outside. Self-reported by the process: worth exactly the
// measurement plus the deploy chain that put the file here, no more.
func executableSHA256() string {
	executableHash.once.Do(func() {
		path, err := os.Executable()
		if err != nil {
			return
		}
		f, err := os.Open(path)
		if err != nil {
			return
		}
		defer f.Close()
		h := sha256.New()
		if _, err := io.Copy(h, f); err != nil {
			return
		}
		executableHash.hex = hex.EncodeToString(h.Sum(nil))
	})
	return executableHash.hex
}
