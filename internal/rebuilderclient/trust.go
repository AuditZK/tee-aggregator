package rebuilderclient

import (
	"crypto/tls"
	"crypto/x509"
	_ "embed"
	"net/http"
)

// isrgRoots are Let's Encrypt's four roots (ISRG Root X1, X2, YE, YR), the
// only authorities trusted for the rebuilder (SEC-03). The requests carry
// decrypted credentials: any other public CA that misissued a certificate
// for the rebuilder's name would otherwise be accepted. All four are listed
// so a certificate renewed on another Let's Encrypt chain still verifies.
//
//go:embed isrg_roots.pem
var isrgRoots []byte

func pinnedTransport() *http.Transport {
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(isrgRoots) {
		panic("rebuilderclient: embedded ISRG roots do not parse")
	}
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	return t
}
