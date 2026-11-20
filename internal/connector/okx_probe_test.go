package connector

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// okxFakeRouter answers per path, which the probe needs: it reads the balance
// and the account config in one call.
type okxFakeRouter struct {
	srv    *httptest.Server
	hits   int32
	byPath map[string]string
}

func newOKXFakeRouter(t *testing.T, byPath map[string]string) *okxFakeRouter {
	t.Helper()
	f := &okxFakeRouter{byPath: byPath}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&f.hits, 1)
		if r.Header.Get("OK-ACCESS-KEY") == "" || r.Header.Get("OK-ACCESS-SIGN") == "" {
			t.Errorf("request to %s is not signed", r.URL.Path)
		}
		body, ok := f.byPath[r.URL.Path]
		w.Header().Set("Content-Type", "application/json")
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			io.WriteString(w, `{"code":"50011","msg":"unknown path"}`)
			return
		}
		io.WriteString(w, body)
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *okxFakeRouter) connector() *OKX {
	creds := &Credentials{Exchange: "okx", APIKey: "key", APISecret: "secret", Passphrase: "pass"}
	return newOKXWithHosts(creds, &http.Client{Timeout: 5 * time.Second}, []string{f.srv.URL})
}

// OKX implements the optional prober; the compiler is the one that should say
// so, not a runtime 501 discovered during an incident.
const okxProbePortfolioBalance = `{"code":"0","data":[{` +
	`"totalEq":"25431.17","isoEq":"0","adjEq":"24876.52","availEq":"23712.08","ordFroz":"0",` +
	`"imr":"1318.64","mmr":"436.21","mgnRatio":"53.87","upl":"-112.73","uTime":"1757548800000","details":[` +
	`{"ccy":"BTC","eq":"0.2013","eqUsd":"21883.88","cashBal":"0.2013","availBal":"0.2013","availEq":"0.2013","frozenBal":"0","isoEq":"0","upl":"0","imr":"","mmr":""},` +
	`{"ccy":"USDC","eq":"3547.29","eqUsd":"3547.29","cashBal":"3547.29","availBal":"3547.29","availEq":"3547.29","frozenBal":"0","isoEq":"0","upl":"0","imr":"","mmr":""}` +
	`]}]}`
