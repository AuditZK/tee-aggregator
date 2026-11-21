package connector

import (
	"context"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
)

// The connector cache hands one instance to every sync of a connection for an
// hour, and nothing serializes two syncs of the same connection. Binance and
// MEXC cache the last balance read on the instance for GetBalanceByMarket;
// without a lock, concurrent reads race on it (caught by -race in CI) and
// Binance's appends can interleave into a breakdown with duplicated markets.
func TestBalanceCaches_ConcurrentReadsOfOneInstance(t *testing.T) {
	const readers = 8

	t.Run("binance", func(t *testing.T) {
		var reqs atomic.Int32
		srv := newBinanceMarketTestServer(t, &reqs, "ok")
		defer srv.Close()
		target, _ := url.Parse(srv.URL)
		b := NewBinanceWithClient(&Credentials{APIKey: "k", APISecret: "s"},
			&http.Client{Transport: hostRewriter{base: http.DefaultTransport, target: target}})

		if _, err := b.GetBalance(context.Background()); err != nil {
			t.Fatalf("GetBalance: %v", err)
		}
		want := len(mustMarket(t, b))

		var wg sync.WaitGroup
		for range readers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if _, err := b.GetBalance(context.Background()); err != nil {
					t.Errorf("GetBalance: %v", err)
				}
				_, _ = b.GetBalanceByMarket(context.Background())
				_ = b.CapabilityWarnings()
				_ = b.Coverage()
			}()
		}
		wg.Wait()

		if got := len(mustMarket(t, b)); got != want {
			t.Fatalf("breakdown has %d markets after concurrent reads, want %d", got, want)
		}
	})

	t.Run("mexc", func(t *testing.T) {
		srv := mexcTestServer(t, false)
		defer srv.Close()
		m := NewMEXC(&Credentials{APIKey: "k", APISecret: "s"})
		m.base.BaseURL = srv.URL

		var wg sync.WaitGroup
		for range readers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if _, err := m.GetBalance(context.Background()); err != nil {
					t.Errorf("GetBalance: %v", err)
				}
				_, _ = m.GetBalanceByMarket(context.Background())
			}()
		}
		wg.Wait()
	})
}
