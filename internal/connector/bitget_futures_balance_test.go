package connector

import (
	"context"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
)

func bitgetBalanceServer(t *testing.T, futuresStatus int, futuresBody string) *Bitget {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(r.URL.Path, "/spot/account/assets"):
			_, _ = w.Write([]byte(`{"code":"00000","data":[{"coin":"USDT","available":"100","frozen":"0"}]}`))
		case strings.Contains(r.URL.Path, "/mix/account/accounts"):
			if futuresStatus != 0 {
				w.WriteHeader(futuresStatus)
			}
			_, _ = w.Write([]byte(futuresBody))
		default:
			_, _ = w.Write([]byte(`{"code":"00000","data":[]}`))
		}
	}))
	t.Cleanup(srv.Close)
	b := NewBitget(&Credentials{APIKey: "k", APISecret: "cw==", Passphrase: "p"})
	b.base.BaseURL = srv.URL
	return b
}

func TestBitgetBalance_FuturesWallet(t *testing.T) {
	cases := []struct {
		name      string
		status    int
		body      string
		wantEq    float64
		wantWarns []string
		wantFail  bool
	}{
		{
			name:   "futures read",
			body:   `{"code":"00000","data":[{"marginCoin":"USDT","accountEquity":"50","unrealizedPL":"5","available":"40"}]}`,
			wantEq: 200, // 100 spot + 50 on each of the two stable-margined products
		},
		{
			// The key cannot open the futures wallet: it is outside the
			// equity, and the user is asked to widen the key.
			name:      "futures refused for permission",
			status:    http.StatusBadRequest,
			body:      `{"code":"40014","msg":"Incorrect permissions"}`,
			wantEq:    100,
			wantWarns: []string{"futures_permission_missing"},
		},
		{
			// Anything else would persist a snapshot short by the whole
			// futures wallet; a failed sync is retried, a wrong one stays.
			name:     "futures failing otherwise",
			status:   http.StatusBadRequest,
			body:     `{"code":"40808","msg":"Parameter verification exception"}`,
			wantFail: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := bitgetBalanceServer(t, tc.status, tc.body)
			bal, err := b.GetBalance(context.Background())
			if tc.wantFail {
				if err == nil {
					t.Fatalf("equity %v persisted without the futures wallet", bal.Equity)
				}
				return
			}
			if err != nil {
				t.Fatalf("GetBalance: %v", err)
			}
			if bal.Equity != tc.wantEq {
				t.Errorf("equity = %v, want %v", bal.Equity, tc.wantEq)
			}
			if got := b.CapabilityWarnings(); !slices.Equal(got, tc.wantWarns) {
				t.Errorf("warnings = %v, want %v", got, tc.wantWarns)
			}
		})
	}
}
