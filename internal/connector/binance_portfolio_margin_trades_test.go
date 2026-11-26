package connector

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// pmFill is a fill the papi fake serves on one list, keyed by symbol.
type pmFill struct {
	symbol string
	time   time.Time
	row    map[string]any
}

// pmIncomeRow is an income row the papi fake serves.
type pmIncomeRow struct {
	symbol, incomeType, income, asset string
	time                              time.Time
	tranID                            int64
}

// binancePMActivityServer fakes a Binance account for the activity reads. In
// Portfolio Margin fapi refuses the key with -2015 and papi answers; out of
// it fapi answers and papi refuses.
type binancePMActivityServer struct {
	portfolioMargin bool
	// failUMFills makes /papi/v1/um/userTrades refuse every call.
	failUMFills bool

	umPositions, cmPositions []string
	umIncome, cmIncome       []pmIncomeRow
	umFills, cmFills, mFills []pmFill
	// crossAssets are the /papi/v1/balance assets holding cross margin.
	crossAssets []string
	// marginErrors are the answers margin/myTrades gives a symbol instead of
	// its fills: status and body.
	marginErrors map[string]pmError
	// fapiDenied refuses fapi on a classic account too: a key without the
	// futures scope, which papi refuses as well.
	fapiDenied bool
	// crossNetBtc is the /sapi/v1/margin/account net asset in BTC ("0" when
	// empty). portfolioAccount, when set, is the /sapi/v1/portfolio/account
	// answer; portfolioStatus replaces it with a refusal of that status.
	crossNetBtc      string
	portfolioAccount string
	portfolioStatus  int
	// coinMBalance is the /dapi/v1/balance answer ("[]" when empty).
	coinMBalance string
	// crossAssetBtc is the gross totalAssetOfBtc of /sapi/v1/margin/account.
	crossAssetBtc string
	// poolAmounts overrides the crossMarginAsset ("2") of a /sapi/v1/portfolio/balance asset.
	poolAmounts map[string]string
	// fapiAccount overrides the /fapi/v2/account answer; cmPositionRisk is the
	// /dapi/v1/positionRisk answer ("[]" when empty).
	fapiAccount    string
	cmPositionRisk string
	// deposits is the /sapi/v1/capital/deposit/hisrec answer ("[]" when empty).
	deposits string

	mu    sync.Mutex
	calls []*url.URL
}

func (s *binancePMActivityServer) count(path string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, u := range s.calls {
		if u.Path == path {
			n++
		}
	}
	return n
}

func (s *binancePMActivityServer) countPrefix(prefix string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, u := range s.calls {
		if strings.HasPrefix(u.Path, prefix) {
			n++
		}
	}
	return n
}

func (s *binancePMActivityServer) symbolsAsked(path string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for _, u := range s.calls {
		if u.Path == path && !slices.Contains(out, u.Query().Get("symbol")) {
			out = append(out, u.Query().Get("symbol"))
		}
	}
	slices.Sort(out)
	return out
}

func inRange(q url.Values, t time.Time) bool {
	from, _ := strconv.ParseInt(q.Get("startTime"), 10, 64)
	to, _ := strconv.ParseInt(q.Get("endTime"), 10, 64)
	ms := t.UnixMilli()
	return ms >= from && ms <= to
}

func (s *binancePMActivityServer) fills(q url.Values, fills []pmFill) []map[string]any {
	out := []map[string]any{}
	for _, f := range fills {
		if f.symbol == q.Get("symbol") && inRange(q, f.time) {
			row := map[string]any{"symbol": f.symbol, "time": f.time.UnixMilli()}
			for k, v := range f.row {
				row[k] = v
			}
			out = append(out, row)
		}
	}
	return out
}

func (s *binancePMActivityServer) income(q url.Values, rows []pmIncomeRow) []map[string]any {
	out := []map[string]any{}
	for _, r := range rows {
		if t := q.Get("incomeType"); t != "" && t != r.incomeType {
			continue
		}
		if inRange(q, r.time) {
			out = append(out, map[string]any{
				"symbol": r.symbol, "incomeType": r.incomeType, "income": r.income,
				"asset": r.asset, "time": r.time.UnixMilli(), "tranId": r.tranID,
			})
		}
	}
	return out
}

func positions(symbols []string) []map[string]any {
	out := []map[string]any{}
	for _, s := range symbols {
		out = append(out, map[string]any{"symbol": s, "positionAmt": "-0.5", "entryPrice": "51000", "markPrice": "50000", "unRealizedProfit": "500"})
	}
	return out
}

func (s *binancePMActivityServer) connector(t *testing.T) *Binance {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		s.calls = append(s.calls, r.URL)
		s.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		q := r.URL.Query()
		denied := func() {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"code":-2015,"msg":"Invalid API-key, IP, or permissions for action."}`))
		}
		write := func(v any) {
			body, _ := json.Marshal(v)
			_, _ = w.Write(body)
		}
		p := r.URL.Path
		switch {
		case strings.Contains(p, "/api/v3/ticker"):
			_, _ = w.Write([]byte(`[{"symbol":"BTCUSDT","price":"50000"},{"symbol":"ETHUSDT","price":"3000"}]`))
		case p == "/api/v3/account":
			_, _ = w.Write([]byte(`{"balances":[{"asset":"USDT","free":"100","locked":"0"}]}`))
		case strings.HasPrefix(p, "/fapi/"), strings.HasPrefix(p, "/dapi/"):
			if s.portfolioMargin || s.fapiDenied {
				denied()
				return
			}
			switch p {
			case "/fapi/v2/account":
				if s.fapiAccount != "" {
					_, _ = w.Write([]byte(s.fapiAccount))
					return
				}
				_, _ = w.Write([]byte(`{"totalMarginBalance":"700","totalUnrealizedProfit":"0","availableBalance":"600"}`))
			case "/fapi/v1/userTrades":
				_, _ = w.Write([]byte(`[{"id":1,"symbol":"BTCUSDT","price":"50000","qty":"0.01","commission":"0.2","commissionAsset":"USDT","time":` +
					strconv.FormatInt(q2ms(q), 10) + `,"side":"BUY","realizedPnl":"0"}]`))
			case "/fapi/v1/income":
				write(s.income(q, s.umIncome))
			case "/dapi/v1/positionRisk":
				if s.cmPositionRisk != "" {
					_, _ = w.Write([]byte(s.cmPositionRisk))
					return
				}
				_, _ = w.Write([]byte(`[]`))
			case "/dapi/v1/income":
				write(s.income(q, s.cmIncome))
			case "/dapi/v1/balance":
				if s.coinMBalance != "" {
					_, _ = w.Write([]byte(s.coinMBalance))
					return
				}
				_, _ = w.Write([]byte(`[]`))
			case "/fapi/v2/positionRisk":
				_, _ = w.Write([]byte(`[]`))
			default:
				_, _ = w.Write([]byte(`{}`))
			}
		case strings.HasPrefix(p, "/papi/"):
			if !s.portfolioMargin {
				denied()
				return
			}
			switch p {
			case "/papi/v1/account":
				_, _ = w.Write([]byte(`{"accountEquity":"4800","actualEquity":"5000","accountInitialMargin":"0","totalMarginOpenLoss":"0"}`))
			case "/papi/v1/balance":
				rows := []map[string]any{{"asset": "BNB", "crossMarginAsset": "0", "crossMarginBorrowed": "0"}}
				for _, a := range s.crossAssets {
					rows = append(rows, map[string]any{"asset": a, "crossMarginAsset": "2", "crossMarginBorrowed": "0", "umUnrealizedPNL": "0", "cmUnrealizedPNL": "0"})
				}
				write(rows)
			case "/papi/v1/um/positionRisk":
				write(positions(s.umPositions))
			case "/papi/v1/cm/positionRisk":
				write(positions(s.cmPositions))
			case "/papi/v1/um/income":
				write(s.income(q, s.umIncome))
			case "/papi/v1/cm/income":
				write(s.income(q, s.cmIncome))
			case "/papi/v1/um/userTrades":
				if s.failUMFills {
					w.WriteHeader(http.StatusBadRequest)
					_, _ = w.Write([]byte(`{"code":-1000,"msg":"An unknown error occurred while processing the request."}`))
					return
				}
				write(s.fills(q, s.umFills))
			case "/papi/v1/cm/userTrades":
				write(s.fills(q, s.cmFills))
			case "/papi/v1/margin/myTrades":
				if e, ok := s.marginErrors[q.Get("symbol")]; ok {
					w.WriteHeader(e.status)
					_, _ = w.Write([]byte(e.body))
					return
				}
				write(s.fills(q, s.mFills))
			default:
				_, _ = w.Write([]byte(`{}`))
			}
		case p == "/sapi/v1/portfolio/account":
			if s.portfolioStatus != 0 {
				w.WriteHeader(s.portfolioStatus)
				body := `{"code":-2015,"msg":"Invalid API-key, IP, or permissions for action."}`
				if s.portfolioAccount != "" {
					body = s.portfolioAccount
				}
				_, _ = w.Write([]byte(body))
				return
			}
			if s.portfolioAccount == "" {
				denied()
				return
			}
			_, _ = w.Write([]byte(s.portfolioAccount))
		case p == "/sapi/v1/portfolio/balance":
			rows := []map[string]any{{"asset": "BNB", "crossMarginAsset": "0", "crossMarginBorrowed": "0"}}
			for _, a := range s.crossAssets {
				amount := "2"
				if v, ok := s.poolAmounts[a]; ok {
					amount = v
				}
				rows = append(rows, map[string]any{"asset": a, "crossMarginAsset": amount, "crossMarginBorrowed": "0"})
			}
			write(rows)
		case p == "/sapi/v1/margin/myTrades":
			if e, ok := s.marginErrors[q.Get("symbol")]; ok {
				w.WriteHeader(e.status)
				_, _ = w.Write([]byte(e.body))
				return
			}
			write(s.fills(q, s.mFills))
		case p == "/sapi/v1/margin/account":
			net := s.crossNetBtc
			if net == "" {
				net = "0"
			}
			gross := s.crossAssetBtc
			if gross == "" {
				gross = net
			}
			_, _ = w.Write([]byte(`{"totalAssetOfBtc":"` + gross + `","totalLiabilityOfBtc":"0","totalNetAssetOfBtc":"` + net + `"}`))
		case p == "/sapi/v1/capital/deposit/hisrec":
			if s.deposits != "" {
				_, _ = w.Write([]byte(s.deposits))
				return
			}
			_, _ = w.Write([]byte(`[]`))
		case p == "/sapi/v1/capital/withdraw/history":
			_, _ = w.Write([]byte(`[]`))
		case strings.HasPrefix(p, "/sapi/v1/margin/"):
			_, _ = w.Write([]byte(`{"totalNetAssetOfBtc":"0"}`))
		case strings.HasPrefix(p, "/sapi/v1/simple-earn/"):
			_, _ = w.Write([]byte(`{"rows":[],"total":0}`))
		default:
			_, _ = w.Write([]byte(`{}`))
		}
	}))
	t.Cleanup(srv.Close)
	target, _ := url.Parse(srv.URL)
	return NewBinanceWithClient(&Credentials{APIKey: "k", APISecret: "s"},
		&http.Client{Transport: hostRewriter{base: http.DefaultTransport, target: target}})
}

// q2ms places the fake fapi fill at the start of the asked range.
func q2ms(q url.Values) int64 {
	v, _ := strconv.ParseInt(q.Get("startTime"), 10, 64)
	return v
}

var pmDay = time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)

func pmAccountWithActivity() *binancePMActivityServer {
	at := func(h int) time.Time { return pmDay.Add(time.Duration(h) * time.Hour) }
	return &binancePMActivityServer{
		portfolioMargin: true,
		umPositions:     []string{"BTCUSDT"},
		// ETHUSDT was opened and closed inside the day: only its income
		// names it, as it does 1000PEPEUSDT.
		umIncome: []pmIncomeRow{
			{symbol: "ETHUSDT", incomeType: "COMMISSION", income: "-0.6", asset: "USDT", time: at(3), tranID: 1},
			{symbol: "1000PEPEUSDT", incomeType: "COMMISSION", income: "-0.1", asset: "USDT", time: at(4), tranID: 2},
			{symbol: "BTCUSDT", incomeType: "FUNDING_FEE", income: "-1.5", asset: "USDT", time: at(8), tranID: 3},
			{symbol: "", incomeType: "TRANSFER", income: "100", asset: "USDT", time: at(1), tranID: 4},
		},
		cmIncome: []pmIncomeRow{
			{symbol: "BTCUSD_PERP", incomeType: "COMMISSION", income: "-0.00001", asset: "BTC", time: at(5), tranID: 1},
			{symbol: "BTCUSD_PERP", incomeType: "FUNDING_FEE", income: "-0.0001", asset: "BTC", time: at(16), tranID: 2},
		},
		umFills: []pmFill{
			{symbol: "BTCUSDT", time: at(2), row: map[string]any{"id": 11, "side": "SELL", "price": "50000", "qty": "0.5", "commission": "5", "commissionAsset": "USDT", "realizedPnl": "0"}},
			{symbol: "ETHUSDT", time: at(3), row: map[string]any{"id": 12, "side": "BUY", "price": "3000", "qty": "1", "commission": "0.6", "commissionAsset": "USDT", "realizedPnl": "0"}},
			{symbol: "ETHUSDT", time: at(6), row: map[string]any{"id": 13, "side": "SELL", "price": "3100", "qty": "1", "commission": "0.62", "commissionAsset": "USDT", "realizedPnl": "100"}},
		},
		cmFills: []pmFill{
			{symbol: "BTCUSD_PERP", time: at(5), row: map[string]any{"id": 21, "pair": "BTCUSD", "side": "BUY", "price": "50000", "qty": "10", "baseQty": "0.02", "commission": "0.00001", "commissionAsset": "BTC", "marginAsset": "BTC", "realizedPnl": "0"}},
		},
		crossAssets: []string{"ETH"},
		mFills: []pmFill{
			{symbol: "ETHUSDT", time: at(7), row: map[string]any{"id": 31, "isBuyer": true, "price": "3000", "qty": "0.1", "commission": "0.0001", "commissionAsset": "ETH"}},
		},
	}
}

func TestBinance_PortfolioMargin_TradesFromUMCMAndMargin(t *testing.T) {
	srv := pmAccountWithActivity()
	b := srv.connector(t)
	if _, err := b.GetBalance(context.Background()); err != nil {
		t.Fatalf("GetBalance: %v", err)
	}

	trades, err := b.GetTrades(context.Background(), pmDay, pmDay.Add(24*time.Hour))
	if err != nil {
		t.Fatalf("GetTrades: %v", err)
	}
	byID := map[string]*Trade{}
	for _, tr := range trades {
		byID[tr.MarketType+":"+tr.ID] = tr
	}
	if len(trades) != 5 {
		t.Fatalf("trades = %d (%v), want 5: one BTCUSDT and two ETHUSDT UM fills, one COIN-M, one margin", len(trades), byID)
	}
	for _, id := range []string{"swap:11", "swap:12", "swap:13", "swap:21", "spot:31"} {
		if byID[id] == nil {
			t.Fatalf("fill %s missing from %v", id, byID)
		}
	}
	if um := byID["swap:13"]; um.Side != "sell" || um.Fee != 0.62 || um.FeeCurrency != "USDT" || um.RealizedPnL != 100 || um.Notional() != 3100 {
		t.Fatalf("UM fill = %+v", um)
	}
	// 10 contracts of 100 USD = 0.02 BTC at 50000; the BTC fee valued at the
	// fill price.
	if cm := byID["swap:21"]; cm.Notional() != 1000 || math.Abs(cm.Fee-0.5) > 1e-9 || cm.FeeCurrency != "USD" || cm.Side != "buy" {
		t.Fatalf("COIN-M fill = %+v, want 1000 notional and a 0.5 USD fee", cm)
	}
	if m := byID["spot:31"]; m.Side != "buy" || m.Notional() != 300 || m.Fee != 0.0001 || m.FeeCurrency != "ETH" {
		t.Fatalf("margin fill = %+v", m)
	}

	if got := srv.symbolsAsked("/papi/v1/um/userTrades"); !slices.Equal(got, []string{"1000PEPEUSDT", "BTCUSDT", "ETHUSDT"}) {
		t.Fatalf("UM symbols asked = %v", got)
	}
	if got := srv.symbolsAsked("/papi/v1/cm/userTrades"); !slices.Equal(got, []string{"BTCUSD_PERP"}) {
		t.Fatalf("COIN-M symbols asked = %v", got)
	}
	if got := srv.symbolsAsked("/papi/v1/margin/myTrades"); !slices.Equal(got, []string{"ETHUSDC", "ETHUSDT"}) {
		t.Fatalf("margin symbols asked = %v, want only the held asset ETH against USDT and USDC", got)
	}
	if n := srv.count("/fapi/v1/userTrades"); n != 0 {
		t.Fatalf("fapi userTrades called %d times on a portfolio margin account", n)
	}
	if n := srv.count("/papi/v1/account"); n != 1 {
		t.Fatalf("papi account read %d times, want once (the balance read): the verdict is cached", n)
	}
	if w := b.CapabilityWarnings(); slices.Contains(w, binancePMTradesUnreadable) {
		t.Fatalf("warnings = %v after a clean read", w)
	}
}

func TestBinance_PortfolioMargin_VerdictProbedOnceWithoutBalanceRead(t *testing.T) {
	srv := pmAccountWithActivity()
	b := srv.connector(t)
	for range 2 {
		trades, err := b.GetTrades(context.Background(), pmDay, pmDay.Add(24*time.Hour))
		if err != nil || len(trades) != 5 {
			t.Fatalf("GetTrades = %d trades, %v; want 5", len(trades), err)
		}
	}
	if n := srv.count("/papi/v1/account"); n != 1 {
		t.Fatalf("papi probed %d times over two reads, want 1", n)
	}
	if n := srv.count("/fapi/v1/userTrades"); n != 1 {
		t.Fatalf("fapi userTrades called %d times, want 1 (the read that found the refusal)", n)
	}
}

func TestBinance_PortfolioMargin_FillWindows(t *testing.T) {
	srv := &binancePMActivityServer{portfolioMargin: true, umPositions: []string{"BTCUSDT"}, cmPositions: []string{"BTCUSD_PERP"}}
	b := srv.connector(t)
	if _, err := b.GetBalance(context.Background()); err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if _, err := b.GetTrades(context.Background(), pmDay, pmDay.Add(20*24*time.Hour)); err != nil {
		t.Fatalf("GetTrades: %v", err)
	}
	// UM: 7 + 7 + 6 days. COIN-M: twenty 24-hour windows. UM income: 7-day
	// windows; COIN-M income: one 200-day window. Margin: BTCUSDT is not in
	// the income and no cross asset is held, so nothing is asked.
	for path, want := range map[string]int{
		"/papi/v1/um/userTrades":   3,
		"/papi/v1/cm/userTrades":   20,
		"/papi/v1/um/income":       3,
		"/papi/v1/cm/income":       1,
		"/papi/v1/margin/myTrades": 0,
	} {
		if n := srv.count(path); n != want {
			t.Errorf("%s called %d times over 20 days, want %d", path, n, want)
		}
	}
	srv.mu.Lock()
	defer srv.mu.Unlock()
	for _, u := range srv.calls {
		q := u.Query()
		from, _ := strconv.ParseInt(q.Get("startTime"), 10, 64)
		to, _ := strconv.ParseInt(q.Get("endTime"), 10, 64)
		span := time.Duration(to-from) * time.Millisecond
		if (u.Path == "/papi/v1/um/userTrades" && span > 7*24*time.Hour) || (u.Path == "/papi/v1/cm/userTrades" && span > 24*time.Hour) {
			t.Fatalf("%s asked for %v, over its cap", u.Path, span)
		}
	}
}

func TestBinance_PortfolioMargin_FundingFeesFromIncome(t *testing.T) {
	now := time.Now().UTC()
	day := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	srv := &binancePMActivityServer{
		portfolioMargin: true,
		umIncome: []pmIncomeRow{
			{symbol: "BTCUSDT", incomeType: "FUNDING_FEE", income: "-1.5", asset: "USDT", time: day.Add(-16 * time.Hour), tranID: 1},
			{symbol: "BTCUSDT", incomeType: "FUNDING_FEE", income: "0.5", asset: "USDT", time: day.Add(-8 * time.Hour), tranID: 2},
			{symbol: "BTCUSDT", incomeType: "COMMISSION", income: "-9", asset: "USDT", time: day.Add(-8 * time.Hour), tranID: 3},
			// The day before the window.
			{symbol: "BTCUSDT", incomeType: "FUNDING_FEE", income: "-7", asset: "USDT", time: day.Add(-30 * time.Hour), tranID: 4},
		},
		cmIncome: []pmIncomeRow{
			{symbol: "BTCUSD_PERP", incomeType: "FUNDING_FEE", income: "-0.0001", asset: "BTC", time: day.Add(-4 * time.Hour), tranID: 1},
		},
	}
	b := srv.connector(t)
	if _, err := b.GetBalance(context.Background()); err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	fees, err := b.GetFundingFees(context.Background(), nil, day.Add(-24*time.Hour))
	if err != nil {
		t.Fatalf("GetFundingFees: %v", err)
	}
	sum := 0.0
	for _, f := range fees {
		sum += f.Amount
	}
	// -1.5 + 0.5 USDT, and 0.0001 BTC at 50000 = -5.
	if len(fees) != 3 || math.Abs(sum-(-6)) > 1e-9 {
		t.Fatalf("funding = %d rows summing %v, want 3 rows summing -6", len(fees), sum)
	}
}

func TestBinance_PortfolioMargin_UnreadableFillsRaiseMarker(t *testing.T) {
	srv := pmAccountWithActivity()
	srv.failUMFills = true
	b := srv.connector(t)
	if _, err := b.GetBalance(context.Background()); err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if _, err := b.GetTrades(context.Background(), pmDay, pmDay.Add(24*time.Hour)); err == nil {
		t.Fatal("GetTrades hid the papi failure")
	}
	if !slices.Contains(b.CapabilityWarnings(), binancePMTradesUnreadable) {
		t.Fatalf("warnings = %v, want %s", b.CapabilityWarnings(), binancePMTradesUnreadable)
	}
	// The next clean read clears it.
	srv.failUMFills = false
	if _, err := b.GetTrades(context.Background(), pmDay, pmDay.Add(24*time.Hour)); err != nil {
		t.Fatalf("GetTrades: %v", err)
	}
	if slices.Contains(b.CapabilityWarnings(), binancePMTradesUnreadable) {
		t.Fatal("marker kept after a clean read")
	}
}

// A classic account must issue exactly the requests it did before Portfolio
// Margin activity existed: not one papi call, and the same fapi fills.
func TestBinance_NonPortfolioMargin_NoPapiCalls(t *testing.T) {
	srv := &binancePMActivityServer{portfolioMargin: false}
	b := srv.connector(t)
	ctx := context.Background()
	if _, err := b.GetBalance(ctx); err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	trades, err := b.GetTrades(ctx, pmDay, pmDay.Add(24*time.Hour))
	if err != nil {
		t.Fatalf("GetTrades: %v", err)
	}
	if len(trades) != 1 || trades[0].MarketType != MarketSwap || trades[0].ID != "1" {
		t.Fatalf("trades = %+v, want the one fapi fill", trades)
	}
	if _, err := b.GetPositions(ctx); err != nil {
		t.Fatalf("GetPositions: %v", err)
	}
	fees, err := b.GetFundingFees(ctx, []string{"BTCUSDT"}, pmDay)
	if !errors.Is(err, ErrFundingNotRead) || fees != nil {
		t.Fatalf("GetFundingFees = %v, %v; want ErrFundingNotRead: no funding is read", fees, err)
	}
	if n := srv.countPrefix("/papi/"); n != 0 {
		t.Fatalf("%d papi calls on a classic account, want 0", n)
	}
	if n := srv.count("/fapi/v1/userTrades"); n != 1 {
		t.Fatalf("fapi userTrades called %d times, want 1", n)
	}
	if n := srv.count("/api/v3/myTrades"); n != 0 {
		t.Fatalf("spot myTrades called %d times, want 0 (only USDT held)", n)
	}

	// Same without a balance read first (the recount path).
	srv2 := &binancePMActivityServer{portfolioMargin: false}
	b2 := srv2.connector(t)
	if _, err := b2.GetTrades(ctx, pmDay, pmDay.Add(24*time.Hour)); err != nil {
		t.Fatalf("GetTrades: %v", err)
	}
	if _, err := b2.GetFundingFees(ctx, nil, pmDay); !errors.Is(err, ErrFundingNotRead) {
		t.Fatalf("GetFundingFees: %v", err)
	}
	if n := srv2.countPrefix("/papi/"); n != 0 {
		t.Fatalf("%d papi calls on a classic account read without a balance first, want 0", n)
	}
}

// pmError is a refusal the papi fake answers instead of a page.
type pmError struct {
	status int
	body   string
}

// One pair the venue refuses for good (-1100 here) costs that pair only: the
// UM, COIN-M and other margin fills still count, and no marker is raised.
func TestBinance_PortfolioMargin_OneMarginPairRefusedKeepsTheRest(t *testing.T) {
	srv := pmAccountWithActivity()
	srv.crossAssets = []string{"ETH", "SOL"}
	srv.marginErrors = map[string]pmError{
		"SOLUSDT": {http.StatusBadRequest, `{"code":-1100,"msg":"Illegal characters found in parameter 'symbol'."}`},
		"SOLUSDC": {http.StatusBadRequest, `{"code":-1121,"msg":"Invalid symbol."}`},
	}
	b := srv.connector(t)
	if _, err := b.GetBalance(context.Background()); err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	trades, err := b.GetTrades(context.Background(), pmDay, pmDay.Add(24*time.Hour))
	if err != nil {
		t.Fatalf("GetTrades: %v", err)
	}
	if len(trades) != 5 {
		t.Fatalf("trades = %d, want the 5 fills of the pairs that answered", len(trades))
	}
	if got := srv.symbolsAsked("/papi/v1/margin/myTrades"); !slices.Equal(got, []string{"ETHUSDC", "ETHUSDT", "SOLUSDC", "SOLUSDT"}) {
		t.Fatalf("margin symbols asked = %v", got)
	}
	if slices.Contains(b.CapabilityWarnings(), binancePMTradesUnreadable) {
		t.Fatal("a refused pair raised the marker")
	}
}

// A ban is not a refusal of the pair: the read stops and the gap is marked.
func TestBinance_PortfolioMargin_BannedMarginPairRaisesMarker(t *testing.T) {
	srv := pmAccountWithActivity()
	srv.marginErrors = map[string]pmError{
		"ETHUSDC": {http.StatusTeapot, `{"code":-1003,"msg":"Way too many requests; IP banned."}`},
	}
	b := srv.connector(t)
	if _, err := b.GetBalance(context.Background()); err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if _, err := b.GetTrades(context.Background(), pmDay, pmDay.Add(24*time.Hour)); err == nil {
		t.Fatal("GetTrades hid the ban")
	}
	if !slices.Contains(b.CapabilityWarnings(), binancePMTradesUnreadable) {
		t.Fatalf("warnings = %v, want %s", b.CapabilityWarnings(), binancePMTradesUnreadable)
	}
}

// An exact day is one call per COIN-M and per margin symbol, the margin one
// stopping 1 ms short of the next day ("less than 24 hours").
func TestBinance_PortfolioMargin_ExactDayIsOneCallPerSymbol(t *testing.T) {
	srv := &binancePMActivityServer{portfolioMargin: true, cmPositions: []string{"BTCUSD_PERP"}, crossAssets: []string{"ETH"}}
	b := srv.connector(t)
	if _, err := b.GetBalance(context.Background()); err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if _, err := b.GetTrades(context.Background(), pmDay, pmDay.Add(24*time.Hour)); err != nil {
		t.Fatalf("GetTrades: %v", err)
	}
	if n := srv.count("/papi/v1/cm/userTrades"); n != 1 {
		t.Fatalf("COIN-M calls = %d, want 1", n)
	}
	if n := srv.count("/papi/v1/margin/myTrades"); n != 2 {
		t.Fatalf("margin calls = %d, want 2 (ETHUSDT, ETHUSDC)", n)
	}
	srv.mu.Lock()
	defer srv.mu.Unlock()
	want := strconv.FormatInt(pmDay.Add(24*time.Hour-time.Millisecond).UnixMilli(), 10)
	for _, u := range srv.calls {
		if u.Path == "/papi/v1/margin/myTrades" && u.Query().Get("endTime") != want {
			t.Fatalf("margin endTime = %s, want %s", u.Query().Get("endTime"), want)
		}
	}
}

// A long read spends its papi weight minute by minute instead of all at once.
func TestBinance_PortfolioMargin_PapiPacedUnderBudget(t *testing.T) {
	srv := &binancePMActivityServer{portfolioMargin: true, umPositions: []string{"BTCUSDT"}, cmPositions: []string{"BTCUSD_PERP"}}
	b := srv.connector(t)
	clock := pmDay
	var slept []time.Duration
	b.pacer.budget = 100
	b.pacer.now = func() time.Time { return clock }
	b.pacer.sleep = func(_ context.Context, d time.Duration) error {
		slept = append(slept, d)
		clock = clock.Add(d)
		return nil
	}
	if _, err := b.GetBalance(context.Background()); err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if _, err := b.GetTrades(context.Background(), pmDay, pmDay.Add(20*24*time.Hour)); err != nil {
		t.Fatalf("GetTrades: %v", err)
	}
	srv.mu.Lock()
	spent := 0
	for _, u := range srv.calls {
		if strings.HasPrefix(u.Path, "/papi/") {
			spent += binancePacedWeights[u.Path]
		}
	}
	srv.mu.Unlock()
	// 20 COIN-M calls alone weigh 400: at 100 a minute that is at least four
	// waits, each no longer than the rest of its minute.
	if minWaits := (spent+99)/100 - 1; len(slept) < minWaits {
		t.Fatalf("waited %d times for %d weight at 100/min, want at least %d", len(slept), spent, minWaits)
	}
	for _, d := range slept {
		if d <= 0 || d > time.Minute {
			t.Fatalf("waited %v, want within one minute", d)
		}
	}
}

func TestBinance_PapiPacer_StopsOnCancelledContext(t *testing.T) {
	p := &binanceWeightPacer{budget: 10}
	ctx, cancel := context.WithCancel(context.Background())
	if err := p.wait(ctx, 10); err != nil {
		t.Fatalf("first wait: %v", err)
	}
	cancel()
	if err := p.wait(ctx, 10); !errors.Is(err, context.Canceled) {
		t.Fatalf("wait over budget on a cancelled context = %v, want context.Canceled", err)
	}
}

// A classic key without the futures scope: fapi and papi both refuse. The
// balance read settles that once, and the trade read then asks papi nothing.
func TestBinance_SpotOnlyKey_NoPapiCallsInGetTrades(t *testing.T) {
	srv := &binancePMActivityServer{portfolioMargin: false, fapiDenied: true}
	b := srv.connector(t)
	ctx := context.Background()
	if _, err := b.GetBalance(ctx); err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	before := srv.countPrefix("/papi/")
	if _, err := b.GetTrades(ctx, pmDay, pmDay.Add(24*time.Hour)); err != nil {
		t.Fatalf("GetTrades: %v", err)
	}
	if _, err := b.GetFundingFees(ctx, nil, pmDay); !errors.Is(err, ErrFundingNotRead) {
		t.Fatalf("GetFundingFees err = %v, want ErrFundingNotRead", err)
	}
	if n := srv.countPrefix("/papi/") - before; n != 0 {
		t.Fatalf("%d papi calls after the balance read, want 0", n)
	}
}
