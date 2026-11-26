package connector

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const polymarketTestWallet = "0x00000000000000000000000000000000000a11ce"

// polymarketServer serves one fixed body per path, and counts requests.
func polymarketServer(t *testing.T, routes map[string]string) (*Polymarket, *atomic.Int32) {
	t.Helper()
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		body, ok := routes[r.URL.Path]
		if !ok {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	p := NewPolymarket(&Credentials{APIKey: polymarketTestWallet})
	p.baseURL = srv.URL
	return p, &hits
}

const polymarketTestFlows = `{"data":[
	{"timestamp":1000,"type":"DEPOSIT","side":"","size":500,"usdc_size":500},
	{"timestamp":2000,"type":"TIP","side":"OUT","size":120,"usdc_size":120},
	{"timestamp":3000,"type":"WITHDRAWAL","side":"","size":80,"usdc_size":80},
	{"timestamp":4000,"type":"TIP","side":"IN","size":0,"usdc_size":25}
],"pagination":{"next_cursor":null}}`

func TestPolymarketGetBalance_TransfersPlusEconomicPnL(t *testing.T) {
	p, _ := polymarketServer(t, map[string]string{
		"/user-stats": `{"data":{"all_time_pnl":{"timestamp":5000,"economic_pnl":75.5,"unrealized_pnl":-4.5}}}`,
		"/activity":   polymarketTestFlows,
		"/value":      `{"data":{"value":60}}`,
	})
	bal, err := p.GetBalance(context.Background())
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	// 500 - 120 - 80 + 25 of transfers, plus 75.5 of pnl.
	if !approxEq(bal.Equity, 400.5) {
		t.Errorf("equity = %v, want 400.5", bal.Equity)
	}
	if !approxEq(bal.Available, 340.5) {
		t.Errorf("available = %v, want equity less the 60 held in positions", bal.Available)
	}
	if !approxEq(bal.UnrealizedPnL, -4.5) {
		t.Errorf("unrealized = %v, want -4.5", bal.UnrealizedPnL)
	}
}

func TestPolymarketGetBalance_FundedWalletWithoutPnLYet(t *testing.T) {
	p, _ := polymarketServer(t, map[string]string{
		"/user-stats": `{"data":{"all_time_pnl":null}}`,
		"/activity":   `{"data":[{"timestamp":1000,"type":"DEPOSIT","usdc_size":250}],"pagination":{"next_cursor":null}}`,
		"/value":      `{"data":{"value":0}}`,
	})
	bal, err := p.GetBalance(context.Background())
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if !approxEq(bal.Equity, 250) || !approxEq(bal.Available, 250) {
		t.Errorf("equity, available = %v, %v, want the deposit on both", bal.Equity, bal.Available)
	}
}

func TestPolymarketGetBalance_Refusals(t *testing.T) {
	cases := []struct {
		name      string
		stats     string
		flows     string
		transient bool
		is        error
	}{
		{
			name:  "no account at the address",
			stats: `{"data":null}`,
			flows: `{"data":[]}`,
		},
		{
			name:  "pnl deeper than every transfer in",
			stats: `{"data":{"all_time_pnl":{"timestamp":5000,"economic_pnl":-900,"unrealized_pnl":0}}}`,
			flows: polymarketTestFlows,
			is:    ErrPolymarketUnreconciled,
		},
		{
			name:      "pnl not marked",
			stats:     `{"data":{"all_time_pnl":{"timestamp":5000,"economic_pnl":null}}}`,
			flows:     polymarketTestFlows,
			transient: true,
			is:        ErrTransient,
		},
		{
			name:  "transfer without a direction",
			stats: `{"data":{"all_time_pnl":null}}`,
			flows: `{"data":[{"timestamp":1000,"type":"TIP","side":"","usdc_size":10}]}`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, _ := polymarketServer(t, map[string]string{
				"/user-stats": tc.stats,
				"/activity":   tc.flows,
				"/value":      `{"data":{"value":0}}`,
			})
			bal, err := p.GetBalance(context.Background())
			if err == nil {
				t.Fatalf("GetBalance returned equity %v, want a refusal", bal.Equity)
			}
			if tc.is != nil && !errors.Is(err, tc.is) {
				t.Errorf("error = %v, want %v", err, tc.is)
			}
			if got := errors.Is(err, ErrTransient); got != tc.transient {
				t.Errorf("transient = %v, want %v: %v", got, tc.transient, err)
			}
		})
	}
}

func TestPolymarket_MalformedAddressNeverLeaves(t *testing.T) {
	for _, addr := range []string{"", "alice", "0x1234", polymarketTestWallet + "00", "https://example.test/@alice"} {
		p, hits := polymarketServer(t, map[string]string{})
		p.wallet = addr
		if err := p.TestConnection(context.Background()); err == nil {
			t.Errorf("address %q accepted", addr)
		}
		if hits.Load() != 0 {
			t.Errorf("address %q reached the venue", addr)
		}
	}
}

func TestPolymarketGetCashflows_FollowsCursorAndRestatesFilters(t *testing.T) {
	var pages atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if q.Get("user") != polymarketTestWallet || q.Get("type") != "DEPOSIT,WITHDRAWAL,TIP" ||
			q.Get("exclude_deposits_withdrawals") != "false" || q.Get("start") != "1500" {
			t.Errorf("page %d dropped a filter: %s", pages.Load(), r.URL.RawQuery)
		}
		pages.Add(1)
		switch q.Get("cursor") {
		case "":
			_, _ = w.Write([]byte(`{"data":[{"timestamp":2000,"type":"DEPOSIT","usdc_size":300}],"pagination":{"next_cursor":"p2"}}`))
		case "p2":
			_, _ = w.Write([]byte(`{"data":[{"timestamp":3000,"type":"WITHDRAWAL","usdc_size":40},{"timestamp":3500,"type":"TIP","side":"out","usdc_size":10}],"pagination":{"next_cursor":null}}`))
		default:
			t.Errorf("unexpected cursor %q", q.Get("cursor"))
		}
	}))
	defer srv.Close()
	p := NewPolymarket(&Credentials{APIKey: strings.ToUpper(polymarketTestWallet[2:])})
	p.wallet = polymarketTestWallet
	p.baseURL = srv.URL

	flows, err := p.GetCashflows(context.Background(), time.Unix(1500, 0))
	if err != nil {
		t.Fatalf("GetCashflows: %v", err)
	}
	want := []float64{300, -40, -10}
	if len(flows) != len(want) {
		t.Fatalf("got %d flows, want %d", len(flows), len(want))
	}
	for i, w := range want {
		if !approxEq(flows[i].Amount, w) {
			t.Errorf("flow %d = %v, want %v", i, flows[i].Amount, w)
		}
	}
	if !flows[0].Timestamp.Equal(time.Unix(2000, 0)) {
		t.Errorf("first flow stamped %v", flows[0].Timestamp)
	}
	if pages.Load() != 2 {
		t.Errorf("read %d pages, want 2", pages.Load())
	}
}

func TestPolymarketFetchFlows_FeedPastTheCapFails(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"data":[{"timestamp":2000,"type":"DEPOSIT","usdc_size":1}],"pagination":{"next_cursor":"again"}}`))
	}))
	defer srv.Close()
	p := NewPolymarket(&Credentials{APIKey: polymarketTestWallet})
	p.baseURL = srv.URL
	if _, err := p.GetCashflows(context.Background(), time.Time{}); err == nil {
		t.Fatal("an endless transfer feed was summed as if complete")
	}
}

func TestPolymarketGetTradesAndPositions(t *testing.T) {
	p, _ := polymarketServer(t, map[string]string{
		"/activity": `{"data":[
			{"timestamp":7000,"type":"TRADE","side":"BUY","size":10,"usdc_size":4.2,"price":0.42,"slug":"rain-tomorrow","outcome":"Yes","transaction_hash":"0xfeed"},
			{"timestamp":7100,"type":"TRADE","side":"SELL","size":4,"usdc_size":2,"price":0.5,"slug":"","outcome":"","token_id":"77"},
			{"timestamp":7200,"type":"REDEEM","side":"","size":6,"usdc_size":6}
		],"pagination":{"next_cursor":null}}`,
		"/positions": `{"data":[
			{"slug":"rain-tomorrow","outcome":"Yes","current_size":6,"avg_price":0.42,"current_price":0.6,"unrealized_pnl":1.08},
			{"slug":"settled","outcome":"No","current_size":0}
		],"pagination":{"next_cursor":null}}`,
	})
	trades, err := p.GetTrades(context.Background(), time.Unix(6000, 0), time.Unix(8000, 0))
	if err != nil {
		t.Fatalf("GetTrades: %v", err)
	}
	if len(trades) != 2 {
		t.Fatalf("got %d trades, want the two fills and not the redemption", len(trades))
	}
	if tr := trades[0]; tr.Symbol != "rain-tomorrow/Yes" || tr.Side != "buy" || !approxEq(tr.Notional(), 4.2) || tr.MarketType != MarketSpot {
		t.Errorf("first trade = %+v", tr)
	}
	if tr := trades[1]; tr.Symbol != "77" || tr.Side != "sell" {
		t.Errorf("second trade = %+v", tr)
	}

	positions, err := p.GetPositions(context.Background())
	if err != nil {
		t.Fatalf("GetPositions: %v", err)
	}
	if len(positions) != 1 {
		t.Fatalf("got %d positions, want the one still held", len(positions))
	}
	if pos := positions[0]; pos.Side != "long" || !approxEq(pos.Size, 6) || !approxEq(pos.MarkPrice, 0.6) || !approxEq(pos.UnrealizedPnL, 1.08) {
		t.Errorf("position = %+v", pos)
	}
}

func TestPolymarket_ErrorsNeverCarryTheAddress(t *testing.T) {
	t.Run("venue error echoing the address", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":"invalid user address: ` + polymarketTestWallet + `"}`))
		}))
		defer srv.Close()
		p := NewPolymarket(&Credentials{APIKey: polymarketTestWallet})
		p.baseURL = srv.URL
		_, err := p.GetBalance(context.Background())
		if err == nil {
			t.Fatal("want an error")
		}
		if strings.Contains(err.Error(), polymarketTestWallet) {
			t.Errorf("error carries the address: %v", err)
		}
		if errors.Is(err, ErrTransient) {
			t.Errorf("a 400 is a verdict, not a transient failure: %v", err)
		}
	})
	t.Run("unreachable venue", func(t *testing.T) {
		srv := httptest.NewServer(http.NotFoundHandler())
		p := NewPolymarket(&Credentials{APIKey: polymarketTestWallet})
		p.baseURL = srv.URL
		srv.Close()
		_, err := p.GetBalance(context.Background())
		if !errors.Is(err, ErrTransient) {
			t.Fatalf("error = %v, want transient", err)
		}
		if strings.Contains(err.Error(), polymarketTestWallet) {
			t.Errorf("error carries the address: %v", err)
		}
	})
	t.Run("throttled", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusTooManyRequests)
		}))
		defer srv.Close()
		p := NewPolymarket(&Credentials{APIKey: polymarketTestWallet})
		p.baseURL = srv.URL
		if _, err := p.GetBalance(context.Background()); !errors.Is(err, ErrTransient) {
			t.Fatalf("error = %v, want transient", err)
		}
	})
}

func f64(v float64) *float64 { return &v }

func polymarketDay(d int) time.Time {
	return time.Date(2031, time.March, d, 0, 0, 0, 0, time.UTC)
}

func TestPolymarketDailyRows(t *testing.T) {
	flows := []polymarketFlow{
		{at: polymarketDay(1).Add(20 * time.Hour), amount: 1000},
		{at: polymarketDay(3).Add(6 * time.Hour), amount: -200},
		{at: polymarketDay(5), amount: 50},
	}
	points := []polymarketPnL{
		{Timestamp: polymarketDay(3).Unix(), Economic: f64(30), Unrealized: f64(5), Fees: f64(-1), VolumeUSDC: 400, TradeCount: 12},
		{Timestamp: polymarketDay(4).Unix(), Economic: nil, VolumeUSDC: 700, TradeCount: 20},
		{Timestamp: polymarketDay(5).Unix(), Economic: f64(10), Unrealized: f64(0), Fees: f64(-4), VolumeUSDC: 900, TradeCount: 27},
		// The series ends on the instant it was read, not on a midnight.
		{Timestamp: polymarketDay(6).Add(9 * time.Hour).Unix(), Economic: f64(999), VolumeUSDC: 5000, TradeCount: 90},
	}
	now := polymarketDay(6).Add(9 * time.Hour)

	rows, err := polymarketDailyRows(flows, points, time.Time{}, now)
	if err != nil {
		t.Fatalf("polymarketDailyRows: %v", err)
	}
	type want struct {
		day                                     int
		equity, realized, deposits, withdrawals float64
		trades                                  int
		volume, fees                            float64
	}
	wants := []want{
		{day: 2, equity: 1000, realized: 1000, deposits: 1000},
		{day: 3, equity: 1030, realized: 1025, trades: 12, volume: 400, fees: 1},
		// Day 4 has no mark: left out, its transfer and activity carried on.
		{day: 5, equity: 860, realized: 860, deposits: 50, trades: 7, volume: 200},
		{day: 6, equity: 860, realized: 860},
	}
	if len(rows) != len(wants) {
		t.Fatalf("got %d rows, want %d", len(rows), len(wants))
	}
	for i, w := range wants {
		r := rows[i]
		if !r.Date.Equal(polymarketDay(w.day)) {
			t.Fatalf("row %d dated %v, want day %d", i, r.Date, w.day)
		}
		if !approxEq(r.TotalEquity, w.equity) || !approxEq(r.RealizedBalance, w.realized) {
			t.Errorf("day %d equity, realized = %v, %v, want %v, %v", w.day, r.TotalEquity, r.RealizedBalance, w.equity, w.realized)
		}
		if !approxEq(r.Deposits, w.deposits) || !approxEq(r.Withdrawals, w.withdrawals) {
			t.Errorf("day %d deposits, withdrawals = %v, %v, want %v, %v", w.day, r.Deposits, r.Withdrawals, w.deposits, w.withdrawals)
		}
		if r.TotalTrades != w.trades || !approxEq(r.TotalVolume, w.volume) || !approxEq(r.TotalFees, w.fees) {
			t.Errorf("day %d trades, volume, fees = %v, %v, %v, want %v, %v, %v", w.day, r.TotalTrades, r.TotalVolume, r.TotalFees, w.trades, w.volume, w.fees)
		}
	}

	t.Run("a later start keeps the running totals", func(t *testing.T) {
		rows, err := polymarketDailyRows(flows, points, polymarketDay(5).Add(3*time.Hour), now)
		if err != nil {
			t.Fatalf("polymarketDailyRows: %v", err)
		}
		if len(rows) != 2 || !rows[0].Date.Equal(polymarketDay(5)) || !approxEq(rows[0].TotalEquity, 860) {
			t.Fatalf("rows = %+v", rows)
		}
	})
	t.Run("an empty wallet has no history", func(t *testing.T) {
		rows, err := polymarketDailyRows(nil, nil, time.Time{}, now)
		if err != nil || rows != nil {
			t.Fatalf("rows, err = %v, %v", rows, err)
		}
	})
	t.Run("a day below zero fails the whole history", func(t *testing.T) {
		deep := []polymarketPnL{{Timestamp: polymarketDay(3).Unix(), Economic: f64(-5000)}}
		if _, err := polymarketDailyRows(flows, deep, time.Time{}, now); !errors.Is(err, ErrPolymarketUnreconciled) {
			t.Fatalf("error = %v, want ErrPolymarketUnreconciled", err)
		}
	})
}

func TestPolymarketDailyRows_RebatesReadAsNegativeFees(t *testing.T) {
	points := []polymarketPnL{
		{Timestamp: polymarketDay(2).Unix(), Economic: f64(0), Fees: f64(-2)},
		// Charges up by 1, a refund of 0.5 and rebates of 4 and 0.5.
		{Timestamp: polymarketDay(3).Unix(), Economic: f64(3), Fees: f64(-3), FeesRefunded: f64(0.5), MakerRebate: f64(4), TakerRebate: f64(0.5)},
		{Timestamp: polymarketDay(4).Unix(), Economic: f64(3), Fees: f64(-3), FeesRefunded: f64(0.5), MakerRebate: f64(4), TakerRebate: f64(0.5)},
	}
	flows := []polymarketFlow{{at: polymarketDay(1).Add(time.Hour), amount: 100}}

	rows, err := polymarketDailyRows(flows, points, time.Time{}, polymarketDay(4))
	if err != nil {
		t.Fatalf("polymarketDailyRows: %v", err)
	}
	want := map[int]float64{2: 2, 3: -4, 4: 0}
	if len(rows) != len(want) {
		t.Fatalf("got %d rows, want %d", len(rows), len(want))
	}
	for _, r := range rows {
		if w := want[r.Date.Day()]; !approxEq(r.TotalFees, w) {
			t.Errorf("day %d fees = %v, want %v", r.Date.Day(), r.TotalFees, w)
		}
	}
}

func TestPolymarketGetNetTradingFees(t *testing.T) {
	since := polymarketDay(4)
	hour := func(h int) int64 { return since.Add(time.Duration(h) * time.Hour).Unix() }
	series := `{"data":{"points":[
		{"timestamp":` + strconv.FormatInt(hour(-1), 10) + `,"fees":-1,"maker_rebate":2},
		{"timestamp":` + strconv.FormatInt(hour(0), 10) + `,"fees":-1.5,"maker_rebate":3},
		{"timestamp":` + strconv.FormatInt(hour(23), 10) + `,"fees":-2,"maker_rebate":9},
		{"timestamp":` + strconv.FormatInt(hour(24), 10) + `,"fees":-2.25,"maker_rebate":10,"fees_refunded":0.25}
	]}}`

	cases := []struct {
		name               string
		until              time.Time
		interval, fidelity string
		want               float64
	}{
		{"a day on the hourly grid", since.Add(24*time.Hour + 5*time.Minute), "1w", "1h", -6.5},
		{"a mark short of the window end", since.Add(23*time.Hour + 30*time.Minute), "1w", "1h", -5.5},
		{"a catch-up over two weeks", since.Add(14 * 24 * time.Hour), "1m", "1h", -6.5},
		{"a catch-up past a month", since.Add(40 * 24 * time.Hour), "max", "1d", -6.5},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				q := r.URL.Query()
				if r.URL.Path != "/user-pnl" || q.Get("user") != polymarketTestWallet ||
					q.Get("interval") != tc.interval || q.Get("fidelity") != tc.fidelity {
					t.Errorf("request %s?%s, want interval %s at fidelity %s", r.URL.Path, r.URL.RawQuery, tc.interval, tc.fidelity)
				}
				_, _ = w.Write([]byte(series))
			}))
			defer srv.Close()
			p := NewPolymarket(&Credentials{APIKey: polymarketTestWallet})
			p.baseURL = srv.URL
			got, err := p.GetNetTradingFees(context.Background(), since, tc.until)
			if err != nil {
				t.Fatalf("GetNetTradingFees: %v", err)
			}
			if !approxEq(got, tc.want) {
				t.Errorf("fees = %v, want %v", got, tc.want)
			}
		})
	}

	t.Run("a wallet younger than the window paid from zero", func(t *testing.T) {
		got, err := polymarketFeesBetween([]polymarketPnL{
			{Timestamp: hour(5), Fees: f64(-1), MakerRebate: f64(0.25)},
		}, since, since.Add(24*time.Hour))
		if err != nil || !approxEq(got, 0.75) {
			t.Fatalf("fees, err = %v, %v, want 0.75", got, err)
		}
	})
	t.Run("a wallet without a mark paid nothing", func(t *testing.T) {
		got, err := polymarketFeesBetween(nil, since, since.Add(24*time.Hour))
		if err != nil || got != 0 {
			t.Fatalf("fees, err = %v, %v, want 0", got, err)
		}
	})
	t.Run("charges the venue cannot state fail the read", func(t *testing.T) {
		for _, pts := range [][]polymarketPnL{
			{{Timestamp: hour(0), Fees: f64(-1)}, {Timestamp: hour(24)}},
			{{Timestamp: hour(0)}, {Timestamp: hour(24), Fees: f64(-1)}},
		} {
			if got, err := polymarketFeesBetween(pts, since, since.Add(24*time.Hour)); err == nil {
				t.Errorf("fees %v stated from %+v", got, pts)
			}
		}
	})
}

func TestPolymarketGetHistoricalSnapshots(t *testing.T) {
	start := truncUTCDay(time.Now().UTC()).Add(-72 * time.Hour)
	stamp := func(d time.Duration) string { return polymarketEpoch(start.Add(d)) }
	p, _ := polymarketServer(t, map[string]string{
		"/activity": `{"data":[{"timestamp":` + stamp(-time.Hour) + `,"type":"DEPOSIT","usdc_size":100}],"pagination":{"next_cursor":null}}`,
		"/user-pnl": `{"data":{"points":[
			{"timestamp":` + stamp(24*time.Hour) + `,"economic_pnl":8,"unrealized_pnl":2,"volume_usdc":50,"trade_count":3}
		]}}`,
	})
	rows, err := p.GetHistoricalSnapshots(context.Background(), time.Time{})
	if err != nil {
		t.Fatalf("GetHistoricalSnapshots: %v", err)
	}
	if len(rows) != 4 {
		t.Fatalf("got %d rows, want one per midnight up to today's", len(rows))
	}
	if !rows[0].Date.Equal(start) || !approxEq(rows[0].TotalEquity, 100) || !approxEq(rows[0].Deposits, 100) {
		t.Errorf("first row = %+v", rows[0])
	}
	if !approxEq(rows[1].TotalEquity, 108) || !approxEq(rows[1].RealizedBalance, 106) || rows[1].TotalTrades != 3 {
		t.Errorf("second row = %+v", rows[1])
	}
	if !approxEq(rows[3].TotalEquity, 108) || rows[3].TotalTrades != 0 {
		t.Errorf("last row = %+v, want the last mark carried with no activity", rows[3])
	}
}
