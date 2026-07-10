// Package rebuilderclient is the enclave's HTTP client to history-rebuilder-go
// (lives in track_record_site/history-rebuilder-go).
//
// SEC-ZK-001: this package deliberately leaks plaintext credentials across
// the SEV-SNP attestation perimeter. The receiving service runs OUTSIDE the
// enclave; historical snapshots produced from the leaked creds are NOT
// signed by the report chain. This is an explicit product decision —
// historical reconstruction is not sold as verifiable. Do not extend this
// client to carry data that IS sold as verifiable (live snapshots, signed
// reports, attestations) — those must stay inside the enclave.
//
// Used only for non-IBKR exchanges (Hyperliquid, Lighter, Bitget, …). IBKR
// keeps its in-enclave Flex-based rebuild because the verification stays
// inside the ZK perimeter (single cheap Flex call, signed by the report chain).
//
// The wire contract is request/response: POST /history/rebuild blocks for
// the duration of the rebuild (HL: ~30-60s) and returns the snapshots in
// the body. The aggregator (this caller) is the sole writer of user
// snapshots — the rebuilder is stateless w.r.t. user data.
package rebuilderclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/trackrecord/enclave/internal/connector"
	"go.uber.org/zap"
)

// Credentials carries the plaintext credentials from the enclave to the
// rebuilder service. The enclave never persists them outside the encrypted
// store; this struct exists for the duration of one HTTP POST.
//
// LOG-CREDS-001: NEVER log a Credentials value. No %v, no zap.Any, no
// String() method. If you need to identify a request use the surrounding
// RebuildRequest's UserUID + Exchange + Label only.
type Credentials struct {
	WalletAddress string `json:"walletAddress,omitempty"`
	APIKey        string `json:"apiKey,omitempty"`
	APISecret     string `json:"apiSecret,omitempty"`
	Passphrase    string `json:"passphrase,omitempty"`
}

// RebuildRequest is the body of POST /history/rebuild.
type RebuildRequest struct {
	UserUID     string      `json:"userUid"`
	Exchange    string      `json:"exchange"`
	Label       string      `json:"label"`
	Credentials Credentials `json:"credentials"`
	// EndEquityOverride: optional. When > 0, the rebuilder skips its
	// live-equity API call and anchors the MTM walk's offset calibration on
	// this value. Used by the midnight UTC recalibration pass to align
	// historical equities to the exact midnight snapshot value without
	// re-fetching live equity (avoiding API drift between 00:00 and ~00:04).
	EndEquityOverride float64 `json:"endEquityOverride,omitempty"`
}

// rebuildResponse is the wire shape returned by the rebuilder. Internal —
// the caller receives []*connector.HistoricalSnapshot, which already has
// the canonical in-enclave field naming.
type rebuildResponse struct {
	Exchange   string                  `json:"exchange"`
	Count      int                     `json:"count"`
	DurationMs int64                   `json:"durationMs"`
	Snapshots  []rebuildSnapshotOnWire `json:"snapshots"`
}

// rebuildSnapshotOnWire mirrors the rebuilder's HistoricalSnapshot JSON
// shape (camelCase). Mapped to connector.HistoricalSnapshot before return
// so callers stay agnostic of the wire format.
type rebuildSnapshotOnWire struct {
	Date            time.Time                       `json:"date"`
	TotalEquity     float64                         `json:"totalEquity"`
	RealizedBalance float64                         `json:"realizedBalance"`
	Deposits        float64                         `json:"deposits"`
	Withdrawals     float64                         `json:"withdrawals"`
	TotalTrades     int                             `json:"totalTrades"`
	TotalVolume     float64                         `json:"totalVolume"`
	TotalFees       float64                         `json:"totalFees"`
	LongTrades      int                             `json:"longTrades"`
	ShortTrades     int                             `json:"shortTrades"`
	LongVolume      float64                         `json:"longVolume"`
	ShortVolume     float64                         `json:"shortVolume"`
	Breakdown       map[string]*marketBalanceOnWire `json:"breakdown,omitempty"`
}

type marketBalanceOnWire struct {
	MarketType      string  `json:"marketType"`
	Equity          float64 `json:"equity"`
	AvailableMargin float64 `json:"availableMargin"`
}

// RebuildResult bundles what /history/rebuild returned to the caller.
// Snapshots use connector.HistoricalSnapshot so the enclave's existing
// historical-snapshot persistence path can consume them unchanged.
type RebuildResult struct {
	Snapshots  []*connector.HistoricalSnapshot
	DurationMs int64
}

// Client is a thin HTTP client. The pointed-at service runs on a separate
// VPS (see DEPLOYMENT.md); production deployments front it with mTLS and
// source-IP-restricted nginx.
type Client struct {
	baseURL    string
	authToken  string
	httpClient *http.Client
	logger     *zap.Logger
}

// New constructs a Client. baseURL is the rebuilder service root (no trailing
// slash); authToken is sent as `X-Internal-Token` and must match the
// rebuilder's REBUILDER_INTERNAL_TOKEN env. Either may be empty in dev mode
// (Client.Rebuild becomes a no-op-then-error so dev compose stacks without
// the rebuilder still boot cleanly — enclave-only setups stay functional).
func New(baseURL, authToken string, logger *zap.Logger) *Client {
	return &Client{
		baseURL:   baseURL,
		authToken: authToken,
		httpClient: &http.Client{
			// /history/rebuild is synchronous on the rebuilder side: the
			// response body lands AFTER the per-exchange reconstruction
			// completes. Binance HF accounts page their income ledger paced
			// against Binance's request-weight cap (~70 calls/min), which
			// runs up to ~8-9 min — the whole chain must survive it: this
			// client, the rebuilder's REBUILD_TIMEOUT_SECONDS and nginx's
			// proxy_read_timeout on the rebuilder vhost (the shortest link
			// cancels the request context and aborts the rebuild mid-page).
			Timeout: 720 * time.Second,
		},
		logger: logger,
	}
}

// Configured reports whether this client will actually issue requests.
// The Sync hook checks this so missing-rebuilder scenarios degrade silently
// instead of logging an error every time a non-IBKR connection is created
// in an enclave-only dev environment.
func (c *Client) Configured() bool {
	return c != nil && c.baseURL != "" && c.authToken != ""
}

// Rebuild fires a single POST /history/rebuild and waits for the rebuilder
// to return the reconstructed snapshots. The caller is responsible for
// persisting the returned snapshots to the aggregator's DB — this client
// (and the rebuilder service) never write user data.
//
// LOG-CREDS-001: the marshaled `body` carries plaintext credentials. Never
// log it (no fmt.Sprintf into messages, no zap.ByteString). Errors returned
// from this function are wrapped without including request/response bodies
// for the same reason. The httpClient deliberately uses no transport-level
// tracer / DumpRequestOut — Go's default Transport doesn't, but if you ever
// inject a custom one make sure it doesn't dump bodies.
func (c *Client) Rebuild(ctx context.Context, req RebuildRequest) (*RebuildResult, error) {
	if !c.Configured() {
		return nil, fmt.Errorf("rebuilder client not configured")
	}

	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/history/rebuild", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("X-Internal-Token", c.authToken)

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("rebuilder request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		// LOG-CREDS-001: status code only — never the response body, in case
		// the rebuilder ever echoes part of the request back in its error
		// payload.
		return nil, fmt.Errorf("rebuilder returned HTTP %d", resp.StatusCode)
	}

	// 16 MiB ceiling on the response body. ~140 snapshots × <1 KiB each is
	// the realistic upper bound for the worst exchange today; a larger body
	// is a misbehaving server (or worse, an attempt to OOM us).
	const maxResponseBytes = 16 << 20
	dec := json.NewDecoder(http.MaxBytesReader(nil, resp.Body, maxResponseBytes))
	var out rebuildResponse
	if err := dec.Decode(&out); err != nil {
		return nil, fmt.Errorf("decode rebuilder response: %w", err)
	}

	return &RebuildResult{
		Snapshots:  mapWireSnapshots(out.Snapshots),
		DurationMs: out.DurationMs,
	}, nil
}

func mapWireSnapshots(in []rebuildSnapshotOnWire) []*connector.HistoricalSnapshot {
	out := make([]*connector.HistoricalSnapshot, 0, len(in))
	for _, s := range in {
		out = append(out, &connector.HistoricalSnapshot{
			Date:            s.Date,
			TotalEquity:     s.TotalEquity,
			RealizedBalance: s.RealizedBalance,
			Deposits:        s.Deposits,
			Withdrawals:     s.Withdrawals,
			TotalTrades:     s.TotalTrades,
			TotalVolume:     s.TotalVolume,
			TotalFees:       s.TotalFees,
			LongTrades:      s.LongTrades,
			ShortTrades:     s.ShortTrades,
			LongVolume:      s.LongVolume,
			ShortVolume:     s.ShortVolume,
			Breakdown:       mapWireBreakdown(s.Breakdown),
		})
	}
	return out
}

func mapWireBreakdown(in map[string]*marketBalanceOnWire) map[string]*connector.MarketBalance {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]*connector.MarketBalance, len(in))
	for k, mb := range in {
		if mb == nil {
			continue
		}
		out[k] = &connector.MarketBalance{
			MarketType:      mb.MarketType,
			Equity:          mb.Equity,
			AvailableMargin: mb.AvailableMargin,
		}
	}
	return out
}
