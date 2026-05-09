// Package rebuilderclient is the enclave's HTTP client to history-rebuilder-service
// (lives in track_record_site/history-rebuilder-service).
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
package rebuilderclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"go.uber.org/zap"
)

// Credentials carries the plaintext credentials from the enclave to the
// rebuilder service. The enclave never persists them outside the encrypted
// store; this struct exists for the duration of one HTTP POST.
//
// LOG-CREDS-001: NEVER log a Credentials value. No %v, no zap.Any, no
// String() method. If you need to identify a request use the surrounding
// QueueRebuildRequest's UserUID + Exchange + Label only.
type Credentials struct {
	WalletAddress string `json:"walletAddress,omitempty"`
	APIKey        string `json:"apiKey,omitempty"`
	APISecret     string `json:"apiSecret,omitempty"`
	Passphrase    string `json:"passphrase,omitempty"`
}

// QueueRebuildRequest is the body of POST /history/rebuild.
type QueueRebuildRequest struct {
	UserUID     string      `json:"userUid"`
	Exchange    string      `json:"exchange"`
	Label       string      `json:"label"`
	Credentials Credentials `json:"credentials"`
}

// QueueRebuildResponse is the rebuilder's reply: it acknowledges the job
// without waiting for completion (rebuilds run async on the worker).
type QueueRebuildResponse struct {
	JobID  string `json:"jobId"`
	Status string `json:"status"`
}

// Client is a thin HTTP client. The pointed-at service is expected to be
// reachable on the internal docker network; production deployments should
// front it with mTLS at the proxy.
type Client struct {
	baseURL    string
	authToken  string
	httpClient *http.Client
	logger     *zap.Logger
}

// New constructs a Client. baseURL is the rebuilder service root (no trailing
// slash); authToken is sent as `X-Internal-Token` and must match the
// rebuilder's REBUILDER_INTERNAL_TOKEN env. Either may be empty in dev mode
// (Client.QueueRebuild becomes a no-op so dev compose stacks without the
// rebuilder still boot cleanly — enclave-only setups stay functional).
func New(baseURL, authToken string, logger *zap.Logger) *Client {
	return &Client{
		baseURL:   baseURL,
		authToken: authToken,
		httpClient: &http.Client{
			// Rebuild jobs are queued, not executed inline — a short timeout
			// is safe and prevents the post-create hook from hanging if the
			// rebuilder is down.
			Timeout: 10 * time.Second,
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

// QueueRebuild fires a single POST /history/rebuild. Returns the rebuilder's
// jobId; the caller doesn't wait for the rebuild itself to complete.
//
// LOG-CREDS-001: the marshaled `body` carries plaintext credentials. Never
// log it (no fmt.Sprintf into messages, no zap.ByteString). Errors returned
// from this function are wrapped without including request/response bodies
// for the same reason. The httpClient deliberately uses no transport-level
// tracer / DumpRequestOut — Go's default Transport doesn't, but if you ever
// inject a custom one make sure it doesn't dump bodies.
func (c *Client) QueueRebuild(ctx context.Context, req QueueRebuildRequest) (*QueueRebuildResponse, error) {
	if !c.Configured() {
		return nil, fmt.Errorf("rebuilder client not configured")
	}

	body, err := json.Marshal(req)
	if err != nil {
		// LOG-CREDS-001: bare error, no body.
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

	var out QueueRebuildResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode rebuilder response: %w", err)
	}
	return &out, nil
}
