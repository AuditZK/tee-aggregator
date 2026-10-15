package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/trackrecord/enclave/internal/connector"
	"github.com/trackrecord/enclave/internal/service"
	"go.uber.org/zap"
)

type fakeHandlerConnectionService struct {
	createErr error
	lastReq   *service.CreateConnectionRequest
}

func (f *fakeHandlerConnectionService) Create(_ context.Context, req *service.CreateConnectionRequest) error {
	f.lastReq = req
	return f.createErr
}

func (f *fakeHandlerConnectionService) GetExcludedConnectionKeys(_ context.Context, _ string) (map[string]struct{}, error) {
	return map[string]struct{}{}, nil
}

func TestCreateUserConnectionHandler_Success(t *testing.T) {
	fake := &fakeHandlerConnectionService{}
	h := &Handler{logger: zap.NewNop(), connSvc: fake}

	body := map[string]any{
		"user_uid":            "user_abc1234567890",
		"exchange":            "alpaca",
		"label":               "main",
		"api_key":             "key",
		"api_secret":          "secret",
		"exclude_from_report": true,
	}
	rr := callCreateUserConnectionHandler(t, h, body)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusOK)
	}

	resp := decodeJSONBody(t, rr.Body.Bytes())
	if ok, _ := resp["success"].(bool); !ok {
		t.Fatalf("expected success=true, got response=%v", resp)
	}
	if gotUID, _ := resp["user_uid"].(string); gotUID != "user_abc1234567890" {
		t.Fatalf("unexpected user_uid: %q", gotUID)
	}
	if fake.lastReq == nil {
		t.Fatal("expected connection service Create() to be called")
	}
	if !fake.lastReq.ExcludeFromReport {
		t.Fatal("expected exclude_from_report to be forwarded to service layer")
	}
}

func TestCreateUserConnectionHandler_AlreadyExistsNoop(t *testing.T) {
	fake := &fakeHandlerConnectionService{createErr: service.ErrConnectionAlreadyExists}
	h := &Handler{logger: zap.NewNop(), connSvc: fake}

	body := map[string]any{
		"user_uid":   "user_abc1234567890",
		"exchange":   "alpaca",
		"label":      "main",
		"api_key":    "key",
		"api_secret": "secret",
	}
	rr := callCreateUserConnectionHandler(t, h, body)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusOK)
	}

	resp := decodeJSONBody(t, rr.Body.Bytes())
	if ok, _ := resp["success"].(bool); !ok {
		t.Fatalf("expected success=true, got response=%v", resp)
	}
	if gotErr, _ := resp["error"].(string); gotErr != service.ExistingConnectionNoopMessage {
		t.Fatalf("unexpected error message: %q", gotErr)
	}
}

func TestCreateUserConnectionHandler_OperationalFailure(t *testing.T) {
	fake := &fakeHandlerConnectionService{createErr: errors.New("storage down")}
	h := &Handler{logger: zap.NewNop(), connSvc: fake}

	body := map[string]any{
		"user_uid":   "user_abc1234567890",
		"exchange":   "alpaca",
		"label":      "main",
		"api_key":    "key",
		"api_secret": "secret",
	}
	rr := callCreateUserConnectionHandler(t, h, body)

	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusInternalServerError)
	}

	resp := decodeJSONBody(t, rr.Body.Bytes())
	if ok, _ := resp["success"].(bool); ok {
		t.Fatalf("expected success=false, got response=%v", resp)
	}
	if gotErr, _ := resp["error"].(string); gotErr != "failed to create connection" {
		t.Fatalf("unexpected error message: %q", gotErr)
	}
}

func callCreateUserConnectionHandler(t *testing.T, h *Handler, body map[string]any) *httptest.ResponseRecorder {
	t.Helper()

	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/connection", bytes.NewReader(raw))
	rr := httptest.NewRecorder()
	h.CreateUserConnection(rr, req)
	return rr
}

func decodeJSONBody(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("unmarshal response JSON: %v (raw=%s)", err, string(raw))
	}
	return out
}

// A source-IP refusal is the caller's to fix — one line in their broker's IP
// whitelist — and it used to be answered with the generic 500, which the
// frontend renders as "we're having a problem on our end" (OUR_FAULT). The
// holder then waited out an outage that did not exist. The message must be
// the ErrIPRestricted wording, because that is what the frontend's IP rule
// (src/lib/broker-error-messages.ts) matches on to say "add our IP".
func TestConnectCreateFailure_Mapping(t *testing.T) {
	binanceBody := `HTTP 401: {"code":-2015,"msg":"Invalid API-key, IP, or permissions for action."}`

	cases := []struct {
		name        string
		err         error
		wantStatus  int
		wantMessage string
	}{
		{
			name:        "ip restricted sentinel",
			err:         fmt.Errorf("%w: %s", connector.ErrIPRestricted, binanceBody),
			wantStatus:  http.StatusBadRequest,
			wantMessage: "ip not whitelisted",
		},
		{
			name:        "ip marker without the sentinel",
			err:         errors.New("bitget API error: invalid ip (code 40018)"),
			wantStatus:  http.StatusBadRequest,
			wantMessage: "ip not whitelisted",
		},
		{
			name:        "unknown MT server",
			err:         fmt.Errorf("%w: %s", connector.ErrUnknownServer, `mt-bridge status 422: {"error":{"code":"UNKNOWN_SERVER"}}`),
			wantStatus:  http.StatusBadRequest,
			wantMessage: "broker server name not recognized",
		},
		{
			name:        "rejected key",
			err:         errors.New("invalid credentials: bitget API error: Apikey does not exist (code 40037)"),
			wantStatus:  http.StatusBadRequest,
			wantMessage: "invalid credentials",
		},
		{
			name:        "ours",
			err:         errors.New("check duplicate credentials: dial tcp enclave-db:5432: connect: connection refused"),
			wantStatus:  http.StatusInternalServerError,
			wantMessage: "failed to create connection",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, message := connectCreateFailure(tc.err)
			if status != tc.wantStatus {
				t.Fatalf("status = %d, want %d", status, tc.wantStatus)
			}
			if message != tc.wantMessage {
				t.Fatalf("message = %q, want %q", message, tc.wantMessage)
			}
		})
	}
}

func TestCreateUserConnectionHandler_IPRestrictedIsCallerError(t *testing.T) {
	venueBody := `HTTP 401: {"code":-2015,"msg":"Invalid API-key, IP, or permissions for action."}`
	fake := &fakeHandlerConnectionService{
		createErr: fmt.Errorf("%w: %s", connector.ErrIPRestricted, venueBody),
	}
	h := &Handler{logger: zap.NewNop(), connSvc: fake}

	rr := callCreateUserConnectionHandler(t, h, map[string]any{
		"user_uid":   "user_abc1234567890",
		"exchange":   "binance",
		"label":      "main",
		"api_key":    "key",
		"api_secret": "secret",
	})

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusBadRequest)
	}

	resp := decodeJSONBody(t, rr.Body.Bytes())
	if ok, _ := resp["success"].(bool); ok {
		t.Fatalf("expected success=false, got response=%v", resp)
	}
	gotErr, _ := resp["error"].(string)
	if gotErr != "ip not whitelisted" {
		t.Fatalf("error = %q, want %q", gotErr, "ip not whitelisted")
	}
	// SEC-07: the venue body carries upstream detail and must not cross.
	if strings.Contains(rr.Body.String(), "-2015") {
		t.Fatalf("venue error text leaked to the client: %s", rr.Body.String())
	}
}
