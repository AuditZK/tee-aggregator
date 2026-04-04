package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

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
