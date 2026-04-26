package grpc

import (
	"context"
	"errors"
	"testing"
	"time"

	pb "github.com/trackrecord/enclave/api/proto"
	"github.com/trackrecord/enclave/internal/repository"
	"github.com/trackrecord/enclave/internal/service"
	"go.uber.org/zap"
)

type fakeConnectionService struct {
	createErr error
	lastReq   *service.CreateConnectionRequest
}

func (f *fakeConnectionService) Create(_ context.Context, req *service.CreateConnectionRequest) error {
	f.lastReq = req
	return f.createErr
}

func (f *fakeConnectionService) GetExcludedConnectionKeys(_ context.Context, _ string) (map[string]struct{}, error) {
	return map[string]struct{}{}, nil
}

func (f *fakeConnectionService) GetActiveConnections(_ context.Context, _ string) ([]*repository.ExchangeConnection, error) {
	return []*repository.ExchangeConnection{}, nil
}

func TestCreateUserConnection_BufconnSuccessPayload(t *testing.T) {
	fake := &fakeConnectionService{}
	srv := NewServer(zap.NewNop(), Services{ConnSvc: fake}, ServerOptions{})
	client, cleanup := newBufconnClient(t, srv)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err := client.CreateUserConnection(ctx, &pb.CreateUserConnectionRequest{
		UserUid:           "user_abc1234567890",
		Exchange:          "alpaca",
		Label:             "main",
		ApiKey:            "key",
		ApiSecret:         "secret",
		ExcludeFromReport: true,
	})
	if err != nil {
		t.Fatalf("CreateUserConnection() error = %v", err)
	}
	if !resp.Success {
		t.Fatalf("expected success=true, got false (error=%q)", resp.Error)
	}
	if resp.UserUid != "user_abc1234567890" {
		t.Fatalf("unexpected user uid: %q", resp.UserUid)
	}
	if resp.Error != "" {
		t.Fatalf("expected empty error, got %q", resp.Error)
	}

	if fake.lastReq == nil {
		t.Fatal("expected connection service Create() to be called")
	}
	if !fake.lastReq.ExcludeFromReport {
		t.Fatal("expected exclude_from_report to be forwarded to service layer")
	}
}

func TestCreateUserConnection_BufconnAlreadyExistsNoopPayload(t *testing.T) {
	fake := &fakeConnectionService{createErr: service.ErrConnectionAlreadyExists}
	srv := NewServer(zap.NewNop(), Services{ConnSvc: fake}, ServerOptions{})
	client, cleanup := newBufconnClient(t, srv)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err := client.CreateUserConnection(ctx, &pb.CreateUserConnectionRequest{
		UserUid:   "user_abc1234567890",
		Exchange:  "alpaca",
		Label:     "main",
		ApiKey:    "key",
		ApiSecret: "secret",
	})
	if err != nil {
		t.Fatalf("CreateUserConnection() error = %v", err)
	}
	if !resp.Success {
		t.Fatalf("expected success=true, got false (error=%q)", resp.Error)
	}
	if resp.Error != service.ExistingConnectionNoopMessage {
		t.Fatalf("expected noop message %q, got %q", service.ExistingConnectionNoopMessage, resp.Error)
	}
}

func TestCreateUserConnection_BufconnOperationalFailureAsPayload(t *testing.T) {
	fake := &fakeConnectionService{createErr: errors.New("db write failed")}
	srv := NewServer(zap.NewNop(), Services{ConnSvc: fake}, ServerOptions{})
	client, cleanup := newBufconnClient(t, srv)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err := client.CreateUserConnection(ctx, &pb.CreateUserConnectionRequest{
		UserUid:   "user_abc1234567890",
		Exchange:  "alpaca",
		Label:     "main",
		ApiKey:    "key",
		ApiSecret: "secret",
	})
	if err != nil {
		t.Fatalf("CreateUserConnection() error = %v", err)
	}
	if resp.Success {
		t.Fatalf("expected success=false, got true")
	}
	if resp.Error != "failed to create connection" {
		t.Fatalf("unexpected error message: %q", resp.Error)
	}
}
