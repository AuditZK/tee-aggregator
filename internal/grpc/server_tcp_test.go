package grpc

import (
	"context"
	"net"
	"testing"
	"time"

	pb "github.com/trackrecord/enclave/api/proto"
	"go.uber.org/zap"
	gogrpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func newTCPClient(t *testing.T, srv *Server) (pb.EnclaveServiceClient, func()) {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen on random tcp port: %v", err)
	}

	grpcServer := gogrpc.NewServer(gogrpc.UnaryInterceptor(srv.loggingInterceptor))
	pb.RegisterEnclaveServiceServer(grpcServer, srv)

	go func() {
		_ = grpcServer.Serve(lis)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	conn, err := gogrpc.DialContext(
		ctx,
		lis.Addr().String(),
		gogrpc.WithTransportCredentials(insecure.NewCredentials()),
		gogrpc.WithBlock(),
	)
	if err != nil {
		grpcServer.Stop()
		_ = lis.Close()
		t.Fatalf("failed to dial tcp grpc server: %v", err)
	}

	cleanup := func() {
		_ = conn.Close()
		grpcServer.Stop()
		_ = lis.Close()
	}

	return pb.NewEnclaveServiceClient(conn), cleanup
}

func TestHealthCheck_TCPRoundTrip(t *testing.T) {
	srv := NewServer(zap.NewNop(), nil, nil, nil, nil, nil, nil, nil, ServerOptions{})
	client, cleanup := newTCPClient(t, srv)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err := client.HealthCheck(ctx, &pb.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("HealthCheck() error = %v", err)
	}
	if !resp.Enclave {
		t.Fatal("expected enclave=true")
	}
}

func TestCreateUserConnection_TCPDatabaseUnavailable(t *testing.T) {
	srv := NewServer(zap.NewNop(), nil, nil, nil, nil, nil, nil, nil, ServerOptions{})
	client, cleanup := newTCPClient(t, srv)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err := client.CreateUserConnection(ctx, &pb.CreateUserConnectionRequest{
		UserUid:   "user_abc1234567890",
		Exchange:  "binance",
		Label:     "main",
		ApiKey:    "key",
		ApiSecret: "secret",
	})
	if err != nil {
		t.Fatalf("expected nil gRPC error, got %v", err)
	}
	if resp.Success {
		t.Fatalf("expected success=false when database unavailable, got %+v", resp)
	}
	if resp.Error == "" {
		t.Fatalf("expected non-empty error when database unavailable")
	}
}

func TestCreateUserConnection_TCPValidationRunsBeforeServiceCheck(t *testing.T) {
	srv := NewServer(zap.NewNop(), nil, nil, nil, nil, nil, nil, nil, ServerOptions{})
	client, cleanup := newTCPClient(t, srv)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := client.CreateUserConnection(ctx, &pb.CreateUserConnectionRequest{
		UserUid:   "user_abc1234567890",
		Exchange:  "bin@ance",
		Label:     "main",
		ApiKey:    "key",
		ApiSecret: "secret",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v (err=%v)", status.Code(err), err)
	}
}

func TestProcessSyncJob_TCPServiceUnavailableReturnsPayloadError(t *testing.T) {
	srv := NewServer(zap.NewNop(), nil, nil, nil, nil, nil, nil, nil, ServerOptions{})
	client, cleanup := newTCPClient(t, srv)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err := client.ProcessSyncJob(ctx, &pb.SyncJobRequest{
		UserUid: "user_abc1234567890",
	})
	if err != nil {
		t.Fatalf("expected nil gRPC error, got %v", err)
	}
	if resp.Success {
		t.Fatalf("expected success=false when sync service unavailable, got %+v", resp)
	}
	if resp.Error == "" {
		t.Fatalf("expected non-empty payload error when sync service unavailable")
	}
}
