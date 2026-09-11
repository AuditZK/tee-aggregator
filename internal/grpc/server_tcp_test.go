package grpc

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
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

	// NewClient replaces the deprecated DialContext + WithBlock combo. NewClient
	// is always non-blocking, so the 3-second context above now bounds the
	// first RPC (made by the tests) rather than the dial itself.
	conn, err := gogrpc.NewClient(
		lis.Addr().String(),
		gogrpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		grpcServer.Stop()
		_ = lis.Close()
		t.Fatalf("failed to dial tcp grpc server: %v", err)
	}
	_ = ctx // used by callers for the first RPC

	cleanup := func() {
		_ = conn.Close()
		grpcServer.Stop()
		_ = lis.Close()
	}

	return pb.NewEnclaveServiceClient(conn), cleanup
}

func TestHealthCheck_TCPRoundTrip(t *testing.T) {
	srv := NewServer(zap.NewNop(), Services{}, ServerOptions{})
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
	srv := NewServer(zap.NewNop(), Services{}, ServerOptions{})
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
	srv := NewServer(zap.NewNop(), Services{}, ServerOptions{})
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
	srv := NewServer(zap.NewNop(), Services{}, ServerOptions{})
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

const streamCapWait = 10 * time.Second

func TestServer_ConcurrentStreamsCapped(t *testing.T) {
	const calls = grpcMaxConcurrentStreams + 6

	srv := NewServer(zap.NewNop(), Services{}, ServerOptions{})

	var inFlight atomic.Int32
	var once sync.Once
	gate := make(chan struct{})
	release := func() { once.Do(func() { close(gate) }) }

	opts := append(srv.serverOptions(nil), gogrpc.ChainUnaryInterceptor(
		func(ctx context.Context, req any, info *gogrpc.UnaryServerInfo, handler gogrpc.UnaryHandler) (any, error) {
			inFlight.Add(1)
			defer inFlight.Add(-1)
			<-gate
			return handler(ctx, req)
		},
	))

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen on random tcp port: %v", err)
	}
	grpcServer := gogrpc.NewServer(opts...)
	pb.RegisterEnclaveServiceServer(grpcServer, srv)
	go func() {
		_ = grpcServer.Serve(lis)
	}()

	conn, err := gogrpc.NewClient(
		lis.Addr().String(),
		gogrpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		grpcServer.Stop()
		t.Fatalf("dial tcp grpc server: %v", err)
	}

	defer func() {
		_ = conn.Close()
		grpcServer.Stop()
	}()
	defer release()

	ctx, cancel := context.WithTimeout(context.Background(), 3*streamCapWait)
	defer cancel()

	client := pb.NewEnclaveServiceClient(conn)
	errs := make(chan error, calls)
	for range calls {
		go func() {
			_, err := client.HealthCheck(ctx, &pb.HealthCheckRequest{})
			errs <- err
		}()
	}

	deadline := time.Now().Add(streamCapWait)
	for inFlight.Load() < grpcMaxConcurrentStreams {
		if time.Now().After(deadline) {
			t.Fatalf("handlers in flight = %d, want %d", inFlight.Load(), grpcMaxConcurrentStreams)
		}
		time.Sleep(time.Millisecond)
	}

	time.Sleep(300 * time.Millisecond)
	if got := inFlight.Load(); got != grpcMaxConcurrentStreams {
		t.Fatalf("handlers in flight = %d, want %d", got, grpcMaxConcurrentStreams)
	}

	release()
	for i := range calls {
		select {
		case err := <-errs:
			if err != nil {
				t.Fatalf("HealthCheck() error = %v", err)
			}
		case <-time.After(streamCapWait):
			t.Fatalf("only %d of %d calls returned", i, calls)
		}
	}
}
