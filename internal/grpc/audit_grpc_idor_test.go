package grpc

// AUDIT — gRPC JWT IDOR regression tests (AUTH-002).
//
// Original finding: doc/audit/findings/AUTH-002-grpc-partial-jwt.md.
// The gRPC server only JWT-gated GenerateSignedReport; every other RPC
// accepting a user_uid was authenticated solely by mTLS + client-cert CN
// allowlist. A compromised caller could pass an arbitrary user_uid and
// impersonate any user.
//
// Status: **fixed**. methodsRequireJWT now covers CreateUserConnection,
// ProcessSyncJob, GetPerformanceMetrics, GetSnapshotTimeSeries, and
// GetAggregatedMetrics. Each handler calls resolveUserUID(ctx, req.UserUid)
// so claims.Sub wins over a body-supplied user_uid whenever the JWT is
// verified.
//
// These tests inject auth.WithUserUID directly into the handler context
// (bypassing the interceptor, which requires a real signed JWT). This
// proves the handler uses the context uid, not the body uid.
import (
	"context"
	"testing"

	pb "github.com/trackrecord/enclave/api/proto"
	"github.com/trackrecord/enclave/internal/auth"
	"go.uber.org/zap"
)

func TestAuditGRPCJWTIDORCreateUserConnection(t *testing.T) {
	const (
		victimUID   = "user_victim1234567890"
		attackerUID = "user_attacker098765432"
	)

	fake := &fakeConnectionService{}
	srv := NewServer(zap.NewNop(), Services{ConnSvc: fake}, ServerOptions{})

	// Inject attacker uid into context the way authInterceptor would.
	ctx := auth.WithUserUID(context.Background(), attackerUID)

	resp, err := srv.CreateUserConnection(ctx, &pb.CreateUserConnectionRequest{
		UserUid:   victimUID, // attacker smuggles the victim uid in the body
		Exchange:  "alpaca",
		Label:     "main",
		ApiKey:    "k",
		ApiSecret: "s",
	})
	if err != nil {
		t.Fatalf("CreateUserConnection returned error: %v", err)
	}
	if !resp.Success {
		t.Fatalf("expected success=true, got error=%q", resp.Error)
	}
	if resp.UserUid != attackerUID {
		t.Fatalf("response uid should be attacker's (from JWT), got %q want %q", resp.UserUid, attackerUID)
	}
	if fake.lastReq == nil {
		t.Fatal("connection service Create was not called")
	}
	if fake.lastReq.UserUID != attackerUID {
		t.Fatalf("handler did not override body user_uid with JWT sub: got %q want %q (body was %q)", fake.lastReq.UserUID, attackerUID, victimUID)
	}
}

func TestAuditGRPCJWTIDORCreateUserConnection_DevModeFallback(t *testing.T) {
	const bodyUID = "user_devuser1234567890"

	fake := &fakeConnectionService{}
	srv := NewServer(zap.NewNop(), Services{ConnSvc: fake}, ServerOptions{})

	// No auth.WithUserUID on the context — simulates dev mode.
	ctx := context.Background()

	resp, err := srv.CreateUserConnection(ctx, &pb.CreateUserConnectionRequest{
		UserUid:   bodyUID,
		Exchange:  "alpaca",
		Label:     "main",
		ApiKey:    "k",
		ApiSecret: "s",
	})
	if err != nil {
		t.Fatalf("CreateUserConnection returned error: %v", err)
	}
	if !resp.Success {
		t.Fatalf("expected success=true, got error=%q", resp.Error)
	}
	if resp.UserUid != bodyUID {
		t.Fatalf("dev mode should use body user_uid: got %q want %q", resp.UserUid, bodyUID)
	}
	if fake.lastReq == nil {
		t.Fatal("connection service Create was not called")
	}
	if fake.lastReq.UserUID != bodyUID {
		t.Fatalf("dev-mode handler used wrong uid: got %q want %q", fake.lastReq.UserUID, bodyUID)
	}
}

// TestAuditGRPCMethodsRequireJWTExpanded is a structural assertion that the
// methodsRequireJWT map now covers every RPC that accepts a user_uid. If a
// new RPC is added that reads user_uid from the request, this test will
// remind the author to include it.
func TestAuditGRPCMethodsRequireJWTExpanded(t *testing.T) {
	for _, m := range []string{
		"/enclave.EnclaveService/GenerateSignedReport",
		"/enclave.EnclaveService/CreateUserConnection",
		"/enclave.EnclaveService/ProcessSyncJob",
		"/enclave.EnclaveService/GetPerformanceMetrics",
		"/enclave.EnclaveService/GetSnapshotTimeSeries",
		"/enclave.EnclaveService/GetAggregatedMetrics",
	} {
		if !methodsRequireJWT[m] {
			t.Errorf("methodsRequireJWT missing %q — every RPC accepting user_uid must be gated", m)
		}
	}
}
