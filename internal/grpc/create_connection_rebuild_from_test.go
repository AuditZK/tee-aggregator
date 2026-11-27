package grpc

import (
	"context"
	"testing"
	"time"

	pb "github.com/trackrecord/enclave/api/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCreateUserConnection_RebuildFrom(t *testing.T) {
	now := time.Now().UTC()
	asked := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC).AddDate(0, -3, 0)
	for _, tc := range []struct {
		name     string
		rebuild  bool
		from     string
		wantCode codes.Code
		wantFrom time.Time
	}{
		{"month asked", true, asked.Format("2006-01"), codes.OK, asked},
		{"no month", true, "", codes.OK, time.Time{}},
		{"invalid month", true, "2025-3", codes.InvalidArgument, time.Time{}},
		{"ignored without rebuild", false, "not-a-month", codes.OK, time.Time{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeConnectionService{}
			client, cleanup := newBufconnClient(t, NewServer(zap.NewNop(), Services{ConnSvc: fake}, ServerOptions{}))
			defer cleanup()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			_, err := client.CreateUserConnection(ctx, &pb.CreateUserConnectionRequest{
				UserUid: "user_abc1234567890", Exchange: "binance", Label: "main",
				ApiKey: "synthetic-key", ApiSecret: "synthetic-secret",
				RebuildHistory: tc.rebuild, RebuildFrom: tc.from,
			})
			if got := status.Code(err); got != tc.wantCode {
				t.Fatalf("code = %v, want %v (err %v)", got, tc.wantCode, err)
			}
			if tc.wantCode != codes.OK {
				if fake.lastReq != nil {
					t.Fatal("an invalid rebuild_from still reached the connection service")
				}
				return
			}
			if fake.lastReq == nil {
				t.Fatal("connection service not called")
			}
			if !fake.lastReq.RebuildFrom.Equal(tc.wantFrom) {
				t.Fatalf("RebuildFrom = %v, want %v", fake.lastReq.RebuildFrom, tc.wantFrom)
			}
		})
	}
}
