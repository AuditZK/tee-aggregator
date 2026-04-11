package grpc

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	pb "github.com/trackrecord/enclave/api/proto"
	"github.com/trackrecord/enclave/internal/repository"
	"github.com/trackrecord/enclave/internal/service"
	"github.com/trackrecord/enclave/internal/signing"
	"go.uber.org/zap"
	gogrpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

const testBufSize = 1024 * 1024

func newBufconnClient(t *testing.T, srv *Server) (pb.EnclaveServiceClient, func()) {
	t.Helper()

	lis := bufconn.Listen(testBufSize)
	grpcServer := gogrpc.NewServer(gogrpc.UnaryInterceptor(srv.loggingInterceptor))
	pb.RegisterEnclaveServiceServer(grpcServer, srv)

	go func() {
		_ = grpcServer.Serve(lis)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	conn, err := gogrpc.DialContext(
		ctx,
		"bufnet",
		gogrpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		gogrpc.WithTransportCredentials(insecure.NewCredentials()),
		gogrpc.WithBlock(),
	)
	if err != nil {
		grpcServer.Stop()
		_ = lis.Close()
		t.Fatalf("failed to dial bufconn grpc server: %v", err)
	}

	cleanup := func() {
		_ = conn.Close()
		grpcServer.Stop()
		_ = lis.Close()
	}

	return pb.NewEnclaveServiceClient(conn), cleanup
}

func TestHealthCheck_BufconnRoundTrip(t *testing.T) {
	srv := NewServer(zap.NewNop(), nil, nil, nil, nil, nil, nil)
	client, cleanup := newBufconnClient(t, srv)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err := client.HealthCheck(ctx, &pb.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("HealthCheck() error = %v", err)
	}

	if resp.Status != pb.HealthCheckResponse_HEALTHY {
		t.Fatalf("unexpected status: got %v want %v", resp.Status, pb.HealthCheckResponse_HEALTHY)
	}
	if !resp.Enclave {
		t.Fatal("expected enclave=true")
	}
	if resp.Version == "" {
		t.Fatal("expected non-empty version")
	}
	if resp.Uptime < 0 {
		t.Fatalf("expected non-negative uptime, got %f", resp.Uptime)
	}
}

func TestCreateUserConnection_InvalidExchangeFormat_IsRejected(t *testing.T) {
	srv := NewServer(
		zap.NewNop(),
		service.NewConnectionService(nil, nil),
		nil,
		nil,
		nil,
		nil,
		nil,
	)
	client, cleanup := newBufconnClient(t, srv)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := client.CreateUserConnection(ctx, &pb.CreateUserConnectionRequest{
		UserUid:   "user_abc1234567890",
		Exchange:  "bit@stamp",
		Label:     "main",
		ApiKey:    "key",
		ApiSecret: "secret",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v (err=%v)", status.Code(err), err)
	}
	if !strings.Contains(status.Convert(err).Message(), "invalid exchange format") {
		t.Fatalf("expected invalid exchange format validation error, got %q", status.Convert(err).Message())
	}
}

func TestProcessSyncJob_InvalidUser_IsRejected(t *testing.T) {
	srv := NewServer(
		zap.NewNop(),
		nil,
		service.NewSyncService(nil, nil, nil, zap.NewNop()),
		nil,
		nil,
		nil,
		nil,
	)
	client, cleanup := newBufconnClient(t, srv)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := client.ProcessSyncJob(ctx, &pb.SyncJobRequest{
		UserUid: "bad",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v (err=%v)", status.Code(err), err)
	}
	if !strings.Contains(status.Convert(err).Message(), "invalid user_uid format") {
		t.Fatalf("expected user_uid validation error, got %q", status.Convert(err).Message())
	}
}

func TestProcessSyncJob_UppercaseExchange_IsRejected(t *testing.T) {
	srv := NewServer(
		zap.NewNop(),
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
	)
	client, cleanup := newBufconnClient(t, srv)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := client.ProcessSyncJob(ctx, &pb.SyncJobRequest{
		UserUid:  "user_abc1234567890",
		Exchange: "Binance",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v (err=%v)", status.Code(err), err)
	}
	if !strings.Contains(status.Convert(err).Message(), "invalid exchange format") {
		t.Fatalf("expected invalid exchange format validation error, got %q", status.Convert(err).Message())
	}
}

func TestGetSnapshotTimeSeries_InvalidExchange_ReturnsInvalidArgument(t *testing.T) {
	srv := NewServer(zap.NewNop(), nil, nil, nil, nil, &repository.SnapshotRepo{}, nil)
	client, cleanup := newBufconnClient(t, srv)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := client.GetSnapshotTimeSeries(ctx, &pb.SnapshotTimeSeriesRequest{
		UserUid:  "user_abc1234567890",
		Exchange: "bit@stamp",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v (err=%v)", status.Code(err), err)
	}
}

func TestGetSnapshotTimeSeries_InvalidTimestamp_ReturnsInvalidArgument(t *testing.T) {
	srv := NewServer(zap.NewNop(), nil, nil, nil, nil, &repository.SnapshotRepo{}, nil)
	client, cleanup := newBufconnClient(t, srv)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := client.GetSnapshotTimeSeries(ctx, &pb.SnapshotTimeSeriesRequest{
		UserUid:   "user_abc1234567890",
		StartDate: -1,
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v (err=%v)", status.Code(err), err)
	}
}

func TestGetSnapshotTimeSeries_EndBeforeStart_ReturnsInvalidArgument(t *testing.T) {
	srv := NewServer(zap.NewNop(), nil, nil, nil, nil, &repository.SnapshotRepo{}, nil)
	client, cleanup := newBufconnClient(t, srv)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	now := time.Now().UnixMilli()
	_, err := client.GetSnapshotTimeSeries(ctx, &pb.SnapshotTimeSeriesRequest{
		UserUid:   "user_abc1234567890",
		StartDate: now,
		EndDate:   now - 1000,
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v (err=%v)", status.Code(err), err)
	}
}

func TestGetSnapshotTimeSeries_RangeTooLarge_ReturnsInvalidArgument(t *testing.T) {
	srv := NewServer(zap.NewNop(), nil, nil, nil, nil, &repository.SnapshotRepo{}, nil)
	client, cleanup := newBufconnClient(t, srv)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	end := time.Now().Add(-24 * time.Hour).UnixMilli()
	start := time.UnixMilli(end).AddDate(-6, 0, 0).UnixMilli()
	_, err := client.GetSnapshotTimeSeries(ctx, &pb.SnapshotTimeSeriesRequest{
		UserUid:   "user_abc1234567890",
		StartDate: start,
		EndDate:   end,
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v (err=%v)", status.Code(err), err)
	}
}

func TestGetAggregatedMetrics_InvalidExchange_ReturnsInvalidArgument(t *testing.T) {
	srv := NewServer(zap.NewNop(), nil, nil, nil, nil, &repository.SnapshotRepo{}, nil)
	client, cleanup := newBufconnClient(t, srv)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := client.GetAggregatedMetrics(ctx, &pb.AggregatedMetricsRequest{
		UserUid:  "user_abc1234567890",
		Exchange: "bit@stamp",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v (err=%v)", status.Code(err), err)
	}
}

func TestVerifyReportSignature_MissingFields_ReturnsInvalidArgument(t *testing.T) {
	reportSvc := service.NewReportService(nil, nil, signing.MustNewReportSignerGenerate())
	srv := NewServer(zap.NewNop(), nil, nil, nil, reportSvc, nil, nil)
	client, cleanup := newBufconnClient(t, srv)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := client.VerifyReportSignature(ctx, &pb.VerifySignatureRequest{})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v (err=%v)", status.Code(err), err)
	}
}

func TestMapExchangeDetails_UsesDetailedData(t *testing.T) {
	got := mapExchangeDetails(
		[]signing.ExchangeInfo{
			{Name: "binance", KYCLevel: "basic", IsPaper: false},
			{Name: "bybit", KYCLevel: "", IsPaper: true},
		},
		[]string{"binance", "bybit"},
	)

	if len(got) != 2 {
		t.Fatalf("len(mapExchangeDetails) = %d, want 2", len(got))
	}
	if got[1].Name != "bybit" || !got[1].IsPaper {
		t.Fatalf("unexpected mapped exchange detail: %+v", got[1])
	}
}

func TestMapExchangeDetails_FallsBackToExchanges(t *testing.T) {
	got := mapExchangeDetails(nil, []string{"okx"})

	if len(got) != 1 {
		t.Fatalf("len(mapExchangeDetails) = %d, want 1", len(got))
	}
	if got[0].Name != "okx" || got[0].KycLevel != "" || got[0].IsPaper {
		t.Fatalf("unexpected fallback exchange detail: %+v", got[0])
	}
}

func TestMapMarketBreakdown_MapsStocksAndGlobal(t *testing.T) {
	in := &repository.MarketBreakdown{
		Stocks: &repository.MarketMetrics{
			Volume:      1000,
			Trades:      2,
			TradingFees: 3,
			FundingFees: 0,
		},
		Spot: &repository.MarketMetrics{
			Volume:      500,
			Trades:      1,
			TradingFees: 1,
			FundingFees: 0,
		},
	}

	out := mapMarketBreakdown(in)
	if out == nil {
		t.Fatal("expected non-nil market breakdown")
	}
	if out.Stocks == nil {
		t.Fatal("expected stocks metrics to be mapped")
	}
	if out.Stocks.Volume != 1000 || out.Stocks.Trades != 2 {
		t.Fatalf("unexpected stocks metrics: %+v", out.Stocks)
	}
	if out.Global == nil {
		t.Fatal("expected global metrics")
	}
	if out.Global.Volume != 1500 || out.Global.Trades != 3 {
		t.Fatalf("unexpected global metrics: %+v", out.Global)
	}
}

func TestAggregateSyncResultsForSyncJobResponse_AllAndPartial(t *testing.T) {
	t1 := time.Date(2026, 2, 28, 10, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 2, 28, 11, 0, 0, 0, time.UTC)

	success, synced, snapshots, latest, errMsg := aggregateSyncResultsForSyncJobResponse([]*service.SyncResult{
		{
			Exchange:          "binance",
			Label:             "main",
			Success:           true,
			TradeCount:        3,
			SnapshotEquity:    1000,
			SnapshotTimestamp: t1,
		},
		{
			Exchange: "bybit",
			Label:    "alt",
			Success:  false,
			Error:    "timeout",
		},
		{
			Exchange:          "okx",
			Label:             "main",
			Success:           true,
			TradeCount:        5,
			SnapshotEquity:    1400,
			SnapshotTimestamp: t2,
		},
	})

	if !success {
		t.Fatal("expected success=true")
	}
	if synced != 8 {
		t.Fatalf("expected synced=8, got %d", synced)
	}
	if snapshots != 2 {
		t.Fatalf("expected snapshots=2, got %d", snapshots)
	}
	if latest == nil || latest.Timestamp != t2.UnixMilli() || latest.Equity != 1400 {
		t.Fatalf("unexpected latest snapshot: %+v", latest)
	}
	if !strings.Contains(errMsg, "bybit/alt: timeout") {
		t.Fatalf("unexpected aggregated error: %q", errMsg)
	}
}

func TestAggregateSyncResultsForSyncJobResponse_AllFailed(t *testing.T) {
	success, synced, snapshots, latest, errMsg := aggregateSyncResultsForSyncJobResponse([]*service.SyncResult{
		{Exchange: "binance", Label: "main", Success: false, Error: "auth failed"},
		{Exchange: "bybit", Label: "alt", Success: false, Error: "network"},
	})

	if success {
		t.Fatal("expected success=false")
	}
	if synced != 0 || snapshots != 0 {
		t.Fatalf("expected synced=0/snapshots=0, got synced=%d snapshots=%d", synced, snapshots)
	}
	if latest != nil {
		t.Fatalf("expected latest=nil, got %+v", latest)
	}
	if !strings.Contains(errMsg, "auth failed") || !strings.Contains(errMsg, "network") {
		t.Fatalf("unexpected aggregated error: %q", errMsg)
	}
}
