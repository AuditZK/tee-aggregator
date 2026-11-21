package grpc

// End-to-end chain for exclude_from_report against a real Postgres carrying
// the production schema. Until 2026-11 the gateway dropped the flag, so the
// report signed every connection whatever the user chose (AUD-012).
//
//   gRPC CreateUserConnection(exclude_from_report) -> ConnectionService.Create
//   -> ConnectionRepo insert -> exclude_from_report column
//   -> gRPC GenerateSignedReport -> GetExcludedConnectionKeys -> report filter
//
// Runs only under scripts/test-db.sh (TEST_DATABASE_URL).

import (
	"bytes"
	"context"
	"math"
	"testing"
	"time"

	pb "github.com/trackrecord/enclave/api/proto"
	"github.com/trackrecord/enclave/internal/encryption"
	"github.com/trackrecord/enclave/internal/repository"
	"github.com/trackrecord/enclave/internal/service"
	"github.com/trackrecord/enclave/internal/signing"
	"github.com/trackrecord/enclave/internal/testdb"
	"go.uber.org/zap"
)

func TestExcludeFromReportChain_DB(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()

	enc, err := encryption.New(bytes.Repeat([]byte{0x42}, 32))
	if err != nil {
		t.Fatalf("encryption: %v", err)
	}
	conns := repository.NewConnectionRepo(pool)
	snaps := repository.NewSnapshotRepo(pool)
	users := repository.NewUserRepo(pool)
	connSvc := service.NewConnectionService(conns, enc)
	metricsSvc := service.NewMetricsService(snaps)
	reportSvc := service.NewReportService(metricsSvc, snaps, signing.MustNewReportSignerGenerate())

	srv := NewServer(zap.NewNop(), Services{
		ConnSvc:      connSvc,
		MetricsSvc:   metricsSvc,
		ReportSvc:    reportSvc,
		SnapshotRepo: snaps,
		UserRepo:     users,
	}, ServerOptions{})
	client, cleanup := newBufconnClient(t, srv)
	defer cleanup()

	const uid = "user_abc1234567890"

	// Two connections over the wire. The "mock" connector answers
	// TestConnection without any network (disabled in production only).
	for _, c := range []struct {
		label   string
		exclude bool
	}{{"kept", false}, {"hidden", true}} {
		resp, err := client.CreateUserConnection(ctx, &pb.CreateUserConnectionRequest{
			UserUid:           uid,
			Exchange:          "mock",
			Label:             c.label,
			ApiKey:            "synthetic-key-" + c.label,
			ApiSecret:         "synthetic-secret-" + c.label,
			ExcludeFromReport: c.exclude,
		})
		if err != nil || !resp.Success {
			t.Fatalf("CreateUserConnection(%s): err=%v resp=%+v", c.label, err, resp)
		}
	}

	// 1. Stored: read the column itself, not the repo's view of it.
	for label, want := range map[string]bool{"kept": false, "hidden": true} {
		var got bool
		q := `SELECT "excludeFromReport" FROM exchange_connections WHERE "userUid" = $1 AND label = $2`
		if !conns.IsTSSchema(ctx) {
			q = `SELECT exclude_from_report FROM exchange_connections WHERE user_uid = $1 AND label = $2`
		}
		if err := pool.QueryRow(ctx, q, uid, label).Scan(&got); err != nil {
			t.Fatalf("read exclude flag for %s: %v", label, err)
		}
		if got != want {
			t.Fatalf("stored exclude flag for %s = %v, want %v", label, got, want)
		}
	}

	// 2. Read back as the excluded key set the report path uses.
	keys, err := connSvc.GetExcludedConnectionKeys(ctx, uid)
	if err != nil {
		t.Fatalf("GetExcludedConnectionKeys: %v", err)
	}
	if _, ok := keys["mock/hidden"]; !ok || len(keys) != 1 {
		t.Fatalf("excluded keys = %v, want exactly {mock/hidden}", keys)
	}

	// 3. Snapshots: "kept" gains 1%/day, "hidden" halves every day.
	day0 := time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, -10)
	kept := []float64{1000, 1010, 1020.1, 1030.301}
	hidden := []float64{100000, 50000, 25000, 12500}
	for i := range kept {
		ts := day0.AddDate(0, 0, i)
		for label, eq := range map[string]float64{"kept": kept[i], "hidden": hidden[i]} {
			if err := snaps.Upsert(ctx, &repository.Snapshot{
				UserUID: uid, Exchange: "mock", Label: label, Timestamp: ts,
				TotalEquity: eq, RealizedBalance: eq,
			}); err != nil {
				t.Fatalf("upsert snapshot: %v", err)
			}
		}
	}

	// 4. Signed report over the wire.
	rep, err := client.GenerateSignedReport(ctx, &pb.ReportRequest{
		UserUid:   uid,
		StartDate: day0.Format("2006-01-02"),
		EndDate:   day0.AddDate(0, 0, len(kept)-1).Format("2006-01-02"),
	})
	if err != nil || !rep.Success {
		t.Fatalf("GenerateSignedReport: err=%v resp.Error=%q", err, rep.GetError())
	}
	t.Logf("signed report: total_return=%v data_points=%d exchanges=%v", rep.TotalReturn, rep.DataPoints, rep.Exchanges)

	// Only "kept" counted: +3.0301% (as fraction or percent, depending on unit).
	wantFrac := kept[len(kept)-1]/kept[0] - 1
	if math.Abs(rep.TotalReturn-wantFrac) > 1e-6 && math.Abs(rep.TotalReturn-wantFrac*100) > 1e-4 {
		t.Fatalf("total_return = %v, want %v (fraction) or %v (percent): the excluded connection leaked into the signed report",
			rep.TotalReturn, wantFrac, wantFrac*100)
	}

	// Control: with no exclusion the hidden connection dominates and the
	// return is deeply negative, so the assertion above is not vacuous.
	ctrl, err := reportSvc.GenerateReport(ctx, &service.GenerateReportRequest{
		UserUID:   uid,
		StartDate: day0,
		EndDate:   day0.AddDate(0, 0, len(kept)-1),
	})
	if err != nil {
		t.Fatalf("control report: %v", err)
	}
	t.Logf("control (no exclusion): total_return=%v", ctrl.TotalReturn)
	if ctrl.TotalReturn >= 0 {
		t.Fatalf("control total_return = %v, expected negative with the hidden connection included", ctrl.TotalReturn)
	}

	// 5. Pausing the excluded connection keeps it excluded. Its snapshots are
	// still read into the report, and a free-plan downgrade pauses with no
	// user action, so an active-only rule put its whole history back in.
	idQuery := `SELECT id FROM exchange_connections WHERE "userUid" = $1 AND label = 'hidden'`
	if !conns.IsTSSchema(ctx) {
		idQuery = `SELECT id FROM exchange_connections WHERE user_uid = $1 AND label = 'hidden'`
	}
	var hiddenID string
	if err := pool.QueryRow(ctx, idQuery, uid).Scan(&hiddenID); err != nil {
		t.Fatalf("read hidden id: %v", err)
	}
	if err := conns.Deactivate(ctx, hiddenID); err != nil {
		t.Fatalf("deactivate hidden: %v", err)
	}
	keys, err = connSvc.GetExcludedConnectionKeys(ctx, uid)
	if err != nil {
		t.Fatalf("GetExcludedConnectionKeys after pause: %v", err)
	}
	if _, ok := keys["mock/hidden"]; !ok {
		t.Fatalf("excluded keys after pause = %v, want mock/hidden still excluded", keys)
	}
	paused, err := client.GenerateSignedReport(ctx, &pb.ReportRequest{
		UserUid:   uid,
		StartDate: day0.Format("2006-01-02"),
		EndDate:   day0.AddDate(0, 0, len(kept)-1).Format("2006-01-02"),
	})
	if err != nil || !paused.Success {
		t.Fatalf("GenerateSignedReport after pause: err=%v resp.Error=%q", err, paused.GetError())
	}
	if paused.TotalReturn != rep.TotalReturn {
		t.Fatalf("total_return after pause = %v, want %v: the paused excluded connection came back", paused.TotalReturn, rep.TotalReturn)
	}

	// 6. Connecting the paused account again under its label is a no-op: the
	// flag it was created with is the one that stays.
	resp, err := client.CreateUserConnection(ctx, &pb.CreateUserConnectionRequest{
		UserUid: uid, Exchange: "mock", Label: "hidden",
		ApiKey: "synthetic-key-hidden-2", ApiSecret: "synthetic-secret-hidden-2",
	})
	if err != nil || !resp.Success || resp.Error != service.ExistingConnectionNoopMessage {
		t.Fatalf("re-connect paused hidden: err=%v resp=%+v, want the no-op answer", err, resp)
	}
	keys, err = connSvc.GetExcludedConnectionKeys(ctx, uid)
	if err != nil {
		t.Fatalf("GetExcludedConnectionKeys after re-connect: %v", err)
	}
	if _, ok := keys["mock/hidden"]; !ok {
		t.Fatalf("excluded keys after re-connect = %v, want mock/hidden still excluded", keys)
	}
}
