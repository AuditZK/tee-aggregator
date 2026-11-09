package grpc

// A signed report scoped to a portfolio, over the wire, against a real Postgres
// carrying the production schema: only the members count, the composition and the
// verified boundary sit under the signature, and what left stays consolidated.
//
// Runs only under scripts/test-db.sh (TEST_DATABASE_URL).

import (
	"bytes"
	"context"
	"encoding/json"
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

func TestPortfolioReportChain_DB(t *testing.T) {
	pool := testdb.New(t)
	ctx := context.Background()

	enc, err := encryption.New(bytes.Repeat([]byte{0x42}, 32))
	if err != nil {
		t.Fatalf("encryption: %v", err)
	}
	conns := repository.NewConnectionRepo(pool)
	snaps := repository.NewSnapshotRepo(pool)
	connSvc := service.NewConnectionService(conns, enc)
	metricsSvc := service.NewMetricsService(snaps)
	reportSvc := service.NewReportService(metricsSvc, snaps, signing.MustNewReportSignerGenerate())
	portfolios := service.NewPortfolioService(repository.NewPortfolioRepo(pool), connSvc)

	srv := NewServer(zap.NewNop(), Services{
		ConnSvc:      connSvc,
		MetricsSvc:   metricsSvc,
		ReportSvc:    reportSvc,
		SnapshotRepo: snaps,
		UserRepo:     repository.NewUserRepo(pool),
		PortfolioSvc: portfolios,
	}, ServerOptions{})
	client, cleanup := newBufconnClient(t, srv)
	defer cleanup()

	const uid = "user_abc1234567890"
	for _, label := range []string{"kept", "gone", "other"} {
		resp, err := client.CreateUserConnection(ctx, &pb.CreateUserConnectionRequest{
			UserUid: uid, Exchange: "mock", Label: label,
			ApiKey: "synthetic-key-" + label, ApiSecret: "synthetic-secret-" + label,
		})
		if err != nil || !resp.Success {
			t.Fatalf("CreateUserConnection(%s): err=%v resp=%+v", label, err, resp)
		}
	}

	// "kept" gains 1% a day. "gone" doubles every day after it leaves, "other",
	// never a member, halves every day: either leaking in would be plain to see.
	day0 := time.Now().UTC().Truncate(24*time.Hour).AddDate(0, 0, -20)
	const days = 12
	kept, gone := 1000.0, 1000.0
	other := 100000.0
	for i := 0; i < days; i++ {
		ts := day0.AddDate(0, 0, i)
		for label, eq := range map[string]float64{"kept": kept, "gone": gone, "other": other} {
			if err := snaps.Upsert(ctx, &repository.Snapshot{
				UserUID: uid, Exchange: "mock", Label: label, Timestamp: ts,
				TotalEquity: eq, RealizedBalance: eq,
				FromExternalRebuilder: label == "other" && i == 9,
			}); err != nil {
				t.Fatalf("upsert: %v", err)
			}
		}
		kept *= 1.01
		other /= 2
		if i >= 2 {
			gone *= 2
		} else {
			gone *= 1.01
		}
	}

	portfolio, err := portfolios.Create(ctx, uid, "Book A", []repository.ConnectionRef{
		{Exchange: "mock", Label: "kept"}, {Exchange: "mock", Label: "gone"},
	})
	if err != nil {
		t.Fatalf("create portfolio: %v", err)
	}
	removedAt := day0.AddDate(0, 0, 2)
	if _, err := pool.Exec(ctx, `UPDATE portfolio_members SET removed_at = $1 WHERE portfolio_id = $2 AND label = 'gone'`, removedAt, portfolio.ID); err != nil {
		t.Fatalf("close the stay: %v", err)
	}

	request := func(id, user string) *pb.ReportRequest {
		return &pb.ReportRequest{
			UserUid: user, PortfolioId: id,
			StartDate: day0.Format("2006-01-02"), EndDate: day0.AddDate(0, 0, days-1).Format("2006-01-02"),
		}
	}

	rep, err := client.GenerateSignedReport(ctx, request(portfolio.ID, uid))
	if err != nil || !rep.Success {
		t.Fatalf("GenerateSignedReport: err=%v resp.Error=%q", err, rep.GetError())
	}

	// 1% a day throughout, except the day "gone" leaves: it left at the close, so
	// that day's base still holds its equal share and "kept"'s 1% counts for half.
	// "other" would make this deeply negative, "gone" after its removal hugely
	// positive.
	want := math.Pow(1.01, days-2)*1.005 - 1
	if math.Abs(rep.TotalReturn-want) > 1e-4 && math.Abs(rep.TotalReturn-want*100) > 1e-2 {
		t.Fatalf("total_return = %v, want %v as a fraction or a percent", rep.TotalReturn, want)
	}

	var payload struct {
		PayloadVersion string `json:"payloadVersion"`
		VerifiedFrom   string `json:"verifiedFrom"`
		Portfolio      struct {
			Name                string `json:"name"`
			ConnectionsIncluded int    `json:"connectionsIncluded"`
			ConnectionsTotal    int    `json:"connectionsTotal"`
			Stays               []struct {
				Exchange  string `json:"exchange"`
				AddedAt   string `json:"addedAt"`
				RemovedAt string `json:"removedAt"`
			} `json:"stays"`
		} `json:"portfolio"`
		DailyReturns []struct {
			Date               string `json:"date"`
			VerifiabilityClass string `json:"verifiabilityClass"`
		} `json:"dailyReturns"`
	}
	if err := json.Unmarshal([]byte(rep.SignedPayload), &payload); err != nil {
		t.Fatalf("signed payload: %v", err)
	}
	if payload.PayloadVersion != signing.PayloadVersion {
		t.Errorf("payload version = %q", payload.PayloadVersion)
	}
	if payload.Portfolio.Name != "Book A" || payload.Portfolio.ConnectionsIncluded != 1 || payload.Portfolio.ConnectionsTotal != 3 {
		t.Errorf("composition = %+v", payload.Portfolio)
	}
	if len(payload.Portfolio.Stays) != 2 {
		t.Fatalf("stays = %+v", payload.Portfolio.Stays)
	}
	var closed int
	for _, stay := range payload.Portfolio.Stays {
		if stay.RemovedAt == removedAt.Format("2006-01-02") {
			closed++
		}
	}
	if closed != 1 {
		t.Errorf("no dated exit in %+v", payload.Portfolio.Stays)
	}
	if bytes.Contains([]byte(rep.SignedPayload), []byte(`"gone"`)) || bytes.Contains([]byte(rep.SignedPayload), []byte(`"kept"`)) {
		t.Error("an account label reached the signed payload")
	}

	// "other" was rebuilt on day 9, and it is not in this portfolio.
	if payload.VerifiedFrom != day0.Format("2006-01-02") {
		t.Errorf("verifiedFrom = %q, want the first day: a stranger's rebuild moved it", payload.VerifiedFrom)
	}
	for _, d := range payload.DailyReturns {
		if d.VerifiabilityClass == signing.VerifiabilityClassRebuilderService {
			t.Errorf("day %s labelled from a stranger's rebuild", d.Date)
		}
	}

	// A member's own rebuild does move it.
	if err := snaps.Upsert(ctx, &repository.Snapshot{
		UserUID: uid, Exchange: "mock", Label: "kept", Timestamp: day0.AddDate(0, 0, 5),
		TotalEquity: 1000 * math.Pow(1.01, 5), RealizedBalance: 1000 * math.Pow(1.01, 5), FromExternalRebuilder: true,
	}); err != nil {
		t.Fatalf("upsert rebuilt: %v", err)
	}
	moved, err := client.GenerateSignedReport(ctx, request(portfolio.ID, uid))
	if err != nil || !moved.Success {
		t.Fatalf("report after a rebuild: err=%v resp.Error=%q", err, moved.GetError())
	}
	if err := json.Unmarshal([]byte(moved.SignedPayload), &payload); err != nil {
		t.Fatalf("signed payload: %v", err)
	}
	if want := day0.AddDate(0, 0, 6).Format("2006-01-02"); payload.VerifiedFrom != want {
		t.Errorf("verifiedFrom = %q, want %q", payload.VerifiedFrom, want)
	}

	// The percent view carries the same composition, under its own signature.
	var percent struct {
		VerifiedFrom string          `json:"verifiedFrom"`
		Portfolio    json.RawMessage `json:"portfolio"`
	}
	if err := json.Unmarshal([]byte(moved.PercentSignedPayload), &percent); err != nil {
		t.Fatalf("percent payload: %v", err)
	}
	if percent.VerifiedFrom != payload.VerifiedFrom || len(percent.Portfolio) == 0 {
		t.Errorf("percent payload = %+v", percent)
	}

	// Another user's id, or an unknown one, signs nothing.
	missing, err := client.GenerateSignedReport(ctx, request("ffffffffffffffffffffffffffffffff", uid))
	if err != nil {
		t.Fatalf("unknown portfolio: %v", err)
	}
	if missing.Success {
		t.Error("signed a report for a portfolio that does not exist")
	}
	if _, err := pool.Exec(ctx, `INSERT INTO users (id, uid, "updatedAt") VALUES ('user_other1234567', 'user_other1234567', now())`); err != nil {
		t.Fatalf("second user: %v", err)
	}
	stranger, err := client.GenerateSignedReport(ctx, request(portfolio.ID, "user_other1234567"))
	if err != nil {
		t.Fatalf("another user's portfolio: %v", err)
	}
	if stranger.Success {
		t.Error("signed a report from another user's portfolio")
	}

	// A report with no portfolio still covers everything, and says so by carrying
	// no composition.
	full, err := client.GenerateSignedReport(ctx, request("", uid))
	if err != nil || !full.Success {
		t.Fatalf("full report: err=%v resp.Error=%q", err, full.GetError())
	}
	if bytes.Contains([]byte(full.SignedPayload), []byte(`"portfolio"`)) {
		t.Error("a report with no portfolio carries a composition")
	}
	if math.Abs(full.TotalReturn-rep.TotalReturn) < 1e-6 {
		t.Errorf("full report return %v matches the portfolio's: the scope changed nothing", full.TotalReturn)
	}
}
