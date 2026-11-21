package service

import (
	"context"
	"testing"
	"time"

	"github.com/trackrecord/enclave/internal/connector"
)

// plainStubConnector implements connector.Connector without freshness
// reporting — the guard must fail open for it.
type plainStubConnector struct{}

func (c *plainStubConnector) GetBalance(ctx context.Context) (*connector.Balance, error) {
	return &connector.Balance{}, nil
}
func (c *plainStubConnector) GetPositions(ctx context.Context) ([]*connector.Position, error) {
	return nil, nil
}
func (c *plainStubConnector) GetTrades(ctx context.Context, start, end time.Time) ([]*connector.Trade, error) {
	return nil, nil
}
func (c *plainStubConnector) TestConnection(ctx context.Context) error { return nil }
func (c *plainStubConnector) Exchange() string                        { return "test" }

// staleStubConnector adds BalanceFreshnessProvider with a fixed as-of date.
type staleStubConnector struct {
	plainStubConnector
	asOf time.Time
}

func (c *staleStubConnector) BalanceAsOf() time.Time { return c.asOf }

func utcDay(y int, m time.Month, d int) time.Time {
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

func TestLastExpectedStatementDate(t *testing.T) {
	cases := []struct {
		name       string
		startOfDay time.Time
		want       time.Time
	}{
		{"tuesday expects monday", utcDay(2026, 8, 4), utcDay(2026, 8, 3)},
		{"monday expects friday", utcDay(2026, 8, 3), utcDay(2026, 7, 31)},
		{"sunday expects friday", utcDay(2026, 8, 2), utcDay(2026, 7, 31)},
		{"saturday expects friday", utcDay(2026, 8, 1), utcDay(2026, 7, 31)},
		{"wednesday expects tuesday", utcDay(2026, 7, 29), utcDay(2026, 7, 28)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := lastExpectedStatementDate(tc.startOfDay); !got.Equal(tc.want) {
				t.Fatalf("lastExpectedStatementDate(%s) = %s, want %s",
					tc.startOfDay.Format("2006-01-02"), got.Format("2006-01-02"), tc.want.Format("2006-01-02"))
			}
		})
	}
}

func TestStaleBalanceSkipReason(t *testing.T) {
	saturday := utcDay(2026, 8, 1)
	friday := utcDay(2026, 7, 31)

	t.Run("connector without freshness fails open", func(t *testing.T) {
		if r := staleBalanceSkipReason(&plainStubConnector{}, saturday); r != "" {
			t.Fatalf("expected no skip, got %q", r)
		}
	})

	t.Run("zero as-of fails open", func(t *testing.T) {
		if r := staleBalanceSkipReason(&staleStubConnector{}, saturday); r != "" {
			t.Fatalf("expected no skip, got %q", r)
		}
	})

	t.Run("friday statement is fresh for saturday", func(t *testing.T) {
		c := &staleStubConnector{asOf: utcDay(2026, 7, 31)}
		if r := staleBalanceSkipReason(c, saturday); r != "" {
			t.Fatalf("expected no skip, got %q", r)
		}
	})

	t.Run("thursday statement is stale for saturday", func(t *testing.T) {
		// The exact phantom case from the 2026-08-01 audit: the Saturday
		// 00:00 sync only had Thursday's close and wrote it dated Saturday.
		c := &staleStubConnector{asOf: utcDay(2026, 7, 30)}
		if r := staleBalanceSkipReason(c, saturday); r == "" {
			t.Fatal("expected a skip reason, got none")
		}
	})

	t.Run("thursday statement is fresh for friday", func(t *testing.T) {
		c := &staleStubConnector{asOf: utcDay(2026, 7, 30)}
		if r := staleBalanceSkipReason(c, friday); r != "" {
			t.Fatalf("expected no skip, got %q", r)
		}
	})

	t.Run("wednesday statement is stale for friday", func(t *testing.T) {
		c := &staleStubConnector{asOf: utcDay(2026, 7, 29)}
		if r := staleBalanceSkipReason(c, friday); r == "" {
			t.Fatal("expected a skip reason, got none")
		}
	})
}
