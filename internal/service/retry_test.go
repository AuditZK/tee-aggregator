package service

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRetryWithBackoff(t *testing.T) {
	t.Run("succeeds on first attempt", func(t *testing.T) {
		calls := 0
		err := retryWithBackoff(context.Background(), 3, time.Millisecond, func() error {
			calls++
			return nil
		})
		if err != nil || calls != 1 {
			t.Fatalf("err=%v calls=%d, want nil/1", err, calls)
		}
	})

	t.Run("succeeds after transient failures", func(t *testing.T) {
		calls := 0
		err := retryWithBackoff(context.Background(), 3, time.Millisecond, func() error {
			calls++
			if calls < 3 {
				return errors.New("transient")
			}
			return nil
		})
		if err != nil || calls != 3 {
			t.Fatalf("err=%v calls=%d, want nil/3", err, calls)
		}
	})

	t.Run("returns the final error when attempts are exhausted", func(t *testing.T) {
		calls := 0
		want := errors.New("persistent")
		err := retryWithBackoff(context.Background(), 3, time.Millisecond, func() error {
			calls++
			return want
		})
		if !errors.Is(err, want) || calls != 3 {
			t.Fatalf("err=%v calls=%d, want persistent/3", err, calls)
		}
	})

	t.Run("aborts on context cancellation without further attempts", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		calls := 0
		err := retryWithBackoff(ctx, 5, time.Second, func() error {
			calls++
			return errors.New("fail")
		})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err=%v, want context.Canceled", err)
		}
		if calls != 1 {
			t.Fatalf("calls=%d, want 1 — cancellation must abort before the second attempt", calls)
		}
	})
}
