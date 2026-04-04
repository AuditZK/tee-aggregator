package service

import (
	"testing"
	"time"
)

func TestRateLimiter_Constants(t *testing.T) {
	expectedCooldown := 23 * time.Hour
	if rateLimitCooldown != expectedCooldown {
		t.Fatalf("rateLimitCooldown: got %v, want %v", rateLimitCooldown, expectedCooldown)
	}

	expectedRetention := 7 * 24 * time.Hour
	if cleanupRetention != expectedRetention {
		t.Fatalf("cleanupRetention: got %v, want %v", cleanupRetention, expectedRetention)
	}
}
