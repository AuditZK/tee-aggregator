package connector

import (
	"context"
	"testing"
)

func TestCTraderDetectIsPaper(t *testing.T) {
	demo := NewCTrader(&Credentials{Exchange: "ctrader", APIKey: "token", Passphrase: "demo"})
	isPaperDemo, err := demo.DetectIsPaper(context.Background())
	if err != nil {
		t.Fatalf("demo DetectIsPaper error: %v", err)
	}
	if !isPaperDemo {
		t.Fatal("expected demo account to be paper")
	}

	live := NewCTrader(&Credentials{Exchange: "ctrader", APIKey: "token", Passphrase: "live"})
	isPaperLive, err := live.DetectIsPaper(context.Background())
	if err != nil {
		t.Fatalf("live DetectIsPaper error: %v", err)
	}
	if isPaperLive {
		t.Fatal("expected live account to be non-paper")
	}
}
