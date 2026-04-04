package connector

import (
	"context"
	"testing"
)

func TestAlpacaDetectIsPaper(t *testing.T) {
	tests := []struct {
		name    string
		apiKey  string
		expects bool
	}{
		{name: "paper key uppercase", apiKey: "PK-TEST-KEY", expects: true},
		{name: "paper key lowercase", apiKey: "pk-test-key", expects: true},
		{name: "live key", apiKey: "AK-TEST-KEY", expects: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a := NewAlpaca(&Credentials{APIKey: tc.apiKey, APISecret: "secret"})
			isPaper, err := a.DetectIsPaper(context.Background())
			if err != nil {
				t.Fatalf("DetectIsPaper returned error: %v", err)
			}
			if isPaper != tc.expects {
				t.Fatalf("DetectIsPaper mismatch: got %v, want %v", isPaper, tc.expects)
			}
		})
	}
}
