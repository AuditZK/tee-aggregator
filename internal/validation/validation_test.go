package validation

import (
	"strings"
	"testing"
	"time"
)

func TestValidateUserUID(t *testing.T) {
	tests := []struct {
		name    string
		uid     string
		wantErr bool
	}{
		{"empty", "", true},
		{"clerk id", "user_abc1234567890", false},
		{"uuid", "550e8400-e29b-41d4-a716-446655440000", false},
		{"cuid", "clabcdefghij1234567890", false},
		{"too long", strings.Repeat("a", 101), true},
		{"invalid format", "not-valid", true},
		{"clerk too short", "user_abc", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateUserUID(tt.uid)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateUserUID(%q) error = %v, wantErr %v", tt.uid, err, tt.wantErr)
			}
		})
	}
}

func TestValidateExchange(t *testing.T) {
	tests := []struct {
		name     string
		exchange string
		wantErr  bool
	}{
		{"empty", "", true},
		{"binance", "binance", false},
		{"binance_futures", "binance_futures", false},
		{"binanceusdm", "binanceusdm", false},
		{"bybit", "bybit", false},
		{"kraken", "kraken", false},
		{"deribit", "deribit", false},
		{"mt4", "mt4", false},
		{"mt5", "mt5", false},
		{"mock", "mock", false},
		{"valid unknown", "bitstamp", false},
		{"invalid chars", "bin@nce!", true},
		{"too long", strings.Repeat("x", 51), true},
		{"uppercase rejected", "Binance", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateExchange(tt.exchange)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateExchange(%q) error = %v, wantErr %v", tt.exchange, err, tt.wantErr)
			}
		})
	}
}

func TestValidateLabel(t *testing.T) {
	if err := ValidateLabel(""); err == nil {
		t.Error("empty label should be invalid")
	}
	if err := ValidateLabel("my-account"); err != nil {
		t.Error("normal label should be valid")
	}
	if err := ValidateLabel(strings.Repeat("x", 101)); err == nil {
		t.Error("label > 100 chars should be invalid")
	}
}

func TestValidateAPIKey(t *testing.T) {
	if err := ValidateAPIKey("normal-key"); err != nil {
		t.Error("normal key should be valid")
	}
	if err := ValidateAPIKey(strings.Repeat("x", 501)); err == nil {
		t.Error("key > 500 chars should be invalid")
	}
}

func TestValidateAPISecret(t *testing.T) {
	if err := ValidateAPISecret(""); err != nil {
		t.Error("empty secret should be valid")
	}
	if err := ValidateAPISecret("normal-secret"); err != nil {
		t.Error("normal secret should be valid")
	}
	if err := ValidateAPISecret(strings.Repeat("x", 501)); err == nil {
		t.Error("secret > 500 chars should be invalid")
	}
}

func TestValidatePassphrase(t *testing.T) {
	if err := ValidatePassphrase(""); err != nil {
		t.Error("empty passphrase should be valid")
	}
	if err := ValidatePassphrase("normal-passphrase"); err != nil {
		t.Error("normal passphrase should be valid")
	}
	if err := ValidatePassphrase(strings.Repeat("x", 501)); err == nil {
		t.Error("passphrase > 500 chars should be invalid")
	}
}

func TestValidateSyncIntervalMinutes(t *testing.T) {
	tests := []struct {
		name    string
		minutes int
		wantErr bool
	}{
		{"not provided", 0, false},
		{"valid low", 5, false},
		{"valid common", 60, false},
		{"valid high", 10080, false},
		{"too low", 4, true},
		{"too high", 10081, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateSyncIntervalMinutes(tt.minutes)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateSyncIntervalMinutes(%d) error = %v, wantErr %v", tt.minutes, err, tt.wantErr)
			}
		})
	}
}

func TestValidateCreateConnection(t *testing.T) {
	tests := []struct {
		name    string
		req     *CreateConnectionRequest
		wantErr bool
	}{
		{
			"valid CEX",
			&CreateConnectionRequest{
				UserUID: "user_abc1234567890", Exchange: "binance",
				Label:  "main",
				APIKey: "key123", APISecret: "secret456", SyncIntervalMinutes: 60,
			},
			false,
		},
		{
			"valid DEX",
			&CreateConnectionRequest{
				UserUID: "user_abc1234567890", Exchange: "hyperliquid",
				Label:  "wallet-1",
				APIKey: "0xWalletAddress",
			},
			false,
		},
		{
			"valid mock",
			&CreateConnectionRequest{
				UserUID: "user_abc1234567890", Exchange: "mock",
				Label:  "stress",
				APIKey: "test",
			},
			false,
		},
		{
			"missing label",
			&CreateConnectionRequest{
				UserUID: "user_abc1234567890", Exchange: "binance",
				APIKey: "key123", APISecret: "secret456",
			},
			true,
		},
		{
			"missing api_secret allowed (TS parity)",
			&CreateConnectionRequest{
				UserUID: "user_abc1234567890", Exchange: "binance",
				Label:  "main",
				APIKey: "key123",
			},
			false,
		},
		{
			"missing api_key",
			&CreateConnectionRequest{
				UserUID: "user_abc1234567890", Exchange: "binance",
				Label: "main",
			},
			true,
		},
		{
			"api_secret too long",
			&CreateConnectionRequest{
				UserUID: "user_abc1234567890", Exchange: "binance",
				Label:     "main",
				APIKey:    "key",
				APISecret: strings.Repeat("x", 501),
			},
			true,
		},
		{
			"passphrase too long",
			&CreateConnectionRequest{
				UserUID: "user_abc1234567890", Exchange: "binance",
				Label:      "main",
				APIKey:     "key",
				APISecret:  "secret",
				Passphrase: strings.Repeat("x", 501),
			},
			true,
		},
		{
			"invalid user",
			&CreateConnectionRequest{
				UserUID: "bad", Exchange: "binance",
				Label:  "main",
				APIKey: "key", APISecret: "secret",
			},
			true,
		},
		{
			"invalid sync interval",
			&CreateConnectionRequest{
				UserUID: "user_abc1234567890", Exchange: "binance",
				Label:  "main",
				APIKey: "key", APISecret: "secret", SyncIntervalMinutes: 1,
			},
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateCreateConnection(tt.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateCreateConnection() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidateReportRequest(t *testing.T) {
	tests := []struct {
		name    string
		req     *ReportRequest
		wantErr bool
	}{
		{
			"valid",
			&ReportRequest{
				UserUID: "user_abc1234567890", StartDate: "2025-01-01", EndDate: "2025-12-31",
			},
			false,
		},
		{
			"missing start (optional)",
			&ReportRequest{
				UserUID: "user_abc1234567890", EndDate: "2025-12-31",
			},
			false,
		},
		{
			"missing end (optional)",
			&ReportRequest{
				UserUID: "user_abc1234567890", StartDate: "2025-01-01",
			},
			false,
		},
		{
			"both missing (optional)",
			&ReportRequest{
				UserUID: "user_abc1234567890",
			},
			false,
		},
		{
			"end before start",
			&ReportRequest{
				UserUID: "user_abc1234567890", StartDate: "2025-12-31", EndDate: "2025-01-01",
			},
			true,
		},
		{
			"range too large",
			&ReportRequest{
				UserUID: "user_abc1234567890", StartDate: "2020-01-01", EndDate: "2026-01-01",
			},
			true,
		},
		{
			"invalid date format",
			&ReportRequest{
				UserUID: "user_abc1234567890", StartDate: "01-01-2025", EndDate: "12-31-2025",
			},
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateReportRequest(tt.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateReportRequest() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidateTimestampRange(t *testing.T) {
	tests := []struct {
		name      string
		startYear int
		endYear   int
		wantErr   bool
	}{
		{"1 year", 2025, 2026, false},
		{"5 years exact", 2020, 2025, false},
		{"6 years", 2019, 2025, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start := time.Date(tt.startYear, 1, 1, 0, 0, 0, 0, time.UTC)
			end := time.Date(tt.endYear, 1, 1, 0, 0, 0, 0, time.UTC)
			err := ValidateTimestampRange(start, end)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateTimestampRange() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidateOptionalTimestampMillis(t *testing.T) {
	now := time.Now().UnixMilli()
	tests := []struct {
		name    string
		ts      int64
		field   string
		wantErr bool
	}{
		{name: "optional zero", ts: 0, field: "start_date", wantErr: false},
		{name: "valid now", ts: now, field: "start_date", wantErr: false},
		{name: "negative invalid", ts: -1, field: "start_date", wantErr: true},
		{name: "too future invalid", ts: now + (25 * 60 * 60 * 1000), field: "end_date", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateOptionalTimestampMillis(tt.ts, tt.field)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateOptionalTimestampMillis(%d, %q) error = %v, wantErr %v", tt.ts, tt.field, err, tt.wantErr)
			}
		})
	}
}
