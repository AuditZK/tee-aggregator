package connector

import (
	"errors"
	"fmt"
	"testing"
)

func TestIsIPRestriction(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{
			// The case that cost two users a month: Binance answers -2015 for a
			// perfectly good key reached from an address the key does not allow.
			"binance -2015",
			fmt.Errorf(`HTTP 401: {"code":-2015,"msg":"Invalid API-key, IP, or permissions for action."}`),
			true,
		},
		{
			"okx 50110",
			fmt.Errorf(`HTTP 401: {"code":"50110","msg":"Invalid IP"}`),
			true,
		},
		{
			"bybit ip mismatch",
			fmt.Errorf(`HTTP 200: {"retCode":10010,"retMsg":"request ip mismatch"}`),
			true,
		},
		{
			"mexc not in whitelist",
			fmt.Errorf(`HTTP 400: {"code":700006,"msg":"IP not in whitelist"}`),
			true,
		},
		{
			"sentinel wrapped",
			fmt.Errorf("test connection: %w", ErrIPRestricted),
			true,
		},
		{
			// A genuinely bad secret must NOT be reported as an IP problem — the
			// user would go whitelist an address and stay broken.
			"binance -2014 bad key format",
			fmt.Errorf(`HTTP 401: {"code":-2014,"msg":"API-key format invalid."}`),
			false,
		},
		{
			"bad signature",
			fmt.Errorf(`HTTP 401: {"code":-1022,"msg":"Signature for this request is not valid."}`),
			false,
		},
		{
			"transient",
			fmt.Errorf("futures balance: %w", ErrTransient),
			false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsIPRestriction(tc.err); got != tc.want {
				t.Errorf("IsIPRestriction(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// A body that merely mentions an IP-shaped balance or a timestamp must not trip
// the classifier — the markers are phrases, never bare codes.
func TestIsIPRestrictionNoFalsePositiveOnDigits(t *testing.T) {
	err := fmt.Errorf(`HTTP 200: {"balance":"50110.00","updateTime":1700006000}`)
	if IsIPRestriction(err) {
		t.Error("numeric body must not classify as an IP restriction")
	}
}

func TestErrIPRestrictedIsMatchable(t *testing.T) {
	wrapped := fmt.Errorf("create connection: %w", ErrIPRestricted)
	if !errors.Is(wrapped, ErrIPRestricted) {
		t.Error("ErrIPRestricted must survive wrapping")
	}
}
