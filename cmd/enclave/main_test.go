package main

import "testing"

// CFG-004: in production a benchmark URL must be https AND carry an internal
// token (the series feeds the signed report). An unset URL and development are
// exempt (dev http + tokenless test stacks are allowed).
func TestCheckBenchmarkConfig(t *testing.T) {
	const token = "tok-abcdefghijklmnopqrstuvwx" // ≥24 chars

	cases := []struct {
		name    string
		url     string
		token   string
		isDev   bool
		wantErr bool
	}{
		{"unset url ok", "", "", false, false},
		{"dev http no token ok", "http://api-test.auditzk.com", "", true, false},
		{"dev https no token ok", "https://benchmark.auditzk.com", "", true, false},
		{"prod http rejected", "http://87.106.4.85:8080", token, false, true},
		{"prod https no token rejected", "https://benchmark.auditzk.com", "", false, true},
		{"prod https with token ok", "https://benchmark.auditzk.com", token, false, false},
		{"prod scheme is case-insensitive", "HTTPS://benchmark.auditzk.com", token, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := checkBenchmarkConfig(tc.url, tc.token, tc.isDev)
			if (err != nil) != tc.wantErr {
				t.Fatalf("checkBenchmarkConfig(%q, hasToken=%v, dev=%v) err=%v, wantErr=%v",
					tc.url, tc.token != "", tc.isDev, err, tc.wantErr)
			}
		})
	}
}
