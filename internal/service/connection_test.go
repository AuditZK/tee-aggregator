package service

import "testing"

func TestNormalizeKYCLevel(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{input: "none", want: "none"},
		{input: "BASIC", want: "basic"},
		{input: " intermediate ", want: "intermediate"},
		{input: "advanced", want: "advanced"},
		{input: "", want: ""},
		{input: "unknown", want: ""},
	}

	for _, tc := range tests {
		if got := normalizeKYCLevel(tc.input); got != tc.want {
			t.Fatalf("normalizeKYCLevel(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}
