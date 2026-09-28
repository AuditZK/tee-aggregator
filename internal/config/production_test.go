package config

import "testing"

// ARCH-008: the boot guards (IsDevelopment) and everything reading the
// environment directly (IsProductionEnv) must agree on every value.
func TestOneDefinitionOfProduction(t *testing.T) {
	cases := []struct {
		env, nodeEnv string
		production   bool
	}{
		{"production", "", true},
		{"Production", "", true},
		{" production ", "", true},
		{"staging", "production", true},
		{"", "PRODUCTION", true},
		{"development", "", false},
		{"", "", false},
		{"prod", "", false},
	}
	for _, tc := range cases {
		t.Setenv("ENV", tc.env)
		t.Setenv("NODE_ENV", tc.nodeEnv)
		cfg := &Config{Env: tc.env, NodeEnv: tc.nodeEnv}

		if got := IsProductionEnv(); got != tc.production {
			t.Errorf("ENV=%q NODE_ENV=%q: IsProductionEnv = %v, want %v", tc.env, tc.nodeEnv, got, tc.production)
		}
		if cfg.IsDevelopment() == IsProductionEnv() {
			t.Errorf("ENV=%q NODE_ENV=%q: the boot guards and the rest of the enclave disagree", tc.env, tc.nodeEnv)
		}
	}
}
