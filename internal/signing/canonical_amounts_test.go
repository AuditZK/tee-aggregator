package signing

import (
	"bytes"
	"encoding/json"
	"math"
	"testing"
)

// DATA-01: amounts are float64 from the connector to the signature, and the
// production columns are double precision, so nothing is rounded on the way.
// What a verifier needs is that the canonical form it rebuilds from the
// parsed payload is byte-identical to the one that was signed.
func TestCanonicalAmountsRoundTripExactly(t *testing.T) {
	amounts := []float64{
		0.1 + 0.2,
		1e-12,
		-0.000000123456789,
		123456789.12345678,
		987654321987.6543,
		math.MaxFloat64,
		math.SmallestNonzeroFloat64,
		1 << 53,
	}
	payload := map[string]any{"amounts": amounts, "equity": 1234567.891011121}
	for i, a := range amounts {
		payload[string(rune('a'+i))] = a
	}

	signed, err := marshalSortedJSON(payload)
	if err != nil {
		t.Fatal(err)
	}
	var parsed any
	if err := json.Unmarshal(signed, &parsed); err != nil {
		t.Fatal(err)
	}
	rebuilt, err := marshalSortedJSON(parsed)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(signed, rebuilt) {
		t.Fatalf("canonical form moved after a parse:\nsigned  %s\nrebuilt %s", signed, rebuilt)
	}
}

func TestCanonicalRefusesNonFiniteAmounts(t *testing.T) {
	for _, v := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		if out, err := marshalSortedJSON(map[string]any{"equity": v}); err == nil {
			t.Errorf("%v encoded as %s, want signing to fail", v, out)
		}
	}
}
