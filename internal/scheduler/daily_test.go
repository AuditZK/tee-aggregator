package scheduler

import (
	"testing"

	"github.com/trackrecord/enclave/internal/rebuilderclient"
)

func TestRecalibrationBudgetFitsSeveralFullRebuilds(t *testing.T) {
	if recalibrationBudget < 3*rebuilderclient.RequestTimeout {
		t.Fatalf("recalibration budget %v fits fewer than three full rebuilder calls (%v each)", recalibrationBudget, rebuilderclient.RequestTimeout)
	}
}
