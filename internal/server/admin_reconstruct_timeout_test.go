package server

import (
	"testing"

	"github.com/trackrecord/enclave/internal/rebuilderclient"
)

func TestAdminReconstructOutlastsTheRebuilderCall(t *testing.T) {
	if adminReconstructTimeout <= rebuilderclient.RequestTimeout {
		t.Fatalf("admin reconstruct %v expires before the rebuilder call %v", adminReconstructTimeout, rebuilderclient.RequestTimeout)
	}
}
