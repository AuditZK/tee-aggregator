package connector

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

// A server name decides where the bridge connects. One that carries a port,
// a scheme or a path is refused before the bridge is ever called, and reads as
// an unknown server so the holder is asked for the right name.
func TestMetaTraderRefusesWhatIsNotAServerName(t *testing.T) {
	var calls atomic.Int32
	bridge := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer bridge.Close()
	t.Setenv("NODE_ENV", "development")
	t.Setenv("ENV", "")
	t.Setenv("MT_BRIDGE_URL", bridge.URL)

	for _, server := range []string{
		"host.example:8090", "wss://host.example", "host.example/path", "10.0.0.5:22",
		"a@host.example", "-Broker", "Broker\nLive",
	} {
		mt, err := NewMetaTrader(&Credentials{Exchange: "mt5", APIKey: "12345", APISecret: "pw", Passphrase: server})
		if err != nil {
			t.Fatalf("%q: build: %v", server, err)
		}
		err = mt.TestConnection(context.Background())
		if ClassifyConnectFailure(err) != ConnectFailureUnknownServer {
			t.Errorf("%q: err %v, want an unknown server", server, err)
		}
	}
	if n := calls.Load(); n != 0 {
		t.Fatalf("the bridge was called %d times with a refused name", n)
	}
}

func TestMetaTraderAcceptsServerNames(t *testing.T) {
	for _, server := range []string{"BrokerName-Live 3", "BrokerSC-MT5-2", "Broker_Demo01", "mt5.broker.example"} {
		if !mtServerName.MatchString(server) {
			t.Errorf("%q refused", server)
		}
	}
}
