package service

import (
	"bytes"
	"testing"
)

// The same keys open a live and a demo BingX connector. Sharing one cache
// entry would let a demo row sync the live account, or the other way round.
func TestConnectorCacheHash_SeparatesDemo(t *testing.T) {
	live := &Credentials{APIKey: "k", APISecret: "s"}
	demo := &Credentials{APIKey: "k", APISecret: "s", Demo: true}
	if bytes.Equal(connectorCacheHash(live), connectorCacheHash(demo)) {
		t.Fatal("live and demo connectors share a cache key")
	}
	if !bytes.Equal(connectorCacheHash(live), connectorCacheHash(&Credentials{APIKey: "k", APISecret: "s"})) {
		t.Fatal("cache key is not stable for the same live credentials")
	}
}
