module github.com/trackrecord/enclave

// SUPPLY-001: pin to 1.26.4 to pick up the stdlib CVEs fixed across the
// 1.26.x series. The two reachable at this commit (govulncheck) are
// GO-2026-5037 (crypto/x509 VerifyHostname) and GO-2026-5039 (net/textproto
// error injection), both fixed in 1.26.4 — which also carries GO-2026-5038
// (mime CPU exhaustion). Earlier pins covered the TLS 1.3 KeyUpdate DoS,
// x509 chain-building / name-constraint bypasses, net/url IPv6 host parse,
// os FileInfo root escape (≤1.26.2), html/template escaper-bypass XSS
// (GO-2026-4982/4980), net NUL-byte panic (GO-2026-4971) and the net/http
// HTTP/2 SETTINGS infinite loop (GO-2026-4918) (≤1.26.3). The Docker images
// pin golang:1.26.4-alpine; this makes local developer builds match.
go 1.26.4

require (
	github.com/google/uuid v1.6.0
	github.com/gorilla/websocket v1.4.2
	github.com/jackc/pgx/v5 v5.9.2
	go.uber.org/zap v1.26.0
	golang.org/x/crypto v0.52.0
	golang.org/x/sync v0.20.0
	golang.org/x/term v0.43.0
	google.golang.org/grpc v1.79.3
	google.golang.org/protobuf v1.36.10
)

require (
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	go.uber.org/goleak v1.3.0 // indirect
	go.uber.org/multierr v1.10.0 // indirect
	golang.org/x/net v0.55.0 // indirect
	golang.org/x/sys v0.45.0 // indirect
	golang.org/x/text v0.37.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20251202230838-ff82c1b0f217 // indirect
)
