module github.com/trackrecord/enclave

// SUPPLY-001: pin to 1.26.2 to pick up the stdlib CVEs fixed in 1.26.1
// and 1.26.2 (TLS 1.3 KeyUpdate DoS, x509 chain-building / name-constraint
// auth bypasses, html/template XSS, net/url IPv6 host parse, os FileInfo
// root escape). The Docker images already pin golang:1.26.2-alpine; this
// makes local developer builds match.
go 1.26.2

require (
	github.com/google/uuid v1.6.0
	github.com/gorilla/websocket v1.4.2
	github.com/jackc/pgx/v5 v5.9.0
	go.uber.org/zap v1.26.0
	golang.org/x/crypto v0.46.0
	golang.org/x/sync v0.19.0
	golang.org/x/term v0.42.0
	google.golang.org/grpc v1.79.3
	google.golang.org/protobuf v1.36.10
)

require (
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	go.uber.org/goleak v1.3.0 // indirect
	go.uber.org/multierr v1.10.0 // indirect
	golang.org/x/net v0.48.0 // indirect
	golang.org/x/sys v0.43.0 // indirect
	golang.org/x/text v0.32.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20251202230838-ff82c1b0f217 // indirect
)
