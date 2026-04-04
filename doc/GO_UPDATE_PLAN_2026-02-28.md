# Go Update Plan (2026-02-28)

## Scope
Repository: `D:\Dev\zero-knowledge-aggregator-go`
Reference parity target: `D:\Dev\zero-knowledge-aggregator` (TypeScript)

## Current Status

### Already in place
- `platform_hash` migration exists: `migrations/008_add_platform_hash.sql`.
- `credentials_hash` and `sync_interval_minutes` migrations exist: `migrations/009_add_credentials_hash.sql`.
- `Mock` connector exists: `internal/connector/mock.go`.
- Production plaintext REST credential endpoint is disabled (`/api/v1/connection` returns 410 outside dev).

### Updated in this pass
- gRPC hardening:
  - `GRPC_INSECURE` is now enforced (allowed only in development).
  - gRPC starts with TLS when `GRPC_INSECURE=false`.
  - Internal gRPC errors are sanitized in production (`Internal server error`).
- gRPC protocol parity baseline:
  - `api/proto/enclave.proto` is now aligned to TS proto (with Go `go_package` only).
  - Go protobuf + gRPC bindings are generated in `api/proto/*.pb.go`.
  - `internal/grpc/server.go` now uses generated protobuf service/types (manual JSON descriptor removed).
- Lighter connector:
  - Trades API updated to use `account_index` + pagination (`/api/v1/trades`).
  - Legacy fallback kept (`/api/v1/fills/{wallet}`) for compatibility.
- Connection metadata parity:
  - `credentials_hash` is generated and persisted for parity metadata.
  - Create connection is idempotent on `(user_uid, exchange, label)` (TS behavior): existing active connection returns success/no-op instead of hard failure.
  - `sync_interval_minutes` is accepted and persisted when the column exists.
  - `exclude_from_report` is accepted and persisted when the column exists.
  - Exchange identifiers are normalized (`lowercase/trim`) on create/sync paths to avoid case-sensitive connector mismatches.
  - Repository keeps backward compatibility if migrations are missing.
- Exclusion flow parity:
  - `exclude_from_report` is now applied in REST/gRPC metrics, snapshot time series, aggregated metrics, and signed report generation.
  - Filtering now supports `exchange/label` keys with fallback to exchange-level keys.
- Snapshot label parity:
  - Added snapshot label migration support to store per-connection snapshots (`exchange+label`) instead of exchange-only rows.
  - Sync pipeline now writes snapshots per active connection label and resolves credentials by `user+exchange+label`.
  - Exclusion filtering now supports keys `exchange/label` with fallback `exchange`.
  - `SyncExchange(user, exchange)` now synchronizes all active labels for that exchange and aggregates the response.
- Exchange metadata parity groundwork:
  - Added migration `migrations/012_add_exchange_metadata.sql` with `exchange_connections.kyc_level` and `exchange_connections.is_paper`.
  - `ReportService` now enriches signed report `exchange_details` from connection metadata when available (fallback defaults preserved).
- Exchange metadata capture parity:
  - `ConnectionService.Create` now triggers non-blocking metadata capture after successful connection creation (TS flow parity).
  - Optional connector capabilities added:
    - `FetchKYCLevel(ctx)` for KYC metadata
    - `DetectIsPaper(ctx)` for paper/live status
  - Initial Go implementations:
    - `CCXTConnector.FetchKYCLevel` for `bybit` / `okx` / `kucoin` with TS-equivalent normalization (`none/basic/intermediate/advanced`).
    - `CCXTConnector.DetectIsPaper` (live by default, TS parity).
    - `Alpaca.DetectIsPaper` (API key prefix `PK`).
    - `CTrader.DetectIsPaper` (demo endpoint -> paper).
    - `TradeStation.DetectIsPaper` (all account types prefixed `Sim*`).
    - `IBKR.DetectIsPaper` (Flex `accountId` prefix `DU`/`DF`).
    - `Deribit` / `Hyperliquid` / `Lighter` paper detection defaults to live (`false`) like TS connectors.
  - Repository now persists metadata updates via:
    - `UpdateKYCLevel(connection_id, level)`
    - `UpdateIsPaper(connection_id, is_paper)`
  - Metadata capture failures are intentionally non-fatal to preserve connection creation success semantics.
- Metrics parity uplift:
  - `internal/service/metrics.go` now uses report-aligned TWR daily conversion for multi-exchange series.
  - gRPC/REST metrics exchange filter is now effectively applied (was previously validated but ignored).
  - IBKR `stocks` market type is now preserved end-to-end (sync aggregation + repository breakdown + gRPC `DailySnapshot.breakdown.stocks` mapping), instead of being folded into `spot`.
- Scheduler interval parity:
  - `internal/scheduler/daily.go` now evaluates users every 5 minutes and applies `sync_interval_minutes` at connection level.
  - `internal/service/sync.go` now syncs only due connections based on `sync_statuses.last_sync_time`.
  - Sync attempts are now recorded back into `sync_statuses` to enforce interval-based scheduling.
- gRPC sync response correctness:
  - `ProcessSyncJob` now aggregates all per-connection results when syncing all exchanges (total `synced`, `snapshots_generated`, latest snapshot, aggregated errors), instead of returning only the first result.
  - gRPC response timestamps now use epoch milliseconds for TS parity (`latest_snapshot.timestamp`, `last_sync`, `period_start`, `period_end`).
- gRPC validation and report period parity:
  - `CreateUserConnection` and `ProcessSyncJob` validation failures now return gRPC `INVALID_ARGUMENT` status (TS behavior).
  - Validation now runs before service-availability checks in gRPC handlers (`CreateUserConnection`, `ProcessSyncJob`) to match TS request-processing order.
  - `VerifyReportSignature` missing fields now returns gRPC `INVALID_ARGUMENT`.
  - `CreateUserConnection`/`ProcessSyncJob` operational failures now follow TS flow with response payload errors (`success=false`) rather than forcing gRPC `INTERNAL` status.
  - Report dates are optional in validation; when `start_date` or `end_date` is missing, Go now resolves the missing bound(s) from available snapshots.
- Report signing crypto parity:
  - Go signer migrated from `Ed25519` to `ECDSA-P256-SHA256` (TS parity).
  - `public_key` is now base64 DER (SPKI), and signature verification follows TS semantics (`sign(reportHash string)`).
  - Attestation `reportSigning.algorithm` now reports `ECDSA-P256-SHA256`.
- REST attestation + E2E payload parity:
  - `/api/v1/attestation` now returns TS-style camelCase payload (`attestation`, `tlsBinding`, `e2eEncryption`, `reportSigning`, `security`).
  - `/health` now matches TS shape: `{ status: "ok", service: "enclave-rest", tls: true|false }`.
  - `/api/v1/credentials/connect` now accepts TS encrypted envelope:
    - `encrypted.ephemeralPublicKey`
    - `encrypted.iv`
    - `encrypted.ciphertext`
    - `encrypted.tag`
  - Plaintext submission is explicitly rejected with TS-style hint text.
- Log server parity uplift:
  - Added public endpoints `GET /attestation` and `GET /attestation/info` on Go log stream server.
  - Log server auth now matches TS fail-closed behavior when `LOG_STREAM_API_KEY` is missing (returns 503 instead of allowing access).
  - Response shapes now align with TS for:
    - `GET /health` (`service: enclave-log-server`)
    - `GET /logs` (`{ logs, count }`)
    - `POST /logs/clear` (`{ success: true, message: "Logs cleared" }`)
- Connection validation parity:
  - `label` is now required and capped at 100 chars (TS schema parity).
  - `exchange` validation now enforces TS schema rules (required, max 50, `[a-z0-9_-]+`) without Go-only hardcoded support-list rejection.
  - Exchange identifiers are now strictly lowercase at validation boundary (uppercase rejected, TS regex parity); normalization to lowercase is applied only after validation.
  - `api_secret` and `passphrase` now follow TS schema-style length checks (max 500) and are not hard-required at validation layer.
- Snapshot request timestamp validation parity:
  - gRPC `GetSnapshotTimeSeries` now validates optional `start_date` / `end_date` against TS-style constraints:
    - positive epoch millis when provided
    - `< now + 24h`
    - `start_date < end_date`
    - maximum 5-year range
- Exchange coverage progress:
  - Added Kraken support in Go (`validation` + `connector factory` + dedicated connector implementation).
  - Added Deribit support in Go (`validation` + `connector factory` + dedicated connector implementation).
  - Added MetaTrader support in Go (`mt4`/`mt5` via `mt-bridge`) (`validation` + `connector factory` + dedicated connector implementation).
  - Added Binance aliases for TS parity: `binance_futures`, `binanceusdm` -> Go `Binance` connector.
  - Unsupported-exchange factory errors now include the supported list (TS-style diagnostic message).
- REST exposure parity hardening:
  - REST now starts in HTTPS mode using file-mounted certificate/key (TS parity).
  - Legacy REST endpoints are now opt-in only via `ENABLE_LEGACY_REST=true` (strict TS surface by default).
  - TLS fingerprint format now matches TS style (`AA:BB:...` without `SHA-256:` prefix).
- Exchange coverage parity:
  - Added CCXT generic connector support for TS-missing Go exchanges: `bitget`, `mexc`, `kucoin`, `coinbase`, `gate`, `bingx`, `huobi`.
  - Factory now routes those exchanges through CCXT with `defaultType=swap` and optional proxy routing (`EXCHANGE_HTTP_PROXY` + `PROXY_EXCHANGES`) like TS.
- cTrader parity uplift:
  - Go cTrader connector migrated from REST-style calls to cTrader Open API WebSocket flow (TS parity baseline).
  - Added app auth + account auth request lifecycle with correlated `clientMsgId` responses.
  - Added token-expiry handling on `CH_ACCESS_TOKEN_INVALID` with OAuth refresh, reconnect, re-auth, and automatic retry.
  - Added symbol ID -> symbol name lookup with in-memory cache and TS-style fallback (`SYMBOL_<id>`).
  - Added WebSocket regression tests for refresh/retry on `getAccounts` and account auth paths.
- TLS policy parity (REST + gRPC):
  - REST now requires file-based TLS cert/key at startup (`TLS_CERT_PATH`, `TLS_KEY_PATH`), with hard fail if files are missing/invalid.
  - gRPC now requires file-based TLS material (`TLS_CA_CERT`, `TLS_SERVER_CERT`, `TLS_SERVER_KEY`) unless explicit dev mode `GRPC_INSECURE=true`.
  - gRPC now honors `REQUIRE_CLIENT_CERT` in dev and always requires client certs in production (mTLS parity with TS behavior).
- Regression tests added:
  - cTrader token refresh retry path (`internal/connector/ctrader_test.go`).
  - Multi-label metrics/exclusion behavior (`internal/service/metrics_test.go`).
  - Migration file parsing/order/duplicate detection (`internal/db/migrate_test.go`).
  - Due-interval evaluation behavior (`internal/service/sync_test.go`).
  - gRPC bufconn round-trip validation for key handlers (`internal/grpc/server_test.go`).
  - gRPC TCP local round-trip validation for external-client behavior (`internal/grpc/server_tcp_test.go`).
  - gRPC exchange-details mapping coverage for signed reports (`internal/grpc/server_test.go`).
  - gRPC `CreateUserConnection` payload behavior coverage for success / no-op / operational error (`internal/grpc/create_connection_test.go`).
  - REST legacy `CreateUserConnection` handler coverage for success / no-op / operational error (`internal/server/handler_create_connection_test.go`).
- Startup schema audit:
  - DB connection now checks critical columns (`platform_hash`, `credentials_hash`, `sync_interval_minutes`, `exclude_from_report`, `snapshot_data.label`) and logs warnings if missing.
- Migration robustness uplift:
  - Optional startup migration runner added (`AUTO_MIGRATE=true`, `MIGRATIONS_DIR=migrations`).
  - Applied migrations are tracked in `schema_migrations`.
- Developer docs:
  - `DEV_SETUP.md` API/gRPC payload examples corrected (`user_uid`, `api_key`, `api_secret`, proper report date format).

## Remaining Gaps

### P0 (must fix first)
1. gRPC contract mismatch with TS proto
 - Bufconn and local-TCP gRPC round-trip tests are now in place for health + validation paths.
 - Remaining: verify with full Gateway -> gRPC integration tests in CI/runtime environment.

### P1 (high)
1. cTrader parity
- Go connector now uses the same WebSocket API family as TS with app/account auth and `CH_ACCESS_TOKEN_INVALID` refresh/retry semantics.
- Remaining gap: broaden integration coverage for production network edge-cases (disconnect storms/rate limits) to match TS test depth.

2. Metrics parity
- TWR multi-exchange aggregation is now aligned at service level.
- Label-aware exclusion keys (`exchange/label`) are now supported in Go service filtering.

3. Report parity
- TS report includes exchange metadata (`exchangeDetails`, paper/KYC info).
- Go report now includes `exchange_details` structure with DB-backed metadata fields (`kyc_level`, `is_paper`) and post-create metadata capture.
- Snapshot/report API contracts still do not expose label in gRPC `DailySnapshot` (TS can reason on label keys internally).

### P2 (quality)
1. Tests
- Coverage improved for connector/service/db critical paths; integration coverage (REST/gRPC end-to-end) remains limited.

2. Migration robustness
- Runtime migration bootstrap is now available (opt-in).
- Remaining: define deployment policy (prod enable/disable and rollback strategy).

## Execution Order

### Phase 1 (security + interoperability)
1. Freeze and align canonical proto with TS (`src/proto/enclave.proto` as source of truth).
2. Replace manual gRPC request/response structs with generated protobuf bindings in Go.
3. Keep production error sanitization and TLS policy as baseline.

### Phase 2 (data and feature parity)
1. Extend `sync_interval_minutes` usage into scheduler execution strategy (not only persistence).
   - Status: done (5-minute scheduler + due checks via `sync_statuses`).
2. Add `exclude_from_report` column + flow and use it in report/metrics data selection.
   - Status: done with label-aware filtering support.
3. Add admin/ops visibility endpoints for connection metadata parity fields.
   - Status: deferred to keep strict parity with TypeScript API surface.

### Phase 3 (connector + analytics parity)
1. Extend cTrader toward WS/API parity beyond refresh retry (account auth lifecycle, richer retry semantics).
2. Keep metrics/report snapshot behavior aligned as multi-label data grows (regression tests).
3. Align report exchange metadata (paper/KYC) or explicitly document intentional divergence.

### Phase 4 (validation + QA)
1. Add gRPC/REST integration tests for parity-critical methods.
2. Add connector regression tests (Lighter, cTrader, IBKR).
3. Fix docs/examples and publish a migration checklist.
