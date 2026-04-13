# Track Record Enclave Worker (Go)

**Trusted Computing Base for Confidential Trading Data Aggregation**

[![License: ASAL](https://img.shields.io/badge/License-ASAL%20v1.0-blue.svg)](LICENSE)
[![TCB](https://img.shields.io/badge/TCB-~8,500%20LOC-green.svg)]()
[![Go](https://img.shields.io/badge/Go-1.22+-00ADD8.svg)]()
[![AMD SEV-SNP](https://img.shields.io/badge/AMD-SEV--SNP-red.svg)]()

## Overview

This repository contains the **Go implementation** of the Track Record platform's Enclave Worker — the **Trusted Computing Base (TCB)** responsible for confidential trading data aggregation within an AMD SEV-SNP hardware-isolated enclave.

This is a full rewrite of the [TypeScript enclave](https://github.com/AuditZK/zero-knowledge-aggregator) in Go, with native exchange connectors replacing CCXT, lower memory footprint, and improved performance.

**This repository serves two purposes:**

1. **Primary Development Repository**: Active development of the Go Enclave Worker.
2. **Public Audit & Verification**: Published for independent security audits and reproducible build verification.

## Table of Contents

- [Security Model](#security-model)
- [Architecture](#architecture)
- [Trusted Computing Base](#trusted-computing-base)
- [Threat Model](#threat-model)
- [Audit Process](#audit-process)
- [Reproducible Builds](#reproducible-builds)
- [API Specification](#api-specification)

## Security Model

### Trust Assumptions

1. **Hardware Root of Trust**: AMD SEV-SNP provides memory encryption and attestation
2. **Database Isolation**: PostgreSQL user `enclave_user` has exclusive access to sensitive tables
3. **Network Isolation**: Enclave not exposed to public internet — gRPC over mTLS only
4. **Cryptographic Primitives**: AES-256-GCM for credential encryption (FIPS 140-2 compliant)

### Security Guarantees

| Property | Guarantee | Mechanism |
|----------|-----------|-----------|
| **Credential Confidentiality** | API keys never leave enclave memory | Hardware memory encryption (SEV-SNP) |
| **Trade Privacy** | Individual trades never transmitted | Data aggregation within enclave boundary |
| **Code Integrity** | Binary matches audited source | Reproducible builds + attestation |
| **Isolation** | Hypervisor cannot access memory | AMD SEV-SNP VMPL protection |

### Non-Goals

- Protection against timing side-channels (out of scope)
- Protection against physical access to hardware
- Protection against compromised AMD firmware

## Architecture

### System Context

```
┌─────────────────────────────────────────────────────────────┐
│  API Gateway (Untrusted Zone)                               │
│  - HTTP REST API (public-facing)                            │
│  - Authentication, rate limiting                            │
│  - Access: aggregated data only                             │
│  - Database: snapshot_data (READ)                           │
│  - Code: NOT in this repository (proprietary)               │
└────────────────────────┬────────────────────────────────────┘
                         │ gRPC over mTLS
                         │ Port: 50051 (internal network)
                         ▼
╔═════════════════════════════════════════════════════════════╗
║  Enclave Worker — Go (Trusted Zone — THIS REPOSITORY)       ║
╟─────────────────────────────────────────────────────────────╢
║  AMD SEV-SNP VM (Hardware Isolation)                        ║
║                                                             ║
║  ┌───────────────────────────────────────────────────────┐ ║
║  │  Autonomous Daily Sync Scheduler (00:00 UTC)         │ ║
║  │  - SyncScheduler (internal/scheduler)                │ ║
║  │  - Triggers daily snapshots for ALL active users     │ ║
║  │  - Rate-limited (23h cooldown per user/exchange)     │ ║
║  │  - Audit trail: sync_statuses table                  │ ║
║  └───────────────────────────────────────────────────────┘ ║
║                                                             ║
║  ┌───────────────────────────────────────────────────────┐ ║
║  │  gRPC Server (Port 50051)                             │ ║
║  │  - ProcessSyncJob (manual sync)                      │ ║
║  │  - GetAggregatedMetrics                              │ ║
║  │  - HealthCheck                                       │ ║
║  └───────────────────────────────────────────────────────┘ ║
║                                                             ║
║  ┌───────────────────────────────────────────────────────┐ ║
║  │  Core Services (internal/service)                     │ ║
║  │  ┌─────────────────────────────────────────────────┐ │ ║
║  │  │  EncryptionService                              │ │ ║
║  │  │  - AES-256-GCM credential decryption            │ │ ║
║  │  │  - DEK unwrap from data_encryption_keys table   │ │ ║
║  │  │  - SEV-SNP master key derivation                │ │ ║
║  │  └─────────────────────────────────────────────────┘ │ ║
║  │  ┌─────────────────────────────────────────────────┐ │ ║
║  │  │  SyncService                                    │ │ ║
║  │  │  - Exchange polling orchestration               │ │ ║
║  │  │  - Atomic snapshot batch save (all-or-nothing)  │ │ ║
║  │  └─────────────────────────────────────────────────┘ │ ║
║  │  ┌─────────────────────────────────────────────────┐ │ ║
║  │  │  RateLimiterService                             │ │ ║
║  │  │  - 23-hour cooldown enforcement                 │ │ ║
║  │  │  - Prevents cherry-picking via manual API calls │ │ ║
║  │  └─────────────────────────────────────────────────┘ │ ║
║  └───────────────────────────────────────────────────────┘ ║
║                                                             ║
║  ┌───────────────────────────────────────────────────────┐ ║
║  │  Exchange Connectors (internal/connector)             │ ║
║  │  - Native HTTP connectors (no CCXT)                  │ ║
║  │  - Binance, Bybit, OKX, Kraken, Deribit, MEXC       │ ║
║  │  - Bitget, KuCoin, Coinbase, Gate, BingX, Huobi      │ ║
║  │  - IBKR (Flex Report), Alpaca, TradeStation          │ ║
║  │  - Hyperliquid, Lighter (DEX)                        │ ║
║  │  - cTrader, MetaTrader 4/5                           │ ║
║  └───────────────────────────────────────────────────────┘ ║
║                                                             ║
║  ┌───────────────────────────────────────────────────────┐ ║
║  │  Database Layer (internal/repository)                 │ ║
║  │  - Driver: pgx/v5 (parameterized queries only)       │ ║
║  │  - Tables: snapshot_data (W), sync_statuses (R/W)    │ ║
║  │  - Dual-schema: TS camelCase / Go snake_case compat  │ ║
║  └───────────────────────────────────────────────────────┘ ║
╚═════════════════════════════════════════════════════════════╝

Output: Aggregated daily snapshots only (no individual trades)
Autonomous: Daily sync at 00:00 UTC for all active users
```

### Data Flow

```
AUTONOMOUS SCHEDULER (00:00 UTC daily)
         │
         ▼
External Exchange APIs (Binance, IBKR, Alpaca, …)
         │
         ▼
  ┌──────────────┐
  │ Credentials  │ ◄── Encrypted in PostgreSQL (AES-256-GCM)
  │ (decrypted   │     DEK unwrapped via SEV-SNP measurement
  │  in RAM)     │     or ENCRYPTION_KEY env var (dev only)
  └──────────────┘
         │
         ▼
  ┌──────────────┐
  │ Current      │ ◄── Fetched via HTTPS (TLS 1.2+)
  │ Account      │     totalEquity, realizedBalance, unrealizedPnL
  │ State        │     Deposits/withdrawals
  └──────────────┘
         │
         ▼
  ┌──────────────┐
  │ Create Daily │ ◄── SyncService.SyncUserScheduledDueAtomic
  │ Snapshot     │     Timestamp: 00:00 UTC (startOfDay)
  └──────────────┘     Fields: totalEquity, realizedBalance,
         │               unrealizedPnL, deposits, withdrawals,
         │               breakdown_by_market (per market type)
         ▼
  ┌──────────────┐
  │ Atomic Save  │ ◄── PostgreSQL snapshot_data table
  │ (all-or-     │     All-or-nothing transaction per user
  │  nothing)    │     One snapshot per day per user/exchange/label
  └──────────────┘
         │
         ▼
  ┌──────────────┐
  │ Rate Limiter │ ◄── sync_statuses table
  │ / Audit Log  │     Prevents manual cherry-picking (23h cooldown)
  └──────────────┘
         │
         ▼
  API Gateway (untrusted) → Frontend (public)
```

## Trusted Computing Base

### Size Metrics

| Component | Package | LOC | Purpose |
|-----------|---------|-----|---------|
| **EncryptionService** | `internal/encryption` | ~600 | AES-256-GCM, DEK management, SEV-SNP key derivation |
| **Exchange Connectors** | `internal/connector` | ~4,200 | 20+ native connectors (no CCXT) |
| **SyncService** | `internal/service` | ~1,150 | Orchestration, snapshot creation, rate limiting |
| **gRPC Server** | `internal/grpc` | ~800 | Server, request handling, mTLS |
| **REST Server** | `internal/server` | ~200 | Admin endpoints, health check |
| **Repositories** | `internal/repository` | ~900 | Database access (pgx, dual-schema) |
| **Scheduler** | `internal/scheduler` | ~200 | Autonomous daily sync (00:00 UTC) |
| **Attestation** | `internal/attestation` | ~200 | SEV-SNP VCEK verification |
| **Proxy / Cache / Security** | `internal/{proxy,cache,security}` | ~250 | HTTP proxy, connector cache, mlock |
| **Entry Point** | `cmd/enclave` | ~300 | main.go, DI wiring |
| **Total** | | **~8,800** | Minimized attack surface |

**Rationale**: Native Go connectors replace CCXT (150MB per LoadMarkets call → ~5MB per connector), enabling a larger connector matrix with a smaller memory footprint. The TCB is larger than the TS implementation in LOC but eliminates an entire npm dependency tree from the audit scope.

### Dependencies

Critical dependencies (included in TCB audit scope):

| Module | Version | Purpose |
|--------|---------|---------|
| `github.com/jackc/pgx/v5` | 5.x | PostgreSQL driver (parameterized queries) |
| `google.golang.org/grpc` | 1.x | gRPC implementation |
| `go.uber.org/zap` | 2.x | Structured logging (PII redaction) |
| `github.com/golang-jwt/jwt/v5` | 5.x | JWT validation (cTrader OAuth) |

Total direct dependencies: ~15 modules (`go.sum` pins exact hashes for all transitive deps)

## Threat Model

### In-Scope Threats

#### 1. Compromised API Gateway
**Threat**: Attacker gains control of the API Gateway.

**Mitigation**:
- Gateway runs outside enclave with READ-only access to `snapshot_data`
- gRPC responses contain only aggregated daily snapshots
- sync_statuses audit trail proves systematic snapshot creation

#### 2. Compromised Hypervisor
**Threat**: Cloud provider or attacker reads VM memory.

**Mitigation**:
- AMD SEV-SNP encrypts VM memory with hardware-managed keys
- Attestation verifies binary hash before DEK unwrap

#### 3. Supply Chain Attack
**Threat**: Malicious code in Go module.

**Mitigation**:
- `go.sum` pins cryptographic hashes for all modules
- Minimal dependency surface (~15 direct deps vs ~49 for TS)
- Reproducible builds allow hash verification

#### 4. Malicious Insider
**Threat**: Infrastructure access to deploy modified binary.

**Mitigation**:
- SEV-SNP measurement covers the full binary
- Gateway verifies attestation report before connecting

### Out-of-Scope Threats

- Compromised AMD SEV-SNP firmware
- Physical server access
- Timing side-channel attacks
- Denial of Service

### Attack Surface

| Interface | Exposure | Mitigation |
|-----------|----------|------------|
| **gRPC API** | Internal mTLS only | Protobuf schema, mTLS client cert |
| **REST API** | Internal, API key | Admin endpoints only, HTTPS |
| **PostgreSQL** | Local network | pgx parameterized queries, `enclave_user` role |
| **Exchange APIs** | HTTPS outbound | TLS validation, response sanitization |
| **Go modules** | Build time only | `go.sum` hash pinning |

## Audit Process

### Scope

Auditors should focus on:

1. **Credential Handling**: Verify decryption only in-memory, no logging
2. **Trade Privacy**: Confirm individual trades never leave enclave boundary
3. **Cryptographic Correctness**: Review AES-256-GCM in `internal/encryption`
4. **Input Validation**: Check all external inputs (gRPC, exchange APIs)
5. **Output Sanitization**: Verify gRPC responses contain no trade details
6. **Reproducible Builds**: Verify deployed binary matches source

### Audit Checklist

**Credential Security:**
- [ ] Review `internal/encryption/` — key derivation, AES-GCM usage, DEK management
- [ ] Verify no credentials logged in `internal/logredact/` redaction rules
- [ ] Confirm decrypted credentials not written to disk or global variables
- [ ] Check `internal/security/mlock.go` — memory locking for key material

**Snapshot Privacy:**
- [ ] Review `internal/service/sync.go` — snapshot creation, field selection
- [ ] Verify gRPC responses in `internal/grpc/server.go` contain only aggregated data
- [ ] Confirm no individual trades in `snapshot_data` writes
- [ ] Review `internal/scheduler/daily.go` — 00:00 UTC systematic execution
- [ ] Verify sync_statuses rate-limit enforcement in `internal/service/sync.go`

**Input Validation:**
- [ ] Review gRPC message validation in `internal/grpc/server.go`
- [ ] Check exchange API response parsing in `internal/connector/*.go`
- [ ] Verify all SQL queries use pgx parameterized statements (`$1`, `$2`, …)

**Dependencies:**
- [ ] Run `go mod verify` and `govulncheck ./...`
- [ ] Review `go.sum` hash entries for critical modules
- [ ] Check for suspicious indirect dependencies

**Build Verification:**
- [ ] Reproduce build on clean Ubuntu 22.04 VM (see [Reproducible Builds](#reproducible-builds))
- [ ] Verify binary SHA-256 matches published hash
- [ ] Review `Dockerfile` for unexpected layers

### Previous Audits

| Date | Auditor | Version | Status | Report |
|------|---------|---------|--------|--------|
| TBD | TBD | v1.0.0 | Pending | - |

### Responsible Disclosure

Security vulnerabilities should be reported privately to:
- **Email**: security@auditzk.com
- **Response SLA**: 48 hours for acknowledgment, 7 days for initial assessment

Please **do not** open public GitHub issues for security vulnerabilities.

## Reproducible Builds

### Purpose

Reproducible builds allow verification that the production binary matches the audited source, preventing "trusting trust" attacks.

### Build Process

```bash
# Clone repository
git clone https://github.com/AuditZK/zero-knowledge-aggregator-go.git
cd zero-knowledge-aggregator-go

# Checkout specific version
git checkout v1.0.0

# Verify commit hash
git rev-parse HEAD
# Expected: <COMMIT_HASH> (published on release page)

# Build (identical flags to production Dockerfile)
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
  -trimpath \
  -ldflags="-w -s" \
  -o enclave \
  ./cmd/enclave

# Calculate binary hash
sha256sum enclave
# Expected: <BINARY_HASH> (published on release page)
```

### Build Environment

For bit-for-bit reproducibility:
- **Go**: 1.22.x (exact version pinned in `go.mod`)
- **OS**: Ubuntu 22.04 LTS
- **Architecture**: x86_64 (linux/amd64)
- **CGO**: disabled (`CGO_ENABLED=0`)
- **Flags**: `-trimpath -ldflags="-w -s"` (strip debug info, deterministic paths)

### Attestation (Production Only)

In production on AMD SEV-SNP:

1. Enclave generates attestation report containing binary hash (SHA-256), VM measurement, SEV-SNP firmware version
2. API Gateway verifies attestation before connecting via mTLS
3. Attestation report independently verifiable by auditors

## API Specification

### gRPC Service Definition

```protobuf
service EnclaveService {
  rpc ProcessSyncJob(SyncJobRequest) returns (SyncJobResponse);
  rpc GetAggregatedMetrics(AggregatedMetricsRequest) returns (AggregatedMetricsResponse);
  rpc HealthCheck(HealthCheckRequest) returns (HealthCheckResponse);
}
```

Full specification: [api/proto/enclave.proto](api/proto/enclave.proto)

### Security Properties

All gRPC responses contain **only aggregated data**. Individual trade prices, timestamps, or sizes are **never** transmitted outside the enclave.

## Compliance

- **GDPR Article 32**: Technical measures to protect personal data
- **FIPS 140-2 Level 1**: AES-256-GCM via Go `crypto/aes` + `crypto/cipher`
- **SOC 2 Type II**: Audit controls for data processing (in progress)

## References

- [AMD SEV-SNP Whitepaper](https://www.amd.com/content/dam/amd/en/documents/epyc-business-docs/white-papers/SEV-SNP-strengthening-vm-isolation-with-integrity-protection-and-more.pdf)
- [gRPC Security Guide](https://grpc.io/docs/guides/auth/)
- [Reproducible Builds Project](https://reproducible-builds.org/)
- [Go Module Authentication](https://go.dev/ref/mod#authenticating)
- [TypeScript Enclave (predecessor)](https://github.com/AuditZK/zero-knowledge-aggregator)

## License

AuditZK Source-Available License (ASAL) v1.0 — See [LICENSE](LICENSE)

This code is published for transparency and audit purposes. Third-party deployment is not supported.

## Contact

- **Security**: security@auditzk.com
- **Support**: support@auditzk.com
- **GitHub Issues**: https://github.com/AuditZK/zero-knowledge-aggregator-go/issues
