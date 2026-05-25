# Security Documentation

**AuditZK Enclave - Confidential Trading Data Aggregation (Go)**

This document describes the security architecture, mechanisms, and guarantees of the AuditZK Enclave Worker.

---

## Table of Contents

- [Security Overview](#security-overview)
- [Zero-Knowledge Architecture](#zero-knowledge-architecture)
- [Hardware Isolation (AMD SEV-SNP)](#hardware-isolation-amd-sev-snp)
- [Cryptographic Protection](#cryptographic-protection)
- [Secure Logging System](#secure-logging-system)
- [Memory Protection](#memory-protection)
- [Database Security](#database-security)
- [Rate Limiting & Anti-Manipulation](#rate-limiting--anti-manipulation)
- [Audit Trail](#audit-trail)
- [Threat Model](#threat-model)
- [Security Guarantees](#security-guarantees)
- [Compliance](#compliance)

---

## Security Overview

The AuditZK Enclave implements a **zero-knowledge architecture** for processing sensitive trading data. The system is designed with the following core security principles:

1. **Hardware Root of Trust**: AMD SEV-SNP provides memory encryption and attestation
2. **Minimal Trust Boundary**: ~27,000 LOC of hand-written Go in the Trusted Computing Base (TCB)
3. **Data Minimization**: Individual trades never leave the enclave
4. **Defense in Depth**: Multiple layers of security controls
5. **Auditability**: All security mechanisms are auditable and reproducible

---

## Zero-Knowledge Architecture

### Principle

**NO individual trading data ever leaves the enclave boundary.**

The enclave processes sensitive data (API credentials, individual trades, positions) but only outputs **aggregated daily snapshots** containing:
- Total equity
- Realized balance
- Unrealized P&L
- Deposits/withdrawals (cash flow)

### Data Flow Isolation

```
┌─────────────────────────────────────────────────────────┐
│  INSIDE ENCLAVE (AMD SEV-SNP Protected Memory)          │
│  ┌────────────────────────────────────────────┐         │
│  │  Encrypted Credentials (AES-256-GCM)       │         │
│  │  - API Keys, Secrets, Passphrases          │         │
│  │  - Decrypted ONLY in enclave memory        │         │
│  └────────────────────────────────────────────┘         │
│                     │                                    │
│                     ▼                                    │
│  ┌────────────────────────────────────────────┐         │
│  │  Individual Trades (NEVER transmitted)     │         │
│  │  - Trade prices, timestamps, sizes         │         │
│  │  - Position details                        │         │
│  │  - Account balances by market              │         │
│  └────────────────────────────────────────────┘         │
│                     │                                    │
│                     ▼                                    │
│  ┌────────────────────────────────────────────┐         │
│  │  Aggregation Engine                        │         │
│  │  - Daily snapshots at 00:00 UTC            │         │
│  │  - P&L calculation                         │         │
│  │  - Deposit/withdrawal detection            │         │
│  └────────────────────────────────────────────┘         │
│                     │                                    │
└─────────────────────┼────────────────────────────────────┘
                      │
                      ▼ ENCLAVE BOUNDARY (gRPC over mTLS)
                      │
            ┌─────────────────────┐
            │  Aggregated Data    │
            │  ONLY (safe to exit)│
            │  - Total equity     │
            │  - Realized balance │
            │  - Unrealized P&L   │
            │  - Deposits/withdrawals
            └─────────────────────┘
                      │
                      ▼
            API Gateway (Untrusted Zone)
```

### Security Properties

| Data Type | Inside Enclave | Crosses Boundary | Available to API Gateway |
|-----------|----------------|------------------|--------------------------|
| **API Credentials** | ✅ Decrypted | ❌ NEVER | ❌ NEVER |
| **Individual Trades** | ✅ Processed | ❌ NEVER | ❌ NEVER |
| **Trade Prices** | ✅ Used for P&L | ❌ NEVER | ❌ NEVER |
| **Position Sizes** | ✅ Aggregated | ❌ NEVER | ❌ NEVER |
| **Daily Equity Snapshots** | ✅ Created | ✅ YES | ✅ YES (read-only) |
| **Total P&L** | ✅ Calculated | ✅ YES | ✅ YES (read-only) |

---

## Hardware Isolation (AMD SEV-SNP)

### AMD SEV-SNP Protection

The enclave runs inside an **AMD Secure Encrypted Virtualization - Secure Nested Paging (SEV-SNP)** virtual machine, which provides:

#### 1. Memory Encryption
- **AES-128-ECB** encryption of all VM memory
- **Ephemeral keys** generated per VM (inaccessible to hypervisor)
- **DMA protection** prevents direct memory access attacks

#### 2. Attestation
The enclave generates cryptographically signed attestation reports that prove:
- The binary hash matches audited source code
- The VM is running on genuine AMD SEV-SNP hardware
- Memory encryption is active
- The hypervisor cannot access enclave memory

#### 3. Attestation Implementation

Location: [internal/attestation/](internal/attestation/)

**Supported Platforms:**
- **Bare Metal / KVM**: `/dev/sev-guest` device
- **Azure Confidential VMs**: IMDS attestation endpoint
- **GCP Confidential VMs**: Metadata server attestation

**Attestation API** ([internal/attestation/attestation.go](internal/attestation/attestation.go)):

```go
// GetAttestation returns the current attestation report (cached ~5s).
func (s *Service) GetAttestation(ctx context.Context) (*AttestationReport, error)

// GetAttestationWithNonce returns a fresh report whose report_data binds the
// enclave's TLS / E2E / signing public keys — and the caller's nonce — into
// the SEV-SNP quote (anti-replay).
func (s *Service) GetAttestationWithNonce(ctx context.Context, nonce []byte) (*AttestationReport, error)
```

Internally, `fetchWithSnpguest` invokes the `snpguest` tool to produce and
parse the SEV-SNP quote, and `fetchAndCacheVCEK` validates the VCEK / VLEK
certificate chain against the AMD root. The resulting flags — `Verified`,
`VcekVerified`, `ReportDataBoundToRequest` — gate production startup via
`enforceProductionAttestation`.

**Verification Output:**

```json
{
  "verified": true,
  "enclave": true,
  "sevSnpEnabled": true,
  "measurement": "a3f5...b8c2",
  "platformVersion": "3",
  "reportData": null
}
```

#### 4. Threat Mitigation

| Threat | Without SEV-SNP | With SEV-SNP |
|--------|-----------------|--------------|
| **Malicious Hypervisor** | Can read all VM memory | Cannot decrypt memory |
| **Cold Boot Attack** | Memory readable after shutdown | Memory encrypted with ephemeral keys |
| **DMA Attack** | Device can access VM memory | DMA protection blocks access |
| **VM Migration Attack** | Memory exposed during migration | Attestation fails if migrated |

---

## Cryptographic Protection

### Encryption Service

Location: [internal/encryption/](internal/encryption/)

#### Algorithm: AES-256-GCM

**Properties:**
- **Symmetric encryption**: 256-bit keys (FIPS 140-2 compliant)
- **Authenticated encryption**: Galois/Counter Mode (GCM) provides integrity
- **Nonce**: 12 bytes random per encryption
- **Authentication Tag**: 16 bytes for tamper detection

**Why AES-256-GCM?**
- ✅ Industry standard for confidential data
- ✅ Hardware acceleration (AES-NI on modern CPUs)
- ✅ Authenticated encryption prevents tampering
- ✅ NIST approved (SP 800-38D)

#### Key Derivation (AMD SEV-SNP Hardware)

```go
// internal/encryption/key_derivation.go — deriveMasterKey()
// HKDF-SHA256(measurement, salt=platformVersion, info="track-record-enclave-dek")
reader := hkdf.New(sha256.New, measurement, salt, []byte(hkdfInfoMasterKey))
s.masterKey = make([]byte, masterKeySize) // 32 bytes
if _, err := io.ReadFull(reader, s.masterKey); err != nil {
    return fmt.Errorf("derive master key from sev-snp measurement: %w", err)
}
s.isHardware = true
```

The HKDF `info` string (`hkdfInfoMasterKey`) and 32-byte length match the TS
enclave exactly — a divergence would derive a different master key and fail to
unwrap existing DEKs.

**Security:**
- Master key derived from AMD SEV-SNP hardware measurement (NOT environment variables)
- Key changes automatically when enclave code is updated
- Key never stored — derived on-demand from hardware
- NO FALLBACK: AMD SEV-SNP hardware is REQUIRED

#### Trustless Binary Upgrades (B2 Handoff)

The naive consequence of measurement-bound master keys is that every
binary update would render existing encrypted credentials unreadable —
forcing every user to re-submit. The B2 design avoids this without
weakening the trust model:

- The operator publishes a hardcoded **Ed25519 pubkey** in the binary
  source (`internal/bootstrap/signed_allowlist.go::operatorPubkey`).
  This pubkey is the long-term root of trust — auditable on GitHub.

- Each release ships with a **SignedAllowlist** (JSON, signed by the
  operator's matching privkey) listing the SEV-SNP measurements of
  every approved binary version.

- At upgrade time, the new enclave (v_N+1) attests via SEV-SNP and
  asks the running enclave (v_N) for the master key. v_N validates:

  1. SignedAllowlist signature matches `operatorPubkey`
  2. v_N+1's measurement is in the allowlist
  3. v_N+1's SEV-SNP attestation chain is valid (silicon-signed)
  4. v_N+1's `report_data` binds the request to its ephemeral ECIES
     pubkey + a fresh nonce (anti-replay, anti-MITM)

  Then v_N encrypts the master key with v_N+1's ECIES pubkey and
  returns it. The successor decrypts inside its TEE.

- The operator's privkey lives **only on the operator's laptop**,
  encrypted with a passphrase by `cmd/release-sign`. It never reaches
  the enclave or any server. A compromise of GCP metadata, the host
  filesystem, or the database does NOT yield the ability to sign a
  malicious release.

- Implementation:
  - Server: `internal/bootstrap/handoff_server.go` (mounted on
    `POST /api/v1/admin/handoff`)
  - Client: `internal/bootstrap/handoff_client.go` (called from
    `cmd/enclave/main.go` when `HANDOFF_PEER_URL` is set)
  - Tooling: `cmd/release-sign` (operator) and
    `cmd/admin-export-master-key` (one-shot for v0→v1 migration)

- See [doc/RELEASE_PROCEDURE.md](doc/RELEASE_PROCEDURE.md) for the
  steady-state release workflow and
  [doc/MIGRATION_FROM_LEGACY.md](doc/MIGRATION_FROM_LEGACY.md) for the
  one-time bridge from a pre-B2 binary.

**Trust model summary:** users trust (a) AMD SEV-SNP silicon and (b)
the operator's published Ed25519 pubkey + the GitHub source. The
operator does NOT gain the ability to decrypt user credentials — they
gain the ability to sign new binaries that, once attested, can in
turn decrypt them inside the TEE.

#### Encryption Format

```
[Nonce (12 bytes)] + [Auth Tag (16 bytes)] + [Encrypted Data (variable)]
       ↓                       ↓                        ↓
   Random                 Integrity                 Ciphertext
   per message            protection             (API keys, secrets)
```

#### Credential Storage

**Database Schema:**
```sql
CREATE TABLE exchange_connections (
  encrypted_api_key      TEXT NOT NULL,  -- AES-256-GCM encrypted
  encrypted_api_secret   TEXT NOT NULL,  -- AES-256-GCM encrypted
  encrypted_passphrase   TEXT,           -- AES-256-GCM encrypted (optional)
  credentials_hash       TEXT            -- SHA-256 hash for deduplication
);
```

**Decryption Process:**
```go
// Credentials decrypted ONLY in enclave memory
apiKey, err := encryptionSvc.Decrypt(conn.EncryptedAPIKey)
apiSecret, err := encryptionSvc.Decrypt(conn.EncryptedAPISecret)

// Used for exchange API authentication
exchange, err := factory.New(conn.Exchange, &Credentials{
    APIKey:    apiKey,    // In-memory only
    APISecret: apiSecret, // In-memory only
})

// Credentials NEVER logged (see Secure Logging)
// Credentials NEVER transmitted outside enclave
```

**Credentials Hash (Deduplication):**
```go
// SHA-256 hash to detect duplicate credentials without storing plaintext
h := sha256.New()
h.Write([]byte(apiKey + ":" + apiSecret + ":" + passphrase))
hash := hex.EncodeToString(h.Sum(nil))
```

---

## Secure Logging System

### Design Philosophy

The logging system implements **deterministic multi-tier redaction** to ensure NO sensitive data ever leaves the enclave, even in logs.

Location: [internal/logredact/redact.go](internal/logredact/redact.go)

### Two-Tier Redaction (ALWAYS Active)

#### TIER 1: Credentials & Secrets
**Always redacted** in all environments (production, development, testing)

Patterns matched (regex):
```
- API keys: api_key, apiKey, api-key
- Secrets: api_secret, apiSecret, secret_key
- Passwords: password, passwd, pwd
- Tokens: token, access_token, jwt, bearer_token
- Encryption: encryption_key, private_key
- Authentication: auth, authorization, credentials, passphrase
- Encrypted fields: any field containing "encrypted"
```

#### TIER 2: Business Data & PII
**Always redacted** to prevent leaking user identity and trading activity

Patterns matched:
```
- User identification: user_uid, user_id, account_id, customer_id
- Exchange identification: exchange, exchange_name, broker, platform
- Financial amounts: balance, equity, amount, value, price, total, pnl, profit, loss
- Trading activity: trade, position, order, quantity, size, volume, synced, count
- Personal information: name, email, phone, address, ssn, tax_id
```

### Redaction Examples

**Input (sensitive data):**
```go
logger.Info("Sync completed",
    zap.String("user_uid", "550e8400-e29b-41d4-a716-446655440000"),
    zap.String("exchange", "binance"),
    zap.Float64("total_equity", 10500.00),
    zap.String("api_key", "sk_live_abc123..."),
    zap.Bool("synced", true),
    zap.Int("count", 42),
)
```

**Output (redacted):**
```json
{
  "timestamp": "2025-01-15T12:00:00.000Z",
  "level": "INFO",
  "message": "Sync completed",
  "user_uid": "[REDACTED]",
  "exchange": "[REDACTED]",
  "total_equity": "[REDACTED]",
  "api_key": "[REDACTED]",
  "synced": "[REDACTED]",
  "count": "[REDACTED]"
}
```

**Safe logs (not redacted):**
```go
✅ logger.Info("Sync job started")
✅ logger.Info("Database connection established")
✅ logger.Error("Validation failed", zap.Error(err))
✅ logger.Info("Enclave initialized successfully")
```

### Verification

**Auditors can verify:**
1. All log emissions use the `zap.Logger` wrapper (not `fmt.Print*` or `log.*`)
2. TIER 1 and TIER 2 redaction are ALWAYS active (no conditional logic)
3. `fmt.Println` / `log.Print*` are NOT used in enclave code (auditable via grep)
4. Log fields pass through `filterSensitiveFields()` before emission

---

## Memory Protection

Location: [internal/security/](internal/security/)

### Protection Mechanisms

`MemoryProtection.Apply()` runs three hardening steps at boot; `WipeBuffer`
overwrites a buffer on demand:

```go
// internal/security/memory_linux.go
func (m *MemoryProtection) Apply() {
    m.DisableCoreDumps()      // RLIMIT_CORE = 0 — no credential leak to disk
    m.CheckPtraceProtection() // warns if yama ptrace_scope < 2
    m.CheckMlock()            // reports the process VmLck status
}

// WipeBuffer overwrites a buffer with random data, then zeros.
func (m *MemoryProtection) WipeBuffer(buf []byte) {
    rand.Read(buf)
    for i := range buf {
        buf[i] = 0
    }
}
```

| Step | Threat | What the code does |
|------|--------|--------------------|
| `DisableCoreDumps` | Core dumps leak decrypted credentials to disk | `syscall.Setrlimit(RLIMIT_CORE, 0)` |
| `CheckPtraceProtection` | Debuggers attach and read process memory | Reads `yama/ptrace_scope`; warns if `< 2` |
| `CheckMlock` | Sensitive pages swapped to disk | Reports `VmLck` from `/proc/self/status` |
| `WipeBuffer` | Decrypted secrets linger in memory | Random-then-zero overwrite |

`CheckPtraceProtection` and `CheckMlock` are **observability checks** — the host
environment must actually set `ptrace_scope` and grant `CAP_IPC_LOCK` (see
Production Recommendations below). The SEV-SNP VM is the primary memory
protection; these steps are defence-in-depth on top of it.

### Production Recommendations

```bash
# 1. Disable core dumps (systemd)
[Service]
LimitCORE=0

# 2. Enable ptrace protection
sudo sysctl kernel.yama.ptrace_scope=2

# 3. Enable ASLR (Address Space Layout Randomization)
sudo sysctl kernel.randomize_va_space=2

# 4. Enable seccomp (system call filtering)
[Service]
SystemCallFilter=@system-service
SystemCallErrorNumber=EPERM

# 5. Run in AMD SEV-SNP VM
# Memory encryption at hardware level
```

---

## Database Security

### Architecture

The enclave uses **PostgreSQL** with strict privilege separation and parameterized queries via `pgx/v5`.

Location: [internal/repository/](internal/repository/)

### Privilege Separation

```sql
-- Enclave user (FULL access to sensitive tables)
CREATE USER enclave_user WITH PASSWORD 'strong_password';
GRANT SELECT, INSERT, UPDATE, DELETE ON exchange_connections TO enclave_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON snapshot_data TO enclave_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON sync_statuses TO enclave_user;

-- Gateway user (READ-ONLY access to aggregated data)
CREATE USER gateway_user WITH PASSWORD 'different_password';
GRANT SELECT ON snapshot_data TO gateway_user;
-- NO access to exchange_connections table
-- NO access to sync_statuses table
```

### Sensitive Tables

#### 1. exchange_connections (Credentials)

**Access:** Enclave only (enclave_user)

```sql
CREATE TABLE exchange_connections (
  id                    TEXT PRIMARY KEY,
  "userUid"             TEXT NOT NULL,
  exchange              TEXT NOT NULL,
  "encryptedApiKey"     TEXT NOT NULL,  -- AES-256-GCM
  "encryptedApiSecret"  TEXT NOT NULL,  -- AES-256-GCM
  "encryptedPassphrase" TEXT,           -- AES-256-GCM (optional)
  "credentialsHash"     TEXT,           -- SHA-256 (for deduplication)
  "isActive"            BOOLEAN DEFAULT TRUE,
  "createdAt"           TIMESTAMP DEFAULT NOW(),
  "updatedAt"           TIMESTAMP DEFAULT NOW(),
  UNIQUE("userUid", exchange)
);
```

**Security:**
- All credentials **AES-256-GCM encrypted** at rest
- Gateway has **NO access** (cannot read credentials)
- `credentialsHash` allows duplicate detection without decryption

#### 2. snapshot_data (Aggregated Output)

**Access:** Enclave (read/write) + Gateway (read-only)

```sql
CREATE TABLE snapshot_data (
  id                   TEXT PRIMARY KEY,
  "userUid"            TEXT NOT NULL,
  timestamp            TIMESTAMP NOT NULL,   -- Daily 00:00 UTC
  exchange             TEXT NOT NULL,
  total_equity         REAL NOT NULL,        -- Total account value
  realized_balance     REAL NOT NULL,        -- Available cash
  unrealized_pnl       REAL NOT NULL,        -- Open positions P&L
  deposits             REAL DEFAULT 0,       -- Cash in
  withdrawals          REAL DEFAULT 0,       -- Cash out
  breakdown_by_market  JSONB,               -- Market breakdown (spot/swap/options)
  "createdAt"          TIMESTAMP DEFAULT NOW(),
  UNIQUE("userUid", timestamp, exchange)
);
```

**Security:**
- Gateway can read (but NOT modify) snapshots
- NO individual trade data in this table
- Only daily aggregated equity

#### 3. sync_statuses (Audit Trail & Rate Limiting)

**Access:** Enclave only (enclave_user)

```sql
CREATE TABLE sync_statuses (
  id              TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
  "userUid"       TEXT NOT NULL,
  exchange        TEXT NOT NULL,
  label           TEXT NOT NULL,
  "lastSyncTime"  TIMESTAMP NOT NULL,
  status          TEXT NOT NULL,
  "totalTrades"   INTEGER DEFAULT 0,
  "errorMessage"  TEXT,
  "createdAt"     TIMESTAMP DEFAULT NOW(),
  "updatedAt"     TIMESTAMP DEFAULT NOW(),
  UNIQUE("userUid", exchange, label)
);
```

**Security:**
- Proves snapshots are systematic (not cherry-picked)
- One snapshot per day enforced (see Rate Limiting)
- Gateway has NO access (prevents manipulation)

### SQL Injection Prevention

**All queries use `pgx/v5` with parameterized statements.**

```go
// ❌ DANGEROUS: String concatenation
query := fmt.Sprintf("SELECT * FROM exchange_connections WHERE \"userUid\" = '%s'", userUID)

// ✅ SAFE: Parameterized query (pgx default)
rows, err := pool.Query(ctx,
    `SELECT * FROM exchange_connections WHERE "userUid" = $1`,
    userUID,  // Automatically parameterized
)
```

**Verification:**
- Query *values* are always passed as `$1`, `$2`, … parameters — never interpolated
- Some queries assemble static column lists with `fmt.Sprintf` / `strings.Join`; these carry no caller input
- pgx handles quoting and escaping internally

---

## Rate Limiting & Anti-Manipulation

### Purpose

**Prevent cherry-picking** by enforcing systematic daily snapshots.

Location: [internal/service/sync.go](internal/service/sync.go)

### Threat Model

**Threat:** User manipulates snapshot timing to hide losses

**Example Attack:**
```
Day 1: Portfolio up +10% → User triggers snapshot (shows profit)
Day 2: Portfolio down -15% → User SKIPS snapshot (hides loss)
Day 3: Portfolio up +5% → User triggers snapshot (shows profit)
```

**Result:** Performance appears better than reality (deceptive track record)

### Mitigation: One Snapshot Per Day

A manual sync cannot create a second snapshot for a day that already has one.
`SyncService.isManualSyncAllowed` runs a targeted existence check before any
manual sync, and fails *closed* on a database error:

```go
// internal/service/sync.go
func (s *SyncService) isManualSyncAllowed(ctx context.Context, userUID, exchange, label string) (bool, error) {
    exists, err := s.snapshotRepo.ExistsForUserExchangeLabel(ctx, userUID, exchange, label)
    if err != nil {
        // Fail closed: surface the DB error instead of silently letting a
        // manual sync overwrite an existing committed snapshot.
        return false, fmt.Errorf("anti-cherry-pick check failed: %w", err)
    }
    return !exists, nil
}
```

Daily snapshots are otherwise produced systematically by the in-enclave
scheduler, not on demand — so the series cannot be cherry-picked by choosing
when to sync.

### Audit Trail

**Every sync is recorded:**
```go
func (s *SyncService) recordSync(ctx context.Context, userUID, exchange string) error {
    return s.syncStatusRepo.Upsert(ctx, &SyncStatus{
        UserUID:      userUID,
        Exchange:     exchange,
        LastSyncTime: time.Now().UTC(),
        Status:       "completed",
    })
}
```

**Database audit log:**
```
userUid                              | exchange | lastSyncTime         | status
550e8400-e29b-41d4-a716-446655440000 | binance  | 2025-01-14 00:00:00 | completed
550e8400-e29b-41d4-a716-446655440000 | binance  | 2025-01-15 00:00:00 | completed
550e8400-e29b-41d4-a716-446655440000 | binance  | 2025-01-16 00:00:00 | completed
```

**Proof of systematic snapshots:**
- Auditors can verify timestamps are ~24 hours apart
- No gaps in the snapshot sequence
- Manual syncs cannot create a second snapshot for a day that already has one

### Autonomous Scheduler

Location: [internal/scheduler/daily.go](internal/scheduler/daily.go)

`SyncScheduler` fires at the next 00:00 UTC, then every 24 hours. The timezone
is forced to UTC so the snapshot cadence cannot be shifted:

```go
// internal/scheduler/daily.go
func timeUntilMidnightUTC() time.Duration {
    now := time.Now().UTC()
    nextMidnight := time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 0, 0, time.UTC)
    return nextMidnight.Sub(now)
}
```

On each tick, `executeDailySync` syncs every user via
`SyncService.SyncUserScheduledDueAtomic` (bounded to 3 concurrent users).

**Properties:**
- Runs inside the hardware-attested enclave (cannot be manipulated)
- Systematic timing (00:00 UTC daily), not user-triggered
- The anti-cherry-pick guard prevents manual abuse
- `sync_statuses` audit trail proves systematic execution

---

## Audit Trail

### What is Auditable?

1. **Source Code** (this repository)
   - All security mechanisms are open source
   - Reproducible builds verify deployed binary matches audited code

2. **Sync Status Logs** (database)
   - Timestamp of every snapshot creation
   - Proves systematic execution (not cherry-picked)

3. **Attestation Reports** (AMD SEV-SNP)
   - Binary hash (SHA-384)
   - Platform version
   - Cryptographic signature (ECDSA)

4. **Application Logs** (filtered)
   - Enclave initialization
   - Sync job execution
   - Errors and warnings
   - NO sensitive data (TIER 1 + TIER 2 redacted)

### Audit Tools

**For Independent Auditors:**

```bash
# 1. Verify source code matches deployed binary
git clone https://github.com/AuditZK/zero-knowledge-aggregator-go.git
cd zero-knowledge-aggregator-go
git checkout v1.0.0
go build -trimpath -ldflags="-w -s" -o enclave ./cmd/enclave
sha256sum enclave  # Compare with published hash

# 2. Check attestation report
curl -X POST https://enclave.auditzk.com/api/v1/attestation
# Verify measurement matches build hash

# 3. Review sync status logs (database query)
SELECT "userUid", exchange, "lastSyncTime", status
FROM sync_statuses
ORDER BY "lastSyncTime" DESC;
# Verify ~24-hour intervals

# 4. Analyze logs for sensitive data leaks
grep -i "api.key\|password\|secret" /var/log/enclave/*.log
# Should return ZERO results (all redacted)

# 5. Verify SQL values are parameterized
#    Repository queries pass values as $1, $2, … — never interpolated.
#    Static column lists may be assembled with fmt.Sprintf/strings.Join;
#    those carry no caller input.
```

---

## Threat Model

### In-Scope Threats

#### 1. Compromised API Gateway

**Threat:** Attacker gains full control of the API Gateway / report-service
component sitting between the user-facing frontend and the enclave.

**Impact without enclave:**
- Access to encrypted credentials (could attempt offline brute-force)
- Access to all snapshot data
- Ability to forge requests for arbitrary `user_uid` values

**Mitigations:**

1. **Confidentiality of credentials at rest** —
   Gateway has NO access to `exchange_connections` table (PostgreSQL
   privileges below). The DEK is unwrapped only inside the enclave, and
   the master key is derived from the SEV-SNP launch measurement.

2. **Confidentiality of individual trades** —
   Only aggregated snapshots cross the enclave boundary; per-trade data
   never leaves the trusted zone.

3. **Per-user authorization (AUTH-001 / AUTH-002 — audit hardening)** —
   The enclave does **NOT** trust the gateway for `user_uid` authorization.
   Every RPC and REST handler that accepts a `user_uid` argument runs
   through `resolveUserUID(ctx, bodyUID)`, which prefers the JWT-verified
   `claims.Sub` over whatever the caller wrote in the request. A
   compromised gateway holding a valid HS256 token for user A cannot
   exfiltrate / mutate data for user B by setting `user_uid=B` in the
   payload — the JWT subject wins.

   - gRPC: `methodsRequireJWT` covers `GenerateSignedReport`,
     `CreateUserConnection`, `ProcessSyncJob`, `GetPerformanceMetrics`,
     `GetSnapshotTimeSeries`, `GetAggregatedMetrics`. See
     `internal/grpc/server.go`.
   - REST: same enforcement on `/api/v1/credentials/connect` and the
     legacy `/api/v1/{connection,sync,metrics,snapshots,report}` set.
     See `internal/server/handler.go`.

4. **Authentication boundary hardening** —
   - mTLS is mandatory in production (`buildGRPCTLSConfig` in
     `cmd/enclave/main.go`).
   - `GRPC_CLIENT_CERT_CN_ALLOWLIST` must be populated; the enclave
     refuses to start with an empty allowlist in production.
   - `ENCLAVE_JWT_EXPECTED_ISSUER`, when set, pins the JWT `iss` claim
     so a token signed with the enclave secret but minted by a different
     service is rejected.

5. **Rate-limiter audit log** —
   `sync_statuses` writes prove daily snapshots run on the cron
   schedule rather than being cherry-picked.

**Verification:**
```sql
-- Verify gateway_user cannot read credentials
SELECT * FROM information_schema.table_privileges
WHERE grantee = 'gateway_user' AND table_name = 'exchange_connections';
-- Should return ZERO rows
```

```bash
# Per-user authorization regression tests (AUTH-001 / AUTH-002)
go test ./internal/server/ ./internal/grpc/ -run Audit -v
```

#### 2. Compromised Hypervisor

**Threat:** Malicious cloud provider or attacker compromises VM hypervisor

**Impact without SEV-SNP:**
- Hypervisor can read all VM memory
- Can extract decrypted credentials

**Mitigation:**
- AMD SEV-SNP encrypts VM memory with hardware keys
- Hypervisor cannot decrypt memory (keys inaccessible)
- Attestation verifies memory encryption is active

**Verification:**
```bash
# Check SEV-SNP status
dmesg | grep -i sev
# Should show: AMD Secure Encrypted Virtualization (SEV) active

# Verify attestation
curl http://localhost:50051/health
# Should return: sevSnpEnabled: true
```

#### 3. Supply Chain Attack

**Threat:** Malicious code injected via compromised Go module

**Impact:**
- Attacker could exfiltrate credentials
- Backdoor in dependencies

**Mitigation:**
- `go.sum` pins exact module versions and SHA-256 hashes
- Reproducible builds allow verification of deployed binary
- Regular `govulncheck` scans for known vulnerabilities
- Minimal dependencies (9 direct modules, all auditable)

**Verification:**
```bash
# Check for known vulnerabilities in dependencies
govulncheck ./...
# Should return: No vulnerabilities found

# Verify module integrity hashes
go mod verify
# Should return: all modules verified

# Reproducible build
go build -trimpath -ldflags="-w -s" ./cmd/enclave
```

#### 4. Malicious Insider

**Threat:** Infrastructure admin attempts to extract sensitive data

**Impact without attestation:**
- Admin could deploy modified enclave code
- Could add logging to exfiltrate credentials

**Mitigation:**
- SEV-SNP attestation verifies binary hash before Gateway connects
- Debug interfaces disabled in production build (`-w -s` strips debug info)
- All enclave access logged (audit trail)
- Reproducible builds prove deployed binary matches audited source

**Verification:**
```go
// Gateway verifies attestation before connecting
report, err := attestationSvc.GetAttestationReport(ctx, req)
if !report.Verified {
    return nil, status.Error(codes.FailedPrecondition,
        "enclave attestation failed - refusing to connect")
}
```

### Out-of-Scope Threats

- **Compromised AMD SEV-SNP firmware**: Requires hardware root of trust
- **Physical access to server**: Physical security is operational concern
- **Side-channel attacks**: Timing/power analysis out of scope
- **Denial of Service**: Availability separate from confidentiality

---

## Security Guarantees

### What the Enclave Guarantees

✅ **Credential Confidentiality**
- API keys decrypted ONLY in enclave memory (AMD SEV-SNP protected)
- NEVER logged (TIER 1 redaction)
- NEVER transmitted outside enclave
- Wiped from memory after use

✅ **Trade Privacy**
- Individual trades NEVER leave enclave boundary
- Only aggregated daily snapshots transmitted
- Gateway cannot access credentials (PostgreSQL privileges)

✅ **Code Integrity**
- Reproducible builds verify deployed binary matches audited source
- SEV-SNP attestation proves binary hash
- Auditors can independently verify build

✅ **Systematic Snapshots**
- Daily scheduler runs inside the attested enclave
- Anti-cherry-pick guard: one snapshot per day per user/exchange/label
- Audit trail proves systematic execution (not cherry-picked)

✅ **Hypervisor Isolation**
- AMD SEV-SNP memory encryption prevents hypervisor access
- Attestation verifies hardware protection is active

### What the Enclave Does NOT Guarantee

❌ **Timing Side-Channels**
- Cache timing attacks out of scope
- Constant-time crypto not implemented for performance

❌ **Physical Security**
- Physical access to server is operational concern
- Cold boot attacks mitigated by SEV-SNP (ephemeral keys)

❌ **Network Security**
- TLS/mTLS for gRPC is Gateway's responsibility
- Enclave trusts network layer

❌ **Availability**
- DoS attacks are separate from confidentiality
- Rate limiting prevents abuse but not targeted DoS

---

## Compliance

### Standards & Frameworks

#### GDPR (Article 32)
**Security of Processing**

✅ Pseudonymization: User UIDs (no email/name in enclave database)
✅ Encryption: AES-256-GCM for credentials at rest
✅ Confidentiality: AMD SEV-SNP hardware memory encryption
✅ Integrity: Authenticated encryption (GCM), attestation
✅ Availability: Database backups, rate limiting
✅ Resilience: Error handling, structured logging, monitoring

#### FIPS 140-2 Level 1
**Cryptographic Module**

✅ AES-256-GCM (NIST SP 800-38D approved) — `crypto/aes` + `crypto/cipher`
✅ SHA-256/SHA-384 (NIST FIPS 180-4 approved) — `crypto/sha256`, `crypto/sha512`
✅ Go standard library crypto (BoringCrypto mode available for FIPS compliance)

**Production Recommendation:**
```bash
# Use Go with BoringCrypto for FIPS 140-2 compliance
GOEXPERIMENT=boringcrypto go build -trimpath ./cmd/enclave
```

#### SOC 2 Type II
**Security Controls** (in progress)

✅ Access Controls: Database privilege separation
✅ Audit Logging: Sync status logs, application logs
✅ Encryption: At-rest (AES-256-GCM), in-memory (SEV-SNP)
✅ Change Management: Git version control, reproducible builds
✅ Monitoring: Structured zap logging, Prometheus metrics

### Certifications

| Certification | Status | Notes |
|---------------|--------|-------|
| **AMD SEV-SNP** | ✅ Production | Hardware attestation available |
| **FIPS 140-2** | ✅ Crypto compliant | BoringCrypto mode recommended |
| **SOC 2 Type II** | 🔄 In progress | Audit controls implemented |
| **ISO 27001** | ⏳ Planned | Information security management |

---

## Security Contact

### Responsible Disclosure

If you discover a security vulnerability, please report it privately:

**Email:** security@auditzk.com

**Response SLA:**
- Acknowledgment: 48 hours
- Initial assessment: 7 days
- Patch timeline: Based on severity (critical: 7 days, high: 30 days)

**Please do NOT:**
- Open public GitHub issues for security vulnerabilities
- Exploit vulnerabilities on production systems
- Publicly disclose before patch is available

### Security Researchers

We welcome responsible security research and will:
- Credit researchers in security advisories (unless anonymity requested)
- Provide detailed technical responses
- Consider bug bounty for critical findings (contact us for details)

---

## References

### AMD SEV-SNP
- [SEV-SNP Whitepaper](https://www.amd.com/content/dam/amd/en/documents/epyc-business-docs/white-papers/SEV-SNP-strengthening-vm-isolation-with-integrity-protection-and-more.pdf)
- [SEV API Specification](https://www.amd.com/system/files/TechDocs/55766_SEV-KM_API_Specification.pdf)

### Cryptography
- [NIST SP 800-38D](https://nvlpubs.nist.gov/nistpubs/Legacy/SP/nistspecialpublication800-38d.pdf) (AES-GCM)
- [FIPS 180-4](https://nvlpubs.nist.gov/nistpubs/FIPS/NIST.FIPS.180-4.pdf) (SHA-2)
- [FIPS 140-2](https://csrc.nist.gov/publications/detail/fips/140/2/final) (Cryptographic Modules)
- [Go BoringCrypto](https://github.com/golang/go/blob/master/src/crypto/internal/boring/README.md)

### Security Best Practices
- [OWASP Top 10](https://owasp.org/www-project-top-ten/)
- [CWE/SANS Top 25](https://cwe.mitre.org/top25/archive/2023/2023_top25_list.html)
- [NIST Cybersecurity Framework](https://www.nist.gov/cyberframework)

### Reproducible Builds
- [Reproducible Builds Project](https://reproducible-builds.org/)
- [Go Reproducible Builds](https://go.dev/blog/rebuild)

---

**Document Version:** 1.0.0
**Last Updated:** 2026-05-17
**Maintained by:** AuditZK Security Team
