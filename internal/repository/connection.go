package repository

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrNotFound is returned when a repository lookup finds no matching row.
var ErrNotFound = errors.New("not found")

// ErrAlreadyExists is returned when an insert collides with an existing
// uniqueness constraint (e.g. one active connection per user/exchange).
var ErrAlreadyExists = errors.New("already exists")

// ExchangeConnection represents an encrypted exchange connection
type ExchangeConnection struct {
	ID                  string    `json:"id"`
	UserUID             string    `json:"user_uid"`
	Exchange            string    `json:"exchange"`
	Label               string    `json:"label"`
	CredentialsHash     string    `json:"credentials_hash,omitempty"`      // SHA-256 hash(apiKey:apiSecret:passphrase)
	SyncIntervalMinutes int       `json:"sync_interval_minutes,omitempty"` // Per-connection sync interval
	ExcludeFromReport   bool      `json:"exclude_from_report,omitempty"`   // true = analytics only, excluded from certified report
	KYCLevel            string    `json:"kyc_level,omitempty"`             // Optional exchange KYC level metadata
	IsPaper             bool      `json:"is_paper,omitempty"`              // true = paper/testnet account
	EncryptedAPIKey     string    `json:"encrypted_api_key"`
	APIKeyIV            string    `json:"api_key_iv"`
	APIKeyAuthTag       string    `json:"api_key_auth_tag"`
	EncryptedAPISecret  string    `json:"encrypted_api_secret"`
	APISecretIV         string    `json:"api_secret_iv"`
	APISecretAuthTag    string    `json:"api_secret_auth_tag"`
	EncryptedPassphrase string    `json:"encrypted_passphrase,omitempty"`
	PassphraseIV        string    `json:"passphrase_iv,omitempty"`
	PassphraseAuthTag   string    `json:"passphrase_auth_tag,omitempty"`
	IsActive            bool      `json:"is_active"`
	CreatedAt           time.Time `json:"created_at"`
	UpdatedAt           time.Time `json:"updated_at"`
}

// ConnectionRepo handles exchange connection persistence.
// Supports both TS (Prisma camelCase) and Go (snake_case) column naming.
type ConnectionRepo struct {
	pool *pgxpool.Pool

	capMu                   sync.Mutex
	capabilitiesLoaded      bool
	hasCredentialsHashCol   bool
	hasSyncIntervalMinsCol  bool
	hasExcludeFromReportCol bool
	hasKYCLevelCol          bool
	hasIsPaperCol           bool
	isTSSchema              bool // true = TS Prisma camelCase columns
}

// NewConnectionRepo creates a new connection repository
func NewConnectionRepo(pool *pgxpool.Pool) *ConnectionRepo {
	return &ConnectionRepo{pool: pool}
}

// Create inserts a new exchange connection
func (r *ConnectionRepo) Create(ctx context.Context, conn *ExchangeConnection) error {
	now := time.Now().UTC()
	conn.CreatedAt = now
	conn.UpdatedAt = now
	conn.IsActive = true
	if conn.SyncIntervalMinutes <= 0 {
		conn.SyncIntervalMinutes = 1440
	}

	hasCredHash, hasSyncMins, hasExclude, hasKYCLevel, hasIsPaper := r.getCapabilityFlags(ctx)

	if r.isTSSchema {
		return r.createTS(ctx, conn, hasCredHash, hasSyncMins, hasExclude, hasKYCLevel, hasIsPaper)
	}

	columns := []string{
		"user_uid", "exchange", "label",
	}
	args := []any{
		conn.UserUID, conn.Exchange, conn.Label,
	}

	if hasCredHash {
		columns = append(columns, "credentials_hash")
		args = append(args, conn.CredentialsHash)
	}
	if hasSyncMins {
		columns = append(columns, "sync_interval_minutes")
		args = append(args, conn.SyncIntervalMinutes)
	}
	if hasExclude {
		columns = append(columns, "exclude_from_report")
		args = append(args, conn.ExcludeFromReport)
	}
	if hasKYCLevel {
		columns = append(columns, "kyc_level")
		args = append(args, conn.KYCLevel)
	}
	if hasIsPaper {
		columns = append(columns, "is_paper")
		args = append(args, conn.IsPaper)
	}

	columns = append(columns,
		"encrypted_api_key", "api_key_iv", "api_key_auth_tag",
		"encrypted_api_secret", "api_secret_iv", "api_secret_auth_tag",
		"encrypted_passphrase", "passphrase_iv", "passphrase_auth_tag",
		"is_active", "created_at", "updated_at",
	)
	args = append(args,
		conn.EncryptedAPIKey, conn.APIKeyIV, conn.APIKeyAuthTag,
		conn.EncryptedAPISecret, conn.APISecretIV, conn.APISecretAuthTag,
		conn.EncryptedPassphrase, conn.PassphraseIV, conn.PassphraseAuthTag,
		conn.IsActive, conn.CreatedAt, conn.UpdatedAt,
	)

	placeholders := make([]string, len(args))
	for i := range args {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}

	query := fmt.Sprintf(
		"INSERT INTO exchange_connections (%s) VALUES (%s) RETURNING id",
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
	)

	err := r.pool.QueryRow(ctx, query, args...).Scan(&conn.ID)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return ErrAlreadyExists
		}
		return err
	}
	return nil
}

// createTS inserts into the TS/Prisma-shaped exchange_connections: camelCase
// column names, single encrypted* column per secret (hex-packed iv+tag+ciphertext),
// and a client-generated id because Prisma's @default(cuid()) isn't a DB default.
// The service layer hands us Go-format fields (ciphertext/iv/authTag in base64) —
// we repack them into the TS hex layout so existing TS readers can decrypt.
func (r *ConnectionRepo) createTS(
	ctx context.Context,
	conn *ExchangeConnection,
	hasCredHash, hasSyncMins, hasExclude, hasKYCLevel, hasIsPaper bool,
) error {
	encAPIKey, err := repackToTSFormat(conn.EncryptedAPIKey, conn.APIKeyIV, conn.APIKeyAuthTag)
	if err != nil {
		return fmt.Errorf("repack api_key for TS: %w", err)
	}
	encAPISecret, err := repackToTSFormat(conn.EncryptedAPISecret, conn.APISecretIV, conn.APISecretAuthTag)
	if err != nil {
		return fmt.Errorf("repack api_secret for TS: %w", err)
	}
	var encPassphrase *string
	if conn.EncryptedPassphrase != "" {
		p, err := repackToTSFormat(conn.EncryptedPassphrase, conn.PassphraseIV, conn.PassphraseAuthTag)
		if err != nil {
			return fmt.Errorf("repack passphrase for TS: %w", err)
		}
		encPassphrase = &p
	}

	columns := []string{`id`, `"userUid"`, `exchange`, `label`}
	args := []any{"cuid_" + randomHex(20), conn.UserUID, conn.Exchange, conn.Label}

	if hasCredHash {
		columns = append(columns, `"credentialsHash"`)
		args = append(args, conn.CredentialsHash)
	}
	if hasSyncMins {
		columns = append(columns, `"syncIntervalMinutes"`)
		args = append(args, conn.SyncIntervalMinutes)
	}
	if hasExclude {
		columns = append(columns, `"excludeFromReport"`)
		args = append(args, conn.ExcludeFromReport)
	}
	if hasKYCLevel {
		columns = append(columns, `"kycLevel"`)
		args = append(args, conn.KYCLevel)
	}
	if hasIsPaper {
		columns = append(columns, `"isPaper"`)
		args = append(args, conn.IsPaper)
	}

	columns = append(columns,
		`"encryptedApiKey"`, `"encryptedApiSecret"`, `"encryptedPassphrase"`,
		`"isActive"`, `"createdAt"`, `"updatedAt"`,
	)
	args = append(args,
		encAPIKey, encAPISecret, encPassphrase,
		conn.IsActive, conn.CreatedAt, conn.UpdatedAt,
	)

	placeholders := make([]string, len(args))
	for i := range args {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}
	query := fmt.Sprintf(
		"INSERT INTO exchange_connections (%s) VALUES (%s) RETURNING id",
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
	)

	err = r.pool.QueryRow(ctx, query, args...).Scan(&conn.ID)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return ErrAlreadyExists
		}
		return err
	}
	return nil
}

// repackToTSFormat converts Go-format encrypted data (base64 ciphertext + base64
// iv + base64 auth tag) into the TS-enclave's single hex string layout
// (iv|tag|ciphertext, all hex). Required when the Go enclave writes to the
// TS/Prisma exchange_connections table, which has one column per secret.
func repackToTSFormat(ciphertextB64, ivB64, authTagB64 string) (string, error) {
	ivBytes, err := base64.StdEncoding.DecodeString(ivB64)
	if err != nil {
		return "", fmt.Errorf("decode iv: %w", err)
	}
	tagBytes, err := base64.StdEncoding.DecodeString(authTagB64)
	if err != nil {
		return "", fmt.Errorf("decode auth tag: %w", err)
	}
	ctBytes, err := base64.StdEncoding.DecodeString(ciphertextB64)
	if err != nil {
		return "", fmt.Errorf("decode ciphertext: %w", err)
	}
	return hex.EncodeToString(ivBytes) + hex.EncodeToString(tagBytes) + hex.EncodeToString(ctBytes), nil
}

// randomHex returns 2n hex characters of random data, used as the random
// suffix on client-generated cuid-shaped ids.
func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// ExistsActiveByCredentialsHash checks whether an active connection already exists
// for this user with the same credentials hash.
func (r *ConnectionRepo) ExistsActiveByCredentialsHash(ctx context.Context, userUID, credentialsHash string) (bool, error) {
	if userUID == "" || credentialsHash == "" {
		return false, nil
	}

	hasCredHash, _, _, _, _ := r.getCapabilityFlags(ctx)
	if !hasCredHash {
		return false, nil
	}

	query := `
		SELECT EXISTS (
			SELECT 1 FROM exchange_connections
			WHERE user_uid = $1 AND credentials_hash = $2 AND is_active = true
		)`
	var exists bool
	if err := r.pool.QueryRow(ctx, query, userUID, credentialsHash).Scan(&exists); err != nil {
		// Graceful fallback if migration has not been applied yet.
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "42703" {
			return false, nil
		}
		return false, err
	}
	return exists, nil
}

// GetByUserAndExchange retrieves a connection by user and exchange
func (r *ConnectionRepo) GetByUserAndExchange(ctx context.Context, userUID, exchange string) (*ExchangeConnection, error) {
	r.getCapabilityFlags(ctx) // Ensure schema detection

	if r.isTSSchema {
		return r.getByUserAndExchangeTS(ctx, userUID, exchange)
	}

	query := `
		SELECT id, user_uid, exchange, label,
			encrypted_api_key, api_key_iv, api_key_auth_tag,
			encrypted_api_secret, api_secret_iv, api_secret_auth_tag,
			encrypted_passphrase, passphrase_iv, passphrase_auth_tag,
			is_active, created_at, updated_at
		FROM exchange_connections
		WHERE user_uid = $1 AND exchange = $2 AND is_active = true`

	var conn ExchangeConnection
	err := r.pool.QueryRow(ctx, query, userUID, exchange).Scan(
		&conn.ID, &conn.UserUID, &conn.Exchange, &conn.Label,
		&conn.EncryptedAPIKey, &conn.APIKeyIV, &conn.APIKeyAuthTag,
		&conn.EncryptedAPISecret, &conn.APISecretIV, &conn.APISecretAuthTag,
		&conn.EncryptedPassphrase, &conn.PassphraseIV, &conn.PassphraseAuthTag,
		&conn.IsActive, &conn.CreatedAt, &conn.UpdatedAt,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	return &conn, nil
}

// getByUserAndExchangeTS reads from TS Prisma camelCase schema.
// TS has no separate IV/AuthTag columns — encrypted data is a single hex field.
func (r *ConnectionRepo) getByUserAndExchangeTS(ctx context.Context, userUID, exchange string) (*ExchangeConnection, error) {
	query := `
		SELECT id, "userUid", exchange, label,
			"encryptedApiKey", "encryptedApiSecret", "encryptedPassphrase",
			"isActive", "createdAt", "updatedAt"
		FROM exchange_connections
		WHERE "userUid" = $1 AND exchange = $2 AND "isActive" = true`

	var conn ExchangeConnection
	var label, encPassphrase *string
	err := r.pool.QueryRow(ctx, query, userUID, exchange).Scan(
		&conn.ID, &conn.UserUID, &conn.Exchange, &label,
		&conn.EncryptedAPIKey, &conn.EncryptedAPISecret, &encPassphrase,
		&conn.IsActive, &conn.CreatedAt, &conn.UpdatedAt,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	if label != nil {
		conn.Label = *label
	}
	if encPassphrase != nil {
		conn.EncryptedPassphrase = *encPassphrase
	}
	return &conn, nil
}

// GetActiveByUser retrieves all active connections for a user
func (r *ConnectionRepo) GetActiveByUser(ctx context.Context, userUID string) ([]*ExchangeConnection, error) {
	hasCredHash, hasSyncMins, hasExclude, hasKYCLevel, hasIsPaper := r.getCapabilityFlags(ctx)

	if r.isTSSchema {
		return r.getActiveByUserTS(ctx, userUID, hasCredHash, hasSyncMins, hasExclude, hasKYCLevel, hasIsPaper)
	}

	columns := []string{"id", "user_uid", "exchange", "label"}
	if hasCredHash {
		columns = append(columns, "credentials_hash")
	}
	if hasSyncMins {
		columns = append(columns, "sync_interval_minutes")
	}
	if hasExclude {
		columns = append(columns, "exclude_from_report")
	}
	if hasKYCLevel {
		columns = append(columns, "kyc_level")
	}
	if hasIsPaper {
		columns = append(columns, "is_paper")
	}
	columns = append(columns,
		"encrypted_api_key", "api_key_iv", "api_key_auth_tag",
		"encrypted_api_secret", "api_secret_iv", "api_secret_auth_tag",
		"encrypted_passphrase", "passphrase_iv", "passphrase_auth_tag",
		"is_active", "created_at", "updated_at",
	)

	query := fmt.Sprintf(`
		SELECT %s
		FROM exchange_connections
		WHERE user_uid = $1 AND is_active = true
		ORDER BY created_at`,
		strings.Join(columns, ", "),
	)

	rows, err := r.pool.Query(ctx, query, userUID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return r.scanConnectionsGo(rows, hasCredHash, hasSyncMins, hasExclude, hasKYCLevel, hasIsPaper)
}

func (r *ConnectionRepo) getActiveByUserTS(ctx context.Context, userUID string, hasCredHash, hasSyncMins, hasExclude, hasKYCLevel, hasIsPaper bool) ([]*ExchangeConnection, error) {
	columns := []string{`id`, `"userUid"`, `exchange`, `label`}
	if hasCredHash {
		columns = append(columns, `"credentialsHash"`)
	}
	if hasSyncMins {
		columns = append(columns, `"syncIntervalMinutes"`)
	}
	if hasExclude {
		columns = append(columns, `"excludeFromReport"`)
	}
	if hasKYCLevel {
		columns = append(columns, `"kycLevel"`)
	}
	if hasIsPaper {
		columns = append(columns, `"isPaper"`)
	}
	columns = append(columns,
		`"encryptedApiKey"`, `"encryptedApiSecret"`, `"encryptedPassphrase"`,
		`"isActive"`, `"createdAt"`, `"updatedAt"`,
	)

	query := fmt.Sprintf(`
		SELECT %s
		FROM exchange_connections
		WHERE "userUid" = $1 AND "isActive" = true
		ORDER BY "createdAt"`,
		strings.Join(columns, ", "),
	)

	rows, err := r.pool.Query(ctx, query, userUID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var connections []*ExchangeConnection
	for rows.Next() {
		var conn ExchangeConnection
		var label, credHash, kycLevel, encPassphrase *string
		var isPaper, excludeFromReport *bool
		scanArgs := []any{&conn.ID, &conn.UserUID, &conn.Exchange, &label}
		if hasCredHash {
			scanArgs = append(scanArgs, &credHash)
		}
		if hasSyncMins {
			scanArgs = append(scanArgs, &conn.SyncIntervalMinutes)
		}
		if hasExclude {
			scanArgs = append(scanArgs, &excludeFromReport)
		}
		if hasKYCLevel {
			scanArgs = append(scanArgs, &kycLevel)
		}
		if hasIsPaper {
			scanArgs = append(scanArgs, &isPaper)
		}
		// TS: single encrypted field, no IV/AuthTag columns
		scanArgs = append(scanArgs,
			&conn.EncryptedAPIKey, &conn.EncryptedAPISecret, &encPassphrase,
			&conn.IsActive, &conn.CreatedAt, &conn.UpdatedAt,
		)

		if err := rows.Scan(scanArgs...); err != nil {
			return nil, err
		}

		if label != nil {
			conn.Label = *label
		}
		if credHash != nil {
			conn.CredentialsHash = *credHash
		}
		if kycLevel != nil {
			conn.KYCLevel = *kycLevel
		}
		if isPaper != nil {
			conn.IsPaper = *isPaper
		}
		if excludeFromReport != nil {
			conn.ExcludeFromReport = *excludeFromReport
		}
		if encPassphrase != nil {
			conn.EncryptedPassphrase = *encPassphrase
		}
		if !hasSyncMins {
			conn.SyncIntervalMinutes = 1440
		}

		connections = append(connections, &conn)
	}
	return connections, rows.Err()
}

func (r *ConnectionRepo) scanConnectionsGo(rows pgx.Rows, hasCredHash, hasSyncMins, hasExclude, hasKYCLevel, hasIsPaper bool) ([]*ExchangeConnection, error) {
	var connections []*ExchangeConnection
	for rows.Next() {
		var conn ExchangeConnection
		scanArgs := []any{
			&conn.ID, &conn.UserUID, &conn.Exchange, &conn.Label,
		}
		if hasCredHash {
			scanArgs = append(scanArgs, &conn.CredentialsHash)
		}
		if hasSyncMins {
			scanArgs = append(scanArgs, &conn.SyncIntervalMinutes)
		}
		if hasExclude {
			scanArgs = append(scanArgs, &conn.ExcludeFromReport)
		}
		if hasKYCLevel {
			scanArgs = append(scanArgs, &conn.KYCLevel)
		}
		if hasIsPaper {
			scanArgs = append(scanArgs, &conn.IsPaper)
		}
		scanArgs = append(scanArgs,
			&conn.EncryptedAPIKey, &conn.APIKeyIV, &conn.APIKeyAuthTag,
			&conn.EncryptedAPISecret, &conn.APISecretIV, &conn.APISecretAuthTag,
			&conn.EncryptedPassphrase, &conn.PassphraseIV, &conn.PassphraseAuthTag,
			&conn.IsActive, &conn.CreatedAt, &conn.UpdatedAt,
		)

		if err := rows.Scan(scanArgs...); err != nil {
			return nil, err
		}

		if !hasSyncMins {
			conn.SyncIntervalMinutes = 1440
		}

		connections = append(connections, &conn)
	}
	return connections, rows.Err()
}

// GetExcludedExchangesByUser returns all exchanges that should be excluded from reports/analytics.
func (r *ConnectionRepo) GetExcludedExchangesByUser(ctx context.Context, userUID string) (map[string]struct{}, error) {
	excludedKeys, err := r.GetExcludedConnectionKeysByUser(ctx, userUID)
	if err != nil {
		return nil, err
	}

	excludedExchanges := make(map[string]struct{}, len(excludedKeys))
	for key := range excludedKeys {
		parts := strings.SplitN(key, "/", 2)
		excludedExchanges[parts[0]] = struct{}{}
	}
	return excludedExchanges, nil
}

// GetExcludedConnectionKeysByUser returns excluded connection keys.
// Key format is "exchange" or "exchange/label" (lowercase).
func (r *ConnectionRepo) GetExcludedConnectionKeysByUser(ctx context.Context, userUID string) (map[string]struct{}, error) {
	_, _, hasExclude, _, _ := r.getCapabilityFlags(ctx)
	excluded := make(map[string]struct{})
	if !hasExclude {
		return excluded, nil
	}

	var query string
	if r.isTSSchema {
		query = `
			SELECT DISTINCT exchange, COALESCE(label, '')
			FROM exchange_connections
			WHERE "userUid" = $1 AND "isActive" = true AND "excludeFromReport" = true`
	} else {
		query = `
			SELECT DISTINCT exchange, COALESCE(label, '')
			FROM exchange_connections
			WHERE user_uid = $1 AND is_active = true AND exclude_from_report = true`
	}

	rows, err := r.pool.Query(ctx, query, userUID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var exchange string
		var label string
		if err := rows.Scan(&exchange, &label); err != nil {
			return nil, err
		}
		excluded[connectionKey(exchange, label)] = struct{}{}
	}

	return excluded, rows.Err()
}

// ExchangeDetails stores report-relevant metadata for an exchange.
type ExchangeDetails struct {
	Exchange string
	KYCLevel string
	IsPaper  bool
}

// GetExchangeDetailsByUser returns exchange metadata for active user connections.
func (r *ConnectionRepo) GetExchangeDetailsByUser(ctx context.Context, userUID string) ([]*ExchangeDetails, error) {
	_, _, _, hasKYCLevel, hasIsPaper := r.getCapabilityFlags(ctx)

	var (
		query string
		rows  pgx.Rows
		err   error
	)

	if hasKYCLevel && hasIsPaper {
		if r.isTSSchema {
			query = `
				SELECT exchange,
					COALESCE(MAX(NULLIF("kycLevel", '')), '') AS kyc_level,
					BOOL_OR("isPaper") AS is_paper
				FROM exchange_connections
				WHERE "userUid" = $1 AND "isActive" = true
				GROUP BY exchange
				ORDER BY exchange`
		} else {
			query = `
				SELECT exchange,
					COALESCE(MAX(NULLIF(kyc_level, '')), '') AS kyc_level,
					BOOL_OR(is_paper) AS is_paper
				FROM exchange_connections
				WHERE user_uid = $1 AND is_active = true
				GROUP BY exchange
				ORDER BY exchange`
		}
		rows, err = r.pool.Query(ctx, query, userUID)
		if err != nil {
			return nil, err
		}
	} else {
		if r.isTSSchema {
			query = `SELECT DISTINCT exchange FROM exchange_connections WHERE "userUid" = $1 AND "isActive" = true ORDER BY exchange`
		} else {
			query = `SELECT DISTINCT exchange FROM exchange_connections WHERE user_uid = $1 AND is_active = true ORDER BY exchange`
		}
		rows, err = r.pool.Query(ctx, query, userUID)
		if err != nil {
			return nil, err
		}
	}
	defer rows.Close()

	details := make([]*ExchangeDetails, 0)
	for rows.Next() {
		item := &ExchangeDetails{}
		if hasKYCLevel && hasIsPaper {
			if err := rows.Scan(&item.Exchange, &item.KYCLevel, &item.IsPaper); err != nil {
				return nil, err
			}
		} else {
			if err := rows.Scan(&item.Exchange); err != nil {
				return nil, err
			}
			item.KYCLevel = ""
			item.IsPaper = false
		}
		details = append(details, item)
	}

	return details, rows.Err()
}

// UpdateKYCLevel updates kyc_level metadata for a connection.
// It no-ops when the column is not present.
func (r *ConnectionRepo) UpdateKYCLevel(ctx context.Context, connectionID, kycLevel string) error {
	if strings.TrimSpace(connectionID) == "" {
		return nil
	}

	_, _, _, hasKYCLevel, _ := r.getCapabilityFlags(ctx)
	if !hasKYCLevel {
		return nil
	}

	query := `UPDATE exchange_connections SET kyc_level = $1, updated_at = $2 WHERE id = $3`
	_, err := r.pool.Exec(ctx, query, strings.TrimSpace(kycLevel), time.Now().UTC(), connectionID)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "42703" {
			return nil
		}
		return err
	}

	return nil
}

// UpdateIsPaper updates is_paper metadata for a connection.
// It no-ops when the column is not present.
func (r *ConnectionRepo) UpdateIsPaper(ctx context.Context, connectionID string, isPaper bool) error {
	if strings.TrimSpace(connectionID) == "" {
		return nil
	}

	_, _, _, _, hasIsPaper := r.getCapabilityFlags(ctx)
	if !hasIsPaper {
		return nil
	}

	query := `UPDATE exchange_connections SET is_paper = $1, updated_at = $2 WHERE id = $3`
	_, err := r.pool.Exec(ctx, query, isPaper, time.Now().UTC(), connectionID)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "42703" {
			return nil
		}
		return err
	}

	return nil
}

func connectionKey(exchange, label string) string {
	ex := strings.ToLower(strings.TrimSpace(exchange))
	lb := strings.ToLower(strings.TrimSpace(label))
	if lb == "" {
		return ex
	}
	return ex + "/" + lb
}

// GetByUserExchangeLabel retrieves a connection by user, exchange, and label
func (r *ConnectionRepo) GetByUserExchangeLabel(ctx context.Context, userUID, exchange, label string) (*ExchangeConnection, error) {
	r.getCapabilityFlags(ctx)

	if r.isTSSchema {
		query := `
			SELECT id, "userUid", exchange, label,
				"encryptedApiKey", "encryptedApiSecret", "encryptedPassphrase",
				"isActive", "createdAt", "updatedAt"
			FROM exchange_connections
			WHERE "userUid" = $1 AND exchange = $2 AND TRIM(label) = TRIM($3) AND "isActive" = true`

		var conn ExchangeConnection
		var encPassphrase *string
		err := r.pool.QueryRow(ctx, query, userUID, exchange, label).Scan(
			&conn.ID, &conn.UserUID, &conn.Exchange, &conn.Label,
			&conn.EncryptedAPIKey, &conn.EncryptedAPISecret, &encPassphrase,
			&conn.IsActive, &conn.CreatedAt, &conn.UpdatedAt,
		)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		if encPassphrase != nil {
			conn.EncryptedPassphrase = *encPassphrase
		}
		return &conn, err
	}

	query := `
		SELECT id, user_uid, exchange, label,
			encrypted_api_key, api_key_iv, api_key_auth_tag,
			encrypted_api_secret, api_secret_iv, api_secret_auth_tag,
			encrypted_passphrase, passphrase_iv, passphrase_auth_tag,
			is_active, created_at, updated_at
		FROM exchange_connections
		WHERE user_uid = $1 AND exchange = $2 AND TRIM(label) = TRIM($3) AND is_active = true`

	var conn ExchangeConnection
	err := r.pool.QueryRow(ctx, query, userUID, exchange, label).Scan(
		&conn.ID, &conn.UserUID, &conn.Exchange, &conn.Label,
		&conn.EncryptedAPIKey, &conn.APIKeyIV, &conn.APIKeyAuthTag,
		&conn.EncryptedAPISecret, &conn.APISecretIV, &conn.APISecretAuthTag,
		&conn.EncryptedPassphrase, &conn.PassphraseIV, &conn.PassphraseAuthTag,
		&conn.IsActive, &conn.CreatedAt, &conn.UpdatedAt,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return &conn, err
}

// Deactivate soft-deletes a connection
func (r *ConnectionRepo) Deactivate(ctx context.Context, id string) error {
	query := `UPDATE exchange_connections SET is_active = false, updated_at = $1 WHERE id = $2`
	_, err := r.pool.Exec(ctx, query, time.Now().UTC(), id)
	return err
}

func (r *ConnectionRepo) getCapabilityFlags(ctx context.Context) (hasCredentialsHash bool, hasSyncIntervalMinutes bool, hasExcludeFromReport bool, hasKYCLevel bool, hasIsPaper bool) {
	r.capMu.Lock()
	defer r.capMu.Unlock()

	if r.capabilitiesLoaded {
		return r.hasCredentialsHashCol, r.hasSyncIntervalMinsCol, r.hasExcludeFromReportCol, r.hasKYCLevelCol, r.hasIsPaperCol
	}

	// Detect TS Prisma schema (camelCase) vs Go schema (snake_case)
	// If "userUid" column exists → TS schema; if "user_uid" → Go schema
	tsSchema, _ := r.columnExists(ctx, "exchange_connections", "userUid")
	r.isTSSchema = tsSchema

	// Check capability columns using the correct naming
	credHashCol, _ := r.columnExists(ctx, "exchange_connections", r.colName("credentials_hash"))
	syncIntervalCol, _ := r.columnExists(ctx, "exchange_connections", r.colName("sync_interval_minutes"))
	excludeFromReportCol, _ := r.columnExists(ctx, "exchange_connections", r.colName("exclude_from_report"))
	kycLevelCol, _ := r.columnExists(ctx, "exchange_connections", r.colName("kyc_level"))
	isPaperCol, _ := r.columnExists(ctx, "exchange_connections", r.colName("is_paper"))

	r.hasCredentialsHashCol = credHashCol
	r.hasSyncIntervalMinsCol = syncIntervalCol
	r.hasExcludeFromReportCol = excludeFromReportCol
	r.hasKYCLevelCol = kycLevelCol
	r.hasIsPaperCol = isPaperCol
	r.capabilitiesLoaded = true

	return r.hasCredentialsHashCol, r.hasSyncIntervalMinsCol, r.hasExcludeFromReportCol, r.hasKYCLevelCol, r.hasIsPaperCol
}

// colName returns the raw column name (without quotes) for columnExists checks.
func (r *ConnectionRepo) colName(snakeName string) string {
	if !r.isTSSchema {
		return snakeName
	}
	if mapped, ok := tsColumnMap[snakeName]; ok {
		return mapped
	}
	return snakeName
}

func (r *ConnectionRepo) columnExists(ctx context.Context, tableName, columnName string) (bool, error) {
	query := `
		SELECT EXISTS (
			SELECT 1
			FROM information_schema.columns
			WHERE table_schema = 'public'
			  AND table_name = $1
			  AND column_name = $2
		)`
	var exists bool
	if err := r.pool.QueryRow(ctx, query, tableName, columnName).Scan(&exists); err != nil {
		return false, fmt.Errorf("check column %s.%s: %w", tableName, columnName, err)
	}
	return exists, nil
}

// tsColumnMap maps Go snake_case column names to TS Prisma camelCase names.
var tsColumnMap = map[string]string{
	"user_uid":              "userUid",
	"encrypted_api_key":     "encryptedApiKey",
	"encrypted_api_secret":  "encryptedApiSecret",
	"encrypted_passphrase":  "encryptedPassphrase",
	"credentials_hash":      "credentialsHash",
	"is_active":             "isActive",
	"sync_interval_minutes": "syncIntervalMinutes",
	"exclude_from_report":   "excludeFromReport",
	"is_paper":              "isPaper",
	"kyc_level":             "kycLevel",
	"created_at":            "createdAt",
	"updated_at":            "updatedAt",
}
