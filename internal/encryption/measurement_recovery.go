package encryption

import (
	"context"
	"crypto/ecdsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"fmt"

	"go.uber.org/zap"
)

// handleUnwrapFailure is called by initializeDEK when the primary unwrap with
// the current measurement-derived (or externally supplied) master key fails.
// It attempts auto-recovery from historical signed_reports measurements, and on
// success sets s.currentDEK, s.dekID, s.dekSchema, s.derivation, and
// s.legacyMasterKeyApplied so the re-wrap check in initializeDEK proceeds
// normally. Returns the original unwrapErr (wrapped with context) on all failure
// paths so callers always get a descriptive message.
func (s *KeyManagementService) handleUnwrapFailure(ctx context.Context, wrapped *EncryptedData, dek *DEK, schema string, unwrapErr error) error {
	if !s.autoRecovery {
		return fmt.Errorf(
			"unwrap active DEK (id=%s, stored_master_key_id=%s, derived_master_key_id=%s): %w — "+
				"auto-recovery is disabled (MEASUREMENT_AUTO_RECOVERY=false); "+
				"set LEGACY_MASTER_KEY_HEX or re-enable auto-recovery to recover",
			dek.ID, dek.MasterKeyID, s.derivation.GetMasterKeyID(), unwrapErr,
		)
	}

	recovered, recovErr := s.tryMeasurementRecovery(ctx, wrapped)
	if recovErr != nil {
		return fmt.Errorf(
			"unwrap DEK (id=%s): %w; measurement auto-recovery query failed: %v — "+
				"set LEGACY_MASTER_KEY_HEX to recover manually",
			dek.ID, unwrapErr, recovErr,
		)
	}
	if !recovered {
		return fmt.Errorf(
			"unwrap DEK (id=%s, stored_master_key_id=%s, derived_master_key_id=%s): %w — "+
				"measurement auto-recovery found no matching historical measurement; "+
				"set LEGACY_MASTER_KEY_HEX to recover manually",
			dek.ID, dek.MasterKeyID, s.derivation.GetMasterKeyID(), unwrapErr,
		)
	}

	// tryMeasurementRecovery set s.currentDEK, s.derivation, s.legacyMasterKeyApplied.
	s.dekID = dek.ID
	s.dekSchema = schema
	return nil
}

// tryMeasurementRecovery attempts to unwrap the active DEK using master keys
// derived from historical SEV-SNP measurements stored in signed_reports. On
// success it sets s.currentDEK, s.derivation (the recovering key), and
// s.legacyMasterKeyApplied=true so the re-wrap check in initializeDEK fires.
// Returns (true, nil) on success, (false, nil) when no candidate matched, or
// (false, err) on an unexpected query failure.
func (s *KeyManagementService) tryMeasurementRecovery(ctx context.Context, wrapped *EncryptedData) (bool, error) {
	// DISTINCT ON picks the most recent report per distinct measurement.
	// LIMIT 20: bounds unwrap attempts to a predictable cost; a healthy
	// enclave has at most ~5 distinct measurements over 6 months.
	// Two payload shapes coexist in signed_reports and recovery must read both,
	// because the rows that predate a measurement change are the only ones that
	// can rescue it:
	//   - Go (this service): json.Marshal of SignedReport — snake_case keys with
	//     the measurement nested under enclave_attestation;
	//   - TypeScript (the predecessor enclave): camelCase keys with the
	//     measurement at the root. Every row currently in production is this one.
	// The column names are camelCase in both cases; the previous snake_case form
	// raised 42703 on every call, so recovery reported "no historical
	// measurements" and the enclave died on a measurement change with perfectly
	// usable candidates sitting in the table.
	rows, err := s.pool.Query(ctx, `
		WITH candidates AS (
			SELECT
				COALESCE(
					"reportData"->'enclave_attestation'->>'measurement',
					"reportData"->>'measurement'
				) AS measurement,
				"reportHash" AS report_hash,
				signature,
				COALESCE(
					"reportData"->>'public_key',
					"reportData"->>'publicKey'
				) AS public_key,
				COALESCE(
					"reportData"->>'signature_algorithm',
					"reportData"->>'signatureAlgorithm',
					'ECDSA-P256-SHA256'
				) AS sig_algo,
				"createdAt"
			FROM signed_reports
			WHERE "createdAt" > NOW() - make_interval(days => $1)
		)
		SELECT DISTINCT ON (measurement)
			measurement, report_hash, signature, public_key, sig_algo
		FROM candidates
		WHERE measurement IS NOT NULL AND measurement <> ''
		ORDER BY measurement, "createdAt" DESC
		LIMIT 20`,
		s.recoveryLookbackDays,
	)
	if err != nil {
		return false, fmt.Errorf("query historical measurements: %w", err)
	}
	defer rows.Close()

	type candidate struct {
		measurement string
		reportHash  string
		signature   string
		publicKey   string
		algorithm   string
	}

	var candidates []candidate
	for rows.Next() {
		var c candidate
		if err := rows.Scan(&c.measurement, &c.reportHash, &c.signature, &c.publicKey, &c.algorithm); err != nil {
			continue
		}
		candidates = append(candidates, c)
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("scan historical measurements: %w", err)
	}

	if len(candidates) == 0 {
		if s.logger != nil {
			s.logger.Info("measurement auto-recovery: no historical measurements in signed_reports within lookback window",
				zap.Int("lookback_days", s.recoveryLookbackDays),
			)
		}
		return false, nil
	}

	tried := 0
	for _, c := range candidates {
		// Check the ECDSA signature before deriving a key from this measurement.
		// SEC-008: this is self-referential — the signature is verified against
		// the public key embedded in the SAME signed_reports row, so it does
		// NOT authenticate the measurement against a trusted key. It only
		// filters out corrupt / partially-written rows. The real gate is the
		// UnwrapKey call below: a measurement an attacker chose cannot derive
		// the key that wrapped the genuine DEK (HKDF preimage resistance).
		ok, verifyErr := verifyECDSAReportSignature(c.reportHash, c.signature, c.publicKey, c.algorithm)
		if verifyErr != nil || !ok {
			if s.logger != nil {
				prefix := c.measurement
				if len(prefix) > 16 {
					prefix = prefix[:16]
				}
				s.logger.Debug("measurement auto-recovery: skipping report with invalid signature",
					zap.String("measurement_prefix", prefix),
					zap.Bool("sig_valid", ok),
					zap.Error(verifyErr),
				)
			}
			continue
		}

		measurementBytes, hexErr := hex.DecodeString(c.measurement)
		if hexErr != nil || len(measurementBytes) == 0 {
			continue
		}

		candidateDeriv, derivErr := newKeyDerivationServiceFromMeasurementBytes(measurementBytes, nil)
		if derivErr != nil {
			continue
		}

		if candidateDeriv.GetMasterKeyID() == s.derivation.GetMasterKeyID() {
			continue // already tried this key above
		}

		tried++
		unwrapped, unwrapErr := candidateDeriv.UnwrapKey(wrapped)
		if unwrapErr != nil {
			continue
		}

		// Successful unwrap. Setting legacyMasterKeyApplied=true causes the
		// re-wrap check in initializeDEK to fire and persist the DEK under the
		// current measurement-derived key so subsequent boots need no intervention.
		s.currentDEK = unwrapped
		s.derivation = candidateDeriv
		s.legacyMasterKeyApplied = true

		if s.logger != nil {
			s.logger.Info("measurement auto-recovery succeeded",
				zap.String("old_measurement", c.measurement),
				zap.String("old_master_key_id", candidateDeriv.GetMasterKeyID()),
				zap.String("new_master_key_id", s.measurementMasterKeyID),
				zap.Int("candidates_tried", tried),
			)
		}

		if s.onMeasurementRecovery != nil {
			s.onMeasurementRecovery()
		}

		return true, nil
	}

	if s.logger != nil {
		s.logger.Warn("measurement auto-recovery: exhausted all candidates without finding a matching key",
			zap.Int("candidates_tried", tried),
			zap.Int("candidates_found", len(candidates)),
		)
	}
	return false, nil
}

// verifyECDSAReportSignature checks the ECDSA-P256-SHA256 signature over
// reportHash using publicKey (base64 DER-encoded SPKI). Returns (false, nil)
// for a well-formed but invalid signature; (false, err) for malformed inputs.
// Ed25519 legacy reports predate the enclave_attestation.measurement field and
// are excluded by the SQL WHERE clause, so only ECDSA is accepted here.
func verifyECDSAReportSignature(reportHash, signatureB64, publicKey, algorithm string) (bool, error) {
	switch algorithm {
	case "", "ECDSA-P256-SHA256":
		// expected
	default:
		return false, fmt.Errorf("unsupported algorithm for recovery: %q", algorithm)
	}

	sigDER, err := base64.StdEncoding.DecodeString(signatureB64)
	if err != nil {
		return false, fmt.Errorf("decode signature: %w", err)
	}
	pubDER, err := base64.StdEncoding.DecodeString(publicKey)
	if err != nil {
		return false, fmt.Errorf("decode public key: %w", err)
	}
	parsed, err := x509.ParsePKIXPublicKey(pubDER)
	if err != nil {
		return false, fmt.Errorf("parse public key: %w", err)
	}
	ecKey, ok := parsed.(*ecdsa.PublicKey)
	if !ok {
		return false, fmt.Errorf("public key is not ECDSA")
	}
	digest := sha256.Sum256([]byte(reportHash))
	return ecdsa.VerifyASN1(ecKey, digest[:], sigDER), nil
}
