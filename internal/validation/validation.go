package validation

import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

const (
	maxUserUIDLen          = 100
	maxExchangeLen         = 50
	maxLabelLen            = 100
	maxAPIKeyLen           = 500
	maxAPISecretLen        = 500
	maxPassphraseLen       = 500
	maxTimestampRangeYears = 5
	maxFutureTimestampSkew = 24 * time.Hour
	minSyncIntervalMinutes = 5
	maxSyncIntervalMinutes = 10080 // 7 days
)

var (
	clerkIDRegex  = regexp.MustCompile(`^user_[a-zA-Z0-9]{10,50}$`)
	uuidRegex     = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
	cuidRegex     = regexp.MustCompile(`^c[a-z0-9]{20,30}$`)
	exchangeRegex = regexp.MustCompile(`^[a-z0-9_-]+$`)
)

// ValidateUserUID checks if the UID matches Clerk ID, UUID, or CUID format.
func ValidateUserUID(uid string) error {
	if uid == "" {
		return fmt.Errorf("user_uid is required")
	}
	if len(uid) > maxUserUIDLen {
		return fmt.Errorf("user_uid too long: max %d characters", maxUserUIDLen)
	}
	if clerkIDRegex.MatchString(uid) || uuidRegex.MatchString(uid) || cuidRegex.MatchString(uid) {
		return nil
	}
	return fmt.Errorf("invalid user_uid format: must be Clerk ID, UUID, or CUID")
}

// ValidateExchange checks exchange format constraints.
// TS parity: format validation is enforced, not a hardcoded support list.
func ValidateExchange(exchange string) error {
	if exchange == "" {
		return fmt.Errorf("exchange is required")
	}
	if len(exchange) > maxExchangeLen {
		return fmt.Errorf("exchange too long: max %d characters", maxExchangeLen)
	}
	if !exchangeRegex.MatchString(exchange) {
		return fmt.Errorf("invalid exchange format: must match [a-z0-9_-]+")
	}
	return nil
}

// ValidateLabel checks label constraints.
func ValidateLabel(label string) error {
	if strings.TrimSpace(label) == "" {
		return fmt.Errorf("label is required")
	}
	if len(label) > maxLabelLen {
		return fmt.Errorf("label too long: max %d characters", maxLabelLen)
	}
	return nil
}

// ValidateAPIKey checks API key length constraints.
func ValidateAPIKey(key string) error {
	if len(key) > maxAPIKeyLen {
		return fmt.Errorf("api_key too long: max %d characters", maxAPIKeyLen)
	}
	return nil
}

// ValidateAPISecret checks API secret length constraints.
func ValidateAPISecret(secret string) error {
	if len(secret) > maxAPISecretLen {
		return fmt.Errorf("api_secret too long: max %d characters", maxAPISecretLen)
	}
	return nil
}

// ValidatePassphrase checks passphrase length constraints.
func ValidatePassphrase(passphrase string) error {
	if len(passphrase) > maxPassphraseLen {
		return fmt.Errorf("passphrase too long: max %d characters", maxPassphraseLen)
	}
	return nil
}

// ValidateSyncIntervalMinutes validates optional per-connection sync interval.
// Zero means "not provided" and defaults are applied by the service layer.
func ValidateSyncIntervalMinutes(minutes int) error {
	if minutes == 0 {
		return nil
	}
	if minutes < minSyncIntervalMinutes || minutes > maxSyncIntervalMinutes {
		return fmt.Errorf("sync_interval_minutes must be between %d and %d", minSyncIntervalMinutes, maxSyncIntervalMinutes)
	}
	return nil
}

// ValidateTimestampRange checks that start < end and range is within maxTimestampRangeYears.
func ValidateTimestampRange(start, end time.Time) error {
	if !start.IsZero() && !end.IsZero() {
		if end.Before(start) {
			return fmt.Errorf("end_date must be after start_date")
		}
		maxEnd := start.AddDate(maxTimestampRangeYears, 0, 0)
		if end.After(maxEnd) {
			return fmt.Errorf("date range too large: max %d years", maxTimestampRangeYears)
		}
	}
	return nil
}

// ValidateOptionalTimestampMillis validates optional epoch-millisecond timestamps.
// Zero is treated as "not provided", matching gRPC optional semantics.
func ValidateOptionalTimestampMillis(ts int64, field string) error {
	if ts == 0 {
		return nil
	}
	if ts < 0 {
		return fmt.Errorf("%s must be a positive Unix timestamp in milliseconds", field)
	}
	maxAllowed := time.Now().Add(maxFutureTimestampSkew).UnixMilli()
	if ts >= maxAllowed {
		return fmt.Errorf("%s must be less than 24h in the future", field)
	}
	return nil
}

// CreateConnectionRequest represents validated connection input.
type CreateConnectionRequest struct {
	UserUID             string
	Exchange            string
	Label               string
	APIKey              string
	APISecret           string
	Passphrase          string
	SyncIntervalMinutes int
}

// ValidateCreateConnection validates all fields for connection creation.
func ValidateCreateConnection(req *CreateConnectionRequest) error {
	if err := ValidateUserUID(req.UserUID); err != nil {
		return err
	}
	if err := ValidateExchange(req.Exchange); err != nil {
		return err
	}
	if err := ValidateLabel(req.Label); err != nil {
		return err
	}
	if err := ValidateAPIKey(req.APIKey); err != nil {
		return err
	}
	if err := ValidateAPISecret(req.APISecret); err != nil {
		return err
	}
	if err := ValidatePassphrase(req.Passphrase); err != nil {
		return err
	}
	if err := ValidateSyncIntervalMinutes(req.SyncIntervalMinutes); err != nil {
		return err
	}
	if req.APIKey == "" {
		return fmt.Errorf("api_key is required")
	}
	return nil
}

// SyncJobRequest represents validated sync input.
type SyncJobRequest struct {
	UserUID  string
	Exchange string // Optional
}

// ValidateSyncRequest validates sync request fields.
func ValidateSyncRequest(req *SyncJobRequest) error {
	if err := ValidateUserUID(req.UserUID); err != nil {
		return err
	}
	if req.Exchange != "" {
		if err := ValidateExchange(req.Exchange); err != nil {
			return err
		}
	}
	return nil
}

// ReportRequest represents validated report input.
type ReportRequest struct {
	UserUID   string
	StartDate string
	EndDate   string
	Benchmark string
}

// ValidateReportRequest validates report request fields.
func ValidateReportRequest(req *ReportRequest) error {
	if err := ValidateUserUID(req.UserUID); err != nil {
		return err
	}

	// start_date / end_date are optional (TS parity). When provided, validate format.
	var (
		start time.Time
		end   time.Time
		err   error
	)
	if req.StartDate != "" {
		start, err = time.Parse("2006-01-02", req.StartDate)
		if err != nil {
			return fmt.Errorf("invalid start_date format: use YYYY-MM-DD")
		}
	}
	if req.EndDate != "" {
		end, err = time.Parse("2006-01-02", req.EndDate)
		if err != nil {
			return fmt.Errorf("invalid end_date format: use YYYY-MM-DD")
		}
	}

	// Range checks apply only when both dates are provided.
	if !start.IsZero() && !end.IsZero() {
		if err := ValidateTimestampRange(start, end); err != nil {
			return err
		}
	}
	return nil
}
