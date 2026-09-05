// Package rules holds the eight fraud detection rules, one file per rule. Each
// rule reads pre-computed state from Cassandra and compares it to a threshold —
// no machine learning, every rule explainable in one sentence.
package rules

import (
	"fmt"
	"time"

	"github.com/apache/cassandra-gocql-driver/v2"
	"github.com/google/uuid"

	"pulse/internal/config"
	"pulse/internal/model"
)

// Checker is the signature every rule satisfies. It returns zero, one, or more
// alerts (V1 can raise both a 1-min and a 5-min alert).
type Checker func(s *gocql.Session, t model.Transaction) ([]model.Alert, error)

// All returns every rule, in evaluation order. Register new rules here.
func All() []Checker {
	return []Checker{
		V1Velocity,
		A1AmountAnomaly,
		G1SpatialImpossibility,
		S1Structuring,
		T1OddHour,
		C1CardTesting,
		D1DormantReactivation,
		M1HighRiskMerchant,
	}
}

// readWindowCounter reads the bucketed velocity counter for the window that the
// transaction's timestamp falls into (a missing row means zero, not an error).
// Shared by V1 (count) and C1 (count + total).
func readWindowCounter(s *gocql.Session, account, window string, ts time.Time) (count, totalPaise int64, err error) {
	bucket := ts.Unix() / config.WindowSeconds(window)
	scanErr := s.Query(
		`SELECT txn_count, total_amount FROM velocity_counters WHERE account_id = ? AND window = ? AND bucket = ?`,
		account, window, bucket).Scan(&count, &totalPaise)
	if scanErr == gocql.ErrNotFound {
		return 0, 0, nil
	}
	if scanErr != nil {
		return 0, 0, fmt.Errorf("velocity counter read (%s): %w", window, scanErr)
	}
	return count, totalPaise, nil
}

// newAlert stamps a fired alert with a fresh id and UTC timestamp.
func newAlert(t model.Transaction, rule, severity, detail string) model.Alert {
	return model.Alert{
		AccountID: t.AccountID,
		RaisedAt:  time.Now().UTC(),
		AlertID:   uuid.New(),
		Rule:      rule,
		Severity:  severity,
		Detail:    detail,
		TxnID:     t.TxnID,
	}
}
