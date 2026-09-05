// Package engine runs every rule against a transaction and persists any alerts.
package engine

import (
	"fmt"

	"github.com/apache/cassandra-gocql-driver/v2"

	"pulse/internal/config"
	"pulse/internal/model"
	"pulse/internal/rules"
)

const alertHourLayout = "2006-01-02T15"

var checkers = rules.All()

// Evaluate runs all rules against t, persists each fired alert, and returns them
// (for logging / metrics).
func Evaluate(s *gocql.Session, t model.Transaction) ([]model.Alert, error) {
	var fired []model.Alert
	for _, check := range checkers {
		alerts, err := check(s, t)
		if err != nil {
			return fired, err
		}
		fired = append(fired, alerts...)
	}
	for _, a := range fired {
		if err := writeAlert(s, a); err != nil {
			return fired, err
		}
	}
	return fired, nil
}

// writeAlert persists to alerts_by_account (per-account history) and to the
// hour-bucketed alerts_recent (bounded "recent across all accounts" read for the
// dashboard). Both are denormalized views of the same alert.
func writeAlert(s *gocql.Session, a model.Alert) error {
	if err := s.Query(
		`INSERT INTO alerts_by_account
		 (account_id, raised_at, alert_id, rule, severity, detail, txn_id)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		a.AccountID, a.RaisedAt, gocql.UUID(a.AlertID), a.Rule, a.Severity, a.Detail, gocql.UUID(a.TxnID),
	).Exec(); err != nil {
		return fmt.Errorf("alert write (by_account): %w", err)
	}
	if err := s.Query(
		`INSERT INTO alerts_recent
		 (bucket, raised_at, alert_id, account_id, rule, severity, detail)
		 VALUES (?, ?, ?, ?, ?, ?, ?) USING TTL ?`,
		a.RaisedAt.Format(alertHourLayout), a.RaisedAt, gocql.UUID(a.AlertID),
		a.AccountID, a.Rule, a.Severity, a.Detail, config.TxnTTLSeconds,
	).Exec(); err != nil {
		return fmt.Errorf("alert write (recent): %w", err)
	}
	return nil
}
