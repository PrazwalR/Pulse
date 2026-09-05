// Package engine runs every rule against a transaction and persists any alerts.
package engine

import (
	"fmt"

	"github.com/apache/cassandra-gocql-driver/v2"

	"pulse/internal/model"
	"pulse/internal/rules"
)

var checkers = rules.All()

// Evaluate runs all rules against t, writes each fired alert to
// alerts_by_account, and returns them (for logging / metrics).
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

func writeAlert(s *gocql.Session, a model.Alert) error {
	if err := s.Query(
		`INSERT INTO alerts_by_account
		 (account_id, raised_at, alert_id, rule, severity, detail, txn_id)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		a.AccountID, a.RaisedAt, gocql.UUID(a.AlertID), a.Rule, a.Severity, a.Detail, gocql.UUID(a.TxnID),
	).Exec(); err != nil {
		return fmt.Errorf("alert write: %w", err)
	}
	return nil
}
