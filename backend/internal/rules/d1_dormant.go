package rules

import (
	"fmt"
	"time"

	"github.com/apache/cassandra-gocql-driver/v2"

	"pulse/internal/config"
	"pulse/internal/model"
)

// D1 Dormant reactivation: an account silent for weeks, then a sudden large
// transaction — a classic account-takeover signature. Reads account_last_seen
// BEFORE the producer refreshes it, so it compares against the *previous*
// activity.
func D1DormantReactivation(s *gocql.Session, t model.Transaction) ([]model.Alert, error) {
	var lastSeen time.Time
	err := s.Query(
		`SELECT last_txn_at FROM account_last_seen WHERE account_id = ?`,
		t.AccountID).Scan(&lastSeen)
	if err != nil && err != gocql.ErrNotFound {
		return nil, fmt.Errorf("d1 read: %w", err)
	}
	if lastSeen.IsZero() {
		return nil, nil // first time we've seen this account
	}

	dormancy := t.Timestamp.Sub(lastSeen)
	if dormancy.Hours() > 24.0*float64(config.DormancyDays) && t.Amount >= config.DormancyMinAmount {
		return []model.Alert{newAlert(t, "dormant_reactivation", "medium",
			fmt.Sprintf("first activity in %.0f days, amount Rs %.2f", dormancy.Hours()/24.0, t.Amount))}, nil
	}
	return nil, nil
}
