package rules

import (
	"fmt"

	"github.com/apache/cassandra-gocql-driver/v2"

	"pulse/internal/config"
	"pulse/internal/model"
)

// S1 Structuring: several transactions kept just under a reporting threshold — a
// classic laundering signature. Scans today's partition for this account (a
// single bounded (account_id, day) partition read, never a cross-partition scan).
func S1Structuring(s *gocql.Session, t model.Transaction) ([]model.Alert, error) {
	iter := s.Query(
		`SELECT amount FROM txn_by_account_day WHERE account_id = ? AND day = ?`,
		t.AccountID, t.Timestamp.Format("2006-01-02")).Iter()
	var amount float64
	near := 0
	for iter.Scan(&amount) {
		if amount >= 0.9*config.StructuringThreshold && amount < config.StructuringThreshold {
			near++
		}
	}
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("s1 read: %w", err)
	}
	if near >= config.StructuringMinCount {
		return []model.Alert{newAlert(t, "structuring", "medium",
			fmt.Sprintf("%d transactions at 90-100%% of threshold today", near))}, nil
	}
	return nil, nil
}
