package rules

import (
	"fmt"

	"github.com/apache/cassandra-gocql-driver/v2"

	"pulse/internal/config"
	"pulse/internal/model"
)

// T1 Odd-hour: a transaction at an hour this specific account has never used.
// Reads hourly_activity BEFORE the producer increments it for the current
// transaction, so a zero count for this hour means "first-ever at this hour".
// The history guard prevents flagging brand-new accounts, where every hour is a
// first time.
func T1OddHour(s *gocql.Session, t model.Transaction) ([]model.Alert, error) {
	hour := t.Timestamp.Hour()

	var countThisHour int64
	err := s.Query(
		`SELECT txn_count FROM hourly_activity WHERE account_id = ? AND hour_of_day = ?`,
		t.AccountID, hour).Scan(&countThisHour)
	if err != nil && err != gocql.ErrNotFound {
		return nil, fmt.Errorf("t1 hour read: %w", err)
	}

	// total history across all hours — still a single-partition query
	iter := s.Query(
		`SELECT txn_count FROM hourly_activity WHERE account_id = ?`,
		t.AccountID).Iter()
	var c, total int64
	for iter.Scan(&c) {
		total += c
	}
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("t1 total read: %w", err)
	}

	if countThisHour == 0 && total >= config.OddHourMinHistory {
		return []model.Alert{newAlert(t, "odd_hour", "low",
			fmt.Sprintf("first-ever transaction at %02d:00 (history: %d txns)", hour, total))}, nil
	}
	return nil, nil
}
