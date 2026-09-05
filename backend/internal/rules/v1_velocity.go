package rules

import (
	"fmt"

	"github.com/apache/cassandra-gocql-driver/v2"

	"pulse/internal/config"
	"pulse/internal/model"
)

// V1 Velocity: too many transactions in a short window (a card being drained).
// Reads the pre-computed, time-bucketed velocity_counters, which the producer
// increments before this rule runs so the current transaction is counted.
func V1Velocity(s *gocql.Session, t model.Transaction) ([]model.Alert, error) {
	oneMin, _, err := readWindowCounter(s, t.AccountID, "1min", t.Timestamp)
	if err != nil {
		return nil, err
	}
	fiveMin, _, err := readWindowCounter(s, t.AccountID, "5min", t.Timestamp)
	if err != nil {
		return nil, err
	}

	var out []model.Alert
	if oneMin > config.Velocity1MinThreshold {
		out = append(out, newAlert(t, "velocity_1min", "high",
			fmt.Sprintf("%d transactions in the last minute", oneMin)))
	}
	if fiveMin > config.Velocity5MinThreshold {
		out = append(out, newAlert(t, "velocity_5min", "medium",
			fmt.Sprintf("%d transactions in the last 5 minutes", fiveMin)))
	}
	return out, nil
}
