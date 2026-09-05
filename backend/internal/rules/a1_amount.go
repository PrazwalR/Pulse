package rules

import (
	"fmt"

	"github.com/apache/cassandra-gocql-driver/v2"

	"pulse/internal/config"
	"pulse/internal/model"
)

// A1 Amount anomaly: a single transaction far above this account's normal spend.
// Uses a z-score against the account's baseline (mean + std-dev). A new account
// with no baseline is never flagged.
func A1AmountAnomaly(s *gocql.Session, t model.Transaction) ([]model.Alert, error) {
	var avg, stddev float64
	err := s.Query(
		`SELECT avg_amount, stddev_amount FROM account_baseline WHERE account_id = ?`,
		t.AccountID).Scan(&avg, &stddev)
	if err == gocql.ErrNotFound {
		return nil, nil // no baseline yet
	}
	if err != nil {
		return nil, fmt.Errorf("a1 read: %w", err)
	}
	if stddev <= 0 {
		return nil, nil
	}
	z := (t.Amount - avg) / stddev
	if z > config.ZScoreThreshold {
		return []model.Alert{newAlert(t, "amount_anomaly", "high",
			fmt.Sprintf("z=%.1f (Rs %.2f vs avg Rs %.2f)", z, t.Amount, avg))}, nil
	}
	return nil, nil
}
