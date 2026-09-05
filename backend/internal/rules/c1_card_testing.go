package rules

import (
	"fmt"

	"github.com/apache/cassandra-gocql-driver/v2"

	"pulse/internal/config"
	"pulse/internal/model"
)

// C1 Card testing: a burst of many transactions whose average amount is tiny —
// thieves sending small test charges to confirm a stolen card is live before
// draining it. Reuses the 1-minute velocity counter (total_amount is int paise),
// no new table.
func C1CardTesting(s *gocql.Session, t model.Transaction) ([]model.Alert, error) {
	count, totalPaise, err := readWindowCounter(s, t.AccountID, "1min", t.Timestamp)
	if err != nil {
		return nil, err
	}
	if count < int64(config.CardTestingMinCount) {
		return nil, nil
	}
	avgRupees := float64(totalPaise) / 100.0 / float64(count)
	if avgRupees < config.CardTestingMaxAvg {
		return []model.Alert{newAlert(t, "card_testing", "high",
			fmt.Sprintf("%d micro-transactions in the last minute, avg Rs %.2f", count, avgRupees))}, nil
	}
	return nil, nil
}
