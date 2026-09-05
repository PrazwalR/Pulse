package rules

import (
	"fmt"

	"github.com/apache/cassandra-gocql-driver/v2"

	"pulse/internal/config"
	"pulse/internal/model"
)

// M1 High-risk merchant: a large transaction at a gambling / crypto /
// wire-transfer merchant. A pure check on the current transaction — the category
// is already in hand, so no database read is needed (the session parameter is
// kept only to match the Checker signature).
func M1HighRiskMerchant(_ *gocql.Session, t model.Transaction) ([]model.Alert, error) {
	if !config.HighRiskCategories[t.MerchantCategory] {
		return nil, nil
	}
	if t.Amount < config.HighRiskMinAmount {
		return nil, nil
	}
	return []model.Alert{newAlert(t, "high_risk_merchant", "medium",
		fmt.Sprintf("%s transaction of Rs %.2f", t.MerchantCategory, t.Amount))}, nil
}
