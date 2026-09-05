// Package model holds the types shared across the producer and the rule engine.
package model

import (
	"time"

	"github.com/google/uuid"
)

// Transaction is the unit that flows through the whole system.
type Transaction struct {
	AccountID        string
	CardID           string
	TxnID            uuid.UUID
	Timestamp        time.Time // always UTC (INV-11)
	Amount           float64   // rupees
	Merchant         string
	MerchantCategory string
	City             string
	Channel          string // "card" or "upi"

	// Ground truth planted by the producer, for precision/recall (experiment E6).
	IsFraud bool
	Label   string // which scenario was planted ("velocity", "geo", ... ; "" if legitimate)
}

// Alert is what a rule emits when it fires.
type Alert struct {
	AccountID string
	RaisedAt  time.Time
	AlertID   uuid.UUID
	Rule      string
	Severity  string // "high", "medium", "low"
	Detail    string
	TxnID     uuid.UUID
}
