// Package config is the single source of tunable values: connection settings
// (env-overridable) and every detection threshold. No magic numbers elsewhere.
package config

import (
	"os"
	"strconv"
	"strings"
)

// --- Cassandra connection (env-overridable, local-friendly defaults) ---
var (
	ContactPoints = splitEnv("CASSANDRA_CONTACT_POINTS", "127.0.0.1")
	Port          = intEnv("CASSANDRA_PORT", 9042)
	Keyspace      = getenv("CASSANDRA_KEYSPACE", "pulse")
	Username      = os.Getenv("CASSANDRA_USERNAME") // empty => no auth (matches the no-auth compose)
	Password      = os.Getenv("CASSANDRA_PASSWORD")
	// Consistency is read at startup so experiment E1 can switch ONE/QUORUM/ALL
	// via env without recompiling.
	Consistency       = getenv("CASSANDRA_CONSISTENCY", "QUORUM")
	ReplicationFactor = intEnv("CASSANDRA_REPLICATION_FACTOR", 3)
)

// --- Detection thresholds (amounts in rupees unless noted) ---
const (
	Velocity1MinThreshold = 5   // V1: > this many txns in 1 min
	Velocity5MinThreshold = 15  // V1: > this many txns in 5 min
	ZScoreThreshold       = 3.0 // A1: std-devs above the account mean
	StructuringThreshold  = 50000.0
	StructuringMinCount   = 3     // S1: >= this many just-under-threshold txns today
	OddHourMinHistory     = 20    // T1: don't flag accounts with less history than this
	CardTestingMinCount   = 3     // C1: >= this many micro-txns in 1 min
	CardTestingMaxAvg     = 100.0 // C1: average below this (rupees) = card testing
	DormancyDays          = 30    // D1: silent for more than this many days
	DormancyMinAmount     = 1000.0
	HighRiskMinAmount     = 5000.0 // M1

	TxnTTLSeconds = 604800 // 7 days, on the transaction-history tables
)

// --- Producer ---
const (
	ProducerRatePerSec = 200
	FraudProbability   = 0.03 // chance per tick that a planted-fraud scenario runs
	WorkerPoolSize     = 8
	// A large pool keeps each account's legitimate rate realistically sparse, so
	// V1/G1 fire on genuine fraud bursts rather than on synthetic density.
	NumAccounts = 5000
)

// Window is a rolling velocity-counter window. `Seconds` is both the window
// length and the bucket size: a transaction at unix time u lands in
// bucket = u / Seconds, so each counter row covers exactly one window.
type Window struct {
	Name    string
	Seconds int64
}

// Windows are maintained on every write; V1 reads 1min/5min, C1 reads 1min.
var Windows = []Window{{"1min", 60}, {"5min", 300}}

// WindowSeconds returns the bucket size for a window name (default 60).
func WindowSeconds(name string) int64 {
	for _, w := range Windows {
		if w.Name == name {
			return w.Seconds
		}
	}
	return 60
}

var (
	HighRiskCategories = map[string]bool{"gambling": true, "crypto": true, "wire_transfer": true}
	Cities             = []string{"Chennai", "Mumbai", "Delhi", "Bengaluru", "Kolkata", "Hyderabad", "Pune"}
	MerchantCategories = []string{
		"groceries", "fuel", "dining", "electronics", "utilities", // common
		"travel", "jewelry", // occasional
		"gambling", "crypto", "wire_transfer", // rare, high-risk
	}
)

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func splitEnv(key, fallback string) []string {
	raw := getenv(key, fallback)
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	return out
}

func intEnv(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(strings.TrimSpace(v)); err == nil {
			return n
		}
	}
	return fallback
}
