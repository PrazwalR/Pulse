// Package producer generates a transaction stream (with planted fraud for ground
// truth) through a bounded goroutine worker pool, and evaluates rules inline.
package producer

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/apache/cassandra-gocql-driver/v2"
	"github.com/brianvoe/gofakeit/v7"
	"github.com/google/uuid"

	"pulse/internal/config"
	"pulse/internal/db"
	"pulse/internal/engine"
	"pulse/internal/model"
)

// Options configure a run; zero values fall back to config defaults.
type Options struct {
	Duration  time.Duration
	Rate      int     // transactions/sec paced by the ticker
	Workers   int     // goroutine pool size
	FraudProb float64 // probability per tick of planting a fraud scenario
	TruthPath string  // ground-truth JSONL (default ../ground_truth.jsonl)
}

func (o Options) withDefaults() Options {
	if o.Rate <= 0 {
		o.Rate = config.ProducerRatePerSec
	}
	if o.Workers <= 0 {
		o.Workers = config.WorkerPoolSize
	}
	if o.FraudProb <= 0 {
		o.FraudProb = config.FraudProbability
	}
	if o.TruthPath == "" {
		o.TruthPath = "../ground_truth.jsonl"
	}
	return o
}

func generateAccounts(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("ACC%05d", i)
	}
	return out
}

func cardOf(account string) string { return "CARD-" + account[len(account)-4:] }

func randCity() string { return config.Cities[rand.Intn(len(config.Cities))] }
func randCategory() string {
	return config.MerchantCategories[rand.Intn(len(config.MerchantCategories))]
}

func newTxn(account string, amount float64, city, category string, isFraud bool, label string) model.Transaction {
	return model.Transaction{
		AccountID:        account,
		CardID:           cardOf(account),
		TxnID:            uuid.New(),
		Timestamp:        time.Now().UTC(),
		Amount:           amount,
		Merchant:         gofakeit.Company(),
		MerchantCategory: category,
		City:             city,
		Channel:          "upi",
		IsFraud:          isFraud,
		Label:            label,
	}
}

// emit persists one transaction and evaluates rules inline. Ordering matters:
// velocity counters are incremented BEFORE evaluation (so V1/C1 count the
// current transaction), while hourly_activity and account_last_seen are written
// AFTER (so T1 and D1 compare against state as it was before this transaction).
func emit(s *gocql.Session, t model.Transaction) ([]model.Alert, error) {
	day := t.Timestamp.Format("2006-01-02")

	if err := s.Query(
		`INSERT INTO txn_by_account_day
		 (account_id, day, ts, txn_id, amount, merchant, merchant_category, city, channel, card_id)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		t.AccountID, day, t.Timestamp, gocql.UUID(t.TxnID), t.Amount, t.Merchant, t.MerchantCategory,
		t.City, t.Channel, t.CardID,
	).Exec(); err != nil {
		return nil, fmt.Errorf("history write: %w", err)
	}

	if err := s.Query(
		`INSERT INTO txn_by_card_hour
		 (card_id, hour, ts, txn_id, account_id, city, amount)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		t.CardID, t.Timestamp.Truncate(time.Hour), t.Timestamp, gocql.UUID(t.TxnID), t.AccountID, t.City, t.Amount,
	).Exec(); err != nil {
		return nil, fmt.Errorf("card write: %w", err)
	}

	// velocity counters (bucketed) — BEFORE evaluation. Amount in int paise.
	amountPaise := int64(math.Round(t.Amount * 100))
	for _, w := range config.Windows {
		bucket := t.Timestamp.Unix() / w.Seconds
		if err := s.Query(
			`UPDATE velocity_counters SET txn_count = txn_count + 1, total_amount = total_amount + ?
			 WHERE account_id = ? AND window = ? AND bucket = ?`,
			amountPaise, t.AccountID, w.Name, bucket,
		).Exec(); err != nil {
			return nil, fmt.Errorf("counter %s: %w", w.Name, err)
		}
	}

	alerts, err := engine.Evaluate(s, t)
	if err != nil {
		return alerts, err
	}

	// hourly_activity — AFTER T1 read it
	if err := s.Query(
		`UPDATE hourly_activity SET txn_count = txn_count + 1 WHERE account_id = ? AND hour_of_day = ?`,
		t.AccountID, t.Timestamp.Hour(),
	).Exec(); err != nil {
		return alerts, fmt.Errorf("hourly: %w", err)
	}

	// account_last_seen — AFTER D1 read it
	if err := s.Query(
		`INSERT INTO account_last_seen (account_id, last_txn_at) VALUES (?, ?)`,
		t.AccountID, t.Timestamp,
	).Exec(); err != nil {
		return alerts, fmt.Errorf("last-seen: %w", err)
	}
	return alerts, nil
}

// runWorkers starts a fixed-size pool of goroutines consuming from jobs — the
// bounded-concurrency pattern (INV-9). Never spawn one goroutine per transaction.
func runWorkers(s *gocql.Session, jobs <-chan model.Transaction, count int) *sync.WaitGroup {
	wg := &sync.WaitGroup{}
	for w := 0; w < count; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for t := range jobs {
				if _, err := emit(s, t); err != nil {
					log.Printf("worker %d: %v", id, err)
				}
			}
		}(w)
	}
	return wg
}

// Run drives the stream for opt.Duration, planting a fraud scenario with
// probability opt.FraudProb per tick and logging ground truth for experiment E6.
func Run(opt Options) error {
	opt = opt.withDefaults()

	s, err := db.Connect(db.DefaultConsistency())
	if err != nil {
		return err
	}
	defer s.Close()

	accounts := generateAccounts(config.NumAccounts)
	jobs := make(chan model.Transaction, 500)
	wg := runWorkers(s, jobs, opt.Workers)

	truth, _ := os.OpenFile(opt.TruthPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	logTruth := func(account, label string) {
		if truth == nil {
			return
		}
		line, _ := json.Marshal(map[string]string{
			"account_id": account, "label": label, "at": time.Now().UTC().Format(time.RFC3339),
		})
		_, _ = truth.Write(append(line, '\n'))
	}
	if truth != nil {
		defer truth.Close()
	}

	tick := time.NewTicker(time.Second / time.Duration(opt.Rate))
	defer tick.Stop()
	stop := time.After(opt.Duration)

	for {
		select {
		case <-stop:
			close(jobs)
			wg.Wait()
			return nil
		case <-tick.C:
			if rand.Float64() < opt.FraudProb {
				plantFraud(s, jobs, accounts, logTruth)
			} else {
				acct := accounts[rand.Intn(len(accounts))]
				jobs <- newTxn(acct, 100+rand.Float64()*3900, randCity(), randCategory(), false, "")
			}
		}
	}
}

// plantFraud pushes a known fraud scenario (ground truth for precision/recall).
func plantFraud(s *gocql.Session, jobs chan<- model.Transaction, accounts []string, logTruth func(string, string)) {
	acct := accounts[rand.Intn(len(accounts))]
	switch rand.Intn(8) {
	case 0: // V1 velocity: a burst of small-to-medium charges
		logTruth(acct, "velocity")
		for i := 0; i < 8; i++ {
			jobs <- newTxn(acct, 60+rand.Float64()*240, randCity(), randCategory(), true, "velocity")
		}
	case 1: // C1 card testing: a burst of micro charges (also trips V1)
		logTruth(acct, "card_testing")
		for i := 0; i < 6; i++ {
			jobs <- newTxn(acct, 1+rand.Float64()*40, randCity(), "groceries", true, "card_testing")
		}
	case 2: // G1 geo impossibility: same card, two far cities
		logTruth(acct, "geo")
		jobs <- newTxn(acct, 200+rand.Float64()*800, "Chennai", randCategory(), true, "geo")
		jobs <- newTxn(acct, 200+rand.Float64()*800, "Delhi", randCategory(), true, "geo")
	case 3: // S1 structuring: several charges just under the threshold
		logTruth(acct, "structuring")
		for i := 0; i < 4; i++ {
			jobs <- newTxn(acct, 45000+rand.Float64()*4900, randCity(), randCategory(), true, "structuring")
		}
	case 4: // M1 high-risk merchant: one large charge at a risky category
		logTruth(acct, "high_risk")
		cat := []string{"gambling", "crypto", "wire_transfer"}[rand.Intn(3)]
		jobs <- newTxn(acct, 6000+rand.Float64()*40000, randCity(), cat, true, "high_risk")
	case 5: // A1 amount anomaly: one charge far above normal (fires once a baseline exists)
		logTruth(acct, "amount")
		jobs <- newTxn(acct, 80000+rand.Float64()*220000, randCity(), randCategory(), true, "amount")
	case 6: // D1 dormant reactivation: seed an old last-seen, then a large charge
		logTruth(acct, "dormant")
		old := time.Now().UTC().Add(-time.Duration(40*24) * time.Hour)
		_ = s.Query(`INSERT INTO account_last_seen (account_id, last_txn_at) VALUES (?, ?)`, acct, old).Exec()
		jobs <- newTxn(acct, 1500+rand.Float64()*20000, randCity(), randCategory(), true, "dormant")
	default: // T1 odd-hour: seed history at OTHER hours, then transact at an unused one
		logTruth(acct, "odd_hour")
		h := time.Now().UTC().Hour()
		for k := 1; k <= 7; k++ { // 7 * 3 = 21 prior txns, none in the current hour
			_ = s.Query(
				`UPDATE hourly_activity SET txn_count = txn_count + 3 WHERE account_id = ? AND hour_of_day = ?`,
				acct, (h+k)%24).Exec()
		}
		jobs <- newTxn(acct, 100+rand.Float64()*3900, randCity(), randCategory(), true, "odd_hour")
	}
}
