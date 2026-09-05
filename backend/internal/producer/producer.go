// Package producer generates a synthetic transaction stream with planted fraud
// (ground truth for precision/recall) through a bounded goroutine worker pool,
// evaluating the rules inline on every write. It is the system's test data
// source; there is no real payment feed.
package producer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
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
	Duration    time.Duration
	Rate        int
	Workers     int
	FraudProb   float64
	TruthPath   string
	MetricsPath string // if set, per-second throughput is written here as CSV
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

// homeCity gives each account a stable home city derived from its id. Modelling
// accounts as single-city is what makes the spatial rule (G1) trustworthy: a
// normal account never appears in two cities within an hour, so G1 fires only on
// the geo-fraud scenario, not on synthetic randomness.
func homeCity(account string) string {
	n := 0
	if len(account) > 3 {
		n, _ = strconv.Atoi(account[3:])
	}
	return config.Cities[n%len(config.Cities)]
}

func randCategory() string {
	return config.MerchantCategories[rand.Intn(len(config.MerchantCategories))]
}

// safeCategory returns a non-high-risk category, so a large planted charge for a
// non-M1 scenario does not also trip the high-risk-merchant rule.
func safeCategory() string {
	for {
		if c := randCategory(); !config.HighRiskCategories[c] {
			return c
		}
	}
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

// emit persists one transaction and evaluates the rules inline. Ordering is
// load-bearing: velocity counters are incremented BEFORE evaluation (so V1/C1
// count the current transaction), while hourly_activity and account_last_seen
// are written AFTER (so T1 and D1 compare against state as it was before it).
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

	if err := s.Query(
		`UPDATE hourly_activity SET txn_count = txn_count + 1 WHERE account_id = ? AND hour_of_day = ?`,
		t.AccountID, t.Timestamp.Hour(),
	).Exec(); err != nil {
		return alerts, fmt.Errorf("hourly: %w", err)
	}

	if err := s.Query(
		`INSERT INTO account_last_seen (account_id, last_txn_at) VALUES (?, ?)`,
		t.AccountID, t.Timestamp,
	).Exec(); err != nil {
		return alerts, fmt.Errorf("last-seen: %w", err)
	}
	return alerts, nil
}

// stat holds live throughput counters shared across the worker pool.
type stat struct {
	emitted atomic.Int64
	failed  atomic.Int64
}

// runWorkers starts a fixed-size pool of goroutines consuming from jobs — the
// bounded-concurrency pattern. Never spawn one goroutine per transaction. Each
// worker records its own emit latencies into lat[id] (no lock contention).
func runWorkers(s *gocql.Session, jobs <-chan model.Transaction, count int, st *stat, lat [][]time.Duration) *sync.WaitGroup {
	wg := &sync.WaitGroup{}
	for w := 0; w < count; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for t := range jobs {
				start := time.Now()
				if _, err := emit(s, t); err != nil {
					st.failed.Add(1)
					log.Printf("worker %d: %v", id, err)
					continue
				}
				st.emitted.Add(1)
				lat[id] = append(lat[id], time.Since(start))
			}
		}(w)
	}
	return wg
}

func quantile(sorted []time.Duration, q float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(q * float64(len(sorted)))
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func summarize(st *stat, lat [][]time.Duration, elapsed time.Duration) {
	var all []time.Duration
	for _, s := range lat {
		all = append(all, s...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	secs := elapsed.Seconds()
	if secs <= 0 {
		secs = 1
	}
	log.Printf("SUMMARY emitted=%d failed=%d wps=%.0f p50=%s p99=%s over=%.1fs",
		st.emitted.Load(), st.failed.Load(), float64(st.emitted.Load())/secs,
		quantile(all, 0.50).Round(time.Microsecond),
		quantile(all, 0.99).Round(time.Microsecond), secs)
}

// Run drives the stream until the duration elapses or an interrupt arrives,
// planting a fraud scenario with probability opt.FraudProb per tick.
func Run(opt Options) error {
	opt = opt.withDefaults()

	s, err := db.Connect(db.DefaultConsistency())
	if err != nil {
		return err
	}
	defer s.Close()

	accounts := generateAccounts(config.NumAccounts)
	jobs := make(chan model.Transaction, 500)
	st := &stat{}
	lat := make([][]time.Duration, opt.Workers)
	wg := runWorkers(s, jobs, opt.Workers, st, lat)

	truth, err := os.OpenFile(opt.TruthPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open ground-truth file: %w", err)
	}
	defer truth.Close()
	logTruth := func(account, label string) {
		line, _ := json.Marshal(map[string]string{
			"account_id": account, "label": label, "at": time.Now().UTC().Format(time.RFC3339),
		})
		if _, werr := truth.Write(append(line, '\n')); werr != nil {
			log.Printf("ground-truth write: %v", werr)
		}
	}

	ctx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()

	runStart := time.Now()
	reportStop := make(chan struct{})
	go report(st, opt.MetricsPath, runStart, reportStop)

	interval := time.Second / time.Duration(opt.Rate)
	if interval <= 0 {
		interval = time.Nanosecond
	}
	tick := time.NewTicker(interval)
	defer tick.Stop()
	deadline := time.After(opt.Duration)

	drain := func() error {
		close(jobs)
		wg.Wait()
		close(reportStop)
		summarize(st, lat, time.Since(runStart))
		return nil
	}
	for {
		select {
		case <-ctx.Done():
			log.Println("interrupt received; draining workers...")
			return drain()
		case <-deadline:
			return drain()
		case <-tick.C:
			if rand.Float64() < opt.FraudProb {
				plantFraud(s, jobs, accounts, logTruth)
			} else {
				acct := accounts[rand.Intn(len(accounts))]
				jobs <- newTxn(acct, 100+rand.Float64()*3900, homeCity(acct), randCategory(), false, "")
			}
		}
	}
}

// report logs per-second throughput and, when metricsPath is set, appends it as
// CSV — the raw data for the E1/E2 experiment charts.
func report(st *stat, metricsPath string, start time.Time, stop <-chan struct{}) {
	var csv *os.File
	if metricsPath != "" {
		if f, err := os.Create(metricsPath); err == nil {
			csv = f
			fmt.Fprintln(csv, "elapsed_sec,emitted_per_sec,failed_per_sec")
			defer csv.Close()
		} else {
			log.Printf("metrics file: %v", err)
		}
	}
	t := time.NewTicker(time.Second)
	defer t.Stop()
	var lastE, lastF int64
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			e, f := st.emitted.Load(), st.failed.Load()
			de, df := e-lastE, f-lastF
			lastE, lastF = e, f
			elapsed := int(time.Since(start).Seconds())
			log.Printf("t=%02ds emitted/s=%d failed/s=%d", elapsed, de, df)
			if csv != nil {
				fmt.Fprintf(csv, "%d,%d,%d\n", elapsed, de, df)
			}
		}
	}
}

func seed(s *gocql.Session, what, stmt string, args ...interface{}) {
	if err := s.Query(stmt, args...).Exec(); err != nil {
		log.Printf("seed %s: %v", what, err)
	}
}

// plantFraud pushes one known fraud scenario. Each scenario is shaped to trip
// only its intended rule (home city, safe category, and seeded prerequisites),
// so the ground truth supports meaningful per-rule precision/recall.
func plantFraud(s *gocql.Session, jobs chan<- model.Transaction, accounts []string, logTruth func(string, string)) {
	acct := accounts[rand.Intn(len(accounts))]
	city := homeCity(acct)
	switch rand.Intn(8) {
	case 0: // V1 velocity: a burst of small-to-medium charges
		logTruth(acct, "velocity")
		for i := 0; i < 8; i++ {
			jobs <- newTxn(acct, 60+rand.Float64()*240, city, randCategory(), true, "velocity")
		}
	case 1: // C1 card testing: a burst of micro charges
		logTruth(acct, "card_testing")
		for i := 0; i < 6; i++ {
			jobs <- newTxn(acct, 1+rand.Float64()*40, city, "groceries", true, "card_testing")
		}
	case 2: // G1 geo impossibility: same card in two far cities within the hour
		logTruth(acct, "geo")
		jobs <- newTxn(acct, 200+rand.Float64()*800, "Chennai", safeCategory(), true, "geo")
		jobs <- newTxn(acct, 200+rand.Float64()*800, "Delhi", safeCategory(), true, "geo")
	case 3: // S1 structuring: several charges just under the threshold
		logTruth(acct, "structuring")
		for i := 0; i < 4; i++ {
			jobs <- newTxn(acct, 45000+rand.Float64()*4900, city, safeCategory(), true, "structuring")
		}
	case 4: // M1 high-risk merchant: one large charge at a risky category
		logTruth(acct, "high_risk")
		cat := []string{"gambling", "crypto", "wire_transfer"}[rand.Intn(3)]
		jobs <- newTxn(acct, 6000+rand.Float64()*40000, city, cat, true, "high_risk")
	case 5: // A1 amount anomaly: establish a normal profile, then a charge far above it
		logTruth(acct, "amount")
		seed(s, "baseline",
			`INSERT INTO account_baseline (account_id, avg_amount, stddev_amount, normal_city, normal_txn_per_day, updated_at)
			 VALUES (?, ?, ?, ?, ?, ?)`,
			acct, 2000.0, 700.0, city, 5.0, time.Now().UTC())
		jobs <- newTxn(acct, 80000+rand.Float64()*220000, city, safeCategory(), true, "amount")
	case 6: // D1 dormant reactivation: seed an old last-seen, then a large charge
		logTruth(acct, "dormant")
		seed(s, "last-seen",
			`INSERT INTO account_last_seen (account_id, last_txn_at) VALUES (?, ?)`,
			acct, time.Now().UTC().Add(-40*24*time.Hour))
		jobs <- newTxn(acct, 1500+rand.Float64()*20000, city, safeCategory(), true, "dormant")
	default: // T1 odd-hour: seed history at OTHER hours, then transact at an unused one
		logTruth(acct, "odd_hour")
		h := time.Now().UTC().Hour()
		for k := 1; k <= 7; k++ { // 7*3 = 21 prior txns, none in the current hour
			seed(s, "hourly",
				`UPDATE hourly_activity SET txn_count = txn_count + 3 WHERE account_id = ? AND hour_of_day = ?`,
				acct, (h+k)%24)
		}
		jobs <- newTxn(acct, 100+rand.Float64()*3900, city, randCategory(), true, "odd_hour")
	}
}
