// Command baseline recomputes each account's spending profile (avg, std-dev,
// usual city) from recent history, feeding rule A1. Run it periodically.
package main

import (
	"fmt"
	"log"
	"math"
	"time"

	"pulse/internal/config"
	"pulse/internal/db"
)

func mostFrequent(counts map[string]int) string {
	best, bestN := "", -1
	for city, n := range counts {
		if n > bestN {
			best, bestN = city, n
		}
	}
	return best
}

func main() {
	s, err := db.Connect(db.DefaultConsistency())
	if err != nil {
		log.Fatal(err)
	}
	defer s.Close()

	now := time.Now().UTC()
	days := []string{
		now.Format("2006-01-02"),
		now.AddDate(0, 0, -1).Format("2006-01-02"),
	}

	updated := 0
	for i := 0; i < config.NumAccounts; i++ {
		account := fmt.Sprintf("ACC%05d", i)

		var amounts []float64
		cities := map[string]int{}
		for _, day := range days {
			iter := s.Query(
				`SELECT amount, city FROM txn_by_account_day WHERE account_id = ? AND day = ?`,
				account, day).Iter()
			var amt float64
			var city string
			for iter.Scan(&amt, &city) {
				amounts = append(amounts, amt)
				cities[city]++
			}
			if err := iter.Close(); err != nil {
				log.Fatalf("read %s: %v", account, err)
			}
		}
		if len(amounts) < 2 {
			continue // not enough history to characterise
		}

		var sum float64
		for _, a := range amounts {
			sum += a
		}
		mean := sum / float64(len(amounts))
		var variance float64
		for _, a := range amounts {
			d := a - mean
			variance += d * d
		}
		stddev := math.Sqrt(variance / float64(len(amounts)))
		perDay := float64(len(amounts)) / float64(len(days))

		if err := s.Query(
			`INSERT INTO account_baseline
			 (account_id, avg_amount, stddev_amount, normal_city, normal_txn_per_day, updated_at)
			 VALUES (?, ?, ?, ?, ?, ?)`,
			account, mean, stddev, mostFrequent(cities), perDay, now,
		).Exec(); err != nil {
			log.Fatalf("write %s: %v", account, err)
		}
		updated++
	}
	fmt.Printf("baselines updated for %d accounts\n", updated)
}
