// Command quality measures detection quality (experiment E6): it joins the
// producer's planted ground truth against the alerts the engine actually wrote
// and reports account-level precision and recall per fraud type. It reads real
// data from Cassandra — no numbers are fabricated.
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"

	"pulse/internal/db"
)

var ruleToLabel = map[string]string{
	"velocity_1min":        "velocity",
	"velocity_5min":        "velocity",
	"card_testing":         "card_testing",
	"geo_impossible":       "geo",
	"structuring":          "structuring",
	"high_risk_merchant":   "high_risk",
	"amount_anomaly":       "amount",
	"dormant_reactivation": "dormant",
	"odd_hour":             "odd_hour",
}

var labelOrder = []string{"velocity", "card_testing", "geo", "structuring", "high_risk", "amount", "dormant", "odd_hour"}

func ratio(a, b int) float64 {
	if b == 0 {
		return 0
	}
	return float64(a) / float64(b)
}

func main() {
	truthPath := flag.String("truth", "../ground_truth.jsonl", "ground-truth JSONL path")
	flag.Parse()

	planted := loadPlanted(*truthPath)
	alerted, ruleCounts := loadAlerts()

	fmt.Println("=== event-level alert counts (per rule) ===")
	rules := make([]string, 0, len(ruleCounts))
	for r := range ruleCounts {
		rules = append(rules, r)
	}
	sort.Strings(rules)
	for _, r := range rules {
		fmt.Printf("  %-20s %d\n", r, ruleCounts[r])
	}

	fmt.Println("\n=== account-level precision / recall (per fraud type) ===")
	fmt.Printf("  %-13s %8s %6s %6s %6s %11s %8s\n", "type", "planted", "TP", "FP", "FN", "precision", "recall")
	for _, label := range labelOrder {
		plantedAccts := accountsWith(planted, label)
		alertedAccts := accountsWith(alerted, label)
		tp, fp, fn := 0, 0, 0
		for acct := range alertedAccts {
			if plantedAccts[acct] {
				tp++
			} else {
				fp++
			}
		}
		for acct := range plantedAccts {
			if !alertedAccts[acct] {
				fn++
			}
		}
		fmt.Printf("  %-13s %8d %6d %6d %6d %10.1f%% %7.1f%%\n",
			label, len(plantedAccts), tp, fp, fn, ratio(tp, tp+fp)*100, ratio(tp, tp+fn)*100)
	}
}

func accountsWith(m map[string]map[string]bool, label string) map[string]bool {
	out := map[string]bool{}
	for acct, labels := range m {
		if labels[label] {
			out[acct] = true
		}
	}
	return out
}

func loadPlanted(path string) map[string]map[string]bool {
	f, err := os.Open(path)
	if err != nil {
		log.Fatalf("open ground truth %q: %v", path, err)
	}
	defer f.Close()

	planted := map[string]map[string]bool{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var rec struct {
			AccountID string `json:"account_id"`
			Label     string `json:"label"`
		}
		if err := json.Unmarshal(sc.Bytes(), &rec); err != nil || rec.AccountID == "" {
			continue
		}
		if planted[rec.AccountID] == nil {
			planted[rec.AccountID] = map[string]bool{}
		}
		planted[rec.AccountID][rec.Label] = true
	}
	if err := sc.Err(); err != nil {
		log.Fatalf("read ground truth: %v", err)
	}
	return planted
}

func loadAlerts() (map[string]map[string]bool, map[string]int) {
	s, err := db.Connect(db.DefaultConsistency())
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer s.Close()

	alerted := map[string]map[string]bool{}
	ruleCounts := map[string]int{}
	// Offline analysis: a full range scan over alerts_by_account is acceptable
	// here (not the hot path).
	iter := s.Query(`SELECT account_id, rule FROM alerts_by_account`).Iter()
	var account, rule string
	for iter.Scan(&account, &rule) {
		ruleCounts[rule]++
		label, ok := ruleToLabel[rule]
		if !ok {
			continue
		}
		if alerted[account] == nil {
			alerted[account] = map[string]bool{}
		}
		alerted[account][label] = true
	}
	if err := iter.Close(); err != nil {
		log.Fatalf("scan alerts: %v", err)
	}
	return alerted, ruleCounts
}
