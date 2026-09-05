// Command schema creates the keyspace and all tables. Run once after the
// cluster is up (idempotent — safe to re-run).
package main

import (
	"fmt"
	"log"

	"github.com/apache/cassandra-gocql-driver/v2"

	"pulse/internal/config"
	"pulse/internal/db"
)

func tableDDL() []string {
	ttl := config.TxnTTLSeconds
	twcs := "{'class':'TimeWindowCompactionStrategy','compaction_window_unit':'HOURS','compaction_window_size':6}"
	return []string{
		// 1) full history, bucketed by (account, day) so partitions stay bounded
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS pulse.txn_by_account_day (
			account_id text, day date, ts timestamp, txn_id uuid,
			amount double, merchant text, merchant_category text,
			city text, channel text, card_id text,
			PRIMARY KEY ((account_id, day), ts, txn_id)
		) WITH CLUSTERING ORDER BY (ts DESC, txn_id ASC)
		  AND default_time_to_live = %d AND compaction = %s`, ttl, twcs),

		// 2) same transactions keyed by (card, hour) for the spatial rule (G1)
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS pulse.txn_by_card_hour (
			card_id text, hour timestamp, ts timestamp, txn_id uuid,
			account_id text, city text, amount double,
			PRIMARY KEY ((card_id, hour), ts, txn_id)
		) WITH CLUSTERING ORDER BY (ts DESC, txn_id ASC)
		  AND default_time_to_live = %d`, ttl),

		// 3) rolling per-account counters (V1, C1). Counter columns cannot mix
		// with non-counter columns, and total_amount is int paise. The `bucket`
		// clustering column is the key fix: a plain counter never resets (no TTL
		// on counters), so it would be a lifetime total, not a window. Bucketing
		// by floor(unix_ts / window_seconds) makes each row a real time window.
		`CREATE TABLE IF NOT EXISTS pulse.velocity_counters (
			account_id text, window text, bucket bigint,
			txn_count counter, total_amount counter,
			PRIMARY KEY ((account_id), window, bucket))`,

		// 4) behavioural baseline (A1), recomputed by cmd/baseline
		`CREATE TABLE IF NOT EXISTS pulse.account_baseline (
			account_id text PRIMARY KEY,
			avg_amount double, stddev_amount double,
			normal_city text, normal_txn_per_day double,
			updated_at timestamp)`,

		// 5) alerts raised by the rule engine, per account (full history)
		`CREATE TABLE IF NOT EXISTS pulse.alerts_by_account (
			account_id text, raised_at timestamp, alert_id uuid,
			rule text, severity text, detail text, txn_id uuid,
			PRIMARY KEY (account_id, raised_at, alert_id))
		  WITH CLUSTERING ORDER BY (raised_at DESC, alert_id ASC)`,

		// 5b) alerts bucketed by hour so the dashboard reads a bounded partition
		// ("recent alerts across all accounts") instead of scanning every account.
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS pulse.alerts_recent (
			bucket text, raised_at timestamp, alert_id uuid,
			account_id text, rule text, severity text, detail text,
			PRIMARY KEY ((bucket), raised_at, alert_id))
		  WITH CLUSTERING ORDER BY (raised_at DESC, alert_id ASC)
		  AND default_time_to_live = %d AND compaction = %s`, ttl, twcs),

		// 6) per-account transactions-per-hour-of-day (T1). No TTL: the "has this
		// account ever used this hour" signal must outlive the 7-day history.
		`CREATE TABLE IF NOT EXISTS pulse.hourly_activity (
			account_id text, hour_of_day int, txn_count counter,
			PRIMARY KEY (account_id, hour_of_day))`,

		// 7) last-seen per account (D1). No TTL, for the same reason.
		`CREATE TABLE IF NOT EXISTS pulse.account_last_seen (
			account_id text PRIMARY KEY, last_txn_at timestamp)`,
	}
}

func main() {
	// keyspace first, on a session with no keyspace bound
	sys, err := db.ConnectSystem(gocql.Quorum)
	if err != nil {
		log.Fatalf("system connect: %v", err)
	}
	ks := fmt.Sprintf(
		`CREATE KEYSPACE IF NOT EXISTS pulse WITH replication =
		 {'class':'SimpleStrategy','replication_factor':%d}`, config.ReplicationFactor)
	if err := sys.Query(ks).Exec(); err != nil {
		log.Fatalf("keyspace: %v", err)
	}
	sys.Close()

	// then the tables, bound to the keyspace
	s, err := db.Connect(gocql.Quorum)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer s.Close()

	ddl := tableDDL()
	for i, stmt := range ddl {
		if err := s.Query(stmt).Exec(); err != nil {
			log.Fatalf("table %d: %v\n%s", i+1, err, stmt)
		}
		fmt.Printf("applied table %d/%d\n", i+1, len(ddl))
	}
	fmt.Println("schema ready")
}
