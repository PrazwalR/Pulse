# AGENTS.md — PULSE invariants

Paste this into any AI coding assistant before asking for code. Without it, an
assistant will confidently write SQL-shaped queries that Cassandra rejects.

## Project

PULSE: a real-time payment-fraud detection engine on **Apache Cassandra 5.0**,
with the ingestion service and rule engine in **Go** (`github.com/apache/cassandra-gocql-driver/v2`)
and a read-only **Streamlit** dashboard. Coursework for a NoSQL Databases module —
code must be explainable in a viva, not just functional.

- Go module root: `backend/` (module `pulse`). Run `go` commands from there.
- Dashboard: `frontend/app.py`. Cluster: `extra/docker-compose.yml` (3 nodes, RF 3).
- Eight rules live in `backend/internal/rules/` (one file each); the engine
  (`internal/engine`) runs them inline after every write and persists alerts.

## Hard invariants (violating these is a bug, not a style issue)

- **INV-1 Query-first, always.** Never design a table before stating the exact
  query it serves. No joins, no ad-hoc queries, no cross-partition aggregation.
- **INV-2 Every hot-path query hits one partition.** A read specifies the full
  partition key. `ALLOW FILTERING` in the hot path is forbidden — it means the
  table design is wrong. (The dashboard's `PER PARTITION LIMIT` scan is the one
  allowed exception: read-only, off the hot path.)
- **INV-3 Bound every partition.** Partition keys include a time bucket
  (day/hour/window bucket) so partitions never grow without limit. `account_id`
  alone as a partition key is a bug.
- **INV-4 Consistency is explicit.** Sessions set consistency explicitly
  (default QUORUM, env-overridable via `CASSANDRA_CONSISTENCY` for experiment E1).
- **INV-5 Counters are integer-only and cannot mix with regular columns.** A
  counter table's non-key columns are all counters. Amounts in counter tables are
  int64 **paise**, never float. `velocity_counters` is time-bucketed
  (`PRIMARY KEY ((account_id), window, bucket)`, `bucket = unix_ts / window_seconds`)
  because a plain counter never resets (no TTL on counters) and would be a
  lifetime total, not a rolling window.
- **INV-6 No updates/deletes of transaction history.** Append-only; use TTL
  (7 days) for expiry, never DELETE. Note `hourly_activity` and
  `account_last_seen` deliberately have **no** TTL — their signals must outlive
  the history window.
- **INV-7 Parameterised queries only.** Always `?`-bound arguments, never
  string-formatted CQL with data. gocql caches prepared statements per query
  string automatically.
- **INV-8 Denormalization is correct, not a smell.** The same transaction is
  written to `txn_by_account_day` and `txn_by_card_hour` on purpose. Do not
  "refactor" into one table with joins.
- **INV-9 Concurrency goes through the bounded worker pool.** Producer writes
  flow through a fixed-size goroutine pool consuming a buffered channel. Never
  `go emit(...)` per transaction.
- **INV-10 Always check and close `gocql.Iter`.** Every `.Iter()` is drained via
  `Scan` and finished with `iter.Close()`, whose error is checked.
- **INV-11 Timestamps are always UTC.** Every `time.Now()` feeding a Cassandra
  timestamp uses `.UTC()`. Local time silently corrupts every rolling-window
  calculation.
- **INV-12 Evaluation ordering (rule-engine specific).** In `producer.emit`,
  increment `velocity_counters` BEFORE `engine.Evaluate` (so V1/C1 count the
  current transaction), but write `hourly_activity` and `account_last_seen`
  AFTER (so T1 and D1 compare against state as it was before this transaction).

## Types & schema notes

- Amounts in the history/baseline tables are CQL **`double`** (maps cleanly to Go
  `float64`), not `decimal` (which gocql surfaces as `inf.Dec`).
- The eight rules and their tables:
  V1 velocity / C1 card-testing → `velocity_counters`; A1 amount → `account_baseline`;
  G1 spatial → `txn_by_card_hour` + a static travel-time table; S1 structuring →
  `txn_by_account_day` (today's partition); T1 odd-hour → `hourly_activity`;
  D1 dormant → `account_last_seen`; M1 high-risk → the transaction's own category.
- Alerts are written to `alerts_by_account` (per-account history) and
  `alerts_recent` (hour-bucketed) so the dashboard reads a bounded partition
  instead of scanning every account.
- The synthetic producer models each account with a stable **home city**; only
  the geo-fraud scenario crosses cities. This keeps G1 trustworthy — a normal
  account never appears in two cities within an hour. Real deployments would
  tolerate genuine travel, which G1's conservative travel-time table handles.

## Common AI failure modes on this project

1. SQL-style JOINs or GROUP BY across partitions — impossible in Cassandra.
2. Suggesting `ALLOW FILTERING` to make a broken query "work".
3. `account_id` alone as a partition key (unbounded partition).
4. A counter column beside regular columns — rejected at schema creation.
5. A plain (non-bucketed) counter used as a "rolling window" — never resets.
6. Modelling one table and querying it many different ways.
7. Secondary indexes to avoid denormalization — prefer a purpose-built table.
8. One goroutine per transaction with no bound.
9. `float64`/`decimal` for a counter-table amount (must be int64 paise).
10. Forgetting `.UTC()` on `time.Now()`.
11. Not checking `iter.Close()`.
12. Incrementing `hourly_activity`/`account_last_seen` before evaluating T1/D1.
