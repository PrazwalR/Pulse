# Benchmarks — E1, E2, E6

Measured on a local 3-node Apache Cassandra 5.0 cluster (RF 3), driven by the Go
producer. Charts are regenerated from the raw CSVs with `python3 benchmarks/chart.py`
(standard library only — no external chart dependency).

Build the producer binary first (from `backend/`): `go build -o bin/producer ./cmd/producer`.

## E1 — Consistency vs throughput
`data/e1_consistency.csv` · `charts/e1_consistency.svg`

Same offered load at each consistency level (32 workers, 15 s, all nodes up):

| CL | writes/sec | p50 | p99 | survives 1 node down |
|----|-----------:|----:|----:|:--------------------:|
| ONE | 1551 | 19.9 ms | 34.4 ms | yes |
| QUORUM | 797 | 39.0 ms | 65.3 ms | yes |
| ALL | 659 | 47.0 ms | 78.0 ms | no |

```bash
cd backend
for CL in ONE QUORUM ALL; do
  CASSANDRA_CONSISTENCY=$CL ./bin/producer -duration 15s -rate 5000 -workers 32 -fraud-prob 0.001
done   # read the SUMMARY line (emitted / wps / p50 / p99)
```

## E2 — Node-kill / CAP
`data/e2_quorum.csv`, `data/e2_all.csv` · `charts/e2_node_kill.svg`

The producer streams while `cass3` is stopped mid-run and later restarted. Same
cluster, same failure — only the consistency level differs:

- **QUORUM** — `failed = 0` throughout; stays available, zero data loss.
- **ALL** — every write fails while `cass3` is down (needs 3/3 acks); availability
  is lost, then recovers the instant the node returns.

```bash
cd backend
CASSANDRA_CONSISTENCY=QUORUM ./bin/producer -duration 85s -rate 600 -workers 16 \
  -metrics ../benchmarks/data/e2_quorum.csv &
sleep 25; docker stop cass3; sleep 35; docker start cass3; wait
# repeat with CASSANDRA_CONSISTENCY=ALL (shorter) for the contrast
```

## E6 — Detection quality (precision / recall)
```bash
cd backend
go run ./cmd/quality   # joins planted ground truth against the alerts actually written
```

## Regenerate charts
```bash
python3 benchmarks/chart.py   # data/*.csv -> charts/*.svg
```
