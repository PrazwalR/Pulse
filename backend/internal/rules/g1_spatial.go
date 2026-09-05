package rules

import (
	"fmt"
	"time"

	"github.com/apache/cassandra-gocql-driver/v2"

	"pulse/internal/model"
)

// minTravelHours: fastest realistic travel time between city pairs. Deliberately
// conservative — an unknown pair is never flagged, so we err toward fewer false
// positives. Symmetric: looked up in both orders.
var minTravelHours = map[[2]string]float64{
	{"Chennai", "Mumbai"}:      2.0,
	{"Chennai", "Delhi"}:       2.5,
	{"Chennai", "Bengaluru"}:   1.0,
	{"Chennai", "Hyderabad"}:   1.0,
	{"Chennai", "Kolkata"}:     2.5,
	{"Chennai", "Pune"}:        1.5,
	{"Mumbai", "Delhi"}:        2.0,
	{"Mumbai", "Bengaluru"}:    1.5,
	{"Mumbai", "Hyderabad"}:    1.5,
	{"Mumbai", "Kolkata"}:      2.5,
	{"Mumbai", "Pune"}:         0.5,
	{"Delhi", "Bengaluru"}:     2.5,
	{"Delhi", "Hyderabad"}:     2.0,
	{"Delhi", "Kolkata"}:       2.0,
	{"Delhi", "Pune"}:          2.0,
	{"Bengaluru", "Hyderabad"}: 1.0,
	{"Bengaluru", "Kolkata"}:   2.5,
	{"Bengaluru", "Pune"}:      1.0,
	{"Hyderabad", "Kolkata"}:   2.0,
	{"Hyderabad", "Pune"}:      1.5,
	{"Kolkata", "Pune"}:        2.5,
}

func minTravel(a, b string) (float64, bool) {
	if a == b {
		return 0, true
	}
	if h, ok := minTravelHours[[2]string{a, b}]; ok {
		return h, true
	}
	if h, ok := minTravelHours[[2]string{b, a}]; ok {
		return h, true
	}
	return 0, false
}

// G1 Spatial impossibility: the same card seen in two cities faster than travel
// between them allows. Reads the current and previous hour buckets (a "last hour"
// window can straddle two) and checks every pair of distinct-city sightings.
func G1SpatialImpossibility(s *gocql.Session, t model.Transaction) ([]model.Alert, error) {
	type sighting struct {
		city string
		ts   time.Time
	}
	var sightings []sighting
	now := t.Timestamp

	for _, bucket := range []time.Time{
		now.Add(-time.Hour).Truncate(time.Hour),
		now.Truncate(time.Hour),
	} {
		iter := s.Query(
			`SELECT city, ts FROM txn_by_card_hour WHERE card_id = ? AND hour = ?`,
			t.CardID, bucket).Iter()
		var city string
		var ts time.Time
		for iter.Scan(&city, &ts) {
			if now.Sub(ts) <= time.Hour {
				sightings = append(sightings, sighting{city, ts})
			}
		}
		if err := iter.Close(); err != nil {
			return nil, fmt.Errorf("g1 read: %w", err)
		}
	}

	for i := 0; i < len(sightings); i++ {
		for j := i + 1; j < len(sightings); j++ {
			a, b := sightings[i], sightings[j]
			if a.city == b.city {
				continue
			}
			elapsed := b.ts.Sub(a.ts)
			if elapsed < 0 {
				elapsed = -elapsed
			}
			minHrs, known := minTravel(a.city, b.city)
			if !known || elapsed.Hours() >= minHrs {
				continue
			}
			return []model.Alert{newAlert(t, "geo_impossible", "high",
				fmt.Sprintf("%s -> %s in %.1fh (min %.1fh)",
					a.city, b.city, elapsed.Hours(), minHrs))}, nil
		}
	}
	return nil, nil
}
