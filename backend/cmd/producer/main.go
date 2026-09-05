// Command producer runs the transaction stream and evaluates rules inline.
package main

import (
	"flag"
	"log"
	"time"

	"pulse/internal/producer"
)

func main() {
	dur := flag.Duration("duration", 10*time.Minute, "how long to run")
	rate := flag.Int("rate", 0, "transactions/sec (0 = config default)")
	workers := flag.Int("workers", 0, "worker pool size (0 = config default)")
	fraud := flag.Float64("fraud-prob", 0, "fraud probability per tick (0 = config default)")
	flag.Parse()

	log.Printf("PULSE producer starting (duration=%s)...", *dur)
	if err := producer.Run(producer.Options{
		Duration:  *dur,
		Rate:      *rate,
		Workers:   *workers,
		FraudProb: *fraud,
	}); err != nil {
		log.Fatalf("producer: %v", err)
	}
	log.Println("done.")
}
