package main

import (
	"fmt"
	"log"
	"message-broker-benchmark/internal/broker/kafka"

	"message-broker-benchmark/internal/benchmark"
)

func main() {

	scenarios := []benchmark.Scenario{
		{"low-load", 10, 1, 10, 2},
	}

	var results []benchmark.Result

	for _, sc := range scenarios {

		for run := 1; run <= sc.Runs; run++ {

			fmt.Println("Running:", sc.Name, "Run:", run)

			b := &kafka.KafkaBroker{}
			// b.Connect() is handled inside benchmark.Run
			t, avg, p50, p95, p99 := benchmark.Run(b, sc)

			results = append(results, benchmark.Result{
				Broker:        "kafka",
				Scenario:      sc.Name,
				TotalMessages: sc.TotalMessages,
				Producers:     sc.Producers,
				MessageSize:   sc.MessageSize,
				Run:           run,
				Throughput:    t,
				AvgLatency:    avg,
				P50:           p50,
				P95:           p95,
				P99:           p99,
			})
		}
	}

	err := benchmark.WriteCSV("results/kafka_results.csv", results)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Results saved to results/kafka_results.csv")
}
