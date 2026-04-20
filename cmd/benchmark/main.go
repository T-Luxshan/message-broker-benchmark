package main

import (
	"fmt"
	"log"

	"message-broker-benchmark/internal/benchmark"
	"message-broker-benchmark/internal/broker/rabbitmq"
)

func main() {

	scenarios := []benchmark.Scenario{
		{"low-load", 1000, 1, 256, 2},
		{"medium-load", 1000, 5, 256, 2},
		{"high-load", 1000, 10, 256, 2},
		{"large-msg", 1000, 10, 1024, 2},
	}

	var results []benchmark.Result

	for _, sc := range scenarios {

		for run := 1; run <= sc.Runs; run++ {

			fmt.Println("Running:", sc.Name, "Run:", run)

			b := &rabbitmq.RabbitMQ{}

			t, avg, p50, p95, p99 := benchmark.Run(b, sc)

			results = append(results, benchmark.Result{
				Broker:        "rabbitmq",
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

	err := benchmark.WriteCSV("results/rabbitmq.csv", results)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Results saved to results/rabbitmq.csv")
}
