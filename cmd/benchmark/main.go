package main

import (
	"fmt"
	"log"
	"message-broker-benchmark/internal/broker/rabbitmq"
	"sort"
	"sync"
	"time"
)

const (
	totalMessages = 1000
	producerCount = 10
	messageSize   = 256
)

func main() {

	b := &rabbitmq.RabbitMQ{}

	err := b.Connect()
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := b.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	// Preallocate latency slice
	latencies := make([]float64, 0, totalMessages)
	var mu sync.Mutex

	// Wait for all messages consumed
	var done sync.WaitGroup
	done.Add(totalMessages)

	// Consumer
	go func() {
		err := b.Consume(func(body []byte) {

			ts := extractTimestamp(body)
			now := time.Now().UnixNano()

			latency := float64(now-ts) / 1e6 // ms

			mu.Lock()
			latencies = append(latencies, latency)
			mu.Unlock()

			done.Done()
		})

		if err != nil {
			log.Fatal(err)
		}
	}()

	time.Sleep(time.Second) // ensure consumer ready

	start := time.Now()

	// Producer wait group
	var prodWg sync.WaitGroup
	prodWg.Add(producerCount)

	messagesPerProducer := totalMessages / producerCount

	for p := 0; p < producerCount; p++ {

		go func(pid int) {
			defer prodWg.Done()

			startIdx := pid * messagesPerProducer
			endIdx := startIdx + messagesPerProducer

			// Fix: ensure all messages are sent
			if pid == producerCount-1 {
				endIdx = totalMessages
			}

			for i := startIdx; i < endIdx; i++ {

				msg := generateMessage(i, messageSize)

				err := b.Publish(msg)
				if err != nil {
					log.Println("publish error:", err)
				}
			}
		}(p)
	}

	prodWg.Wait() // wait producers finish
	done.Wait()   // wait all messages consumed

	duration := time.Since(start)

	fmt.Println("Total messages:", totalMessages)
	fmt.Println("Producers:", producerCount)
	fmt.Println("Message size:", messageSize, "bytes")
	fmt.Println("Time taken:", duration)
	fmt.Println("Throughput:", float64(totalMessages)/duration.Seconds(), "msg/sec")

	avg, p50, p95, p99 := calculateStats(latencies)

	fmt.Println("Avg latency (ms):", avg)
	fmt.Println("P50 latency (ms):", p50)
	fmt.Println("P95 latency (ms):", p95)
	fmt.Println("P99 latency (ms):", p99)
}

func generateMessage(i int, size int) []byte {
	timestamp := time.Now().UnixNano()

	base := fmt.Sprintf("%d|msg-%d-", timestamp, i)

	padding := size - len(base)
	if padding < 0 {
		padding = 0
	}

	return []byte(base + string(make([]byte, padding)))
}

func extractTimestamp(msg []byte) int64 {
	var ts int64
	_, err := fmt.Sscanf(string(msg), "%d|", &ts)
	if err != nil {
		return 0
	}
	return ts
}

func calculateStats(latencies []float64) (avg, p50, p95, p99 float64) {

	sort.Float64s(latencies)

	n := len(latencies)
	if n == 0 {
		return
	}

	sum := 0.0
	for _, l := range latencies {
		sum += l
	}

	avg = sum / float64(n)

	p50 = percentile(latencies, 0.50)
	p95 = percentile(latencies, 0.95)
	p99 = percentile(latencies, 0.99)

	return
}

func percentile(latencies []float64, p float64) float64 {
	index := int(float64(len(latencies)-1) * p)
	return latencies[index]
}
