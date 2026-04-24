package benchmark

import (
	"context"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"message-broker-benchmark/internal/broker"
)

func Run(b broker.Broker, sc Scenario) (throughput, avg, p50, p95, p99 float64) {

	// -------------------------
	// CONNECT BROKER
	// -------------------------
	err := b.Connect()
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		_ = b.Close()
	}()

	// -------------------------
	// METRICS STORAGE
	// -------------------------
	latencies := make([]float64, 0, sc.TotalMessages)
	var mu sync.Mutex

	received := 0
	done := make(chan struct{}, 1) // Buffered to prevent blocking
	var once sync.Once

	// -------------------------
	// CONSUMER (ASYNC)
	// -------------------------
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		err := b.Consume(ctx, func(body []byte) {
			mu.Lock()
			defer mu.Unlock()

			if received >= sc.TotalMessages {
				return
			}

			ts := extractTimestamp(body)
			now := time.Now().UnixNano()
			lat := float64(now-ts) / 1e6 // ms

			latencies = append(latencies, lat)
			received++

			if received >= sc.TotalMessages {
				once.Do(func() {
					done <- struct{}{}
				})
			}
		})

		if err != nil && ctx.Err() == nil {
			log.Println("consume error:", err)
		}
	}()

	// allow consumer to initialize and join group (especially important for Kafka)
	time.Sleep(3 * time.Second)

	start := time.Now()

	// -------------------------
	// PRODUCERS
	// -------------------------
	var prodWg sync.WaitGroup
	prodWg.Add(sc.Producers)

	perProducer := sc.TotalMessages / sc.Producers

	for p := 0; p < sc.Producers; p++ {

		go func(pid int) {
			defer prodWg.Done()

			startIdx := pid * perProducer
			endIdx := startIdx + perProducer

			if pid == sc.Producers-1 {
				endIdx = sc.TotalMessages
			}

			for i := startIdx; i < endIdx; i++ {

				msg := generateMessage(i, sc.MessageSize)

				err := b.Publish(msg)
				if err != nil {
					log.Println("publish error:", err)
					return
				}
			}
		}(p)
	}

	prodWg.Wait()

	// -------------------------
	// WAIT FOR CONSUMPTION END
	// -------------------------
	select {
	case <-done:
		// Success
	case <-time.After(30 * time.Second):
		log.Printf("Timeout waiting for messages! Received %d/%d", received, sc.TotalMessages)
	}

	duration := time.Since(start)

	// -------------------------
	// THROUGHPUT
	// -------------------------
	throughput = float64(sc.TotalMessages) / duration.Seconds()

	// -------------------------
	// STATS
	// -------------------------
	avg, p50, p95, p99 = calculateStats(latencies)

	return
}

//
// -------------------------
// HELPERS
// -------------------------
//

func generateMessage(i int, size int) []byte {
	ts := time.Now().UnixNano()

	base := fmt.Sprintf("%d|msg-%d-", ts, i)

	padding := size - len(base)
	if padding < 0 {
		padding = 0
	}

	return []byte(base + string(make([]byte, padding)))
}

func extractTimestamp(msg []byte) int64 {
	var ts int64
	fmt.Sscanf(string(msg), "%d|", &ts)
	return ts
}

func calculateStats(latencies []float64) (avg, p50, p95, p99 float64) {

	if len(latencies) == 0 {
		return
	}

	sort.Float64s(latencies)

	sum := 0.0
	for _, l := range latencies {
		sum += l
	}

	avg = sum / float64(len(latencies))

	p50 = percentile(latencies, 0.50)
	p95 = percentile(latencies, 0.95)
	p99 = percentile(latencies, 0.99)

	return
}

func percentile(latencies []float64, p float64) float64 {
	idx := int(float64(len(latencies)-1) * p)
	return latencies[idx]
}
