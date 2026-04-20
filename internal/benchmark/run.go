package benchmark

import (
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"message-broker-benchmark/internal/broker"
)

func Run(b broker.Broker, sc Scenario) (throughput, avg, p50, p95, p99 float64) {

	err := b.Connect()
	if err != nil {
		log.Fatal(err)
	}
	defer b.Close()

	latencies := make([]float64, 0, sc.TotalMessages)
	var mu sync.Mutex

	var done sync.WaitGroup
	done.Add(sc.TotalMessages)

	go func() {
		err := b.Consume(func(body []byte) {
			ts := extractTimestamp(body)
			now := time.Now().UnixNano()

			lat := float64(now-ts) / 1e6

			mu.Lock()
			latencies = append(latencies, lat)
			mu.Unlock()

			done.Done()
		})
		if err != nil {
			log.Fatal(err)
		}
	}()

	time.Sleep(time.Second)

	start := time.Now()

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
				b.Publish(msg)
			}
		}(p)
	}

	prodWg.Wait()
	done.Wait()

	duration := time.Since(start)

	throughput = float64(sc.TotalMessages) / duration.Seconds()
	avg, p50, p95, p99 = calculateStats(latencies)

	return
}

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
	sort.Float64s(latencies)

	n := len(latencies)
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
	idx := int(float64(len(latencies)-1) * p)
	return latencies[idx]
}
