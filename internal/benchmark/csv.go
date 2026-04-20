package benchmark

import (
	"encoding/csv"
	"os"
	"strconv"
)

// Result struct
type Result struct {
	Broker        string
	Scenario      string
	TotalMessages int
	Producers     int
	MessageSize   int
	Run           int
	Throughput    float64
	AvgLatency    float64
	P50           float64
	P95           float64
	P99           float64
}

func WriteCSV(path string, results []Result) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func(f *os.File) {
		err := f.Close()
		if err != nil {
			panic(err)
		}
	}(f)

	w := csv.NewWriter(f)
	defer w.Flush()

	header := []string{
		"broker", "scenario", "total_messages", "producers",
		"message_size", "run", "throughput",
		"avg_latency", "p50", "p95", "p99",
	}
	if err := w.Write(header); err != nil {
		return err
	}

	for _, r := range results {
		row := []string{
			r.Broker,
			r.Scenario,
			strconv.Itoa(r.TotalMessages),
			strconv.Itoa(r.Producers),
			strconv.Itoa(r.MessageSize),
			strconv.Itoa(r.Run),
			strconv.FormatFloat(r.Throughput, 'f', 2, 64),
			strconv.FormatFloat(r.AvgLatency, 'f', 2, 64),
			strconv.FormatFloat(r.P50, 'f', 2, 64),
			strconv.FormatFloat(r.P95, 'f', 2, 64),
			strconv.FormatFloat(r.P99, 'f', 2, 64),
		}
		if err := w.Write(row); err != nil {
			return err
		}
	}

	return nil
}
