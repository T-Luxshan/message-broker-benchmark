package main

import (
	"fmt"
	"log"
	"message-broker-benchmark/internal/broker/rabbitmq"
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
	defer func(b *rabbitmq.RabbitMQ) {
		err := b.Close()
		if err != nil {
			log.Fatal(err)
		}
	}(b)

	// Wait for all messages consumed
	var done sync.WaitGroup
	done.Add(totalMessages)

	// Consumer
	go func() {
		err := b.Consume(func(body []byte) {
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
	fmt.Println("Message size:", messageSize)
	fmt.Println("Time taken:", duration)
	fmt.Println("Throughput:", float64(totalMessages)/duration.Seconds(), "msg/sec")
}

func generateMessage(i int, size int) []byte {
	base := fmt.Sprintf("msg-%d-", i)

	padding := size - len(base)
	if padding < 0 {
		padding = 0
	}

	return []byte(base + string(make([]byte, padding)))
}
