package kafka

import (
	"context"
	"log"
	"sync/atomic"

	"github.com/segmentio/kafka-go"
)

type KafkaBroker struct {
	writer   *kafka.Writer
	reader   *kafka.Reader
	topic    string
	brokers  []string
	produced int64
	consumed int64
}

func (k *KafkaBroker) Connect() error {
	k.topic = "benchmark-topic"
	k.brokers = []string{"localhost:9092"}

	log.Println("Kafka connecting to:", k.brokers)

	k.writer = &kafka.Writer{
		Addr:                   kafka.TCP(k.brokers...),
		Topic:                  k.topic,
		Balancer:               &kafka.LeastBytes{},
		Async:                  false,
		AllowAutoTopicCreation: true,
	}

	groupID := "benchmark-group"

	k.reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:     k.brokers,
		Topic:       k.topic,
		GroupID:     groupID,
		StartOffset: kafka.FirstOffset,
	})

	log.Println("Kafka connected. Topic:", k.topic, "Group:", groupID)

	return nil
}

func (k *KafkaBroker) Publish(msg []byte) error {

	err := k.writer.WriteMessages(context.Background(),
		kafka.Message{Value: msg},
	)

	if err == nil {
		count := atomic.AddInt64(&k.produced, 1)

		// log every 100 messages (avoid spam)
		if count%100 == 0 {
			log.Printf("Kafka producer progress: %d messages\n", count)
		}
	}

	return err
}

func (k *KafkaBroker) Consume(ctx context.Context, handler func([]byte)) error {
	log.Println("Kafka consumer started")

	for {
		m, err := k.reader.ReadMessage(ctx)
		if err != nil {
			return err
		}

		handler(m.Value)

		count := atomic.AddInt64(&k.consumed, 1)

		// log every 100 messages
		if count%100 == 0 {
			log.Printf("Kafka consumer progress: %d messages\n", count)
		}
	}
}

func (k *KafkaBroker) Close() error {
	log.Println("Kafka shutting down")

	if k.writer != nil {
		_ = k.writer.Close()
	}
	if k.reader != nil {
		_ = k.reader.Close()
	}
	return nil
}
