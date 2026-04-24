package kafka

import (
	"context"
	"github.com/segmentio/kafka-go"
	"log"
)

type KafkaBroker struct {
	writer  *kafka.Writer
	reader  *kafka.Reader
	topic   string
	brokers []string
}

func (k *KafkaBroker) Connect() error {
	k.topic = "benchmark-topic"
	k.brokers = []string{"localhost:9092"}

	k.writer = &kafka.Writer{
		Addr:                   kafka.TCP(k.brokers...),
		Topic:                  k.topic,
		Balancer:               &kafka.LeastBytes{},
		Async:                  false, // Synchronous for accurate benchmarking
		AllowAutoTopicCreation: true,
	}

	groupID := "benchmark-group"

	k.reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:     k.brokers,
		Topic:       k.topic,
		GroupID:     groupID,
		StartOffset: kafka.FirstOffset, // Ensure we start from the beginning if the group is new
	})

	return nil
}

func (k *KafkaBroker) Publish(msg []byte) error {
	return k.writer.WriteMessages(context.Background(),
		kafka.Message{
			Value: msg,
		},
	)
}

func (k *KafkaBroker) Consume(ctx context.Context, handler func([]byte)) error {
	log.Println("Kafka consumer started with group:", k.reader.Config().GroupID)
	for {
		m, err := k.reader.ReadMessage(ctx)
		if err != nil {
			// stop when context is cancelled or reader is closed
			return err
		}
		handler(m.Value)
	}
}

func (k *KafkaBroker) Close() error {
	if k.writer != nil {
		_ = k.writer.Close()
	}
	if k.reader != nil {
		_ = k.reader.Close()
	}
	return nil
}
