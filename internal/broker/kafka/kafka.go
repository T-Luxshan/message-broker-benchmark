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
		Addr:     kafka.TCP(k.brokers...),
		Topic:    k.topic,
		Balancer: &kafka.LeastBytes{},
	}

	k.reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers: k.brokers,
		Topic:   k.topic,
		GroupID: "benchmark-group",
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

func (k *KafkaBroker) Consume(handler func([]byte)) error {
	log.Println("Kafka consumer started")
	for {
		m, err := k.reader.ReadMessage(context.Background())
		if err != nil {
			log.Println("consume error:", err)
			continue
		}
		log.Println("message received")
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
