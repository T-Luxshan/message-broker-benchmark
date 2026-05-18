package kafka

import (
	"encoding/json"
	"log"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
)

type Order struct {
	OrderID  string `json:"order_id"`
	User     string `json:"user"`
	Item     string `json:"item"`
	Quantity int    `json:"quantity"`
}

func deliveryReport(e kafka.Event) {
	switch ev := e.(type) {
	case *kafka.Message:
		if ev.TopicPartition.Error != nil {
			log.Printf("Message delivery failed: %v\n", ev.TopicPartition.Error)
		} else {
			log.Printf("Message delivered: %s\n", string(ev.Value))
		}
	case kafka.Error:
		log.Printf("Error: %v\n", ev)
	default:
		log.Printf("Ignored event: %v\n", e)
	}
}

func ProduceOrders() error {
	config := kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
	}

	producer, err := kafka.NewProducer(&config)
	if err != nil {
		log.Printf("Error creating producer: %v\n", err)
		return err
	}
	defer producer.Close()
	topic := "orders"
	order := Order{
		OrderID:  uuid.New().String(),
		User:     "Luxshan",
		Item:     "Pizza",
		Quantity: 2,
	}

	value, err := json.Marshal(order)
	if err != nil {
		log.Printf("Error marshaling order: %v\n", err)
		return err
	}

	deliveryChan := make(chan kafka.Event, 1)

	err = producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value: value,
	}, deliveryChan)

	if err != nil {
		log.Printf("Error producing message: %v\n", err)
		return err
	}

	// Wait for delivery report
	e := <-deliveryChan
	deliveryReport(e)

	// Flush any remaining messages
	remaining := producer.Flush(15000)
	if remaining > 0 {
		log.Printf("%d messages were not delivered\n", remaining)
	}

	return nil
}
