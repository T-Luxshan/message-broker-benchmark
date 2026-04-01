package main

import (
	"fmt"
	"log"
	"message-broker-benchmark/internal/broker/rabbitmq"
	"time"
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

	go func() {

		for i := 0; i < 20; i++ {

			msg := fmt.Sprintf("message %d", i)

			err := b.Publish([]byte(msg))
			if err != nil {
				log.Println(err)
			}

			fmt.Println("Sent:", msg)

			time.Sleep(time.Second)
		}

	}()

	err = b.Consume(func(body []byte) {
		fmt.Println("Received:", string(body))
	})

	if err != nil {
		log.Fatal(err)
	}
}
