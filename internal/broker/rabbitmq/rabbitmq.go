package rabbitmq

import (
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitMQ struct {
	conn  *amqp.Connection
	ch    *amqp.Channel
	queue amqp.Queue
}

func (r *RabbitMQ) Connect() error {

	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		return err
	}

	ch, err := conn.Channel()
	if err != nil {
		return err
	}

	q, err := ch.QueueDeclare(
		"test-queue",
		true,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		return err
	}

	r.conn = conn
	r.ch = ch
	r.queue = q

	return nil
}

func (r *RabbitMQ) Close() error {
	err := r.ch.Close()
	if err != nil {
		return err
	}
	return r.conn.Close()
}

func (r *RabbitMQ) Publish(body []byte) error {

	return r.ch.Publish(
		"",
		r.queue.Name,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        body,
		},
	)
}

func (r *RabbitMQ) Consume(handler func([]byte)) error {

	msgs, err := r.ch.Consume(
		r.queue.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		return err
	}

	go func() {
		for msg := range msgs {
			handler(msg.Body)
		}
	}()

	log.Println("Waiting for messages...")
	select {}
}
