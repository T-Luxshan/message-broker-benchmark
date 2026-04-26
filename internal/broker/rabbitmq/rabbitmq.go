package rabbitmq

import (
	"context"
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
	if r.ch != nil {
		_ = r.ch.Close()
	}
	if r.conn != nil {
		return r.conn.Close()
	}
	return nil
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

func (r *RabbitMQ) Consume(ctx context.Context, handler func([]byte)) error {

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

	log.Println("RabbitMQ consumer started")

	// process messages
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-msgs:
				if !ok {
					return
				}
				handler(msg.Body)
			}
		}
	}()

	// block until context cancellation
	<-ctx.Done()

	log.Println("RabbitMQ consumer stopping")

	return nil
}
