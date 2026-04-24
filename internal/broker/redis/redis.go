package redis

import (
	"context"
	"log"

	"github.com/redis/go-redis/v9"
)

type Redis struct {
	client  *redis.Client
	pubsub  *redis.PubSub
	channel string
	ctx     context.Context
}

func (r *Redis) Connect() error {
	r.ctx = context.Background()

	r.client = redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	// simple ping check
	if err := r.client.Ping(r.ctx).Err(); err != nil {
		return err
	}

	r.channel = "benchmark-channel"
	return nil
}

func (r *Redis) Close() error {
	if r.pubsub != nil {
		_ = r.pubsub.Close()
	}
	return r.client.Close()
}

func (r *Redis) Publish(body []byte) error {
	return r.client.Publish(r.ctx, r.channel, body).Err()
}

func (r *Redis) Consume(handler func([]byte)) error {
	r.pubsub = r.client.Subscribe(r.ctx, r.channel)

	ch := r.pubsub.Channel()

	log.Println("Waiting for Redis messages...")

	go func() {
		for msg := range ch {
			handler([]byte(msg.Payload))
		}
	}()

	select {}
}
