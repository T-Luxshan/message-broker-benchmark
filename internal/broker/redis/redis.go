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
}

func (r *Redis) Connect() error {

	r.client = redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	// health check
	if err := r.client.Ping(context.Background()).Err(); err != nil {
		return err
	}

	r.channel = "benchmark-channel"
	return nil
}

func (r *Redis) Close() error {
	if r.pubsub != nil {
		_ = r.pubsub.Close()
	}
	if r.client != nil {
		return r.client.Close()
	}
	return nil
}

func (r *Redis) Publish(body []byte) error {
	return r.client.Publish(context.Background(), r.channel, body).Err()
}

func (r *Redis) Consume(ctx context.Context, handler func([]byte)) error {

	r.pubsub = r.client.Subscribe(ctx, r.channel)

	ch := r.pubsub.Channel()

	log.Println("Redis consumer started")

	// message processing
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-ch:
				if !ok {
					return
				}
				handler([]byte(msg.Payload))
			}
		}
	}()

	// block until cancelled
	<-ctx.Done()

	log.Println("Redis consumer stopping")

	return nil
}
