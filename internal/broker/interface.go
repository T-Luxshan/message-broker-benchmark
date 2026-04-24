package broker

import "context"

type Broker interface {
	Connect() error
	Close() error
	Publish([]byte) error
	Consume(ctx context.Context, handler func([]byte)) error
}
