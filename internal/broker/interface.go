package broker

type Broker interface {
	Connect() error
	Close() error
	Publish([]byte) error
	Consume(func([]byte)) error
}
