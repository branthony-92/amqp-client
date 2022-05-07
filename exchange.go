package broker

import "github.com/streadway/amqp"

type ExchangeType string

const (
	DirectExchange ExchangeType = "direct"
	TopicExchange  ExchangeType = "topic"
	HeaderExchange ExchangeType = "headers"
	FanoutExchange ExchangeType = "fanout"
)

type ExchangeOption func(*exchangeOptions) error

type exchangeOptions struct {
	Type       ExchangeType
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
}

type exchange struct {
	name    string
	channel *amqp.Channel

	opts exchangeOptions
}

func newExchange(channel *amqp.Channel, name string, options ...ExchangeOption) (*exchange, error) {
	opts := exchangeOptions{
		Type:       FanoutExchange, // type
		Durable:    true,           // durable
		AutoDelete: false,          // auto-deleted
		Internal:   false,          // internal
		NoWait:     false,          // no-wait
	}

	for _, o := range options {
		if err := o(&opts); err != nil {
			return nil, err
		}
	}

	err := channel.ExchangeDeclare(
		name,
		string(opts.Type),
		opts.Durable,
		opts.AutoDelete,
		opts.Internal,
		opts.NoWait,
		nil,
	)

	if err != nil {
		return nil, err
	}

	ex := exchange{
		opts:    opts,
		channel: channel,
		name:    name,
	}

	return &ex, nil
}

func (e *exchange) Publish(data []byte, opts ...PublishOption) error {
	options := publishOptions{
		exchange: e.name,
	}

	// allow any options set on broker creation to be overridden
	for _, o := range opts {
		err := o(&options)
		if err != nil {
			return err
		}
	}
	return publish(*e.channel, data, options)
}
