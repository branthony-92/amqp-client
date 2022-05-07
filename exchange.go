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
	exchangeType ExchangeType
	durable      bool
	autoDelete   bool
	internal     bool
	noWait       bool
}

func WithExchangeTypee(t ExchangeType) ExchangeOption {
	return func(o *exchangeOptions) error {
		o.exchangeType = t
		return nil
	}
}

func WithExchangeDurable(val bool) ExchangeOption {
	return func(o *exchangeOptions) error {
		o.durable = val
		return nil
	}
}

func WithExchangeAutoDelete(val bool) ExchangeOption {
	return func(o *exchangeOptions) error {
		o.autoDelete = val
		return nil
	}
}

func WithExchangeInternal(val bool) ExchangeOption {
	return func(o *exchangeOptions) error {
		o.internal = val
		return nil
	}
}

func WithExchangeNoWait(val bool) ExchangeOption {
	return func(o *exchangeOptions) error {
		o.noWait = val
		return nil
	}
}

type exchange struct {
	name    string
	channel *amqp.Channel

	opts exchangeOptions
}

func newExchange(channel *amqp.Channel, name string, options ...ExchangeOption) (*exchange, error) {
	opts := exchangeOptions{
		exchangeType: FanoutExchange, // type
		durable:      true,           // durable
		autoDelete:   false,          // auto-deleted
		internal:     false,          // internal
		noWait:       false,          // no-wait
	}

	for _, o := range options {
		if err := o(&opts); err != nil {
			return nil, err
		}
	}

	err := channel.ExchangeDeclare(
		name,
		string(opts.exchangeType),
		opts.durable,
		opts.autoDelete,
		opts.internal,
		opts.noWait,
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
