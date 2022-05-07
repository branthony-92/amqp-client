package broker

import "github.com/streadway/amqp"

type publishOptions struct {
	exchange    string
	routingKey  string
	contentType string
	mandatory   bool
	immediate   bool
}

type PublishOption func(*publishOptions) error

func WithRoutingKey(n string) PublishOption {
	return func(o *publishOptions) error {
		o.routingKey = n
		return nil
	}
}
func SetContentType(c string) PublishOption {
	return func(o *publishOptions) error {
		o.contentType = c
		return nil
	}
}
func SetMandatory(val bool) PublishOption {
	return func(o *publishOptions) error {
		o.mandatory = val
		return nil
	}
}
func SetImmediate(val bool) PublishOption {
	return func(o *publishOptions) error {
		o.immediate = val
		return nil
	}
}

func defaultPublishOpts() publishOptions {
	return publishOptions{
		exchange:    "",
		routingKey:  "",
		contentType: "text/plain",
		mandatory:   false,
		immediate:   false,
	}
}

func publish(ch amqp.Channel, msg []byte, options publishOptions) error {

	message := amqp.Publishing{
		ContentType: options.contentType,
		Body:        []byte(msg),
	}

	return ch.Publish(
		options.exchange,
		options.routingKey,
		options.mandatory,
		options.immediate,
		message,
	)
}
