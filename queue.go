package broker

import (
	"github.com/streadway/amqp"
)

type queue struct {
	name    string
	channel *amqp.Channel
	q       amqp.Queue

	opts queueOptions
}

type queueOptions struct {
	durable    bool
	autoDelete bool
	exclusive  bool
	noWait     bool
}

func WithQueueDurable(val bool) QueueOption {
	return func(o *queueOptions) error {
		o.durable = val
		return nil
	}
}

func WithQueueAutoDelete(val bool) QueueOption {
	return func(o *queueOptions) error {
		o.autoDelete = val
		return nil
	}
}

func WithQueueExclusive(val bool) QueueOption {
	return func(o *queueOptions) error {
		o.exclusive = val
		return nil
	}
}

func WithQueueNoWait(val bool) QueueOption {
	return func(o *queueOptions) error {
		o.noWait = val
		return nil
	}
}

type QueueOption func(*queueOptions) error

func newQueue(channel *amqp.Channel, name string, opts ...QueueOption) (*queue, error) {

	options := queueOptions{}

	for _, o := range opts {
		err := o(&options)
		if err != nil {
			return nil, err
		}
	}

	// declare the queue

	q, err := channel.QueueDeclare(
		name,               // queue name
		options.durable,    // durable
		options.autoDelete, // auto delete
		options.exclusive,  // exclusive
		options.noWait,     // no wait
		nil,                // arguments
	)
	if err != nil {
		return nil, err
	}

	return &queue{
		name:    name,
		opts:    options,
		channel: channel,
		q:       q,
	}, err
}

func (q *queue) Publish(data []byte, opts ...PublishOption) error {

	options := publishOptions{
		routingKey: q.name,
	}

	// allow any options set on broker creation to be overridden
	for _, o := range opts {
		err := o(&options)
		if err != nil {
			return err
		}
	}
	return publish(*q.channel, data, options)
}
