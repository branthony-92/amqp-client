package broker

import (
	"context"
	"errors"

	"github.com/streadway/amqp"
)

type Consumer interface {
	RegisterRoute(queue, key, exchange string, handler MessageHandler) error
	StartConsuming(ctx context.Context, queueName string, opts ...ConsumeOption) error
}

type consumer struct {
	name    string
	router  messageRouter
	channel *amqp.Channel
}

type consumeOptions struct {
	autoAck        bool
	exclusive      bool
	noLocal        bool
	noWait         bool
	defaultHandler MessageHandler
}

type ConsumeOption func(*consumeOptions) error

func WithConsumeAutoAck(a bool) ConsumeOption {
	return func(o *consumeOptions) error {
		o.autoAck = a
		return nil
	}
}

func WithConsumeExclusuve(e bool) ConsumeOption {
	return func(o *consumeOptions) error {
		o.exclusive = e
		return nil
	}
}

func WithConsumeNoLocal(n bool) ConsumeOption {
	return func(o *consumeOptions) error {
		o.noLocal = n
		return nil
	}
}

func WithConsumeNoWait(n bool) ConsumeOption {
	return func(o *consumeOptions) error {
		o.noWait = n
		return nil
	}
}

func WithDefaultHandler(h MessageHandler) ConsumeOption {
	return func(o *consumeOptions) error {
		o.defaultHandler = h
		return nil
	}
}

func (c *consumer) StartConsuming(ctx context.Context, queueName string, opts ...ConsumeOption) error {

	options := consumeOptions{
		autoAck:        true,
		exclusive:      false,
		noLocal:        false,
		noWait:         false,
		defaultHandler: func(m Message) {},
	}

	for _, o := range opts {
		if err := o(&options); err != nil {
			return err
		}
	}

	msgs, err := c.channel.Consume(
		queueName,
		c.name,
		options.autoAck,
		options.exclusive,
		options.noLocal,
		options.noWait,
		nil,
	)

	if err != nil {
		return err
	}

	for {
		select {
		case data, ok := <-msgs:
			if !ok {
				return errors.New("message channel has closed")
			}
			msg := &message{
				msg:      data,
				queue:    queueName,
				exchange: data.Exchange,
			}
			options.defaultHandler(msg)
			c.router.routeMessage(msg)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (c *consumer) RegisterRoute(queue, key, exchange string, handler MessageHandler) error {

	err := c.channel.QueueBind(queue, key, exchange, false, nil)

	if err != nil {
		return err
	}
	r := route{
		key:      key,
		queue:    queue,
		exchange: exchange,
		handler:  handler,
	}

	c.router.addRoute(&r)

	return nil
}
