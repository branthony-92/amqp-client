package broker

import (
	"context"
	"errors"

	"github.com/streadway/amqp"
)

type MessageBroker interface {
	Publish([]byte, ...PublishOption) error
	RegisterRoute(queue string, key string, exchange string, handler MessageHandler) error
	Shutdown()
}

type messageBroker struct {
	publishOpts publishOptions
	conn        *amqp.Connection
	channel     *amqp.Channel

	router messageRouter

	ctx context.Context
}

type brokerOptions struct {
	queueName  string
	durable    bool
	autoDelete bool
	exclusive  bool
	noWait     bool
	args       amqp.Table
}

type BrokerOption func(*brokerOptions) error

func WithQueueName(n string) BrokerOption {
	return func(o *brokerOptions) error {
		o.queueName = n
		return nil
	}
}

func WithDurable(val bool) BrokerOption {
	return func(o *brokerOptions) error {
		o.durable = val
		return nil
	}
}

func WithAutoDelete(val bool) BrokerOption {
	return func(o *brokerOptions) error {
		o.autoDelete = val
		return nil
	}
}

func WithExclusive(val bool) BrokerOption {
	return func(o *brokerOptions) error {
		o.exclusive = val
		return nil
	}
}

func WithNoWait(val bool) BrokerOption {
	return func(o *brokerOptions) error {
		o.noWait = val
		return nil
	}
}

func WithAgs(args amqp.Table) BrokerOption {
	return func(o *brokerOptions) error {
		o.args = args
		return nil
	}
}

func NewMessageBroker(ctx context.Context, url string, opts ...BrokerOption) (MessageBroker, error) {

	options := defaultBrokerOpts()

	for _, o := range opts {
		err := o(&options)
		if err != nil {
			return nil, err
		}
	}

	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	broker := messageBroker{
		conn:    conn,
		channel: ch,
		publishOpts: publishOptions{
			queueName: options.queueName,
		},
		router: messageRouter{
			routes: make(map[string]*route),
		},
		ctx: ctx,
	}

	// declare the queue
	_, err = broker.channel.QueueDeclare(
		options.queueName,  // queue name
		options.durable,    // durable
		options.autoDelete, // auto delete
		options.exclusive,  // exclusive
		options.noWait,     // no wait
		options.args,       // arguments
	)
	if err != nil {
		return nil, err
	}

	return &broker, nil
}

func defaultBrokerOpts() brokerOptions {
	return brokerOptions{}
}

type publishOptions struct {
	exchange    string
	queueName   string
	contentType string
	mandatory   bool
	immediate   bool
}

type PublishOption func(*publishOptions) error

func SetExchange(e string) PublishOption {
	return func(o *publishOptions) error {
		o.exchange = e
		return nil
	}
}
func SetQueueName(n string) PublishOption {
	return func(o *publishOptions) error {
		o.queueName = n
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
		queueName:   "",
		contentType: "text/plain",
		mandatory:   false,
		immediate:   false,
	}
}

func (m *messageBroker) Publish(msg []byte, opts ...PublishOption) error {

	options := m.publishOpts

	// allow any options set on broker creation to be overridden
	for _, o := range opts {
		err := o(&options)
		if err != nil {
			return err
		}
	}

	message := amqp.Publishing{
		ContentType: options.contentType,
		Body:        []byte(msg),
	}

	return m.channel.Publish(
		options.exchange,
		options.queueName,
		options.mandatory,
		options.immediate,
		message,
	)
}

func (m *messageBroker) RegisterRoute(queue, key, exchange string, handler MessageHandler) error {

	err := m.channel.QueueBind(queue, key, exchange, false, nil)

	if err != nil {
		return err
	}
	r := route{
		key:      key,
		queue:    queue,
		exchange: exchange,
		handler:  handler,
	}

	m.router.addRoute(&r)

	return nil
}

type channelOptions struct {
	consumer       string
	autoAck        bool
	exclusive      bool
	noLocal        bool
	noWait         bool
	defaultHandler MessageHandler
}

type ChannelOption func(*channelOptions) error

func WithConsumeConsumer(c string) ChannelOption {
	return func(o *channelOptions) error {
		o.consumer = c
		return nil
	}
}

func WithConsumeAutoAck(a bool) ChannelOption {
	return func(o *channelOptions) error {
		o.autoAck = a
		return nil
	}
}

func WithConsumeExclusuve(e bool) ChannelOption {
	return func(o *channelOptions) error {
		o.exclusive = e
		return nil
	}
}

func WithConsumeNoLocal(n bool) ChannelOption {
	return func(o *channelOptions) error {
		o.noLocal = n
		return nil
	}
}

func WithConsumeNoWait(n bool) ChannelOption {
	return func(o *channelOptions) error {
		o.noWait = n
		return nil
	}
}

func WithDefaultHandler(h MessageHandler) ChannelOption {
	return func(o *channelOptions) error {
		o.defaultHandler = h
		return nil
	}
}

func (m *messageBroker) StartConsuming(opts ...ChannelOption) error {

	options := channelOptions{
		consumer:       "",
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

	msgs, err := m.channel.Consume(
		m.publishOpts.queueName,
		options.consumer,
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
				return errors.New("channel closed early")
			}
			msg := &message{
				msg:      data,
				queue:    m.publishOpts.queueName,
				exchange: m.publishOpts.exchange,
			}
			options.defaultHandler(msg)
			m.router.routeMessage(msg)
		case <-m.ctx.Done():
			return m.ctx.Err()
		}
	}
}

func (m *messageBroker) Shutdown() {
	if m.channel != nil {
		m.channel.Close()
	}
	if m.conn != nil {
		m.conn.Close()
	}
}
