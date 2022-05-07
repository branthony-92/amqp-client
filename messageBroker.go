package broker

import (
	"github.com/streadway/amqp"
)

type Publisher interface {
	Publish(data []byte, opts ...PublishOption) error
}

type MessageBroker interface {
	CreateQueue(name string, options ...QueueOption) (Publisher, error)
	CreateExchange(name string, options ...ExchangeOption) (Publisher, error)
	CreateConsumer(name string) Consumer
	Shutdown()
}

type messageBroker struct {
	conn    *amqp.Connection
	channel *amqp.Channel

	exchanges map[string]*exchange
	queues    map[string]*queue
}

func NewMessageBroker(url string) (MessageBroker, error) {

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

		exchanges: make(map[string]*exchange, 0),
		queues:    make(map[string]*queue, 0),
	}

	if err != nil {
		return nil, err
	}

	return &broker, nil
}

func (m *messageBroker) Shutdown() {
	if m.channel != nil {
		m.channel.Close()
	}
	if m.conn != nil {
		m.conn.Close()
	}
}

func (m *messageBroker) CreateExchange(name string, options ...ExchangeOption) (Publisher, error) {
	exchange, err := newExchange(m.channel, name, options...)
	if err != nil {
		return nil, err
	}
	m.exchanges[name] = exchange
	return exchange, nil
}
func (m *messageBroker) CreateQueue(name string, options ...QueueOption) (Publisher, error) {
	queue, err := newQueue(m.channel, name, options...)
	if err != nil {
		return nil, err
	}
	m.queues[name] = queue
	return queue, nil
}

func (m *messageBroker) CreateConsumer(name string) Consumer {
	c := consumer{
		router: messageRouter{
			routes:     make(map[string]*route),
			httpRoutes: make(map[string]*httpRoute),
		},
		channel: m.channel,
		name:    name,
	}
	return &c
}
