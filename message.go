package broker

import (
	"encoding/json"

	"github.com/streadway/amqp"
)

type Message interface {
	GetBody(v interface{}) error
	GetRawBody() []byte
	GetContentType() string
	GetContentEncoding() string
	GetRouteKey() string
	GetQueue() string
	GetExchange() string
}

type message struct {
	msg      amqp.Delivery
	queue    string
	exchange string
}

func (m *message) GetBody(v interface{}) error {
	switch data := v.(type) {
	case *string:
		*data = string(m.msg.Body)
	case *[]byte:
		*data = m.msg.Body
	case []byte:
		copy(data, m.msg.Body)
		data = m.msg.Body
	default:
		return json.Unmarshal(m.msg.Body, v)
	}
	return nil
}
func (m *message) GetRawBody() []byte {
	return m.msg.Body
}
func (m *message) GetContentType() string {
	return m.msg.ContentType
}
func (m *message) GetContentEncoding() string {
	return m.msg.ContentEncoding
}
func (m *message) GetRouteKey() string {
	return m.msg.RoutingKey
}
func (m *message) GetQueue() string {
	return m.queue
}
func (m *message) GetExchange() string {
	return m.exchange
}
