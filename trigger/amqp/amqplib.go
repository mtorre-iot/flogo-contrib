//
package amqp

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/streadway/amqp"
)

//
// AMQPExchange contains all parameters required to create or open an exchenge
//
//
//
type AMQPExchange struct {
	Uri          string
	HostName     string
	ExchangeName string
	ExchangeType string
	QueueName    string
	RoutingKey   string
	UserName     string
	Password     string
	Durable      bool
	AutoDelete   bool
	Reliable     bool
	Connection   *amqp.Connection
	Channel      *amqp.Channel
	Queue        *amqp.Queue
	Messages     []string
	Confirms     chan amqp.Confirmation
	IsOpen       bool
}
// AMQPConfiguration main AMQP Connection configuration
type AMQPConfiguration struct {
	UriPrefix 	string 	// Broker access URI prefix
	Port		int		// broker port
}
var (
	msgs     amqp.Delivery
	msgsLock sync.Mutex
	conf 	 AMQPConfiguration
)
// AMQPInit AMPQ intializer
func AMQPInit (prefix string, port int) {
	conf.UriPrefix = prefix
	conf.Port = port
}
//
//
// AMQPExchangeNew creates a new exchange in the Broker
func AMQPExchangeNew (hostName string, exchangeName string, exchangeType string, queueName string, routingKey string,
	userName string, password string, durable bool, autoDelete bool, reliable bool) *AMQPExchange {
	if conf.UriPrefix == "" {
		return nil // broker has not been initialized
	}
	uri := buildURI(hostName, userName, password)
	exch := AMQPExchange{uri, hostName, exchangeName, exchangeType, queueName, routingKey, userName, password, durable, autoDelete, reliable, nil, nil, nil, []string{}, nil, false}
	return &exch
}

// buildURI creates the full broker URI string
func buildURI(hostName string, userName string, password string) string {
	// "amqp://" + userName + ":" + password + "@" + hostName + ":5672"
	return fmt.Sprintf("%s%s:%s@%s:%d", conf.UriPrefix, userName, password, hostName, conf.Port)
}

// Open opens a previosly created Exchange
func (exch *AMQPExchange) Open(isQueued bool) error {
	conn, err := amqp.Dial(exch.Uri)
	if err != nil {
		return fmt.Errorf("Connection: %s", err)
	}
	exch.Connection = conn
	chn, err := exch.Connection.Channel()
	if err != nil {
		exch.Connection.Close()
		return fmt.Errorf("Channel: %s", err)
	}
	exch.Channel = chn
	if err := exch.Channel.ExchangeDeclare(
		exch.ExchangeName, // name
		exch.ExchangeType, // type
		exch.Durable,      // durable
		exch.AutoDelete,   // auto-delete
		false,             // internal
		false,             // noWait
		nil,               // arguments
	); err != nil {
		exch.Connection.Close()
		exch.Channel = nil
		exch.Connection = nil
		exch.IsOpen = false
		return fmt.Errorf("Exchange Declare: %s", err)
	}
	if exch.Reliable {
		if err := exch.Channel.Confirm(false); err != nil {
			exch.Connection.Close()
			exch.Channel = nil
			exch.Connection = nil
			exch.IsOpen = false
			return fmt.Errorf("Channel could not be put into confirm mode: %s", err)
		}
		confirms := exch.Channel.NotifyPublish(make(chan amqp.Confirmation, 1))
		exch.Confirms = confirms
	}
	if isQueued == true {
		queue, err := exch.Channel.QueueDeclare(
			exch.QueueName,  // name
			exch.Durable,    // durable
			exch.AutoDelete, // delete when unused
			true,            // exclusive
			false,           // no-wait
			nil,             // arguments
		)
		if err != nil {
			exch.Connection.Close()
			exch.Queue = nil
			exch.Channel = nil
			exch.Connection = nil
			exch.IsOpen = false
			return fmt.Errorf("Queue Declare: %s", err)
		}

		if err = exch.Channel.QueueBind(
			exch.QueueName,    // queue name
			exch.RoutingKey,   // routing key
			exch.ExchangeName, // exchange
			false,
			nil); err != nil {
			exch.Connection.Close()
			exch.Queue = nil
			exch.Channel = nil
			exch.Connection = nil
			exch.IsOpen = false
			return fmt.Errorf("Queue Bind: %s", err)
		}
		exch.Queue = &queue
	}
	exch.IsOpen = true
	return err
}

// Close closes the exchange
func (exch *AMQPExchange) Close() error {
	if (exch.Connection == nil) || (exch.IsOpen == false) {
		return fmt.Errorf("Connection for exchange: " + exch.ExchangeName + " was not open")
	}
	if err := exch.Channel.Close(); err != nil {
		return fmt.Errorf("Channel for exchange: " + exch.ExchangeName + " couldn't be closed")
	}
	if err := exch.Connection.Close(); err != nil {
		return fmt.Errorf("Connection for exchange: " + exch.ExchangeName + " couldn't be closed")
	}
	exch.Channel = nil
	exch.Connection = nil
	exch.IsOpen = false
	return nil
}

// PublishObject serializes objects (JSON) and publish to existing exchange
func (exch *AMQPExchange) PublishObject(obj interface{}) error {
	bytes, err := json.Marshal(obj)
	if err != nil {
		return err
	}
	return exch.Publish(string(bytes))
}

// Publish publishes a new message into an existing exchange
func (exch *AMQPExchange) Publish(body string) error {

	if (exch.Connection == nil) || (exch.IsOpen == false) {
		return fmt.Errorf("Connection for exchange: " + exch.ExchangeName + " is not open")
	}

	if err := exch.Channel.Publish(
		exch.ExchangeName, // publish to an exchange
		exch.RoutingKey,   // routing to 0 or more queues
		false,             // mandatory
		false,             // immediate
		amqp.Publishing{
			Headers:         amqp.Table{},
			ContentType:     "text/plain",
			ContentEncoding: "",
			Body:            []byte(body),
			DeliveryMode:    amqp.Transient, // 1=non-persistent, 2=persistent
			Priority:        0,              // 0-9
		},
	); err != nil {
		exch.Connection.Close()
		exch.IsOpen = false
		return fmt.Errorf("Exchange Publish: %s", err)
	}
	// Reliable publisher confirms require confirm.select support from the
	// connection.
	if exch.Reliable {
		if cf := confirmOne(exch.Confirms); cf == false {
			return fmt.Errorf("Publish was not confirmed")
		}
	}
	return nil
}

// PrepareReceiveFunc Prepares exchange/queue to receive messages
func (exch *AMQPExchange) PrepareReceiveFunc(f func(msgs <-chan amqp.Delivery)) error {
	msgs, err := exch.Channel.Consume(
		exch.QueueName, // queue
		"",             // consumer
		true,           // auto ack
		false,          // exclusive
		false,          // no local
		false,          // no wait
		nil,            // args
	)
	if err != nil {
		exch.Connection.Close()
		exch.Queue = nil
		exch.Channel = nil
		exch.Connection = nil
		exch.IsOpen = false
		return fmt.Errorf("Exchange PrepareReceiveFunc: %s", err)
	}
	//go receiverTask(exch, msgs)
	go f(msgs)
	return nil
}

// ReadMessages reads the messajes accumulated in the queue
func (exch *AMQPExchange) ReadMessages() ([]string, error) {
	msgsLock.Lock()
	msgsRead := make([]string, len(exch.Messages))
	copy(msgsRead, exch.Messages)
	exch.Messages = nil // clear queue
	msgsLock.Unlock()
	return msgsRead, nil
}

func confirmOne(confirms <-chan amqp.Confirmation) bool {
	confirmed := <-confirms
	return confirmed.Ack
}

func receiverTask(exch *AMQPExchange, msgs <-chan amqp.Delivery) {
	for d := range msgs {
		msgsLock.Lock()
		strm := fmt.Sprintf("%s", d.Body)
		exch.Messages = append(exch.Messages, strm)
		msgsLock.Unlock()
	}
}
