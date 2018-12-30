package amqp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"

	"github.com/TIBCOSoftware/flogo-lib/core/data"
	"github.com/TIBCOSoftware/flogo-lib/core/trigger"
	"github.com/TIBCOSoftware/flogo-lib/logger"
	"github.com/streadway/amqp"
)

// log is the default package logger
var (
	log            = logger.GetLogger("trigger-flogo-amqp")
	rqHostName     = "requestHostName"
	rqPort         = "requestPort"
	rqExchangeName = "requestExchangeName"
	rqExchangeType = "requestExchangeType"
	rqRoutingKey   = "requestRoutingKey"
	rqTopic        = "requestTopic"
	rqDurable      = "requestDurable"
	rqAutoDelete   = "requestAutoDelete"
	rqReliable     = "requestReliable"
	rqUser         = "requestUser"
	rqPassword     = "requestPassword"
	rsHostName     = "responseHostName"
	rsPort         = "responsePort"
	rsExchangeName = "responseExchangeName"
	rsExchangeType = "responseExchangeType"
	rsRoutingKey   = "responseRoutingKey"
	rsUser         = "responseUser"
	rsPassword     = "responsePassword"
	rsDurable      = "responseDurable"
	rsAutoDelete   = "responseAutoDelete"
	rsReliable     = "responseReliable"
	msgs           amqp.Delivery
	msgsLock       sync.Mutex
)

//
// AMQPExchange contains all parameters required to create or open an exchenge
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

//
// AmqpTrigger is simple AMQP trigger
type AmqpTrigger struct {
	metadata       *trigger.Metadata
	config         *trigger.Config
	handlers       []*trigger.Handler
	topicToHandler map[string]*trigger.Handler
	reqExch        *AMQPExchange
	resExch        *AMQPExchange
}

//NewFactory create a new Trigger factory
func NewFactory(md *trigger.Metadata) trigger.Factory {
	return &AMQPFactory{metadata: md}
}

// AMQPFactory AMQP Trigger factory
type AMQPFactory struct {
	metadata *trigger.Metadata
}

//New Creates a new trigger instance for a given id
func (t *AMQPFactory) New(config *trigger.Config) trigger.Trigger {
	return &AmqpTrigger{metadata: t.metadata, config: config}
}

// Metadata implements trigger.Trigger.Metadata
func (t *AmqpTrigger) Metadata() *trigger.Metadata {
	return t.metadata
}

// Initialize implements trigger.Initializable.Initialize
func (t *AmqpTrigger) Initialize(ctx trigger.InitContext) error {
	t.handlers = ctx.GetHandlers()
	return nil
}
func (t *AmqpTrigger) checkParameter(attribute string) (string, error) {
	param := t.config.GetSetting(attribute)
	if param == "" {
		errMsg := fmt.Sprintf("Setting '%s' not found.", attribute) 
		return param, errors.New(errMsg)
	}
	return param, nil
}

// Start implements trigger.Trigger.Start
func (t *AmqpTrigger) Start() error {
	//
	if t.config.Settings == nil {
		errMsg := fmt.Sprintf("No Settings found for trigger '%s'", t.config.Id) 
		log.Error(errMsg)
		return errors.New(errMsg)
	}
	requestHostName, err := t.checkParameter(rqHostName)
	if err != nil {
		return err
	}
	requestPortStr, err := t.checkParameter(rqPort)
	if err != nil {
		return err
	}
	requestPort, err := strconv.Atoi(requestPortStr)
	if err != nil {
		log.Error("Request Exchange: Error converting \"Port\" to an integer ", err.Error())
		return err
	}
	requestExchangeName, err:= t.checkParameter(rqExchangeName)
	if  err != nil {
		return err
	}
	requestQueueName := t.config.Id

	requestExchangeType, err := t.checkParameter(rqExchangeType)
	if err != nil {
		return err
	}
	requestRoutingKey, err := t.checkParameter(rqRoutingKey)
	if err != nil {
		return err
	}
	requestUser, err := t.checkParameter(rqUser);
	if err != nil {
		return err
	}
	requestPassword, err := t.checkParameter(rqPassword)
	if err != nil {
		return err
	}
	requestReliableStr, err := t.checkParameter(rqReliable);
	if err != nil {
		requestReliableStr = "false"
	}
	requestReliable, err := data.CoerceToBoolean(requestReliableStr)
	if err != nil {
		log.Warn("Request Exchange: Error converting \"Reliable\" to a boolean. Assuming default (false).")
		requestReliable = false
	}
	requestDurableStr, err := t.checkParameter(rqDurable)
	if err != nil {
		requestDurableStr = "false"
	}
	requestDurable, err := data.CoerceToBoolean(requestDurableStr)
	if err != nil {
		log.Warn("Request Exchange: Error converting \"Durable\" to a boolean. Assuming default (false).")
		requestDurable = false
	}
	requestAutoDeleteStr, err := t.checkParameter(rqAutoDelete)
	if err != nil {
		requestAutoDeleteStr = "true"
	}
	requestAutoDelete, err := data.CoerceToBoolean(requestAutoDeleteStr)
	if err != nil {
		log.Warn("Request Exchange: Error converting \"AutoDelete\" to a boolean. Assuming default (true).")
		requestAutoDelete = true
	}

	// Response configuration is optional

	responseHostName := t.config.GetSetting(rsHostName)
	responsePortStr := t.config.GetSetting(rsPort)
	responsePort := 5672
	if (responsePortStr != "") {
		responsePort, err = strconv.Atoi(responsePortStr)
		if err != nil {
			log.Error("Response Exchange: Error converting \"Port\" to an integer ", err.Error())
			return err
		}
	}
	responseExchangeName := t.config.GetSetting(rsExchangeName)
	responseExchangeType := t.config.GetSetting(rsExchangeType)
	responseRoutingKey := t.config.GetSetting(rsRoutingKey)
	responseUser := t.config.GetSetting(rsUser)
	responsePassword := t.config.GetSetting(rsPassword)

	responseReliableStr, err := t.checkParameter(rsReliable) 
	if err != nil {
		responseReliableStr = "true"
	}
	responseReliable, err := data.CoerceToBoolean(responseReliableStr)
	if err != nil {
		log.Debug("Response Exchange: Error converting \"Reliable\" to a boolean. Assuming default (true).")
		responseReliable = true
	}
	responseDurableStr, err := t.checkParameter(rqDurable)
	if err != nil {
		responseDurableStr = "false"
	}
	responseDurable, err := data.CoerceToBoolean(responseDurableStr)
	if err != nil {
		log.Debug("Response Exchange: Error converting \"Durable\" to a boolean. Assuming default (false).")
		responseDurable = false
	}
	responseAutoDeleteStr, err := t.checkParameter(rsAutoDelete)
	if err != nil {
		responseAutoDeleteStr = "true"
	}
	responseAutoDelete, err := data.CoerceToBoolean(responseAutoDeleteStr)
	if err != nil {
		log.Debug("Response Exchange: Error converting \"AutoDelete\" to a boolean. Assuming default (true).")
		responseAutoDelete = true
	}
	//
	//	Create the request exchange object
	//
	t.reqExch = AMQPExchangeNew(requestHostName,
		requestPort,
		requestExchangeName,
		requestExchangeType,
		requestQueueName,
		requestRoutingKey,
		requestUser,
		requestPassword,
		requestDurable,
		requestAutoDelete,
		requestReliable)

	if t.reqExch == nil {
		errMsg := fmt.Sprintf("Request Exchange: Unable to Create Exchange Object: %s", t.reqExch.ExchangeName)
		log.Error(errMsg)
		return errors.New(errMsg)
	}
	//
	// Create the AMQP Request Exchange
	//
	if err := t.reqExch.Open(true); err != nil {
		log.Errorf("Request Exchange: Unable to Open Exchange: %s : %s", t.reqExch.ExchangeName, err)
		return err
	}
	//
	// Prepare to receive
	//
	if err := t.reqExch.PrepareReceiveFunc(t.receiverHandler); err != nil {
		log.Errorf("Request Exchange: Unable to Prepare: %s to Receive. Error: %s. Bail out", err)
		return err
	}
	//
	//	Create the response exchange object
	//
	if responseHostName != "" {
		t.resExch = AMQPExchangeNew(responseHostName,
			responsePort,
			responseExchangeName,
			responseExchangeType,
			"",
			responseRoutingKey,
			responseUser,
			responsePassword,
			responseDurable,
			responseAutoDelete,
			responseReliable)

		if t.resExch == nil {
			errMsg := fmt.Sprintf("Response Exchange: Unable to Create Exchange Object: %s", t.resExch.ExchangeName)
			log.Error(errMsg)
			return errors.New(errMsg)
		}
		//
		// Create the AMQP Response Exchange
		//
		if err := t.resExch.Open(false); err != nil {
			log.Errorf("Response Exchange: Unable to Open Exchange: %s : %s", t.resExch.ExchangeName, err)
			return err
		}
	}
	t.topicToHandler = make(map[string]*trigger.Handler)

	for _, handler := range t.handlers {
		topic := handler.GetStringSetting("topic")
		t.topicToHandler[topic] = handler
	}

	return nil
}

func (t *AmqpTrigger) receiverHandler(msgs <-chan amqp.Delivery) {
	for d := range msgs {
		payload := fmt.Sprintf("%s", d.Body)
		log.Debugf("Message received: %s", payload)
		topic := fmt.Sprintf("%s", d.RoutingKey)
		handler, found := t.topicToHandler[topic]
		if found {
			t.RunHandler(handler, payload)
		} else {
			log.Warnf("handler for topic '%s' not found", topic)
		}
	}
}

// Stop implements ext.Trigger.Stop
func (t *AmqpTrigger) Stop() error {
	//Close Exchange
	t.reqExch.Close()
	t.resExch.Close()
	return nil
}

// RunHandler runs the handler and associated action
func (t *AmqpTrigger) RunHandler(handler *trigger.Handler, payload string) {
	trgData := make(map[string]interface{})
	trgData["message"] = payload

	results, err := handler.Handle(context.Background(), trgData)

	if err != nil {
		log.Error("Error starting action: ", err.Error())
	}

	var replyData interface{}

	if len(results) != 0 {
		dataAttr, ok := results["data"]
		if ok {
			replyData = dataAttr.Value()
		}
	}
	if replyData != nil {
		dataJson, err := json.Marshal(replyData)
		if err != nil {
			log.Error(err)
		} else {
			t.publishMessage(string(dataJson))
		}
	}
}

func (t *AmqpTrigger) publishMessage(message string) {

	log.Info("Replying message: ", message)

	err := t.resExch.Publish(message)
	if err != nil {
		// Timeout occurred
		log.Errorf("Error occurred while trying to publish to Exchange '%s'", t.resExch.ExchangeName)
		return
	}
}

//
// AMQPExchangeNew creates a new exchange in the Broker
func AMQPExchangeNew(hostName string, port int, exchangeName string, exchangeType string, queueName string, routingKey string,
	userName string, password string, durable bool, autoDelete bool, reliable bool) *AMQPExchange {
	uri := buildURI(hostName, port, userName, password)
	exch := AMQPExchange{uri, hostName, exchangeName, exchangeType, queueName, routingKey, userName, password, durable, autoDelete, reliable, nil, nil, nil, []string{}, nil, false}
	return &exch
}

// buildURI creates the full broker URI string
func buildURI(hostName string, port int, userName string, password string) string {
	// "amqp://" + userName + ":" + password + "@" + hostName + ":5672"
	return fmt.Sprintf("%s%s:%s@%s:%d", "amqp://", userName, password, hostName, port)
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
