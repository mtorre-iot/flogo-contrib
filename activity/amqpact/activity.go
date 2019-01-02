package amqpact

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"

	//"github.com/TIBCOSoftware/flogo-lib/core/data"
	"github.com/TIBCOSoftware/flogo-lib/core/activity"
	"github.com/TIBCOSoftware/flogo-lib/logger"
	"github.com/streadway/amqp"
)

// log is the default package logger
var (
	activityLog = logger.GetLogger("activity-flogo-activity")
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
	ivInputMessage = "inputMessage"
	ovMessage	   = "outputMessage"
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
// AmqpActivity is simple AMQP Activity
type AmqpActivity struct {
	metadata       *activity.Metadata
	resExch        *AMQPExchange
}

func init() {
	activityLog.SetLogLevel(logger.InfoLevel)
}

// NewActivity creates a new AppActivity
func NewActivity(md *activity.Metadata) activity.Activity {
	return &AmqpActivity{metadata: md}
}

// Metadata returns the activity's metadata
func (a *AmqpActivity) Metadata() *activity.Metadata {
	return a.metadata
}

func CheckParameter (context activity.Context, attribute string) (string, error) {
	param := context.GetInput(attribute).(string)
	if param == "" {
		errMsg := fmt.Sprintf("Input '%s' not found.", attribute) 
		return param, errors.New(errMsg)
	}
	return param, nil
}

// Eval implements api.Activity.Eval - Logs the Message
func (a *AmqpActivity) Eval(context activity.Context) (done bool, err error) {
	//
	// Response configuration is mandatory
	//
	responseHostName,_ := CheckParameter(context, rsHostName)
	responsePortStr,_  := CheckParameter(context, rsPort)
	responsePort := 5672
	if (responsePortStr != "") {
		responsePort, err = strconv.Atoi(responsePortStr)
		if err != nil {
			activityLog.Error(fmt.Sprintf("Response Exchange: Error converting \"Port\" to an integer. Error: %s", err.Error()))
			return false, err
		}
	}
	responseExchangeName,_ := CheckParameter(context, rsExchangeName)
	responseExchangeType,_ := CheckParameter(context, rsExchangeType)
	responseRoutingKey,_ := CheckParameter(context, rsRoutingKey)
	responseUser,_ := CheckParameter(context, rsUser)
	responsePassword,_ := CheckParameter(context,rsPassword)

	responseReliable := context.GetInput(rsReliable).(bool)
	responseDurable := context.GetInput(rsDurable).(bool)
	responseAutoDelete := context.GetInput(rsAutoDelete).(bool)
	//
	// Get message 
	//
	message :=  context.GetInput(ivInputMessage).(string)
	//
	//	Create the response exchange object
	//
	if message != "" {
		a.resExch = AMQPExchangeNew(responseHostName,
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

		if a.resExch == nil {
			errMsg := fmt.Sprintf("Response Exchange: Unable to Create Exchange Object: %s", a.resExch.ExchangeName)
			activityLog.Error(fmt.Sprintf("%s", errMsg))
			return false, errors.New(errMsg)
		}
		//
		// Create the AMQP Response Exchange
		//
		if err := a.resExch.Open(false); err != nil {
			activityLog.Error(fmt.Sprintf("Response Exchange: Unable to Open Exchange: %s : %s", a.resExch.ExchangeName, err))
			return false, err
		}
		//
		// publish the message
		//
		err := a.PublishMessage(message)
		if (err != nil) {
			return false, err
		}
		context.SetOutput(ovMessage, message)
		//
		// Close the Response Exchange
		//
		if err:= a.resExch.Close(); err != nil {
			activityLog.Error(fmt.Sprintf("Response Exchange: Unable to Close Exchange: %s : %s", a.resExch.ExchangeName, err))
			return false, err
		}
	}
	return true, nil
}


func (a *AmqpActivity) PublishMessage(message string) error {

	err := a.resExch.Publish(message)
	if err != nil {
		// Timeout occurred
		activityLog.Error(fmt.Sprintf("Error occurred while trying to publish to Exchange '%s'", a.resExch.ExchangeName))
		return err
	}
	return nil
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
