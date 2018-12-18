package amqp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/TIBCOSoftware/flogo-lib/core/data"
	"github.com/TIBCOSoftware/flogo-lib/core/trigger"
	"github.com/TIBCOSoftware/flogo-lib/logger"
	"github.com/eclipse/paho.mqtt.golang"
	"github.com/streadway/amqp"
)

// log is the default package logger
var (
	log            = logger.GetLogger("trigger-flogo-amqp")
	ivHostName     = "hostName"
	ivPort         = "port"
	ivExchangeName = "exchangeName"
	ivQueueName    = "queueName"
	ivExchangeType = "exchangeType"
	ivRoutingKey   = "routingKey"
	ivTopic        = "topic"
	ivDurable      = "durable"
	ivAutoDelete   = "autoDelete"
	ivReliable     = "reliable"
	ivUser         = "user"
	ivPassword     = "password"
	exch           *AMQPExchange
	tr             *AmqpTrigger
)

// AmqpTrigger is simple AMQP trigger
type AmqpTrigger struct {
	metadata       *trigger.Metadata
	client         mqtt.Client
	config         *trigger.Config
	handlers       []*trigger.Handler
	topicToHandler map[string]*trigger.Handler
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

// Start implements trigger.Trigger.Start
func (t *AmqpTrigger) Start() error {
	//
	tr = t
	hostName := t.config.GetSetting(ivHostName)
	port := t.config.GetSetting(ivPort)
	exchangeName := t.config.GetSetting(ivExchangeName)
	queueName := t.config.GetSetting(ivQueueName)
	exchangeType := t.config.GetSetting(ivExchangeType)
	routingKey := t.config.GetSetting(ivRoutingKey)
	user := t.config.GetSetting(ivUser)
	password := t.config.GetSetting(ivPassword)
	reliable, err := data.CoerceToBoolean(t.config.Settings[ivReliable])
	if err != nil {
		log.Error("Error converting \"Reliable\" to a boolean ", err.Error())
		return err
	}
	durable, err := data.CoerceToBoolean(t.config.Settings[ivDurable])
	if err != nil {
		log.Error("Error converting \"Durable\" to a boolean ", err.Error())
		return err
	}
	autoDelete, err := data.CoerceToBoolean(t.config.Settings[ivAutoDelete])
	if err != nil {
		log.Error("Error converting \"AutoDelete\" to a boolean ", err.Error())
		return err
	}
	//
	// Initialize URI
	//
	err = AMQPInit("amqp://", port)
	if err != nil {
		log.Errorf("Error trying to configure AMQP URI. %s", err.Error())
	}
	//
	//	Create the exchange object
	//
	exch := AMQPExchangeNew(hostName, exchangeName, exchangeType, queueName, routingKey, user, password, durable, autoDelete, reliable)
	if exch == nil {
		errMsg := fmt.Sprintf("Unable to Create Exchange Object: %s", exch.ExchangeName)
		log.Error(errMsg)
		return errors.New(errMsg)
	}
	//
	// Create the AMQP exchange
	//
	if err := exch.Open(true); err != nil {
		log.Errorf("Unable to Open Exchange: %s : %s", exch.ExchangeName, err)
		return err
	}
	//
	// Prepare to receive
	//
	if err := exch.PrepareReceiveFunc(receiverHandler); err != nil {
		log.Errorf("Unable to Prepare: %s to Receive. Error: %s. Bail out", err)
		return err
	}

	t.topicToHandler = make(map[string]*trigger.Handler)

	for _, handler := range t.handlers {
		topic := handler.GetStringSetting("topic")
		t.topicToHandler[topic] = handler
	}

	return nil
}

func receiverHandler(msgs <-chan amqp.Delivery) {
	for d := range msgs {
		payload := fmt.Sprintf("%s", d.Body)
		//exch.Messages = append(exch.Messages, strm)
		log.Infof("Message received: %s", payload)
		topic := fmt.Sprintf("%s", d.RoutingKey)
		handler, found := tr.topicToHandler[topic]
		if found {
			tr.RunHandler(handler, payload)
		} else {
			log.Errorf("handler for topic '%s' not found", topic)
		}
	}
}

// Stop implements ext.Trigger.Stop
func (t *AmqpTrigger) Stop() error {
	//Close Exchange
	exch.Close()
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

	log.Infof("Ran Handler: [%s], Results: %s", handler, results["data"])

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
			replyTo := handler.GetStringSetting("topic")
			if replyTo != "" {
				t.publishMessage(replyTo, string(dataJson))
			}
		}
	}
}

func (t *AmqpTrigger) publishMessage(topic string, message string) {

	log.Debug("ReplyTo topic: ", topic)
	log.Debug("Publishing message: ", message)

	qos, err := strconv.Atoi(t.config.GetSetting("qos"))
	if err != nil {
		log.Error("Error converting \"qos\" to an integer ", err.Error())
		return
	}
	if len(topic) == 0 {
		log.Warn("Invalid empty topic to publish to")
		return
	}
	token := t.client.Publish(topic, byte(qos), false, message)
	sent := token.WaitTimeout(5000 * time.Millisecond)
	if !sent {
		// Timeout occurred
		log.Errorf("Timeout occurred while trying to publish to topic '%s'", topic)
		return
	}
}
