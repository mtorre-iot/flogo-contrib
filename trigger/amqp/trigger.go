package amqp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

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
	rsHostName     = "responsehostName"
	rsPort         = "responsePort"
	rsExchangeName = "responseExchangeName"
	rsExchangeType = "responseExchangeType"
	rsRoutingKey   = "responseRoutingKey"
	rsUser         = "responseUser"
	rsPassword     = "responsePassword"
	rsDurable      = "responseDurable"
	rsAutoDelete   = "responseAutoDelete"
	rsReliable     = "responseReliable"

	reqExch *AMQPExchange
	resExch *AMQPExchange
	tr      *AmqpTrigger
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
	port, err := strconv.Atoi(t.config.GetSetting(ivPort))
	if err != nil {
		log.Error("Request Exchange: Error converting \"Port\" to an integer ", err.Error())
		return err
	}
	exchangeName := t.config.GetSetting(ivExchangeName)
	queueName := t.config.GetSetting(ivQueueName)
	exchangeType := t.config.GetSetting(ivExchangeType)
	routingKey := t.config.GetSetting(ivRoutingKey)
	user := t.config.GetSetting(ivUser)
	password := t.config.GetSetting(ivPassword)
	reliable, err := data.CoerceToBoolean(t.config.Settings[ivReliable])
	if err != nil {
		log.Error("Request Exchange: Error converting \"Reliable\" to a boolean ", err.Error())
		return err
	}
	durable, err := data.CoerceToBoolean(t.config.Settings[ivDurable])
	if err != nil {
		log.Error("Request Exchange: Error converting \"Durable\" to a boolean ", err.Error())
		return err
	}
	autoDelete, err := data.CoerceToBoolean(t.config.Settings[ivAutoDelete])
	if err != nil {
		log.Error("Request Exchange: Error converting \"AutoDelete\" to a boolean ", err.Error())
		return err
	}

	responseHostName := t.config.GetSetting(rsHostName)
	responsePort, err := strconv.Atoi(t.config.GetSetting(rsPort))
	if err != nil {
		log.Error("Response Exchange: Error converting \"Port\" to an integer ", err.Error())
		return err
	}
	responseExchangeName := t.config.GetSetting(rsExchangeName)
	responseExchangeType := t.config.GetSetting(rsExchangeType)
	responseRoutingKey := t.config.GetSetting(rsRoutingKey)
	responseUser := t.config.GetSetting(rsUser)
	responsePassword := t.config.GetSetting(rsPassword)
	responseReliable, err := data.CoerceToBoolean(t.config.Settings[rsReliable])
	if err != nil {
		log.Error("Response Exchange: Error converting \"Reliable\" to a boolean ", err.Error())
		return err
	}
	responseDurable, err := data.CoerceToBoolean(t.config.Settings[rsDurable])
	if err != nil {
		log.Error("Response Exchange: Error converting \"Durable\" to a boolean ", err.Error())
		return err
	}
	responseAutoDelete, err := data.CoerceToBoolean(t.config.Settings[rsAutoDelete])
	if err != nil {
		log.Error("Response Exchange: Error converting \"AutoDelete\" to a boolean ", err.Error())
		return err
	}
	log.Info(responseHostName, responsePort, responseExchangeName, responseExchangeType, responseUser, responsePassword, responseReliable, responseDurable, responseAutoDelete)
	//
	//	Create the request exchange object
	//
	reqExch := AMQPExchangeNew(hostName, port, exchangeName, exchangeType, queueName, routingKey, user, password, durable, autoDelete, reliable)
	if reqExch == nil {
		errMsg := fmt.Sprintf("Request Exchange: Unable to Create Exchange Object: %s", reqExch.ExchangeName)
		log.Error(errMsg)
		return errors.New(errMsg)
	}
	//
	// Create the AMQP Request Exchange
	//
	if err := reqExch.Open(true); err != nil {
		log.Errorf("Request Exchange: Unable to Open Exchange: %s : %s", reqExch.ExchangeName, err)
		return err
	}
	//
	// Prepare to receive
	//
	if err := reqExch.PrepareReceiveFunc(receiverHandler); err != nil {
		log.Errorf("Request Exchange: Unable to Prepare: %s to Receive. Error: %s. Bail out", err)
		return err
	}
	//
	//	Create the response exchange object
	//
	if responseHostName != "" {
		resExch := AMQPExchangeNew(responseHostName,
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

		if resExch == nil {
			errMsg := fmt.Sprintf("Response Exchange: Unable to Create Exchange Object: %s", reqExch.ExchangeName)
			log.Error(errMsg)
			return errors.New(errMsg)
		}
		//
		// Create the AMQP Response Exchange
		//
		if err := resExch.Open(false); err != nil {
			log.Errorf("Response Exchange: Unable to Open Exchange: %s : %s", reqExch.ExchangeName, err)
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

func receiverHandler(msgs <-chan amqp.Delivery) {
	for d := range msgs {
		payload := fmt.Sprintf("%s", d.Body)
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
	reqExch.Close()
	resExch.Close()
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
	replyData = "OK"
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

	log.Info("ReplyTo topic: ", topic)
	log.Info("Publishing message: ", message)

	if len(topic) == 0 {
		log.Warn("Invalid empty topic to publish to")
		return
	}
	err := resExch.Publish(message)
	if err != nil {
		// Timeout occurred
		log.Errorf("Error occurred while trying to publish to Exchange '%s'", resExch.ExchangeName)
		return
	}
}
