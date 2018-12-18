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
	rqHostName     = "requestHostName"
	rqPort         = "requestPort"
	rqExchangeName = "requestExchangeName"
	rqQueueName    = "requestQueueName"
	rqExchangeType = "requestExchangeType"
	rqRoutingKey   = "requestRoutingKey"
	rqTopic        = "requestTopic"
	rqDurable      = "requestDurable"
	rqAutoDelete   = "requestAutoDelete"
	rqReliable     = "requestReliable"
	rqUser         = "requestUser"
	rqPassword     = "requestPassword"
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
)

// AmqpTrigger is simple AMQP trigger
type AmqpTrigger struct {
	metadata       *trigger.Metadata
	client         mqtt.Client
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

// Start implements trigger.Trigger.Start
func (t *AmqpTrigger) Start() error {
	//
	requestHostName := t.config.GetSetting(rqHostName)
	requestPort, err := strconv.Atoi(t.config.GetSetting(rqPort))
	if err != nil {
		log.Error("Request Exchange: Error converting \"Port\" to an integer ", err.Error())
		return err
	}
	requestExchangeName := t.config.GetSetting(rqExchangeName)
	requestQueueName := t.config.GetSetting(rqQueueName)
	requestExchangeType := t.config.GetSetting(rqExchangeType)
	requestRoutingKey := t.config.GetSetting(rqRoutingKey)
	requestUser := t.config.GetSetting(rqUser)
	requestPassword := t.config.GetSetting(rqPassword)
	requestReliable, err := data.CoerceToBoolean(t.config.Settings[rqReliable])
	if err != nil {
		log.Error("Request Exchange: Error converting \"Reliable\" to a boolean ", err.Error())
		return err
	}
	requestDurable, err := data.CoerceToBoolean(t.config.Settings[rqDurable])
	if err != nil {
		log.Error("Request Exchange: Error converting \"Durable\" to a boolean ", err.Error())
		return err
	}
	requestAutoDelete, err := data.CoerceToBoolean(t.config.Settings[rqAutoDelete])
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
	
	log.Infof("Error %s", err)
	 
	for res := range results {
		log.Infof("Results: %v", res)
	}

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
	err := t.resExch.Publish(message)
	if err != nil {
		// Timeout occurred
		log.Errorf("Error occurred while trying to publish to Exchange '%s'", t.resExch.ExchangeName)
		return
	}
}
