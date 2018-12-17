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
	ivUri          = "uri"
	ivExchangeName = "exchangeName"
	ivExchangeType = "exchangeType"
	ivRoutingKey   = "routingKey"
	ivBody         = "body"
	ivReliable     = "reliable"
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
	log.Info("NewFactory")
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
	uri := t.config.GetSetting(ivUri)
	log.Info(uri)
	exchangeName := t.config.GetSetting(ivExchangeName)
	log.Info(exchangeName)
	exchangeType := t.config.GetSetting(ivExchangeType)
	log.Info(exchangeType)
	routingKey := t.config.GetSetting(ivRoutingKey)
	log.Info(routingKey)
	body := t.config.GetSetting(ivBody)
	log.Info(body)
	reliable, err := data.CoerceToBoolean(t.config.Settings[ivReliable])
	if err != nil {
		log.Error("Error converting \"ivReliable\" to a boolean ", err.Error())
		return err
	}
	log.Info(reliable)

	//
	// Initialize URI
	//
	AMQPInit("amqp://", 5672)
	//
	//	Create the exchange object
	//
	exch := AMQPExchangeNew("localhost", "exchange1", "topic", "queue1", "#", "mqtt", "mqtt", false, true, true)
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

	//opts := mqtt.NewClientOptions()
	//opts.AddBroker(t.config.GetSetting("broker"))
	///opts.SetClientID(t.config.GetSetting("id"))
	///opts.SetUsername(t.config.GetSetting("user"))
	//opts.SetPassword(t.config.GetSetting("password"))
	//b, err := data.CoerceToBoolean(t.config.Settings["cleansess"])
	//if err != nil {
	//	log.Error("Error converting \"cleansess\" to a boolean ", err.Error())
	//	return err
	//}
	//opts.SetCleanSession(b)
	//if storeType := t.config.Settings["store"]; storeType != ":memory:" {
	//	opts.SetStore(mqtt.NewFileStore(t.config.GetSetting("store")))
	//}

	//opts.SetDefaultPublishHandler(func(client mqtt.Client, msg mqtt.Message) {
	//	topic := msg.Topic()
	//	//TODO we should handle other types, since mqtt message format are data-agnostic
	//	payload := string(msg.Payload())
	//	log.Debug("Received msg:", payload)
	//	handler, found := t.topicToHandler[topic]
	//	if found {
	//		t.RunHandler(handler, payload)
	//	} else {
	//		log.Errorf("handler for topic '%s' not found", topic)
	//	}
	//})

	//client := mqtt.NewClient(opts)
	//t.client = client
	//if token := client.Connect(); token.Wait() && token.Error() != nil {
	//	panic(token.Error())
	//}

	//i, err := data.CoerceToDouble(t.config.Settings["qos"])
	//if err != nil {
	//	log.Error("Error converting \"qos\" to an integer ", err.Error())
	//	return err
	//}

	//t.topicToHandler = make(map[string]*trigger.Handler)

	//for _, handler := range t.handlers {

	//	topic := handler.GetStringSetting("topic")

	//	if token := t.client.Subscribe(topic, byte(i), nil); token.Wait() && token.Error() != nil {
	//		log.Errorf("Error subscribing to topic %s: %s", topic, token.Error())
	//		return token.Error()
	//	} else {
	//		log.Debugf("Subscribed to topic: %s, will trigger handler: %s", topic, handler)
	//		t.topicToHandler[topic] = handler
	//	}
	//}

	return nil
}

func receiverHandler(msgs <-chan amqp.Delivery) {
	for d := range msgs {
		payload := fmt.Sprintf("%s", d.Body)
		//exch.Messages = append(exch.Messages, strm)
		log.Infof("Message received: %s", payload)
		topic := "flogo/#"
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
	log.Info("Stop")
	//unsubscribe from topic
	for _, handlerCfg := range t.config.Handlers {
		log.Debug("Unsubscribing from topic: ", handlerCfg.GetSetting("topic"))
		if token := t.client.Unsubscribe(handlerCfg.GetSetting("topic")); token.Wait() && token.Error() != nil {
			log.Errorf("Error unsubscribing from topic %s: %s", handlerCfg.Settings["topic"], token.Error())
		}
	}

	t.client.Disconnect(250)

	return nil
}

// RunHandler runs the handler and associated action
func (t *AmqpTrigger) RunHandler(handler *trigger.Handler, payload string) {
	log.Info("RunHandler")
	trgData := make(map[string]interface{})
	trgData["message"] = payload

	results, err := handler.Handle(context.Background(), trgData)

	if err != nil {
		log.Error("Error starting action: ", err.Error())
	}

	log.Debugf("Ran Handler: [%s]", handler)

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

	log.Info("PublishMessage")
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
