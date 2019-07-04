package amqpact

import (
	"errors"
	"fmt"
	"strconv"
	"github.com/TIBCOSoftware/flogo-lib/core/activity"
	"github.com/TIBCOSoftware/flogo-lib/logger"
	"github.com/mtorre-iot/flogo-contrib/activity/kxcommon"
	"github.com/streadway/amqp"
)

// log is the default package logger
var (
	activityLog = logger.GetLogger("activity-flogo-amqpact")
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
)
//
// AmqpActivity is simple AMQP Activity
type AmqpActivity struct {
	metadata       *activity.Metadata
	resExch        *kxcommon.AMQPExchange
}

func init() {
	activityLog.SetLogLevel(logger.DebugLevel)
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
		errMsg := fmt.Sprintf("[amqpact] Input '%s' not found.", attribute) 
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
			activityLog.Error(fmt.Sprintf("[amqpact] Response Exchange: Error converting \"Port\" to an integer. Error: %s", err.Error()))
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
		a.resExch = kxcommon.AMQPExchangeNew(responseHostName,
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
			errMsg := fmt.Sprintf("[amqpact] Response Exchange: Unable to Create Exchange Object: %s", a.resExch.ExchangeName)
			activityLog.Error(fmt.Sprintf("%s", errMsg))
			return false, errors.New(errMsg)
		}
		//
		// Create the AMQP Response Exchange
		//
		if err := a.resExch.Open(false); err != nil {
			activityLog.Error(fmt.Sprintf("[amqpact] Response Exchange: Unable to Open Exchange: %s : %s", a.resExch.ExchangeName, err))
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
			activityLog.Error(fmt.Sprintf("[amqpact] Response Exchange: Unable to Close Exchange: %s : %s", a.resExch.ExchangeName, err))
			return false, err
		}
	}
	return true, nil
}


func (a *AmqpActivity) PublishMessage(message string) error {

	err := a.resExch.Publish(message)
	if err != nil {
		// Timeout occurred
		activityLog.Error(fmt.Sprintf("[amqpact] Error occurred while trying to publish to Exchange '%s'", a.resExch.ExchangeName))
		return err
	}
	return nil
}
