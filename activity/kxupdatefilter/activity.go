package kxupdatefilter

import (
	"fmt"
	"errors"
	"encoding/json"
	"github.com/TIBCOSoftware/flogo-lib/core/activity"
	"github.com/TIBCOSoftware/flogo-lib/logger"
)

// activityLog is the default logger for the Log Activity
var activityLog = logger.GetLogger("activity-flogo-kxupdatefilter")

const (
	ivMessage   = "message"
	ivAddToFlow = "addToFlow"

	ovMessage = "message"
)

func init() {
	activityLog.SetLogLevel(logger.InfoLevel)
}

// KXUpdateFilterActivity is an Activity that is used to deserialize messages from KXDataProc, to get changes to invoke other activities 
// inputs : {message}
// outputs: none
type KXUpdateFilterActivity struct {
	metadata *activity.Metadata
}

// NewActivity creates a new AppActivity
func NewActivity(metadata *activity.Metadata) activity.Activity {
	return &KXUpdateFilterActivity{metadata: metadata}
}

// Metadata returns the activity's metadata
func (a *KXUpdateFilterActivity) Metadata() *activity.Metadata {
	return a.metadata
}

// Eval implements api.Activity.Eval - Logs the Message
func (a *KXUpdateFilterActivity) Eval(context activity.Context) (done bool, err error) {

	//mv := context.GetInput(ivMessage)
	message, _ := context.GetInput(ivMessage).(string)
	addToFlow, _ := toBool(context.GetInput(ivAddToFlow))
	//
	// decode it from Json
	//
	decodedMessage := DecodeUpdateMessage(message)
	if (decodedMessage == nil) {
		return false, errors.New("Incoming message could not be deserialized. Message: " + message)
	}
	//
	// test - print the tags
	//
	for _, rtPObject := range decodedMessage {
		activityLog.Info(fmt.Sprintf("Tag: %s", rtPObject.Tag))
	}

	if addToFlow {
		context.SetOutput(ovMessage, message)
	}
	return true, nil
}


// DecodeUpdateMessage get messages coming from a KXDataProc
func DecodeUpdateMessage (message string) []KXRTPObject {

	rtn := make([]KXRTPObject, 0)
	var updateMessage []KXRTPObject 
	// decode message
	if err := json.Unmarshal([]byte(message), &updateMessage); err != nil {
		return nil
	}
	return rtn
}
