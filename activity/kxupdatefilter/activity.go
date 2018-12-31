package kxupdatefilter

import (
	"fmt"
	"errors"
	"github.com/TIBCOSoftware/flogo-lib/core/activity"
	"github.com/TIBCOSoftware/flogo-lib/logger"
)

// activityLog is the default logger for the Log Activity
var activityLog = logger.GetLogger("activity-flogo-kxupdatefilter")

const (
	ivMessage   = "message"
	ivAddToFlow = "addToFlow"
	ivTriggerTag = "triggerTag"
	ivInputTag1 = "funcInputTag1"
	ivInputTag2 = "funcInputTag2"

	ovMessage = "message"
	ovOutputTag1 = "funcOutputTag1"
	ovOutputValue1 = "funcOutputValue1"
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

	message, _ := context.GetInput(ivMessage).(string)
	trIf,_ := context.GetSetting(ivTriggerTag)
	i1If,_ := context.GetSetting(ivInputTag1)
	i2If,_ := context.GetSetting(ivInputTag2)
	//o1If,_ := context.GetSetting(ovOutputTag1)
	triggerTag := trIf.(string)
	inputTag1 := i1If.(string)
	inputTag2 := i2If.(string)
	//outputTag1 := o1If.(string)
	addToFlow, _ := ToBool(context.GetInput(ivAddToFlow))
	var input1Value float64 
	var input2Value float64
	var output1Value float64
	var triggerObj KXRTPObject
	var input1Obj KXRTPObject 
	var input2Obj KXRTPObject 
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
	foundTrig:= false
	for _, rtPObject := range decodedMessage {
		activityLog.Info(fmt.Sprintf("Tag: %s", rtPObject.Tag))
		// 
		// Check if any of the received tags is the associated trigger
		//
		if rtPObject.Tag == triggerTag {
			activityLog.Info(fmt.Sprintf("Found %s in the trigger!", triggerTag))
			triggerObj = rtPObject
			foundTrig = true
		} 
		if rtPObject.Tag == inputTag1 {
			input1Obj = triggerObj
		}
		if triggerTag == inputTag2 {
			input2Obj = triggerObj
		}
	}
	if (foundTrig == true) {
		//
		// Trigger was found. Check if the inputs were also in the incoming message. Otherwise, read them from RTDB.
		//
		//
		// Open the RealTime DB
		//
		db, err := OpenRTDB("/home/mtorre/go/src/knox/kxdb/data.db")
		if err != nil {
			activityLog.Error(fmt.Sprintf("Realtime Database could not be opened. Error %s", err))
			return false, err
		}
		defer CloseRTDB(db)
		if input1Obj.Tag == "" {
			input1Obj, err = GetRTPObject(db, inputTag1)
			if (err != nil)	{
				activityLog.Error(fmt.Sprintf("Tag: %s could not be accessed from Realtime Database. Error %s", inputTag1, err))
			}
		}
		if input2Obj.Tag == "" {
			input2Obj, err = GetRTPObject(db, inputTag2)
			if (err != nil)	{
				activityLog.Error(fmt.Sprintf("Tag: %s could not be accessed from Realtime Database. Error %s", inputTag2, err))
			}
		}
		//
		// We should have the input values. Let's to the operation
		//
		input1Value = input1Obj.Cv.Value
		input2Value = input2Obj.Cv.Value
		output1Value = input1Value + input2Value
		activityLog.Info(fmt.Sprintf("Result: %f", output1Value))
	}
	if addToFlow {
		context.SetOutput(ovMessage, message)
	}
	return true, nil
}
