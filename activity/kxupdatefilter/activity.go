package kxupdatefilter

import (
	"fmt"
	"time"
	"errors"
	"github.com/TIBCOSoftware/flogo-lib/core/activity"
	"github.com/TIBCOSoftware/flogo-lib/logger"
)

// activityLog is the default logger for the Log Activity
var activityLog = logger.GetLogger("activity-flogo-kxupdatefilter")

const (
	ivMessage   = "message"
	ivTriggerTag = "triggerTag"
	ivInputTag1 = "inputTag1"
	ivInputTag2 = "inputTag2"

	ovOutput = "outputVal"
	ovOutputTag1 = "outputTag1"
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
	triggerTag,_ := context.GetInput(ivTriggerTag).(string)
	inputTag1,_ := context.GetInput(ivInputTag1).(string)
	inputTag2,_ := context.GetInput(ivInputTag2).(string)
	outputTag1,_ := context.GetInput(ovOutputTag1).(string)

	var input1Value float64 
	var input2Value float64
	var output1Value float64
	var triggerObj KXRTPObject
	var input1Obj KXRTPObject 
	var input2Obj KXRTPObject 
	var output1Obj KXRTPObject
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
	ok := true
	if (foundTrig == true) {
		//
		// Trigger was found. Check if the inputs were also in the incoming message. Otherwise, read them from RTDB.
		// Open the RealTime DB
		//
		db, err := OpenRTDB("/home/mtorre/go/src/knox/kxdb/data.db")
		if err != nil {
			activityLog.Error(fmt.Sprintf("Realtime Database could not be opened. Error %s", err))
			return false, err
		}
		activityLog.Info("RTDB opened")
		if input1Obj.Tag == "" {
			input1Obj, err = GetRTPObject(db, inputTag1)
			if (err != nil)	{
				activityLog.Error(fmt.Sprintf("Tag: %s could not be accessed from Realtime Database. Error %s", inputTag1, err))
				ok = false
			}
		}
		if input2Obj.Tag == "" {
			input2Obj, err = GetRTPObject(db, inputTag2)
			if (err != nil)	{
				activityLog.Error(fmt.Sprintf("Tag: %s could not be accessed from Realtime Database. Error %s", inputTag2, err))
				ok = false
			}
		}
		//
		// Get the Output object
		//
		output1Obj, err = GetRTPObject(db, outputTag1)
		if (err != nil)	{
			activityLog.Error(fmt.Sprintf("Tag: %s could not be accessed from Realtime Database. Error %s", outputTag1, err))
			ok = false
		}
		//
		// Close RTDB
		//
		err = CloseRTDB(db)
		if err != nil {
			activityLog.Error(fmt.Sprintf("Realtime Database could not be closed. Error %s", err))
		}
		activityLog.Info("RTDB closed")
		if ok == false {
			return false, errors.New("Error found accessing RTDB")
		}
		//
		// We should have the input values. Let's to the operation
		//
		input1Value = input1Obj.Cv.Value
		input2Value = input2Obj.Cv.Value
		output1Value = input1Value + input2Value
		//
		// Create the json scan message back to KXDataproc
		//
		scanMessage := ScanMessageNew()
		smu := ScanMessageUnitNew(output1Obj.ID, outputTag1, fmt.Sprintf("%f", output1Value), QualityOk.String(), MessageUnitTypeValue, time.Now().UTC())
		scanMessage.ScanMessageAdd(smu)
		jsonMessage, err := SerializeObject(scanMessage)
		if err != nil {
			activityLog.Error(fmt.Sprintf("Error trying to serialize output message. Error %s", err))
			return false, err
		}
		activityLog.Info(fmt.Sprintf("Output Message: %s", jsonMessage))
		context.SetOutput(ovOutput, jsonMessage)
	}
	return true, nil
}
