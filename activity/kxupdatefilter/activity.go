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
	ivTriggerTag = "triggerTag"
//	ivInputTag1 = "inputTag1"
//	ivInputTag2 = "inputTag2"
	ivInputTags = "inputTags"
	ovOutput = "outputStream"
)

func init() {
	activityLog.SetLogLevel(logger.DebugLevel)
}

// KXUpdateFilterActivity is an Activity that is used to deserialize messages from KXDataProc, to get changes to invoke other activities 
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
	//inputTag1,_ := context.GetInput(ivInputTag1).(string)
	//inputTag2,_ := context.GetInput(ivInputTag2).(string)
	
	val := context.GetInput(ivInputTags)
	inputTags := val.(map[string]string)

	//var input1Value float64 
	//var input2Value float64
	var inputValues map[string]float64

	var triggerObj KXRTPObject
	//var input1Obj KXRTPObject 
	//var input2Obj KXRTPObject
	var inputObjs map[string]KXRTPObject 
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
	inputValues = make(map[string]float64, len(inputTags))
	inputObjs = make(map[string]KXRTPObject, len(inputTags))

	for key := range inputTags {
			inputObjs[key] = KXRTPObject{}
			inputValues[key] = 0.0
	}

	for key, pobj := range inputObjs {
		fmt.Printf("key: %s, tag: %s", key, pobj.Tag)
	}

	for _, rtPObject := range decodedMessage {
		// 
		// Check if any of the received tags is the associated trigger
		//
		if rtPObject.Tag == triggerTag {
			activityLog.Info(fmt.Sprintf("Found %s in the trigger!", triggerTag))
			triggerObj = rtPObject
			foundTrig = true
		} 
/* 		if rtPObject.Tag == inputTag1 {
			input1Obj = triggerObj
		}
		if triggerTag == inputTag2 {
			input2Obj = triggerObj
		} */

		for key, intag := range inputTags {
			if rtPObject.Tag == intag {
				inputObjs[key] = triggerObj
			}
		}
	}
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
		// make sure it closes after finish
		defer CloseRTDB(db)
		// check all tags
/* 		if input1Obj.Tag == "" {
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
		} */
		for key, pobj := range inputObjs {
			if pobj.Tag == "" {
				inputObjs[key], err = GetRTPObject(db, key)
				if (err != nil)	{
					activityLog.Error(fmt.Sprintf("Tag: %s could not be accessed from Realtime Database. Error %s", pobj.Tag, err))
					return false, err
				}
			}
		}
		//
		// We should have the input values. Let's to create the output argument message
		//
		//input1Value = input1Obj.Cv.Value
		//input2Value = input2Obj.Cv.Value
		// {"func": "process1", "args":[{"name": "arg1","value": "1234", "quality": "OK"},{ "name": "arg2", "value":"222", "quality": "OK"}]}
		// Create the request message
		//
		//args:= [] AnalyticsArg{}
		//args = append(args, AnalyticsArgNew("arg1", fmt.Sprintf("%f", input1Value), input1Obj.Cv.Quality.String()))
		//args = append(args, AnalyticsArgNew("arg2", fmt.Sprintf("%f", input2Value), input1Obj.Cv.Quality.String()))

		args:= [] AnalyticsArg{}
		cnt := 1
		for _, pobj := range inputObjs {
			args = append(args, AnalyticsArgNew(fmt.Sprintf("arg%d", cnt), fmt.Sprintf("%f", pobj.Cv.Value), pobj.Cv.Quality.String()))
			cnt++
		}

		request := AnalyticsRequestNew("process1", args)

		requestJson, err := SerializeObject(request)
		if (err != nil) {
			activityLog.Error(fmt.Sprintf("Error trying to serialize analytics request message. Error %s", err))
			return false, err
		}
		activityLog.Info(fmt.Sprintf("Output Message: %s", requestJson))
		context.SetOutput(ovOutput, requestJson)
	}
	return foundTrig, nil
}
