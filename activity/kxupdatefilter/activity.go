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
	ivInputTags = "inputTags"
	ivFunctionName = "functionName"
	ovOutput = "outputStream"
)

func init() {
	activityLog.SetLogLevel(logger.InfoLevel)
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
	
	val := context.GetInput(ivInputTags)
	inputTags := val.(map[string]string)
	functionName := context.GetInput(ivFunctionName).(string)
	if functionName == "" {
		return false, errors.New("a function name must be provided")
	}

	var inputValues map[string]float64

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
	inputValues = make(map[string]float64)
	inputObjs = make(map[string]KXRTPObject)

	for _,tag := range inputTags {
			inputObjs[tag] = KXRTPObject{}
			inputValues[tag] = 0.0
	}

	for _, rtPObject := range decodedMessage {
		// 
		// Check if any of the received tags is the associated trigger
		//
		if rtPObject.Tag == triggerTag {
			activityLog.Info(fmt.Sprintf("Found %s in the trigger!", triggerTag))
			foundTrig = true
		} 

		for _, intag := range inputTags {
			if rtPObject.Tag == intag {
				inputObjs[intag] = rtPObject
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
		for key, pobj := range inputObjs {
			if pobj.Tag == "" {
				inputObjs[key], err = GetRTPObject(db, key)
				if (err != nil)	{
					activityLog.Error(fmt.Sprintf("Tag: %s could not be accessed from Realtime Database. Error %s", key, err))
					return false, err
				}
			}
		}
		//
		// We should have the input values. Let's to create the output argument message
		//
		args:= [] AnalyticsArg{}
		for _, pobj := range inputObjs {
			key,_ := Mapkey(inputTags, pobj.Tag)
			fmt.Printf("****************key: %s, tag: %s\n", key, pobj.Tag )
			args = append(args, AnalyticsArgNew(key, fmt.Sprintf("%f", pobj.Cv.Value), pobj.Cv.Quality.String()))
		}

		request := AnalyticsRequestNew(functionName, args)

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
