package kxreadrtdb

import (
	"fmt"
	"errors"
	"strings"
	"strconv"
	"github.com/TIBCOSoftware/flogo-lib/core/activity"
	"github.com/TIBCOSoftware/flogo-lib/logger"
	"github.com/mtorre-iot/flogo-contrib/activity/kxcommon"
)

// activityLog is the default logger for the Log Activity
var activityLog = logger.GetLogger("activity-flogo-kxreadrtdb")

const (
	ivInputTags = "inputTags"
	ivRTDBFile = "RTDBFile"
	ivFunctionName = "functionName"
	ovOutput = "outputStream"
)

func init() {
	activityLog.SetLogLevel(logger.InfoLevel)
}

// KXReadRTDBActivity is an Activity that is used to deserialize messages from KXDataProc, to get changes to invoke other activities 
type KXReadRTDBActivity struct {
	metadata *activity.Metadata
}

// NewActivity creates a new AppActivity
func NewActivity(metadata *activity.Metadata) activity.Activity {
	return &KXReadRTDBActivity{metadata: metadata}
}

// Metadata returns the activity's metadata
func (a *KXReadRTDBActivity) Metadata() *activity.Metadata {
	return a.metadata
}

// Eval implements api.Activity.Eval - Logs the Message
func (a *KXReadRTDBActivity) Eval(context activity.Context) (done bool, err error) {

	rtdbFile := context.GetInput(ivRTDBFile).(string)
	
	val := context.GetInput(ivInputTags)
	inputTagsInterface := val.(map[string]interface{})
	inputTags := make(map[string]string) 

	for key, value := range inputTagsInterface {
    	strKey := fmt.Sprintf("%v", key)
        strValue := fmt.Sprintf("%v", value)

        inputTags[strKey] = strValue
    }

	functionName := context.GetInput(ivFunctionName).(string)
	if functionName == "" {
		return false, errors.New("[kxreadrtdb] A function name must be provided")
	}

	inputValues := make(map[string]float64)
	inputObjs := make(map[string]kxcommon.KXRTPObject)

	for _,tag := range inputTags {
			inputObjs[tag] = kxcommon.KXRTPObject{}
			inputValues[tag] = 0.0
	}

	// decode (unmarshall) the RTDB server pars
	rtdbPars := strings.Split(rtdbFile,":")
	// create the realtime DB access object
	var rtdb kxcommon.RTDB
	port, err := strconv.Atoi(rtdbPars[1])
	if err != nil {
		return false, err
	}
	rtdb.RTDBNew(rtdbPars[0], port, rtdbPars[2],rtdbPars[3], 0, "json") 
	// Open the RealTime DB
	err = rtdb.OpenRTDB()
	if err != nil {
		activityLog.Error(fmt.Sprintf("[kxreadrtdb] Realtime Database could not be opened. Error %s", err))
		return false, err
	}
	// make sure it closes after finish
	defer rtdb.CloseRTDB()
	// check all tags
	for key, pobj := range inputObjs {
		if pobj.Tag == "" {
			inputObjs[key], err = rtdb.GetRTPObject(key)
			if (err != nil)	{
				activityLog.Error(fmt.Sprintf("[kxreadrtdb] Tag: %s could not be accessed from Realtime Database. Error %s", key, err))
				return false, err
			}
		}
	}
	//
	// We should have the input values. Let's create the output argument message
	//
	args:= [] kxcommon.AnalyticsArg{}
	for _, pobj := range inputObjs {
		key,_ := kxcommon.Mapkey(inputTags, pobj.Tag)
		args = append(args, kxcommon.AnalyticsArgNew(key, fmt.Sprintf("%f", pobj.Cv.Value), pobj.Cv.Quality.String()))
	}
	request := kxcommon.AnalyticsRequestNew(functionName, args)

	requestJson, err := kxcommon.SerializeObject(request)
	if (err != nil) {
		activityLog.Error(fmt.Sprintf("[kxreadrtdb] Error trying to serialize analytics request message. Error %s", err))
		return false, err
	}
	activityLog.Debugf("[kxreadrtdb] Output Message: %s", requestJson)
	context.SetOutput(ovOutput, requestJson)

	return true, nil
}
