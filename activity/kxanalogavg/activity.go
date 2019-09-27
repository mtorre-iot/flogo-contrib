package kxanalogavg

import (
	//"fmt"
	//"errors"
	//"strings"
	//"strconv"
	"github.com/TIBCOSoftware/flogo-lib/core/activity"
	"github.com/TIBCOSoftware/flogo-lib/logger"
	//"github.com/mtorre-iot/flogo-contrib/activity/kxcommon"
)

// activityLog is the default logger for the Log Activity
var activityLog = logger.GetLogger("activity-flogo-kxanalogavg")

const (
	ivpObjectConfigFile = "pObjectConfigFile"
	ivInputTags = "inputTags"
	ivOutputTags = "outputTags"
	ivTSDB = "TSDB"
	ovOutput = "outputStream"
)

func init() {
	activityLog.SetLogLevel(logger.InfoLevel)
}

// KXAnalogAvgActivity is an Activity that is used get time stamp data from tags, and calculate averages 
type KXAnalogAvgActivity struct {
	metadata *activity.Metadata
}

// NewActivity creates a new AppActivity
func NewActivity(metadata *activity.Metadata) activity.Activity {
	return &KXAnalogAvgActivity{metadata: metadata}
}

// Metadata returns the activity's metadata
func (a *KXAnalogAvgActivity) Metadata() *activity.Metadata {
	return a.metadata
}

// Eval implements api.Activity.Eval - Logs the Message
func (a *KXAnalogAvgActivity) Eval(context activity.Context) (done bool, err error) {
/* 
	tsdbString := context.GetInput(ivTSDB).(string)
	pObjectConfigFile := context.GetInput(ivpObjectConfigFile).(string)
	inputTags := context.GetInput(ivInputTags)
	outputTags := context.GetInput(ivOutputTags) */

	// decode (unmarshall) the TSDB server pars
	//<hostname>:<port>:<userName>:<password>:<databaseNane>:<historyTable>:<precision>
	
/* 	tsdbPars := strings.Split(tsdbString,":")
	// create the Time Stamp DB access object

	port, err := strconv.Atoi(tsdbPars[1])
	if err != nil {
		return false, err
	}
	tsdb := kxcommon.TSDBNew(tsdbPars[0], port, tsdbPars[2],tsdbPars[3]) 
	// Open the TSDB
	err = tsdb.OpenTSDB()
	if err != nil {
		activityLog.Error(fmt.Sprintf("[kxanalogavg] Time Stamp Database could not be opened. Error %s", err))
		return false, err
	}
	// make sure it closes after finish
	defer tsdb.CloseTSDB()
	// check all tags
	_ = inputTags
	_ =outputTags
	_ =pObjectConfigFile */
/* 	for key, pobj := range inputObjs {
		if pobj.Tag == "" {
						inputObjs[key], err = rtdb.GetRTPObject(key)
			if (err != nil)	{
				activityLog.Error(fmt.Sprintf("[kxanalogavg] Tag: %s could not be accessed from Realtime Database. Error %s", key, err))
				return false, err
			}
		}
	} */
	//
	// We should have the input values. Let's create the output argument message
	//
/* 	args:= [] kxcommon.AnalyticsArg{}
	for _, pobj := range inputObjs {
		key,_ := kxcommon.Mapkey(inputTags, pobj.Tag)
		args = append(args, kxcommon.AnalyticsArgNew(key, fmt.Sprintf("%f", pobj.Cv.Value), pobj.Cv.Quality.String()))
	} */
	//request := kxcommon.AnalyticsRequestNew(functionName, args)

	//requestJson, err := kxcommon.SerializeObject(request)
	//if (err != nil) {
	//	activityLog.Error(fmt.Sprintf("[kxanalogavg] Error trying to serialize analytics request message. Error %s", err))
	//	return false, err
	//}
	//activityLog.Debugf("[kxanalogavg] Output Message: %s", requestJson)
	//context.SetOutput(ovOutput, "OK")

	return true, nil
}
