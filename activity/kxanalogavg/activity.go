package kxanalogavg

import (
	"fmt"
	//"errors"
	"time"
	"strings"
	"strconv"
	"encoding/json"
	"github.com/TIBCOSoftware/flogo-lib/core/activity"
	"github.com/TIBCOSoftware/flogo-lib/logger"
	"github.com/mtorre-iot/flogo-contrib/activity/kxcommon"
)

// activityLog is the default logger for the Log Activity
var activityLog = logger.GetLogger("activity-flogo-kxanalogavg")

type avgItems struct {
	tim  time.Time 
	val float64
}

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
 
	tsdbString := context.GetInput(ivTSDB).(string)
	pObjectConfigFile := context.GetInput(ivpObjectConfigFile).(string)
	outputTags := context.GetInput(ivOutputTags) 

	// decode (unmarshall) the TSDB server pars
	//<hostname>:<port>:<userName>:<password>:<databaseName>:<historyTable>:<precision>
	
 	tsdbPars := strings.Split(tsdbString,":")
	// create the Time Stamp DB access object

	port, err := strconv.Atoi(tsdbPars[1])
	if err != nil {
		return false, err
	}
	hostName := tsdbPars[0]
	userName := tsdbPars[2]
	password := tsdbPars[3]
	databaseName := tsdbPars[4]
	tableName := tsdbPars[5]

	// get the input tags
	inputTagsInterface :=  context.GetInput(ivInputTags).(map[string]interface{})
	inputTags := make(map[string]string) 

	for key, value := range inputTagsInterface {
    	strKey := fmt.Sprintf("%v", key)
        strValue := fmt.Sprintf("%v", value)
        inputTags[strKey] = strValue
	}

	// Open the TSDB
	tsdb := kxcommon.TSDBNew(hostName, port, userName, password) 
	err = tsdb.OpenTSDB()
	if err != nil {
		activityLog.Error(fmt.Sprintf("[kxanalogavg] Time Stamp Database could not be opened. Error %s", err))
		return false, err
	}
	// make sure it closes after finish
	defer tsdb.CloseTSDB()
	// check all tags
	_ =outputTags
	_ =pObjectConfigFile 
	//
	// Go get history data of each tag from kxhistDB
	// 
 	for key, tag := range inputTags {
		if tag != "" {

			windowStartTime := time.Date(2019, 9, 26, 23, 34,  8, 224000000, time.UTC)
			windowEndTime :=   time.Date(2019, 9, 26, 23, 34, 23, 446000000, time.UTC)

			windowResult, err := tsdb.QueryTSOneTagTimeRange(databaseName, tableName, tag,
				 windowStartTime, windowEndTime)
			if (err != nil)	{
				activityLog.Error(fmt.Sprintf("[kxanalogavg] Tag: %s could not be accessed from Time Stamp database. Error %s", key, err))
				return false, err
			}
			
			activityLog.Infof("result %v", windowResult)
			
			lastValueOutOfWindow, err := tsdb.QueryTSOneTagLastValue(databaseName, tableName, tag, windowStartTime)
		   	if (err != nil)	{
			   activityLog.Error(fmt.Sprintf("[kxanalogavg] Tag: %s could not be accessed from Time Stamp database. Error %s", key, err))
			   return false, err
		   	}
			noDataBeforeWindow := len(lastValueOutOfWindow) == 0 
			if (len(windowResult) == 0) && noDataBeforeWindow {
				activityLog.Debugf("[kxanalogavg] No time stamp records found for %s in the time window - skipped", tag)
				continue
			}
		   	activityLog.Infof("result2 %v", lastValueOutOfWindow)
			//
			//
			avgData := make([]avgItems, 0)
			var t0 time.Time 
			var v float64
			
			t0 = windowStartTime

			if !noDataBeforeWindow {  
				v, err = lastValueOutOfWindow["value"].(json.Number).Float64()
				if err != nil {
					activityLog.Debugf("[kxanalogavg] value is invalid %s for tag %s - skipped", lastValueOutOfWindow["value"].(string), tag)
					continue
				}
				avgItem := avgItems {t0, v}
				avgData = append(avgData, avgItem) 
			}
			// go through all values
			for _, wr := range windowResult {
				//get record time

				str := wr["time"].(string)
				t, err := time.Parse("2006-01-02T15:04:05.000Z", str)
				if err != nil {
					activityLog.Debugf("[kxanalogavg] time  is invalid %s for tag %s - skipped", str, tag)
					continue
				} 
				v, err = wr["value"].(json.Number).Float64()
				if err != nil {
					activityLog.Debugf("[kxanalogavg] value is invalid %s for tag %s - skipped", lastValueOutOfWindow["value"].(string))
					continue
				}
				avgItem := avgItems {t, v}
				avgData = append(avgData, avgItem)
			}
			var avg float64
			var prevTime time.Time
			var prevVal float64
			var diff float64
			totalInterval := windowEndTime.Sub(windowStartTime).Nanoseconds() 
			activityLog.Infof("total Interval %d", totalInterval)
			for i, v := range avgData {
				if i == 0 {
					prevTime = v.tim
					prevVal = v.val
				} else if i == len(avgData) {
					diff = float64(windowEndTime.Sub(prevTime).Nanoseconds())
					avg = avg +  diff * prevVal / float64(totalInterval)
				} else {
					diff = float64(v.tim.Sub(prevTime).Nanoseconds())
					avg = avg + diff * prevVal / float64(totalInterval)
					prevTime = v.tim
					prevVal = v.val
				}
				activityLog.Infof("Time %d, Diff: %f, Value %f", v.tim.UnixNano(), diff, v.val)

			}
			activityLog.Infof("average %f", avg)
			
		}
	} 
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
	context.SetOutput(ovOutput, "OK")

	return true, nil
}
