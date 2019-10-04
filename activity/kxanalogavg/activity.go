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

			windowStartTime := time.Date(2019, 9, 26, 23, 33, 57, 000000000, time.UTC)
			windowEndTime :=   time.Date(2019, 9, 26, 23, 35, 00, 000000000, time.UTC)
			//windowStartTime := time.Date(2019, 9, 26, 23, 37, 25, 000000000, time.UTC)
			//windowEndTime :=   time.Date(2019, 9, 26, 23, 39, 24, 000000000, time.UTC)


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
			noDataInWindow := len(windowResult) == 0
			if noDataInWindow && noDataBeforeWindow {
				activityLog.Infof("[kxanalogavg] No time stamp records found for %s in the time window - skipped", tag)
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
					activityLog.Infof("[kxanalogavg] value is invalid %s for tag %s - skipped", lastValueOutOfWindow["value"].(string), tag)
					continue
				}
				avgItem := avgItems {t0, v}
				activityLog.Infof("%v", avgItem)
				avgData = append(avgData, avgItem) 
			}
			// go through all values
			for _, wr := range windowResult {
				//get record time

				tint, err := wr["time"].(json.Number).Int64()
				if err != nil {
					activityLog.Infof("[kxanalogavg] time  is invalid %d for tag %s - skipped", wr["time"].(json.Number), tag)
					continue
				}
				t := time.Unix(0, tint)
				v, err = wr["value"].(json.Number).Float64()
				if err != nil {
					activityLog.Infof("[kxanalogavg] value is invalid %d for tag %s - skipped", wr["value"].(json.Number), tag)
					continue
				}
				avgItem := avgItems {t, v}
				activityLog.Infof("%v", avgItem)
				avgData = append(avgData, avgItem)
			}
			// add remaining 
			//
			if !noDataInWindow {
				v, err = windowResult[len(windowResult)-1]["value"].(json.Number).Float64()
				if err != nil {
					activityLog.Infof("[kxanalogavg] value is invalid %d for tag %s - skipped", windowResult[len(windowResult)-1]["value"].(json.Number), tag)
					continue
				}
			} else {
				v, err = lastValueOutOfWindow["value"].(json.Number).Float64()
				if err != nil {
					activityLog.Infof("[kxanalogavg] value is invalid %d for tag %s - skipped", lastValueOutOfWindow["value"].(json.Number), tag)
					continue
				}
			}
			avgItem := avgItems {windowEndTime, v}
			activityLog.Infof("%v", avgItem)
			avgData = append(avgData, avgItem) 

			var avg float64
			var prevTime time.Time
			var prevVal float64
			var diff float64
			var apt float64

			totalInterval := windowEndTime.Sub(windowStartTime).Nanoseconds() 
			activityLog.Infof("total Interval %d", totalInterval)
			for i, v := range avgData {
				if i == len(avgData) {
					diff = float64(windowEndTime.Sub(prevTime).Nanoseconds())
					apt = diff * prevVal / float64(totalInterval)
					avg = avg + apt
				} else if (i != 0) {
					diff = float64(v.tim.Sub(prevTime).Nanoseconds())
					apt = diff * prevVal / float64(totalInterval)
					avg = avg + apt
				}
				activityLog.Infof("Time %d, Diff: %f, Curvalue %f, PrevVal: %f, Apt: %f", v.tim.UnixNano(), diff, v.val, prevVal, apt)
				prevTime = v.tim
				prevVal = v.val
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
