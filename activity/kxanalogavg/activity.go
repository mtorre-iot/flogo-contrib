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
	activityLog.SetLogLevel(logger.DebugLevel)
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
	// get the output tags
	outputTagsInterface :=  context.GetInput(ivOutputTags).(map[string]interface{})
	outputTags := make(map[string]string) 

	for key, value := range outputTagsInterface {
    	strKey := fmt.Sprintf("%v", key)
        strValue := fmt.Sprintf("%v", value)
        outputTags[strKey] = strValue
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
	_ =pObjectConfigFile 
	var response kxcommon.AnalyticsResponse 
	//
	// Go get history data of each tag from kxhistDB
	// 
	badData := false
 	for key, tag := range inputTags {
		var avg float64

		if tag != "" {

			windowStartTime := time.Date(2019, 9, 26, 23, 34, 14, 000000000, time.UTC)
			windowEndTime :=   time.Date(2019, 9, 26, 23, 34, 21, 000000000, time.UTC)
			//windowStartTime := time.Date(2019, 9, 26, 23, 37, 25, 000000000, time.UTC)
			//windowEndTime :=   time.Date(2019, 9, 26, 23, 39, 24, 000000000, time.UTC)


			windowResult, err := tsdb.QueryTSOneTagTimeRange(databaseName, tableName, tag,
				 windowStartTime, windowEndTime)
			if (err != nil)	{
				activityLog.Errorf("[kxanalogavg] Tag: %s could not be accessed from Time Stamp database. Error %s", key, err)
				return false, err
			}
			
			lastValueOutOfWindow, err := tsdb.QueryTSOneTagLastValue(databaseName, tableName, tag, windowStartTime)
		   	if (err != nil)	{
			   activityLog.Errorf("[kxanalogavg] Tag: %s could not be accessed from Time Stamp database. Error %s", key, err)
			   return false, err
		   	}
			noDataBeforeWindow := len(lastValueOutOfWindow) == 0 
			noDataInWindow := len(windowResult) == 0
			if noDataInWindow && noDataBeforeWindow {
				activityLog.Warnf("[kxanalogavg] No time stamp records found for %s in the time window - skipped", tag)
				badData = true
			}
			avgData := make([]avgItems, 0)
			var t0 time.Time 
			var v float64

			if !badData {
			
				t0 = windowStartTime

				if !noDataBeforeWindow {  
					v, err = lastValueOutOfWindow["value"].(json.Number).Float64()
					if err != nil {
						activityLog.Warnf("[kxanalogavg] value is invalid %s for tag %s - skipped", lastValueOutOfWindow["value"].(string), tag)
						badData = true
						continue
					}
					avgItem := avgItems {t0, v}
					activityLog.Debugf("%v", avgItem)
					avgData = append(avgData, avgItem) 
				}
				// go through all values
				for _, wr := range windowResult {
					//get record time

					tint, err := wr["time"].(json.Number).Int64()
					if err != nil {
						activityLog.Warnf("[kxanalogavg] time is invalid %d for tag %s - skipped", wr["time"].(json.Number), tag)
						badData = true
						continue
					}
					t := time.Unix(0, tint)
					v, err = wr["value"].(json.Number).Float64()
					if err != nil {
						activityLog.Warnf("[kxanalogavg] value is invalid %d for tag %s - skipped", wr["value"].(json.Number), tag)
						badData = true
						continue
					}
					avgItem := avgItems {t, v}
					activityLog.Debugf("%v", avgItem)
					avgData = append(avgData, avgItem)
				}
			}
			//
			// add remaining 
			//
			if !badData {
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
				activityLog.Debugf("%v", avgItem)
				avgData = append(avgData, avgItem) 

				var prevTime time.Time
				var prevVal float64
				var diff float64
				var apt float64

				totalInterval := avgData[len(avgData)-1].tim.Sub(avgData[0].tim).Nanoseconds() 
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
					activityLog.Debugf("Time %d, Diff: %f, Curvalue %f, PrevVal: %f, Apt: %f", v.tim.UnixNano(), diff, v.val, prevVal, apt)
					prevTime = v.tim
					prevVal = v.val
				}
				activityLog.Debugf("average %f", avg)
			}
		}
		//
		// Calculation complete. Now place in the buffer for transmittal
		//
		if !badData {
			outp := kxcommon.AnalyticsArgNew(key, fmt.Sprintf("%f", avg), kxcommon.QualityOk.String())
			response.Results = append(response.Results, outp)
		} else {
			outp := kxcommon.AnalyticsArgNew(key, fmt.Sprintf("%f", 0.0), kxcommon.QualityBad.String())
			response.Results = append(response.Results, outp)
		}
	}
	//
	// Create the json scan message back to KXDataproc
	//
	scanMessage := kxcommon.ScanMessageNew()
	for _,res := range response.Results {
		messageType := kxcommon.MessageUnitTypeValue
		if badData { messageType = kxcommon.MessageUnitTypeQuality} 
		smu := kxcommon.ScanMessageUnitNew(-1, outputTags[res.Name], res.Value, res.Quality, messageType, time.Now().UTC())
		scanMessage.ScanMessageAdd(smu)
	}
	jsonMessage, err := kxcommon.SerializeObject(scanMessage)
	if err != nil {
		activityLog.Error(fmt.Sprintf("[kxanalogavg] Error trying to serialize output message. Error %s", err))
		return false, err
	}
	activityLog.Debug(fmt.Sprintf("[kxanalogavg] Output Message: %s", jsonMessage))
	context.SetOutput(ovOutput, jsonMessage) 
	return true, nil
}
