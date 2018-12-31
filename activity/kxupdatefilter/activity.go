package kxupdatefilter

import (
	"fmt"
	"errors"
	//"time"
	//"strconv"
	//"encoding/json"
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
	addToFlow, _ := ToBool(context.GetInput(ivAddToFlow))
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
/*
// Quality enum
type Quality int

const (
	QualityOk	    	Quality = iota 
	QualityOld   
	QualityBad   
	QualityUnknown
)

func (quality Quality) String() string {
    names := [...]string{
        "OK", 
        "OLD", 
        "BAD", 
        "UNKNOWN"}
    if quality < QualityOk || quality > QualityUnknown {
      return "UNKNOWN"
    }
    return names[quality]
}

func GetQualityFromString(qualityStr string) (Quality, error) {
	qual := map[string]Quality {
		"OK": QualityOk,
		"OLD": QualityOld,
		"BAD": QualityBad,
		"UNKNOWN": QualityUnknown }
	rtn, ok := qual[qualityStr]
	if (ok == false) {
			return QualityUnknown, errors.New("Quality " + qualityStr + "is unkonwn")
		}
	return rtn, nil
}

// KXRTPObject configuration structure for Physical Objects
type KXRTPObject struct {
	ID int
	Tag string
	Ptype string
	Cv *RtVal
	Pv *RtVal
	Avg *RtAvg
}
// RtVal represent a realtime sample
type RtVal struct {
	Value float64
	ValueStr string
	Quality Quality
	Timestamp time.Time
}
// RtAvg represent Realtime calculated averages
type RtAvg struct {
	Average float64
	Variance float64
}


// DecodeUpdateMessage get messages coming from a KXDataProc
func DecodeUpdateMessage (message string) []KXRTPObject {

	var updateMessage []KXRTPObject 
	// decode message
	if err := json.Unmarshal([]byte(message), &updateMessage); err != nil {
		return nil
	}
	return updateMessage
}
*/
