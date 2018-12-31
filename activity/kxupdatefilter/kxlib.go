package kxupdatefilter

import (
	//"fmt"
	//"errors"
	"time"
	//"strconv"
	"encoding/json"
	//"github.com/TIBCOSoftware/flogo-lib/core/activity"
	//"github.com/TIBCOSoftware/flogo-lib/logger"
)
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
