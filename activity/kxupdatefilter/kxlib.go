package kxupdatefilter

import (
	"fmt"
	"time"
	"errors"
	"strconv"
)



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

func toBool(val interface{}) (bool, error) {

	b, ok := val.(bool)
	if !ok {
		s, ok := val.(string)

		if !ok {
			return false, fmt.Errorf("unable to convert to boolean")
		}

		var err error
		b, err = strconv.ParseBool(s)

		if err != nil {
			return false, err
		}
	}

	return b, nil
}