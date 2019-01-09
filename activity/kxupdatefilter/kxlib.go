package kxupdatefilter

import (
	"fmt"
	"errors"
	"time"
	"strconv"
	"encoding/json"
	"crypto/rand"
)
//
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
func DecodeUpdateMessage (message string) (KXRTPObject, error) {

	var updateMessage KXRTPObject 
	// decode message
	if err := json.Unmarshal([]byte(message), &updateMessage); err != nil {
		return KXRTPObject{}, err
	}
	return updateMessage, nil
}
// Scan message Types

type KXScanMessageUnitType int

const (
	MessageUnitTypeValue	KXScanMessageUnitType = iota
	MessageUnitTypeQuality 	
	MessageUnitTypeUnknown
)

func (scanMessageUnitQuality KXScanMessageUnitType) String() string {
    names := [...]string{
        "VALUE", 
        "QUALITY", 
        "UNKNOWN"}
    if scanMessageUnitQuality < MessageUnitTypeValue || scanMessageUnitQuality > MessageUnitTypeUnknown {
      return "UNKNOWN"
    }
    return names[scanMessageUnitQuality]
}
//
// ScanMessage holds a group of new data sent from any scanner to the data processor
//
type ScanMessage struct {
	MID		string
	Payload	[]ScanMessageUnit

}
// ScanMessageUnit contains a single new value to be sent to the data processor
type ScanMessageUnit struct {
	ID				int
	Tag				string
	Value			string
	Quality			string
	MType 			KXScanMessageUnitType	
	TimeStamp		time.Time
}
// GUIDNew Generates a new Guid
func GUIDNew() string {
	b := make([]byte, 16)
	rand.Read(b)
	guid := fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
	return guid
}

// ScanMessageNew creates a new scan message
func ScanMessageNew() ScanMessage {
	return ScanMessage{ GUIDNew(), nil }
}

// ScanMessageAdd Add a new ScanMessageUnit to existing ScanMessage
func (sm *ScanMessage) ScanMessageAdd(smu ScanMessageUnit) {
	sm.Payload = append(sm.Payload, smu)
} 

// ScanMessageUnitNew creates a new scan message Unit
func ScanMessageUnitNew(pOID int, tag string, value string, quality string, mType KXScanMessageUnitType, timeStamp time.Time) ScanMessageUnit {
	return ScanMessageUnit{ pOID, tag, value, quality, mType, timeStamp };
}

func SerializeObject (obj interface{}) (string, error) {
	bytes, err := json.Marshal(obj)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

func ToBool(val interface{}) (bool, error) {

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

// Serialize - convert a RTKXPObject to Json
func (rtpObject * KXRTPObject) Serialize() (string, error) {
	rtn, err := json.Marshal(rtpObject)
	return string(rtn), err
}

// Deserialize - attempt to convert a Json to RTKXPObject  
func (rtpObject * KXRTPObject) Deserialize(jsonInput string) error {
	bArray := []byte(jsonInput)
	err := json.Unmarshal(bArray, rtpObject)
	return err
}

// AnalyticsRequest main request to analytic calculation
type AnalyticsRequest struct {
	Function string
	Args	 []AnalyticsArg
}
// AnalyticsArg argument into the request
type AnalyticsArg struct {
	Name 	string
	Value 	string
	Quality string
}

type AnalyticsResponse struct {
	Results 	[]AnalyticsArg
	Success		bool
}

func AnalyticsRequestNew (function string, args []AnalyticsArg) AnalyticsRequest {
	return AnalyticsRequest {function, args}
}

func AnalyticsArgNew (name string, value string, quality string) AnalyticsArg {
	return AnalyticsArg {name, value, quality}
}

func Mapkey(m map[string]string, value string) (key string, ok bool) {
	for k, v := range m {
	  if v == value { 
		key = k
		ok = true
		return
	  }
	}
	return
  }