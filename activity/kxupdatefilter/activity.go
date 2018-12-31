package kxupdatefilter

import (
	"fmt"
	"errors"
	"github.com/mtorre-iot/buntdb"
	"github.com/TIBCOSoftware/flogo-lib/core/activity"
	"github.com/TIBCOSoftware/flogo-lib/logger"
)

// activityLog is the default logger for the Log Activity
var activityLog = logger.GetLogger("activity-flogo-kxupdatefilter")

const (
	ivMessage   = "message"
	ivTriggerTag = "triggerTag"
	ivInputTag1 = "inputTag1"
	ivInputTag2 = "inputTag2"

	ovMessage = "message"
	ovOutputTag1 = "outputTag1"
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

	message, _ := context.GetInput(ivMessage).(string)
	triggerTag,_ := context.GetInput(ivTriggerTag).(string)
	inputTag1,_ := context.GetInput(ivInputTag1).(string)
	inputTag2,_ := context.GetInput(ivInputTag2).(string)
	//outputTag1,_ := context.GetSetting(ovOutputTag1).(string)

	var input1Value float64 
	var input2Value float64
	var output1Value float64
	var triggerObj KXRTPObject
	var input1Obj KXRTPObject 
	var input2Obj KXRTPObject 
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
	for _, rtPObject := range decodedMessage {
		activityLog.Info(fmt.Sprintf("Tag: %s", rtPObject.Tag))
		// 
		// Check if any of the received tags is the associated trigger
		//
		if rtPObject.Tag == triggerTag {
			activityLog.Info(fmt.Sprintf("Found %s in the trigger!", triggerTag))
			triggerObj = rtPObject
			foundTrig = true
		} 
		if rtPObject.Tag == inputTag1 {
			input1Obj = triggerObj
		}
		if triggerTag == inputTag2 {
			input2Obj = triggerObj
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
		defer CloseRTDB(db)
		if input1Obj.Tag == "" {
			input1Obj, err = GetRTPObject(db, inputTag1)
			if (err != nil)	{
				activityLog.Error(fmt.Sprintf("Tag: %s could not be accessed from Realtime Database. Error %s", inputTag1, err))
			}
		}
		if input2Obj.Tag == "" {
			input2Obj, err = GetRTPObject(db, inputTag2)
			if (err != nil)	{
				activityLog.Error(fmt.Sprintf("Tag: %s could not be accessed from Realtime Database. Error %s", inputTag2, err))
			}
		}
		//
		// We should have the input values. Let's to the operation
		//
		input1Value = input1Obj.Cv.Value
		input2Value = input2Obj.Cv.Value
		output1Value = input1Value + input2Value
		activityLog.Info(fmt.Sprintf("Result: %f", output1Value))
	}
	context.SetOutput(ovMessage, message)

	return true, nil
}



var (
)
// OpenRTDB opens the realTimeDB
func OpenRTDB(persistenceFileName string) (*buntdb.DB, error) {
	db, err := buntdb.Open(persistenceFileName)
	return db, err
}

// CloseRTDB closes a previously opened RTDB
func  CloseRTDB(db *buntdb.DB) error {
	var err error
	if (db != nil) {
		err = db.Close()
	}
	return err
}

// GetRTPObject get RTpObject form RTDB (if exists)
func GetRTPObject(db *buntdb.DB, key string) (KXRTPObject, error) {
	var rtpObject KXRTPObject
	jsonStr, err := GetValueFromKey(db, key)
	if (err == nil) {
		err = rtpObject.Deserialize(jsonStr)
	}
	return rtpObject, err
}
// UpdateRTPObject updates RTPObject into RTDB
func UpdateRTPObject (db *buntdb.DB, key string, rtpObject KXRTPObject) error {
	jsonStr, err := rtpObject.Serialize()
	if err == nil {
		err = SetValueForKey(db, key, jsonStr)
	}
	return err
}

// SetValueForKey updates db value for a key 
func SetValueForKey(db *buntdb.DB, key string, value string) error {
	err := db.Update(func(tx *buntdb.Tx) error {
		_, _, err := tx.Set(key, value, nil)
		return err
	})
	return err
}

// GetValueFromKey queries a value from key
func GetValueFromKey (db *buntdb.DB, key string) (string, error) {
	var result string
	err := db.View(
		
		func(tx *buntdb.Tx) error {
			val, err := tx.Get(key)
			if err != nil{
				return err
			}
			result = val
			return nil
	})
	return result, err
}

// CompactDB will make the database file smaller by removing redundant log entries
func CompactDB(db *buntdb.DB) error {
	return db.Shrink()
} 