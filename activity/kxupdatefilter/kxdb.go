package kxupdatefilter

import (
	"github.com/mtorre-iot/buntdb"
)

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