package kxrest

import (
	"fmt"
	"github.com/go-redis/redis"
)


// RTDB Real Time DB Class
type RTDB struct {
	hostName		string
	port			int
	userName 		string
	password		string
	defaultDB		int
	client			*redis.Client
	isConnected		bool
	jsonField		string
}

var (
)
// RTDBNew creates a new RT DB connection object
func (rtdb *RTDB) RTDBNew (hostName string, port int, userName string, password string, defaultDB int, jsonField string) {
	rtdb.hostName = hostName
	rtdb.port = port
	rtdb.userName = userName
	rtdb.password = password
	rtdb.defaultDB = defaultDB
	rtdb.client = nil
	rtdb.isConnected = false
	rtdb.jsonField = jsonField
}

// OpenRTDB opens the Real Time DB
func (rtdb *RTDB) OpenRTDB() error {

	client := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", rtdb.hostName, rtdb.port),
		Password: rtdb.password, 
		DB:       rtdb.defaultDB,
	})

	_, err := client.Ping().Result()

	if err != nil {
		return err
	}

	rtdb.client = client
	rtdb.isConnected = true
	return nil
}

// CloseRTDB closes the RealTime DB
func (rtdb *RTDB) CloseRTDB() error {
	return rtdb.client.Close()
}

// IsRTDBConnected checks if RTDB is connected
func (rtdb *RTDB) IsRTDBConnected() bool {
	return rtdb.isConnected 
}


// GetRTPObject get RTpObject form RTDB (if exists)
func (rtdb *RTDB) GetRTPObject(key string) (KXRTPObject, error) {
	var rtpObject KXRTPObject
	jsonStr, err := rtdb.GetValueFromKey(key)
	if (err == nil) {
		err = rtpObject.Deserialize(jsonStr)
	}
	return rtpObject, err
}

// UpdateRTPObject updates RTPObject into RTDB
func (rtdb *RTDB) UpdateRTPObject (key string, rtpObject KXRTPObject) error {
	jsonStr, err := rtpObject.Serialize()
	if err == nil {
		err = rtdb.SetValueForKey(key, jsonStr)
	}
	return err
}

// SetValueForKey updates db value for a key 
func (rtdb *RTDB) SetValueForKey(key string, value string) error {
	
	boolCmd := rtdb.client.HSet(key, rtdb.jsonField, value)
	return boolCmd.Err()
}

// GetValueFromKey queries a value from key
func (rtdb *RTDB) GetValueFromKey (key string) (string, error) {
	stringCmd := rtdb.client.HGet(key, rtdb.jsonField)
	return stringCmd.Result()
}