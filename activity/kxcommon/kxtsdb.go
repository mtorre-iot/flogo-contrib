package kxcommon

import (
	"fmt"
	"time"
	influxdb "github.com/influxdata/influxdb1-client/v2"
)


// TSDB Time Series DB Class
type TSDB struct {
	hostName		string
	port			int
	userName 		string
	password		string
	connection		influxdb.Client
	isConnected		bool
}

// TSRecord Time Series record structure
type TSRecord	struct {
	TimeStamp 	time.Time
	Tags	map[string]string
	Fields  map[string]interface{}
}

var (
)

// TSDBNew creates a new Time Series DB connection object
func TSDBNew (hostName string, port int, userName string, password string) *TSDB {
	return &TSDB{hostName, port, userName, password, nil, false}
}

// OpenTSDB opens the Time-Stamped DB
func (tsdb *TSDB) OpenTSDB () error {
	con, err := influxdb.NewHTTPClient(influxdb.HTTPConfig{
		Addr:     fmt.Sprintf("http://%s:%d", tsdb.hostName, tsdb.port),
		Username: tsdb.userName,
		Password: tsdb.password,
	})
	if err != nil {
		return err
	}
	tsdb.connection = con
	tsdb.isConnected = true
	return nil
}

// CloseTSDB closes the Time-Stamped DB
func (tsdb *TSDB) CloseTSDB() error {
	return tsdb.connection.Close()
}