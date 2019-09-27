package kxcommon

import (
	"fmt"
	"time"
	_ "github.com/influxdata/influxdb1-client" // this is important because of the bug in go mod
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
// KXHistTSRecord defines the structure of the time series record in TSDB
type KXHistTSRecord struct {
	Tag			string
	PType		string
	Value		float64
	ValueStr	string
	Quality		string
	TimeStamp	time.Time
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

// QueryTSOneTagTimeRange get records from TimeStamped database for one tag in a time range
func (tsdb *TSDB)  QueryTSOneTagTimeRange(database string, table string, tag string, startTimeStamp time.Time, endTimeStamp time.Time) (interface{}, error){
	// start building the query sentence
	// select time, "tag", value from timeseries where "tag" = 'IED1.A.TOTALSCANS' and "time" = 1553356273872000000

	startTimeMs := startTimeStamp.Format(time.RFC3339)
	endTimeMs := endTimeStamp.Format(time.RFC3339)
	//var rtn []KXHistTSRecord

	queryStr := " select %s from %s %s"
	fieldStr := "*"
//	q = fmt.Sprintf("SELECT * FROM %s WHERE time > '%s' - 3600s", Measurement, t)
	whereClause := fmt.Sprintf(" where time >= '%s' and time <= '%s'", startTimeMs, endTimeMs)

	queryStr = fmt.Sprintf(queryStr, fieldStr, table, whereClause)

	queryStr2 := " select %s from %s"
	queryStr2 = fmt.Sprintf(queryStr2, fieldStr, table)

	query := influxdb.Query {
		Command: queryStr,
		Database: database,
	}
	resp, err := tsdb.connection.Query(query)
	if err != nil {
		return nil, err
	}
	//
	// any rows returned?
	//
	if (len(resp.Results) > 0) {
		 // create an array of KXHistTSRecord out of the response
		 //fmt.Println(resp.Results)
	}
	// not found - return nil
	return resp.Results, nil
}