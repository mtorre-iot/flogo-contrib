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
	precision		string
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
	return &TSDB{hostName, port, userName, password, "ns", nil, false}
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
func (tsdb *TSDB)  QueryTSOneTagTimeRange(database string, table string, tag string, startTimeStamp time.Time, endTimeStamp time.Time) ([]map[string]interface{}, error){
	// start building the query sentence
	// select time, "tag", value from timeseries where "tag" = 'IED1.A.TOTALSCANS' and "time" = 1553356273872000000

	startTimeMs := startTimeStamp.UnixNano()
	endTimeMs := endTimeStamp.UnixNano()
	var rtn []map[string]interface{}

	//fmt.Printf("start Time: %d - end time: %d\n", startTimeMs, endTimeMs)

	queryStr := " select %s from %s %s"
	fieldStr := "*"
	whereClause := fmt.Sprintf(" where \"tag\" = '%s' and time >= %d and time <= %d", tag, startTimeMs, endTimeMs)

	queryStr = fmt.Sprintf(queryStr, fieldStr, table, whereClause)
	//fmt.Printf("Query: %s\n", queryStr)

	query := influxdb.Query {
		Command: queryStr,
		Database: database,
		Precision: tsdb.precision,
	}
	resp, err := tsdb.connection.Query(query)
	if err != nil {
		return nil, err
	}
	//
	// any rows returned?
	//
	if (len(resp.Results) > 0) {
		 //res := make (map[string]string, len(res.Series.Columns))
		 for _, res := range resp.Results {
			 for _,sr := range res.Series {
				for _, val := range sr.Values {
					rec := make(map[string]interface{})
					for j, col := range sr.Columns {
						if col == "time" {
							rec[col] = val[j] 
							fmt.Printf("Time: %s\n", val[j])
						} else {
							rec[col] = val[j] 
						}
					}
					rtn = append(rtn, rec)
				}
			 }
		 }
	}
	return rtn, nil
}

// QueryTSOneTagLastValue get lasr record from TimeStamped database for one tag where time < specified
func (tsdb *TSDB)  QueryTSOneTagLastValue(database string, table string, tag string, endTimeStamp time.Time) (map[string]interface{}, error){
	// start building the query sentence
	// select time, "tag", value from timeseries where "tag" = 'IED1.A.TOTALSCANS' and "time" = 1553356273872000000

	endTimeMs := endTimeStamp.UnixNano()
	var rtn map[string]interface{}

	//fmt.Printf("end time: %d\n", endTimeMs)

	queryStr := " select %s from %s %s"
	fieldStr := "*"
	whereClause := fmt.Sprintf(" where \"tag\" = '%s' and time < %d order by time desc limit 1", tag, endTimeMs)

	queryStr = fmt.Sprintf(queryStr, fieldStr, table, whereClause)
	//fmt.Printf("Query: %s\n", queryStr)

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
		 //res := make (map[string]string, len(res.Series.Columns))
		 // create an array of KXHistTSRecord out of the response
		 for _, res := range resp.Results {
			 for _,sr := range res.Series {
				//fmt.Printf("name: %s\n", sr.Name)
				rtn = make(map[string]interface{})
				for j, col := range sr.Columns {
					rtn[col] = sr.Values[0][j] 
				}
			 }
		 }
	}
	return rtn, nil
}