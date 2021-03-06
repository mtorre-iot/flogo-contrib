package kxrest

import (
	"bytes"
	"fmt"
	"time"
	"crypto/tls"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"strconv"
	"github.com/TIBCOSoftware/flogo-lib/core/activity"
	"github.com/TIBCOSoftware/flogo-lib/logger"
	"github.com/mtorre-iot/flogo-contrib/activity/kxcommon"
)

// log is the default package logger
var activityLog = logger.GetLogger("activity-knox-kxrest")

const (
	methodGET    = "GET"
	methodPOST   = "POST"
	methodPUT    = "PUT"
	methodPATCH  = "PATCH"
	methodDELETE = "DELETE"

	ivMethod      = "method"
	ivURI         = "uri"
	ivPathParams  = "pathParams"
	ivQueryParams = "queryParams"
	ivHeader      = "header"
	ivContent     = "content"
	ivParams      = "params"
	ivProxy       = "proxy"
	ivSkipSsl     = "skipSsl"
	ivRTDBFile 	  = "RTDBFile"

	ivOutputTags  = "outputTags"

	ovMessage = "message"
)

var validMethods = []string{methodGET, methodPOST, methodPUT, methodPATCH, methodDELETE}

// KXRESTActivity is an Activity that is used to invoke a REST Operation
// inputs : {method,uri,params}
// outputs: {result}
type KXRESTActivity struct {
	metadata *activity.Metadata
}

func init() {
	activityLog.SetLogLevel(logger.InfoLevel) 
}

// NewActivity creates a new RESTActivity
func NewActivity(metadata *activity.Metadata) activity.Activity {
	return &KXRESTActivity {metadata: metadata}
}

// Metadata returns the activity's metadata
func (a *KXRESTActivity) Metadata() *activity.Metadata {
	return a.metadata
}

// Eval implements api.Activity.Eval - Invokes a REST Operation
func (a *KXRESTActivity) Eval(context activity.Context) (done bool, err error) {

	method := strings.ToUpper(context.GetInput(ivMethod).(string))
	uri := context.GetInput(ivURI).(string)
	rtdbFile := context.GetInput(ivRTDBFile).(string)

	containsParam := strings.Index(uri, "/:") > -1

	if containsParam {

		val := context.GetInput(ivPathParams)

		if val == nil {
			val = context.GetInput(ivParams)

			if val == nil {
				err := activity.NewError("[kxrest] Path Params not specified, required for URI: "+uri, "", nil)
				return false, err
			}
		}

		pathParams := val.(map[string]string)
		uri = BuildURI(uri, pathParams)
	}

	if queryParams, ok := context.GetInput(ivQueryParams).(map[string]string); ok && len(queryParams) > 0 {
		qp := url.Values{}

		for key, value := range queryParams {
			qp.Set(key, value)
		}
		uri = uri + "?" + qp.Encode()
	}

	activityLog.Debugf("[kxrest] REST Call: [%s] %s\n", method, uri)

	var reqBody io.Reader

	contentType := "application/json; charset=UTF-8"

	if method == methodPOST || method == methodPUT || method == methodPATCH {

		content := context.GetInput(ivContent)

		contentType = getContentType(content)

		if content != nil {
			if str, ok := content.(string); ok {
				reqBody = bytes.NewBuffer([]byte(str))
			} else {
				b, _ := json.Marshal(content) //todo handle error
				reqBody = bytes.NewBuffer([]byte(b))
			}
		}
	} else {
		reqBody = nil
	}

	req, err := http.NewRequest(method, uri, reqBody)

	if err != nil {
		return false, err
	}

	if reqBody != nil {
		req.Header.Set("Content-Type", contentType)
	}

	// Set headers
	activityLog.Debugf("[kxrest] Setting HTTP request headers...")
	if headers, ok := context.GetInput(ivHeader).(map[string]string); ok && len(headers) > 0 {
		for key, value := range headers {
			activityLog.Debugf("[kxrest] %s: %s", key, value)
			req.Header.Set(key, value)
		}
	}

	httpTransportSettings := &http.Transport{}

	// Set the proxy server to use, if supplied
	proxy := context.GetInput(ivProxy)
	var client *http.Client
	var proxyValue, ok = proxy.(string)
	if ok && len(proxyValue) > 0 {
		proxyURL, urlErr := url.Parse(proxyValue)
		if urlErr != nil {
			activityLog.Debug("[kxrest] Error parsing proxy url:", urlErr)
			return false, urlErr
		}

		activityLog.Debug("[kxrest] Setting proxy server:", proxyValue)
		httpTransportSettings.Proxy = http.ProxyURL(proxyURL)
	}

	// Skip ssl validation
	skipSsl, ok := context.GetInput(ivSkipSsl).(bool)
	if ok && skipSsl {
		httpTransportSettings.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}

	client = &http.Client{Transport: httpTransportSettings}
	resp, err := client.Do(req)
	defer resp.Body.Close()

	if err != nil {
		return false, err
	}

	activityLog.Debug("[kxrest] response Status:", resp.Status)
	respBody, _ := ioutil.ReadAll(resp.Body)

	var result interface{}

	d := json.NewDecoder(bytes.NewReader(respBody))
	d.UseNumber()
	err = d.Decode(&result)

	activityLog.Debug("[kxrest] response Body:", result)

	var resultx kxcommon.AnalyticsResponse
	json.Unmarshal(respBody, &resultx)
	//
	// Get the Output object
	//
	val := context.GetInput(ivOutputTags)

	outputTagsInterface := val.(map[string]interface{})
	outputTags := make(map[string]string) 

	for key, value := range outputTagsInterface {
    	strKey := fmt.Sprintf("%v", key)
        strValue := fmt.Sprintf("%v", value)

        outputTags[strKey] = strValue
    }

	outputObjs := make(map[string] kxcommon.KXRTPObject)
	// decode (unmarshall) the RTDB server pars
	rtdbPars := strings.Split(rtdbFile,":")
	// create the realtime DB access object
	var rtdb kxcommon.RTDB
	port, err := strconv.Atoi(rtdbPars[1])
	if err != nil {
		return false, err
	}
	rtdb.RTDBNew(rtdbPars[0], port, rtdbPars[2],rtdbPars[3], 0, "json") 
	// Open the RealTime DB
	err = rtdb.OpenRTDB()
	if err != nil {
		activityLog.Error(fmt.Sprintf("[kxrest] Realtime Database could not be opened. Error %s", err))
	return false, err
	}
	// make sure it closes after finish
	defer rtdb.CloseRTDB()

	for _, tag := range outputTags {
		outputObjs[tag], err = rtdb.GetRTPObject(tag)
		if (err != nil)	{
			activityLog.Error(fmt.Sprintf("[kxrest] Tag: %s could not be accessed from Realtime Database. Error %s", tag, err))
			return false, err
		}
	}
	//
	// Create the json scan message back to KXDataproc
	//
	scanMessage := kxcommon.ScanMessageNew()
	for _,res := range resultx.Results {
		smu := kxcommon.ScanMessageUnitNew(outputObjs[outputTags[res.Name]].ID, outputTags[res.Name], res.Value, kxcommon.QualityOk.String(), kxcommon.MessageUnitTypeValue, time.Now().UTC())
		scanMessage.ScanMessageAdd(smu)
	}
	jsonMessage, err := kxcommon.SerializeObject(scanMessage)
	if err != nil {
		activityLog.Error(fmt.Sprintf("[kxrest] Error trying to serialize output message. Error %s", err))
		return false, err
	}
	activityLog.Debug(fmt.Sprintf("[kxrest] Output Message: %s", jsonMessage))
	context.SetOutput(ovMessage, jsonMessage) 

	return true, nil
}

////////////////////////////////////////////////////////////////////////////////////////
// Utils

//todo just make contentType a setting
func getContentType(replyData interface{}) string {

	contentType := "application/json; charset=UTF-8"

	switch v := replyData.(type) {
	case string:
		if !strings.HasPrefix(v, "{") && !strings.HasPrefix(v, "[") {
			contentType = "text/plain; charset=UTF-8"
		}
	case int, int64, float64, bool, json.Number:
		contentType = "text/plain; charset=UTF-8"
	default:
		contentType = "application/json; charset=UTF-8"
	}

	return contentType
}

func methodIsValid(method string) bool {

	if !stringInList(method, validMethods) {
		return false
	}

	//validate path

	return true
}

func stringInList(str string, list []string) bool {
	for _, value := range list {
		if value == str {
			return true
		}
	}
	return false
}

// BuildURI is a temporary crude URI builder
func BuildURI(uri string, values map[string]string) string {

	var buffer bytes.Buffer
	buffer.Grow(len(uri))

	addrStart := strings.Index(uri, "://")

	i := addrStart + 3

	for i < len(uri) {
		if uri[i] == '/' {
			break
		}
		i++
	}

	buffer.WriteString(uri[0:i])

	for i < len(uri) {
		if uri[i] == ':' {
			j := i + 1
			for j < len(uri) && uri[j] != '/' {
				j++
			}

			if i+1 == j {

				buffer.WriteByte(uri[i])
				i++
			} else {

				param := uri[i+1 : j]
				value := values[param]
				buffer.WriteString(value)
				if j < len(uri) {
					buffer.WriteString("/")
				}
				i = j + 1
			}

		} else {
			buffer.WriteByte(uri[i])
			i++
		}
	}

	return buffer.String()
}
