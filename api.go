package gorqlite

/*
	this file has low level stuff:

	rqliteApiGet()
	rqliteApiPost()

	There is some code duplication between those and they should
	probably be combined into one function.

	nothing public here.

*/

import "bytes"
import "encoding/json"
import "errors"
import "fmt"
import "io/ioutil"
import "net/http"
import "time"

/* *****************************************************************

   method: rqliteApiGet() - for api_STATUS

	- lowest level interface - does not do any JSON unmarshaling
	- handles retries
	- handles timeouts

 * *****************************************************************/

func (conn *Connection) rqliteApiGet(apiOp apiOperation) ([]byte, error) {
	var responseBody []byte
	trace("%s: rqliteApiGet() called", conn.ID)

	// only api_STATUS now - maybe someday BACKUP
	if apiOp != api_STATUS {
		return responseBody, errors.New("rqliteApiGet() called for invalid api operation")
	}

	// just to be safe, check this
	peersToTry := conn.cluster.makePeerList()
	if len(peersToTry) < 1 {
		return responseBody, errors.New("I don't have any cluster info")
	}
	trace("%s: I have a peer list %d peers long", conn.ID, len(peersToTry))

	// failure log is used so that if all peers fail, we can say something
	// about why each failed
	failureLog := make([]string, 0)

PeerLoop:
	for peerNum, peerToTry := range peersToTry {
		trace("%s: attemping to contact peer %d", conn.ID, peerNum)
		// docs say default GET policy is up to 10 follows automatically
		url := conn.assembleURL(api_STATUS, peerToTry)
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			trace("%s: got error '%s' doing http.NewRequest", conn.ID, err.Error())
			failureLog = append(failureLog, fmt.Sprintf("%s failed due to %s", url, err.Error()))
			continue PeerLoop
		}
		trace("%s: http.NewRequest() OK", conn.ID)
		req.Header.Set("Content-Type", "application/json")
		client := &http.Client{}
		client.Timeout = time.Duration(conn.timeout) * time.Second
		response, err := client.Do(req)
		if err != nil {
			trace("%s: got error '%s' doing client.Do", conn.ID, err.Error())
			failureLog = append(failureLog, fmt.Sprintf("%s failed due to %s", url, err.Error()))
			continue PeerLoop
		}
		defer response.Body.Close()
		trace("%s: client.Do() OK", conn.ID)
		responseBody, err := ioutil.ReadAll(response.Body)
		if err != nil {
			trace("%s: got error '%s' doing ioutil.ReadAll", conn.ID, err.Error())
			failureLog = append(failureLog, fmt.Sprintf("%s failed due to %s", url, err.Error()))
			continue PeerLoop
		}
		trace("%s: ioutil.ReadAll() OK", conn.ID)
		if response.Status != "200 OK" {
			trace("%s: got code %s", conn.ID, response.Status)
			failureLog = append(failureLog, fmt.Sprintf("%s failed, got: %s", url, response.Status))
			continue PeerLoop
		}
		// if we got here, we succeeded
		trace("%s: api call OK, returning", conn.ID)
		return responseBody, nil
	}

	// if we got here, all peers failed.  Let's build a verbose error message
	var stringBuffer bytes.Buffer
	stringBuffer.WriteString("tried all peers unsuccessfully. here are the results:\n")
	for n, v := range failureLog {
		stringBuffer.WriteString(fmt.Sprintf("   peer #%d: %s\n", n, v))
	}
	return responseBody, errors.New(stringBuffer.String())
}

/* *****************************************************************

   method: rqliteApiPost() - for api_QUERY and api_WRITE

	- lowest level interface - does not do any JSON unmarshaling
 	- handles 301s, etc.
	- handles retries
	- handles timeouts

	it is called with an apiOperation type because the URL it will use varies
	depending on the API operation type (api_QUERY vs. api_WRITE)

 * *****************************************************************/

func (conn *Connection) rqliteApiPost(apiOp apiOperation, sqlStatements []string) ([]byte, error) {
	var responseBody []byte

	switch apiOp {
	case api_QUERY:
		trace("%s: rqliteApiGet() post called for a QUERY of %d statements", conn.ID, len(sqlStatements))
	case api_WRITE:
		trace("%s: rqliteApiGet() post called for a QUERY of %d statements", conn.ID, len(sqlStatements))
	default:
		return responseBody, errors.New("weird! called for an invalid apiOperation in rqliteApiPost()")
	}

	// jsonify the statements.  not really needed in the
	// case of api_STATUS but doesn't hurt

	jStatements, err := json.Marshal(sqlStatements)
	if err != nil {
		return nil, err
	}

	// just to be safe, check this
	peersToTry := conn.cluster.makePeerList()
	if len(peersToTry) < 1 {
		return responseBody, errors.New("I don't have any cluster info")
	}

	// failure log is used so that if all peers fail, we can say something
	// about why each failed
	failureLog := make([]string, 0)

PeerLoop:
	for peerNum, peer := range peersToTry {
		trace("%s: trying peer #%d", conn.ID, peerNum)

		// we're doing a post, and the RFCs say that if you get a 301, it's not
		// automatically followed, so we have to do that ourselves

		responseStatus := "Haven't Tried Yet"
		var url string
		for responseStatus == "Haven't Tried Yet" || responseStatus == "301 Moved Permanently" {
			url = conn.assembleURL(apiOp, peer)
			req, err := http.NewRequest("POST", url, bytes.NewBuffer(jStatements))
			if err != nil {
				trace("%s: got error '%s' doing http.NewRequest", conn.ID, err.Error())
				failureLog = append(failureLog, fmt.Sprintf("%s failed due to %s", url, err.Error()))
				continue PeerLoop
			}
			req.Header.Set("Content-Type", "application/json")
			client := &http.Client{}
			response, err := client.Do(req)
			if err != nil {
				trace("%s: got error '%s' doing client.Do", conn.ID, err.Error())
				failureLog = append(failureLog, fmt.Sprintf("%s failed due to %s", url, err.Error()))
				continue PeerLoop
			}
			defer response.Body.Close()
			responseBody, err = ioutil.ReadAll(response.Body)
			if err != nil {
				trace("%s: got error '%s' doing ioutil.ReadAll", conn.ID, err.Error())
				failureLog = append(failureLog, fmt.Sprintf("%s failed due to %s", url, err.Error()))
				continue PeerLoop
			}
			responseStatus = response.Status
			if responseStatus == "301 Moved Permanently" {
				v := response.Header["Location"]
				failureLog = append(failureLog, fmt.Sprintf("%s redirected me to %s", url, v[0]))
				url = v[0]
				continue PeerLoop
			} else if responseStatus == "200 OK" {
				trace("%s: api call OK, returning", conn.ID)
				return responseBody, nil
			} else {
				trace("%s: got error in responseStatus: %s", conn.ID, responseStatus)
				failureLog = append(failureLog, fmt.Sprintf("%s failed, got: %s", url, response.Status))
				continue PeerLoop
			}
		}
	}

	// if we got here, all peers failed.  Let's build a verbose error message
	var stringBuffer bytes.Buffer
	stringBuffer.WriteString("tried all peers unsuccessfully. here are the results:\n")
	for n, v := range failureLog {
		stringBuffer.WriteString(fmt.Sprintf("   peer #%d: %s\n", n, v))
	}
	return responseBody, errors.New(stringBuffer.String())
}
