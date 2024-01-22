package gorqlite

// this file has low level stuff:
//
// rqliteApiGet()
// rqliteApiPost()
//
// There is some code duplication between those and they should
// probably be combined into one function.
//
// nothing public here.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	nurl "net/url"
	"strings"
)

type ParameterizedStatement struct {
	Query     string
	Arguments []interface{}
}

// method: rqliteApiCall() - internally handles api calls,
// not supposed to be used by other files
//
//   - handles retries
//   - handles timeouts
func (conn *Connection) rqliteApiCall(ctx context.Context, apiOp apiOperation, method string, requestBody []byte) ([]byte, error) {
	// Verify that we have at least a single peer to which we can make the request
	peers := conn.cluster.PeerList()
	if len(peers) < 1 {
		return nil, errors.New("don't have any cluster info")
	}
	trace("%s: I have a peer list %d peers long", conn.ID, len(peers))

	// Keep list of failed requests to each peer, return in case all peers fail to answer
	var failureLog []string

	for i, peer := range peers {
		trace("%s: attemping to contact peer %d", conn.ID, i)
		url := conn.assembleURL(apiOp, peer)

		// Prepare request
		var bodyReader io.Reader
		if requestBody != nil {
			bodyReader = bytes.NewBuffer(requestBody)
		}
		req, err := http.NewRequestWithContext(ctx, method, url, bodyReader)
		if err != nil {
			trace("%s: got error '%s' doing http.NewRequest", conn.ID, err.Error())
			failureLog = append(failureLog, fmt.Sprintf("%s failed due to %s", redactURL(url), err.Error()))
			continue
		}
		trace("%s: http.NewRequest() OK", conn.ID)
		req.Header.Set("Content-Type", "application/json")

		// Execute request using shared client
		// We will close the response body as soon as we can to allow
		// the TCP connection to escape back into client's pool
		response, err := conn.client.Do(req)
		if err != nil {
			trace("%s: got error '%s' doing client.Do", conn.ID, err.Error())
			failureLog = append(failureLog, fmt.Sprintf("%s failed due to %s", redactURL(url), err.Error()))
			continue
		}

		// Read response body even if not a successful answer to return a descriptive error message
		responseBody, err := io.ReadAll(response.Body)
		if err != nil {
			trace("%s: got error '%s' doing ioutil.ReadAll", conn.ID, err.Error())
			failureLog = append(failureLog, fmt.Sprintf("%s failed due to %s", redactURL(url), err.Error()))
			response.Body.Close()
			continue
		}
		trace("%s: ioutil.ReadAll() OK", conn.ID)

		// Check that we've got a successful answer
		if response.StatusCode != http.StatusOK {
			trace("%s: got code %s", conn.ID, response.Status)
			failureLog = append(failureLog, fmt.Sprintf("%s failed, got: %s, message: %s", redactURL(url), response.Status, string(responseBody)))
			response.Body.Close()
			continue
		}
		response.Body.Close()
		trace("%s: client.Do() OK", conn.ID)

		return responseBody, nil
	}

	// All peers have failed to answer us, build a verbose error message
	var builder strings.Builder
	builder.WriteString("tried all peers unsuccessfully. here are the results:\n")
	for n, v := range failureLog {
		builder.WriteString(fmt.Sprintf("   peer #%d: %s\n", n, v))
	}
	return nil, errors.New(builder.String())
}

// redactURL redacts URL from the given parameter to be
// safely read by the client
func redactURL(url string) string {
	u, err := nurl.Parse(url)
	if err != nil {
		return ""
	}
	return u.Redacted()
}

//	   method: rqliteApiGet() - for api_STATUS and api_NODES
//
//		- lowest level interface - does not do any JSON unmarshaling
//		- handles retries
//		- handles timeouts
func (conn *Connection) rqliteApiGet(ctx context.Context, apiOp apiOperation) ([]byte, error) {
	var responseBody []byte
	trace("%s: rqliteApiGet() called", conn.ID)

	// Allow only api_STATUS and api_NODES now - maybe someday BACKUP
	if apiOp != api_STATUS && apiOp != api_NODES {
		return responseBody, errors.New("rqliteApiGet() called for invalid api operation")
	}

	return conn.rqliteApiCall(ctx, apiOp, "GET", nil)
}

//	   method: rqliteApiPost() - for api_QUERY and api_WRITE
//
//		- lowest level interface - does not do any JSON unmarshaling
//		- handles retries
//		- handles timeouts
func (conn *Connection) rqliteApiPost(ctx context.Context, apiOp apiOperation, sqlStatements []ParameterizedStatement) ([]byte, error) {
	var responseBody []byte

	// Allow only api_QUERY, api_WRITE and api_REQUEST
	if apiOp != api_QUERY && apiOp != api_WRITE && apiOp != api_REQUEST {
		return responseBody, errors.New("rqliteApiPost() called for invalid api operation")
	}

	trace("%s: rqliteApiPost() called for a QUERY of %d statements", conn.ID, len(sqlStatements))

	formattedStatements := make([][]interface{}, 0, len(sqlStatements))

	for _, statement := range sqlStatements {
		formattedStatement := make([]interface{}, 0, len(statement.Arguments)+1)
		formattedStatement = append(formattedStatement, statement.Query)
		formattedStatement = append(formattedStatement, statement.Arguments...)
		formattedStatements = append(formattedStatements, formattedStatement)
	}

	body, err := json.Marshal(formattedStatements)
	if err != nil {
		return nil, err
	}

	return conn.rqliteApiCall(ctx, apiOp, "POST", body)
}
