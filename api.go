package gorqlite

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

// rqliteApiCall executes one HTTP request, retrying against each
// known peer in turn. Returns the body of the first 2xx response, or
// a combined error listing every peer's failure.
//
// If the supplied context is already done before or between attempts,
// the call short-circuits and returns ctx.Err(): walking the rest of
// the peer list buys nothing once the caller has given up.
func (conn *Connection) rqliteApiCall(ctx context.Context, apiOp apiOperation, method string, requestBody []byte) ([]byte, error) {
	peers := conn.snapshotPeerList()
	if len(peers) < 1 {
		return nil, errors.New("don't have any cluster info")
	}
	trace("%s: I have a peer list %d peers long", conn.ID, len(peers))

	var failureLog []string

	for i, p := range peers {
		if err := ctx.Err(); err != nil {
			trace("%s: ctx done before peer %d, aborting retries", conn.ID, i)
			return nil, err
		}
		trace("%s: attempting to contact peer %d", conn.ID, i)
		url := conn.assembleURL(apiOp, p)

		body, err := conn.doOnce(ctx, method, url, requestBody)
		if err != nil {
			failureLog = append(failureLog, fmt.Sprintf("%s failed due to %s", redactURL(url), err.Error()))
			continue
		}
		return body, nil
	}

	var builder strings.Builder
	builder.WriteString("tried all peers unsuccessfully. here are the results:\n")
	for n, v := range failureLog {
		builder.WriteString(fmt.Sprintf("   peer #%d: %s\n", n, v))
	}
	return nil, errors.New(builder.String())
}

// doOnce performs a single HTTP attempt. The response body is closed
// before returning regardless of outcome.
func (conn *Connection) doOnce(ctx context.Context, method, url string, requestBody []byte) ([]byte, error) {
	var bodyReader io.Reader
	if requestBody != nil {
		bodyReader = bytes.NewBuffer(requestBody)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, bodyReader)
	if err != nil {
		trace("%s: got error '%s' doing http.NewRequest", conn.ID, err.Error())
		return nil, err
	}
	trace("%s: http.NewRequest() OK", conn.ID)
	req.Header.Set("Content-Type", "application/json")

	response, err := conn.client.Do(req)
	if err != nil {
		trace("%s: got error '%s' doing client.Do", conn.ID, err.Error())
		return nil, err
	}
	defer response.Body.Close()

	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		trace("%s: got error '%s' reading response body", conn.ID, err.Error())
		return nil, err
	}
	trace("%s: response body read OK", conn.ID)

	if response.StatusCode != http.StatusOK {
		trace("%s: got code %s", conn.ID, response.Status)
		return nil, fmt.Errorf("got: %s, message: %s", response.Status, string(responseBody))
	}
	trace("%s: client.Do() OK", conn.ID)
	return responseBody, nil
}

// redactURL replaces the userinfo portion of url with the mask
// produced by net/url's Redacted helper. Returns "" if the url is
// malformed.
func redactURL(url string) string {
	u, err := nurl.Parse(url)
	if err != nil {
		return ""
	}
	return u.Redacted()
}

// rqliteApiGet is the GET variant of rqliteApiCall, restricted to
// read-only endpoints.
func (conn *Connection) rqliteApiGet(ctx context.Context, apiOp apiOperation) ([]byte, error) {
	trace("%s: rqliteApiGet() called", conn.ID)

	if apiOp != api_STATUS && apiOp != api_NODES {
		return nil, errors.New("rqliteApiGet() called for invalid api operation")
	}

	return conn.rqliteApiCall(ctx, apiOp, "GET", nil)
}

// rqliteApiPost is the POST variant, used for query/write/request.
// It serialises the parameterized statements into rqlite's
// nested-array body format.
func (conn *Connection) rqliteApiPost(ctx context.Context, apiOp apiOperation, sqlStatements []ParameterizedStatement) ([]byte, error) {
	if apiOp != api_QUERY && apiOp != api_WRITE && apiOp != api_WRITE_QUEUED && apiOp != api_REQUEST {
		return nil, errors.New("rqliteApiPost() called for invalid api operation")
	}

	trace("%s: rqliteApiPost() called for %s of %d statements", conn.ID, apiOpName(apiOp), len(sqlStatements))

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
