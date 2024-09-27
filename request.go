package gorqlite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// RequestResult holds the result of a single statement sent to Unified Endpoint.
//
// If statement failed, Err contains the error, neither Query nor Write is set.
// If statement succeeded, either of Query or Write is set — depending on the type of the statement.
// Query.Err and Write.Err are never set.
type RequestResult struct {
	Err   error
	Query *QueryResult
	Write *WriteResult
}

// Request is used to access Unified Endpoint to send read and writes requests in one operation.
func (conn *Connection) Request(sqlStatements []string) (results []RequestResult, err error) {
	parameterizedStatements := make([]ParameterizedStatement, 0, len(sqlStatements))
	for _, sqlStatement := range sqlStatements {
		parameterizedStatements = append(parameterizedStatements, ParameterizedStatement{
			Query: sqlStatement,
		})
	}

	return conn.RequestParameterized(parameterizedStatements)
}

// RequestContext is used to access Unified Endpoint to send read and writes requests in one operation.
//
// To use RequestContext with parameterized queries, use RequestParameterizedContext.
func (conn *Connection) RequestContext(ctx context.Context, sqlStatements []string) (results []RequestResult, err error) {
	parameterizedStatements := make([]ParameterizedStatement, 0, len(sqlStatements))
	for _, sqlStatement := range sqlStatements {
		parameterizedStatements = append(parameterizedStatements, ParameterizedStatement{
			Query: sqlStatement,
		})
	}

	return conn.RequestParameterizedContext(ctx, parameterizedStatements)
}

// RequestParameterized is used to access Unified Endpoint to send read and writes requests in one operation.
//
// It takes an array of parameterized SQL statements and executes them in a single transaction,
// returning an array of RequestResult vars.

// RequestParameterized returns an error if one is encountered during its operation.
// If it's something like a call to the rqlite API, then it'll return that error.
// If one statement out of several has an error, you can look at the individual statement's Err for more info.
//
// RequestParameterized uses context.Background() internally; to specify the context, use RequestParameterizedContext.
func (conn *Connection) RequestParameterized(sqlStatements []ParameterizedStatement) (results []RequestResult, err error) {
	return conn.RequestParameterizedContext(context.Background(), sqlStatements)
}

// RequestParameterizedContext is used to access Unified Endpoint to send read and writes requests in one operation.
//
// It takes an array of parameterized SQL statements and executes them in a single transaction,
// returning an array of RequestResult vars.

// RequestParameterizedContext returns an error if one is encountered during its operation.
// If it's something like a call to the rqlite API, then it'll return that error.
// If one statement out of several has an error, you can look at the individual statement's Err for more info.
func (conn *Connection) RequestParameterizedContext(ctx context.Context, sqlStatements []ParameterizedStatement) (results []RequestResult, err error) {
	results = make([]RequestResult, 0)

	if conn.hasBeenClosed {
		var errResult RequestResult
		errResult.Err = ErrClosed
		results = append(results, errResult)
		return results, ErrClosed
	}

	trace("%s: Request() for %d statements", conn.ID, len(sqlStatements))

	before := time.Now()
	// if we get an error POSTing, that's a showstopper
	response, err := conn.rqliteApiPost(ctx, api_REQUEST, sqlStatements)
	after := time.Now()
	if err != nil {
		trace("%s: rqliteApiCall() ERROR: %s", conn.ID, err.Error())
		var errResult RequestResult
		errResult.Err = err
		results = append(results, errResult)
		return results, err
	}
	trace("%s: rqliteApiCall() OK, duration: %s", conn.ID, after.Sub(before))

	// if we get an error Unmarshaling, that's a showstopper
	var sections map[string]interface{}
	err = json.Unmarshal(response, &sections)
	if err != nil {
		trace("%s: json.Unmarshal() ERROR: %s", conn.ID, err.Error())
		var errResult RequestResult
		errResult.Err = err
		results = append(results, errResult)
		return results, err
	}

	// if we got an error from the api, that's a showstopper
	if errMsg, ok := sections["error"].(string); ok && errMsg != "" {
		trace("%s: api ERROR: %s", conn.ID, errMsg)
		var errResult RequestResult
		errResult.Err = fmt.Errorf("%s", errMsg)
		results = append(results, errResult)
		return results, errResult.Err
	}

	// at this point, we have a "results" section and
	// a "time" section.  we can ignore the latter.

	resultsArray, ok := sections["results"].([]interface{})
	if !ok {
		err = errors.New("result key is missing from response")
		trace("%s: sections[\"results\"] ERROR: %s", conn.ID, err)
		var errResult RequestResult
		errResult.Err = err
		results = append(results, errResult)
		return results, err
	}

	var errs []error
	for n, r := range resultsArray {
		trace("%s: parsing result %d", conn.ID, n)
		var thisR RequestResult

		// r is a hash with columns, types, values, and time
		thisResult := r.(map[string]interface{})

		// did we get an error?
		_, ok := thisResult["error"]
		if ok {
			trace("%s: have an error on this result: %s", conn.ID, thisResult["error"].(string))
			thisR.Err = errors.New(thisResult["error"].(string))
			results = append(results, thisR)
			errs = append(errs, thisR.Err)
			continue
		}

		_, hasValues := thisResult["values"]
		_, hasColumns := thisResult["columns"]
		if hasValues || hasColumns {
			// Presence of these keys means this is a query result
			qr := conn.parseQueryResult(thisResult)
			qr.conn = conn
			thisR.Query = &qr
		} else {
			wr := conn.parseWriteResult(thisResult)
			wr.conn = conn
			thisR.Write = &wr
		}
		results = append(results, thisR)
	}

	trace("%s: finished parsing, returning %d results", conn.ID, len(results))

	return results, joinErrors(errs...)
}
