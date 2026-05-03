package gorqlite

import (
	"context"
	"encoding/json"
	"errors"
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
	return conn.RequestContext(context.Background(), sqlStatements)
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

	if conn.isClosed() {
		results = append(results, RequestResult{Err: ErrClosed})
		return results, ErrClosed
	}

	trace("%s: Request() for %d statements", conn.ID, len(sqlStatements))

	before := time.Now()
	response, err := conn.rqliteApiPost(ctx, api_REQUEST, sqlStatements)
	if err != nil {
		trace("%s: rqliteApiCall() ERROR: %s", conn.ID, err.Error())
		results = append(results, RequestResult{Err: err})
		return results, err
	}
	trace("%s: rqliteApiCall() OK, duration: %s", conn.ID, time.Since(before))

	var resp responseJSON
	if err := json.Unmarshal(response, &resp); err != nil {
		trace("%s: json.Unmarshal() ERROR: %s", conn.ID, err.Error())
		results = append(results, RequestResult{Err: err})
		return results, err
	}

	if resp.Error != "" {
		trace("%s: api ERROR: %s", conn.ID, resp.Error)
		apiErr := errors.New(resp.Error)
		results = append(results, RequestResult{Err: apiErr})
		return results, apiErr
	}

	if resp.Results == nil {
		err = errors.New("result key is missing from response")
		trace("%s: missing results key: %s", conn.ID, err)
		results = append(results, RequestResult{Err: err})
		return results, err
	}

	var errs []error
	for n, r := range resp.Results {
		trace("%s: parsing result %d", conn.ID, n)
		var thisR RequestResult

		if r.Error != "" {
			thisR.Err = errors.New(r.Error)
			results = append(results, thisR)
			errs = append(errs, thisR.Err)
			continue
		}

		// A query result always has a columns array; a write result
		// never does. This is what the unified endpoint guarantees.
		if r.Columns != nil || r.Values != nil {
			qr := conn.parseQueryResult(r)
			thisR.Query = &qr
		} else {
			wr := conn.parseWriteResult(r)
			thisR.Write = &wr
		}
		results = append(results, thisR)
	}

	trace("%s: finished parsing, returning %d results", conn.ID, len(results))

	return results, joinErrors(errs...)
}
