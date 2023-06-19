package gorqlite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

// RequestOne wraps Request() into a single-statement method.
//
// RequestOne uses context.Background() internally; to specify the context, use RequestOneContext.
func (conn *Connection) RequestOne(sqlStatement string) (RequestResult, error) {
	wra, err := conn.Request([]string{sqlStatement})
	return wra[0], err
}

// RequestOneContext wraps RequestContext() into a single-statement
func (conn *Connection) RequestOneContext(ctx context.Context, sqlStatement string) (RequestResult, error) {
	wra, err := conn.RequestContext(ctx, []string{sqlStatement})
	return wra[0], err
}

// RequestOneParameterized wraps RequestParameterized() into a single-statement method.
//
// RequestOneParameterized uses context.Background() internally; to specify the context, use RequestOneParameterizedContext.
func (conn *Connection) RequestOneParameterized(statement ParameterizedStatement) (RequestResult, error) {
	wra, err := conn.RequestParameterized([]ParameterizedStatement{statement})
	return wra[0], err
}

// RequestOneParameterizedContext wraps RequestParameterizedContext into
// a single-statement method.
func (conn *Connection) RequestOneParameterizedContext(ctx context.Context, statement ParameterizedStatement) (RequestResult, error) {
	wra, err := conn.RequestParameterizedContext(ctx, statement)
	return wra[0], err
}

func (conn *Connection) RequestStmt(ctx context.Context, sqlStatements ...*Statement) ([]RequestResult, error) {
	return conn.RequestParameterizedContext(ctx, makeParameterizedStatements(sqlStatements)...)
}

// Request is used to perform DDL/DML in the database synchronously without parameters.
//
// Request uses context.Background() internally; to specify the context, use RequestContext.
// To use Request with parameterized queries, use RequestParameterized.
func (conn *Connection) Request(sqlStatements []string) ([]RequestResult, error) {
	parameterizedStatements := make([]ParameterizedStatement, 0, len(sqlStatements))
	for _, sqlStatement := range sqlStatements {
		parameterizedStatements = append(parameterizedStatements, ParameterizedStatement{
			Query: sqlStatement,
		})
	}

	return conn.RequestParameterized(parameterizedStatements)
}

// RequestContext is used to perform DDL/DML in the database synchronously without parameters.
//
// To use RequestContext with parameterized queries, use RequestParameterizedContext.
func (conn *Connection) RequestContext(ctx context.Context, sqlStatements []string) ([]RequestResult, error) {
	parameterizedStatements := make([]ParameterizedStatement, 0, len(sqlStatements))
	for _, sqlStatement := range sqlStatements {
		parameterizedStatements = append(parameterizedStatements, ParameterizedStatement{
			Query: sqlStatement,
		})
	}

	return conn.RequestParameterizedContext(ctx, parameterizedStatements...)
}

// RequestParameterized is used to perform DDL/DML in the database synchronously.
//
// RequestParameterized takes an array of SQL statements, and returns an equal-sized array of RequestResults,
// each corresponding to the SQL statement that produced it.
//
// All statements are executed as a single transaction.
//
// RequestParameterized returns an error if one is encountered during its operation.
// If it's something like a call to the rqlite API, then it'll return that error.
// If one statement out of several has an error, it will return a generic
// "there were %d statement errors" and you'll have to look at the individual statement's Err for more info.
//
// RequestParameterized uses context.Background() internally; to specify the context, use RequestParameterizedContext.
func (conn *Connection) RequestParameterized(sqlStatements []ParameterizedStatement) ([]RequestResult, error) {
	return conn.RequestParameterizedContext(context.Background(), sqlStatements...)
}

// RequestParameterizedContext is used to perform DDL/DML in the database synchronously.
//
// RequestParameterizedContext takes an array of SQL statements, and returns an equal-sized array of RequestResults,
// each corresponding to the SQL statement that produced it.
//
// All statements are executed as a single transaction.
//
// RequestParameterizedContext returns an error if one is encountered during its operation.
// If it's something like a call to the rqlite API, then it'll return that error.
// If one statement out of several has an error, it will return a generic
// "there were %d statement errors" and you'll have to look at the individual statement's Err for more info.
func (conn *Connection) RequestParameterizedContext(ctx context.Context, sqlStatements ...ParameterizedStatement) ([]RequestResult, error) {
	results := make([]RequestResult, 0)

	if conn.hasBeenClosed {
		results = append(results, RequestResult{Err: ErrClosed})
		return results, ErrClosed
	}

	trace("%s: Request() for %d statements", conn.ID, len(sqlStatements))

	response, err := conn.rqliteApiPost(ctx, api_REQUEST, sqlStatements)
	if err != nil {
		trace("%s: rqliteApiCall() ERROR: %s", conn.ID, err.Error())
		results = append(results, RequestResult{Err: err})
		return results, err
	}
	trace("%s: rqliteApiCall() OK", conn.ID)

	var sections map[string]interface{}
	err = json.Unmarshal(response, &sections)
	if err != nil {
		trace("%s: json.Unmarshal() ERROR: %s", conn.ID, err.Error())
		results = append(results, RequestResult{Err: err})
		return results, err
	}
	// stop if we got an error from the api
	if errMsg, ok := sections["error"].(string); ok && errMsg != "" {
		trace("%s: api ERROR: %s", conn.ID, errMsg)

		err = fmt.Errorf("%s", errMsg)
		results = append(results, RequestResult{Err: err})
		return results, err
	}

	// at this point, we have a "results" section and
	// a "time" section.  we can ignore the latter.

	resultsArray, ok := sections["results"].([]interface{})
	if !ok {
		err = errors.New("result key is missing from response")
		trace("%s: sections[\"results\"] ERROR: %s", conn.ID, err)
		results = append(results, RequestResult{Err: err})
		return results, err
	}

	trace("%s: I have %d result(s) to parse", conn.ID, len(resultsArray))
	numStatementErrors := 0
	for n, k := range resultsArray {
		trace("%s: starting on result %d", conn.ID, n)
		thisRR := conn.makeRequestResult(k.(map[string]interface{}))
		if thisRR.Err != nil {
			numStatementErrors++
		} else if !thisRR.Write.IsZero() {
			trace("%s: this result (LII,RA,T): %d %d %f", conn.ID, thisRR.Write.LastInsertID, thisRR.Write.RowsAffected, thisRR.Write.Timing)
		} else {
			trace("%s: this result (#col,time) %d %f", conn.ID, len(thisRR.Query.columns), thisRR.Query.Timing)
		}
		results = append(results, thisRR)
	}

	trace("%s: finished parsing, returning %d results", conn.ID, len(results))
	if numStatementErrors > 0 {
		return results, fmt.Errorf("there were %d statement errors", numStatementErrors)
	}

	return results, nil
}

func (conn *Connection) makeRequestResult(thisResult map[string]interface{}) RequestResult {
	_, cok := thisResult["columns"].([]interface{})
	_, tok := thisResult["types"].([]interface{})
	var q QueryResult
	var w WriteResult
	var err error
	if cok && tok {
		q = conn.makeQueryResult(thisResult)
		err = q.Err
	} else {
		w = conn.makeWriteResult(thisResult)
		err = w.Err
	}
	return RequestResult{
		Err:   err,
		Write: w,
		Query: q,
	}
}

// RequestResult holds the result of a request.
// Err is assigned if there was an error, otherwise only one of Write or Query
// is assigned and has its ID field set.
type RequestResult struct {
	Err   error // don't trust the rest if this isn't nil
	Write WriteResult
	Query QueryResult
}
